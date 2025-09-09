# routes/cold_candidates.py (FULL OVERWRITE)
from __future__ import annotations

from fastapi import APIRouter, Query
from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime, date as date_cls, timezone, timedelta
import unicodedata
import httpx
import pytz
import math

# NEW: real Statcast overlays (Batch fetch from Baseball Savant)
# Requires you to have services/statcast_enrichment.py in your tree (I provided a full version earlier).
# If you didn’t add it yet, ask and I’ll paste that full file again here so you can drop it in.
from services.statcast_enrichment import fetch_statcast_overlays  # <-- real Savant CSV enrichment

router = APIRouter()
MLB_BASE = "https://statsapi.mlb.com/api/v1"
_EASTERN = pytz.timezone("US/Eastern")

# ----------------- time & utils -----------------
def _eastern_today_str() -> str:
    return datetime.now(_EASTERN).date().isoformat()

def _is_today_et(ymd: str) -> bool:
    try:
        return _eastern_today_str() == ymd
    except Exception:
        return False

def _parse_ymd(s: str) -> date_cls:
    return datetime.strptime(s, "%Y-%m-%d").date()

def _next_ymd_str(s: str) -> str:
    return (_parse_ymd(s) + timedelta(days=1)).isoformat()

def _normalize(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower().strip()

def _parse_dt_utc(maybe: Optional[str]) -> Optional[datetime]:
    if not maybe:
        return None
    s = str(maybe)
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None

def _fetch_json(client: httpx.Client, url: str, params: Optional[Dict] = None) -> Dict:
    r = client.get(url, params=params)
    r.raise_for_status()
    return r.json()

def _fetch_json_safe(client: httpx.Client, url: str, params: Optional[Dict], dbg: Optional[List[Dict]], label: str) -> Dict:
    try:
        return _fetch_json(client, url, params=params)
    except Exception as e:
        if dbg is not None:
            dbg.append({"fetch_error": label, "error": f"{type(e).__name__}: {e}"})
        return {}

# ----------------- schedule helpers -----------------
def _schedule_for_date(client: httpx.Client, date_str: str, dbg: Optional[List[Dict]]) -> Dict:
    return _fetch_json_safe(client, f"{MLB_BASE}/schedule", {"sportId": 1, "date": date_str}, dbg, f"schedule:{date_str}")

def _not_started_team_ids_for_date(schedule_json: Dict) -> Set[int]:
    """
    STRICT pregame set: treat P=Preview, S=Scheduled (not started yet), PW=Pre-Game Warmup as NOT started.
    """
    ns_ids: Set[int] = set()
    for d in schedule_json.get("dates", []) or []:
        for g in d.get("games", []) or []:
            code = (g.get("status", {}) or {}).get("statusCode", "")
            if code in ("P", "S", "PW"):
                try:
                    ns_ids.add(int(g["teams"]["home"]["team"]["id"]))
                    ns_ids.add(int(g["teams"]["away"]["team"]["id"]))
                except Exception:
                    pass
    return ns_ids

def _team_ids_from_schedule(schedule_json: Dict) -> List[int]:
    ids: Set[int] = set()
    for d in schedule_json.get("dates", []) or []:
        for g in d.get("games", []) or []:
            try:
                ids.add(int(g["teams"]["home"]["team"]["id"]))
                ids.add(int(g["teams"]["away"]["team"]["id"]))
            except Exception:
                pass
    return sorted(ids)

def _schedule_rows(schedule_json: Dict) -> List[Tuple[str,str,str,int]]:
    """
    Returns (away@home, statusCode, statusText, gamePk) for footer.
    """
    out: List[Tuple[str,str,str,int]] = []
    for d in schedule_json.get("dates", []) or []:
        for g in d.get("games", []) or []:
            home = (((g.get("teams") or {}).get("home") or {}).get("team") or {}).get("name") or "?"
            away = (((g.get("teams") or {}).get("away") or {}).get("team") or {}).get("name") or "?"
            st = (g.get("status", {}) or {})
            code = st.get("statusCode", "")
            text = st.get("detailedState") or st.get("abstractGameState") or ""
            pk = g.get("gamePk") or 0
            try: pk = int(pk)
            except: pk = 0
            out.append((f"{away} @ {home}", code, text, pk))
    return out

def _game_pks_for_date(schedule_json: Dict) -> Set[int]:
    pks: Set[int] = set()
    for d in schedule_json.get("dates", []) or []:
        for g in d.get("games", []) or []:
            pk = g.get("gamePk")
            try:
                if pk is not None:
                    pks.add(int(pk))
            except Exception:
                continue
    return pks

def _all_mlb_team_ids(client: httpx.Client, season: int, dbg: Optional[List[Dict]]) -> List[int]:
    data = _fetch_json_safe(client, f"{MLB_BASE}/teams", {"sportId": 1, "season": season}, dbg, f"teams:{season}")
    teams = data.get("teams", []) or []
    out: List[int] = []
    for t in teams:
        try:
            out.append(int(t["id"]))
        except Exception:
            pass
    return sorted(out)

# ----------------- stats & logs -----------------
def _choose_best_mlb_season_split(splits: List[Dict]) -> Optional[Dict]:
    if not splits:
        return None
    def score(sp: Dict) -> Tuple[int, int]:
        league_id = (((sp.get("league") or {}) or {}).get("id"))
        sport_id = (((sp.get("sport") or {}) or {}).get("id"))
        if sport_id is None:
            sport_id = (((sp.get("team") or {}).get("sport") or {}) or {}).get("id")
        try:
            ab = int((sp.get("stat") or {}).get("atBats") or 0)
        except Exception:
            ab = 0
        pri = 2 if league_id in (103, 104) else (1 if sport_id == 1 else 0)
        return (pri, ab)
    best, key = None, (-1, -1)
    for sp in splits:
        k = score(sp)
        if k > key:
            key = k
            best = sp
    return best

def _season_avg_from_people_like(obj: Dict) -> Optional[float]:
    stats = (obj.get("stats") or [])
    for block in stats:
        gdn = (block.get("group") or {}).get("displayName", "")
        gcd = (block.get("group") or {}).get("code", "")
        tdn = (block.get("type") or {}).get("displayName", "")
        tcd = (block.get("type") or {}).get("code", "")
        if (gdn == "hitting" or gcd == "hitting") and (tdn.lower() == "season" or tcd == "season"):
            splits = block.get("splits") or []
            chosen = _choose_best_mlb_season_split(splits)
            if not chosen:
                continue
            try:
                return float(str((chosen.get("stat") or {}).get("avg")))
            except Exception:
                return None
    return None

def _season_ab_gp_from_people_like(obj: Dict) -> Tuple[Optional[int], Optional[int]]:
    stats = (obj.get("stats") or [])
    for block in stats:
        gdn = (block.get("group") or {}).get("displayName", "")
        gcd = (block.get("group") or {}).get("code", "")
        tdn = (block.get("type") or {}).get("displayName", "")
        tcd = (block.get("type") or {}).get("code", "")
        if (gdn == "hitting" or gcd == "hitting") and (tdn.lower() == "season" or tcd == "season"):
            splits = block.get("splits") or []
            chosen = _choose_best_mlb_season_split(splits)
            if not chosen:
                continue
            st = chosen.get("stat") or {}
            try:
                ab = int(st.get("atBats") or 0)
            except Exception:
                ab = None
            try:
                gp = int(st.get("gamesPlayed") or st.get("games") or 0)
            except Exception:
                gp = None
            return ab, gp
    return None, None

def _expected_abs_from_person(obj: Dict) -> float:
    ab, gp = _season_ab_gp_from_people_like(obj)
    if ab is not None and gp and gp > 0:
        try:
            v = float(ab) / float(gp)
            return max(2.0, min(5.5, v))
        except Exception:
            pass
    return 4.0

def _break_prob_from_avg_and_ab(avg: float, expected_abs: float) -> float:
    p_no_hit = (1.0 - max(0.0, min(1.0, float(avg)))) ** max(0.0, float(expected_abs))
    return 1.0 - p_no_hit

def _game_log_regular_season_desc(client: httpx.Client, pid: int, season: int, max_entries: int, dbg: Optional[List[Dict]]) -> List[Dict]:
    data = _fetch_json_safe(
        client,
        f"{MLB_BASE}/people/{pid}/stats",
        {"stats": "gameLog", "group": "hitting", "season": season, "sportIds": 1},
        dbg, f"gameLog:{pid}:{season}"
    )
    splits = ((data.get("stats") or [{}])[0].get("splits")) or []

    def is_regular(s: Dict) -> bool:
        gt = s.get("gameType")
        return (gt is None) or (gt == "R")

    def sort_key(s: Dict[str, Any]) -> tuple:
        dt = _parse_dt_utc(s.get("gameDate") or s.get("date"))
        ts = dt.timestamp() if dt else -1.0
        pk = s.get("game", {}).get("gamePk") or s.get("gamePk") or 0
        try:
            pk = int(pk)
        except Exception:
            pk = 0
        return (ts, pk)

    filtered = [s for s in splits if is_regular(s)]
    filtered.sort(key=sort_key, reverse=True)
    return filtered[:max_entries]

def _date_in_eastern(dt_utc: datetime) -> date_cls:
    return dt_utc.astimezone(_EASTERN).date()

def _extract_team_name_from_person_or_logs(
    person_like: Dict,
    team_map: Optional[Dict[int, Tuple[int, str]]] = None,
    pid: Optional[int] = None,
    logs: Optional[List[Dict]] = None,
    slate_date_ymd: Optional[str] = None,
) -> str:
    team_info = (person_like.get("currentTeam") or {}) if isinstance(person_like, dict) else {}
    name = (team_info.get("name") or "").strip()
    if name:
        return name
    t2 = (person_like.get("team") or {})
    name2 = (t2.get("name") or "").strip()
    if name2:
        return name2
    if logs and slate_date_ymd:
        slate_date = _parse_ymd(slate_date_ymd)
        for s in logs:
            dt_utc = _parse_dt_utc(s.get("gameDate") or s.get("date"))
            if not dt_utc:
                continue
            if _date_in_eastern(dt_utc) >= slate_date:
                continue
            t = (s.get("team") or {})
            nm = (t.get("name") or "").strip()
            if nm:
                return nm
    if pid is not None and team_map and pid in team_map:
        return (team_map[pid][1] or "").strip() or "N/A"
    return "N/A"

def _current_hitless_streak_before_slate(
    game_splits: List[Dict],
    slate_date_ymd: str,
    exclude_game_pks: Optional[Set[int]] = None
) -> int:
    slate_date = _parse_ymd(slate_date_ymd)
    exclude_game_pks = exclude_game_pks or set()
    streak = 0
    for s in game_splits:
        pk = s.get("game", {}).get("gamePk") or s.get("gamePk")
        try:
            if pk is not None and int(pk) in exclude_game_pks:
                continue
        except Exception:
            pass

        dt_utc = _parse_dt_utc(s.get("gameDate") or s.get("date"))
        if not dt_utc:
            continue
        if _date_in_eastern(dt_utc) >= slate_date:
            continue  # exclude same-day/future

        stat = s.get("stat") or {}
        try:
            ab = int(stat.get("atBats") or 0)
            hits = int(stat.get("hits") or 0)
        except Exception:
            ab, hits = 0, 0
        if ab <= 0:
            continue
        if hits == 0:
            streak += 1
        else:
            break
    return streak

def _average_hitless_streak_before_slate(
    game_splits: List[Dict],
    slate_date_ymd: str,
    exclude_game_pks: Optional[Set[int]] = None
) -> Optional[float]:
    slate_date = _parse_ymd(slate_date_ymd)
    exclude_game_pks = exclude_game_pks or set()

    prior: List[Dict] = []
    for s in game_splits:
        pk = s.get("game", {}).get("gamePk") or s.get("gamePk")
        try:
            if pk is not None and int(pk) in exclude_game_pks:
                continue
        except Exception:
            pass

        dt_utc = _parse_dt_utc(s.get("gameDate") or s.get("date"))
        if not dt_utc:
            continue
        if _date_in_eastern(dt_utc) >= slate_date:
            continue
        stat = s.get("stat") or {}
        try:
            ab = int(stat.get("atBats") or 0)
        except Exception:
            ab = 0
        if ab > 0:
            prior.append(s)
    prior.reverse()  # oldest -> newest

    streaks: List[int] = []
    run = 0
    for s in prior:
        stat = s.get("stat") or {}
        try:
            hits = int(stat.get("hits") or 0)
        except Exception:
            hits = 0
        if hits == 0:
            run += 1
        else:
            if run > 0:
                streaks.append(run)
                run = 0

    if not streaks:
        return None
    return sum(streaks) / len(streaks)

# ----------------- sorting helpers -----------------
_VALID_SORT_KEYS = {
    "hitless_streak", "season_avg", "avg_hitless_streak_season",
    "break_prob_next", "pressure", "score", "hit_chance_pct",
    "overdue_ratio", "ranking_score", "score_plus", "composite"
}

def _parse_sort_by(sort_by: Optional[str]) -> List[Tuple[str, bool]]:
    default = [("hitless_streak", True), ("season_avg", True), ("avg_hitless_streak_season", True)]
    if not sort_by:
        return default
    out: List[Tuple[str, bool]] = []
    for raw in sort_by.split(","):
        k = raw.strip()
        if not k:
            continue
        desc = k.startswith("-")
        field = k[1:] if desc else k
        if field in _VALID_SORT_KEYS:
            out.append((field, desc))
    return out or default

def _apply_sort(candidates: List[Dict], sort_spec: List[Tuple[str, bool]]) -> List[Dict]:
    def key_fn(item: Dict):
        keys = []
        for field, desc in sort_spec:
            v = item.get(field, 0)
            try:
                v = float(v)
            except Exception:
                v = 0.0
            keys.append(-v if desc else v)
        return tuple(keys)
    return sorted(candidates, key=key_fn)

# ----------------- candidate scoring helpers -----------------
def _decorate_candidate_with_scores(base: Dict, person_like: Dict) -> None:
    season_avg = float(base.get("season_avg", 0.0))
    expected_abs = _expected_abs_from_person(person_like)
    break_prob = _break_prob_from_avg_and_ab(season_avg, expected_abs)  # 0..1

    current = int(base.get("hitless_streak", 0))
    avg_streak = base.get("avg_hitless_streak_season", None)
    try:
        avg_streak_f = float(avg_streak) if avg_streak is not None else 1.0
    except Exception:
        avg_streak_f = 1.0
    denom = max(0.5, avg_streak_f)
    pressure = (float(current) / denom) if denom > 0 else float(current)

    score = break_prob * pressure

    base["expected_abs"] = round(expected_abs, 2)
    base["break_prob_next"] = round(break_prob * 100.0, 1)  # percent display
    base["pressure"] = round(pressure, 3)
    base["score"] = round(score * 100.0, 1)

    # canonical aliases
    base["hit_chance_pct"] = base["break_prob_next"]
    base["overdue_ratio"] = base["pressure"]
    base["ranking_score"] = base["score"]

def _scale_overdue_for_composite(overdue_ratio: float) -> float:
    # Scale ratio to 0..100 (3.0 ≈ 100, 2.0 ≈ 66.7)
    return max(0.0, min(100.0, overdue_ratio * 33.33))

def _scale_elite_avg_for_composite(avg: float) -> float:
    # 0 @ .260, 100 @ .340+, linear in between
    if avg <= 0.260: return 0.0
    if avg >= 0.340: return 100.0
    return (avg - 0.260) / (0.340 - 0.260) * 100.0

def _scale_statcast_for_composite(hh_percent_14d: Optional[float], xba_delta_14d: Optional[float]) -> float:
    # Combine HH% and xBA–BA to 0..100; if missing, treat as 0
    contrib = []
    if hh_percent_14d is not None:
        # 30 → 0, 50 → 100 (linear)
        if hh_percent_14d <= 30: hh_s = 0.0
        elif hh_percent_14d >= 50: hh_s = 100.0
        else: hh_s = (float(hh_percent_14d) - 30.0) / 20.0 * 100.0
        contrib.append(hh_s)
    if xba_delta_14d is not None:
        # +.00 → 0, +.08 → 100
        if xba_delta_14d <= 0.0: dx_s = 0.0
        elif xba_delta_14d >= 0.08: dx_s = 100.0
        else: dx_s = float(xba_delta_14d) / 0.08 * 100.0
        contrib.append(dx_s)
    if not contrib:
        return 0.0
    return sum(contrib) / len(contrib)

def _recompute_composite_in_place(c: Dict, w_hit_chance: float, w_overdue: float, w_elite_avg: float, w_statcast: float) -> None:
    hit_chance = float(c.get("hit_chance_pct", 0.0))          # 0..100
    overdue_ratio = float(c.get("overdue_ratio", 0.0))        # ~0..many
    avg = float(c.get("season_avg", 0.0))                     # 0..1
    sc = c.get("_statcast", {}) or {}
    hh = sc.get("hh_percent_14d")
    dx = sc.get("xba_delta_14d")

    overdue_scaled = _scale_overdue_for_composite(overdue_ratio)
    elite_scaled = _scale_elite_avg_for_composite(avg)
    stat_scaled = _scale_statcast_for_composite(hh, dx)

    composite = (
        w_hit_chance * (hit_chance / 100.0) +
        w_overdue * (overdue_scaled / 100.0) +
        w_elite_avg * (elite_scaled / 100.0) +
        w_statcast * (stat_scaled / 100.0)
    )

    c["score_plus"] = round(float(c.get("score", 0.0)), 1)
    c["composite"] = round(float(composite), 1)

# ----------------- route -----------------
@router.get("/cold_candidates")
def cold_candidates(
    date: str = Query("today", description="YYYY-MM-DD or 'today' (US/Eastern)"),
    season: int = Query(2025, ge=1900, le=2100),
    names: Optional[str] = Query(None, description="Optional comma-separated player names. If omitted, scans slate rosters."),
    min_season_avg: float = Query(0.26, ge=0.0, le=1.0, description="Only include hitters with season AVG ≥ this."),
    min_hitless_games: int = Query(1, ge=1, description="Current hitless streak (AB>0) must be ≥ this."),
    min_season_ab: int = Query(100, ge=0, description="Minimum season AB to qualify."),
    min_season_gp: int = Query(40, ge=0, description="Minimum season games played to qualify."),
    limit: int = Query(30, ge=1, le=1000),
    verify: int = Query(1, ge=0, le=1, description="1 = STRICT pregame only (teams not started yet). 0 = include all teams."),
    roll_to_next_slate_if_empty: int = Query(1, ge=0, le=1, description="If verify=1 and there are ZERO pregame teams today, roll to NEXT day."),
    scan_multiplier: int = Query(8, ge=1, le=40, description="How many logs to check: limit × scan_multiplier (cap applies)."),
    max_log_checks: Optional[int] = Query(None, ge=1, le=5000, description="Hard cap for log checks; overrides scan_multiplier."),
    debug: int = Query(0, ge=0, le=1),

    # aliases / presentation
    mode: Optional[str] = Query(None, description="Alias for verify. 'pregame' -> verify=1, 'all' -> verify=0."),
    as_of: Optional[str] = Query(None, description="YYYY-MM-DD snapshot. If not today ET, verification disabled."),
    group_by: str = Query("streak", description="Grouping preset: 'streak' (default) or 'none'."),
    sort_by: Optional[str] = Query(None, description="When group_by='none', comma-separated fields with optional '-' for DESC."),

    # Statcast controls (strict gating default)
    require_statcast_for_tiers: int = Query(1, ge=0, le=1, description="If 1, Tier S/A only shown when Statcast signal exists."),
    hh_recent_days: int = Query(14, ge=7, le=28, description="Statcast lookback window in days."),
    statcast_min_hh_14d: float = Query(40.0, description="HH%% (14d) threshold to treat as positive Statcast."),
    statcast_min_xba_delta_14d: float = Query(0.03, description="xBA–BA (14d) threshold to treat as positive Statcast."),

    # composite weights (your spec)
    w_hit_chance: float = Query(50.0),
    w_overdue: float = Query(17.5),
    w_elite_avg: float = Query(12.5),
    w_statcast: float = Query(20.0),

    # tier thresholds (your spec)
    tier_s_min_composite: float = Query(70.0),
    tier_s_min_hit_chance: float = Query(67.0),
    tier_s_min_overdue: float = Query(2.0),

    tier_a_min_composite: float = Query(55.0),
    tier_a_min_hit_chance: float = Query(62.0),
):
    """
    VERIFIED cold-hitter candidates with **strict pregame verification**,
    AB>0-only streak calculations, **bulk Statcast overlays**, and Statcast-gated Tier S/A.
    """
    requested_date = _eastern_today_str() if _normalize(date) == "today" else date
    effective_date = requested_date

    # alias mapping
    mode_norm = (mode or "").strip().lower() if mode else None
    if mode_norm in ("pregame", "pre", "strict"):
        verify_effective = 1
    elif mode_norm in ("all", "any"):
        verify_effective = 0
    else:
        verify_effective = 1 if int(verify) == 1 else 0

    # as_of snapshot handling
    as_of_norm = (as_of or "").strip()
    historical_mode = False
    if as_of_norm:
        try:
            _ = _parse_ymd(as_of_norm)
            effective_date = as_of_norm
            if not _is_today_et(as_of_norm):
                historical_mode = True
        except Exception:
            pass

    if historical_mode:
        verify_effective = 0
        roll_enabled = False
    else:
        roll_enabled = bool(roll_to_next_slate_if_empty)

    # scan budget
    DEFAULT_MULT = max(1, int(scan_multiplier))
    computed_max = min(3000, limit * DEFAULT_MULT)
    MAX_LOG_CHECKS = max_log_checks if max_log_checks is not None else computed_max

    if verify_effective == 0:
        if max_log_checks is None:
            MAX_LOG_CHECKS = min(MAX_LOG_CHECKS, 400)
        if "limit" in cold_candidates.__signature__.parameters:
            if limit > 30:
                limit = 30

    group_mode = (group_by or "").strip().lower()
    sort_spec = _parse_sort_by(sort_by) if group_mode == "none" else []

    with httpx.Client(timeout=45) as client:
        debug_list: Optional[List[Dict]] = [] if debug else None

        # schedule for date
        sched = _schedule_for_date(client, effective_date, debug_list)
        ns_team_ids_today = _not_started_team_ids_for_date(sched) if (verify_effective == 1) else set()
        slate_team_ids_today = _team_ids_from_schedule(sched) if sched else _all_mlb_team_ids(client, season, debug_list)
        exclude_pks_for_date = _game_pks_for_date(sched) if sched else set()
        sched_rows = _schedule_rows(sched)

        rolled = False
        if (verify_effective == 1) and roll_enabled and len(ns_team_ids_today) == 0:
            effective_date = _next_ymd_str(effective_date)
            sched = _schedule_for_date(client, effective_date, debug_list)
            ns_team_ids_today = _not_started_team_ids_for_date(sched)
            slate_team_ids_today = _team_ids_from_schedule(sched) or slate_team_ids_today
            exclude_pks_for_date = _game_pks_for_date(sched)
            sched_rows = _schedule_rows(sched)
            rolled = True

        # gather people
        candidates: List[Dict] = []

        def _qualify_by_ab_gp(person_like: Dict) -> bool:
            ab, gp = _season_ab_gp_from_people_like(person_like)
            if ab is None or gp is None:
                return False
            return (ab >= min_season_ab) and (gp >= min_season_gp)

        def _decorate_and_add(person: Dict, pid: int, target_date: str, team_map: Optional[Dict[int, Tuple[int, str]]] = None):
            logs = _game_log_regular_season_desc(client, pid, season, max_entries=160, dbg=debug_list)
            streak = _current_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)
            if streak < min_hitless_games:
                return
            avg_season_hitless = _average_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)
            team_name = _extract_team_name_from_person_or_logs(person, team_map, pid, logs, target_date)
            season_avg = _season_avg_from_people_like(person)
            if season_avg is None or season_avg < min_season_avg:
                return

            cand = {
                "name": person.get("fullName") or "",
                "team": team_name,
                "season_avg": round(float(season_avg), 3),
                "hitless_streak": int(streak),
                "avg_hitless_streak_season": round(avg_season_hitless, 2) if avg_season_hitless is not None else 0.0,
            }
            _decorate_candidate_with_scores(cand, person)

            # Placeholder for Statcast (real fill happens in a bulk pass later)
            cand["_statcast"] = {
                "has_signal": False,
                "why": "",
                "hh_percent_14d": None,
                "xba_delta_14d": None,
            }

            # Initial composite without Statcast (will be recomputed later after enrichment)
            _recompute_composite_in_place(
                cand, w_hit_chance=w_hit_chance, w_overdue=w_overdue, w_elite_avg=w_elite_avg, w_statcast=w_statcast
            )

            candidates.append(cand)

        # league mode or explicit names
        if names:
            requested = [n.strip() for n in names.split(",") if n.strip()]
            for name in requested:
                try:
                    data = _fetch_json(client, f"{MLB_BASE}/people/search", params={"names": name})
                    people = data.get("people", []) or []
                    if not people:
                        if debug_list is not None:
                            debug_list.append({"name": name, "skip": "player not found"})
                        continue
                    norm_target = _normalize(name)
                    p0 = next((p for p in people if _normalize(p.get("fullName","")) == norm_target), people[0])
                    pid = int(p0["id"])
                    pdata = _fetch_json(client, f"{MLB_BASE}/people/{pid}", params={"hydrate": f"team,stats(group=hitting,type=season,season={season})"})
                    person = (pdata.get("people") or [{}])[0]
                    if not _qualify_by_ab_gp(person):
                        continue
                    if verify_effective == 1:
                        team_info = person.get("currentTeam") or {}
                        try:
                            team_id = int(team_info.get("id")) if team_info.get("id") is not None else None
                        except Exception:
                            team_id = None
                        if team_id is None or team_id not in ns_team_ids_today:
                            continue
                    _decorate_and_add(person, pid, effective_date)
                    if len(candidates) >= limit:
                        break
                except Exception as e:
                    if debug_list is not None:
                        debug_list.append({"name": name, "error": f"{type(e).__name__}: {e}"})
        else:
            # league scan
            scan_team_ids = sorted(ns_team_ids_today) if verify_effective == 1 else slate_team_ids_today
            union_ids, team_map = _collect_union_player_ids(client, scan_team_ids, season, debug_list)
            people = _batch_people_with_stats(client, union_ids, season, debug_list)

            # normalize team
            for p in people:
                if not p.get("currentTeam"):
                    pid = p.get("id")
                    if isinstance(pid, int) and pid in team_map:
                        tid, tname = team_map[pid]
                        p["currentTeam"] = {"id": tid, "name": tname}

            prospects: List[Tuple[float, Dict]] = []
            for p in people:
                season_avg = _season_avg_from_people_like(p)
                if season_avg is None or season_avg < min_season_avg:
                    continue
                ab, gp = _season_ab_gp_from_people_like(p)
                if ab is None or gp is None or ab < min_season_ab or gp < min_season_gp:
                    continue
                try:
                    pid = int(p.get("id"))
                except Exception:
                    pid = None
                if pid is None:
                    continue
                if verify_effective == 1:
                    team_info = p.get("currentTeam") or {}
                    try:
                        team_id = int(team_info.get("id")) if team_info.get("id") is not None else None
                    except Exception:
                        team_id = None
                    if team_id is None or team_id not in ns_team_ids_today:
                        continue
                prospects.append((float(season_avg), {"pid": pid, "person": p}))

            prospects.sort(key=lambda x: x[0], reverse=True)

            checks = 0
            for _, meta in prospects:
                if checks >= MAX_LOG_CHECKS or len(candidates) >= limit:
                    break
                checks += 1
                try:
                    _decorate_and_add(meta["person"], meta["pid"], effective_date, team_map)
                except Exception as e:
                    if debug_list is not None:
                        dbg_name = (meta["person"] or {}).get("fullName","")
                        debug_list.append({"name": dbg_name, "error": f"{type(e).__name__}: {e}"})

        # -------- Statcast enrichment pass (BULK) --------
        # Enrich all candidates in bulk via Baseball Savant, then recompute composite & re-sort/group.
        try:
            name_list = [c.get("name","") for c in candidates if c.get("name")]
            overlays = fetch_statcast_overlays(
                name_list,
                recent_days=hh_recent_days,
                statcast_min_hh_14d=statcast_min_hh_14d,
                statcast_min_xba_delta_14d=statcast_min_xba_delta_14d,
            )
        except Exception as e:
            overlays = {}
            if debug_list is not None:
                debug_list.append({"statcast_bulk_error": f"{type(e).__name__}: {e}"})

        for c in candidates:
            nm = c.get("name","")
            sc = overlays.get(nm, {}) if nm else {}
            has_signal = bool(sc.get("has_signal"))
            why = sc.get("why") or ""
            hh = sc.get("hh_percent_14d")
            dx = sc.get("xba_delta_14d")
            c["_statcast"] = {
                "has_signal": has_signal,
                "why": why,
                "hh_percent_14d": hh,
                "xba_delta_14d": dx,
            }
            # recompute composite with true Statcast
            _recompute_composite_in_place(
                c, w_hit_chance=w_hit_chance, w_overdue=w_overdue, w_elite_avg=w_elite_avg, w_statcast=w_statcast
            )

        # -------- group/sort after Statcast recompute --------
        if group_mode == "streak":
            buckets: Dict[int, List[Dict]] = {}
            for c in candidates:
                buckets.setdefault(int(c.get("hitless_streak", 0)), []).append(c)
            out_list: List[Dict] = []
            for k in sorted(buckets.keys(), reverse=True):
                grp = sorted(
                    buckets[k],
                    key=lambda x: (
                        float(x.get("composite", 0.0)),
                        float(x.get("ranking_score", x.get("score", 0.0))),
                        float(x.get("season_avg", 0.0)),
                    ),
                    reverse=True
                )
                out_list.extend(grp)
            candidates = out_list[:limit]
        else:
            if sort_spec:
                candidates = _apply_sort(candidates, sort_spec)
            else:
                candidates = sorted(
                    candidates,
                    key=lambda x: (
                        float(x.get("composite", 0.0)),
                        float(x.get("ranking_score", x.get("score", 0.0))),
                        float(x.get("season_avg", 0.0)),
                    ),
                    reverse=True
                )
            candidates = candidates[:limit]

        # -------- Build Tier S / A (Statcast gate enforced if require_statcast_for_tiers=1) --------
        best_targets_s: List[Dict] = []
        best_targets_a: List[Dict] = []
        for c in candidates:
            hit_ch = float(c.get("hit_chance_pct", 0.0))
            overdue = float(c.get("overdue_ratio", 0.0))
            comp = float(c.get("composite", 0.0))
            sc = c.get("_statcast", {}) or {}
            has_sig = bool(sc.get("has_signal"))

            # enforce gate
            if require_statcast_for_tiers == 1 and not has_sig:
                continue

            if comp >= tier_s_min_composite and (hit_ch >= tier_s_min_hit_chance or overdue >= tier_s_min_overdue):
                best_targets_s.append(c)
            elif comp >= tier_a_min_composite and hit_ch >= tier_a_min_hit_chance:
                best_targets_a.append(c)

        # -------- Response --------
        response: Dict[str, Any] = {"date": effective_date, "candidates": candidates}

        # attach footer info so the GPT can print schedule status (now with statusText)
        response["schedule"] = [
            {"matchup": row[0], "statusCode": row[1], "statusText": row[2], "gamePk": row[3]}
            for row in sched_rows
        ]
        response["pregame_counts"] = {
            "pregame_teams": len(ns_team_ids_today),
            "slate_teams": len(slate_team_ids_today),
        }
        response["best_targets"] = {
            "tier_s": best_targets_s,
            "tier_a": best_targets_a,
        }

        if debug_list is not None:
            stamp = {
                "requested_date": requested_date,
                "effective_date": effective_date,
                "verify": int(verify_effective),
                "rolled_to_next_slate": bool(rolled),
                "pregame_team_count": len(ns_team_ids_today),
                "slate_team_count": len(slate_team_ids_today),
                "cutoffs": {
                    "min_season_avg": min_season_avg,
                    "min_hitless_games": min_hitless_games,
                    "min_season_ab": min_season_ab,
                    "min_season_gp": min_season_gp,
                    "limit": limit,
                    "scan_multiplier": DEFAULT_MULT,
                    "max_log_checks": MAX_LOG_CHECKS,
                },
                "statcast": {
                    "require_statcast_for_tiers": require_statcast_for_tiers,
                    "hh_recent_days": hh_recent_days,
                    "statcast_min_hh_14d": statcast_min_hh_14d,
                    "statcast_min_xba_delta_14d": statcast_min_xba_delta_14d,
                },
                "params": {
                    "mode": mode_norm or None,
                    "as_of": as_of_norm or None,
                    "group_by": group_mode,
                    "sort_by": sort_by or None,
                }
            }
            response["debug"] = [stamp] + (debug_list or [])
        return response
