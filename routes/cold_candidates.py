# routes/cold_candidates.py
from __future__ import annotations

from fastapi import APIRouter, Query
from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime, date as date_cls, timezone, timedelta
import unicodedata
import httpx
import pytz
import math

# Optional Statcast overlay imports (kept safe if unavailable)
try:
    # We do NOT hard-require pybaseball; fail-soft and mark in debug
    from pybaseball.statcast_batter import statcast_batter
    HAVE_PYBASEBALL = True
except Exception:
    HAVE_PYBASEBALL = False

router = APIRouter()
MLB_BASE = "https://statsapi.mlb.com/api/v1"
_EASTERN = pytz.timezone("US/Eastern")

# ---------- time & utils ----------
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

# ---------- schedule helpers ----------
_STATUS_MAP = {
    "P": "Preview",       # Not started – lineup/preview state
    "S": "Scheduled",     # Not started – on schedule
    "PW": "Pre-Game",     # Not started – warmup
    "PR": "Pre-Game",     # Not started – warmup (alt code seen occasionally)
    "I": "In Progress",
    "MI": "Mid Inning",
    "IR": "In Progress",
    "D": "Delayed",
    "DR": "Delayed",
    "DI": "Delayed",
    "IP": "In Progress",
    "F": "Final",
    "FO": "Final",
    "FR": "Final",
    "O": "Other",
}

def _schedule_for_date(client: httpx.Client, date_str: str, dbg: Optional[List[Dict]]) -> Dict:
    return _fetch_json_safe(client, f"{MLB_BASE}/schedule", {"sportId": 1, "date": date_str}, dbg, f"schedule:{date_str}")

def _not_started_team_ids_for_date(schedule_json: Dict) -> Set[int]:
    """
    STRICT pregame set: include only statuses that are clearly NOT started yet.
    Codes considered pregame: P (Preview), S (Scheduled), PW/PR (Pre-Game).
    """
    ns_ids: Set[int] = set()
    for d in schedule_json.get("dates", []) or []:
        for g in d.get("games", []) or []:
            code = (g.get("status", {}) or {}).get("statusCode", "")
            if code in ("P", "S", "PW", "PR"):
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

def _game_pks_for_date(schedule_json: Dict) -> Set[int]:
    """
    All gamePk values for the slate date (used to exclude same-day logs robustly).
    """
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

def _schedule_footer(schedule_json: Dict) -> List[Dict]:
    """
    Lightweight schedule info suitable for printing in GPT’s footer.
    """
    rows: List[Dict] = []
    for d in schedule_json.get("dates", []) or []:
        for g in d.get("games", []) or []:
            status_code = (g.get("status", {}) or {}).get("statusCode", "")
            status_text = _STATUS_MAP.get(status_code, status_code or "Unknown")
            try:
                home = g["teams"]["home"]["team"]["name"]
                away = g["teams"]["away"]["team"]["name"]
            except Exception:
                home = (g.get("teams", {}).get("home", {}).get("team", {}).get("name")) or "Home"
                away = (g.get("teams", {}).get("away", {}).get("team", {}).get("name")) or "Away"
            rows.append({
                "home": home,
                "away": away,
                "gamePk": int(g.get("gamePk") or 0),
                "statusCode": status_code,
                "status": status_text,
            })
    return rows

# ---------- stats & logs ----------
def _choose_best_mlb_season_split(splits: List[Dict]) -> Optional[Dict]:
    """
    From season splits, pick MLB-level (AL=103/NL=104) with most AB; fallback to sportId=1; else most AB.
    """
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
    """
    Return (season_atBats, season_gamesPlayed) from the chosen MLB season split if present.
    """
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
    """
    Simple expected ABs tonight = clamp( season_atBats / gamesPlayed, 2.0 .. 5.5 ), fallback 4.0
    """
    ab, gp = _season_ab_gp_from_people_like(obj)
    if ab is not None and gp and gp > 0:
        try:
            v = float(ab) / float(gp)
            return max(2.0, min(5.5, v))
        except Exception:
            pass
    return 4.0

def _break_prob_from_avg_and_ab(avg: float, expected_abs: float) -> float:
    """
    Probability of >=1 hit = 1 - (1-AVG)^(expected_abs)
    """
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

# ---------- overlay helpers (Statcast, safe) ----------
def _statcast_overlay(pid: int, lookback_days: int, slate_date: str, dbg: Optional[List[Dict]]) -> Dict[str, Optional[float]]:
    """
    Returns dict with 14-day (or configured) aggregates for Hard-Hit% and xBA/BA delta.
    If pybaseball is not available or errors, returns nulls.
    """
    out = {
        "hh_pct_recent": None,
        "xba_recent": None,
        "ba_recent": None,
        "xba_delta": None,
    }
    if not HAVE_PYBASEBALL:
        if dbg is not None:
            dbg.append({"statcast": "pybaseball_not_installed"})
        return out

    try:
        end = _parse_ymd(slate_date)
        start = end - timedelta(days=max(7, int(lookback_days)))
        # pybaseball.statcast_batter expects strings
        df = statcast_batter(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), pid)
        if df is None or len(df) == 0:
            return out

        # Approximate recent BA / xBA using per-PA outcomes available
        # Hard-Hit% from 'launch_speed' >= 95 mph rows
        import pandas as pd
        d = df.copy()

        # BA: hits / at-bats. Statcast play-by-play isn't a box score;
        # proxy: treat events that are base hits as H, and non-walk, non-HBP, non-sac as AB
        hit_mask = d['events'].isin(['single', 'double', 'triple', 'home_run'])
        bb_mask = d['events'].isin(['walk', 'intent_walk'])
        hbp_mask = d['events'].isin(['hit_by_pitch'])
        sac_mask = d['events'].str.contains('sacrifice', case=False, na=False)
        ab_mask = ~(bb_mask | hbp_mask | sac_mask)

        hits = int(hit_mask.sum())
        abs_ = int((ab_mask).sum())
        ba_recent = (hits / abs_) if abs_ > 0 else None

        # xBA: mean of estimated_ba on batted balls
        if 'estimated_ba_using_speedangle' in d.columns:
            xba_recent = float(d['estimated_ba_using_speedangle'].dropna().mean()) if not d['estimated_ba_using_speedangle'].dropna().empty else None
        elif 'estimated_ba' in d.columns:
            xba_recent = float(d['estimated_ba'].dropna().mean()) if not d['estimated_ba'].dropna().empty else None
        else:
            xba_recent = None

        # HH%: share of batted balls with EV >=95
        if 'launch_speed' in d.columns:
            bb = d['launch_speed'].dropna()
            denom = len(bb)
            hh = int((bb >= 95.0).sum()) if denom > 0 else 0
            hh_pct = (hh / denom) * 100.0 if denom > 0 else None
        else:
            hh_pct = None

        out["hh_pct_recent"] = round(hh_pct, 1) if hh_pct is not None else None
        out["xba_recent"] = round(xba_recent, 3) if xba_recent is not None else None
        out["ba_recent"] = round(ba_recent, 3) if ba_recent is not None else None
        if out["xba_recent"] is not None and out["ba_recent"] is not None:
            out["xba_delta"] = round(out["xba_recent"] - out["ba_recent"], 3)
        else:
            out["xba_delta"] = None
        return out
    except Exception as e:
        if dbg is not None:
            dbg.append({"statcast_error": f"{type(e).__name__}: {e}"})
        return out

# ---------- team & roster ----------
def _extract_team_name_from_person_or_logs(
    person_like: Dict,
    team_map: Optional[Dict[int, Tuple[int, str]]] = None,
    pid: Optional[int] = None,
    logs: Optional[List[Dict]] = None,
    slate_date_ymd: Optional[str] = None,
) -> str:
    """
    Robust team-name fallback order:
    1) person.currentTeam.name
    2) person.team.name
    3) latest pre-slate log's team.name
    4) team_map[pid].name
    5) "N/A"
    """
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

def _team_roster_ids_multi(client: httpx.Client, team_id: int, season: int, dbg: Optional[List[Dict]]) -> List[int]:
    attempts = [
        ("Active", {"rosterType": "Active"}),
        ("active", {"rosterType": "active"}),
        ("40Man", {"rosterType": "40Man"}),
        ("fullSeason", {"rosterType": "fullSeason", "season": season}),
        ("season", {"season": season}),
    ]
    ids: List[int] = []
    for label, params in attempts:
        try:
            data = _fetch_json(client, f"{MLB_BASE}/teams/{team_id}/roster", params=params)
            roster = data.get("roster", []) or []
            got = 0
            for r in roster:
                person = r.get("person") or {}
                pid = person.get("id")
                try:
                    if pid is not None:
                        ids.append(int(pid)); got += 1
                except Exception:
                    continue
            if dbg is not None:
                dbg.append({"team_id": team_id, "roster_source": label, "count": got})
            if got > 0:
                break
        except Exception as e:
            if dbg is not None:
                dbg.append({"team_id": team_id, "roster_source": label, "error": f"{type(e).__name__}: {e}"})
            continue
    return ids

def _hydrate_team_roster_people(client: httpx.Client, team_ids: List[int], season: int, dbg: Optional[List[Dict]]) -> Tuple[Set[int], Dict[int, Tuple[int, str]]]:
    """
    Returns (person_ids, person_team_map[pid] = (team_id, team_name)) using /teams hydrate.
    """
    person_ids: Set[int] = set()
    team_map: Dict[int, Tuple[int, str]] = {}
    for i in range(0, len(team_ids), 8):
        sub = team_ids[i:i+8]
        params = {
            "teamIds": ",".join(str(t) for t in sub),
            "sportId": 1,
            "season": season,
            "hydrate": f"roster(person,person.stats(group=hitting,type=season,season={season}))"
        }
        try:
            data = _fetch_json(client, f"{MLB_BASE}/teams", params=params)
            added = 0
            for t in data.get("teams", []) or []:
                tid = t.get("id")
                tname = t.get("name", "")
                roster_container = t.get("roster")
                entries = (roster_container.get("roster", []) if isinstance(roster_container, dict) else roster_container) or []
                for entry in entries:
                    person = entry.get("person") or {}
                    pid = person.get("id")
                    if pid is None:
                        continue
                    try:
                        pid = int(pid)
                    except Exception:
                        continue
                    person_ids.add(pid)
                    team_map[pid] = (int(tid) if tid is not None else None, tname)
                    added += 1
            if dbg is not None:
                dbg.append({"teams_hydrate_chunk": sub, "persons_added": added})
        except Exception as e:
            if dbg is not None:
                dbg.append({"teams_hydrate_chunk": sub, "error": f"{type(e).__name__}: {e}"})
    return person_ids, team_map

def _batch_people_with_stats(client: httpx.Client, ids: List[int], season: int, dbg: Optional[List[Dict]]) -> List[Dict]:
    out: List[Dict] = []
    for i in range(0, len(ids), 100):
        sub = ids[i:i+100]
        try:
            params = {
                "personIds": ",".join(str(x) for x in sub),
                "hydrate": f"team,stats(group=hitting,type=season,season={season})"
            }
            data = _fetch_json(client, f"{MLB_BASE}/people", params=params)
            ppl = data.get("people", []) or []
            out.extend(ppl)
            if dbg is not None:
                dbg.append({"people_batch_chunk": len(sub), "returned": len(ppl)})
        except Exception as e:
            if dbg is not None:
                dbg.append({"people_batch_chunk": len(sub), "error": f"{type(e).__name__}: {e}"})
    return out

# ---------- sorting helpers ----------
_VALID_SORT_KEYS = {"hitless_streak", "season_avg", "avg_hitless_streak_season", "break_prob_next", "pressure", "score", "hit_chance_pct", "overdue_ratio", "ranking_score", "score_plus", "bookmaker", "composite"}

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

# ---------- bookmaker composite & tiering ----------
def _compute_score_plus(base_score: float, hit_chance_pct: float, overdue_ratio: float) -> float:
    """
    Keep score_plus on the same 0..300-ish scale as your historical prints.
    If caller already supplies 'score' as break_prob% * overdue, we can start from that.
    """
    # Trust provided 'score' as main body; tiny stabilization on HC and overdue
    bonus = 0.25 * (hit_chance_pct - 60.0) + 5.0 * max(0.0, overdue_ratio - 1.0)
    return max(0.0, base_score + bonus)

def _compute_composite(score_plus: float, bookmaker_0_1: float) -> float:
    # 70/30 blend, scale bookmaker to 0..100
    return round(0.7 * score_plus + 0.3 * (bookmaker_0_1 * 100.0), 1)

def _assign_tier(hit_chance_pct: float, overdue_ratio: float, score_plus: float, composite: float) -> str:
    """
    Tier S should be better than Tier A. Make S stricter.
    """
    if (hit_chance_pct >= 70.0 and overdue_ratio >= 1.50 and composite >= 50.0 and score_plus >= 150.0):
        return "S"
    if (hit_chance_pct >= 65.0 and overdue_ratio >= 1.20 and composite >= 42.0 and score_plus >= 120.0):
        return "A"
    return "B"

# ---------- route ----------
@router.get("/cold_candidates")
def cold_candidates(
    date: str = Query("today", description="YYYY-MM-DD or 'today' (US/Eastern)"),
    season: int = Query(2025, ge=1900, le=2100),
    names: Optional[str] = Query(None, description="Optional comma-separated player names. If omitted, scans slate rosters."),
    min_season_avg: float = Query(0.26, ge=0.0, le=1.0, description="Only include hitters with season AVG ≥ this (default .260)."),
    min_hitless_games: int = Query(1, ge=1, description="Current hitless streak (AB>0) must be ≥ this."),
    limit: int = Query(30, ge=1, le=1000),
    verify: int = Query(1, ge=0, le=1, description="1 = STRICT pregame only for the slate date (teams not started yet). 0 = include all teams."),
    roll_to_next_slate_if_empty: int = Query(1, ge=0, le=1, description="If verify=1 and there are ZERO pregame teams today, roll to NEXT day (strict pregame)."),
    last_n: Optional[int] = Query(None, description="Ignored. Backward compatibility only."),
    scan_multiplier: int = Query(8, ge=1, le=40, description="How many logs to check: limit × scan_multiplier (cap applies)"),
    max_log_checks: Optional[int] = Query(None, ge=1, le=5000, description="Hard cap for log checks; overrides scan_multiplier."),
    debug: int = Query(0, ge=0, le=1),

    # Additive params
    mode: Optional[str] = Query(None, description="Alias for verify. 'pregame' -> verify=1, 'all' -> verify=0. If set, overrides verify."),
    as_of: Optional[str] = Query(None, description="YYYY-MM-DD snapshot for streak math. If not today ET, verification is disabled and no roll-forward."),
    group_by: str = Query("streak", description="Grouping preset: 'streak' (default) or 'none'. If 'streak', we bucket by hitless_streak and sort inside buckets by score."),
    sort_by: Optional[str] = Query(None, description="When group_by='none', comma-separated fields with optional '-' for DESC."),

    # New floors & Statcast lookback
    min_season_ab: int = Query(100, ge=1, le=1000, description="Season AB floor (filters out tiny samples)."),
    min_season_gp: int = Query(40, ge=1, le=200, description="Season GP floor (filters out tiny samples)."),
    hh_recent_days: int = Query(14, ge=7, le=30, description="Statcast lookback window in days for HH% and xBA delta."),

    # bookmaker passthrough placeholder (0..1). If not supplied, defaults to 0.
    bookmaker_hint: Optional[float] = Query(None, ge=0.0, le=1.0, description="Optional bookmaker confidence 0..1 (if your client has an odds feed)."),
):
    """
    VERIFIED cold-hitter candidates with optional Statcast overlays and a schedule footer:
      • Good hitters (season AVG ≥ min_season_avg; also AB/GP floors)
      • Current hitless streak (AB>0 only; DNP/0-AB ignored)
      • STRICT pregame when verify=1 (exclude in-progress/finished)
      • Exclude same-day games from streak calc (previous games only)
      • avg_hitless_streak_season = average length of COMPLETED hitless streaks before the slate date
      • Derived: expected_abs (season AB/G), break_prob_next (0..100%), pressure, score=break_prob_next×pressure
      • Overlay: 14d HH% and xBA–BA delta if available
      • Composite/Tier: score_plus & bookmaker -> composite, Tier S/A/B
      • Schedule footer with statusCode→label mapping
    """
    requested_date = _eastern_today_str() if _normalize(date) == "today" else date
    effective_date = requested_date

    # --- mode alias mapping (mode takes precedence over verify if provided)
    mode_norm = (mode or "").strip().lower() if mode else None
    if mode_norm in ("pregame", "pre", "strict"):
        verify_effective = 1
    elif mode_norm in ("all", "any"):
        verify_effective = 0
    else:
        verify_effective = 1 if int(verify) == 1 else 0

    # --- as_of snapshot handling
    as_of_norm = (as_of or "").strip()
    historical_mode = False
    if as_of_norm:
        try:
            _ = _parse_ymd(as_of_norm)  # validate
            effective_date = as_of_norm
            if not _is_today_et(as_of_norm):
                historical_mode = True
        except Exception:
            pass

    if historical_mode:
        verify_effective = 0  # disable pregame check
        roll_enabled = False
    else:
        roll_enabled = bool(roll_to_next_slate_if_empty)

    # --- scan budget
    DEFAULT_MULT = max(1, int(scan_multiplier))
    computed_max = min(3000, limit * DEFAULT_MULT)
    MAX_LOG_CHECKS = max_log_checks if max_log_checks is not None else computed_max

    # Adaptive load shedding for verify=0 (ALL teams) when user didn't set caps
    if verify_effective == 0:
        if max_log_checks is None:
            MAX_LOG_CHECKS = min(MAX_LOG_CHECKS, 400)
        if "limit" in cold_candidates.__signature__.parameters:
            if limit > 30:
                limit = 30  # gentle cap to avoid timeouts

    # --- sort/group presets
    group_mode = (group_by or "").strip().lower()
    sort_spec = _parse_sort_by(sort_by) if group_mode == "none" else []

    with httpx.Client(timeout=45) as client:
        # schedule & helpers for this date (fault tolerant)
        debug_list: Optional[List[Dict]] = [] if debug else None
        sched = _schedule_for_date(client, effective_date, debug_list)
        ns_team_ids_today = _not_started_team_ids_for_date(sched) if (verify_effective == 1) else set()
        slate_team_ids_today = _team_ids_from_schedule(sched) if sched else _all_mlb_team_ids(client, season, debug_list)
        exclude_pks_for_date = _game_pks_for_date(sched) if sched else set()
        schedule_rows = _schedule_footer(sched) if sched else []

        rolled = False
        if (verify_effective == 1) and roll_enabled and len(ns_team_ids_today) == 0:
            effective_date = _next_ymd_str(effective_date)
            sched = _schedule_for_date(client, effective_date, debug_list)
            ns_team_ids_today = _not_started_team_ids_for_date(sched)
            slate_team_ids_today = _team_ids_from_schedule(sched) or slate_team_ids_today
            exclude_pks_for_date = _game_pks_for_date(sched)
            schedule_rows = _schedule_footer(sched)
            rolled = True

        # ---------- inner helpers ----------
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
                    continue  # same-day/future excluded
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

        def _decorate_candidate(base: Dict, logs: Optional[List[Dict]], as_of_date: str, pid: Optional[int]) -> Dict:
            """
            Add expected_abs, break_prob_next, pressure, score, aliases,
            statcast overlay (if available), and bookmaker/score_plus/composite/tier/summary.
            """
            person_like = base.get("_person_like") or {}
            season_avg = float(base.get("season_avg", 0.0))
            ab, gp = _season_ab_gp_from_people_like(person_like)

            # AB/GP floors
            base["_passes_floors"] = bool(
                (ab is None or ab >= min_season_ab) and
                (gp is None or gp >= min_season_gp)
            )

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

            # canonical fields
            base["expected_abs"] = round(expected_abs, 2)
            base["break_prob_next"] = round(break_prob * 100.0, 1)  # percent display
            base["pressure"] = round(pressure, 3)
            base["score"] = round(score * 100.0, 1)

            # alias fields (preferred names for UI/GPT)
            base["hit_chance_pct"] = base["break_prob_next"]
            base["overdue_ratio"] = base["pressure"]
            base["ranking_score"] = base["score"]

            # Statcast overlay (safe)
            overlay = {"hh_pct_recent": None, "xba_recent": None, "ba_recent": None, "xba_delta": None}
            if pid is not None and base["_passes_floors"]:
                overlay = _statcast_overlay(pid, hh_recent_days, as_of_date, debug_list)
            base.update(overlay)

            # bookmaker / score_plus / composite / tier
            bookmaker_val = float(bookmaker_hint) if (bookmaker_hint is not None) else 0.0
            base["bookmaker"] = round(max(0.0, min(1.0, bookmaker_val)), 3)
            base["score_plus"] = round(_compute_score_plus(base["score"], base["hit_chance_pct"], base["overdue_ratio"]), 1)
            base["composite"] = _compute_composite(base["score_plus"], base["bookmaker"])
            base["tier"] = _assign_tier(base["hit_chance_pct"], base["overdue_ratio"], base["score_plus"], base["composite"])

            # English summary
            parts = [
                f"{base['season_avg']:.3f} AVG",
                f"{current}-game streak vs season avg hitless {base.get('avg_hitless_streak_season', 0.0):.2f}",
                f"Hit chance {base['hit_chance_pct']:.1f}%",
            ]
            if base.get("hh_pct_recent") is not None or base.get("xba_delta") is not None:
                if base.get("hh_pct_recent") is not None:
                    parts.append(f"HH% ({hh_recent_days}d) {base['hh_pct_recent']:.1f}")
                if base.get("xba_delta") is not None:
                    sign = "+" if base['xba_delta'] >= 0 else ""
                    parts.append(f"xBA–BA ({hh_recent_days}d) {sign}{base['xba_delta']:.3f}")
            else:
                parts.append("no recent Statcast signal")
            base["summary"] = "; ".join(parts)

            base.pop("_person_like", None)
            return base

        def run_once_for_date(target_date: str, ns_ids: Set[int], team_ids: List[int]) -> Dict:
            candidates: List[Dict] = []

            # explicit names mode
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
                        season_avg = _season_avg_from_people_like(person)

                        logs = _game_log_regular_season_desc(client, pid, season, max_entries=160, dbg=debug_list)
                        streak = _current_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)
                        if season_avg is None or season_avg < min_season_avg or streak < min_hitless_games:
                            if debug_list is not None:
                                why = []
                                if season_avg is None: why.append("no season stats")
                                elif season_avg < min_season_avg: why.append(f"season_avg {season_avg:.3f} < {min_season_avg:.3f}")
                                if streak < min_hitless_games: why.append(f"hitless_streak {streak} < {min_hitless_games}")
                                debug_list.append({"name": person.get("fullName") or name, "skip": ", ".join(why) or "filtered"})
                            continue

                        # floors gate (AB/GP)
                        ab, gp = _season_ab_gp_from_people_like(person)
                        if (ab is not None and ab < min_season_ab) or (gp is not None and gp < min_season_gp):
                            if debug_list is not None:
                                debug_list.append({"name": person.get("fullName") or name, "skip": f"floors AB {ab} / GP {gp} under {min_season_ab}/{min_season_gp}"})
                            continue

                        team_name = _extract_team_name_from_person_or_logs(person, None, pid, logs, target_date)
                        avg_season_hitless = _average_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)

                        cand = {
                            "pid": pid,
                            "name": person.get("fullName") or name,
                            "team": team_name,
                            "season_avg": round(float(season_avg), 3),
                            "hitless_streak": streak,
                            "avg_hitless_streak_season": round(avg_season_hitless, 2) if avg_season_hitless is not None else 0.0,
                            "_person_like": person,
                        }
                        cand = _decorate_candidate(cand, logs, target_date, pid)
                        candidates.append(cand)
                        if len(candidates) >= limit:
                            break
                    except Exception as e:
                        if debug_list is not None:
                            debug_list.append({"name": name, "error": f"{type(e).__name__}: {e}"})

                # Presentation (same as league flow below)
                if group_mode == "streak":
                    buckets: Dict[int, List[Dict]] = {}
                    for c in candidates:
                        buckets.setdefault(int(c.get("hitless_streak", 0)), []).append(c)
                    out_list: List[Dict] = []
                    for k in sorted(buckets.keys(), reverse=True):
                        grp = sorted(buckets[k], key=lambda x: float(x.get("composite", x.get("score_plus", x.get("ranking_score", 0.0)))), reverse=True)
                        out_list.extend(grp)
                    candidates = out_list[:limit]
                else:
                    if sort_spec:
                        candidates = _apply_sort(candidates, sort_spec)
                    else:
                        candidates = sorted(candidates, key=lambda x: (float(x.get("composite", x.get("score_plus", x.get("ranking_score", 0.0)))), float(x.get("season_avg", 0.0))), reverse=True)
                    candidates = candidates[:limit]
                return {"candidates": candidates}

            # league scan mode
            scan_team_ids = sorted(ns_ids) if verify_effective == 1 else team_ids

            # (A) Union of player IDs across roster endpoints + team hydrate
            union_ids, team_map = _collect_union_player_ids(client, scan_team_ids, season, debug_list)

            # (B) Batch-fetch people with season stats for the union (authoritative)
            people = _batch_people_with_stats(client, union_ids, season, debug_list)

            # (C) Normalize team info using team_map if missing
            for p in people:
                if not p.get("currentTeam"):
                    pid = p.get("id")
                    if isinstance(pid, int) and pid in team_map:
                        tid, tname = team_map[pid]
                        p["currentTeam"] = {"id": tid, "name": tname}

            # (D) Filter to good hitters and (if verify) to pregame teams; floors enforced
            prospects: List[Tuple[float, Dict]] = []
            for p in people:
                season_avg = _season_avg_from_people_like(p)
                if season_avg is None or season_avg < min_season_avg:
                    continue
                try:
                    pid = int(p.get("id"))
                except Exception:
                    pid = None
                if pid is None:
                    continue

                # AB/GP floors
                ab, gp = _season_ab_gp_from_people_like(p)
                if (ab is not None and ab < min_season_ab) or (gp is not None and gp < min_season_gp):
                    continue

                if verify_effective == 1:
                    team_info = p.get("currentTeam") or {}
                    try:
                        team_id = int(team_info.get("id")) if team_info.get("id") is not None else None
                    except Exception:
                        team_id = None
                    if team_id is None or team_id not in ns_ids:
                        continue

                prospects.append((
                    float(season_avg),
                    {
                        "pid": pid,
                        "name": p.get("fullName") or "",
                        "person": p,
                        "season_avg": round(float(season_avg), 3),
                    }
                ))

            prospects.sort(key=lambda x: x[0], reverse=True)

            # (E) Logs+streaks for prospects (capped), and include derived metrics
            checks = 0
            for _, meta in prospects:
                if checks >= MAX_LOG_CHECKS or len(candidates) >= limit:
                    break
                checks += 1
                try:
                    logs = _game_log_regular_season_desc(client, meta["pid"], season, max_entries=160, dbg=debug_list)
                    streak = _current_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)
                    if streak >= min_hitless_games:
                        avg_season_hitless = _average_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)
                        team_name = _extract_team_name_from_person_or_logs(
                            meta["person"], team_map, meta["pid"], logs, target_date
                        )
                        cand = {
                            "pid": meta["pid"],
                            "name": meta["name"],
                            "team": team_name,
                            "season_avg": meta["season_avg"],
                            "hitless_streak": streak,
                            "avg_hitless_streak_season": round(avg_season_hitless, 2) if avg_season_hitless is not None else 0.0,
                            "_person_like": meta["person"],
                        }
                        cand = _decorate_candidate(cand, logs, target_date, meta["pid"])
                        candidates.append(cand)
                except Exception as e:
                    if debug_list is not None:
                        dbg_name = meta.get("name", "")
                        debug_list.append({"name": dbg_name, "error": f"{type(e).__name__}: {e}"})

            # Presentation
            if group_mode == "streak":
                buckets: Dict[int, List[Dict]] = {}
                for c in candidates:
                    buckets.setdefault(int(c.get("hitless_streak", 0)), []).append(c)
                out_list: List[Dict] = []
                for k in sorted(buckets.keys(), reverse=True):
                    grp = sorted(buckets[k], key=lambda x: float(x.get("composite", x.get("score_plus", x.get("ranking_score", 0.0)))), reverse=True)
                    out_list.extend(grp)
                candidates = out_list[:limit]
            else:
                if sort_spec:
                    candidates = _apply_sort(candidates, sort_spec)
                else:
                    candidates = sorted(candidates, key=lambda x: (float(x.get("composite", x.get("score_plus", x.get("ranking_score", 0.0)))), float(x.get("season_avg", 0.0))), reverse=True)
                candidates = candidates[:limit]

            if debug_list is not None:
                debug_list.insert(0, {
                    "prospects_scanned": len(prospects),
                    "log_checks": checks,
                    "max_log_checks": MAX_LOG_CHECKS
                })
            return {"candidates": candidates}

        result = run_once_for_date(effective_date, ns_team_ids_today, slate_team_ids_today)
        items = result["candidates"]

        response: Dict = {
            "date": effective_date,
            "candidates": items,
            "schedule": schedule_rows,  # schedule footer for GPT display
            "counts": {
                "pregame_teams": len(ns_team_ids_today),
                "slate_teams": len(slate_team_ids_today)
            }
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
                    "min_season_ab": min_season_ab,
                    "min_season_gp": min_season_gp,
                    "min_hitless_games": min_hitless_games,
                    "limit": limit,
                    "scan_multiplier": DEFAULT_MULT,
                    "max_log_checks": MAX_LOG_CHECKS,
                    "hh_recent_days": hh_recent_days,
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
