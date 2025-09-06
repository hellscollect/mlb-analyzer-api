# routes/cold_candidates.py
from __future__ import annotations

from fastapi import APIRouter, Query
from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime, date as date_cls, timezone, timedelta
import unicodedata
import httpx
import pytz
import math

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
def _schedule_for_date(client: httpx.Client, date_str: str, dbg: Optional[List[Dict]]) -> Dict:
    return _fetch_json_safe(client, f"{MLB_BASE}/schedule", {"sportId": 1, "date": date_str}, dbg, f"schedule:{date_str}")

def _not_started_team_ids_for_date(schedule_json: Dict) -> Set[int]:
    """
    STRICT pregame set: P = Preview, S = Scheduled (not started yet).
    """
    ns_ids: Set[int] = set()
    for d in schedule_json.get("dates", []) or []:
        for g in d.get("games", []) or []:
            code = (g.get("status", {}) or {}).get("statusCode", "")
            if code in ("P", "S"):
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
    out: List[Dict] = []
    for d in schedule_json.get("dates", []) or []:
        for g in d.get("games", []) or []:
            status = (g.get("status", {}) or {})
            code = status.get("statusCode", "")
            detailed = status.get("detailedState") or status.get("abstractGameState") or status.get("codedGameState") or ""
            home = (((g.get("teams") or {}).get("home") or {}).get("team") or {}).get("name", "")
            away = (((g.get("teams") or {}).get("away") or {}).get("team") or {}).get("name", "")
            out.append({
                "matchup": f"{away} @ {home}" if home and away else "",
                "status": detailed,
                "code": code,
                "gamePk": g.get("gamePk")
            })
    return out

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

def _current_hitless_streak_before_slate(
    game_splits: List[Dict],
    slate_date_ymd: str,
    exclude_game_pks: Optional[Set[int]] = None
) -> int:
    """
    Consecutive MOST-RECENT games with AB>0 and H==0 BEFORE the slate date (ET).
    Ignores 0-AB games. Excludes same-day games. Additionally skips any game
    whose gamePk is on the slate's schedule (to avoid in-progress inclusion).
    """
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
    """
    Average length of COMPLETED hitless streaks (AB>0 only) over the season
    BEFORE the slate date (ET). Excludes same-day games and the current
    ongoing run; also skips any game whose gamePk is on the slate schedule.
    """
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

# ---------- roster & people collection ----------
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

def _collect_union_player_ids(
    client: httpx.Client,
    team_ids: List[int],
    season: int,
    dbg: Optional[List[Dict]]
) -> Tuple[List[int], Dict[int, Tuple[int, str]]]:
    """
    1) Try per-team roster endpoints.
    2) Augment with /teams hydrate.
    Return de-duplicated IDs (sorted) and a map pid -> (team_id, team_name).
    """
    ids: Set[int] = set()
    team_map: Dict[int, Tuple[int, str]] = {}

    for tid in team_ids:
        got = _team_roster_ids_multi(client, tid, season, dbg)
        for pid in got:
            ids.add(pid)
            team_map.setdefault(pid, (tid, ""))

    hydrate_ids, hydrate_map = _hydrate_team_roster_people(client, team_ids, season, dbg)
    for pid in hydrate_ids:
        ids.add(pid)
        if pid in hydrate_map:
            team_map[pid] = hydrate_map[pid]

    out_ids = sorted(ids)
    if dbg is not None:
        dbg.append({"union_player_ids": len(out_ids)})
    return out_ids, team_map

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
_VALID_SORT_KEYS = {
    "hitless_streak", "season_avg", "avg_hitless_streak_season", "break_prob_next",
    "pressure", "score", "hit_chance_pct", "overdue_ratio", "ranking_score",
    "score_plus", "composite"
}

def _parse_sort_by(sort_by: Optional[str]) -> List[Tuple[str, bool]]:
    """
    Returns list of (field, desc). Supports comma-separated fields with optional '-' prefix for DESC.
    Unknown fields are ignored.
    """
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

# ---------- model helpers (bookmaker-style composite & tiers) ----------
def _norm_overdue_to_100(overdue_ratio: float) -> float:
    # Cap at 4.0 to avoid runaway influence; map 0..4 -> 0..100
    x = max(0.0, min(4.0, float(overdue_ratio)))
    return (x / 4.0) * 100.0

def _elite_avg_component(season_avg: float) -> float:
    # Map .260 -> 0, .350+ -> 100 (cap); linear in between
    lo, hi = 0.260, 0.350
    v = (float(season_avg) - lo) / max(1e-9, (hi - lo))
    v = max(0.0, min(1.0, v))
    return v * 100.0

def _statcast_component(hh_percent_14d: Optional[float], xba_delta_14d: Optional[float]) -> float:
    # If absent, return 0; otherwise average normalized HH% and xBA delta (delta .00 -> 0, .10+ -> 100)
    if hh_percent_14d is None and xba_delta_14d is None:
        return 0.0
    hh_norm = 0.0 if hh_percent_14d is None else max(0.0, min(100.0, float(hh_percent_14d)))
    if xba_delta_14d is None:
        xba_norm = 0.0
    else:
        # Typical useful range 0..0.10; cap
        xba_norm = max(0.0, min(100.0, (float(xba_delta_14d) / 0.10) * 100.0))
    return (hh_norm + xba_norm) / 2.0

def _composite_score(hit_chance_pct: float, overdue_ratio: float, season_avg: float,
                     hh_percent_14d: Optional[float], xba_delta_14d: Optional[float]) -> float:
    """
    Composite = 50% Hit chance + 17.5% Overdue(→0..100) + 12.5% Elite AVG + 20% Statcast
    """
    hit_component = max(0.0, min(100.0, float(hit_chance_pct)))
    overdue_component = _norm_overdue_to_100(overdue_ratio)
    elite_component = _elite_avg_component(season_avg)
    statcast_component = _statcast_component(hh_percent_14d, xba_delta_14d)
    return (
        0.50 * hit_component +
        0.175 * overdue_component +
        0.125 * elite_component +
        0.20 * statcast_component
    )

def _tier_for(composite: float, hit_chance_pct: float, overdue_ratio: float) -> str:
    # Tier S: Composite ≥70 AND (Hit chance ≥67 OR Overdue ≥2.0)
    # Tier A: Composite ≥55 AND Hit chance ≥62
    if composite >= 70.0 and (hit_chance_pct >= 67.0 or overdue_ratio >= 2.0):
        return "S"
    if composite >= 55.0 and hit_chance_pct >= 62.0:
        return "A"
    return ""

def _summary_line(name: str, team: str, season_avg: float, streak: int, avg_hitless: float,
                  hit_chance_pct: float, hh_percent_14d: Optional[float], xba_delta_14d: Optional[float]) -> str:
    # Example: ".288 AVG; 5-game streak (season avg hitless 1.57); 65.8% hit chance; HH% (14d) 45.0, xBA–BA (14d) +.040"
    parts = [
        f"{season_avg:.3f} AVG; {streak}-game streak (season avg hitless {avg_hitless:.2f}); {hit_chance_pct:.1f}% hit chance"
    ]
    if (hh_percent_14d is not None) or (xba_delta_14d is not None):
        sc_parts = []
        if hh_percent_14d is not None:
            sc_parts.append(f"HH% (14d) {hh_percent_14d:.1f}")
        if xba_delta_14d is not None:
            sc_parts.append(f"xBA–BA (14d) {xba_delta_14d:+.3f}")
        if sc_parts:
            parts.append("; " + ", ".join(sc_parts))
    else:
        parts.append("; no recent Statcast signal")
    return "".join(parts)

# ---------- route ----------
@router.get("/cold_candidates")
def cold_candidates(
    date: str = Query("today", description="YYYY-MM-DD or 'today' (US/Eastern)"),
    season: int = Query(2025, ge=1900, le=2100),
    names: Optional[str] = Query(None, description="Optional comma-separated player names. If omitted, scans slate rosters."),
    min_season_avg: float = Query(0.26, ge=0.0, le=1.0, description="Only include hitters with season AVG ≥ this (default .260)."),
    min_season_ab: int = Query(100, ge=0, description="Only include hitters with season AB ≥ this (stability filter)."),
    min_season_gp: int = Query(40, ge=0, description="Only include hitters with season GP ≥ this (stability filter)."),
    min_hitless_games: int = Query(1, ge=1, description="Current hitless streak (AB>0) must be ≥ this."),
    limit: int = Query(30, ge=1, le=1000),
    verify: int = Query(1, ge=0, le=1, description="1 = STRICT pregame only for the slate date (teams not started yet). 0 = include all teams."),
    roll_to_next_slate_if_empty: int = Query(1, ge=0, le=1, description="If verify=1 and there are ZERO pregame teams today, roll to NEXT day (strict pregame)."),
    last_n: Optional[int] = Query(None, description="Ignored. Backward compatibility only."),
    scan_multiplier: int = Query(8, ge=1, le=40, description="How many logs to check: limit × scan_multiplier (cap applies)"),
    max_log_checks: Optional[int] = Query(None, ge=1, le=5000, description="Hard cap for log checks; overrides scan_multiplier."),
    hh_recent_days: int = Query(14, ge=0, le=30, description="Lookback window for Statcast overlays; 0 disables."),
    include_schedule_footer: int = Query(1, ge=0, le=1, description="1 = append schedule status footer."),
    debug: int = Query(0, ge=0, le=1),
    # --- Additive params ---
    mode: Optional[str] = Query(None, description="Alias for verify. 'pregame' -> verify=1, 'all' -> verify=0. If set, overrides verify."),
    as_of: Optional[str] = Query(None, description="YYYY-MM-DD snapshot for streak math. If not today ET, verification is disabled and no roll-forward."),
    group_by: str = Query("streak", description="Grouping preset: 'streak' (default) or 'none'. If 'streak', we bucket by hitless_streak and sort inside buckets by score."),
    sort_by: Optional[str] = Query(None, description="When group_by='none', comma-separated fields with optional '-' for DESC. Fields: hitless_streak,season_avg,avg_hitless_streak_season,break_prob_next,pressure,score,hit_chance_pct,overdue_ratio,ranking_score,score_plus,composite."),
):
    """
    VERIFIED cold-hitter candidates:
      • Good hitters (season AVG ≥ min_season_avg; AB≥min_season_ab; GP≥min_season_gp)
      • Current hitless streak (AB>0 only; DNP/0-AB ignored)
      • STRICT pregame when verify=1 (exclude in-progress/finished)
      • Exclude same-day games from streak calc (use previous games only)
      • avg_hitless_streak_season = average length of COMPLETED hitless streaks before the slate date
      • Derived: expected_abs (season AB/G), break_prob_next (0..100%), pressure, score=break_prob_next×pressure
      • Composite (bookmaker-style): 50% Hit chance + 17.5% Overdue + 12.5% Elite AVG + 20% Statcast(14d HH% & xBA–BA)
      • Tiers: S if (Composite≥70 and (HitChance≥67 or Overdue≥2.0)); A if (Composite≥55 and HitChance≥62)
      • Optional schedule footer lists matchup and status/detailedState
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

        rolled = False
        if (verify_effective == 1) and roll_enabled and len(ns_team_ids_today) == 0:
            effective_date = _next_ymd_str(effective_date)
            sched = _schedule_for_date(client, effective_date, debug_list)
            ns_team_ids_today = _not_started_team_ids_for_date(sched)
            slate_team_ids_today = _team_ids_from_schedule(sched) or slate_team_ids_today
            exclude_pks_for_date = _game_pks_for_date(sched)
            rolled = True

        # --- Statcast enrichment placeholders ---
        # NOTE: We leave these as placeholders because the live overlays may not be present on the provider feed.
        # If absent, we return None so summary prints "no recent Statcast signal".
        def _statcast_overlay_for(pid: int, days: int) -> Tuple[Optional[float], Optional[float]]:
            # hh_percent_14d in [0,100], xba_delta_14d as decimal (e.g., +0.040)
            # If not available, return (None, None).
            # Hook here for future Baseball Savant integration.
            _ = pid, days
            return (None, None)

        def _decorate_candidate(base: Dict, logs: Optional[List[Dict]], as_of_date: str) -> Dict:
            """
            Add expected_abs, break_prob_next, pressure, score, composite, tier, and aliases to a candidate dict.
            """
            person_like = base.get("_person_like") or {}
            pid = base.get("_pid")
            season_avg = float(base.get("season_avg", 0.0))

            expected_abs = _expected_abs_from_person(person_like)
            break_prob = _break_prob_from_avg_and_ab(season_avg, expected_abs)  # 0..1
            hit_chance_pct = round(break_prob * 100.0, 1)

            current = int(base.get("hitless_streak", 0))
            avg_streak = base.get("avg_hitless_streak_season", None)
            try:
                avg_streak_f = float(avg_streak) if avg_streak is not None else 1.0
            except Exception:
                avg_streak_f = 1.0
            denom = max(0.5, avg_streak_f)
            overdue = (float(current) / denom) if denom > 0 else float(current)

            score = break_prob * overdue * 100.0  # keep your display convention (percent×ratio)

            # Statcast overlays (if available)
            hh_percent_14d, xba_delta_14d = (None, None)
            if pid is not None and hh_recent_days > 0:
                try:
                    hh_percent_14d, xba_delta_14d = _statcast_overlay_for(int(pid), int(hh_recent_days))
                except Exception:
                    hh_percent_14d, xba_delta_14d = (None, None)

            composite = _composite_score(hit_chance_pct, overdue, season_avg, hh_percent_14d, xba_delta_14d)
            tier = _tier_for(composite, hit_chance_pct, overdue)

            # canonical fields
            base["expected_abs"] = round(expected_abs, 2)
            base["break_prob_next"] = hit_chance_pct  # percent display
            base["pressure"] = round(overdue, 3)
            base["score"] = round(score, 1)

            # aliases
            base["hit_chance_pct"] = base["break_prob_next"]
            base["overdue_ratio"] = base["pressure"]
            base["ranking_score"] = base["score"]

            # bookmaker/score_plus placeholders (until market overlays wired)
            base["bookmaker"] = 0.0
            base["score_plus"] = base["ranking_score"]

            # statcast fields
            base["hh_percent_14d"] = None if hh_percent_14d is None else round(float(hh_percent_14d), 1)
            base["xba_delta_14d"] = None if xba_delta_14d is None else round(float(xba_delta_14d), 3)

            # composite/tier
            base["composite"] = round(composite, 1)
            base["tier"] = tier

            # summary
            avg_hitless = float(base.get("avg_hitless_streak_season", 0.0))
            base["summary"] = _summary_line(
                base.get("name",""), base.get("team",""), float(season_avg), int(current),
                avg_hitless, hit_chance_pct, base["hh_percent_14d"], base["xba_delta_14d"]
            )

            # cleanup
            base.pop("_person_like", None)
            base.pop("_pid", None)
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
                        ab, gp = _season_ab_gp_from_people_like(person)

                        logs = _game_log_regular_season_desc(client, pid, season, max_entries=160, dbg=debug_list)
                        streak = _current_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)

                        # filters
                        if (season_avg is None) or (season_avg < min_season_avg) or (streak < min_hitless_games):
                            if debug_list is not None:
                                why = []
                                if season_avg is None: why.append("no season stats")
                                elif season_avg < min_season_avg: why.append(f"season_avg {season_avg:.3f} < {min_season_avg:.3f}")
                                if streak < min_hitless_games: why.append(f"hitless_streak {streak} < {min_hitless_games}")
                                debug_list.append({"name": person.get("fullName") or name, "skip": ", ".join(why) or "filtered"})
                            continue
                        if (ab is None or ab < min_season_ab) or (gp is None or gp < min_season_gp):
                            if debug_list is not None:
                                debug_list.append({"name": person.get("fullName") or name, "skip": f"AB/GP floors not met (AB={ab}, GP={gp})"})
                            continue

                        team_name = _extract_team_name_from_person_or_logs(person, None, pid, logs, target_date)
                        avg_season_hitless = _average_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)

                        cand = {
                            "name": person.get("fullName") or name,
                            "team": team_name,
                            "season_avg": round(float(season_avg), 3),
                            "hitless_streak": streak,
                            "avg_hitless_streak_season": round(avg_season_hitless, 2) if avg_season_hitless is not None else 0.0,
                            "_person_like": person,
                            "_pid": pid,
                        }
                        cand = _decorate_candidate(cand, logs, target_date)
                        candidates.append(cand)
                        if len(candidates) >= limit:
                            break
                    except Exception as e:
                        if debug_list is not None:
                            debug_list.append({"name": name, "error": f"{type(e).__name__}: {e}"})
                # Presentation
                if group_mode == "streak":
                    buckets: Dict[int, List[Dict]] = {}
                    for c in candidates:
                        buckets.setdefault(int(c.get("hitless_streak", 0)), []).append(c)
                    out_list: List[Dict] = []
                    for k in sorted(buckets.keys(), reverse=True):
                        grp = sorted(buckets[k], key=lambda x: float(x.get("ranking_score", x.get("score", 0.0))), reverse=True)
                        out_list.extend(grp)
                    candidates = out_list[:limit]
                else:
                    if sort_spec:
                        candidates = _apply_sort(candidates, sort_spec)
                    else:
                        candidates = sorted(candidates, key=lambda x: (float(x.get("ranking_score", x.get("score", 0.0))), float(x.get("season_avg", 0.0))), reverse=True)
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

            # (D) Filter to good hitters and (if verify) to pregame teams + AB/GP floors
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

                ab, gp = _season_ab_gp_from_people_like(p)
                if (ab is None or ab < min_season_ab) or (gp is None or gp < min_season_gp):
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
                            "name": meta["name"],
                            "team": team_name,
                            "season_avg": meta["season_avg"],
                            "hitless_streak": streak,
                            "avg_hitless_streak_season": round(avg_season_hitless, 2) if avg_season_hitless is not None else 0.0,
                            "_person_like": meta["person"],
                            "_pid": meta["pid"],
                        }
                        cand = _decorate_candidate(cand, logs, target_date)
                        candidates.append(cand)
                except Exception as e:
                    if debug_list is not None:
                        dbg_name = meta.get("name", "")
                        debug_list.append({"name": dbg_name, "error": f"{type(e).__name__}: {e}"})

            # Presentation (grouping inside buckets by score already consistent with your UI)
            if group_mode == "streak":
                buckets: Dict[int, List[Dict]] = {}
                for c in candidates:
                    buckets.setdefault(int(c.get("hitless_streak", 0)), []).append(c)
                out_list: List[Dict] = []
                for k in sorted(buckets.keys(), reverse=True):
                    grp = sorted(buckets[k], key=lambda x: float(x.get("ranking_score", x.get("score", 0.0))), reverse=True)
                    out_list.extend(grp)
                candidates = out_list[:limit]
            else:
                if sort_spec:
                    candidates = _apply_sort(candidates, sort_spec)
                else:
                    candidates = sorted(candidates, key=lambda x: (float(x.get("ranking_score", x.get("score", 0.0))), float(x.get("season_avg", 0.0))), reverse=True)
                candidates = candidates[:limit]

            if debug_list is not None:
                debug_list.insert(0, {
                    "prospects_scanned": len(prospects),
                    "log_checks": checks,
                    "max_log_checks": MAX_LOG_CHECKS
                })
            return {"candidates": candidates}

        result = run_once_for_date(effective_date, ns_team_ids_today, slate_team_ids_today)
        items: List[Dict] = result["candidates"]

        # Split tiers
        tier_S: List[Dict] = []
        tier_A: List[Dict] = []
        for c in items:
            t = c.get("tier", "")
            if t == "S":
                tier_S.append(c)
            elif t == "A":
                tier_A.append(c)

        response: Dict = {"date": effective_date, "candidates": items}
        if tier_S:
            response["tier_S"] = tier_S
        if tier_A:
            response["tier_A"] = tier_A

        if include_schedule_footer == 1 and sched:
            response["schedule_footer"] = _schedule_footer(sched)

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
                },
                "params": {
                    "mode": mode_norm or None,
                    "as_of": as_of_norm or None,
                    "group_by": group_mode,
                    "sort_by": sort_by or None,
                    "hh_recent_days": hh_recent_days,
                    "include_schedule_footer": include_schedule_footer,
                }
            }
            response["debug"] = [stamp] + (debug_list or [])
        return response
