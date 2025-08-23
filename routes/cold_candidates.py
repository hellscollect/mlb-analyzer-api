# routes/cold_candidates.py
from __future__ import annotations

from fastapi import APIRouter, Query
from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime, date as date_cls, timezone, timedelta
import unicodedata
import time
import httpx
import pytz

router = APIRouter()
MLB_BASE = "https://statsapi.mlb.com/api/v1"
_EASTERN = pytz.timezone("US/Eastern")

# --------------------
# Tunables for resilience
# --------------------
HTTP_TIMEOUT = 12.0                 # per-request timeout (seconds)
HTTP_RETRIES = 3                    # total attempts per call
HTTP_BACKOFF = 0.25                 # seconds between retries (exponential)
HTTP_POOL_LIMITS = httpx.Limits(    # connection pool tuning
    max_keepalive_connections=8,
    max_connections=16,
)

# Hard safety caps for ALL-teams mode unless user overrides
ALL_MODE_DEFAULT_LIMIT_CAP = 30
ALL_MODE_DEFAULT_LOG_CAP = 400

# Absolute ceilings (regardless of user input)
ABS_MAX_LOG_CHECKS = 3000
ABS_MAX_SCAN_MULT = 40
ABS_MAX_SEASON_LOGS = 200  # per player, game logs fetched (desc)

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

# ---------- HTTP helpers (retry + backoff) ----------
def _should_retry(status_code: Optional[int]) -> bool:
    if status_code is None:
        return True
    # retry on common transient statuses
    return status_code in (408, 425, 429, 500, 502, 503, 504)

def _fetch_json_with_retries(client: httpx.Client, url: str, params: Optional[Dict] = None, dbg: Optional[List[Dict]] = None, label: str = "") -> Dict:
    last_err: Optional[str] = None
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            resp = client.get(url, params=params, timeout=HTTP_TIMEOUT)
            if resp.status_code >= 400:
                if _should_retry(resp.status_code) and attempt < HTTP_RETRIES:
                    if dbg is not None:
                        dbg.append({"retry": label, "status": resp.status_code, "attempt": attempt})
                    time.sleep(HTTP_BACKOFF * attempt)
                    continue
                resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            if dbg is not None:
                dbg.append({"fetch_error": label, "attempt": attempt, "error": last_err})
            if attempt < HTTP_RETRIES:
                time.sleep(HTTP_BACKOFF * attempt)
            else:
                break
    # Final failure -> return empty dict; callers remain robust
    return {}

def _fetch_json(client: httpx.Client, url: str, params: Optional[Dict], dbg: Optional[List[Dict]], label: str) -> Dict:
    return _fetch_json_with_retries(client, url, params=params, dbg=dbg, label=label)

# ---------- schedule helpers ----------
def _schedule_for_date(client: httpx.Client, date_str: str, dbg: Optional[List[Dict]]) -> Dict:
    return _fetch_json(client, f"{MLB_BASE}/schedule", {"sportId": 1, "date": date_str}, dbg, f"schedule:{date_str}")

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
    All gamePk values for the slate date (used to exclude same-day logs robustly,
    even if provider timestamps are weird or in-progress rows are present).
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

def _all_mlb_team_ids(client: httpx.Client, season: int, dbg: Optional[List[Dict]]) -> List[int]:
    data = _fetch_json(client, f"{MLB_BASE}/teams", {"sportId": 1, "season": season}, dbg, f"teams:{season}")
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

def _game_log_regular_season_desc(client: httpx.Client, pid: int, season: int, max_entries: int, dbg: Optional[List[Dict]]) -> List[Dict]:
    data = _fetch_json(
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
            data = _fetch_json(client, f"{MLB_BASE}/teams/{team_id}/roster", params=params, dbg=dbg, label=f"roster:{team_id}:{label}")
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
    # slightly smaller chunk to reduce payloads/timeouts
    for i in range(0, len(team_ids), 6):
        sub = team_ids[i:i+6]
        params = {
            "teamIds": ",".join(str(t) for t in sub),
            "sportId": 1,
            "season": season,
            "hydrate": f"roster(person,person.stats(group=hitting,type=season,season={season}))"
        }
        try:
            data = _fetch_json(client, f"{MLB_BASE}/teams", params=params, dbg=dbg, label=f"teams_hydrate:{sub}")
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
    # smaller batch to reduce payload sizes/timeouts in ALL mode
    for i in range(0, len(ids), 80):
        sub = ids[i:i+80]
        try:
            params = {
                "personIds": ",".join(str(x) for x in sub),
                "hydrate": f"team,stats(group=hitting,type=season,season={season})"
            }
            data = _fetch_json(client, f"{MLB_BASE}/people", params=params, dbg=dbg, label=f"people_batch:{i}:{len(sub)}")
            ppl = data.get("people", []) or []
            out.extend(ppl)
            if dbg is not None:
                dbg.append({"people_batch_chunk": len(sub), "returned": len(ppl)})
        except Exception as e:
            if dbg is not None:
                dbg.append({"people_batch_chunk": len(sub), "error": f"{type(e).__name__}: {e}"})
    return out

# ---------- sorting helpers ----------
_VALID_SORT_KEYS = {"hitless_streak", "season_avg", "avg_hitless_streak_season"}

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
    scan_multiplier: int = Query(8, ge=1, le=ABS_MAX_SCAN_MULT, description="How many logs to check: limit × scan_multiplier (cap applies)"),
    max_log_checks: Optional[int] = Query(None, ge=1, le=ABS_MAX_LOG_CHECKS, description="Hard cap for log checks; overrides scan_multiplier."),
    debug: int = Query(0, ge=0, le=1),
    # --- Additive params ---
    mode: Optional[str] = Query(None, description="Alias for verify. 'pregame' -> verify=1, 'all' -> verify=0. If set, overrides verify."),
    as_of: Optional[str] = Query(None, description="YYYY-MM-DD snapshot for streak math. If not today ET, verification is disabled and no roll-forward."),
    group_by: str = Query("streak", description="Streak grouping preset: 'streak' or 'none' (affects sort preset only; response shape unchanged)."),
    sort_by: Optional[str] = Query(None, description="Comma-separated fields with optional '-' for DESC. Fields: hitless_streak,season_avg,avg_hitless_streak_season."),
):
    """
    VERIFIED cold-hitter candidates:
      • Good hitters (season AVG ≥ min_season_avg)
      • Current hitless streak (AB>0 only; DNP/0-AB ignored)
      • STRICT pregame when verify=1 (exclude in-progress/finished)
      • Exclude same-day games from streak calc (use previous games only)
      • avg_hitless_streak_season = average length of COMPLETED hitless streaks before the slate date
    """
    # --------------- guard & normalize ---------------
    requested_date = _eastern_today_str() if _normalize(date) == "today" else date
    effective_date = requested_date

    # mode alias mapping (mode takes precedence over verify if provided)
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

    # scan budget
    DEFAULT_MULT = max(1, int(scan_multiplier))
    computed_max = min(ABS_MAX_LOG_CHECKS, limit * DEFAULT_MULT)
    MAX_LOG_CHECKS = max_log_checks if max_log_checks is not None else computed_max

    # Adaptive load shedding for verify=0 (ALL teams) when user didn't set caps
    if verify_effective == 0:
        if max_log_checks is None:
            MAX_LOG_CHECKS = min(MAX_LOG_CHECKS, ALL_MODE_DEFAULT_LOG_CAP)
        if limit > ALL_MODE_DEFAULT_LIMIT_CAP:
            limit = ALL_MODE_DEFAULT_LIMIT_CAP  # gentle cap to avoid timeouts

    # sort/group presets
    if (group_by or "").strip().lower() == "none":
        sort_spec = _parse_sort_by(sort_by)
    else:
        sort_spec = _parse_sort_by(sort_by) if sort_by else [("hitless_streak", True), ("season_avg", True), ("avg_hitless_streak_season", True)]

    # ---- main logic (fully wrapped to avoid connector 500s) ----
    debug_list: Optional[List[Dict]] = [] if debug else None
    try:
        with httpx.Client(limits=HTTP_POOL_LIMITS, timeout=HTTP_TIMEOUT) as client:
            # schedule & helpers for this date (fault tolerant)
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

            def run_once_for_date(target_date: str, ns_ids: Set[int], team_ids: List[int]) -> Dict:
                candidates: List[Dict] = []

                # explicit names mode
                if names:
                    requested = [n.strip() for n in names.split(",") if n.strip()]
                    for name in requested:
                        try:
                            data = _fetch_json(client, f"{MLB_BASE}/people/search", {"names": name}, debug_list, f"people_search:{name}")
                            people = data.get("people", []) or []
                            if not people:
                                if debug_list is not None:
                                    debug_list.append({"name": name, "skip": "player not found"})
                                continue
                            norm_target = _normalize(name)
                            p0 = next((p for p in people if _normalize(p.get("fullName","")) == norm_target), people[0])
                            pid = int(p0["id"])

                            pdata = _fetch_json(client, f"{MLB_BASE}/people/{pid}", {"hydrate": f"team,stats(group=hitting,type=season,season={season})"}, debug_list, f"person:{pid}")
                            person = (pdata.get("people") or [{}])[0]
                            season_avg = _season_avg_from_people_like(person)

                            logs = _game_log_regular_season_desc(client, pid, season, max_entries=min(ABS_MAX_SEASON_LOGS, 160), dbg=debug_list)
                            streak = _current_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)
                            if season_avg is None or season_avg < min_season_avg or streak < min_hitless_games:
                                if debug_list is not None:
                                    why = []
                                    if season_avg is None: why.append("no season stats")
                                    elif season_avg < min_season_avg: why.append(f"season_avg {season_avg:.3f} < {min_season_avg:.3f}")
                                    if streak < min_hitless_games: why.append(f"hitless_streak {streak} < {min_hitless_games}")
                                    debug_list.append({"name": person.get("fullName") or name, "skip": ", ".join(why) or "filtered"})
                                continue

                            team_name = _extract_team_name_from_person_or_logs(person, None, pid, logs, target_date)
                            avg_season_hitless = _average_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)

                            candidates.append({
                                "name": person.get("fullName") or name,
                                "team": team_name,
                                "season_avg": round(float(season_avg), 3),
                                "hitless_streak": streak,
                                "avg_hitless_streak_season": round(avg_season_hitless, 2) if avg_season_hitless is not None else 0.0,
                            })
                            if len(candidates) >= limit:
                                break
                        except Exception as e:
                            if debug_list is not None:
                                debug_list.append({"name": name, "error": f"{type(e).__name__}: {e}"})
                    return {"candidates": _apply_sort(candidates, sort_spec)[:limit]}

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

                # (D) Filter to good hitters and (if verify) to pregame teams
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

                # (E) Logs+streaks for prospects (capped), and include avg_hitless_streak_season
                checks = 0
                for _, meta in prospects:
                    if checks >= MAX_LOG_CHECKS or len(candidates) >= limit:
                        break
                    checks += 1
                    try:
                        logs = _game_log_regular_season_desc(client, meta["pid"], season, max_entries=min(ABS_MAX_SEASON_LOGS, 160), dbg=debug_list)
                        streak = _current_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)
                        if streak >= min_hitless_games:
                            avg_season_hitless = _average_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)
                            team_name = _extract_team_name_from_person_or_logs(
                                meta["person"], team_map, meta["pid"], logs, target_date
                            )
                            candidates.append({
                                "name": meta["name"],
                                "team": team_name,
                                "season_avg": meta["season_avg"],
                                "hitless_streak": streak,
                                "avg_hitless_streak_season": round(avg_season_hitless, 2) if avg_season_hitless is not None else 0.0,
                            })
                    except Exception as e:
                        if debug_list is not None:
                            debug_list.append({"name": meta.get("name", ""), "error": f"{type(e).__name__}: {e}"})

                if debug_list is not None:
                    debug_list.insert(0, {
                        "prospects_scanned": len(prospects),
                        "log_checks": checks,
                        "max_log_checks": MAX_LOG_CHECKS
                    })
                return {"candidates": _apply_sort(candidates, sort_spec)[:limit]}

            result = run_once_for_date(effective_date, ns_team_ids_today, slate_team_ids_today)
            items = result["candidates"]

            response: Dict = {"date": effective_date, "candidates": items}
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
                        "limit": limit,
                        "scan_multiplier": DEFAULT_MULT,
                        "max_log_checks": MAX_LOG_CHECKS,
                    },
                    "params": {
                        "mode": mode_norm or None,
                        "as_of": as_of_norm or None,
                        "group_by": group_by,
                        "sort_by": sort_by or None,
                    }
                }
                response["debug"] = [stamp] + (debug_list or [])
            return response

    except Exception as e:
        # Final catch-all to prevent connector 500s.
        # Return a valid 200 payload with debug context so your GPT won’t show “Error talking to connector”.
        fail_resp: Dict[str, Any] = {"date": effective_date, "candidates": []}
        if debug_list is not None:
            debug_list.insert(0, {"fatal_error": f"{type(e).__name__}: {e}"})
            fail_resp["debug"] = debug_list
        else:
            # even without debug=1, include a minimal hint
            fail_resp["debug"] = [{"fatal_error": f"{type(e).__name__}: {e}"}]
        return fail_resp
