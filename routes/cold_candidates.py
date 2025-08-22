# routes/cold_candidates.py
from __future__ import annotations

from fastapi import APIRouter, Query
from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime, date as date_cls, timezone, timedelta
import unicodedata
import httpx
import pytz

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

# ---------- schedule helpers ----------
def _schedule_for_date(client: httpx.Client, date_str: str) -> Dict:
    return _fetch_json(client, f"{MLB_BASE}/schedule", params={"sportId": 1, "date": date_str})

def _not_started_team_ids_for_date(schedule_json: Dict) -> Set[int]:
    """
    STRICT pregame set: P = Preview, S = Scheduled (not started yet).
    """
    ns_ids: Set[int] = set()
    for d in schedule_json.get("dates", []):
        for g in d.get("games", []):
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
    for d in schedule_json.get("dates", []):
        for g in d.get("games", []):
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
    for d in schedule_json.get("dates", []):
        for g in d.get("games", []):
            pk = g.get("gamePk")
            try:
                if pk is not None:
                    pks.add(int(pk))
            except Exception:
                continue
    return pks

def _all_mlb_team_ids(client: httpx.Client, season: int) -> List[int]:
    data = _fetch_json(client, f"{MLB_BASE}/teams", params={"sportId": 1, "season": season})
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

def _game_log_regular_season_desc(client: httpx.Client, pid: int, season: int, max_entries: int = 160) -> List[Dict]:
    data = _fetch_json(
        client,
        f"{MLB_BASE}/people/{pid}/stats",
        params={"stats": "gameLog", "group": "hitting", "season": season, "sportIds": 1},
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
        # exclude explicit same-day by gamePk first (robust vs provider quirks)
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

    # Build chronological list of prior games with AB>0 (oldest -> newest)
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
            continue  # exclude same-day and future
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
    scan_multiplier: int = Query(8, ge=1, le=40, description="How many logs to check: limit × scan_multiplier (cap applies)"),
    max_log_checks: Optional[int] = Query(None, ge=1, le=5000, description="Hard cap for log checks; overrides scan_multiplier."),
    debug: int = Query(0, ge=0, le=1),
    # --- New additive params ---
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
    # If as_of is provided and not today in ET, use it for streak math.
    # For historical snapshots, STRICT pregame verification does not make sense → disable verify and do not roll.
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

    # --- sort/group presets
    if (group_by or "").strip().lower() == "none":
        sort_spec = _parse_sort_by(sort_by)
    else:
        sort_spec = _parse_sort_by(sort_by) if sort_by else [("hitless_streak", True), ("season_avg", True), ("avg_hitless_streak_season", True)]

    with httpx.Client(timeout=25) as client:
        # schedule & helpers for this date
        sched = _schedule_for_date(client, effective_date) if not historical_mode or verify_effective else _schedule_for_date(client, effective_date)
        ns_team_ids_today = _not_started_team_ids_for_date(sched) if verify_effective else set()
        slate_team_ids_today = _team_ids_from_schedule(sched) if sched else _all_mlb_team_ids(client, season)
        # robust guard: exclude *any* gamePk on the schedule date from streak math
        exclude_pks_for_date = _game_pks_for_date(sched) if sched else set()

        rolled = False
        if (verify_effective == 1) and roll_enabled and len(ns_team_ids_today) == 0:
            effective_date = _next_ymd_str(effective_date)
            sched = _schedule_for_date(client, effective_date)
            ns_team_ids_today = _not_started_team_ids_for_date(sched)
            slate_team_ids_today = _team_ids_from_schedule(sched) or slate_team_ids_today
            exclude_pks_for_date = _game_pks_for_date(sched)
            rolled = True

        debug_list: Optional[List[Dict]] = [] if debug else None

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
                        team_info = person.get("currentTeam") or {}
                        team_id = team_info.get("id")
                        team_name = (team_info.get("name") or "").strip()
                        if season_avg is None or season_avg < min_season_avg:
                            if debug_list is not None:
                                why = "no season stats" if season_avg is None else f"season_avg {season_avg:.3f} < {min_season_avg:.3f}"
                                debug_list.append({"name": person.get("fullName") or name, "team": team_name, "skip": why})
                            continue
                        if verify_effective:
                            try:
                                tid = int(team_id) if team_id is not None else None
                            except Exception:
                                tid = None
                            if (tid is None) or (tid not in ns_ids):
                                if debug_list is not None:
                                    debug_list.append({"name": person.get("fullName") or name, "team": team_name, "skip": "verify(strict): team not pregame"})
                                continue
                        logs = _game_log_regular_season_desc(client, pid, season, max_entries=160)
                        streak = _current_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)
                        if streak < min_hitless_games:
                            if debug_list is not None:
                                debug_list.append({"name": person.get("fullName") or name, "team": team_name, "skip": f"hitless_streak {streak} < {min_hitless_games}"})
                            continue
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
                candidates = _apply_sort(candidates, sort_spec)
                return {"candidates": candidates[:limit]}

            # league scan mode
            scan_team_ids = sorted(ns_ids) if verify_effective else team_ids

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
                team_info = p.get("currentTeam") or {}
                team_name = (team_info.get("name") or "").strip()
                try:
                    team_id = int(team_info.get("id")) if team_info.get("id") is not None else None
                except Exception:
                    team_id = None
                if verify_effective and (team_id is None or team_id not in ns_ids):
                    continue
                pid = p.get("id")
                try:
                    pid = int(pid)
                except Exception:
                    pid = None
                if pid is None:
                    continue
                prospects.append((
                    float(season_avg),
                    {"pid": pid, "name": p.get("fullName") or "", "team": team_name, "season_avg": round(float(season_avg), 3)}
                ))

            prospects.sort(key=lambda x: x[0], reverse=True)

            # (E) Logs+streaks for prospects (capped), and include avg_hitless_streak_season
            checks = 0
            for _, meta in prospects:
                if checks >= MAX_LOG_CHECKS or len(candidates) >= limit:
                    break
                checks += 1
                try:
                    logs = _game_log_regular_season_desc(client, meta["pid"], season, max_entries=160)
                    streak = _current_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)
                    if streak >= min_hitless_games:
                        avg_season_hitless = _average_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)
                        candidates.append({
                            "name": meta["name"],
                            "team": meta["team"],
                            "season_avg": meta["season_avg"],
                            "hitless_streak": streak,
                            "avg_hitless_streak_season": round(avg_season_hitless, 2) if avg_season_hitless is not None else 0.0,
                        })
                except Exception as e:
                    if debug_list is not None:
                        debug_list.append({"name": meta["name"], "team": meta["team"], "error": f"{type(e).__name__}: {e}"})

            candidates = _apply_sort(candidates, sort_spec)
            if debug_list is not None:
                debug_list.insert(0, {
                    "prospects_scanned": len(prospects),
                    "log_checks": checks,
                    "max_log_checks": MAX_LOG_CHECKS
                })
            return {"candidates": candidates[:limit]}

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
            debug_list.insert(0, stamp)
            response["debug"] = debug_list
        return response
