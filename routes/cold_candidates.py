# routes/cold_candidates.py
from __future__ import annotations

from fastapi import APIRouter, Query
from typing import Dict, List, Optional, Iterable, Any, Tuple
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

def _parse_ymd(s: str) -> date_cls:
    return datetime.strptime(s, "%Y-%m-%d").date()

def _next_ymd_str(s: str) -> str:
    return (_parse_ymd(s) + timedelta(days=1)).isoformat()

def _normalize(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower().strip()

def _fetch_json(client: httpx.Client, url: str, params: Optional[Dict] = None) -> Dict:
    r = client.get(url, params=params)
    r.raise_for_status()
    return r.json()

def _parse_dt(maybe: Optional[str]) -> Optional[datetime]:
    if not maybe:
        return None
    s = str(maybe)
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None

# ---------- schedule helpers ----------
def _schedule_for_date(client: httpx.Client, date_str: str) -> Dict:
    return _fetch_json(client, f"{MLB_BASE}/schedule", params={"sportId": 1, "date": date_str})

def _not_started_team_ids_for_date(schedule_json: Dict) -> set[int]:
    """
    P = Preview, S = Scheduled (not started yet). Others = already started/finished.
    """
    ns_ids: set[int] = set()
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
    ids: set[int] = set()
    for d in schedule_json.get("dates", []):
        for g in d.get("games", []):
            try:
                ids.add(int(g["teams"]["home"]["team"]["id"]))
                ids.add(int(g["teams"]["away"]["team"]["id"]))
            except Exception:
                pass
    return sorted(ids)

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

# ---------- stats helpers ----------
def _season_avg_from_people_like(obj: Dict) -> Optional[float]:
    """
    Extract season AVG from either a 'people' entry or a 'person' (hydrated roster) entry.
    """
    stats = (obj.get("stats") or [])
    for block in stats:
        if (block.get("group", {}).get("displayName") == "hitting" and
            block.get("type", {}).get("displayName", "").lower() == "season"):
            splits = block.get("splits") or []
            if splits:
                avg_str = (splits[0].get("stat") or {}).get("avg")
                try:
                    return float(avg_str)
                except (TypeError, ValueError):
                    return None
    return None

def _game_log_newest_first_regular_season(client: httpx.Client, pid: int, season: int, max_entries: int = 120) -> List[Dict]:
    """
    Fetch hitting game logs and return REGULAR SEASON ('R') only, newest first by parsed gameDate (fallback date).
    """
    data = _fetch_json(
        client,
        f"{MLB_BASE}/people/{pid}/stats",
        params={"stats": "gameLog", "group": "hitting", "season": season, "sportIds": 1},
    )
    splits = ((data.get("stats") or [{}])[0].get("splits")) or []

    filtered: List[Dict] = []
    for s in splits:
        gt = s.get("gameType")
        if gt is not None and gt != "R":
            continue
        filtered.append(s)

    def sort_key(s: Dict[str, Any]) -> tuple:
        dt = _parse_dt(s.get("gameDate") or s.get("date"))
        ts = dt.timestamp() if dt else -1.0
        pk = s.get("game", {}).get("gamePk") or s.get("gamePk") or 0
        try:
            pk = int(pk)
        except Exception:
            pk = 0
        return (ts, pk)

    filtered.sort(key=sort_key, reverse=True)
    return filtered[:max_entries]

def _current_hitless_streak_ab_gt0(game_splits: List[Dict]) -> int:
    """
    Count consecutive MOST-RECENT games with AB>0 and H==0; skip 0-AB/DNP entirely.
    """
    streak = 0
    for s in game_splits:
        stat = s.get("stat") or {}
        ab = int(stat.get("atBats") or 0)
        if ab <= 0:
            continue
        hits = int(stat.get("hits") or 0)
        if hits == 0:
            streak += 1
        else:
            break
    return streak

# ---------- hydrated roster pulls ----------
def _teams_hydrated_rosters(
    client: httpx.Client,
    season: int,
    team_ids: List[int],
) -> Dict[int, List[Dict]]:
    """
    Pull teams with hydrated active rosters including season hitting stats in (typically) one request.
    """
    out: Dict[int, List[Dict]] = {tid: [] for tid in team_ids}
    if not team_ids:
        return out

    params = {
        "sportId": 1,
        "season": season,
        "teamId": ",".join(str(t) for t in team_ids),
        "hydrate": f"roster(rosterType=active,person(stats(group=hitting,type=season,season={season})))",
    }
    data = _fetch_json(client, f"{MLB_BASE}/teams", params=params)
    teams = data.get("teams", []) or []
    for t in teams:
        try:
            tid = int(t["id"])
        except Exception:
            continue
        roster_obj = t.get("roster")
        roster_list = []
        if isinstance(roster_obj, dict):
            roster_list = roster_obj.get("roster") or []
        elif isinstance(roster_obj, list):
            roster_list = roster_obj
        for r in roster_list:
            person = r.get("person") or {}
            if person:
                out.setdefault(tid, []).append(person)
    return out

def _prospects_from_rosters(
    hydrated: Dict[int, List[Dict]],
    min_season_avg: float,
    verify_ns_team_ids: Optional[set[int]],
) -> List[Tuple[float, Dict]]:
    """
    Build list of (season_avg, person_record) filtered by min_season_avg and verify set.
    """
    prospects: List[Tuple[float, Dict]] = []
    for tid, persons in hydrated.items():
        if verify_ns_team_ids is not None and tid not in verify_ns_team_ids:
            continue
        for p in persons:
            season_avg = _season_avg_from_people_like(p)
            if season_avg is None or season_avg < min_season_avg:
                continue
            full = p.get("fullName") or ""
            team = (p.get("currentTeam") or {}).get("name") or ""
            person_id = p.get("id")
            try:
                person_id = int(person_id)
            except Exception:
                person_id = None
            if not person_id:
                continue
            prospects.append((season_avg, {
                "pid": person_id,
                "name": full,
                "team": team,
                "team_id": tid,
                "season_avg": round(float(season_avg), 3),
            }))
    prospects.sort(key=lambda x: x[0], reverse=True)
    return prospects

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
    roll_to_next_slate_if_empty: int = Query(1, ge=0, le=1, description="If verify=1 and pregame set empty or yields no candidates, roll to NEXT day (strict pregame)."),
    last_n: Optional[int] = Query(None, description="Ignored. Backward compatibility only."),
    debug: int = Query(0, ge=0, le=1),
):
    """
    Verified cold-hitter candidates for betting:
      • Good hitters (season AVG ≥ min_season_avg)
      • Current hitless streak (AB>0 only; DNP/0-AB ignored)
      • STRICT pregame when verify=1 (exclude in-progress/finished); optional auto-roll to next slate
    Response: { "date": "<effective_slate_date>", "candidates": [...], "debug": [...]? }
    """
    requested_date = _eastern_today_str() if _normalize(date) == "today" else date
    effective_date = requested_date

    # Internal caps to keep latency low
    LOG_CHECKS_MULTIPLIER = 6  # e.g., limit=30 => up to 180 log checks
    with httpx.Client(timeout=15) as client:
        # 1) Slate + pregame set
        sched = _schedule_for_date(client, effective_date)
        ns_team_ids = _not_started_team_ids_for_date(sched) if verify else set()
        slate_team_ids = _team_ids_from_schedule(sched) or _all_mlb_team_ids(client, season)

        # 2) If strict pregame and pregame set empty or returns no candidates later, we may roll to next day
        rolled = False

        def run_once_for_date(target_date: str, ns_ids: set[int], team_ids: List[int]) -> Dict:
            # Names mode
            if names:
                requested_names = [n.strip() for n in names.split(",") if n.strip()]
                candidates: List[Dict] = []
                dbg: List[Dict] = []
                for name in requested_names:
                    try:
                        data = _fetch_json(client, f"{MLB_BASE}/people/search", params={"names": name})
                        people = data.get("people", []) or []
                        if not people:
                            if debug:
                                dbg.append({"name": name, "skip": "player not found"})
                            continue
                        norm_target = _normalize(name)
                        p0 = None
                        for p in people:
                            if _normalize(p.get("fullName", "")) == norm_target:
                                p0 = p
                                break
                        if p0 is None:
                            p0 = people[0]
                        pid = int(p0["id"])
                        hydrate = f"team,stats(group=hitting,type=season,season={season})"
                        pdata = _fetch_json(client, f"{MLB_BASE}/people/{pid}", params={"hydrate": hydrate})
                        person = (pdata.get("people") or [{}])[0]
                        season_avg = _season_avg_from_people_like(person)
                        team_info = person.get("currentTeam") or {}
                        team_id = team_info.get("id")
                        team_name = (team_info.get("name") or "").strip()
                        if season_avg is None or season_avg < min_season_avg:
                            if debug:
                                reason = "no season stats" if season_avg is None else f"season_avg {season_avg:.3f} < {min_season_avg:.3f}"
                                dbg.append({"name": person.get("fullName") or name, "team": team_name, "skip": reason})
                            continue
                        if verify:
                            try:
                                tid = int(team_id) if team_id is not None else None
                            except Exception:
                                tid = None
                            if (tid is None) or (tid not in ns_ids):
                                if debug:
                                    dbg.append({"name": person.get("fullName") or name, "team": team_name, "skip": "verify(strict): team not pregame"})
                                continue
                        logs = _game_log_newest_first_regular_season(client, pid, season, max_entries=120)
                        streak = _current_hitless_streak_ab_gt0(logs)
                        if streak < min_hitless_games:
                            if debug:
                                dbg.append({"name": person.get("fullName") or name, "team": team_name, "skip": f"hitless_streak {streak} < {min_hitless_games}"})
                            continue
                        candidates.append({
                            "name": person.get("fullName") or name,
                            "team": team_name,
                            "season_avg": round(float(season_avg), 3),
                            "hitless_streak": streak,
                        })
                        if len(candidates) >= limit:
                            break
                    except Exception as e:
                        if debug:
                            dbg.append({"name": name, "error": f"{type(e).__name__}: {e}"})
                candidates.sort(key=lambda x: (x.get("season_avg", 0.0), x.get("hitless_streak", 0)), reverse=True)
                return {"candidates": candidates, "debug": dbg}

            # League scan mode: hydrated rosters → prefilter → selective logs
            hydrated = _teams_hydrated_rosters(client, season, team_ids)
            verify_set = ns_ids if verify else None
            prospects = _prospects_from_rosters(hydrated, min_season_avg=min_season_avg, verify_ns_team_ids=verify_set)

            max_log_checks = max(30, min(500, limit * LOG_CHECKS_MULTIPLIER))
            candidates: List[Dict] = []
            dbg: List[Dict] = []
            checks = 0
            for _, meta in prospects:
                if checks >= max_log_checks:
                    break
                checks += 1
                try:
                    logs = _game_log_newest_first_regular_season(client, meta["pid"], season, max_entries=120)
                    streak = _current_hitless_streak_ab_gt0(logs)
                    if streak >= min_hitless_games:
                        candidates.append({
                            "name": meta["name"],
                            "team": meta["team"],
                            "season_avg": meta["season_avg"],
                            "hitless_streak": streak,
                        })
                        if len(candidates) >= limit:
                            break
                except Exception as e:
                    if debug:
                        dbg.append({"name": meta["name"], "team": meta["team"], "error": f"{type(e).__name__}: {e}"})

            candidates.sort(key=lambda x: (x.get("season_avg", 0.0), x.get("hitless_streak", 0)), reverse=True)
            return {"candidates": candidates, "debug": [{"prospects_scanned": len(prospects), "log_checks": checks, "max_log_checks": max_log_checks}] + dbg if debug else []}

        # First attempt for requested date
        run1 = run_once_for_date(effective_date, ns_team_ids, slate_team_ids)
        candidates = run1["candidates"]

        # If strict pregame & none found (or no pregame teams), optionally roll to next slate
        if verify and roll_to_next_slate_if_empty and (len(ns_team_ids) == 0 or len(candidates) == 0):
            effective_date = _next_ymd_str(effective_date)
            sched2 = _schedule_for_date(client, effective_date)
            ns_team_ids2 = _not_started_team_ids_for_date(sched2)
            slate_team_ids2 = _team_ids_from_schedule(sched2) or slate_team_ids
            rolled = True
            run2 = run_once_for_date(effective_date, ns_team_ids2, slate_team_ids2)
            candidates = run2["candidates"]
            debug_list = (run2["debug"] if debug else None)
            pregame_count = len(ns_team_ids2)
        else:
            debug_list = (run1["debug"] if debug else None)
            pregame_count = len(ns_team_ids)

        resp: Dict = {"date": effective_date, "candidates": candidates}
        if debug:
            stamp = {
                "requested_date": requested_date,
                "effective_date": effective_date,
                "verify": int(verify),
                "rolled_to_next_slate": bool(rolled),
                "pregame_team_count": pregame_count,
                "slate_team_count": len(slate_team_ids),
            }
            if debug_list is None:
                debug_list = []
            debug_list.insert(0, stamp)
            resp["debug"] = debug_list
        return resp
