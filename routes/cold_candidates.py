# routes/cold_candidates.py
from __future__ import annotations

from fastapi import APIRouter, Query
from typing import Dict, List, Optional, Iterable
from datetime import datetime
import unicodedata
import httpx
import pytz

router = APIRouter()

MLB_BASE = "https://statsapi.mlb.com/api/v1"

# ---------------------- time & utils ----------------------

def _eastern_today_str() -> str:
    tz = pytz.timezone("US/Eastern")
    return datetime.now(tz).date().isoformat()

def _normalize(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower().strip()

def _fetch_json(client: httpx.Client, url: str, params: Optional[Dict] = None) -> Dict:
    r = client.get(url, params=params)
    r.raise_for_status()
    return r.json()

# ---------------------- schedule helpers ----------------------

def _schedule_for_date(client: httpx.Client, date_str: str) -> Dict:
    return _fetch_json(client, f"{MLB_BASE}/schedule", params={"sportId": 1, "date": date_str})

def _not_started_team_ids_for_date(schedule_json: Dict) -> set[int]:
    """
    P = Preview, S = Scheduled (not started yet)
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

def _team_active_roster_people(client: httpx.Client, team_id: int, season: int) -> List[Dict]:
    data = _fetch_json(client, f"{MLB_BASE}/teams/{team_id}/roster", params={"rosterType": "active", "season": season})
    return data.get("roster", []) or []

def _iter_league_player_names_for_scan(client: httpx.Client, season: int, date_str: str) -> Iterable[str]:
    """
    Prefer today’s scheduled teams to reduce load; fallback to all MLB teams.
    """
    sched = _schedule_for_date(client, date_str)
    team_ids = _team_ids_from_schedule(sched)
    if not team_ids:
        team_ids = _all_mlb_team_ids(client, season)

    for tid in team_ids:
        roster = _team_active_roster_people(client, tid, season)
        for r in roster:
            person = r.get("person") or {}
            full = person.get("fullName")
            if full:
                yield full

# ---------------------- player helpers ----------------------

def _search_player(client: httpx.Client, name: str) -> Optional[Dict]:
    data = _fetch_json(client, f"{MLB_BASE}/people/search", params={"names": name})
    people = data.get("people", []) or []
    if not people:
        return None
    target = _normalize(name)
    for p in people:
        full = p.get("fullName") or ""
        if _normalize(full) == target:
            return p
    return people[0]

def _person_with_stats(client: httpx.Client, pid: int, season: int) -> Dict:
    hydrate = f"team,stats(group=hitting,type=season,season={season})"
    data = _fetch_json(client, f"{MLB_BASE}/people/{pid}", params={"hydrate": hydrate})
    people = data.get("people", []) or [{}]
    return people[0]

def _season_avg_from_people(people_entry: Dict) -> Optional[float]:
    stats = (people_entry.get("stats") or [])
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

def _game_log_newest_first(client: httpx.Client, pid: int, season: int, last_n_cap: int = 60) -> List[Dict]:
    """
    Return hitting game logs (newest first). We fetch full season log (MLB caps it)
    then sort and take at most last_n_cap entries as a sanity ceiling.
    """
    data = _fetch_json(
        client,
        f"{MLB_BASE}/people/{pid}/stats",
        params={"stats": "gameLog", "group": "hitting", "season": season, "sportIds": 1},
    )
    splits = ((data.get("stats") or [{}])[0].get("splits")) or []
    splits.sort(key=lambda s: s.get("date", s.get("gameDate", "")), reverse=True)
    return splits[:last_n_cap]  # high cap to keep requests reasonable

def _current_hitless_streak_AB_gt0(game_splits: List[Dict]) -> int:
    """
    Count consecutive MOST-RECENT games with AB>0 AND H==0.
    Stops at first game with a hit. Skips DNP/0-AB games entirely.
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

# ---------------------- route ----------------------

@router.get("/cold_candidates")
def cold_candidates(
    date: str = Query("today", description="YYYY-MM-DD or 'today' (US/Eastern)"),
    season: int = Query(2025, ge=1900, le=2100),
    names: Optional[str] = Query(None, description="Optional comma-separated player names. If omitted, scans league rosters for today."),
    min_season_avg: float = Query(0.26, ge=0.0, le=1.0, description="Only include hitters with season AVG >= this."),
    min_hitless_games: int = Query(1, ge=1, description="Current hitless streak in games with AB>0 must be at least this."),
    limit: int = Query(30, ge=1, le=1000),
    verify: int = Query(1, ge=0, le=1, description="1 = only include players on teams that have NOT started yet today (status P/S)."),
    debug: int = Query(0, ge=0, le=1),
):
    """
    Goal: "Who are my cold hitters for today?"
    - Good hitters: season AVG >= min_season_avg (default .260).
    - Must be hitless in their MOST RECENT game with an AB (i.e., current hitless_streak >= 1).
    - Ignore DNP/0-AB games for streak counting.
    - If names omitted, scan today's scheduled teams' active rosters (fallback: all MLB teams).
    - If verify=1, include only players on teams that have NOT started yet today.
    - Sort: season_avg DESC, then hitless_streak DESC (tie-breaker).
    """
    date_str = _eastern_today_str() if _normalize(date) == "today" else date

    with httpx.Client(timeout=20) as client:
        sched = _schedule_for_date(client, date_str)
        ns_team_ids = _not_started_team_ids_for_date(sched) if (verify or debug) else set()

        # Build the evaluation list
        if names:
            requested_names = [n.strip() for n in names.split(",") if n.strip()]
            name_source = "explicit"
        else:
            requested_names = list(_iter_league_player_names_for_scan(client, season, date_str))
            name_source = "league-scan"

        items: List[Dict] = []
        debug_list: Optional[List[Dict]] = [] if debug else None
        seen: set[str] = set()

        for name in requested_names:
            k = _normalize(name)
            if k in seen:
                continue
            seen.add(k)

            try:
                p = _search_player(client, name)
                if not p:
                    if debug_list is not None:
                        debug_list.append({"name": name, "skip": "player not found"})
                    continue

                pid = int(p["id"])
                person = _person_with_stats(client, pid, season)
                full = person.get("fullName") or p.get("fullName") or name
                team_info = person.get("currentTeam") or {}
                team_id = team_info.get("id")
                team_name = (team_info.get("name") or "").strip()

                season_avg = _season_avg_from_people(person)
                if season_avg is None:
                    if debug_list is not None:
                        debug_list.append({"name": full, "skip": "no season stats"})
                    continue
                if season_avg < min_season_avg:
                    if debug_list is not None:
                        debug_list.append({"name": full, "team": team_name, "skip": f"season_avg {season_avg:.3f} < {min_season_avg:.3f}"})
                    continue

                logs = _game_log_newest_first(client, pid, season, last_n_cap=60)
                streak = _current_hitless_streak_AB_gt0(logs)
                if streak < min_hitless_games:
                    if debug_list is not None:
                        debug_list.append({"name": full, "team": team_name, "skip": f"hitless_streak {streak} < {min_hitless_games}"})
                    continue

                # verify filter
                if verify:
                    try:
                        tid = int(team_id) if team_id is not None else None
                    except Exception:
                        tid = None
                    if tid is None or tid not in ns_team_ids:
                        if debug_list is not None:
                            debug_list.append({"name": full, "team": team_name, "skip": "verify: team already started or unknown"})
                        continue

                items.append({
                    "name": full,
                    "team": team_name,
                    "season_avg": round(season_avg, 3),
                    "hitless_streak": streak,
                })

                if len(items) >= limit:
                    break

            except Exception as e:
                if debug_list is not None:
                    debug_list.append({"name": name, "error": f"{type(e).__name__}: {e}"})

        # sort: season_avg DESC, tie-break by hitless_streak DESC
        items.sort(key=lambda x: (x.get("season_avg", 0.0), x.get("hitless_streak", 0)), reverse=True)

        resp: Dict = {
            "date": date_str,
            "season": season,
            "items": items[:limit],
        }
        if debug_list is not None:
            resp["debug"] = debug_list
        if verify or debug:
            resp["verify_context"] = {
                "scan_mode": name_source,
                "not_started_team_count": len(ns_team_ids),
                "names_checked": len(seen),
                "cutoffs": {
                    "min_season_avg": min_season_avg,
                    "min_hitless_games": min_hitless_games,
                },
            }
        return resp
