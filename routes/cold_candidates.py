# routes/cold_candidates.py
from __future__ import annotations

from fastapi import APIRouter, Query
from typing import Dict, List, Optional, Tuple, Iterable
from datetime import datetime
import unicodedata
import httpx
import pytz

router = APIRouter()

# ---- MLB Stats API helpers ---------------------------------------------------

MLB_BASE = "https://statsapi.mlb.com/api/v1"

def _eastern_today_str() -> str:
    tz = pytz.timezone("US/Eastern")
    return datetime.now(tz).date().isoformat()

def _normalize(s: str) -> str:
    # strip accents and lowercase for lenient comparisons
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower().strip()

def _fetch_json(client: httpx.Client, url: str, params: Optional[Dict] = None) -> Dict:
    r = client.get(url, params=params)
    r.raise_for_status()
    return r.json()

def _search_player(client: httpx.Client, name: str) -> Optional[Dict]:
    """
    Returns MLB people dict for best match or None.
    Prefers exact (case/diacritics-insensitive) fullName match among MLB people.
    """
    data = _fetch_json(client, f"{MLB_BASE}/people/search", params={"names": name})
    people = data.get("people", []) or []
    if not people:
        return None

    norm_target = _normalize(name)
    for p in people:
        full = p.get("fullName") or ""
        if _normalize(full) == norm_target:
            return p
    return people[0]

def _person_with_stats(client: httpx.Client, pid: int, season: int) -> Dict:
    """
    Returns /people payload with currentTeam and season stats (hitting season).
    """
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

def _game_log(client: httpx.Client, pid: int, season: int) -> List[Dict]:
    """
    Returns hitting game logs for a player for the season (newest first).
    """
    data = _fetch_json(
        client,
        f"{MLB_BASE}/people/{pid}/stats",
        params={"stats": "gameLog", "group": "hitting", "season": season, "sportIds": 1},
    )
    splits = ((data.get("stats") or [{}])[0].get("splits")) or []
    splits.sort(key=lambda s: s.get("date", s.get("gameDate", "")), reverse=True)
    return splits

def _hitless_streak_from_log(game_splits: List[Dict], last_n: int) -> int:
    """
    Count consecutive *most recent* games with AB>0 and H==0, up to last_n.
    If most recent game with an AB has H>0, streak is 0.
    We skip any games with zero AB (DNP/PH without AB/PR only).
    """
    streak = 0
    count_checked = 0
    for s in game_splits:
        stat = s.get("stat") or {}
        ab = int(stat.get("atBats") or 0)
        if ab <= 0:
            continue  # skip games without an AB
        hits = int(stat.get("hits") or 0)
        count_checked += 1
        if hits == 0:
            streak += 1
        else:
            break
        if count_checked >= last_n:
            break
    return streak

def _schedule_for_date(client: httpx.Client, date_str: str) -> Dict:
    return _fetch_json(client, f"{MLB_BASE}/schedule", params={"sportId": 1, "date": date_str})

def _not_started_team_ids_for_date(schedule_json: Dict) -> set[int]:
    """
    Teams whose game for the date is not started yet (statusCode P or S).
    P = Preview, S = Scheduled. I = In Progress, F = Final, etc.
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
    ids: List[int] = []
    for t in teams:
        try:
            ids.append(int(t["id"]))
        except Exception:
            pass
    return sorted(ids)

def _team_active_roster_people(client: httpx.Client, team_id: int, season: int) -> List[Dict]:
    # Active roster only; hitters/pitchers both returned; we’ll filter by having hitting stats later.
    data = _fetch_json(client, f"{MLB_BASE}/teams/{team_id}/roster", params={"rosterType": "active", "season": season})
    return data.get("roster", []) or []

def _iter_league_player_names_for_scan(client: httpx.Client, season: int, date_str: str) -> Iterable[str]:
    """
    Yields player full names to scan. Prefer today’s scheduled teams to reduce load.
    Fallback to all MLB teams if today has no games in schedule JSON.
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

# ---- Route -------------------------------------------------------------------

@router.get("/cold_candidates")
def cold_candidates(
    date: str = Query("today", description="YYYY-MM-DD or 'today' (US/Eastern)"),
    season: int = Query(2025, ge=1900, le=2100),
    names: Optional[str] = Query(None, description="Comma-separated player names (optional). If omitted, scans league rosters."),
    min_season_avg: float = Query(0.26, ge=0.0, le=1.0),
    min_hitless_games: int = Query(3, ge=1),
    last_n: int = Query(7, ge=1, le=50),
    limit: int = Query(30, ge=1, le=1000),
    verify: int = Query(1, ge=0, le=1, description="1=only include players on teams that have NOT started their game today"),
    debug: int = Query(0, ge=0, le=1),
):
    """
    Returns players who:
      - have season AVG >= min_season_avg
      - have a current hitless streak >= min_hitless_games over their last `last_n` games with an AB (AB>0 only)
    Behavior:
      • If `names` is supplied → only those players are evaluated.
      • If `names` is omitted → scan active rosters for today’s scheduled teams (fallback: all MLB teams).
      • If verify=1 → include only players whose teams have NOT started yet today (status P/S).
    """
    date_str = _eastern_today_str() if _normalize(date) == "today" else date

    # Setup client once for the whole scan
    items: List[Dict] = []
    debug_list: Optional[List[Dict]] = [] if debug else None

    with httpx.Client(timeout=20) as client:
        # Pre-compute schedule context if needed and not-started team ids
        sched = _schedule_for_date(client, date_str)
        ns_team_ids: set[int] = _not_started_team_ids_for_date(sched) if (verify or debug) else set()

        # Determine the name list to evaluate
        if names:
            requested_names = [n.strip() for n in names.split(",") if n.strip()]
            name_source = "explicit"
        else:
            requested_names = list(_iter_league_player_names_for_scan(client, season, date_str))
            name_source = "league-scan"

        # Evaluate players
        seen: set[str] = set()
        for name in requested_names:
            # Deduplicate names that can appear on multiple rosters due to data oddities
            key = _normalize(name)
            if key in seen:
                continue
            seen.add(key)

            try:
                p = _search_player(client, name)
                if not p:
                    if debug_list is not None:
                        debug_list.append({"name": name, "error": "player not found"})
                    continue

                pid = int(p["id"])
                person = _person_with_stats(client, pid, season)
                matched_full = person.get("fullName") or p.get("fullName") or name
                team_info = person.get("currentTeam") or {}
                team_id = int(team_info.get("id")) if team_info.get("id") is not None else None
                team_name = (team_info.get("name") or "").strip()

                # Filter: must have season hitting stats and meet min AVG
                season_avg = _season_avg_from_people(person)
                if season_avg is None:
                    if debug_list is not None:
                        debug_list.append({"name": matched_full, "pid": pid, "team": team_name, "skip": "no season stats"})
                    continue
                if season_avg < float(min_season_avg):
                    if debug_list is not None:
                        debug_list.append({
                            "name": matched_full, "pid": pid, "team": team_name,
                            "skip": f"season_avg {season_avg:.3f} < min {float(min_season_avg):.3f}"
                        })
                    continue

                # Build streak from game logs (AB>0 only)
                logs = _game_log(client, pid, season)
                streak = _hitless_streak_from_log(logs, last_n=last_n)
                if streak < int(min_hitless_games):
                    if debug_list is not None:
                        debug_list.append({
                            "name": matched_full, "pid": pid, "team": team_name,
                            "skip": f"hitless_streak {streak} < min {int(min_hitless_games)}"
                        })
                    continue

                # Verify filter: only include not-started teams when verify=1
                if verify:
                    if team_id is None or team_id not in ns_team_ids:
                        if debug_list is not None:
                            reason = "verify: team already started or unknown"
                            debug_list.append({"name": matched_full, "team": team_name, "skip": reason})
                        continue

                # Candidate survives
                items.append({
                    "name": matched_full,
                    "team": team_name,
                    "season_avg": round(season_avg, 3),
                    "hitless_streak": streak,
                })

                # Respect limit early for league scans to keep latency reasonable
                if len(items) >= limit:
                    break

            except Exception as e:
                if debug_list is not None:
                    debug_list.append({"name": name, "error": f"exception: {type(e).__name__}: {e}"})

        response: Dict = {
            "date": date_str,
            "season": season,
            "items": items[:limit],
        }
        if debug_list is not None:
            response["debug"] = debug_list
        if verify or debug:
            response["verify_context"] = {
                "scan_mode": name_source,
                "not_started_team_count": len(ns_team_ids),
                "names_checked": len(seen),
                "cutoffs": {
                    "min_season_avg": min_season_avg,
                    "min_hitless_games": min_hitless_games,
                    "last_n": last_n,
                },
            }
        return response
