# routes/cold_candidates.py
from __future__ import annotations

from fastapi import APIRouter, Query
from typing import Dict, List, Optional, Tuple
from datetime import datetime, date
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

def _fetch_json(url: str, params: Optional[Dict] = None) -> Dict:
    with httpx.Client(timeout=15) as client:
        r = client.get(url, params=params)
        r.raise_for_status()
        return r.json()

def _search_player(name: str) -> Optional[Dict]:
    """
    Returns MLB people dict for best match or None.
    Prefers exact (case/diacritics-insensitive) fullName match among MLB people.
    """
    data = _fetch_json(f"{MLB_BASE}/people/search", params={"names": name})
    people = data.get("people", []) or []
    if not people:
        return None

    norm_target = _normalize(name)
    # prefer exact full name match ignoring accents/case
    best = None
    for p in people:
        full = p.get("fullName") or ""
        if _normalize(full) == norm_target:
            best = p
            break
    if best is None:
        best = people[0]
    return best

def _person_with_stats(pid: int, season: int) -> Dict:
    """
    Returns /people payload with currentTeam and season stats (hitting season).
    """
    hydrate = "team,stats(group=hitting,type=season,season={})".format(season)
    data = _fetch_json(f"{MLB_BASE}/people/{pid}", params={"hydrate": hydrate})
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

def _game_log(pid: int, season: int) -> List[Dict]:
    """
    Returns hitting game logs for a player for the season.
    """
    data = _fetch_json(
        f"{MLB_BASE}/people/{pid}/stats",
        params={"stats": "gameLog", "group": "hitting", "season": season, "sportIds": 1},
    )
    splits = ((data.get("stats") or [{}])[0].get("splits")) or []
    # ensure newest first
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

def _schedule_for_date(date_str: str) -> Dict:
    return _fetch_json(f"{MLB_BASE}/schedule", params={"sportId": 1, "date": date_str})

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

# ---- Route -------------------------------------------------------------------

@router.get("/cold_candidates")
def cold_candidates(
    date: str = Query("today", description="YYYY-MM-DD or 'today' (US/Eastern)"),
    season: int = Query(2025, ge=1900, le=2100),
    names: Optional[str] = Query(None, description="Comma-separated player names"),
    min_season_avg: float = Query(0.27, ge=0.0, le=1.0),
    min_hitless_games: int = Query(1, ge=1),
    last_n: int = Query(7, ge=1, le=50),
    limit: int = Query(50, ge=1, le=200),
    verify: int = Query(0, ge=0, le=1, description="1=enforce not-started-team filter"),
    debug: int = Query(0, ge=0, le=1),
):
    """
    Returns players (from the supplied `names`) who:
      - have season AVG >= min_season_avg
      - have a current hitless streak >= min_hitless_games over their last `last_n` games with an AB
    When verify=1, results are additionally filtered to players whose teams have not started yet today.
    """
    date_str = _eastern_today_str() if _normalize(date) == "today" else date

    requested_names: List[str] = []
    if names:
        requested_names = [n.strip() for n in names.split(",") if n.strip()]
    items: List[Dict] = []
    debug_list: Optional[List[Dict]] = [] if debug else None

    if not requested_names:
        resp = {
            "date": date_str,
            "season": season,
            "items": [],
            "debug": [{"note": "no names provided"}] if debug_list is not None else [],
        }
        # Include verify_context if verify/debug requested (for parity with prior responses)
        if verify or debug:
            sched = _schedule_for_date(date_str)
            resp["verify_context"] = {
                "not_started_team_count": len(_not_started_team_ids_for_date(sched)),
                "names_checked": 0,
                "cutoffs": {
                    "min_season_avg": min_season_avg,
                    "min_hitless_games": min_hitless_games,
                    "last_n": last_n,
                },
            }
        return resp

    # Pre-compute schedule context if needed (for verify/debug parity)
    ns_team_ids: set[int] = set()
    if verify or debug:
        sched = _schedule_for_date(date_str)
        ns_team_ids = _not_started_team_ids_for_date(sched)

    for name in requested_names:
        try:
            p = _search_player(name)
            if not p:
                if debug_list is not None:
                    debug_list.append({"name": name, "error": "player not found (no exact match)"})
                continue

            pid = int(p["id"])
            person = _person_with_stats(pid, season)
            matched_full = person.get("fullName") or p.get("fullName") or name
            team_info = person.get("currentTeam") or {}
            team_id = int(team_info.get("id")) if team_info.get("id") is not None else None
            team_name = (team_info.get("name") or "").strip()

            season_avg = _season_avg_from_people(person)
            if season_avg is None:
                if debug_list is not None:
                    debug_list.append({
                        "name": matched_full, "pid": pid, "team": team_name,
                        "skip": "no season stats"
                    })
                continue

            # Build streak from game logs (only AB>0 counted)
            logs = _game_log(pid, season)
            streak = _hitless_streak_from_log(logs, last_n=last_n)

            # Apply numeric gates
            if season_avg < float(min_season_avg):
                if debug_list is not None:
                    debug_list.append({
                        "name": matched_full, "pid": pid, "team": team_name,
                        "skip": f"season_avg {season_avg:.3f} < min {float(min_season_avg):.3f}"
                    })
                continue

            if streak < int(min_hitless_games):
                if debug_list is not None:
                    debug_list.append({
                        "name": matched_full, "pid": pid, "team": team_name,
                        "skip": f"hitless_streak {streak} < min {int(min_hitless_games)}"
                    })
                continue

            # Candidate survives numeric gates; stash with team_id for verify filtering
            items.append({
                "name": matched_full,
                "team": team_name,
                "season_avg": round(season_avg, 3),
                "hitless_streak": streak,
                "_team_id": team_id,  # internal field (removed before returning)
            })

        except Exception as e:
            if debug_list is not None:
                debug_list.append({"name": name, "error": f"exception: {type(e).__name__}: {e}"})

    # Enforce verify filter: only not-started teams
    if verify:
        filtered: List[Dict] = []
        for it in items:
            tid = it.get("_team_id")
            if tid is not None and tid in ns_team_ids:
                filtered.append(it)
            else:
                if debug_list is not None:
                    reason = "verify: team already started" if tid in (None, *[]) or (tid not in ns_team_ids) else "verify: team not matched"
                    debug_list.append({
                        "name": it["name"],
                        "team": it["team"],
                        "skip": reason,
                    })
        items = filtered

    # strip internal field
    for it in items:
        if "_team_id" in it:
            del it["_team_id"]

    # Limit
    items = items[:limit]

    # Build response
    response: Dict = {
        "date": date_str,
        "season": season,
        "items": items,
    }
    if debug_list is not None:
        response["debug"] = debug_list
    if verify or debug:
        # include verify context for visibility
        response["verify_context"] = {
            "not_started_team_count": len(ns_team_ids),
            "names_checked": len(requested_names),
            "cutoffs": {
                "min_season_avg": min_season_avg,
                "min_hitless_games": min_hitless_games,
                "last_n": last_n,
            },
        }
    return response
