# routes/cold_candidates.py
from __future__ import annotations

from fastapi import APIRouter, Query
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone
import httpx

from services.schedule_filters import (
    get_not_started_team_ids,  # uses provider.schedule_for_date(...)
)

router = APIRouter()

STATSAPI_BASE = "https://statsapi.mlb.com/api/v1"


def _normalize_date(date_str: str) -> str:
    if not date_str or date_str.lower() == "today":
        # Use today in America/New_York-like sense; UTC date is fine for schedule filter
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return date_str


async def _statsapi_get(client: httpx.AsyncClient, path: str, params: Dict = None) -> Dict:
    r = await client.get(f"{STATSAPI_BASE}{path}", params=params or {}, headers={"Accept": "application/json"})
    r.raise_for_status()
    return r.json()


async def _search_person(client: httpx.AsyncClient, name: str) -> Optional[Dict]:
    # people?search=NAME
    # Returns {"people": [{id, fullName, currentTeam: {id, name}, ...}]} or empty
    try:
        data = await _statsapi_get(client, "/people", {"search": name})
        people = data.get("people") or []
        return people[0] if people else None
    except httpx.HTTPError:
        return None


async def _get_person(client: httpx.AsyncClient, person_id: int) -> Optional[Dict]:
    try:
        data = await _statsapi_get(client, f"/people/{person_id}")
        people = data.get("people") or []
        return people[0] if people else None
    except httpx.HTTPError:
        return None


async def _season_avg(client: httpx.AsyncClient, person_id: int, season: int) -> Optional[float]:
    # /people/{id}/stats?stats=season&group=hitting&season=YYYY
    try:
        data = await _statsapi_get(
            client,
            f"/people/{person_id}/stats",
            {"stats": "season", "group": "hitting", "season": str(season)},
        )
        splits = ((data.get("stats") or [{}])[0].get("splits")) or []
        if not splits:
            return None
        stat = splits[0].get("stat") or {}
        # battingAverage can be like ".298" or "0.298" or missing
        avg_str = stat.get("battingAverage") or stat.get("avg")
        if not avg_str:
            return None
        try:
            # Handle formats ".298" or "0.298"
            return float(avg_str)
        except ValueError:
            if avg_str.startswith("."):
                return float("0" + avg_str)
            return None
    except httpx.HTTPError:
        return None


async def _hitless_streak(
    client: httpx.AsyncClient,
    person_id: int,
    season: int,
    last_n: int,
    thru_date: str,
) -> int:
    """
    Count consecutive games (working backward from thru_date) with AB>0 and H==0.
    Uses gameLog; stops at first game with a hit or when we leave season/limit.
    """
    try:
        data = await _statsapi_get(
            client,
            f"/people/{person_id}/stats",
            {"stats": "gameLog", "group": "hitting", "season": str(season)},
        )
    except httpx.HTTPError:
        return 0

    splits = ((data.get("stats") or [{}])[0].get("splits")) or []
    if not splits:
        return 0

    # Only consider games on or before thru_date
    thru = datetime.strptime(thru_date, "%Y-%m-%d")
    # Splits are usually chronological; safest: sort by date
    def _to_date(s):
        d = (s.get("date") or "").split("T")[0]
        try:
            return datetime.strptime(d, "%Y-%m-%d")
        except Exception:
            return datetime.min

    relevant = [s for s in splits if _to_date(s) <= thru]
    relevant.sort(key=_to_date, reverse=True)

    streak = 0
    for s in relevant[: max(1, last_n)]:
        stat = s.get("stat") or {}
        ab = int(stat.get("atBats") or stat.get("atBatsAllowed") or 0)
        h = int(stat.get("hits") or 0)
        if ab <= 0:
            # Skip games without an AB; they don't break the streak but also don't count toward it.
            continue
        if h == 0:
            streak += 1
        else:
            break
    return streak


def _team_id_and_name(person: Dict) -> Tuple[Optional[int], str]:
    team = (person or {}).get("currentTeam") or {}
    tid = team.get("id")
    tname = team.get("name") or ""
    return (tid if isinstance(tid, int) else None, tname)


@router.get("/cold_candidates")
async def cold_candidates(
    date: str = Query("today"),
    names: Optional[str] = Query(None, description="Comma-separated player names"),
    last_n: int = Query(7, ge=1, le=30),
    min_season_avg: float = Query(0.26, ge=0.0, le=1.0),
    min_hitless_games: int = Query(1, ge=0, le=30),
    limit: int = Query(50, ge=1, le=200),
    verify: int = Query(0, description="No-op; kept for compat"),
    debug: int = Query(0),
):
    """
    Identify 'cold' hitter candidates for the CURRENT DAY ONLY.
    Filters OUT players whose team's game has already started or finished today.
    If ?names=... is provided, only those names are evaluated.
    """
    date_str = _normalize_date(date)
    season = int(date_str.split("-")[0])

    # --- Schedule gating (NOT-STARTED only) ---------------------------------
    # We call StatsAPI schedule through our own small client (doesn't depend on provider internals).
    not_started_team_ids: set[int] = set()
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            sched = await _statsapi_get(client, "/schedule", {"sportId": 1, "date": date_str})
            # mimic services.schedule_filters logic locally to keep route self-contained
            for day in (sched.get("dates") or []):
                for game in day.get("games", []):
                    status = (game.get("status") or {})
                    code = status.get("statusCode")
                    detailed = (status.get("detailedState") or "").lower()
                    abstract = (status.get("abstractGameState") or "").lower()

                    def not_started():
                        if code in {"S", "P"}:
                            return True
                        if code in {"I", "PW", "PR", "F", "O"}:
                            return False
                        for token in ("scheduled", "preview", "pre-game", "pregame", "pre game"):
                            if token in detailed or token in abstract:
                                return True
                        for token in ("warmup", "in progress", "final", "game over", "live"):
                            if token in detailed or token in abstract:
                                return False
                        return False

                    if not_started():
                        try:
                            hid = game["teams"]["home"]["team"]["id"]
                            aid = game["teams"]["away"]["team"]["id"]
                            if isinstance(hid, int):
                                not_started_team_ids.add(hid)
                            if isinstance(aid, int):
                                not_started_team_ids.add(aid)
                        except Exception:
                            pass
    except Exception:
        # If schedule fetch fails, default to empty => everyone is filtered out unless names specified with manual override
        not_started_team_ids = set()

    items: List[Dict] = []
    dbg: List[Dict] = []

    # Parse names list
    name_list: List[str] = []
    if names:
        # Allow commas and pipe as separators
        raw = [n.strip() for n in names.replace("|", ",").split(",")]
        name_list = [n for n in raw if n]

    async with httpx.AsyncClient(timeout=15.0) as client:
        # If no names given, we return empty list (you are driving candidates by explicit names today)
        candidates = name_list if name_list else []

        for nm in candidates:
            person = await _search_person(client, nm)
            if not person:
                dbg.append({"name": nm, "error": "not found in people search"})
                continue

            team_id, team_name = _team_id_and_name(person)
            if not team_id or team_id not in not_started_team_ids:
                dbg.append({
                    "name": nm,
                    "skip": "no not-started game today (not found on any active roster of a not-started team)"
                })
                continue

            person_id = person.get("id")
            if not isinstance(person_id, int):
                dbg.append({"name": nm, "error": "missing person id"})
                continue

            avg = await _season_avg(client, person_id, season)
            if avg is None:
                dbg.append({"name": nm, "team": team_name, "skip": "no season avg"})
                continue
            if avg < float(min_season_avg):
                dbg.append({"name": nm, "team": team_name, "skip": f"season_avg {avg:.3f} < min {min_season_avg:.3f}"})
                continue

            streak = await _hitless_streak(client, person_id, season, last_n=last_n, thru_date=date_str)
            if streak < int(min_hitless_games):
                dbg.append({"name": nm, "team": team_name, "skip": f"hitless_streak {streak} < min {min_hitless_games}"})
                continue

            items.append({
                "name": person.get("fullName") or nm,
                "team": team_name,
                "season_avg": round(avg, 3),
                "hitless_streak": int(streak),
            })

    # Sort: longest hitless streak first, then higher season avg
    items.sort(key=lambda x: (-x["hitless_streak"], -x["season_avg"]))
    if limit and len(items) > limit:
        items = items[:limit]

    out = {
        "date": date_str,
        "season": season,
        "items": items,
        "debug": (dbg if debug else []),
    }
    return out
