# routes/cold_candidates.py
from __future__ import annotations

import unicodedata
from datetime import datetime, date as date_cls, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests
from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter()

# ---------- small local utils (kept self-contained so you can drop-in overwrite) ----------

def _ny_date_today() -> date_cls:
    tz = pytz.timezone("America/New_York")
    return datetime.now(tz).date()

def _parse_date(d: Optional[str]) -> date_cls:
    if not d or d.lower() == "today":
        return _ny_date_today()
    s = d.lower()
    if s == "yesterday":
        return _ny_date_today() - timedelta(days=1)
    if s == "tomorrow":
        return _ny_date_today() + timedelta(days=1)
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date; use today|yesterday|tomorrow|YYYY-MM-DD")

def _norm(s: str) -> str:
    """lowercase & strip accents to help with exact-match selection"""
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower().strip()

def _safe_get(d: dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def _float_or_none(x) -> Optional[float]:
    try:
        if x is None or x == "":
            return None
        return float(x)
    except Exception:
        return None

def _collect_not_started_team_ids(schedule_json: Any) -> Tuple[set, Dict[int, str]]:
    """
    Accepts the raw schedule JSON from provider.schedule_for_date().
    Returns:
      (team_ids_not_started, {team_id: team_name})
    We classify "not started" liberally based on several fields MLB uses.
    """
    not_started = set()
    names: Dict[int, str] = {}

    # common shapes:
    # - {"dates":[{"games":[{... game ...}]}]}
    # - already flattened list of games
    games: List[dict] = []

    if isinstance(schedule_json, dict) and "dates" in schedule_json:
        for d in schedule_json.get("dates", []):
            games.extend(d.get("games", []))
    elif isinstance(schedule_json, list):
        games.extend([g for g in schedule_json if isinstance(g, dict)])
    elif isinstance(schedule_json, dict) and "games" in schedule_json:
        games.extend(schedule_json.get("games") or [])

    def is_not_started(game: dict) -> bool:
        st = game.get("status") or {}
        coded = st.get("codedGameState", "")
        abst = st.get("abstractGameState", "")
        detailed = st.get("detailedState", "")
        # "Preview" / "Pre-Game" / "Scheduled" / "Warmup"
        if coded in ("P", "PW", "S"):
            return True
        if abst in ("Preview", "Pre-Game"):
            return True
        if detailed in ("Pre-Game", "Preview", "Scheduled", "Warmup"):
            return True
        return False

    for g in games:
        if not is_not_started(g):
            continue
        teams = g.get("teams") or {}
        for side in ("away", "home"):
            team = _safe_get(teams, side, "team", default={})
            tid = team.get("id")
            tname = team.get("name")
            if isinstance(tid, int):
                not_started.add(tid)
                if tname:
                    names[tid] = tname

    return not_started, names

# ---------------- MLB Stats API helpers (direct) ----------------

_STATS_BASE = "https://statsapi.mlb.com/api/v1"

def _people_search(name: str) -> List[dict]:
    """
    Try multiple endpoints to avoid sporadic 400s on /people?search=...
    """
    # 1) primary attempt
    urls = [
        f"{_STATS_BASE}/people?search={requests.utils.quote(name)}",
        f"{_STATS_BASE}/people/search?names={requests.utils.quote(name)}",
    ]
    last_err = None
    for url in urls:
        try:
            r = requests.get(url, timeout=12)
            r.raise_for_status()
            data = r.json() or {}
            people = data.get("people") or data.get("results") or []
            if isinstance(people, list):
                return people
        except Exception as e:
            last_err = e
            continue
    # final fallback: return empty, caller will record debug
    return []

def _pick_person(people: List[dict], want_name: str) -> Optional[dict]:
    if not people:
        return None
    # exact (accent-insensitive) full name match first
    want = _norm(want_name)
    for p in people:
        fn = _norm(p.get("fullName", "") or p.get("firstLastName", "") or "")
        if fn == want:
            return p
    # otherwise first
    return people[0]

def _get_season_avg(person_id: int, season_year: int) -> Optional[float]:
    url = f"{_STATS_BASE}/people/{person_id}/stats?stats=season&group=hitting&season={season_year}"
    try:
        r = requests.get(url, timeout=12)
        r.raise_for_status()
        data = r.json() or {}
        splits = _safe_get(data, "stats", 0, "splits", default=[]) or []
        if not splits:
            return None
        stat = splits[0].get("stat", {}) or {}
        # prefer "avg", else compute
        avg = _float_or_none(stat.get("avg"))
        if avg is not None:
            return avg
        hits = _float_or_none(stat.get("hits"))
        ab = _float_or_none(stat.get("atBats"))
        if hits is not None and ab:
            return hits / ab if ab > 0 else 0.0
    except Exception:
        pass
    return None

def _get_team_id_and_name(person: dict) -> Tuple[Optional[int], Optional[str]]:
    team = person.get("currentTeam") or {}
    tid = team.get("id")
    tname = team.get("name")
    return (tid if isinstance(tid, int) else None, tname)

def _fetch_game_log(person_id: int, season_year: int) -> List[dict]:
    """
    Entire year game log; caller filters by date & AB>0.
    """
    url = f"{_STATS_BASE}/people/{person_id}/stats?stats=gameLog&group=hitting&season={season_year}"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json() or {}
        return _safe_get(data, "stats", 0, "splits", default=[]) or []
    except Exception:
        return []

def _iso_to_date(s: str) -> Optional[date_cls]:
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None

def _compute_hitless_streak_before(person_id: int, cutoff_date: date_cls, last_n: int) -> int:
    """
    Count consecutive 0-hit games (with AB>0) immediately BEFORE cutoff_date.
    Never includes any game on cutoff_date itself (live/final).
    Only examines up to 'last_n' most recent qualifying games.
    """
    rows = _fetch_game_log(person_id, cutoff_date.year)
    # build (gameDate, hits, ab)
    games: List[Tuple[date_cls, int, int]] = []
    for s in rows:
        gd = _iso_to_date(s.get("date", ""))
        if not gd or gd >= cutoff_date:
            # exclude today's games entirely (regardless of in-progress/final)
            continue
        stat = s.get("stat", {}) or {}
        ab = int(_float_or_none(stat.get("atBats")) or 0)
        hits = int(_float_or_none(stat.get("hits")) or 0)
        if ab > 0:
            games.append((gd, hits, ab))

    # sort newest first
    games.sort(key=lambda x: x[0], reverse=True)
    # keep last_n most recent
    games = games[: last_n]

    # count consecutive 0-hit from most recent backward
    streak = 0
    for _, hits, _ in games:
        if hits == 0:
            streak += 1
        else:
            break
    return streak

# ---------- core route ----------

@router.get("/cold_candidates", tags=["candidates"])
def cold_candidates(
    request: Request,
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    names: Optional[str] = Query(None, description="Comma-separated player names"),
    min_season_avg: float = Query(0.260, ge=0.0, le=1.000),
    min_hitless_games: int = Query(1, ge=0),
    last_n: int = Query(7, ge=1, le=30),
    limit: int = Query(50, ge=1, le=200),
    verify: int = Query(0, ge=0, le=1),
    debug: int = Query(0, ge=0, le=1),
):
    """
    Returns players meeting your "cold" logic for the CURRENT DAY, with a guard:
    - Only include players whose TEAM has NOT STARTED their game today.
    - "Hitless streak" counts consecutive 0-hit games (AB>0) immediately BEFORE today (excludes today).
    - Season AVG is their current-season batting average.
    """
    the_date = _parse_date(date)
    season = the_date.year

    # ---- load provider from app.state ----
    provider = getattr(request.app.state, "provider", None)
    if provider is None:
        raise HTTPException(status_code=503, detail="Provider not loaded")

    sched_fn = getattr(provider, "schedule_for_date", None)
    if not callable(sched_fn):
        raise HTTPException(status_code=501, detail="Provider does not implement schedule_for_date()")

    # ---- call schedule_for_date with a signature-safe shim (fixes your 500) ----
    try:
        schedule = sched_fn(the_date)
    except TypeError:
        try:
            schedule = sched_fn(date=the_date)
        except TypeError:
            schedule = sched_fn(the_date.isoformat())

    not_started_ids, team_names = _collect_not_started_team_ids(schedule)

    # ---- names input handling ----
    out_items: List[Dict[str, Any]] = []
    dbg: List[Dict[str, Any]] = []

    if not names or not names.strip():
        # no names provided (your earlier debug log showed this)
        return {
            "date": the_date.isoformat(),
            "season": season,
            "items": [],
            "debug": [{"note": "no names provided"}] if debug else [],
        }

    name_list = [n.strip() for n in names.split(",") if n.strip()]
    # hard cap to protect
    name_list = name_list[: max(1, min(limit, 200))]

    for raw_name in name_list:
        # 1) find player
        people = _people_search(raw_name)
        if not people:
            if debug:
                dbg.append({"name": raw_name, "error": "not found in people search"})
            continue
        person = _pick_person(people, raw_name)
        if not person:
            if debug:
                dbg.append({"name": raw_name, "error": "ambiguous or not found"})
            continue

        pid = person.get("id")
        if not isinstance(pid, int):
            if debug:
                dbg.append({"name": raw_name, "error": "invalid person id"})
            continue

        # team gate: must be on a team whose game hasn't started today
        team_id, team_name = _get_team_id_and_name(person)
        if not team_id or team_id not in not_started_ids:
            if debug:
                reason = "no not-started game today (not found on any active roster of a not-started team)"
                dbg.append({"name": raw_name, "skip": reason})
            continue

        # 2) season AVG
        avg = _get_season_avg(pid, season)
        if avg is None:
            if debug:
                dbg.append({"name": raw_name, "team": team_names.get(team_id) or team_name or "", "error": "no season avg"})
            continue

        # 3) hitless streak BEFORE today (exclude any game on the_date)
        streak = _compute_hitless_streak_before(pid, the_date, last_n=last_n)

        # 4) apply thresholds
        if avg < min_season_avg:
            if debug:
                dbg.append({
                    "name": raw_name,
                    "team": team_names.get(team_id) or team_name or "",
                    "skip": f"season_avg {avg:.3f} < min {min_season_avg:.3f}"
                })
            continue
        if streak < min_hitless_games:
            if debug:
                dbg.append({
                    "name": raw_name,
                    "team": team_names.get(team_id) or team_name or "",
                    "skip": f"hitless_streak {streak} < min {min_hitless_games}"
                })
            continue

        out_items.append({
            "name": person.get("fullName") or raw_name,
            "team": team_names.get(team_id) or team_name or "",
            "season_avg": round(avg, 3),
            "hitless_streak": streak,
        })

    # sort: longest streak desc, then higher avg desc
    out_items.sort(key=lambda x: (x["hitless_streak"], x["season_avg"]), reverse=True)
    out_items = out_items[:limit]

    response = {
        "date": the_date.isoformat(),
        "season": season,
        "items": out_items,
        "debug": dbg if debug else [],
    }

    # Optional verification echo (kept minimal to match your earlier usage)
    if verify:
        response["verify_context"] = {
            "not_started_team_count": len(not_started_ids),
            "names_checked": len(name_list),
            "cutoffs": {
                "min_season_avg": min_season_avg,
                "min_hitless_games": min_hitless_games,
                "last_n": last_n,
            }
        }

    return response
