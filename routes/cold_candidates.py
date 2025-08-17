# routes/cold_candidates.py
from fastapi import APIRouter, Query, Request, HTTPException
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, date as date_cls
import pytz
import requests
from urllib.parse import quote_plus

router = APIRouter()

# ------------------
# Local helpers
# ------------------

def _parse_date(d: Optional[str]) -> date_cls:
    tz = pytz.timezone("America/New_York")
    today = datetime.now(tz).date()
    if not d or d.lower() == "today":
        return today
    s = d.lower()
    if s == "yesterday":
        return today - timedelta(days=1)
    if s == "tomorrow":
        return today + timedelta(days=1)
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date; use today|yesterday|tomorrow|YYYY-MM-DD")

def _collect_not_started_team_ids(schedule: Any) -> Tuple[set, Dict[int, str]]:
    """
    Returns (team_ids_not_started, team_id->name map) from an MLB StatsAPI schedule payload.
    Treat 'Scheduled' / 'Pre-Game' / 'Warmup' (coded P/PW/S) / 'Preview' as NOT started.
    """
    team_ids = set()
    team_names: Dict[int, str] = {}

    def consider_game(g: Dict[str, Any]) -> None:
        st = g.get("status", {}) or {}
        code = st.get("codedGameState", "")
        det = st.get("detailedState", "")
        abs_state = st.get("abstractGameState", "")

        not_started = (
            code in ("P", "PW", "S") or
            det in ("Scheduled", "Pre-Game", "Warmup") or
            abs_state == "Preview"
        )
        if not_started:
            for side in ("away", "home"):
                t = (((g.get("teams") or {}).get(side) or {}).get("team") or {})
                tid = t.get("id")
                tname = t.get("name")
                if isinstance(tid, int):
                    team_ids.add(tid)
                    if tname:
                        team_names[tid] = tname

    if isinstance(schedule, dict) and "dates" in schedule:
        for d in schedule.get("dates") or []:
            for g in d.get("games") or []:
                consider_game(g)
    elif isinstance(schedule, dict) and "games" in schedule:
        for g in schedule.get("games") or []:
            consider_game(g)
    elif isinstance(schedule, list):
        for g in schedule:
            if isinstance(g, dict):
                consider_game(g)

    return team_ids, team_names

def _statsapi_people_search(name: str) -> Optional[Dict[str, Any]]:
    """
    Find a person by name using MLB StatsAPI.
    Try '?search=', fall back to '/people/search?names=' if needed.
    """
    base = "https://statsapi.mlb.com/api/v1"
    q = quote_plus(name)

    r = requests.get(f"{base}/people?search={q}", timeout=10)
    if r.status_code == 200:
        data = r.json() or {}
        people = data.get("people") or []
        return people[0] if people else None

    r = requests.get(f"{base}/people/search?names={q}", timeout=10)
    if r.status_code == 200:
        data = r.json() or {}
        people = data.get("people") or data.get("peopleSearchResults") or []
        return people[0] if people else None

    return None

def _statsapi_people_current_team(person_id: int) -> Tuple[Optional[int], Optional[str]]:
    base = "https://statsapi.mlb.com/api/v1"
    r = requests.get(f"{base}/people/{person_id}?hydrate=currentTeam", timeout=10)
    if r.status_code != 200:
        return None, None
    data = r.json() or {}
    current_team = (data.get("people") or [{}])[0].get("currentTeam") or {}
    return current_team.get("id"), current_team.get("name")

def _statsapi_season_avg(person_id: int, year: int) -> Optional[float]:
    base = "https://statsapi.mlb.com/api/v1"
    r = requests.get(f"{base}/people/{person_id}/stats?stats=season&season={year}", timeout=10)
    if r.status_code != 200:
        return None
    data = r.json() or {}
    stats = (data.get("stats") or [])
    if not stats:
        return None
    splits = (stats[0].get("splits") or [])
    if not splits:
        return None
    stat = (splits[0].get("stat") or {})
    avg_str = stat.get("avg")
    if not avg_str:
        return None
    try:
        return float(avg_str)
    except Exception:
        return None

def _statsapi_hitless_streak(person_id: int, year: int, on_or_before: date_cls) -> int:
    """
    Count consecutive games (STRICTLY BEFORE 'on_or_before') with AB>0 and H==0.
    - Excludes any game whose date == on_or_before (prevents live same-day games from affecting the streak)
    - AB==0 games are ignored (do not break or extend)
    """
    base = "https://statsapi.mlb.com/api/v1"
    r = requests.get(f"{base}/people/{person_id}/stats?stats=gameLog&season={year}", timeout=10)
    if r.status_code != 200:
        return 0
    data = r.json() or {}
    stats = (data.get("stats") or [])
    if not stats:
        return 0
    splits = (stats[0].get("splits") or [])

    def split_date(s):
        try:
            return datetime.strptime(s.get("date", ""), "%Y-%m-%d")
        except Exception:
            return datetime.min

    splits_sorted = sorted(splits, key=split_date, reverse=True)

    streak = 0
    for s in splits_sorted:
        try:
            gdate = datetime.strptime(s.get("date", ""), "%Y-%m-%d").date()
        except Exception:
            continue

        # HARD EXCLUSION: do NOT count games on the analysis date (prevents live games from leaking in)
        if gdate >= on_or_before:
            continue

        st = s.get("stat") or {}
        ab = st.get("atBats") or 0
        h = st.get("hits") or 0

        if ab > 0:
            if int(h) == 0:
                streak += 1
            else:
                break
        # ab == 0 -> ignore entirely

    return streak

# ------------------
# Route
# ------------------

@router.get("/cold_candidates", tags=["cold"], operation_id="cold_candidates")
def cold_candidates(
    request: Request,
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    names: Optional[str] = Query(None, description="Comma-separated list of player names"),
    min_season_avg: float = Query(0.270),
    min_hitless_games: int = Query(1, ge=1),
    last_n: int = Query(1, ge=1, description="Kept for parity; streak itself looks across recent games pre-date"),
    limit: int = Query(50, ge=1, le=200),
    verify: int = Query(0, ge=0, le=1),
    debug: int = Query(0, ge=0, le=1),
):
    """
    Current-day cold-hitter picker:
      • Only teams whose game has NOT started yet today
      • Season AVG >= min_season_avg
      • Most recent COMPLETED game with an AB was hitless (streak >= min_hitless_games)
      • Live/same-day games are excluded from the streak math
    """
    the_date = _parse_date(date)
    season = the_date.year

    provider = request.app.state.provider
    if provider is None:
        raise HTTPException(status_code=503, detail="Provider not loaded")

    # 1) Get today's schedule; build NOT-started team set
    schedule = None
    try:
        try:
            schedule = provider.schedule_for_date(date=the_date)
        except TypeError:
            schedule = provider.schedule_for_date(date_str=the_date.isoformat())
    except Exception:
        schedule = None
    not_started_team_ids, team_name_map = _collect_not_started_team_ids(schedule or {})

    out_items: List[Dict[str, Any]] = []
    dbg: List[Dict[str, Any]] = []

    if not names:
        return {"date": the_date.isoformat(), "season": season, "items": [], "debug": [{"note": "no names provided"}]}

    for raw in names.split(","):
        name = raw.strip()
        if not name:
            continue
        person = _statsapi_people_search(name)
        if not person:
            dbg.append({"name": name, "error": "not found in people search"})
            continue

        pid = person.get("id")
        if not isinstance(pid, int):
            dbg.append({"name": name, "error": "missing id from search"})
            continue

        team_id, team_name = _statsapi_people_current_team(pid)
        if not team_id:
            dbg.append({"name": name, "error": "currentTeam unavailable"})
            continue

        # HARD GATE: must be on a team that hasn't started yet today
        if team_id not in not_started_team_ids:
            dbg.append({"name": name, "skip": "no not-started game today (current team not in not-started set)"})
            continue

        season_avg = _statsapi_season_avg(pid, season) or 0.0
        if season_avg < float(min_season_avg):
            dbg.append({"name": name, "team": team_name or "", "skip": f"season_avg {season_avg:.3f} < min {float(min_season_avg):.3f}"})
            continue

        hitless_streak = _statsapi_hitless_streak(pid, season, the_date)
        if hitless_streak < int(min_hitless_games):
            dbg.append({"name": name, "team": team_name or "", "skip": f"hitless_streak {hitless_streak} < min {int(min_hitless_games)}"})
            continue

        out_items.append({
            "name": person.get("fullName") or name,
            "team": team_name or "",
            "season_avg": round(season_avg, 3),
            "hitless_streak": int(hitless_streak),
        })

        if len(out_items) >= limit:
            break

    resp: Dict[str, Any] = {
        "date": the_date.isoformat(),
        "season": season,
        "items": out_items,
        "debug": dbg if debug == 1 else [],
    }

    if verify == 1:
        resp["verify"] = {
            "not_started_team_count": len(not_started_team_ids),
            "not_started_team_ids": sorted(list(not_started_team_ids))[:25],
        }

    return resp
