# routes/league_scan.py
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel
from datetime import datetime, timedelta, date, time
from zoneinfo import ZoneInfo
import requests

router = APIRouter()
ET = ZoneInfo("America/New_York")

# ---------- request model ----------
class LeagueScanRequest(BaseModel):
    date: str = "today"          # "today" | "yesterday" | "YYYY-MM-DD"
    debug: int | None = 1
    only_upcoming: bool = True   # <-- default: filter to games that haven't started (ET)
    min_upcoming_games: int = 1  # <-- if fewer than this remain today, auto-try tomorrow

# ---------- helpers ----------
def _now_et() -> datetime:
    return datetime.now(ET)

def _normalize_date(s: str) -> str:
    today = _now_et().date()
    if s == "today":
        return today.isoformat()
    if s == "yesterday":
        return (today - timedelta(days=1)).isoformat()
    return s  # assume ISO already

def _is_late_night_et() -> bool:
    now = _now_et().time()
    # After 9:30pm ET or before 5:00am ET, allow fallback behavior
    return (now >= time(21, 30)) or (now < time(5, 0))

def _post_json(base: str, path: str, body: dict, timeout: int = 15):
    try:
        r = requests.post(base + path, json=body, timeout=timeout)
        if r.status_code >= 400:
            return None, r.status_code
        return r.json(), r.status_code
    except Exception:
        return None, 599

def _get_json(base: str, path: str, timeout: int = 15):
    try:
        r = requests.get(base + path, timeout=timeout)
        if r.status_code >= 400:
            return None, r.status_code
        return r.json(), r.status_code
    except Exception:
        return None, 599

def _extract_games(schedule_json: dict) -> list:
    if not schedule_json:
        return []
    if isinstance(schedule_json, dict):
        return (
            schedule_json.get("games")
            or schedule_json.get("matchups")
            or schedule_json.get("data", {}).get("games")
            or []
        )
    return []

def _parse_et_time_str(et_time_str: str | None) -> time | None:
    """
    Accepts 'HH:MM' (24h or 12h with AM/PM), 'HH:MM:SS', or None.
    """
    if not et_time_str or not isinstance(et_time_str, str):
        return None
    s = et_time_str.strip()
    # common formats
    for fmt in ("%H:%M", "%H:%M:%S", "%I:%M %p", "%I:%M:%S %p"):
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            continue
    return None

def _game_has_started(game: dict, now_et: datetime) -> bool:
    """
    Returns True if the game is in-progress or final, or if scheduled time <= now.
    Tries status first; falls back to parsing 'et_time' clock.
    """
    status = (game.get("status") or game.get("game_status") or "").strip().lower()
    if status in {"in progress", "live", "final", "completed", "game over", "end"}:
        return True

    # if explicit pregame status exists
    if status in {"scheduled", "pregame", "preview"}:
        # still check time to be safe
        pass

    # parse provided ET time field(s)
    et_time_str = game.get("et_time") or game.get("game_time_et") or game.get("start_time_et")
    t = _parse_et_time_str(et_time_str)
    if t is None:
        # Try ISO datetime in ET if present
        iso_dt = game.get("game_datetime_et") or game.get("start_datetime_et")
        if iso_dt:
            try:
                dt_et = datetime.fromisoformat(iso_dt)
                # if dt without tz, assume ET
                if dt_et.tzinfo is None:
                    dt_et = dt_et.replace(tzinfo=ET)
                return now_et >= dt_et
            except Exception:
                return False
        # No reliable time—assume not started unless marked otherwise
        return status not in {"scheduled", "pregame", "preview"}

    # Compare today's date+time in ET
    sched_dt = datetime.combine(now_et.date(), t, tzinfo=ET)
    return now_et >= sched_dt

def _filter_upcoming_games(games: list) -> list:
    now = _now_et()
    upcoming = []
    for g in games:
        if not _game_has_started(g, now):
            upcoming.append(g)
    return upcoming

def _teams_from_games(games: list) -> set[str]:
    teams = set()
    for g in games:
        # Accept multiple key variants
        home = g.get("home_name") or g.get("home_team") or g.get("homeTeam") or g.get("home")
        away = g.get("away_name") or g.get("away_team") or g.get("awayTeam") or g.get("away")
        if isinstance(home, dict):
            home = home.get("name") or home.get("abbr") or home.get("team_name")
        if isinstance(away, dict):
            away = away.get("name") or away.get("abbr") or away.get("team_name")
        if home:
            teams.add(str(home))
        if away:
            teams.add(str(away))
    return teams

def _filter_players_by_teams(players: list, team_names: set[str]) -> list:
    if not players or not team_names:
        return players or []
    out = []
    for p in players:
        tn = p.get("team_name") or p.get("team") or p.get("teamAbbr")
        if tn and str(tn) in team_names:
            out.append(p)
    return out

def _try_smoke(base: str, d: str, debug: int | None):
    # Full league scan first
    smoke, _ = _post_json(base, "/smoke_post", {"date": d, "max_teams": 30, "per_team": 9, "debug": debug})
    if smoke and isinstance(smoke, dict):
        return smoke
    # Sampler fallback
    smoke_samples, _ = _post_json(base, "/smoke_post", {"date": d, "samples": 3, "debug": debug})
    if smoke_samples and isinstance(smoke_samples, dict):
        return smoke_samples
    return None

def _compose_from_parts(base: str, d: str, debug: int | None):
    # Try diag → schedule_post → schedule_get
    sched, _ = _post_json(base, "/diag_schedule_post", {"date": d})
    if not sched:
        sched, _ = _post_json(base, "/schedule_post", {"date": d})
    if not sched:
        sched, _ = _get_json(base, f"/schedule_get?date={d}")

    games = _extract_games(sched)

    hot, _ = _post_json(base, "/hot_streak_hitters_post", {"date": d, "debug": debug})
    cold, _ = _post_json(base, "/cold_streak_hitters_post", {"date": d, "debug": debug})

    return {
        "games": games or [],
        "hot": hot or [],
        "cold": cold or [],
    }

def _unify_payload(d: str, games: list, hot: list, cold: list, source: str, smoke_debug: dict | None = None):
    return {
        "date": d,
        "counts": {
            "matchups": len(games),
            "hot_hitters": len(hot),
            "cold_hitters": len(cold),
        },
        "matchups": games,
        "hot_hitters": hot,
        "cold_hitters": cold,
        "debug": {
            "source": source,
            "counts": (smoke_debug or {}).get("counts", {}) if smoke_debug else {
                "matchups": len(games),
                "hot_hitters": len(hot),
                "cold_hitters": len(cold),
            }
        }
    }

# ---------- main route ----------
@router.post("/league_scan_post")
def league_scan(req: LeagueScanRequest, request: Request):
    """
    One call that:
      - Normalizes date in ET
      - Tries /smoke_post; falls back to composed (schedule + hot + cold)
      - Filters to upcoming games/players if only_upcoming=True
      - If upcoming < min_upcoming_games, auto-tries tomorrow
      - Late-night ET also allows today→yesterday when helpful
    """
    base = str(request.base_url).rstrip("/")  # e.g., https://your-service.onrender.com
    primary = _normalize_date(req.date)

    # Build an ordered list of dates to try
    dates_to_try: list[str] = [primary]

    # Late-night allowance: today→yesterday for data availability
    if _is_late_night_et():
        yday = (date.fromisoformat(primary) - timedelta(days=1)).isoformat()
        if yday not in dates_to_try:
            dates_to_try.append(yday)

    # User-requested “if nothing left today, try tomorrow”
    tomorrow = (date.fromisoformat(primary) + timedelta(days=1)).isoformat()

    last_detail = None

    # We may do two passes:
    # 1) Pass for today/yesterday candidates
    # 2) If upcoming filter knocks everything out and min_upcoming_games not met, try tomorrow
    def run_for_date(d: str):
        # --- 1) Try SMOKE
        smoke = _try_smoke(base, d, req.debug)
        if smoke:
            games = smoke.get("matchups", []) or []
            hot = smoke.get("hot_hitters", []) or []
            cold = smoke.get("cold_hitters", []) or []

            # Optional upcoming filter
            if req.only_upcoming:
                games_up = _filter_upcoming_games(games)
                teams_scope = _teams_from_games(games_up)
                hot = _filter_players_by_teams(hot, teams_scope)
                cold = _filter_players_by_teams(cold, teams_scope)
                games = games_up

            return _unify_payload(d, games, hot, cold, "smoke", smoke_debug=smoke.get("debug", {}))

        # --- 2) Compose from parts
        parts = _compose_from_parts(base, d, req.debug)
        games = parts["games"]
        hot = parts["hot"]
        cold = parts["cold"]

        if req.only_upcoming:
            games_up = _filter_upcoming_games(games)
            teams_scope = _teams_from_games(games_up)
            hot = _filter_players_by_teams(hot, teams_scope)
            cold = _filter_players_by_teams(cold, teams_scope)
            games = games_up

        if games:
            return _unify_payload(d, games, hot, cold, "composed")

        return None

    # Pass 1: today / (late-night) yesterday
    for d in dates_to_try:
        result = run_for_date(d)
        if result:
            # If min_upcoming not satisfied and tomorrow is available, try tomorrow
            if req.only_upcoming and result["counts"]["matchups"] < req.min_upcoming_games:
                # Try tomorrow; if tomorrow has nothing either, return what we have
                res_tomorrow = run_for_date(tomorrow)
                if res_tomorrow and res_tomorrow["counts"]["matchups"] >= req.min_upcoming_games:
                    return res_tomorrow
            return result

        last_detail = f"no upcoming (or no data) for {d}"

    # Pass 2: explicit tomorrow fallback if today/yesterday failed
    res_tomorrow = run_for_date(tomorrow)
    if res_tomorrow:
        return res_tomorrow

    # Nothing usable
    raise HTTPException(
        status_code=404,
        detail={
            "message": "No targetable upcoming games/players found for requested or fallback dates.",
            "dates_tried": dates_to_try + [tomorrow],
            "last_detail": last_detail,
            "only_upcoming": req.only_upcoming,
            "min_upcoming_games": req.min_upcoming_games,
        },
    )
