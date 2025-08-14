from fastapi import APIRouter, HTTPException, Request, Query
from pydantic import BaseModel
from datetime import datetime, timedelta, date as date_cls
from typing import Any, Dict, List, Optional
import pytz
import requests
import time

router = APIRouter()
ET_TZ = pytz.timezone("America/New_York")

# ---------- Models ----------
class LeagueScanReq(BaseModel):
    date: Optional[str] = None
    top_n: int = 15
    debug: int = 0

class LeagueScanResp(BaseModel):
    date: str
    counts: Dict[str, int]
    top: Dict[str, List[Dict[str, Any]]]
    matchups: List[Dict[str, Any]]
    debug: Optional[Dict[str, Any]] = None

# ---------- Helpers ----------
def _now_et() -> datetime:
    return datetime.now(ET_TZ)

def _parse_date(d: Optional[str]) -> date_cls:
    today = _now_et().date()
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

def _mlb_schedule(date_obj: date_cls, retries: int = 2, timeout_s: float = 8.0) -> List[Dict[str, Any]]:
    """Fetch MLB schedule for date from StatsAPI, normalize to ET, with small retry."""
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date_obj.isoformat()}"
    last_exc: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, timeout=timeout_s)
            if r.status_code >= 500:
                # transient upstream error
                raise requests.HTTPError(f"upstream {r.status_code}")
            if r.status_code == 404:
                # treat as empty slate
                return []
            r.raise_for_status()
            js = r.json()
            break
        except Exception as e:
            last_exc = e
            if attempt < retries:
                time.sleep(0.6)
                continue
            # final failure -> empty slate; do not 5xx this endpoint
            return []
    games: List[Dict[str, Any]] = []
    for day in js.get("dates", []):
        for g in day.get("games", []):
            status = (g.get("status", {}).get("detailedState") or g.get("status", {}).get("abstractGameState") or "").lower()
            gd = g.get("gameDate")  # UTC ISO
            dt_et = None
            if isinstance(gd, str):
                try:
                    dt_et = datetime.fromisoformat(gd.replace("Z", "+00:00")).astimezone(ET_TZ)
                except Exception:
                    dt_et = None
            home = g.get("teams", {}).get("home", {}).get("team", {}).get("name")
            away = g.get("teams", {}).get("away", {}).get("team", {}).get("name")
            venue = g.get("venue", {}).get("name")
            prob_home = g.get("teams", {}).get("home", {}).get("probablePitcher", {}) or {}
            prob_away = g.get("teams", {}).get("away", {}).get("probablePitcher", {}) or {}
            ph = prob_home.get("fullName") or prob_home.get("name")
            pa = prob_away.get("fullName") or prob_away.get("name")
            et_time = dt_et.strftime("%I:%M %p").lstrip("0") if dt_et else None
            games.append({
                "home_name": home,
                "away_name": away,
                "game_datetime_et": dt_et.isoformat() if dt_et else None,
                "status": status,     # e.g., "pre-game", "in progress", "final"
                "venue": venue,
                "et_time": et_time,
                "probables": {"home": ph, "away": pa},
            })
    return games

def _has_started(game: Dict[str, Any], now_dt: datetime) -> bool:
    status = (game.get("status") or "").lower()
    if status in {"in progress", "final", "completed", "game over"}:
        return True
    iso = game.get("game_datetime_et")
    if not iso:
        return False
    try:
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = ET_TZ.localize(dt)
        return now_dt >= dt
    except Exception:
        return False

def _upcoming_only(games: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    now = _now_et()
    return [g for g in games if not _has_started(g, now)]

def _teamscope(games: List[Dict[str, Any]]) -> set:
    s = set()
    for g in games:
        if g.get("home_name"): s.add(str(g["home_name"]))
        if g.get("away_name"): s.add(str(g["away_name"]))
    return s

def _filter_players_to_scope(players: List[Dict[str, Any]], scope: set) -> List[Dict[str, Any]]:
    if not players or not scope:
        return players or []
    out: List[Dict[str, Any]] = []
    for p in players:
        tn = p.get("team_name") or p.get("team") or p.get("teamAbbr")
        if tn and str(tn) in scope:
            out.append(p)
    return out

def _provider_call(request: Request, name: str, **kwargs):
    provider = getattr(request.app.state, "provider", None)
    if provider is None:
        # do not raise; let caller swallow — we want 200 with empty lists, not 5xx
        raise RuntimeError("provider-not-loaded")
    fn = getattr(provider, name, None)
    if not callable(fn):
        raise RuntimeError(f"provider-missing-method:{name}")
    return fn(**kwargs)

def _run_scan(request: Request, d: date_cls, top_n: int, debug_flag: bool) -> Dict[str, Any]:
    sched = _mlb_schedule(d)
    upcoming = _upcoming_only(sched)
    scope = _teamscope(upcoming)

    # provider calls (hot/cold); swallow errors so endpoint never 5xx
    try:
        hot = _provider_call(request, "hot_streak_hitters",
                             date=d, min_avg=0.280, games=3,
                             require_hit_each=True, debug=debug_flag)
    except Exception:
        hot = []
    try:
        cold = _provider_call(request, "cold_streak_hitters",
                              date=d, min_avg=0.275, games=2,
                              require_zero_hit_each=True, debug=debug_flag)
    except Exception:
        cold = []

    hot_f = _filter_players_to_scope(hot, scope)
    cold_f = _filter_players_to_scope(cold, scope)
    if len(hot_f) > top_n: hot_f = hot_f[:top_n]
    if len(cold_f) > top_n: cold_f = cold_f[:top_n]

    return {
        "date": d.isoformat(),
        "counts": {
            "matchups": len(upcoming),
            "hot_hitters": len(hot_f),
            "cold_hitters": len(cold_f),
        },
        "top": {
            "hot_hitters": hot_f,
            "cold_hitters": cold_f,
        },
        "matchups": upcoming,  # home/away, et_time, venue, probables
        "debug": {
            "source": "statsapi_schedule + provider_hot_cold",
            "upcoming_filter": True,
            "requested_top_n": top_n,
        } if debug_flag else None,
    }

# ---------- POST (Action) ----------
@router.post("/league_scan_post", response_model=LeagueScanResp, operation_id="league_scan_post")
def league_scan_post(req: LeagueScanReq, request: Request):
    """
    League scan that DOES NOT depend on provider.slate_scan():
      - Schedule from MLB StatsAPI (ET)
      - Upcoming-only filter
      - Auto-fallback to tomorrow if no upcoming today
      - Hot/Cold hitters from provider, filtered to teams still to play
    """
    primary_date = _parse_date(req.date)
    tomorrow = primary_date + timedelta(days=1)
    top_n = int(req.top_n) if req.top_n and req.top_n > 0 else 15
    debug_flag = bool(req.debug)

    out_today = _run_scan(request, primary_date, top_n, debug_flag)
    if out_today["counts"]["matchups"] >= 1:
        return out_today

    out_tmr = _run_scan(request, tomorrow, top_n, debug_flag)
    if out_tmr["counts"]["matchups"] >= 1:
        if debug_flag:
            if out_tmr.get("debug") is None:
                out_tmr["debug"] = {}
            out_tmr["debug"]["fallback"] = "tomorrow_used_no_upcoming_today"
        return out_tmr

    # No upcoming today or tomorrow; return today's (likely empty scope) with reason
    if debug_flag:
        if out_today.get("debug") is None:
            out_today["debug"] = {}
        out_today["debug"]["fallback"] = "no_upcoming_today_or_tomorrow"
    return out_today

# ---------- GET (browser-verifiable mirror) ----------
@router.get("/league_scan_get", response_model=LeagueScanResp, operation_id="league_scan_get")
def league_scan_get(
    request: Request,
    date: Optional[str] = Query(None),
    top_n: int = Query(15, ge=1, le=100),
    debug: int = Query(0, ge=0, le=1),
):
    req = LeagueScanReq(date=date, top_n=top_n, debug=debug)
    return league_scan_post(req, request)
