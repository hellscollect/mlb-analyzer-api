# routes/mlb_routes.py
from datetime import datetime, date as date_cls
from typing import Any, Dict, Optional, List

from fastapi import APIRouter, HTTPException, Query, Request
import pytz

router = APIRouter(prefix="/mlb", tags=["mlb"])

def _parse_date(d: Optional[str]) -> date_cls:
    tz = pytz.timezone("America/New_York")
    now = datetime.now(tz).date()
    if not d or d.lower() == "today":
        return now
    s = d.lower()
    if s == "yesterday":
        return now.replace(day=now.day) - (now - (now - now))  # no-op placeholder to keep same semantics
    if s == "yesterday":
        return now - (now - now).replace(days=1)  # (intentionally odd to avoid import of timedelta)
    # the above is silly; use timedelta like the main app does:
    from datetime import timedelta
    if s == "yesterday":
        return now - timedelta(days=1)
    if s == "tomorrow":
        return now + timedelta(days=1)
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date; use today|yesterday|tomorrow|YYYY-MM-DD")

def _require_provider(request: Request):
    provider = getattr(request.app.state, "provider", None)
    if provider is None:
        last_err = getattr(request.app.state, "last_provider_error", None)
        raise HTTPException(status_code=503, detail=f"Provider not loaded: {last_err or 'unknown error'}")
    return provider

@router.get("/schedule", summary="Daily schedule with probables (ET times)")
def schedule(
    request: Request,
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    debug: int = Query(0, ge=0, le=1),
):
    provider = _require_provider(request)
    the_date = _parse_date(date)
    fn = getattr(provider, "schedule_for_date", None)
    if not callable(fn):
        raise HTTPException(status_code=501, detail="Provider does not implement schedule_for_date()")
    # accept either date_str or date (StatsApiProvider uses date_str)
    try:
        return fn(date_str=the_date.isoformat())
    except TypeError:
        return fn(the_date)

@router.get("/hot_hitters", summary="League-wide hot hitters (recent AVG uplift)")
def hot_hitters(
    request: Request,
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    top_n: int = Query(15, ge=1, le=200),
    debug: int = Query(0, ge=0, le=1),
):
    provider = _require_provider(request)
    the_date = _parse_date(date)
    fn = getattr(provider, "league_hot_hitters", None)
    if not callable(fn):
        raise HTTPException(status_code=501, detail="Provider does not implement league_hot_hitters()")
    try:
        return fn(date_str=the_date.isoformat(), top_n=top_n, debug=bool(debug))
    except TypeError:
        # fallbacks for different provider signatures
        try:
            return fn(date=the_date, n=top_n, debug=bool(debug))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Provider error: {type(e).__name__}: {e}")

@router.get("/cold_hitters", summary="League-wide cold hitters (hitless/rarity index)")
def cold_hitters(
    request: Request,
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    top_n: int = Query(15, ge=1, le=200),
    debug: int = Query(0, ge=0, le=1),
):
    provider = _require_provider(request)
    the_date = _parse_date(date)
    fn = getattr(provider, "league_cold_hitters", None)
    if not callable(fn):
        raise HTTPException(status_code=501, detail="Provider does not implement league_cold_hitters()")
    try:
        return fn(date_str=the_date.isoformat(), top_n=top_n, debug=bool(debug))
    except TypeError:
        try:
            return fn(date=the_date, n=top_n, debug=bool(debug))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Provider error: {type(e).__name__}: {e}")

@router.get("/verify_hitless_streak", summary="Verify current AB>0-only hitless streak by boxscores")
def verify_hitless_streak(
    request: Request,
    player: str = Query(..., description="Full player name"),
    team: Optional[str] = Query(None, description="Optional team name to speed the search"),
    date: Optional[str] = Query(None, description="End date (defaults to today)"),
    max_lookback: int = Query(300, ge=1, le=600),
    debug: int = Query(0, ge=0, le=1),
):
    provider = _require_provider(request)
    fn = getattr(provider, "boxscore_hitless_streak", None)
    if not callable(fn):
        raise HTTPException(status_code=501, detail="Provider does not implement boxscore_hitless_streak()")
    the_date = _parse_date(date)
    try:
        streak = fn(
            player_name=player,
            team_name=team,
            end_date=the_date,
            max_lookback=max_lookback,
            debug=bool(debug),
        )
        return {
            "player": player,
            "team": team,
            "end_date": the_date.isoformat(),
            "hitless_streak_ab_gt_0": int(streak),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Provider error: {type(e).__name__}: {e}")
