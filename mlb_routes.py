# api/mlb_routes.py
from __future__ import annotations

from functools import lru_cache
from datetime import date as date_cls, datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from providers.statsapi_provider import StatsApiProvider

router = APIRouter(prefix="/mlb", tags=["mlb"])


@lru_cache(maxsize=1)
def _provider() -> StatsApiProvider:
    # single shared instance (keeps the tiny in-memory cache hot)
    return StatsApiProvider()


def _default_date_str() -> str:
    # Default to today's date in ET, formatted YYYY-MM-DD
    try:
        # if zoneinfo is present, let provider handle ET formatting; here keep it simple:
        return datetime.utcnow().date().isoformat()
    except Exception:
        return datetime.utcnow().date().isoformat()


@router.get("/schedule")
def schedule(date: str = Query(default_factory=_default_date_str, description="YYYY-MM-DD")):
    try:
        return {"date": date, "games": _provider().schedule_for_date(date)}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"schedule error: {type(e).__name__}")


@router.get("/hot")
def hot_hitters(
    date: str = Query(default_factory=_default_date_str, description="YYYY-MM-DD"),
    top_n: int = Query(10, ge=1, le=100, description="How many to return"),
):
    try:
        data = _provider().league_hot_hitters(date, top_n)
        return {"date": date, "count": len(data), "hot": data}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"hot hitters error: {type(e).__name__}")


@router.get("/cold")
def cold_hitters(
    date: str = Query(default_factory=_default_date_str, description="YYYY-MM-DD"),
    top_n: int = Query(10, ge=1, le=100, description="How many to return"),
):
    try:
        data = _provider().league_cold_hitters(date, top_n)
        return {"date": date, "count": len(data), "cold": data}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"cold hitters error: {type(e).__name__}")


@router.get("/hitless-streak")
def hitless_streak(
    player_name: str = Query(..., description="Full player name, e.g. 'Juan Soto'"),
    team_name: Optional[str] = Query(None, description="Optional team name to speed matching"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD; defaults to today"),
    max_lookback: int = Query(300, ge=1, le=365, description="Days to scan backwards within current season"),
):
    try:
        end_dt: Optional[date_cls] = None
        if end_date:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        streak = _provider().boxscore_hitless_streak(
            player_name=player_name,
            team_name=team_name,
            end_date=end_dt,
            max_lookback=max_lookback,
        )
        return {
            "player_name": player_name,
            "team_name": team_name,
            "end_date": (end_dt or datetime.utcnow().date()).isoformat(),
            "hitless_streak": streak,
        }
    except ValueError:
        raise HTTPException(status_code=400, detail="end_date must be YYYY-MM-DD")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"hitless-streak error: {type(e).__name__}")


# Optional: tiny health check for this router
@router.get("/_healthz")
def _healthz():
    return {"ok": True, "provider": "statsapi"}
