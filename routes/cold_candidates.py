# routes/cold_candidates.py
from __future__ import annotations

from datetime import datetime, timedelta, date as date_cls
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query, Request
import pytz

router = APIRouter(tags=["cold-candidates"])

# ---------- helpers ----------
def _parse_date(d: Optional[str]) -> date_cls:
    tz = pytz.timezone("America/New_York")
    now = datetime.now(tz).date()
    if not d or d.lower() == "today":
        return now
    s = d.lower()
    if s == "yesterday":
        return now - timedelta(days=1)
    if s == "tomorrow":
        return now + timedelta(days=1)
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date; use today|yesterday|YYYY-MM-DD")

def _require_provider(request: Request):
    provider = getattr(request.app.state, "provider", None)
    if provider is None:
        last_err = getattr(request.app.state, "last_provider_error", None)
        raise HTTPException(status_code=503, detail=f"Provider not loaded: {last_err or 'unknown error'}")
    return provider

def _as_list_from_provider(obj: Any, keys: List[str]) -> List[Dict[str, Any]]:
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for k in keys:
            v = obj.get(k)
            if isinstance(v, list):
                return v
    return []

def _get_float(d: Dict[str, Any], k: str, default: float = 0.0) -> float:
    v = d.get(k, default)
    try:
        return float(v)
    except Exception:
        return default

def _get_int(d: Dict[str, Any], k: str, default: int = 0) -> int:
    v = d.get(k, default)
    try:
        return int(v)
    except Exception:
        return default

# ---------- endpoint ----------
@router.get(
    "/cold_candidates",
    summary="Cold Candidates",
    description=(
        "BUSINESS RULES:\n"
        "  • Count hitless games only if AB > 0 (0-for-X counts; 0-for-0 is ignored and does not break).\n"
        "  • Current season only; require season AVG >= min_season_avg (default .265).\n"
        "  • Include players with hitless streak >= min_hitless_games (default 1) OR recent AVG <= max_recent_avg if provided.\n"
        "  • Ranking: verified_hitless_streak desc → season AVG desc → OBP desc → AB in last 3 games desc.\n"
        "  • verify=1 recomputes AB>0 hitless streak via provider.boxscore_hitless_streak (slower but exact).\n"
        "  • verified_only=1 keeps only rows where AB>0 recompute succeeded."
    ),
)
def cold_candidates(
    request: Request,
    date: Optional[str] = Query(None, description="today|yesterday|YYYY-MM-DD"),
    min_season_avg: float = Query(0.265, description="season AVG floor (current season only)"),
    last_n: int = Query(7, ge=1, le=15, description="seed window for provider’s cold pool"),
    max_recent_avg: Optional[float] = Query(None, description="optional cap on provider’s recent AVG; not required"),
    min_hitless_games: int = Query(1, ge=1, description="minimum AB>0 hitless games to include"),
    limit: int = Query(30, ge=1, le=100),
    team: Optional[str] = Query(None, description="optional team filter"),
    verify: int = Query(1, ge=0, le=1, description="try to recompute AB>0 hitless streak"),
    verified_only: int = Query(0, ge=0, le=1, description="when 1, include only players whose AB>0 streak was verified"),
    debug: int = Query(0, ge=0, le=1),
):
    provider = _require_provider(request)
    the_date = _parse_date(date)

    # 1) Seed from provider's league_cold_hitters (bigger pool, then filter)
    league_fn = getattr(provider, "league_cold_hitters", None)
    if not callable(league_fn):
        raise HTTPException(status_code=501, detail="Provider does not implement league_cold_hitters()")

    # Ask for a generous pool (4x limit, min 100) then filter down
    seed_n = max(limit * 4, 100)
    try:
        raw = league_fn(
            date_str=the_date.isoformat(),
            top_n=seed_n,
            n=seed_n,
            limit=seed_n,
            team=team,
            debug=bool(debug),
        )
    except TypeError:
        # signature fallback
        raw = league_fn(date=the_date, top_n=seed_n)

    items = _as_list_from_provider(raw, ["cold_hitters", "result", "players"]) or (raw if isinstance(raw, list) else [])
    candidates = len(items)

    # 2) Filter by season AVG + (hitless streak >= min_hitless_games OR recent AVG <= max_recent_avg if provided)
    filtered: List[Dict[str, Any]] = []
    for row in items:
        sa = _get_float(row, "season_avg", 0.0)
        if sa < float(min_season_avg):
            continue

        chs = _get_int(row, "current_hitless_streak", 0)

        # Allow via hitless streak threshold
        allow = chs >= int(min_hitless_games)

        # Or allow via recent AVG if present + capped
        if not allow and max_recent_avg is not None:
            # Try common recent keys
            ra: Optional[float] = None
            for k in (f"recent_avg_{last_n}", "recent_avg", "recent_avg_5"):
                if isinstance(row.get(k), (int, float)):
                    ra = float(row[k])
                    break
            if ra is not None and ra <= float(max_recent_avg):
                allow = True

        if not allow:
            continue

        filtered.append(dict(row))  # shallow copy

    # 3) Optional: exact recompute of AB>0 hitless streak using boxscores (slow but correct)
    verified_count = 0
    if verify == 1:
        verify_fn = getattr(provider, "boxscore_hitless_streak", None)
        if callable(verify_fn):
            for r in filtered:
                pname = r.get("player_name") or r.get("name")
                tname = r.get("team_name") or team
                try:
                    v = verify_fn(
                        player_name=str(pname),
                        team_name=str(tname) if tname else None,
                        end_date=the_date,
                        max_lookback=400,
                        debug=bool(debug),
                    )
                    r["verified_hitless_streak"] = int(v)
                    verified_count += 1
                except Exception:
                    # leave as-is if verify fails
                    r["verified_hitless_streak"] = _get_int(r, "current_hitless_streak", 0)
        else:
            # No verify available on provider; mirror current streak
            for r in filtered:
                r["verified_hitless_streak"] = _get_int(r, "current_hitless_streak", 0)
    else:
        for r in filtered:
            r["verified_hitless_streak"] = _get_int(r, "current_hitless_streak", 0)

    # 4) Optionally keep only those that have a strictly positive verified streak
    if verified_only == 1:
        filtered = [r for r in filtered if _get_int(r, "verified_hitless_streak", 0) >= int(min_hitless_games)]

    # 5) Ranking:
    #    verified_hitless_streak desc → season AVG desc → OBP desc → AB last 3 games desc
    def _get_obp(d: Dict[str, Any]) -> float:
        return _get_float(d, "season_obp", _get_float(d, "obp", 0.0))

    def _get_recent_ab3(d: Dict[str, Any]) -> int:
        # if the provider exposes recent ABs; otherwise 0
        return _get_int(d, "recent_ab_3", 0)

    filtered.sort(
        key=lambda r: (
            _get_int(r, "verified_hitless_streak", 0),
            _get_float(r, "season_avg", 0.0),
            _get_obp(r),
            _get_recent_ab3(r),
        ),
        reverse=True,
    )

    out = {
        "date": the_date.isoformat(),
        "filters": {
            "min_season_avg": float(min_season_avg),
            "last_n": int(last_n),
            "max_recent_avg": None if max_recent_avg is None else float(max_recent_avg),
            "min_hitless_games": int(min_hitless_games),
            "limit": int(limit),
            "team": team,
            "verify": int(verify),
            "verified_only": int(verified_only),
        },
        "counts": {
            "candidates": candidates,
            "qualified": len(filtered),
            "verified_attempted": verified_count if verify == 1 else 0,
            "returned": min(len(filtered), int(limit)),
        },
        "results": filtered[:limit],
    }
    if debug == 1:
        out["debug"] = {
            "notes": "Seeded from league_cold_hitters; optional boxscore verify applied per player.",
        }
    return out
