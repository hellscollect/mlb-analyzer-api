# routes/cold_candidates.py
from __future__ import annotations

from datetime import datetime, timedelta, date as date_cls
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
import pytz

router = APIRouter(tags=["cold"], prefix="")

def _parse_date(d: Optional[str]) -> date_cls:
    tz = pytz.timezone("America/New_York")
    today = datetime.now(tz).date()
    if not d or d.lower() == "today":
        return today
    if d.lower() == "yesterday":
        return today - timedelta(days=1)
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

@router.get(
    "/cold_candidates",
    summary="Cold Candidates (AB>0-only, boxscore-verified streaks)",
    description=(
        "RULES:\n"
        "  • Count hitless games only if AB > 0 (0-for-0 is ignored and does not break).\n"
        "  • Current season only; require season AVG >= min_season_avg (default .265).\n"
        "  • Include players with verified hitless streak >= min_hitless_games (default 1).\n"
        "  • Ranking: verified_hitless_streak desc → season AVG desc → (optional) recent AVG desc.\n"
        "This endpoint ALWAYS recomputes hitless streaks via provider.boxscore_hitless_streak."
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
    debug: int = Query(0, ge=0, le=1),
):
    provider = _require_provider(request)
    the_date = _parse_date(date)

    # Seed from provider’s league_cold_hitters (already AB>0-enforced in provider)
    league_fn = getattr(provider, "league_cold_hitters", None)
    if not callable(league_fn):
        raise HTTPException(status_code=501, detail="Provider does not implement league_cold_hitters()")

    try:
        raw = league_fn(date_str=the_date.isoformat(), top_n=max(limit * 4, 120))
    except TypeError:
        raw = league_fn(date=the_date, n=max(limit * 4, 120))

    items = raw if isinstance(raw, list) else []
    candidates: List[Dict[str, Any]] = []
    for row in items:
        sa = row.get("season_avg")
        if not isinstance(sa, (int, float)) or float(sa) < float(min_season_avg):
            continue

        # Optional recent AVG clamp
        if max_recent_avg is not None:
            ra = row.get("recent_avg_5") or row.get("recent_avg") or None
            if isinstance(ra, (int, float)) and float(ra) > float(max_recent_avg):
                continue

        candidates.append(row)

    # ALWAYS re-verify AB>0-only streak via boxscores
    verified: List[Dict[str, Any]] = []
    for row in candidates:
        pname = row.get("player_name") or row.get("name")
        tname = row.get("team_name")
        if not pname:
            continue
        try:
            streak = provider.boxscore_hitless_streak(
                player_name=pname,
                team_name=tname,
                end_date=the_date,
                max_lookback=300,
                debug=bool(debug),
            )
        except Exception:
            continue

        if streak >= int(min_hitless_games):
            out = dict(row)
            out["verified_hitless_streak"] = int(streak)
            verified.append(out)

    # Sort by verified streak desc, then season avg desc, then (if present) recent avg desc
    def _recent_key(d: Dict[str, Any]) -> float:
        v = d.get("recent_avg_5") or d.get("recent_avg") or 0.0
        try:
            return float(v)
        except Exception:
            return 0.0

    verified.sort(
        key=lambda r: (r.get("verified_hitless_streak", 0), r.get("season_avg", 0.0), _recent_key(r)),
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
        },
        "counts": {
            "seeded": len(items),
            "qualified": len(candidates),
            "verified": len(verified),
            "returned": min(len(verified), int(limit)),
        },
        "results": verified[:limit],
    }
    if debug == 1:
        out["debug"] = {
            "note": "All hitless streaks recomputed via boxscores (AB>0-only).",
            "sample": verified[:5],
        }
    return out
