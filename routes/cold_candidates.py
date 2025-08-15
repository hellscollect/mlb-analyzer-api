# routes/cold_candidates.py
from fastapi import APIRouter, Query, HTTPException, Request
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta, date as date_cls
import pytz
import inspect

router = APIRouter()

def _parse_date(d: Optional[str]) -> date_cls:
    tz = pytz.timezone("America/New_York")
    now = datetime.now(tz).date()
    if not d or d.lower() == "today":
        return now
    if d.lower() == "yesterday":
        return now - timedelta(days=1)
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date; use today|yesterday|YYYY-MM-DD")

def _callable(obj: Any, name: str):
    if obj is None:
        return None
    fn = getattr(obj, name, None)
    return fn if callable(fn) else None

def _call_with_sig(fn, **kwargs):
    if fn is None:
        raise HTTPException(status_code=501, detail="Provider method missing")
    try:
        sig = inspect.signature(fn)
        allowed = {k: v for k, v in kwargs.items() if k in sig.parameters}
        try:
            return fn(**allowed)
        except TypeError:
            params = list(sig.parameters.values())
            args = []
            for p in params:
                if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
                    if p.name in allowed:
                        args.append(allowed[p.name])
                    elif p.default is not inspect._empty:
                        args.append(p.default)
                    else:
                        raise
            return fn(*args)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Provider error calling {fn.__name__}: {type(e).__name__}: {e}")

def _fix_text(s: Any) -> Any:
    if not isinstance(s, str):
        return s
    if "Ã" in s or "Â" in s:
        try:
            return s.encode("latin1").decode("utf-8")
        except Exception:
            return s
    return s

def _deep_fix(obj: Any) -> Any:
    if isinstance(obj, dict):
        return { _fix_text(k): _deep_fix(v) for k, v in obj.items() }
    if isinstance(obj, list):
        return [ _deep_fix(x) for x in obj ]
    if isinstance(obj, str):
        return _fix_text(obj)
    return obj

def _season_avg_key(row: Dict[str, Any]) -> float:
    for k in ("season_avg", "seasonAVG", "avg", "AVG"):
        if k in row and isinstance(row[k], (int, float)):
            return float(row[k])
    return float(row.get("season_avg", 0.0))

def _hitless_streak_key(row: Dict[str, Any]) -> int:
    return int(row.get("current_hitless_streak", 0))

@router.get("/cold_candidates", operation_id="cold_candidates")
def cold_candidates(
    request: Request,
    date: Optional[str] = Query(None, description="today|yesterday|YYYY-MM-DD"),
    min_season_avg: float = Query(0.26),
    last_n: int = Query(7, ge=1, le=15, description="lookback window for provider’s cold pool"),
    max_recent_avg: Optional[float] = Query(None, description="optional cap on recent AVG across last_n"),
    min_hitless_games: int = Query(3, ge=1, description="consecutive hitless games required"),
    limit: int = Query(30, ge=1, le=50),
    team: Optional[str] = Query(None, description="optional team filter"),
    verify: int = Query(1, ge=0, le=1, description="when 1, re-check streak using AB>0 only (box scores)"),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = _parse_date(date)
    provider = getattr(request.app.state, "provider", None)
    if provider is None:
        raise HTTPException(status_code=503, detail="Provider not loaded")

    # 1) seed cold pool from provider
    league_cold = _callable(provider, "league_cold_hitters")
    direct_cold = _callable(provider, "cold_streak_hitters")
    if not (league_cold or direct_cold):
        raise HTTPException(status_code=501, detail="Provider lacks league_cold_hitters()/cold_streak_hitters()")

    raw_pool: List[Dict[str, Any]]
    if league_cold:
        res = _call_with_sig(
            league_cold,
            date_str=the_date.isoformat(),
            date=the_date,
            top_n=200,
            n=200,
            limit=200,
            last_n=last_n,
            max_recent_avg=max_recent_avg,
            team=team,
            debug=bool(debug),
        )
        raw_pool = res if isinstance(res, list) else res.get("cold_hitters", [])
    else:
        res = _call_with_sig(
            direct_cold,
            date=the_date,
            games=last_n,
            require_zero_hit_each=False,
            min_avg=0.0,
            debug=bool(debug),
        )
        raw_pool = res if isinstance(res, list) else res.get("cold_hitters", [])

    # 2) filter by season avg & hitless floor (+fix encoding; optional team)
    pool: List[Dict[str, Any]] = []
    for r in raw_pool:
        r = _deep_fix(dict(r))
        if team and r.get("team_name") != team:
            continue
        if _season_avg_key(r) < float(min_season_avg):
            continue
        if _hitless_streak_key(r) < int(min_hitless_games):
            continue
        pool.append(r)

    # 3) optional verification: recompute hitless streak using AB>0 only
    if verify == 1:
        box_fn = _callable(provider, "boxscore_hitless_streak")
        if box_fn:
            verified = []
            for r in pool:
                name = r.get("player_name")
                team_name = r.get("team_name")
                if not name or not team_name:
                    continue
                try:
                    streak = _call_with_sig(
                        box_fn,
                        player_name=name,
                        team_name=team_name,
                        end_date=the_date,
                        max_lookback=15,
                        debug=bool(debug),
                    )
                    r2 = dict(r)
                    if isinstance(streak, int):
                        r2["current_hitless_streak"] = max(r2.get("current_hitless_streak", 0), streak)
                    elif isinstance(streak, dict):
                        v = int(streak.get("consecutive_hitless_ab_gt_0", streak.get("streak", 0)))
                        r2["current_hitless_streak"] = max(r2.get("current_hitless_streak", 0), v)
                        if debug:
                            r2["verify_debug"] = streak
                    verified.append(r2)
                except Exception:
                    verified.append(r)
            pool = verified

    # 4) rank and return
    pool.sort(key=lambda x: (_hitless_streak_key(x), _season_avg_key(x)), reverse=True)

    return {
        "date": the_date.isoformat(),
        "filters": {
            "min_season_avg": float(min_season_avg),
            "last_n": int(last_n),
            "max_recent_avg": float(max_recent_avg) if max_recent_avg is not None else None,
            "min_hitless_games": int(min_hitless_games),
            "limit": int(limit),
            "team": team,
        },
        "counts": {
            "candidates": len(raw_pool),
            "qualified": len(pool),
            "returned": min(len(pool), int(limit)),
        },
        "results": pool[: int(limit)],
    }
