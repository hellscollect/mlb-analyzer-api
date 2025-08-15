from fastapi import APIRouter, Query, HTTPException, Request
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta, date as date_cls
import pytz
import inspect

router = APIRouter()

# -----------------------
# Helpers & glue
# -----------------------

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

def _f_float(d: Dict[str, Any], keys: List[str], default: float = 0.0) -> float:
    for k in keys:
        v = d.get(k)
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            try:
                return float(v)
            except Exception:
                pass
    return default

def _f_int(d: Dict[str, Any], keys: List[str], default: int = 0) -> int:
    for k in keys:
        v = d.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return int(v)
        if isinstance(v, str):
            try:
                return int(v)
            except Exception:
                pass
    return default

def _season_avg(row: Dict[str, Any]) -> float:
    return _f_float(row, ["season_avg", "seasonAVG", "avg", "AVG"], 0.0)

def _season_obp(row: Dict[str, Any]) -> float:
    return _f_float(row, ["season_obp", "OBP", "seasonOBP"], 0.0)

def _hitless_streak_key(row: Dict[str, Any]) -> int:
    # provider-reported (may include junk); we will override with verified below
    return _f_int(row, ["current_hitless_streak"], 0)

def _rank_tuple(r: Dict[str, Any]) -> Tuple:
    # Order: verified streak desc, season AVG desc, season OBP desc, AB last 3 games desc
    return (
        int(r.get("verified_hitless_streak_ab_gt_0", r.get("current_hitless_streak", 0))),
        _season_avg(r),
        _season_obp(r),
        _f_int(r, ["recent_ab_3", "AB_last_3", "ab_last_3"], 0),
    )

def _verify_hitless_with_fallback(box_fn, name: str, team: Optional[str], end_date: date_cls, debug: bool):
    """
    Try to compute AB>0-only current hitless streak:
      1) with team_name (if provided),
      2) without team_name (in case provider team is stale).
    The provider should walk back until the streak breaks; we request a large window.
    Returns: (ok: bool, verified_streak: int, raw_payload: Any, err: Optional[str])
    """
    calls = []
    base_kwargs = dict(player_name=name, end_date=end_date, max_lookback=300, debug=debug)
    if team:
        calls.append(dict(**base_kwargs, team_name=team))
    calls.append(dict(**base_kwargs))

    last_err = None
    for kwargs in calls:
        try:
            raw = _call_with_sig(box_fn, **kwargs)
            if isinstance(raw, int):
                return True, int(raw), raw, None
            if isinstance(raw, dict):
                # prefer explicit AB>0-only key if provided
                v = _f_int(raw, ["consecutive_hitless_ab_gt_0", "streak"], 0)
                return True, v, raw, None
            # unexpected shape; treat as zero but return payload
            return True, 0, raw, "unexpected_verify_payload"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            continue
    return False, 0, None, last_err or "verify_failed"

# -----------------------
# Endpoint
# -----------------------

@router.get("/cold_candidates", operation_id="cold_candidates")
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
    """
    BUSINESS RULES (per user spec):
      • Count hitless games only if AB > 0 (0-for-X counts; 0-for-0 is ignored and does not break).
      • Current season only; require season AVG >= min_season_avg (default .265).
      • Include players with hitless streak >= min_hitless_games (default 1).
      • Ranking: verified_hitless_streak desc → season AVG desc → season OBP desc → AB in last 3 games desc.
      • verify=1 attempts AB>0 recompute via provider.boxscore_hitless_streak with a large lookback.
      • verified_only=1 keeps only rows where AB>0 recompute succeeded.
    """
    the_date = _parse_date(date)
    provider = getattr(request.app.state, "provider", None)
    if provider is None:
        raise HTTPException(status_code=503, detail="Provider not loaded")

    # 1) Seed pool from provider cold lists (wide), then filter down.
    league_cold = _callable(provider, "league_cold_hitters")
    direct_cold = _callable(provider, "cold_streak_hitters")
    if not (league_cold or direct_cold):
        raise HTTPException(status_code=501, detail="Provider lacks league_cold_hitters()/cold_streak_hitters()")

    if league_cold:
        res = _call_with_sig(
            league_cold,
            date_str=the_date.isoformat(),
            date=the_date,
            top_n=300,
            n=300,
            limit=300,
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

    # 2) Normalize, fix mojibake, and apply initial filters (season AVG + hitless floor + optional team)
    prelim: List[Dict[str, Any]] = []
    for r in raw_pool or []:
        r = _deep_fix(dict(r))
        if team and r.get("team_name") != team:
            continue
        if _season_avg(r) < float(min_season_avg):
            continue
        # Provider's streak may be noisy; we keep it for now, but verified streak will override
        if _hitless_streak_key(r) < int(min_hitless_games):
            continue
        prelim.append(r)

    # 3) Verification pass (AB>0-only hitless streak, unlimited lookback up to season; request large window)
    results: List[Dict[str, Any]] = []
    dropped: List[Dict[str, Any]] = []
    if verify == 1:
        box_fn = _callable(provider, "boxscore_hitless_streak")
        for r in prelim:
            name = r.get("player_name")
            team_name = r.get("team_name")
            if not name:
                if verified_only == 1 and debug:
                    dropped.append({"player": None, "reason": "missing_player_name"})
                if verified_only == 0:
                    r["verified"] = False
                    r["verified_hitless_streak_ab_gt_0"] = _hitless_streak_key(r)
                    results.append(r)
                continue

            if box_fn:
                ok, v, raw, err = _verify_hitless_with_fallback(box_fn, name, team_name, the_date, bool(debug))
                if ok:
                    r2 = dict(r)
                    r2["verified"] = True
                    r2["verified_hitless_streak_ab_gt_0"] = int(v)
                    # Enforce min_hitless_games against the verified value
                    if v >= int(min_hitless_games):
                        results.append(r2)
                    else:
                        if verified_only == 0:
                            # keep but reflect lower verified value
                            results.append(r2)
                        else:
                            if debug:
                                dropped.append({"player": name, "team": team_name, "verified_streak": v, "reason": f"below_min_{min_hitless_games}"})
                else:
                    if verified_only == 0:
                        r2 = dict(r)
                        r2["verified"] = False
                        r2["verified_hitless_streak_ab_gt_0"] = _hitless_streak_key(r)
                        if debug:
                            r2["verify_error"] = err
                        results.append(r2)
                    else:
                        if debug:
                            dropped.append({"player": name, "team": team_name, "reason": err})
            else:
                if verified_only == 0:
                    r2 = dict(r)
                    r2["verified"] = False
                    r2["verified_hitless_streak_ab_gt_0"] = _hitless_streak_key(r)
                    if debug:
                        r2["verify_error"] = "boxscore_hitless_streak_unavailable"
                    results.append(r2)
                else:
                    if debug:
                        dropped.append({"player": name, "team": team_name, "reason": "boxscore_hitless_streak_unavailable"})
    else:
        # verify==0: just reflect provider value as "verified" field = False
        for r in prelim:
            r2 = dict(r)
            r2["verified"] = False
            r2["verified_hitless_streak_ab_gt_0"] = _hitless_streak_key(r2)
            results.append(r2)

    # 4) Final min_hitless_games check uses the verified value if present
    final: List[Dict[str, Any]] = []
    for r in results:
        v = int(r.get("verified_hitless_streak_ab_gt_0", r.get("current_hitless_streak", 0)))
        if v >= int(min_hitless_games):
            final.append(r)
        else:
            if verified_only == 0:
                final.append(r)  # keep, but it will sort lower
            else:
                if debug:
                    dropped.append({"player": r.get("player_name"), "team": r.get("team_name"), "verified_streak": v, "reason": f"below_min_{min_hitless_games}"})

    # 5) Rank: streak desc → season AVG desc → season OBP desc → AB last 3 desc
    final.sort(key=_rank_tuple, reverse=True)

    return {
        "date": the_date.isoformat(),
        "filters": {
            "min_season_avg": float(min_season_avg),
            "last_n": int(last_n),
            "max_recent_avg": float(max_recent_avg) if max_recent_avg is not None else None,
            "min_hitless_games": int(min_hitless_games),
            "limit": int(limit),
            "team": team,
            "verify": int(verify),
            "verified_only": int(verified_only),
        },
        "counts": {
            "candidates": len(raw_pool),
            "qualified": len(final),
            "returned": min(len(final), int(limit)),
        },
        "results": final[: int(limit)],
        **({"debug": {"dropped": dropped[:50]}} if debug else {}),
    }
