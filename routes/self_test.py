# routes/self_test.py
from datetime import datetime, timedelta, date as date_cls
from typing import Any, Dict, Optional

import pytz
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

# Force UTF-8 so names like “Agustín Ramírez” render correctly everywhere
class UTF8JSONResponse(JSONResponse):
    media_type = "application/json; charset=utf-8"

router = APIRouter()

# ------------------
# Local helpers (no import of main to avoid circular import)
# ------------------
def parse_date(d: Optional[str]) -> date_cls:
    tz = pytz.timezone("America/New_York")
    now = datetime.now(tz).date()
    if not d or d.lower() == "today":
        return now
    s = d.lower()
    if s == "yesterday":
        return now - timedelta(days=1)
    if s == "tomorrow":
        return now + timedelta(days=1)
    return datetime.strptime(d, "%Y-%m-%d").date()

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

def _callable(obj: Any, name: str):
    if obj is None:
        return None
    fn = getattr(obj, name, None)
    return fn if callable(fn) else None

def _call_with_sig(fn, **kwargs):
    """
    Call function with only the kwargs its signature accepts.
    If that fails (e.g., positional-only args), retry positionally.
    """
    import inspect
    if fn is None:
        raise RuntimeError("No callable provided")
    try:
        sig = inspect.signature(fn)
        allowed_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
        try:
            return fn(**allowed_kwargs)
        except TypeError:
            params = list(sig.parameters.values())
            args = []
            for p in params:
                if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
                    if p.name in allowed_kwargs:
                        args.append(allowed_kwargs[p.name])
                    elif p.default is not inspect._empty:
                        args.append(p.default)
                    else:
                        raise
            return fn(*args)
    except Exception as e:
        raise RuntimeError(f"{type(e).__name__}: {e}")

def _take_n(obj: Any, n: int) -> Any:
    if isinstance(obj, list):
        return obj[:n]
    if isinstance(obj, dict):
        out = dict(obj)
        if "hot_hitters" in out and isinstance(out["hot_hitters"], list):
            out["hot_hitters"] = out["hot_hitters"][:n]
        if "cold_hitters" in out and isinstance(out["cold_hitters"], list):
            out["cold_hitters"] = out["cold_hitters"][:n]
        return out
    return obj

def _check_call(provider: Any, method_name: str, **kwargs) -> Dict[str, Any]:
    fn = _callable(provider, method_name)
    if not fn:
        return {"ok": False, "error": f"NotImplementedError: {method_name} not available in provider"}
    try:
        res = _call_with_sig(fn, **kwargs)
        return {"ok": True, "result": _deep_fix(res)}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ------------------
# Endpoint
# ------------------
@router.get("/self_test", response_class=UTF8JSONResponse)
def self_test(
    request: Request,
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    limit: int = Query(10, ge=1, le=200),
    debug: int = Query(0, ge=0, le=1),
):
    app = request.app
    provider = getattr(app.state, "provider", None)
    provider_module = getattr(app.state, "provider_module", None)
    provider_class = getattr(app.state, "provider_class", None)
    last_provider_error = getattr(app.state, "last_provider_error", None)

    tz = pytz.timezone("America/New_York")
    now_str = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S %Z")
    the_date = parse_date(date)

    # Build calls that accept either date_str or date, and either top_n/n/limit
    common_hit_kwargs = {
        "date_str": the_date.isoformat(),
        "date": the_date,
        "top_n": limit,
        "n": limit,
        "limit": limit,
        "debug": bool(debug),
    }

    checks = {
        "league_hot_hitters": _take_n(
            _check_call(provider, "league_hot_hitters", **common_hit_kwargs), limit
        ),
        "league_cold_hitters": _take_n(
            _check_call(provider, "league_cold_hitters", **common_hit_kwargs), limit
        ),
        "schedule_for_date": _check_call(
            provider, "schedule_for_date", date_str=the_date.isoformat(), date=the_date, debug=bool(debug)
        ),
        # Direct-only probes (OK to be false; your public endpoints adapt)
        "hot_streak_hitters": _take_n(
            _check_call(provider, "hot_streak_hitters", date=the_date, min_avg=0.0, games=5, require_hit_each=False, debug=bool(debug)),
            limit
        ),
        "cold_streak_hitters": _take_n(
            _check_call(provider, "cold_streak_hitters", date=the_date, min_avg=0.0, games=5, require_zero_hit_each=False, debug=bool(debug)),
            limit
        ),
        "pitcher_streaks": _check_call(
            provider, "pitcher_streaks",
            date=the_date,
            hot_max_era=4.0, hot_min_ks_each=6, hot_last_starts=3,
            cold_min_era=4.6, cold_min_runs_each=3, cold_last_starts=2,
            debug=bool(debug)
        ),
    }

    # Probe private fetchers if present
    fetch_probe: Dict[str, Any] = {}
    for name in ("_fetch_hitter_rows", "_fetch_pitcher_rows"):
        fn = _callable(provider, name)
        if not fn:
            fetch_probe[name] = {"ok": False, "error": f"NotImplementedError: {name} not available in provider"}
        else:
            try:
                res = _call_with_sig(fn, date=the_date, game_date=the_date, limit=50, team=None)
                sample = res[:3] if isinstance(res, list) else res
                fetch_probe[name] = {"ok": True, "result_sample": _deep_fix(sample)}
            except Exception as e:
                fetch_probe[name] = {"ok": False, "error": str(e)}
    checks["provider_raw_probe"] = {"ok": True, "result": fetch_probe}

    # UTF-8 sample names (take a few from the hot/cold lists if present)
    utf8_samples = []
    try:
        hot = checks["league_hot_hitters"].get("result") or []
        cold = checks["league_cold_hitters"].get("result") or []
        for row in (hot[:5] + cold[:5]):
            name = row.get("player_name") if isinstance(row, dict) else None
            if name:
                utf8_samples.append(_fix_text(name))
    except Exception:
        pass

    return {
        "app": app.title,
        "version": app.version,
        "date": the_date.isoformat(),
        "now_local": now_str,
        "provider": {
            "loaded": provider is not None,
            "module": provider_module,
            "class": provider_class,
            "last_error": last_provider_error,
        },
        "checks": checks,
        "utf8_samples": utf8_samples,
    }
