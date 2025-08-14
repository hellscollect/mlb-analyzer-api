import os
import importlib
import inspect
from datetime import datetime, timedelta, date as date_cls
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pytz

APP_NAME = "MLB Analyzer API"

# --- Server URL for OpenAPI (required by GPT Actions) ---
EXTERNAL_URL = (
    os.getenv("RENDER_EXTERNAL_URL")  # Render sets this automatically
    or "https://mlb-analyzer-api.onrender.com"  # fallback to your host
)

app = FastAPI(
    title=APP_NAME,
    version="1.1.1",
    description="Custom GPT + API for MLB streak analysis",
    servers=[{"url": EXTERNAL_URL}],
    openapi_url="/openapi.json",
)

# --- CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------
# Provider loading
# ------------------
_last_provider_error: Optional[str] = None

def load_provider() -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """
    Loads the provider referenced by $MLB_PROVIDER.
    Default to statsapi provider since it exists in your repo.
    """
    global _last_provider_error
    target = os.getenv("MLB_PROVIDER", "providers.statsapi_provider:StatsApiProvider")
    module_path, _, class_name = target.partition(":")
    try:
        module = importlib.import_module(module_path)
        provider_cls = getattr(module, class_name)
        instance = provider_cls()
        _last_provider_error = None
        return instance, module_path, class_name
    except Exception as e:
        _last_provider_error = f"{type(e).__name__}: {e}"
        print(f"[provider-load-error] MLB_PROVIDER='{target}' -> {_last_provider_error}")
        return None, None, None

provider, provider_module, provider_class = load_provider()

# Expose provider to routers
app.state.provider = provider
app.state.provider_module = provider_module
app.state.provider_class = provider_class
app.state.last_provider_error = _last_provider_error

# ------------------
# Utilities
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
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date; use today|yesterday|tomorrow|YYYY-MM-DD")

def _callable(obj: Any, name: str):
    if obj is None:
        return None
    fn = getattr(obj, name, None)
    return fn if callable(fn) else None

def _call_with_sig(fn, **kwargs):
    """
    Call function with only the kwargs its signature accepts.
    Useful for adapting to providers with slightly different names.
    """
    if fn is None:
        raise HTTPException(status_code=501, detail="Provider method missing")
    try:
        sig = inspect.signature(fn)
        allowed = {k: v for k, v in kwargs.items() if k in sig.parameters}
        return fn(**allowed)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Provider error calling {fn.__name__}: {type(e).__name__}: {e}")

def safe_call(obj: Any, name: str, *args, **kwargs):
    if obj is None:
        raise HTTPException(status_code=503, detail=f"Provider not loaded: {_last_provider_error or 'unknown error'}")
    fn = getattr(obj, name, None)
    if not callable(fn):
        raise HTTPException(status_code=501, detail=f"Provider does not implement {name}()")
    return fn(*args, **kwargs)

def _smart_call_fetch(method_name: str, the_date: date_cls, limit: Optional[int], team: Optional[str]):
    if provider is None:
        raise HTTPException(status_code=503, detail="Provider not loaded")
    fn = getattr(provider, method_name, None)
    if not callable(fn):
        raise HTTPException(status_code=501, detail=f"Provider does not implement {method_name}()")
    sig = inspect.signature(fn)
    params = sig.parameters
    kwargs = {}
    date_param_name = "date" if "date" in params else ("game_date" if "game_date" in params else None)
    if date_param_name:
        kwargs[date_param_name] = the_date
    if "limit" in params and limit is not None:
        kwargs["limit"] = limit
    if "team" in params and team is not None:
        kwargs["team"] = team
    try:
        if date_param_name or (not params):
            return fn(**kwargs)
        else:
            return fn(the_date, **kwargs)
    except TypeError:
        try:
            return fn(the_date)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error calling {method_name}: {type(e).__name__}: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calling {method_name}: {type(e).__name__}: {e}")

# ------------------
# Models (kept here so OpenAPI is complete)
# ------------------
class HealthResp(BaseModel):
    ok: bool
    provider_loaded: bool
    provider_module: Optional[str]
    provider_class: Optional[str]
    provider_error: Optional[str] = None
    now_local: str

class SlateScanResp(BaseModel):
    hot_hitters: List[Dict[str, Any]]
    cold_hitters: List[Dict[str, Any]]
    hot_pitchers: List[Dict[str, Any]]
    cold_pitchers: List[Dict[str, Any]]
    matchups: List[Dict[str, Any]]
    debug: Optional[Dict[str, Any]] = None

class ProviderRawReq(BaseModel):
    date: Optional[str] = None
    limit: Optional[int] = None
    team: Optional[str] = None
    debug: int = 0

class HotHittersReq(BaseModel):
    date: Optional[str] = None
    min_avg: float = 0.280
    games: int = 3
    require_hit_each: bool = True
    top_n: int = 25
    debug: int = 0

class ColdHittersReq(BaseModel):
    date: Optional[str] = None
    min_avg: float = 0.275
    games: int = 2
    require_zero_hit_each: bool = True
    top_n: int = 25
    debug: int = 0

class PitcherStreaksReq(BaseModel):
    date: Optional[str] = None
    hot_max_era: float = 4.0
    hot_min_ks_each: int = 6
    hot_last_starts: int = 3
    cold_min_era: float = 4.6
    cold_min_runs_each: int = 3
    cold_last_starts: int = 2
    debug: int = 0

class ColdPitchersReq(BaseModel):
    date: Optional[str] = None
    min_era: float = 4.6
    min_runs_each: int = 3
    last_starts: int = 2
    debug: int = 0

class DateOnlyReq(BaseModel):
    date: Optional[str] = None
    debug: int = 0

# ------------------
# Health
# ------------------
@app.get("/health", response_model=HealthResp, operation_id="health")
def health(tz: str = Query("America/New_York", description="IANA timezone for timestamp echo")):
    try:
        zone = pytz.timezone(tz)
    except Exception:
        zone = pytz.timezone("America/New_York")
    now_str = datetime.now(zone).strftime("%Y-%m-%d %H:%M:%S %Z")
    return HealthResp(
        ok=True,
        provider_loaded=provider is not None,
        provider_module=provider_module,
        provider_class=provider_class,
        provider_error=_last_provider_error,
        now_local=now_str,
    )

# ------------------
# Raw provider rows (debug/temporary)
# ------------------
@app.get("/provider_raw", operation_id="provider_raw")
def provider_raw(
    date: Optional[str] = Query(None),
    limit: Optional[int] = Query(None, ge=1, le=5000),
    team: Optional[str] = Query(None),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    hitter_fetch = _callable(provider, "_fetch_hitter_rows")
    pitcher_fetch = _callable(provider, "_fetch_pitcher_rows")
    if not hitter_fetch or not pitcher_fetch:
        raise HTTPException(status_code=501, detail="Provider does not implement _fetch_hitter_rows()/_fetch_pitcher_rows()")
    hitters = _smart_call_fetch("_fetch_hitter_rows", the_date, limit, team)
    pitchers = _smart_call_fetch("_fetch_pitcher_rows", the_date, limit, team)
    out = {
        "meta": {
            "provider_module": provider_module,
            "provider_class": provider_class,
            "date": the_date.isoformat(),
        },
        "hitters_raw": hitters,
        "pitchers_raw": pitchers,
    }
    if debug == 1:
        provider_base = getattr(provider, "base", None)
        provider_key_present = bool(getattr(provider, "key", "") or os.getenv("DATA_API_KEY"))
        out["debug"] = {
            "notes": "Called provider private fetches with signature-aware kwargs.",
            "hitter_fetch_exists": bool(hitter_fetch),
            "pitcher_fetch_exists": bool(pitcher_fetch),
            "requested_args": {"date": the_date.isoformat(), "limit": limit, "team": team},
            "provider_config": {
                "fake_mode": os.getenv("PROD_USE_FAKE", "0") in ("1", "true", "True", "YES", "yes"),
                "data_api_base": provider_base or "(unset)",
                "has_api_key": provider_key_present,
            }
        }
    return out

@app.post("/provider_raw_post", operation_id="provider_raw_post")
def provider_raw_post(req: ProviderRawReq):
    the_date = parse_date(req.date)
    hitter_fetch = _callable(provider, "_fetch_hitter_rows")
    pitcher_fetch = _callable(provider, "_fetch_pitcher_rows")
    if not hitter_fetch or not pitcher_fetch:
        raise HTTPException(status_code=501, detail="Provider does not implement _fetch_hitter_rows()/_fetch_pitcher_rows()")
    hitters = _smart_call_fetch("_fetch_hitter_rows", the_date, req.limit, req.team)
    pitchers = _smart_call_fetch("_fetch_pitcher_rows", the_date, req.limit, req.team)
    out = {"hitters_raw": hitters, "pitchers_raw": pitchers}
    if req.debug == 1:
        out["debug"] = {"requested": req.model_dump()}
    return out

# ------------------
# Compatibility wrappers for this provider (league_* + date_str adapters)
# ------------------
def _hot_hitters_fallback(
    the_date: date_cls,
    min_avg: float,
    games: int,
    require_hit_each: bool,
    debug: bool,
    top_n: int = 25,
):
    # Prefer exact method if implemented
    direct = _callable(provider, "hot_streak_hitters")
    if direct:
        return _call_with_sig(
            direct,
            date=the_date,
            min_avg=min_avg,
            games=games,
            require_hit_each=require_hit_each,
            debug=debug,
        )
    # Fallback to provider's league_* that requires date_str + top_n
    league = _callable(provider, "league_hot_hitters")
    if league:
        return _call_with_sig(
            league,
            date_str=the_date.isoformat(),
            date=the_date,     # in case provider accepts 'date' instead
            top_n=top_n,
            n=top_n,           # alternate name just in case
            limit=top_n,       # alternate name just in case
            debug=debug,
        )
    raise HTTPException(status_code=501, detail="Provider does not implement hot_streak_hitters() or league_hot_hitters().")

def _cold_hitters_fallback(
    the_date: date_cls,
    min_avg: float,
    games: int,
    require_zero_hit_each: bool,
    debug: bool,
    top_n: int = 25,
):
    direct = _callable(provider, "cold_streak_hitters")
    if direct:
        return _call_with_sig(
            direct,
            date=the_date,
            min_avg=min_avg,
            games=games,
            require_zero_hit_each=require_zero_hit_each,
            debug=debug,
        )
    league = _callable(provider, "league_cold_hitters")
    if league:
        return _call_with_sig(
            league,
            date_str=the_date.isoformat(),
            date=the_date,
            top_n=top_n,
            n=top_n,
            limit=top_n,
            debug=debug,
        )
    raise HTTPException(status_code=501, detail="Provider does not implement cold_streak_hitters() or league_cold_hitters().")

def _pitcher_streaks_fallback(the_date: date_cls, hot_max_era: float, hot_min_ks_each: int, hot_last_starts: int,
                              cold_min_era: float, cold_min_runs_each: int, cold_last_starts: int, debug: bool):
    direct = _callable(provider, "pitcher_streaks")
    if direct:
        return _call_with_sig(
            direct,
            date=the_date,
            hot_max_era=hot_max_era,
            hot_min_ks_each=hot_min_ks_each,
            hot_last_starts=hot_last_starts,
            cold_min_era=cold_min_era,
            cold_min_runs_each=cold_min_runs_each,
            cold_last_starts=cold_last_starts,
            debug=debug,
        )
    # If no pitcher method, return empty structure but 200 with a debug note
    return {
        "hot_pitchers": [],
        "cold_pitchers": [],
        "debug": {
            "note": "pitcher_streaks not implemented by provider; returning empty lists",
            "provider_module": provider_module,
            "provider_class": provider_class,
        },
    }

def _schedule_for_date(the_date: date_cls, debug: bool):
    sched_fn = _callable(provider, "schedule_for_date")
    if not sched_fn:
        return []
    # Provider needs date_str; adapt gracefully
    resp = _call_with_sig(
        sched_fn,
        date_str=the_date.isoformat(),
        date=the_date,  # if provider accepts a date instead
        debug=debug,
    )
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict) and "matchups" in resp:
        return resp.get("matchups") or []
    return resp

# ------------------
# Hitters / Pitchers streak endpoints (GET + POST)
# ------------------
@app.get("/hot_streak_hitters", operation_id="hot_streak_hitters")
def hot_streak_hitters(
    date: Optional[str] = Query(None),
    min_avg: float = Query(0.280),
    games: int = Query(3, ge=1),
    require_hit_each: int = Query(1, ge=0, le=1),
    top_n: int = Query(25, ge=1, le=200),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    return _hot_hitters_fallback(
        the_date, min_avg, games, bool(require_hit_each), bool(debug), top_n=top_n
    )

@app.post("/hot_streak_hitters_post", operation_id="hot_streak_hitters_post")
def hot_streak_hitters_post(req: HotHittersReq):
    the_date = parse_date(req.date)
    return _hot_hitters_fallback(
        the_date, req.min_avg, req.games, req.require_hit_each, bool(req.debug), top_n=req.top_n
    )

@app.get("/cold_streak_hitters", operation_id="cold_streak_hitters")
def cold_streak_hitters(
    date: Optional[str] = Query(None),
    min_avg: float = Query(0.275),
    games: int = Query(2, ge=1),
    require_zero_hit_each: int = Query(1, ge=0, le=1),
    top_n: int = Query(25, ge=1, le=200),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    return _cold_hitters_fallback(
        the_date, min_avg, games, bool(require_zero_hit_each), bool(debug), top_n=top_n
    )

@app.post("/cold_streak_hitters_post", operation_id="cold_streak_hitters_post")
def cold_streak_hitters_post(req: ColdHittersReq):
    the_date = parse_date(req.date)
    return _cold_hitters_fallback(
        the_date, req.min_avg, req.games, req.require_zero_hit_each, bool(req.debug), top_n=req.top_n
    )

@app.get("/pitcher_streaks", operation_id="pitcher_streaks")
def pitcher_streaks(
    date: Optional[str] = Query(None),
    hot_max_era: float = Query(4.00),
    hot_min_ks_each: int = Query(6, ge=0),
    hot_last_starts: int = Query(3, ge=1),
    cold_min_era: float = Query(4.60),
    cold_min_runs_each: int = Query(3, ge=0),
    cold_last_starts: int = Query(2, ge=1),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    return _pitcher_streaks_fallback(
        the_date, hot_max_era, hot_min_ks_each, hot_last_starts,
        cold_min_era, cold_min_runs_each, cold_last_starts, bool(debug)
    )

@app.post("/pitcher_streaks_post", operation_id="pitcher_streaks_post")
def pitcher_streaks_post(req: PitcherStreaksReq):
    the_date = parse_date(req.date)
    return _pitcher_streaks_fallback(
        the_date, req.hot_max_era, req.hot_min_ks_each, req.hot_last_starts,
        req.cold_min_era, req.cold_min_runs_each, req.cold_last_starts, bool(req.debug)
    )

@app.get("/cold_pitchers", operation_id="cold_pitchers")
def cold_pitchers(
    date: Optional[str] = Query(None),
    min_era: float = Query(4.60),
    min_runs_each: int = Query(3, ge=0),
    last_starts: int = Query(2, ge=1),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    # Keep old behavior: if provider lacks this, surface 501
    return safe_call(provider, "cold_pitchers",
        date=the_date, min_era=min_era, min_runs_each=min_runs_each,
        last_starts=last_starts, debug=bool(debug))

@app.post("/cold_pitchers_post", operation_id="cold_pitchers_post")
def cold_pitchers_post(req: ColdPitchersReq):
    the_date = parse_date(req.date)
    return safe_call(provider, "cold_pitchers",
        date=the_date, min_era=req.min_era, min_runs_each=req.min_runs_each,
        last_starts=req.last_starts, debug=bool(req.debug))

# ------------------
# League scan (GET convenience wrapper using provider's league_* methods)
# ------------------
@app.get("/league_scan", operation_id="league_scan")
def league_scan(
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    limit: int = Query(15, ge=1, le=200),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    logs: List[str] = []
    result: Dict[str, Any] = {"date": the_date.isoformat()}

    # Matchups / schedule (provider expects date_str)
    try:
        matchups = _schedule_for_date(the_date, bool(debug))
        logs.append("provider_call:schedule_for_date:ok")
    except HTTPException as e:
        if e.status_code == 501:
            logs.append("provider_call:schedule_for_date:missing")
            matchups = []
        else:
            raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"schedule_for_date failed: {type(e).__name__}: {e}")

    # Hot / Cold hitters via league_* (uses date_str + top_n)
    hot = _hot_hitters_fallback(the_date, min_avg=0.0, games=5, require_hit_each=False, debug=bool(debug), top_n=limit)
    cold = _cold_hitters_fallback(the_date, min_avg=0.0, games=5, require_zero_hit_each=False, debug=bool(debug), top_n=limit)

    # Normalize to lists
    hot_list = hot if isinstance(hot, list) else hot.get("hot_hitters", []) if isinstance(hot, dict) else []
    cold_list = cold if isinstance(cold, list) else cold.get("cold_hitters", []) if isinstance(cold, dict) else []

    result["counts"] = {
        "matchups": len(matchups),
        "hot_hitters": len(hot_list),
        "cold_hitters": len(cold_list),
    }
    result["top"] = {
        "hot_hitters": hot_list[:limit],
        "cold_hitters": cold_list[:limit],
    }
    result["matchups"] = matchups

    if debug == 1:
        result["debug"] = {
            "schedule_source": "schedule_for_date",
            "logs": logs,
            "provider_module": provider_module,
            "provider_class": provider_class,
        }
    return result

# ------------------
# Legacy slate_scan wrapper (kept intact; provider may not implement)
# ------------------
@app.post("/slate_scan_post", response_model=SlateScanResp, operation_id="slate_scan_post")
def slate_scan_post(req: DateOnlyReq):
    the_date = parse_date(req.date)
    resp = safe_call(provider, "slate_scan", date=the_date, debug=bool(req.debug))
    out = {
        "hot_hitters": resp.get("hot_hitters", []),
        "cold_hitters": resp.get("cold_hitters", []),
        "hot_pitchers": resp.get("hot_pitchers", []),
        "cold_pitchers": resp.get("cold_pitchers", []),
        "matchups": resp.get("matchups", []),
    }
    if req.debug == 1:
        out["debug"] = resp.get("debug", {})
    return out

# ------------------
# Include routers from routes/
# ------------------
try:
    from routes.league_scan import router as league_scan_router
    app.include_router(league_scan_router)
except Exception:
    # If the routes module is absent in this deployment, ignore.
    pass

# ------------------
# Run local
# ------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
