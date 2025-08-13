# main.py — full file with POST wrappers for Actions reliability

import os
import importlib
import inspect
from datetime import datetime, timedelta, date as date_cls
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pytz

# Ensure OpenAPI has servers for GPT Actions import
from fastapi.openapi.utils import get_openapi

APP_NAME = "MLB Analyzer API"
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")

app = FastAPI(
    title=APP_NAME,
    version="1.0.4",
    description="Custom GPT + API for MLB streak analysis",
)

# Inject `servers` into OpenAPI so GPT Actions can import the spec
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    if PUBLIC_BASE_URL:
        schema["servers"] = [{"url": PUBLIC_BASE_URL}]
    app.openapi_schema = schema
    return app.openapi_schema

app.openapi = custom_openapi

# --- CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],   # includes POST
    allow_headers=["*"],
)

# ------------------
# Provider loading
# ------------------
_last_provider_error: Optional[str] = None

def load_provider() -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """
    Load provider from env MLB_PROVIDER = 'path.to.module:ClassName'
    Falls back to providers.simple_provider:SimpleProvider if not set.
    """
    global _last_provider_error
    target = os.getenv("MLB_PROVIDER", "providers.simple_provider:SimpleProvider")
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

def safe_call(obj: Any, name: str, *args, **kwargs):
    if obj is None:
        raise HTTPException(status_code=503, detail=f"Provider not loaded: {_last_provider_error or 'unknown error'}")
    fn = getattr(obj, name, None)
    if not callable(fn):
        raise HTTPException(status_code=501, detail=f"Provider does not implement {name}()")
    return fn(*args, **kwargs)

def _smart_call_fetch(method_name: str, the_date: date_cls, limit: Optional[int], team: Optional[str]):
    """
    Call provider private fetch with whatever parameters it supports.
    Tries kwargs ('date' or 'game_date', 'limit', 'team'); falls back to positional.
    """
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
# Models
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

# --- Request bodies for POST wrappers ---
class DateOnlyReq(BaseModel):
    date: Optional[str] = None
    debug: int = 0

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
    debug: int = 0

class ColdHittersReq(BaseModel):
    date: Optional[str] = None
    min_avg: float = 0.275
    games: int = 2
    require_zero_hit_each: bool = True
    debug: int = 0

class PitcherStreaksReq(BaseModel):
    date: Optional[str] = None
    hot_max_era: float = 4.00
    hot_min_ks_each: int = 6
    hot_last_starts: int = 3
    cold_min_era: float = 4.60
    cold_min_runs_each: int = 3
    cold_last_starts: int = 2
    debug: int = 0

class ColdPitchersReq(BaseModel):
    date: Optional[str] = None
    min_era: float = 4.60
    min_runs_each: int = 3
    last_starts: int = 2
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
# Raw provider rows (GET) + POST wrapper
# ------------------
@app.get(
    "/provider_raw",
    operation_id="provider_raw",
    summary="Inspect raw rows from provider (temporary endpoint)",
    description="Returns raw hitter and pitcher rows straight from the provider's private fetch methods, without mapping."
)
def provider_raw(
    date: Optional[str] = Query(None),
    limit: Optional[int] = Query(None, ge=1, le=5000),
    team: Optional[str] = Query(None),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
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
            "hitter_fetch_params": list(inspect.signature(getattr(provider, "_fetch_hitter_rows")).parameters.keys()) if hasattr(provider, "_fetch_hitter_rows") else None,
            "pitcher_fetch_params": list(inspect.signature(getattr(provider, "_fetch_pitcher_rows")).parameters.keys()) if hasattr(provider, "_fetch_pitcher_rows") else None,
            "requested_args": {"date": the_date.isoformat(), "limit": limit, "team": team},
            "provider_config": {
                "fake_mode": os.getenv("PROD_USE_FAKE", "0") in ("1", "true", "True", "YES", "yes"),
                "data_api_base": provider_base or "(unset)",
                "has_api_key": provider_key_present,
            }
        }
    return out

@app.post("/provider_raw_post", operation_id="provider_raw_post")
def provider_raw_post(body: ProviderRawReq):
    the_date = parse_date(body.date)
    return provider_raw(
        date=the_date.isoformat(),
        limit=body.limit,
        team=body.team,
        debug=body.debug,
    )

# ------------------
# Existing GET endpoints + POST wrappers
# ------------------
@app.get("/hot_streak_hitters", operation_id="hot_streak_hitters")
def hot_streak_hitters(
    date: Optional[str] = Query(None),
    min_avg: float = Query(0.280),
    games: int = Query(3, ge=1),
    require_hit_each: int = Query(1, ge=0, le=1),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    return safe_call(
        provider, "hot_streak_hitters",
        date=the_date, min_avg=min_avg, games=games,
        require_hit_each=bool(require_hit_each), debug=bool(debug)
    )

@app.post("/hot_streak_hitters_post", operation_id="hot_streak_hitters_post")
def hot_streak_hitters_post(body: HotHittersReq):
    the_date = parse_date(body.date)
    return safe_call(
        provider, "hot_streak_hitters",
        date=the_date, min_avg=body.min_avg, games=body.games,
        require_hit_each=bool(body.require_hit_each), debug=bool(body.debug)
    )

@app.get("/cold_streak_hitters", operation_id="cold_streak_hitters")
def cold_streak_hitters(
    date: Optional[str] = Query(None),
    min_avg: float = Query(0.275),
    games: int = Query(2, ge=1),
    require_zero_hit_each: int = Query(1, ge=0, le=1),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    return safe_call(
        provider, "cold_streak_hitters",
        date=the_date, min_avg=min_avg, games=games,
        require_zero_hit_each=bool(require_zero_hit_each), debug=bool(debug)
    )

@app.post("/cold_streak_hitters_post", operation_id="cold_streak_hitters_post")
def cold_streak_hitters_post(body: ColdHittersReq):
    the_date = parse_date(body.date)
    return safe_call(
        provider, "cold_streak_hitters",
        date=the_date, min_avg=body.min_avg, games=body.games,
        require_zero_hit_each=bool(body.require_zero_hit_each), debug=bool(body.debug)
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
    return safe_call(
        provider, "pitcher_streaks",
        date=the_date, hot_max_era=hot_max_era, hot_min_ks_each=hot_min_ks_each,
        hot_last_starts=hot_last_starts, cold_min_era=cold_min_era,
        cold_min_runs_each=cold_min_runs_each, cold_last_starts=cold_last_starts,
        debug=bool(debug)
    )

@app.post("/pitcher_streaks_post", operation_id="pitcher_streaks_post")
def pitcher_streaks_post(body: PitcherStreaksReq):
    the_date = parse_date(body.date)
    return safe_call(
        provider, "pitcher_streaks",
        date=the_date, hot_max_era=body.hot_max_era, hot_min_ks_each=body.hot_min_ks_each,
        hot_last_starts=body.hot_last_starts, cold_min_era=body.cold_min_era,
        cold_min_runs_each=body.cold_min_runs_each, cold_last_starts=body.cold_last_starts,
        debug=bool(body.debug)
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
    return safe_call(
        provider, "cold_pitchers",
        date=the_date, min_era=min_era, min_runs_each=min_runs_each,
        last_starts=last_starts, debug=bool(debug)
    )

@app.post("/cold_pitchers_post", operation_id="cold_pitchers_post")
def cold_pitchers_post(body: ColdPitchersReq):
    the_date = parse_date(body.date)
    return safe_call(
        provider, "cold_pitchers",
        date=the_date, min_era=body.min_era, min_runs_each=body.min_runs_each,
        last_starts=body.last_starts, debug=bool(body.debug)
    )

@app.get("/slate_scan", response_model=SlateScanResp, operation_id="slate_scan")
def slate_scan(
    date: Optional[str] = Query(None),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    resp = safe_call(provider, "slate_scan", date=the_date, debug=bool(debug))
    out = {
        "hot_hitters": resp.get("hot_hitters", []),
        "cold_hitters": resp.get("cold_hitters", []),
        "hot_pitchers": resp.get("hot_pitchers", []),
        "cold_pitchers": resp.get("cold_pitchers", []),
        "matchups": resp.get("matchups", []),
    }
    if debug == 1:
        out["debug"] = resp.get("debug", {})
    return out

@app.post("/slate_scan_post", response_model=SlateScanResp, operation_id="slate_scan_post")
def slate_scan_post(body: DateOnlyReq):
    return slate_scan(date=body.date, debug=body.debug)

# ------------------
# Run local
# ------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
