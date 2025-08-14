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
ET_TZ = pytz.timezone("America/New_York")

app = FastAPI(
    title=APP_NAME,
    version="1.0.5",
    description="Custom GPT + API for MLB streak analysis",
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
    tz = ET_TZ
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

# Kept for schema compatibility in Actions/GPT
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

# ------------------
# Health
# ------------------
@app.get("/health", response_model=HealthResp, operation_id="health")
def health(tz: str = Query("America/New_York", description="IANA timezone for timestamp echo")):
    try:
        zone = pytz.timezone(tz)
    except Exception:
        zone = ET_TZ
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
# Raw provider rows endpoint (temporary)
# ------------------
@app.get("/provider_raw", operation_id="provider_raw")
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

# ------------------
# Existing endpoints (GET)
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
    return safe_call(provider, "hot_streak_hitters",
        date=the_date, min_avg=min_avg, games=games,
        require_hit_each=bool(require_hit_each), debug=bool(debug))

@app.get("/cold_streak_hitters", operation_id="cold_streak_hitters")
def cold_streak_hitters(
    date: Optional[str] = Query(None),
    min_avg: float = Query(0.275),
    games: int = Query(2, ge=1),
    require_zero_hit_each: int = Query(1, ge=0, le=1),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    return safe_call(provider, "cold_streak_hitters",
        date=the_date, min_avg=min_avg, games=games,
        require_zero_hit_each=bool(require_zero_hit_each), debug=bool(debug))

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
    return safe_call(provider, "pitcher_streaks",
        date=the_date, hot_max_era=hot_max_era, hot_min_ks_each=hot_min_ks_each,
        hot_last_starts=hot_last_starts, cold_min_era=cold_min_era,
        cold_min_runs_each=cold_min_runs_each, cold_last_starts=cold_last_starts,
        debug=bool(debug))

@app.get("/cold_pitchers", operation_id="cold_pitchers")
def cold_pitchers(
    date: Optional[str] = Query(None),
    min_era: float = Query(4.60),
    min_runs_each: int = Query(3, ge=0),
    last_starts: int = Query(2, ge=1),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    return safe_call(provider, "cold_pitchers",
        date=the_date, min_era=min_era, min_runs_each=min_runs_each,
        last_starts=last_starts, debug=bool(debug))

# ------------------
# POST wrappers for Actions
# ------------------
class ProviderRawReq(BaseModel):
    date: Optional[str] = None
    limit: Optional[int] = None
    team: Optional[str] = None
    debug: int = 0

@app.post("/provider_raw_post", operation_id="provider_raw_post")
def provider_raw_post(req: ProviderRawReq):
    the_date = parse_date(req.date)
    hitters = _smart_call_fetch("_fetch_hitter_rows", the_date, req.limit, req.team)
    pitchers = _smart_call_fetch("_fetch_pitcher_rows", the_date, req.limit, req.team)
    out = {
        "hitters_raw": hitters,
        "pitchers_raw": pitchers,
    }
    if req.debug == 1:
        out["debug"] = {"requested": req.model_dump()}
    return out

class HotHittersReq(BaseModel):
    date: Optional[str] = None
    min_avg: float = 0.280
    games: int = 3
    require_hit_each: bool = True
    debug: int = 0

@app.post("/hot_streak_hitters_post", operation_id="hot_streak_hitters_post")
def hot_streak_hitters_post(req: HotHittersReq):
    the_date = parse_date(req.date)
    return safe_call(provider, "hot_streak_hitters",
        date=the_date, min_avg=req.min_avg, games=req.games,
        require_hit_each=req.require_hit_each, debug=bool(req.debug))

class ColdHittersReq(BaseModel):
    date: Optional[str] = None
    min_avg: float = 0.275
    games: int = 2
    require_zero_hit_each: bool = True
    debug: int = 0

@app.post("/cold_streak_hitters_post", operation_id="cold_streak_hitters_post")
def cold_streak_hitters_post(req: ColdHittersReq):
    the_date = parse_date(req.date)
    return safe_call(provider, "cold_streak_hitters",
        date=the_date, min_avg=req.min_avg, games=req.games,
        require_zero_hit_each=req.require_zero_hit_each, debug=bool(req.debug))

class PitcherStreaksReq(BaseModel):
    date: Optional[str] = None
    hot_max_era: float = 4.0
    hot_min_ks_each: int = 6
    hot_last_starts: int = 3
    cold_min_era: float = 4.6
    cold_min_runs_each: int = 3
    cold_last_starts: int = 2
    debug: int = 0

@app.post("/pitcher_streaks_post", operation_id="pitcher_streaks_post")
def pitcher_streaks_post(req: PitcherStreaksReq):
    the_date = parse_date(req.date)
    return safe_call(provider, "pitcher_streaks",
        date=the_date, hot_max_era=req.hot_max_era, hot_min_ks_each=req.hot_min_ks_each,
        hot_last_starts=req.hot_last_starts, cold_min_era=req.cold_min_era,
        cold_min_runs_each=req.cold_min_runs_each, cold_last_starts=req.cold_last_starts,
        debug=bool(req.debug))

class ColdPitchersReq(BaseModel):
    date: Optional[str] = None
    min_era: float = 4.6
    min_runs_each: int = 3
    last_starts: int = 2
    debug: int = 0

@app.post("/cold_pitchers_post", operation_id="cold_pitchers_post")
def cold_pitchers_post(req: ColdPitchersReq):
    the_date = parse_date(req.date)
    return safe_call(provider, "cold_pitchers",
        date=the_date, min_era=req.min_era, min_runs_each=req.min_runs_each,
        last_starts=req.last_starts, debug=bool(req.debug))

class DateOnlyReq(BaseModel):
    date: Optional[str] = None
    debug: int = 0

@app.post("/slate_scan_post", response_model=SlateScanResp, operation_id="slate_scan_post")
def slate_scan_post(req: DateOnlyReq):
    # Unchanged; will 501 if provider doesn't have slate_scan()
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
# Helpers for league scan (no slate dependency)
# ------------------
def _safe_hot(date_obj: date_cls, debug: bool) -> List[Dict[str, Any]]:
    try:
        return safe_call(provider, "hot_streak_hitters",
                         date=date_obj, min_avg=0.280, games=3,
                         require_hit_each=True, debug=debug)
    except Exception:
        return []

def _safe_cold(date_obj: date_cls, debug: bool) -> List[Dict[str, Any]]:
    try:
        return safe_call(provider, "cold_streak_hitters",
                         date=date_obj, min_avg=0.275, games=2,
                         require_zero_hit_each=True, debug=debug)
    except Exception:
        return []

# ------------------
# /league_scan_post WITHOUT slate_scan dependency
# ------------------
@app.post("/league_scan_post", response_model=LeagueScanResp, operation_id="league_scan_post_v2")
def league_scan_post(req: LeagueScanReq):
    """
    Returns hot/cold hitters for the requested date (no slate/matchups dependency).
    If both lists are empty for the requested date, auto-tries tomorrow.
    matchups is an empty list until the provider exposes schedule/matchups.
    """
    primary_date = parse_date(req.date)
    tomorrow_date = primary_date + timedelta(days=1)
    debug_flag = bool(req.debug)

    def run_for(d: date_cls) -> Dict[str, Any]:
        hot = _safe_hot(d, debug_flag)
        cold = _safe_cold(d, debug_flag)
        top_n = int(req.top_n) if req.top_n and req.top_n > 0 else 15
        if len(hot) > top_n: hot = hot[:top_n]
        if len(cold) > top_n: cold = cold[:top_n]
        return {
            "date": d.isoformat(),
            "counts": {
                "matchups": 0,
                "hot_hitters": len(hot),
                "cold_hitters": len(cold),
            },
            "top": {
                "hot_hitters": hot,
                "cold_hitters": cold,
            },
            "matchups": [],
            "debug": {
                "source": "hot_cold_only",
                "note": "Provider lacks slate/matchups; returning hitters only.",
                "requested_top_n": top_n,
            },
        }

    out_primary = run_for(primary_date)
    if out_primary["counts"]["hot_hitters"] > 0 or out_primary["counts"]["cold_hitters"] > 0:
        return out_primary

    out_tomorrow = run_for(tomorrow_date)
    if out_tomorrow["counts"]["hot_hitters"] > 0 or out_tomorrow["counts"]["cold_hitters"] > 0:
        out_tomorrow["debug"]["fallback"] = "tomorrow_used_no_hitters_today"
        return out_tomorrow

    out_primary["debug"]["fallback"] = "no_hitters_today_or_tomorrow"
    return out_primary

# ------------------
# Run local
# ------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
