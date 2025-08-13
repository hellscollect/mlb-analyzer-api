from __future__ import annotations

import importlib
import inspect
import os
from datetime import date as _date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import pytz
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# IMPORTANT: models live in models.py to avoid circular imports
from models import Hitter, Pitcher

APP_VERSION = "1.0.5"  # accepts bool OR int flags in query params

# ---------- FastAPI app ----------
app = FastAPI(
    title="MLB Analyzer API",
    version=APP_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "OPTIONS"],
    allow_headers=["*"],
)

# ---------- helpers ----------
def _now_in_tz(tz: str) -> datetime:
    try:
        tzobj = pytz.timezone(tz)
    except Exception:
        tzobj = pytz.timezone("America/New_York")
    return datetime.now(tzobj)

def _parse_date(s: str, tz: str = "America/New_York") -> _date:
    s = (s or "").strip().lower()
    now = _now_in_tz(tz)
    if s in ("today", ""):
        return now.date()
    if s == "yesterday":
        return (now - timedelta(days=1)).date()
    if s == "tomorrow":
        return (now + timedelta(days=1)).date()
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date; use today|yesterday|tomorrow|YYYY-MM-DD")

def _as_bool(v: Union[bool, int, str, None], default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v != 0
    if isinstance(v, str):
        t = v.strip().lower()
        return t in {"1", "true", "t", "yes", "y", "on"}
    return default

def _provider_path_and_class() -> Tuple[str, str]:
    env = os.getenv("MLB_PROVIDER", "").strip()
    if ":" in env:
        module_path, class_name = env.split(":", 1)
        return module_path.strip(), class_name.strip()
    # sane default
    return "providers.statsapi_provider", "StatsApiProvider"

def _load_provider() -> Tuple[Optional[Any], Optional[str]]:
    module_path, class_name = _provider_path_and_class()
    try:
        mod = importlib.import_module(module_path)
        cls = getattr(mod, class_name)
        return cls(), None
    except Exception as e:
        print(f"[provider-load-error] MLB_PROVIDER='{module_path}:{class_name}' -> {type(e).__name__}: {e}")
        return None, f"{type(e).__name__}: {e}"

# single provider instance
PROVIDER, PROVIDER_ERR = _load_provider()

def _ensure_provider():
    if PROVIDER is None:
        raise HTTPException(status_code=500, detail=f"Provider failed to load: {PROVIDER_ERR}")

def _limit_kwargs(func, **kwargs) -> Dict[str, Any]:
    """Pass only supported kwargs by inspecting signature names."""
    try:
        params = set(inspect.signature(func).parameters.keys())
        return {k: v for k, v in kwargs.items() if k in params}
    except Exception:
        return kwargs

# ---------- API models for responses (thin wrappers) ----------
class HealthResp(BaseModel):
    ok: bool
    provider_loaded: bool
    provider_module: Optional[str] = None
    provider_class: Optional[str] = None
    provider_error: Optional[str] = None
    now_local: str

# ---------- endpoints ----------
@app.get("/health", response_model=HealthResp)
def health(tz: str = Query(default="America/New_York")):
    now_local = _now_in_tz(tz).strftime("%Y-%m-%d %H:%M:%S %Z")
    module_path, class_name = _provider_path_and_class()
    return HealthResp(
        ok=True,
        provider_loaded=PROVIDER is not None,
        provider_module=module_path if PROVIDER else None,
        provider_class=class_name if PROVIDER else None,
        provider_error=PROVIDER_ERR,
        now_local=now_local,
    )

@app.get("/provider_raw")
def provider_raw(
    date: str = Query(..., description="today|yesterday|tomorrow|YYYY-MM-DD"),
    limit: Optional[int] = Query(None, ge=1, le=100),
    team: Optional[str] = Query(None),
    debug: Union[bool, int] = Query(False, description="true/false or 1/0"),
):
    _ensure_provider()
    gdate = _parse_date(date)
    hitters_raw: Iterable[Dict[str, Any]] = []
    pitchers_raw: Iterable[Dict[str, Any]] = []

    # call private fetches if present
    if hasattr(PROVIDER, "_fetch_hitter_rows"):
        fn = getattr(PROVIDER, "_fetch_hitter_rows")
        hitters_raw = list(fn(**_limit_kwargs(fn, game_date=gdate, limit=limit, team=team)))
    if hasattr(PROVIDER, "_fetch_pitcher_rows"):
        fn = getattr(PROVIDER, "_fetch_pitcher_rows")
        pitchers_raw = list(fn(**_limit_kwargs(fn, game_date=gdate, limit=limit, team=team)))

    module_path, class_name = _provider_path_and_class()
    return {
        "meta": {"provider_module": module_path, "provider_class": class_name, "date": gdate.isoformat()},
        "hitters_raw": hitters_raw,
        "pitchers_raw": pitchers_raw,
        "debug": {
            "notes": "Called provider private fetches with signature-aware kwargs.",
            "hitter_fetch_params": list(inspect.signature(getattr(PROVIDER, "_fetch_hitter_rows")).parameters.keys()) if hasattr(PROVIDER, "_fetch_hitter_rows") else [],
            "pitcher_fetch_params": list(inspect.signature(getattr(PROVIDER, "_fetch_pitcher_rows")).parameters.keys()) if hasattr(PROVIDER, "_fetch_pitcher_rows") else [],
            "requested_args": {"date": gdate.isoformat(), "limit": limit, "team": team},
            "provider_config": getattr(PROVIDER, "__dict__", {}),
        } if _as_bool(debug) else None,
    }

@app.get("/hot_streak_hitters")
def hot_streak_hitters(
    date: str = Query(...),
    min_avg: float = Query(0.28),
    games: int = Query(3, ge=1, le=10),
    require_hit_each: Union[bool, int] = Query(True, description="true/false or 1/0"),
    debug: Union[bool, int] = Query(False, description="true/false or 1/0"),
):
    _ensure_provider()
    gdate = _parse_date(date)
    return PROVIDER.hot_streak_hitters(
        gdate, min_avg=min_avg, games=games, require_hit_each=_as_bool(require_hit_each), debug=_as_bool(debug)
    )

@app.get("/cold_streak_hitters")
def cold_streak_hitters(
    date: str = Query(...),
    min_avg: float = Query(0.275),
    games: int = Query(2, ge=1, le=10),
    require_zero_hit_each: Union[bool, int] = Query(True, description="true/false or 1/0"),
    debug: Union[bool, int] = Query(False, description="true/false or 1/0"),
):
    _ensure_provider()
    gdate = _parse_date(date)
    return PROVIDER.cold_streak_hitters(
        gdate, min_avg=min_avg, games=games, require_zero_hit_each=_as_bool(require_zero_hit_each), debug=_as_bool(debug)
    )

@app.get("/pitcher_streaks")
def pitcher_streaks(
    date: str = Query(...),
    hot_max_era: float = Query(4.00),
    hot_min_ks_each: int = Query(6, ge=0),
    hot_last_starts: int = Query(3, ge=1, le=10),
    cold_min_era: float = Query(4.60),
    cold_min_runs_each: int = Query(3, ge=0),
    cold_last_starts: int = Query(2, ge=1, le=10),
    debug: Union[bool, int] = Query(False, description="true/false or 1/0"),
):
    _ensure_provider()
    gdate = _parse_date(date)
    return PROVIDER.pitcher_streaks(
        gdate,
        hot_max_era=hot_max_era,
        hot_min_ks_each=hot_min_ks_each,
        hot_last_starts=hot_last_starts,
        cold_min_era=cold_min_era,
        cold_min_runs_each=cold_min_runs_each,
        cold_last_starts=cold_last_starts,
        debug=_as_bool(debug),
    )

@app.get("/cold_pitchers")
def cold_pitchers(
    date: str = Query(...),
    min_era: float = Query(4.60),
    min_runs_each: int = Query(3, ge=0),
    last_starts: int = Query(2, ge=1, le=10),
    debug: Union[bool, int] = Query(False, description="true/false or 1/0"),
):
    _ensure_provider()
    gdate = _parse_date(date)
    return PROVIDER.cold_pitchers(
        gdate, min_era=min_era, min_runs_each=min_runs_each, last_starts=last_starts, debug=_as_bool(debug)
    )

@app.get("/slate_scan")
def slate_scan(
    date: str = Query(...),
    debug: Union[bool, int] = Query(False, description="true/false or 1/0"),
):
    _ensure_provider()
    gdate = _parse_date(date)
    return PROVIDER.slate_scan(gdate, debug=_as_bool(debug))


if __name__ == "__main__":
    # Render runs your start command; keep this for local dev.
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
