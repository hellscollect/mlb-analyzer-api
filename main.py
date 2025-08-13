import os
import importlib
from datetime import datetime, timedelta, date as date_cls
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pytz

APP_NAME = "MLB Analyzer API"

app = FastAPI(
    title=APP_NAME,
    version="1.0.0",
    description="Custom GPT + API for MLB streak analysis",
)

# --- CORS (open by default; tighten if needed) ---
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
def load_provider() -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """
    Load provider from env MLB_PROVIDER = 'path.to.module:ClassName'
    Falls back to providers.simple_provider:SimpleProvider if not set.
    """
    target = os.getenv("MLB_PROVIDER", "providers.simple_provider:SimpleProvider")
    module_path, _, class_name = target.partition(":")
    try:
        module = importlib.import_module(module_path)
        provider_cls = getattr(module, class_name)
        instance = provider_cls()
        return instance, module_path, class_name
    except Exception as e:
        # Return details; /health will reflect not loaded
        return None, None, None

provider, provider_module, provider_class = load_provider()

# ------------------
# Utilities
# ------------------
def parse_date(d: Optional[str]) -> date_cls:
    """
    Accepts: today | yesterday | tomorrow | YYYY-MM-DD | None
    Defaults to today (America/New_York).
    """
    tz = pytz.timezone("America/New_York")
    now = datetime.now(tz).date()
    if not d or d.lower() == "today":
        return now
    s = d.lower()
    if s == "yesterday":
        return now - timedelta(days=1)
    if s == "tomorrow":
        return now + timedelta(days=1)
    # explicit date
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date; use today|yesterday|tomorrow|YYYY-MM-DD")

def safe_call(obj: Any, name: str, *args, **kwargs):
    """
    Call obj.name(*args, **kwargs) if it exists, else raise 501 so we don't mask wiring mistakes.
    """
    if obj is None:
        raise HTTPException(status_code=503, detail="Provider not loaded")
    fn = getattr(obj, name, None)
    if not callable(fn):
        raise HTTPException(status_code=501, detail=f"Provider does not implement {name}()")
    return fn(*args, **kwargs)

# ------------------
# Models (minimal; keep responses flexible while wiring data)
# ------------------
class HealthResp(BaseModel):
    ok: bool
    provider_loaded: bool
    provider_module: Optional[str]
    provider_class: Optional[str]
    now_local: str

class SlateScanResp(BaseModel):
    hot_hitters: List[Dict[str, Any]]
    cold_hitters: List[Dict[str, Any]]
    hot_pitchers: List[Dict[str, Any]]
    cold_pitchers: List[Dict[str, Any]]
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
        zone = pytz.timezone("America/New_York")
    now_str = datetime.now(zone).strftime("%Y-%m-%d %H:%M:%S %Z")
    return HealthResp(
        ok=True,
        provider_loaded=provider is not None,
        provider_module=provider_module,
        provider_class=provider_class,
        now_local=now_str,
    )

# ------------------
# NEW: Raw provider rows endpoint
# ------------------
@app.get(
    "/provider_raw",
    operation_id="provider_raw",
    summary="Inspect raw rows from provider (temporary endpoint)",
    description=(
        "Returns raw hitter and pitcher rows straight from the provider's private "
        "fetch methods, without mapping. Use this to learn the exact data shape "
        "before wiring _map_hitter/_map_pitcher. This endpoint is temporary and "
        "should be removed after mapping is complete."
    ),
)
def provider_raw(
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD (default: today)"),
    limit: Optional[int] = Query(None, ge=1, le=5000, description="Optional soft cap if provider supports it"),
    team: Optional[str] = Query(None, description="Optional team filter if provider supports it"),
    debug: int = Query(0, ge=0, le=1, description="Include provider echo in response when 1"),
):
    the_date = parse_date(date)

    # Call the provider's *private* fetches directly:
    # Expected to exist in ProdProvider skeleton per your setup:
    #   _fetch_hitter_rows(date=..., **kwargs)
    #   _fetch_pitcher_rows(date=..., **kwargs)
    hitters = safe_call(provider, "_fetch_hitter_rows", date=the_date, limit=limit, team=team)
    pitchers = safe_call(provider, "_fetch_pitcher_rows", date=the_date, limit=limit, team=team)

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
        out["debug"] = {
            "args": {"date": the_date.isoformat(), "limit": limit, "team": team},
            "notes": "Direct pass-through of provider private fetch methods.",
        }
    return out

# ------------------
# Existing endpoints (kept stable; thin wrappers)
# ------------------

@app.get("/hot_streak_hitters", operation_id="hot_streak_hitters")
def hot_streak_hitters(
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD (default: today)"),
    min_avg: float = Query(0.280, description="Minimum AVG threshold over the window"),
    games: int = Query(3, ge=1, description="Window size in games"),
    require_hit_each: int = Query(1, ge=0, le=1, description="1 requires ≥1 hit in each game"),
    debug: int = Query(0, ge=0, le=1, description="Include debug block"),
):
    the_date = parse_date(date)
    return safe_call(
        provider, "hot_streak_hitters",
        date=the_date,
        min_avg=min_avg,
        games=games,
        require_hit_each=bool(require_hit_each),
        debug=bool(debug),
    )

@app.get("/cold_streak_hitters", operation_id="cold_streak_hitters")
def cold_streak_hitters(
    date: Optional[str] = Query(None),
    min_avg: float = Query(0.275, description="Minimum seasonal AVG to consider (filters only ‘capable’ hitters)"),
    games: int = Query(2, ge=1, description="Window size in games"),
    require_zero_hit_each: int = Query(1, ge=0, le=1, description="1 requires 0 hits in each game"),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    return safe_call(
        provider, "cold_streak_hitters",
        date=the_date,
        min_avg=min_avg,
        games=games,
        require_zero_hit_each=bool(require_zero_hit_each),
        debug=bool(debug),
    )

@app.get("/pitcher_streaks", operation_id="pitcher_streaks")
def pitcher_streaks(
    date: Optional[str] = Query(None),
    hot_max_era: float = Query(4.00, description="Hot: ERA ≤ this over last N starts"),
    hot_min_ks_each: int = Query(6, ge=0, description="Hot: Ks in each of last N starts"),
    hot_last_starts: int = Query(3, ge=1, description="Hot: count of recent starts"),
    cold_min_era: float = Query(4.60, description="Cold: ERA ≥ this over last N starts"),
    cold_min_runs_each: int = Query(3, ge=0, description="Cold: runs allowed in each of last N starts"),
    cold_last_starts: int = Query(2, ge=1, description="Cold: count of recent starts"),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    return safe_call(
        provider, "pitcher_streaks",
        date=the_date,
        hot_max_era=hot_max_era,
        hot_min_ks_each=hot_min_ks_each,
        hot_last_starts=hot_last_starts,
        cold_min_era=cold_min_era,
        cold_min_runs_each=cold_min_runs_each,
        cold_last_starts=cold_last_starts,
        debug=bool(debug),
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
        date=the_date,
        min_era=min_era,
        min_runs_each=min_runs_each,
        last_starts=last_starts,
        debug=bool(debug),
    )

@app.get("/slate_scan", response_model=SlateScanResp, operation_id="slate_scan")
def slate_scan(
    date: Optional[str] = Query(None),
    debug: int = Query(0, ge=0, le=1, description="Set to 1 for provider debug echo"),
):
    the_date = parse_date(date)
    resp = safe_call(provider, "slate_scan", date=the_date, debug=bool(debug))
    # Ensure structure is consistent even if provider returns partials
    out = {
        "hot_hitters": resp.get("hot_hitters", []),
        "cold_hitters": resp.get("cold_hitters", []),
        "hot_pitchers": resp.get("hot_pitchers", []),
        "cold_pitchers": resp.get("cold_pitchers", []),
        "matchups": resp.get("matchups", []),
    }
    if debug == 1:
        out["debug"] = resp.get("debug", {"note": "No debug payload from provider"})
    return out

# ------------
# Local dev
# ------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
