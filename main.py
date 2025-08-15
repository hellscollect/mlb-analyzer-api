import os
import importlib
import inspect
from datetime import datetime, timedelta, date as date_cls
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pytz
import requests

APP_NAME = "MLB Analyzer API"
APP_VERSION = "1.1.6"

# --- Force UTF-8 on every JSON response (prevents mojibake in some clients) ---
class UTF8JSONResponse(JSONResponse):
    media_type = "application/json; charset=utf-8"

# --- Server URL for OpenAPI (required by GPT Actions) ---
EXTERNAL_URL = (
    os.getenv("RENDER_EXTERNAL_URL")
    or "https://mlb-analyzer-api.onrender.com"
)

app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    description="Custom GPT + API for MLB streak analysis",
    servers=[{"url": EXTERNAL_URL}],
    openapi_url="/openapi.json",
    default_response_class=UTF8JSONResponse,
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
    If that fails (e.g., positional-only args), retry with positional args
    in the provider's parameter order using any provided values.
    """
    if fn is None:
        raise HTTPException(status_code=501, detail="Provider method missing")
    try:
        sig = inspect.signature(fn)
        allowed_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
        try:
            return fn(**allowed_kwargs)
        except TypeError:
            # Retry positionally in declared parameter order
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

# --- Mojibake fixer (latin1->utf8 for typical "Ã", "Â" artifacts) ---
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
    top_n: Optional[int] = None  # prefer this
    limit: Optional[int] = None  # accept this too
    debug: int = 0

class ColdHittersReq(BaseModel):
    date: Optional[str] = None
    min_avg: float = 0.275
    games: int = 2
    require_zero_hit_each: bool = True
    top_n: Optional[int] = None
    limit: Optional[int] = None
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
# Health + simple root
# ------------------
@app.get("/", operation_id="root")
def root():
    tz = pytz.timezone("America/New_York")
    return {
        "app": APP_NAME,
        "version": APP_VERSION,
        "date": datetime.now(tz).date().isoformat(),
        "docs": f"{EXTERNAL_URL}/docs",
        "health": f"{EXTERNAL_URL}/health?tz=America/New_York",
    }

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
        "hitters_raw": _deep_fix(hitters),
        "pitchers_raw": _deep_fix(pitchers),
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
    out = {"hitters_raw": _deep_fix(hitters), "pitchers_raw": _deep_fix(pitchers)}
    if req.debug == 1:
        out["debug"] = {"requested": req.model_dump()}
    return out

# ------------------
# Compatibility wrappers (adapters for league_* + date_str + top_n)
# ------------------
def _hot_hitters_fallback(
    the_date: date_cls,
    min_avg: float,
    games: int,
    require_hit_each: bool,
    debug: bool,
    top_n: int = 25,
):
    direct = _callable(provider, "hot_streak_hitters")
    if direct:
        res = _call_with_sig(
            direct,
            date=the_date,
            min_avg=min_avg,
            games=games,
            require_hit_each=require_hit_each,
            debug=debug,
        )
        return _take_n(_deep_fix(res), top_n)

    league = _callable(provider, "league_hot_hitters")
    if league:
        res = _call_with_sig(
            league,
            date_str=the_date.isoformat(),
            date=the_date,
            top_n=top_n,
            n=top_n,
            limit=top_n,
            debug=debug,
        )
        return _take_n(_deep_fix(res), top_n)

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
        res = _call_with_sig(
            direct,
            date=the_date,
            min_avg=min_avg,
            games=games,
            require_zero_hit_each=require_zero_hit_each,
            debug=debug,
        )
        return _take_n(_deep_fix(res), top_n)

    league = _callable(provider, "league_cold_hitters")
    if league:
        res = _call_with_sig(
            league,
            date_str=the_date.isoformat(),
            date=the_date,
            top_n=top_n,
            n=top_n,
            limit=top_n,
            debug=debug,
        )
        return _take_n(_deep_fix(res), top_n)

    raise HTTPException(status_code=501, detail="Provider does not implement cold_streak_hitters() or league_cold_hitters().")

def _pitcher_streaks_fallback(the_date: date_cls, hot_max_era: float, hot_min_ks_each: int, hot_last_starts: int,
                              cold_min_era: float, cold_min_runs_each: int, cold_last_starts: int, debug: bool):
    direct = _callable(provider, "pitcher_streaks")
    if direct:
        return _deep_fix(_call_with_sig(
            direct,
            date=the_date,
            hot_max_era=hot_max_era,
            hot_min_ks_each=hot_min_ks_each,
            hot_last_starts=hot_last_starts,
            cold_min_era=cold_min_era,
            cold_min_runs_each=cold_min_runs_each,
            cold_last_starts=cold_last_starts,
            debug=debug,
        ))
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
    resp = _call_with_sig(
        sched_fn,
        date_str=the_date.isoformat(),
        date=the_date,
        debug=debug,
    )
    if isinstance(resp, list):
        return _deep_fix(resp)
    if isinstance(resp, dict) and "matchups" in resp:
        return _deep_fix(resp.get("matchups") or [])
    return _deep_fix(resp)

# ------------------
# Hitters / Pitchers streak endpoints (GET + POST)
# ------------------
@app.get("/hot_streak_hitters", operation_id="hot_streak_hitters")
def hot_streak_hitters(
    date: Optional[str] = Query(None),
    min_avg: float = Query(0.280),
    games: int = Query(3, ge=1),
    require_hit_each: int = Query(1, ge=0, le=1),
    limit: Optional[int] = Query(None, ge=1, le=200),   # keep old client param
    top_n: Optional[int] = Query(None, ge=1, le=200),   # also accept top_n
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    n = top_n or limit or 25
    data = _hot_hitters_fallback(
        the_date, min_avg, games, bool(require_hit_each), bool(debug), top_n=n
    )
    return data

@app.post("/hot_streak_hitters_post", operation_id="hot_streak_hitters_post")
def hot_streak_hitters_post(req: HotHittersReq):
    the_date = parse_date(req.date)
    n = (req.top_n or req.limit or 25)
    data = _hot_hitters_fallback(
        the_date, req.min_avg, req.games, req.require_hit_each, bool(req.debug), top_n=n
    )
    return data

@app.get("/cold_streak_hitters", operation_id="cold_streak_hitters")
def cold_streak_hitters(
    date: Optional[str] = Query(None),
    min_avg: float = Query(0.275),
    games: int = Query(2, ge=1),
    require_zero_hit_each: int = Query(1, ge=0, le=1),
    limit: Optional[int] = Query(None, ge=1, le=200),
    top_n: Optional[int] = Query(None, ge=1, le=200),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    n = top_n or limit or 25
    data = _cold_hitters_fallback(
        the_date, min_avg, games, bool(require_zero_hit_each), bool(debug), top_n=n
    )
    return data

@app.post("/cold_streak_hitters_post", operation_id="cold_streak_hitters_post")
def cold_streak_hitters_post(req: ColdHittersReq):
    the_date = parse_date(req.date)
    n = (req.top_n or req.limit or 25)
    data = _cold_hitters_fallback(
        the_date, req.min_avg, req.games, req.require_zero_hit_each, bool(req.debug), top_n=n
    )
    return data

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
    data = _pitcher_streaks_fallback(
        the_date, hot_max_era, hot_min_ks_each, hot_last_starts,
        cold_min_era, cold_min_runs_each, cold_last_starts, bool(debug)
    )
    return data

@app.post("/pitcher_streaks_post", operation_id="pitcher_streaks_post")
def pitcher_streaks_post(req: PitcherStreaksReq):
    the_date = parse_date(req.date)
    data = _pitcher_streaks_fallback(
        the_date, req.hot_max_era, req.hot_min_ks_each, req.hot_last_starts,
        req.cold_min_era, req.cold_min_runs_each, req.cold_last_starts, bool(req.debug)
    )
    return data

@app.get("/cold_pitchers", operation_id="cold_pitchers")
def cold_pitchers(
    date: Optional[str] = Query(None),
    min_era: float = Query(4.60),
    min_runs_each: int = Query(3, ge=0),
    last_starts: int = Query(2, ge=1),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    data = safe_call(provider, "cold_pitchers",
        date=the_date, min_era=min_era, min_runs_each=min_runs_each,
        last_starts=last_starts, debug=bool(debug))
    return _deep_fix(data)

@app.post("/cold_pitchers_post", operation_id="cold_pitchers_post")
def cold_pitchers_post(req: ColdPitchersReq):
    the_date = parse_date(req.date)
    data = safe_call(provider, "cold_pitchers",
        date=the_date, min_era=req.min_era, min_runs_each=req.min_runs_each,
        last_starts=req.last_starts, debug=bool(req.debug))
    return _deep_fix(data)

# ------------------
# League scan (GET convenience wrapper using provider’s league_* methods)
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

    # Hot / Cold hitters via adapters
    hot = _hot_hitters_fallback(the_date, min_avg=0.0, games=5, require_hit_each=False, debug=bool(debug), top_n=limit)
    cold = _cold_hitters_fallback(the_date, min_avg=0.0, games=5, require_zero_hit_each=False, debug=bool(debug), top_n=limit)

    hot_list = hot if isinstance(hot, list) else hot.get("hot_hitters", []) if isinstance(hot, dict) else []
    cold_list = cold if isinstance(cold, list) else cold.get("cold_hitters", []) if isinstance(cold, dict) else []

    result["counts"] = {
        "matchups": len(matchups),
        "hot_hitters": len(hot_list),
        "cold_hitters": len(cold_list),
    }
    result["top"] = {
        "hot_hitters": _deep_fix(hot_list[:limit]),
        "cold_hitters": _deep_fix(cold_list[:limit]),
    }
    result["matchups"] = _deep_fix(matchups)

    if debug == 1:
        result["debug"] = {
            "schedule_source": "schedule_for_date",
            "logs": logs,
            "provider_module": provider_module,
            "provider_class": provider_class,
        }
    return result

# ------------------
# Cold hitters by GAME (strict, from MLB Stats API directly)
# ------------------
def _mlb_get(url: str) -> Dict[str, Any]:
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MLB Stats API error: {type(e).__name__}: {e}")

def _mlb_schedule(the_date: date_cls) -> Dict[str, Any]:
    return _mlb_get(f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={the_date.isoformat()}")

def _find_game_pk(sched: Dict[str, Any], away_name: str, home_name: str) -> Optional[int]:
    dates = sched.get("dates") or []
    for d in dates:
        for g in d.get("games") or []:
            a = (((g.get("teams") or {}).get("away") or {}).get("team") or {}).get("name", "")
            h = (((g.get("teams") or {}).get("home") or {}).get("team") or {}).get("name", "")
            if a and h and a.lower() == away_name.lower() and h.lower() == home_name.lower():
                return g.get("gamePk")
    return None

def _boxscore_players(game_pk: int) -> List[Dict[str, Any]]:
    j = _mlb_get(f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore")
    out: List[Dict[str, Any]] = []
    for side in ("home", "away"):
        team = (j.get(side) or {}).get("team") or {}
        team_name = team.get("name") or side
        players = (j.get(side) or {}).get("players") or {}
        for pkey, pdata in players.items():
            person = pdata.get("person") or {}
            pos = (pdata.get("position") or {}).get("abbreviation") or ""
            pid = person.get("id")
            name = person.get("fullName") or person.get("lastFirstName") or str(pid)
            # treat non-pitchers as hitters (keeps 2-way guys if they bat)
            if pid and pos.upper() not in ("P", "SP", "RP"):
                out.append({"person_id": pid, "player_name": name, "team_name": team_name})
    return out

def _season_avg(person_id: int, season: int) -> float:
    j = _mlb_get(
        f"https://statsapi.mlb.com/api/v1/people/{person_id}/stats"
        f"?stats=season&group=hitting&season={season}"
    )
    try:
        splits = (((j.get("stats") or [])[0]).get("splits") or [])
        if not splits:
            return 0.0
        avg_str = (splits[0].get("stat") or {}).get("avg") or "0"
        return float(avg_str)
    except Exception:
        return 0.0

def _gamelog(person_id: int, season: int, take_games: int = 20) -> List[Dict[str, Any]]:
    j = _mlb_get(
        f"https://statsapi.mlb.com/api/v1/people/{person_id}/stats"
        f"?stats=gameLog&group=hitting&season={season}"
    )
    try:
        splits = (((j.get("stats") or [])[0]).get("splits") or [])
        return splits[:max(1, take_games)]
    except Exception:
        return []

def _recent_avg_from_log(splits: List[Dict[str, Any]], last_n: int) -> Tuple[float, int, int]:
    hits = ab = taken = 0
    for sp in splits:
        stat = sp.get("stat") or {}
        ab_g = int(stat.get("atBats") or 0)
        h_g = int(stat.get("hits") or 0)
        if ab_g <= 0:
            continue  # skip DNP/PH w/0 AB
        hits += h_g
        ab += ab_g
        taken += 1
        if taken >= last_n:
            break
    return ((hits / ab) if ab > 0 else 0.0, hits, ab)

def _hitless_streak_from_log(splits: List[Dict[str, Any]]) -> int:
    streak = 0
    for sp in splits:
        stat = sp.get("stat") or {}
        ab_g = int(stat.get("atBats") or 0)
        h_g = int(stat.get("hits") or 0)
        if ab_g <= 0:
            continue  # neutral day
        if h_g == 0:
            streak += 1
        else:
            break
    return streak

@app.get("/cold_hitters_game", operation_id="cold_hitters_game")
def cold_hitters_game(
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    away: str = Query(..., description="Away team name exactly as in MLB schedule, e.g. 'Arizona Diamondbacks'"),
    home: str = Query(..., description="Home team name exactly as in MLB schedule, e.g. 'Colorado Rockies'"),
    min_season_avg: float = Query(0.250, ge=0.0, le=1.0, description="Only keep players with season AVG >= this"),
    last_n: int = Query(5, ge=2, le=15, description="Window for recent AVG"),
    max_recent_avg: float = Query(0.200, ge=0.0, le=1.0, description="Recent AVG must be <= this OR hitless-streak rule"),
    min_hitless_games: int = Query(1, ge=1, le=20, description="Consecutive 0-hit games required (OR condition)"),
    limit: int = Query(12, ge=1, le=50),
    debug: int = Query(0, ge=0, le=1),
):
    the_date = parse_date(date)
    season = the_date.year

    # find game
    sched = _mlb_schedule(the_date)
    game_pk = _find_game_pk(sched, away, home)
    if not game_pk:
        raise HTTPException(status_code=404, detail="Game not found for away/home/date. Use exact full team names.")

    # candidate hitters in that game
    candidates = _boxscore_players(game_pk)
    results: List[Dict[str, Any]] = []

    for c in candidates:
        pid = c["person_id"]
        season_avg = _season_avg(pid, season)

        # must be a "good hitter" first
        if season_avg < min_season_avg:
            continue

        log = _gamelog(pid, season, take_games=max(20, last_n * 2))
        recent_avg, r_hits, r_ab = _recent_avg_from_log(log, last_n)
        hitless = _hitless_streak_from_log(log)

        qualifies = (hitless >= min_hitless_games) or (r_ab > 0 and recent_avg <= max_recent_avg)
        if not qualifies:
            continue

        results.append({
            "player_name": c["player_name"],
            "team_name": c["team_name"],
            "season_avg": round(season_avg, 3),
            f"recent_avg_{last_n}": round(recent_avg, 3),
            "recent_sample": {"hits": r_hits, "at_bats": r_ab},
            "current_hitless_streak": hitless,
        })

    # sort: longer hitless streak first, then lower recent avg, then higher season avg
    results.sort(key=lambda x: (-x["current_hitless_streak"], x[f"recent_avg_{last_n}"], -x["season_avg"]))
    out = results[:limit]

    if debug == 1:
        return {
            "date": the_date.isoformat(),
            "game": {"away": away, "home": home, "game_pk": game_pk},
            "filters": {
                "min_season_avg": min_season_avg,
                "last_n": last_n,
                "max_recent_avg": max_recent_avg,
                "min_hitless_games": min_hitless_games,
                "limit": limit,
            },
            "counts": {"candidates": len(candidates), "qualified": len(results), "returned": len(out)},
            "results": out,
        }
    return out

# ------------------
# Include routers from routes/
# ------------------
# Optional routers; include if present without breaking deploys
try:
    from routes.league_scan import router as league_scan_router
    app.include_router(league_scan_router)
except Exception:
    pass

try:
    from routes.self_test import router as self_test_router
    app.include_router(self_test_router)
except Exception:
    pass

# ------------------
# Run local
# ------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
