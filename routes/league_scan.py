from fastapi import APIRouter, HTTPException, Request, Query
from pydantic import BaseModel
from datetime import datetime, timedelta, date as date_cls
from typing import Any, Dict, List, Optional, Tuple, Callable
import pytz
import requests
import time
import traceback
import os

router = APIRouter()
ET_TZ = pytz.timezone("America/New_York")

# ---------------- Models ----------------
class LeagueScanReq(BaseModel):
    date: Optional[str] = None  # "today" | "yesterday" | "tomorrow" | YYYY-MM-DD
    top_n: int = 15
    debug: int = 0

class LeagueScanResp(BaseModel):
    date: str
    counts: Dict[str, int]
    top: Dict[str, List[Dict[str, Any]]]
    matchups: List[Dict[str, Any]]
    debug: Optional[Dict[str, Any]] = None

# ---------------- Utils ----------------
def _now_et() -> datetime:
    return datetime.now(ET_TZ)

def _parse_date(d: Optional[str]) -> date_cls:
    today = _now_et().date()
    if not d or d.lower() == "today":
        return today
    s = d.lower()
    if s == "yesterday":
        return today - timedelta(days=1)
    if s == "tomorrow":
        return today + timedelta(days=1)
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date; use today|yesterday|tomorrow|YYYY-MM-DD")

def _fmt_et(dt: datetime) -> str:
    # Render ET time without leading zero hour; always include "ET"
    try:
        s = dt.strftime("%I:%M %p ET")
        return s.lstrip("0")
    except Exception:
        return dt.isoformat()

def _iso_to_et(iso_utc: str) -> Optional[datetime]:
    try:
        # schedule uses Z (UTC)
        if iso_utc.endswith("Z"):
            dt = datetime.fromisoformat(iso_utc.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(iso_utc)
        return dt.astimezone(ET_TZ)
    except Exception:
        return None

# ---------------- StatsAPI (fallback) ----------------
def _mlb_schedule(date_obj: date_cls, retries: int = 2, timeout_s: float = 8.0) -> List[Dict[str, Any]]:
    """Fetch schedule directly from StatsAPI, normalized to our shape."""
    url = "https://statsapi.mlb.com/api/v1/schedule"
    params = {"sportId": 1, "date": date_obj.isoformat(), "hydrate": "probablePitcher,venue"}
    js = {}
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout_s)
            if r.status_code >= 500:
                raise requests.HTTPError(f"upstream {r.status_code}")
            if r.status_code == 404:
                return []
            r.raise_for_status()
            js = r.json()
            break
        except Exception:
            if attempt < retries:
                time.sleep(0.5)
                continue
            return []

    games_out: List[Dict[str, Any]] = []
    for day in js.get("dates", []):
        for g in day.get("games", []):
            teams = g.get("teams", {}) or {}
            away = teams.get("away", {}) or {}
            home = teams.get("home", {}) or {}
            away_team = (away.get("team") or {}).get("name") or "TBD"
            home_team = (home.get("team") or {}).get("name") or "TBD"

            # probable pitchers
            ap = (away.get("probablePitcher") or {}).get("fullName")
            hp = (home.get("probablePitcher") or {}).get("fullName")

            # time in ET
            dt_et = _iso_to_et(g.get("gameDate") or "")
            et_time = _fmt_et(dt_et) if dt_et else "TBD"

            # venue
            venue_name = (g.get("venue") or {}).get("name") or "TBD"

            games_out.append({
                "away": away_team,
                "home": home_team,
                "et_time": et_time,
                "venue": venue_name,
                "probables": {
                    "away_pitcher": ap or "TBD",
                    "home_pitcher": hp or "TBD",
                }
            })
    return games_out

# ---------------- Provider (if available) ----------------
def _load_stats_provider(debug_log: List[str]) -> Optional[Any]:
    try:
        from providers.statsapi_provider import StatsApiProvider  # type: ignore
        debug_log.append("Loaded providers.statsapi_provider.StatsApiProvider")
        return StatsApiProvider()
    except Exception as e:
        debug_log.append(f"StatsApiProvider not available: {e.__class__.__name__}: {e}")
        return None

def _try_call(obj: Any, name: str, *args, **kwargs) -> Tuple[bool, Any, str]:
    """Attempt a method call by name with flexible signatures."""
    if not hasattr(obj, name):
        return False, None, f"noattr:{name}"
    fn: Callable = getattr(obj, name)
    # Try a few signature patterns without crashing the app
    candidates = [
        (args, kwargs),
        (args + (kwargs.get("top_n"),), {k:v for k,v in kwargs.items() if k != "top_n"}) if "top_n" in kwargs else (args, kwargs),
    ]
    last_err = ""
    for a, kw in candidates:
        try:
            return True, fn(*a, **kw), f"ok:{name}"
        except Exception as e:
            last_err = f"{e.__class__.__name__}: {e}"
            continue
    return False, None, f"fail:{name}:{last_err}"

def _get_top_with_provider(provider: Any, kind: str, target_date: date_cls, top_n: int, debug_log: List[str]) -> List[Any]:
    """
    Try multiple method names to get hot/cold hitters from the provider.
    Returns a list (may contain dicts or strings) – we normalize later.
    """
    if not provider:
        return []

    # candidate method names the provider might expose
    method_names = [
        f"league_{kind}_hitters",
        f"get_{kind}_hitters",
        f"{kind}_hitters",
        f"compute_{kind}_hitters",
        f"scan_{kind}_hitters",
        f"top_{kind}_hitters",
        f"league_{kind}",  # very generic
    ]

    for m in method_names:
        ok, result, tag = _try_call(provider, m, target_date.isoformat(), top_n=top_n)
        debug_log.append(f"provider_call:{m}:{tag}")
        if ok and result is not None:
            return _unwrap_top_container(result)
    return []

def _unwrap_top_container(result: Any) -> List[Any]:
    """
    Accepts many possible shapes and returns a list for hot/cold arrays.
    - list -> pass through
    - dict -> try common keys ('players','items','data','hot_hitters','cold_hitters','top')
    - string -> wrap as single-item list
    """
    if isinstance(result, list):
        return result
    if isinstance(result, dict):
        for k in ("players", "items", "data", "hot_hitters", "cold_hitters", "top"):
            v = result.get(k)
            if isinstance(v, list):
                return v
        # fallback: values that are lists
        for v in result.values():
            if isinstance(v, list):
                return v
        # last resort: single object
        return [result]
    if isinstance(result, str):
        return [result]
    return []

def _normalize_player_obj(p: Any) -> Optional[Dict[str, Any]]:
    """
    Ensure each player row is a dict. If it's a string, map to {"player_name": p}.
    Ignore completely unrecognized types.
    """
    if isinstance(p, dict):
        return p
    if isinstance(p, str):
        return {"player_name": p}
    return None

def _filter_players_to_scope(players: Any, scope: Optional[List[str]]) -> List[Dict[str, Any]]:
    """
    Players can be:
      - list[dict] (ideal)
      - list[str]  (we'll wrap with {"player_name": name})
      - dict with nested list (players/items/data/hot_hitters/cold_hitters/top)
      - single dict or single str
    Returns list[dict], filtered by team scope if provided.
    """
    # Flatten to a list
    base_list = _unwrap_top_container(players)
    out: List[Dict[str, Any]] = []
    for raw in base_list:
        obj = _normalize_player_obj(raw)
        if not obj:
            continue
        # Team name may be under several keys
        tn = obj.get("team_name") or obj.get("team") or obj.get("teamAbbr") or obj.get("team_name_full")
        if scope:
            if tn and tn not in scope:
                continue
        out.append(obj)
    return out

def _compute_avg_uplift_row(row: Dict[str, Any]) -> Dict[str, Any]:
    if "avg_uplift" not in row:
        try:
            r5 = float(row.get("recent_avg_5", 0))
            sea = float(row.get("season_avg", 0))
            row["avg_uplift"] = round(r5 - sea, 3)
        except Exception:
            pass
    return row

# ---------------- Scan core ----------------
def _run_scan(request: Request, target_date: date_cls, top_n: int, debug_flag: int) -> Tuple[LeagueScanResp, Dict[str, Any]]:
    dbg: Dict[str, Any] = {"logs": []}
    try:
        provider = _load_stats_provider(dbg["logs"])
        # schedule via provider if it exists
        matchups: List[Dict[str, Any]] = []
        used_schedule_source = "statsapi_fallback"
        if provider:
            for name in ("get_schedule", "schedule_for_date", "fetch_schedule", "schedule"):
                ok, result, tag = _try_call(provider, name, target_date.isoformat())
                dbg["logs"].append(f"provider_call:{name}:{tag}")
                if ok and isinstance(result, list):
                    matchups = result
                    used_schedule_source = f"provider.{name}"
                    break
        if not matchups:
            matchups = _mlb_schedule(target_date)
            used_schedule_source = "statsapi_fallback"

        # hot / cold via provider if we can
        hot_raw = _get_top_with_provider(provider, "hot", target_date, top_n, dbg["logs"])
        cold_raw = _get_top_with_provider(provider, "cold", target_date, top_n, dbg["logs"])

        # Normalize + scope (optional: if you pass 'scope' in headers later)
        scope_hdr = request.headers.get("X-Team-Scope")
        scope = [s.strip() for s in scope_hdr.split(",")] if scope_hdr else None
        hot = _filter_players_to_scope(hot_raw, scope)
        cold = _filter_players_to_scope(cold_raw, scope)

        # Compute missing fields if possible
        hot = [_compute_avg_uplift_row(r) for r in hot]

        out = LeagueScanResp(
            date=target_date.isoformat(),
            counts={
                "matchups": len(matchups),
                "hot_hitters": len(hot),
                "cold_hitters": len(cold),
            },
            top={
                "hot_hitters": hot[:top_n],
                "cold_hitters": cold[:top_n],
            },
            matchups=matchups,
            debug=None
        )

        if debug_flag:
            out.debug = {
                "schedule_source": used_schedule_source,
                "scope": scope,
                "logs": dbg["logs"][-200:],  # cap
            }
        return out, dbg

    except Exception as e:
        # Never 500 – return a minimal payload with error info
        err = {
            "error": f"{e.__class__.__name__}",
            "message": str(e),
            "trace": traceback.format_exc().splitlines()[-6:],
        }
        out = LeagueScanResp(
            date=target_date.isoformat(),
            counts={"matchups": 0, "hot_hitters": 0, "cold_hitters": 0},
            top={"hot_hitters": [], "cold_hitters": []},
            matchups=[],
            debug={"error": err}
        )
        return out, {"error": err}

# ---------------- Routes ----------------
@router.get("/health")
def health():
    return {"ok": True, "service": "mlb-analyzer-api", "time_et": _now_et().isoformat()}

@router.get("/")
def root_index():
    return {
        "ok": False,
        "message": "Try /health, /league_scan_get, or POST /league_scan_post",
        "endpoints": {
            "GET /health": {},
            "GET /league_scan_get": {"query": {"date": "today", "top_n": 15, "debug": 1}},
            "POST /league_scan_post": {"body": {"date": "today", "top_n": 15, "debug": 1}},
        }
    }

@router.get("/league_scan_get")
def league_scan_get(
    request: Request,
    date: Optional[str] = Query(default="today"),
    top_n: int = Query(default=15, ge=1, le=100),
    debug: int = Query(default=0, ge=0, le=1),
):
    target_date = _parse_date(date)
    out, _ = _run_scan(request, target_date, top_n, debug)
    return out

@router.post("/league_scan_post")
def league_scan_post(body: LeagueScanReq, request: Request):
    target_date = _parse_date(body.date)
    top_n = max(1, min(100, body.top_n or 15))
    debug_flag = 1 if (body.debug or 0) else 0
    out, _ = _run_scan(request, target_date, top_n, debug_flag)
    return out
