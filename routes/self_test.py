# routes/self_test.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import pytz
from fastapi import APIRouter, Query

# Import the running app and helper utilities from main.py
# (safe: FastAPI only instantiates one app; importing main here won't re-register routes)
import main

router = APIRouter()

# Small helpers reused below
def _now_local_str(tz_name: str = "America/New_York") -> str:
    try:
        zone = pytz.timezone(tz_name)
    except Exception:
        zone = pytz.timezone("America/New_York")
    return datetime.now(zone).strftime("%Y-%m-%d %H:%M:%S %Z")

def _ok(result: Any) -> Dict[str, Any]:
    return {"ok": True, "result": result}

def _err(e: Exception) -> Dict[str, Any]:
    return {"ok": False, "error": f"{type(e).__name__}: {e}"}

def _try_provider(method_name: str, **kwargs) -> Dict[str, Any]:
    """
    Call a provider method if present, accepting multiple kw name variants.
    Uses main._call_with_sig to only pass accepted params.
    """
    provider = main.app.state.provider
    fn = getattr(provider, method_name, None)
    if not callable(fn):
        return {"ok": False, "error": f"NotImplementedError: {method_name} not available in provider"}
    try:
        out = main._call_with_sig(fn, **kwargs)
        return _ok(out)
    except Exception as e:
        return _err(e)

def _as_list(obj: Any) -> List[Any]:
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        # try common shapes
        for k in ("hot_hitters", "cold_hitters", "matchups", "items"):
            v = obj.get(k)
            if isinstance(v, list):
                return v
    return [obj] if obj is not None else []

def _take(obj: Any, n: int) -> Any:
    if isinstance(obj, list):
        return obj[:n]
    if isinstance(obj, dict):
        d = dict(obj)
        for k in ("hot_hitters", "cold_hitters", "matchups"):
            if isinstance(d.get(k), list):
                d[k] = d[k][:n]
        return d
    return obj

@router.get("/self_test", summary="One-call smoke test + UTF-8 check", tags=["debug"])
def self_test(
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    limit: int = Query(10, ge=1, le=200),
    debug: int = Query(1, ge=0, le=1),
):
    """
    Single call that verifies:
      - Provider loaded and identity
      - league_hot_hitters / league_cold_hitters
      - schedule_for_date
      - Adapter-backed results used by /hot_streak_hitters and /cold_streak_hitters
      - Aggregated league-like summary
      - UTF-8 sample strings
    Returns one JSON payload so you don't need to hit multiple URLs.
    """
    the_date = main.parse_date(date)
    provider = main.app.state.provider

    # 1) Direct league_* + schedule (provider expects date_str / top_n)
    league_hot = _try_provider(
        "league_hot_hitters",
        date_str=the_date.isoformat(),
        date=the_date,
        top_n=limit,
        n=limit,
        limit=limit,
        debug=bool(debug),
    )
    league_cold = _try_provider(
        "league_cold_hitters",
        date_str=the_date.isoformat(),
        date=the_date,
        top_n=limit,
        n=limit,
        limit=limit,
        debug=bool(debug),
    )

    try:
        schedule = _ok(main._schedule_for_date(the_date, bool(debug)))
        schedule["result"] = _take(schedule["result"], limit)
    except Exception as e:
        schedule = _err(e)

    # 2) Adapter behavior (exactly what /hot_streak_hitters & /cold_streak_hitters endpoints use)
    try:
        hot_adapter = _ok(
            main._hot_hitters_fallback(
                the_date=the_date,
                min_avg=0.0,                 # relaxed to show breadth
                games=5,
                require_hit_each=False,
                debug=bool(debug),
                top_n=limit,
            )
        )
        hot_adapter["result"] = _take(hot_adapter["result"], limit)
    except Exception as e:
        hot_adapter = _err(e)

    try:
        cold_adapter = _ok(
            main._cold_hitters_fallback(
                the_date=the_date,
                min_avg=0.0,
                games=5,
                require_zero_hit_each=False,
                debug=bool(debug),
                top_n=limit,
            )
        )
        cold_adapter["result"] = _take(cold_adapter["result"], limit)
    except Exception as e:
        cold_adapter = _err(e)

    # 3) Build a league-like summary using the same adapter outputs + schedule
    hot_list = _as_list(hot_adapter["result"]) if hot_adapter.get("ok") else []
    cold_list = _as_list(cold_adapter["result"]) if cold_adapter.get("ok") else []
    matchups = _as_list(schedule["result"]) if schedule.get("ok") else []

    league_like_summary = {
        "date": the_date.isoformat(),
        "counts": {
            "matchups": len(matchups),
            "hot_hitters": len(hot_list),
            "cold_hitters": len(cold_list),
        },
        "top": {
            "hot_hitters": _take(hot_list, limit),
            "cold_hitters": _take(cold_list, limit),
        },
        "matchups": _take(matchups, limit),
    }

    # 4) UTF-8 samples (to confirm no mojibake)
    utf8_samples: List[str] = []
    for section in (hot_list, cold_list):
        for row in section:
            name = None
            if isinstance(row, dict):
                name = row.get("player_name") or row.get("name")
            if isinstance(name, str) and name not in utf8_samples:
                utf8_samples.append(name)
            if len(utf8_samples) >= 5:
                break
        if len(utf8_samples) >= 5:
            break

    # 5) Overall status
    hard_requirements_ok = (
        provider is not None
        and league_hot.get("ok", False)
        and league_cold.get("ok", False)
        and schedule.get("ok", False)
        and hot_adapter.get("ok", False)
        and cold_adapter.get("ok", False)
    )
    status = "OK" if hard_requirements_ok else "DEGRADED"

    # 6) Final payload
    payload = {
        "app": main.APP_NAME,
        "version": getattr(main, "APP_VERSION", "unknown"),
        "date": the_date.isoformat(),
        "now_local": _now_local_str(),
        "provider": {
            "loaded": provider is not None,
            "module": getattr(main, "provider_module", None),
            "class": getattr(main, "provider_class", None),
            "last_error": getattr(main, "_last_provider_error", None),
        },
        "status": status,
        "checks": {
            "league_hot_hitters": league_hot,
            "league_cold_hitters": league_cold,
            "schedule_for_date": schedule,
            "hot_endpoint_adapter": hot_adapter,
            "cold_endpoint_adapter": cold_adapter,
            "league_like_summary": {"ok": True, "result": league_like_summary},
        },
        "utf8_samples": utf8_samples,
    }

    if debug:
        payload["debug"] = {
            "notes": [
                "Adapter checks mirror the behavior of /hot_streak_hitters and /cold_streak_hitters endpoints.",
                "UTF-8 wrapper is active if provider module is providers.utf8_wrapper_provider.",
            ],
            "provider_module": getattr(main, "provider_module", None),
            "provider_class": getattr(main, "provider_class", None),
        }

    return payload
