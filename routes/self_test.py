# routes/self_test.py
import inspect
from datetime import datetime, timedelta, date as date_cls
from typing import Any, Dict, Optional

import pytz
from fastapi import APIRouter, HTTPException, Query, Request

router = APIRouter(tags=["diagnostics"])

# Local date parser (mirrors main.parse_date)
def _parse_date(d: Optional[str]) -> date_cls:
    tz = pytz.timezone("America/New_York")
    today = datetime.now(tz).date()
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

def _provider_call(request: Request, method: str, **kwargs):
    prov = getattr(request.app.state, "provider", None)
    last_err = getattr(request.app.state, "last_provider_error", None)
    if prov is None:
        raise HTTPException(status_code=503, detail=f"Provider not loaded: {last_err or 'unknown error'}")
    fn = getattr(prov, method, None)
    if not callable(fn):
        raise HTTPException(status_code=501, detail=f"Provider does not implement {method}()")
    return fn(**kwargs)

@router.get("/self_test", summary="Run a single-shot diagnostic of all key endpoints")
def self_test(
    request: Request,
    date: Optional[str] = Query("today", description="today|yesterday|tomorrow|YYYY-MM-DD"),
    limit: int = Query(10, ge=1, le=50, description="Top N for league_* lists"),
):
    the_date = _parse_date(date)
    tz = pytz.timezone("America/New_York")

    out: Dict[str, Any] = {
        "app": getattr(request.app, "title", "App"),
        "version": getattr(request.app, "version", None),
        "date": the_date.isoformat(),
        "now_local": datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S %Z"),
        "provider": {
            "loaded": getattr(request.app.state, "provider", None) is not None,
            "module": getattr(request.app.state, "provider_module", None),
            "class": getattr(request.app.state, "provider_class", None),
            "last_error": getattr(request.app.state, "last_provider_error", None),
        },
        "checks": {},
    }

    checks = out["checks"]

    def _check(name: str, fn):
        try:
            result = fn()
            checks[name] = {"ok": True, "result": result}
        except Exception as e:
            checks[name] = {"ok": False, "error": f"{type(e).__name__}: {e}"}

    # League-wide lists + schedule (wrapper adapts (date,limit) -> underlying signatures)
    _check("league_hot_hitters", lambda: _provider_call(request, "league_hot_hitters", date=the_date, limit=limit))
    _check("league_cold_hitters", lambda: _provider_call(request, "league_cold_hitters", date=the_date, limit=limit))
    _check("schedule_for_date",  lambda: _provider_call(request, "schedule_for_date",  date=the_date))

    # Streak endpoints
    _check("hot_streak_hitters",  lambda: _provider_call(request, "hot_streak_hitters",
                                                         date=the_date, min_avg=0.280, games=3, require_hit_each=True))
    _check("cold_streak_hitters", lambda: _provider_call(request, "cold_streak_hitters",
                                                         date=the_date, min_avg=0.275, games=2, require_zero_hit_each=True))
    _check("pitcher_streaks",     lambda: _provider_call(request, "pitcher_streaks",
                                                         date=the_date,
                                                         hot_max_era=4.0, hot_min_ks_each=6, hot_last_starts=3,
                                                         cold_min_era=4.6, cold_min_runs_each=3, cold_last_starts=2))

    # Raw provider probes (optional/private methods)
    def _raw_probe():
        prov = request.app.state.provider
        outp: Dict[str, Any] = {}
        for meth in ("_fetch_hitter_rows", "_fetch_pitcher_rows"):
            fn = getattr(prov, meth, None)
            if not callable(fn):
                outp[meth] = "not_implemented"
                continue
            try:
                sig = inspect.signature(fn)
                kwargs = {}
                if "date" in sig.parameters:
                    kwargs["date"] = the_date
                if "limit" in sig.parameters:
                    kwargs["limit"] = min(limit, 25)
                res = fn(**kwargs)
                outp[meth] = {"ok": True, "count": len(res)}
            except Exception as e:
                outp[meth] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        return outp

    _check("provider_raw_probe", _raw_probe)

    # Small UTF-8 name sample (helps eyeball accents)
    samples = []
    if checks.get("league_hot_hitters", {}).get("ok"):
        for row in checks["league_hot_hitters"]["result"][:5]:
            samples.append(str(row.get("player_name", "")))
    out["utf8_samples"] = samples

    return out
