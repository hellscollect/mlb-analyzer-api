# routes/league_scan.py
from fastapi import APIRouter, Request, Body, Query
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, date
from providers.statsapi_provider import StatsApiProvider

router = APIRouter()


# ---------- helpers ----------

def _resolve_date(date_str: Optional[str]) -> str:
    """
    Accepts 'today' or YYYY-MM-DD. Returns YYYY-MM-DD (UTC calendar).
    """
    if not date_str or str(date_str).strip().lower() in {"today", "todays", "now"}:
        return date.today().strftime("%Y-%m-%d")
    # trust caller if it's already a date-like string
    return str(date_str).strip()


def _call_provider(p: Any, names: List[str], logs: List[str], *args, **kwargs) -> Tuple[Any, str]:
    """
    Try a list of method names on the provider. Return (result, method_used).
    Logs each attempt to debug.
    """
    for nm in names:
        if hasattr(p, nm):
            logs.append(f"provider_call:{nm}:found:{nm}")
            fn = getattr(p, nm)
            try:
                return fn(*args, **kwargs), nm
            except Exception as ex:
                logs.append(f"provider_call:{nm}:error:{type(ex).__name__}")
                # fall through to next name
        else:
            logs.append(f"provider_call:{nm}:noattr:{nm}")
    return None, ""


def _ensure_list_of_dicts(items: Any) -> List[Dict[str, Any]]:
    """
    Normalize weird provider outputs to a list[dict].
    Strings are coerced to {'player_name': <string>}; other non-dicts are skipped.
    """
    out: List[Dict[str, Any]] = []
    if not items:
        return out
    if isinstance(items, dict):
        return [items]
    if not isinstance(items, list):
        return out
    for it in items:
        if isinstance(it, dict):
            out.append(it)
        elif isinstance(it, str):
            out.append({"player_name": it})
        # silently skip anything else
    return out


def _filter_players_to_scope(players: Any, scope: Optional[str]) -> List[Dict[str, Any]]:
    """
    Defensively normalize player rows and (optionally) filter to a team scope.
    Prevents crashes like AttributeError: 'str' object has no attribute 'get'.
    """
    safe = _ensure_list_of_dicts(players)

    if not scope:
        return safe

    scope_norm = str(scope).strip().lower()
    out: List[Dict[str, Any]] = []
    for p in safe:
        tn = p.get("team_name") or p.get("team") or p.get("teamAbbr")
        if tn and str(tn).strip().lower() == scope_norm:
            out.append(p)
    return out


def _run_scan(request: Request, primary_date: str, top_n: int, debug_flag: int, scope: Optional[str]) -> Dict[str, Any]:
    logs: List[str] = []
    provider = StatsApiProvider()
    logs.append(f"Loaded {provider.__module__}.{provider.__class__.__name__}")

    # 1) Schedule (try a few method names for broad compatibility)
    schedule, schedule_method = _call_provider(
        provider,
        ["schedule_for_date", "get_schedule", "fetch_schedule", "schedule"],
        logs,
        primary_date
    )
    matchups = schedule or []
    matchups = matchups if isinstance(matchups, list) else _ensure_list_of_dicts(matchups)

    # 2) Hot hitters
    hot, hot_method = _call_provider(
        provider,
        ["league_hot_hitters", "hot_hitters", "top_hot_hitters", "league_hot"],
        logs,
        primary_date,
        top_n
    )
    hot_f = _filter_players_to_scope(hot, scope)

    # 3) Cold hitters
    cold, cold_method = _call_provider(
        provider,
        ["league_cold_hitters", "cold_hitters", "top_cold_hitters", "league_cold"],
        logs,
        primary_date,
        top_n
    )
    cold_f = _filter_players_to_scope(cold, scope)

    out: Dict[str, Any] = {
        "date": primary_date,
        "counts": {
            "matchups": len(matchups),
            "hot_hitters": len(hot_f),
            "cold_hitters": len(cold_f),
        },
        "top": {
            "hot_hitters": hot_f[:top_n] if hot_f else [],
            "cold_hitters": cold_f[:top_n] if cold_f else [],
        },
        "matchups": matchups,
    }

    if debug_flag:
        out["debug"] = {
            "schedule_source": schedule_method or "unknown",
            "scope": scope,
            "logs": logs,
        }

    return out


# ---------- routes ----------

@router.get("/league_scan_get")
def league_scan_get(
    request: Request,
    date: str = Query("today"),
    top_n: int = Query(15, ge=1, le=100),
    debug: int = Query(1, ge=0, le=1),
    scope: Optional[str] = Query(None, description="Optional team filter, e.g., 'Atlanta Braves' or 'ATL'")
):
    """
    GET variant that delegates to the POST logic for a single code path.
    """
    primary_date = _resolve_date(date)
    return _run_scan(request, primary_date, top_n, debug, scope)


@router.post("/league_scan_post")
def league_scan_post(
    payload: Dict[str, Any] = Body(...),
    request: Request = None
):
    """
    POST body: {"date":"today","top_n":15,"debug":1, "scope":"optional team name/abbr"}
    """
    date_raw = payload.get("date", "today")
    top_n = int(payload.get("top_n", 15) or 15)
    debug_flag = int(payload.get("debug", 1) or 1)
    scope = payload.get("scope")
    primary_date = _resolve_date(date_raw)
    return _run_scan(request, primary_date, top_n, debug_flag, scope)
