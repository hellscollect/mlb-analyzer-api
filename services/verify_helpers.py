# services/verify_helpers.py

from __future__ import annotations
from typing import Any, Dict, List, Tuple, Set
from datetime import date as date_cls
import inspect

def _call_with_sig(fn, **kwargs):
    try:
        sig = inspect.signature(fn)
        filtered = {k: v for k, v in kwargs.items() if k in sig.parameters}
        return fn(**filtered)
    except TypeError:
        # Best-effort positional retry
        params = list(inspect.signature(fn).parameters.values())
        args = []
        for p in params:
            if p.name in kwargs:
                args.append(kwargs[p.name])
        return fn(*args)

def collect_not_started_team_ids(schedule_obj: Any) -> Set[int]:
    """
    Extract team IDs for games that have NOT started yet (Scheduled/Preview/Warmup).
    Works with the MLB schedule JSON (dates[].games[]).
    """
    ids: Set[int] = set()
    try:
        dates = (schedule_obj or {}).get("dates", [])
        for d in dates:
            for g in d.get("games", []):
                st = g.get("status", {}) or {}
                abstract = (st.get("abstractGameState") or "").strip()
                detailed = (st.get("detailedState") or "").strip()
                not_started = (
                    abstract in ("Preview", "Pre-Game") or
                    detailed in ("Scheduled", "Pre-Game", "Warmup")
                )
                if not_started:
                    for side in ("home", "away"):
                        tid = ((g.get("teams", {}) or {}).get(side, {}) or {}).get("team", {}).get("id")
                        if isinstance(tid, int):
                            ids.add(tid)
    except Exception:
        # Best-effort only
        pass
    return ids

def verify_and_filter_names_soft(
    the_date: date_cls,
    provider: Any,
    input_names: List[str],
    cutoffs: Dict[str, Any],
    debug_flag: bool,
):
    """
    Soft verify: do NOT filter names. Just inspect schedule (if available) and
    return a context payload that explains 'not-started' teams count.
    """
    not_started_ids: Set[int] = set()
    schedule_obj = None

    if provider is not None:
        sched_fn = getattr(provider, "schedule_for_date", None)
        if callable(sched_fn):
            try:
                schedule_obj = _call_with_sig(sched_fn, date=the_date, date_str=the_date.isoformat(), debug=debug_flag)
            except Exception:
                schedule_obj = None

    if isinstance(schedule_obj, dict):
        not_started_ids = collect_not_started_team_ids(schedule_obj)

    context = {
        "not_started_team_count": len(not_started_ids) if not_started_ids else 30,
        "names_checked": len(input_names),
        "cutoffs": cutoffs,
    }
    return input_names, context
