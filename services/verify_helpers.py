# services/verify_helpers.py

from __future__ import annotations
from typing import Any, Dict, Iterable, List, Set, Tuple

def _get(d: dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return cur if cur is not None else default

def _status_code(game: dict) -> str:
    st = game.get("status") or {}
    # MLB schedule commonly exposes one/both of these:
    return st.get("codedGameState") or st.get("statusCode") or st.get("abstractGameCode") or ""

def collect_not_started_team_names(schedule: Any) -> Set[str]:
    """
    Return set of team NAMES (e.g. 'Los Angeles Angels', 'Athletics') that have NOT started yet
    for the given schedule payload.
    """
    teams: Set[str] = set()
    try:
        dates = schedule.get("dates") if isinstance(schedule, dict) else None
        if not dates:
            return teams
        for d in dates:
            games = d.get("games") or []
            for g in games:
                code = _status_code(g)
                # Treat Scheduled/Preview/Warmup as not-started
                if code in ("S", "PW", "P"):
                    home = _get(g, "teams", "home", "team", "name")
                    away = _get(g, "teams", "away", "team", "name")
                    if isinstance(home, str):
                        teams.add(home)
                    if isinstance(away, str):
                        teams.add(away)
    except Exception:
        # Be defensive: if schedule is unexpected, just return empty set
        return teams
    return teams

def verify_and_filter_names_soft(
    *,
    the_date,
    provider,
    input_names: List[str],
    cutoffs: Dict[str, Any] | None,
    debug_flag: bool,
) -> Tuple[List[str], Dict[str, Any]]:
    """
    SOFT VERIFY:
      - Look at schedule_for_date(the_date) to learn which TEAMS haven't started yet.
      - DO NOT drop any names if we can't prove roster membership.
      - Return the names unchanged, plus a verify_context block for transparency.
    """
    schedule = None
    not_started_team_names: Set[str] = set()
    schedule_error: str | None = None

    # Try to get schedule; if provider doesn't implement it, that's fine.
    try:
        sched_fn = getattr(provider, "schedule_for_date", None)
        if callable(sched_fn):
            # Provider may accept `date` or `date_str`. We pass both safely.
            schedule = sched_fn(date=the_date, date_str=getattr(the_date, "isoformat", lambda: str(the_date))())
            if isinstance(schedule, list):
                # Some providers might return just a list; wrap in a dict-ish object if needed.
                schedule = {"dates": [{"games": schedule}]}
            not_started_team_names = collect_not_started_team_names(schedule or {})
    except Exception as e:
        schedule_error = f"{type(e).__name__}: {e}"

    verify_context = {
        "not_started_team_count": len(not_started_team_names),
        "names_checked": len(input_names),
        "cutoffs": cutoffs or {},
    }
    if debug_flag:
        verify_context["schedule_error"] = schedule_error
        verify_context["not_started_team_names_sample"] = sorted(list(not_started_team_names))[:12]

    # SOFT behavior: never filter out names at this stage.
    return input_names, verify_context
