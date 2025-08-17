# services/schedule_filters.py

from typing import Dict, Iterable, Set

# -------- Status code buckets ----------------------------------------------
# MLB StatsAPI "status.statusCode"
# S = Scheduled, P = Preview/Pre-Game
_NOT_STARTED_STATUSCODES = {"S", "P"}

# I = In Progress, PW = Warmup, PR = Pre-Resume, F = Final, O = Other finished
_STARTED_STATUSCODES = {"I", "PW", "PR", "F", "O"}


def _status_is_not_started(status: Dict) -> bool:
    """
    True if a game's status indicates the game has NOT started yet.
    Prefer 'statusCode'; fall back to textual fields.
    """
    code = (status or {}).get("statusCode")
    if code:
        if code in _NOT_STARTED_STATUSCODES:
            return True
        if code in _STARTED_STATUSCODES:
            return False
        # else unknown -> fall through

    detailed = ((status or {}).get("detailedState") or "").lower()
    abstract = ((status or {}).get("abstractGameState") or "").lower()

    # Clearly not-started
    for token in ("scheduled", "preview", "pre-game", "pregame", "pre game"):
        if token in detailed or token in abstract:
            return True

    # Clearly started / finished
    for token in ("warmup", "in progress", "final", "game over", "live"):
        if token in detailed or token in abstract:
            return False

    # Conservative default: treat as started unless we know it's not started
    return False


def _teams_from_game(game: Dict) -> Iterable[int]:
    """Yield home/away team IDs from a schedule 'game' object."""
    try:
        home_id = game["teams"]["home"]["team"]["id"]
        away_id = game["teams"]["away"]["team"]["id"]
        if isinstance(home_id, int):
            yield home_id
        if isinstance(away_id, int):
            yield away_id
    except Exception:
        return


# -------- Public API --------------------------------------------------------

def collect_not_started_team_ids_from_schedule(schedule_json: Dict) -> Set[int]:
    """
    Given a StatsAPI schedule JSON (as returned by provider.schedule_for_date),
    return the set of team IDs whose games have NOT started yet.
    """
    team_ids: Set[int] = set()
    try:
        for day in (schedule_json.get("dates") or []):
            for game in day.get("games", []):
                if _status_is_not_started(game.get("status") or {}):
                    for tid in _teams_from_game(game):
                        team_ids.add(tid)
    except Exception:
        pass
    return team_ids


def get_not_started_team_ids(provider, date_str: str) -> Set[int]:
    """
    Convenience wrapper that calls provider.schedule_for_date(date_str)
    and returns the not-started team IDs.
    """
    schedule_json = provider.schedule_for_date(date_str)
    return collect_not_started_team_ids_from_schedule(schedule_json)


# -------- Backward-compatible alias (matches your earlier import/usage) -----
def collect_not_started_team_ids(schedule_json: Dict) -> Set[int]:
    """
    Back-compat alias so existing code like:
        schedule = provider.schedule_for_date(date_str)
        not_started_team_ids = collect_not_started_team_ids(schedule)
    continues to work.
    """
    return collect_not_started_team_ids_from_schedule(schedule_json)


__all__ = [
    "collect_not_started_team_ids_from_schedule",
    "get_not_started_team_ids",
    "collect_not_started_team_ids",  # back-compat
]
