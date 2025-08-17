from typing import Dict, Any, Set

_NOT_STARTED_DETAILED = {"Scheduled", "Pre-Game"}  # Warmup is treated as started

def collect_not_started_team_ids(mlb_schedule: Dict[str, Any]) -> Set[int]:
    team_ids: Set[int] = set()
    for d in mlb_schedule.get("dates", []):
        for g in d.get("games", []):
            detailed = (g.get("status", {}).get("detailedState") or "").strip()
            if detailed in _NOT_STARTED_DETAILED:
                team_ids.add(g["teams"]["home"]["team"]["id"])
                team_ids.add(g["teams"]["away"]["team"]["id"])
    return team_ids
