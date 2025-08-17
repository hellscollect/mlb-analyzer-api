from typing import Any, Dict, Iterable, List, Set, Tuple

# ---- helpers ---------------------------------------------------------------

_NOT_STARTED_ABSTRACT_STATES = {"Preview", "Pre-Game", "Warmup", "Scheduled"}
_NOT_STARTED_STATUS_CODES = {"S", "PW", "P"}  # Scheduled, Warmup, Pre-game

def _norm(s: str) -> str:
    return "".join(ch for ch in s.strip().upper() if ch.isalnum() or ch.isspace())

def _candidate_team_keys(team: str) -> Set[str]:
    """
    Build a few normalized keys to improve matching between provider team labels
    and schedule team names (e.g., 'Athletics' vs 'Oakland Athletics').
    """
    t = _norm(team)
    if not t:
        return set()
    parts = [p for p in t.split() if p]
    keys = {t}
    if len(parts) >= 2:
        # Keep full, last word, and without city
        keys.add(parts[-1])
        keys.add(" ".join(parts[1:]))
    return {k for k in keys if k}

# ---- schedule extraction ---------------------------------------------------

def _iter_games(schedule: Any) -> Iterable[Dict[str, Any]]:
    """
    Accepts either a plain list of games or the MLB Stats API schedule dict.
    Yields game dicts with 'status' and 'teams' keys.
    """
    if isinstance(schedule, list):
        for g in schedule:
            if isinstance(g, dict):
                yield g
        return

    if isinstance(schedule, dict):
        dates = schedule.get("dates") or []
        for d in dates:
            for g in d.get("games", []):
                yield g

def _is_not_started(game: Dict[str, Any]) -> bool:
    status = (game.get("status") or {})
    abstract = status.get("abstractGameState") or ""
    code = status.get("statusCode") or ""
    return (abstract in _NOT_STARTED_ABSTRACT_STATES) or (code in _NOT_STARTED_STATUS_CODES)

def _team_name_from_side(side: Dict[str, Any]) -> str:
    team = (side or {}).get("team") or {}
    return str(team.get("name") or "").strip()

def collect_not_started_team_ids(schedule: Any) -> Set[int]:
    """
    Kept for backward-compat with existing imports.
    """
    out: Set[int] = set()
    for g in _iter_games(schedule):
        if not _is_not_started(g):
            continue
        teams = g.get("teams") or {}
        for side in ("home", "away"):
            team = (teams.get(side) or {}).get("team") or {}
            tid = team.get("id")
            if isinstance(tid, int):
                out.add(tid)
    return out

def collect_not_started_team_names(schedule: Any) -> Set[str]:
    names: Set[str] = set()
    for g in _iter_games(schedule):
        if not _is_not_started(g):
            continue
        teams = g.get("teams") or {}
        for side in ("home", "away"):
            nm = _team_name_from_side(teams.get(side) or {})
            if nm:
                names.add(nm.strip())
    return names

# ---- main verifier ---------------------------------------------------------

def verify_candidates(
    candidates: List[Dict[str, Any]],
    schedule: Any,
    debug: bool = False,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    New behavior:
      - Build the set of NOT-STARTED team NAMES from today's schedule.
      - If a candidate has a team and it matches a NOT-STARTED team -> keep.
      - If a candidate has NO team -> keep (with a debug note).
      - Only skip when a candidate's team is present AND does NOT match any
        not-started team.
    """
    not_started_names = collect_not_started_team_names(schedule)
    not_started_keys = {_norm(nm) for nm in not_started_names}
    kept: List[Dict[str, Any]] = []
    logs: List[Dict[str, Any]] = []

    for c in candidates:
        name = str(c.get("name") or "").strip()
        team = str(
            c.get("team")
            or c.get("team_name")
            or c.get("teamLabel")
            or ""
        ).strip()

        if not team:
            kept.append(c)
            if debug:
                logs.append({
                    "name": name or "(unknown)",
                    "note": "kept (no team listed; verify could not match by team name)",
                })
            continue

        cand_keys = _candidate_team_keys(team)
        matched = any((k in not_started_keys) for k in cand_keys)

        if matched:
            kept.append(c)
            if debug:
                logs.append({
                    "name": name or "(unknown)",
                    "team": team,
                    "note": "kept (team has a not-started game today)",
                })
        else:
            if debug:
                logs.append({
                    "name": name or "(unknown)",
                    "team": team,
                    "skip": "no not-started game today for candidate's team (team name check)",
                })

    if debug:
        logs.append({
            "verify_summary": {
                "not_started_team_count": len(not_started_names),
                "not_started_teams": sorted(not_started_names),
                "names_checked": len(candidates),
            }
        })

    return kept, logs
