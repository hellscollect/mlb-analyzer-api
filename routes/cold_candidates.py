# routes/cold_candidates.py

from __future__ import annotations

import math
import requests
from datetime import datetime, date as date_cls
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query, Request

from main import parse_date  # reuse your existing helper
from services.verify_helpers import verify_and_filter_names_soft

router = APIRouter()

# ---------- StatsAPI helpers (defensive; used only if provider doesn’t expose player-level helpers) ----------

_STATSAPI_BASE = "https://statsapi.mlb.com"

def _http_get_json(url: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    headers = {
        "User-Agent": "mlb-analyzer-api/1.0 (cold_candidates)",
        "Accept": "application/json",
    }
    r = requests.get(url, params=params or {}, headers=headers, timeout=12)
    r.raise_for_status()
    return r.json()

def _people_search(full_name: str) -> Optional[int]:
    """
    Best-effort lookup of a player id by name. Returns the first match ID or None.
    """
    try:
        data = _http_get_json(
            f"{_STATsAPI_BASE}/api/v1/people",
            {"search": full_name, "sportId": 1},
        )
    except Exception:
        # Fallback: try the alternate search endpoint (some deployments prefer people/search)
        try:
            data = _http_get_json(
                f"{_STATsAPI_BASE}/api/v1/people/search",
                {"query": full_name, "sportId": 1},
            )
        except Exception:
            return None

    people = data.get("people") or data.get("results") or []
    if not isinstance(people, list) or not people:
        return None
    p = people[0]
    pid = p.get("id") or p.get("person", {}).get("id")
    try:
        return int(pid) if pid is not None else None
    except Exception:
        return None

def _person_team(pid: int) -> str:
    """
    Try to fetch the player's current team name. Returns "" on any failure.
    """
    try:
        data = _http_get_json(f"{_STATsAPI_BASE}/api/v1/people/{pid}", {"hydrate": "team,CurrentTeam"})
        # Try several shapes
        team = (
            (data.get("people") or [{}])[0]
            .get("currentTeam", {})
            .get("name")
        ) or (
            (data.get("people") or [{}])[0]
            .get("team", {})
            .get("name")
        )
        if isinstance(team, str):
            return team
    except Exception:
        pass
    return ""

def _season_avg(pid: int, season: int) -> Optional[float]:
    """
    Return season AVG as float or None.
    """
    try:
        data = _http_get_json(
            f"{_STATsAPI_BASE}/api/v1/people/{pid}/stats",
            {"stats": "season", "group": "hitting", "season": season},
        )
        splits = ((data.get("stats") or [{}])[0].get("splits")) or []
        if not splits:
            return None
        avg_str = (splits[0].get("stat") or {}).get("avg")
        if not isinstance(avg_str, str):
            return None
        # Some APIs return ".273" or "0.273"; both are fine.
        try:
            val = float(avg_str)
        except Exception:
            # Strip leading dot if present
            avg_str2 = avg_str.strip()
            if avg_str2.startswith("."):
                avg_str2 = "0" + avg_str2
            val = float(avg_str2)
        return round(val, 3)
    except Exception:
        return None

def _parse_iso(d: str) -> Optional[date_cls]:
    try:
        return datetime.fromisoformat(d.replace("Z", "+00:00")).date()
    except Exception:
        try:
            return datetime.strptime(d, "%Y-%m-%d").date()
        except Exception:
            return None

def _game_log_splits(pid: int, season: int) -> List[Dict[str, Any]]:
    """
    Fetch hitting game logs for the given season. Returns a list (may be empty).
    """
    try:
        data = _http_get_json(
            f"{_STATsAPI_BASE}/api/v1/people/{pid}/stats",
            {"stats": "gameLog", "group": "hitting", "season": season},
        )
        splits = ((data.get("stats") or [{}])[0].get("splits")) or []
        # Normalize to newest-first by date
        def _key(s: Dict[str, Any]) -> tuple:
            d = s.get("date") or s.get("gameDate") or ""
            dt = _parse_iso(d) or datetime.min.date()
            return (dt.toordinal(),)
        splits_sorted = sorted(splits, key=_key, reverse=True)
        return splits_sorted
    except Exception:
        return []

def _compute_hitless_streak(pid: int, up_to_date: date_cls, last_n: int) -> int:
    """
    Count consecutive games (up to last_n) on or before up_to_date with AB>0 and H==0.
    Ignore games with AB==0. Stop at first game with a hit.
    """
    season = up_to_date.year
    splits = _game_log_splits(pid, season)
    streak = 0
    for s in splits:
        d = s.get("date") or s.get("gameDate")
        dt = _parse_iso(d)
        if not dt or dt > up_to_date:
            continue  # ignore future/today-not-final
        stat = s.get("stat") or {}
        ab = stat.get("atBats") or 0
        h = stat.get("hits") or 0
        # Some payloads store numbers as strings; coerce.
        try:
            ab = int(ab)
        except Exception:
            ab = 0
        try:
            h = int(h)
        except Exception:
            h = 0
        if ab <= 0:
            # no official ABs; ignore this game entirely for streak purposes
            continue
        if h == 0:
            streak += 1
            if streak >= last_n:
                return streak
        else:
            # streak broken
            break
    return streak

# ---------- Main route ----------

@router.get("/cold_candidates", tags=["hitters"])
def cold_candidates(
    request: Request,
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    names: Optional[str] = Query(None, description="Comma-separated list of player full names"),
    min_season_avg: float = Query(0.270, ge=0.0, le=1.0),
    min_hitless_games: int = Query(1, ge=1, le=30),
    last_n: int = Query(7, ge=1, le=30),
    limit: int = Query(50, ge=1, le=200),
    verify: int = Query(0, ge=0, le=1),
    debug: int = Query(0, ge=0, le=1),
):
    """
    For the supplied names, return those whose season AVG >= min_season_avg AND
    whose current hitless-streak (considering up to last_n completed games) >= min_hitless_games.

    - Soft verify mode: consults schedule to understand 'not-started' teams, but never drops names
      just for roster mismatches. It only returns a verify_context for transparency.
    """
    the_date: date_cls = parse_date(date)
    season = the_date.year

    # Parse names
    if not names:
        return {
            "date": the_date.isoformat(),
            "season": season,
            "items": [],
            "debug": [{"note": "no names provided"}],
        }
    raw_names = [n.strip() for n in names.split(",") if n.strip()]
    if not raw_names:
        return {
            "date": the_date.isoformat(),
            "season": season,
            "items": [],
            "debug": [{"note": "no names provided"}],
        }

    # Soft verify (never filters; just returns context)
    filtered_names = raw_names
    verify_ctx: Dict[str, Any] | None = None
    if verify == 1:
        try:
            filtered_names, verify_ctx = verify_and_filter_names_soft(
                the_date=the_date,
                provider=request.app.state.provider,
                input_names=raw_names,
                cutoffs={"min_season_avg": min_season_avg, "min_hitless_games": min_hitless_games, "last_n": last_n},
                debug_flag=bool(debug),
            )
        except Exception as e:
            # Fail open: keep names as-is, attach error in verify_context
            verify_ctx = {"error": f"{type(e).__name__}: {e}", "names_checked": len(raw_names)}

    items: List[Dict[str, Any]] = []
    debug_list: List[Dict[str, Any]] = []

    # Build candidates
    for nm in filtered_names:
        pid = _people_search(nm)
        if pid is None:
            debug_list.append({"name": nm, "error": "not found in people search"})
            continue

        avg = _season_avg(pid, season)
        if avg is None:
            debug_list.append({"name": nm, "error": "season average unavailable"})
            continue

        if avg < min_season_avg:
            debug_list.append({"name": nm, "team": "", "skip": f"season_avg {avg:.3f} < min {min_season_avg:.3f}"})
            continue

        streak = _compute_hitless_streak(pid, the_date, last_n)
        if streak < min_hitless_games:
            debug_list.append({"name": nm, "team": "", "skip": f"hitless_streak {streak} < min {min_hitless_games}"})
            continue

        team_name = _person_team(pid)
        items.append({
            "name": nm,
            "team": team_name or "",
            "season_avg": round(avg, 3),
            "hitless_streak": streak,
        })
        if len(items) >= limit:
            break

    out: Dict[str, Any] = {
        "date": the_date.isoformat(),
        "season": season,
        "items": items,
    }
    if debug_list or debug == 1:
        out["debug"] = debug_list
    if verify_ctx is not None:
        out["verify_context"] = verify_ctx

    return out
