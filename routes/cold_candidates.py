# routes/cold_candidates.py

from __future__ import annotations

import requests
from datetime import datetime, date as date_cls
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request

from main import parse_date  # reuse your helper

# Try to import soft-verify; if unavailable, provide a safe fallback so the router still loads.
try:
    from services.verify_helpers import verify_and_filter_names_soft
except Exception:
    def verify_and_filter_names_soft(the_date, provider, input_names, cutoffs, debug_flag):
        return input_names, {
            "error": "verify_helpers missing; soft-verify skipped",
            "names_checked": len(input_names),
            "not_started_team_count": 30,
            "cutoffs": cutoffs,
        }

router = APIRouter()

# ---------- StatsAPI helpers ----------

_STAS_API_TIMEOUT = 12
_STATSAPI_BASE = "https://statsapi.mlb.com"

def _http_get_json(url: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    r = requests.get(
        url,
        params=params or {},
        headers={
            "User-Agent": "mlb-analyzer-api/1.0 (cold_candidates)",
            "Accept": "application/json",
        },
        timeout=_STAS_API_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()

def _people_search(full_name: str) -> Optional[int]:
    """
    Best-effort lookup of a player id by name. Returns first match ID or None.
    """
    # Primary
    try:
        data = _http_get_json(
            f"{_STATSAPI_BASE}/api/v1/people",
            {"search": full_name, "sportId": 1},
        )
        people = data.get("people") or []
        if isinstance(people, list) and people:
            pid = people[0].get("id")
            return int(pid) if pid is not None else None
    except Exception:
        pass
    # Fallback endpoint
    try:
        data = _http_get_json(
            f"{_STATSAPI_BASE}/api/v1/people/search",
            {"query": full_name, "sportId": 1},
        )
        people = data.get("results") or data.get("people") or []
        if isinstance(people, list) and people:
            pid = (people[0].get("id")
                   or people[0].get("person", {}).get("id"))
            return int(pid) if pid is not None else None
    except Exception:
        pass
    return None

def _person_team(pid: int) -> str:
    """
    Try to fetch the player's current team name. Returns "" on failure.
    """
    try:
        data = _http_get_json(
            f"{_STATSAPI_BASE}/api/v1/people/{pid}",
            {"hydrate": "team,CurrentTeam"},
        )
        ppl = data.get("people") or []
        if not ppl:
            return ""
        p0 = ppl[0] or {}
        team = (p0.get("currentTeam", {}) or {}).get("name") \
            or (p0.get("team", {}) or {}).get("name") \
            or ""
        return team if isinstance(team, str) else ""
    except Exception:
        return ""

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
    Fetch hitting game logs for the season. Returns newest-first list.
    """
    try:
        data = _http_get_json(
            f"{_STATSAPI_BASE}/api/v1/people/{pid}/stats",
            {"stats": "gameLog", "group": "hitting", "season": season},
        )
        splits = ((data.get("stats") or [{}])[0].get("splits")) or []
        def _k(s: Dict[str, Any]):
            d = s.get("date") or s.get("gameDate") or ""
            dt = _parse_iso(d) or datetime.min.date()
            return (dt.toordinal(),)
        return sorted(splits, key=_k, reverse=True)
    except Exception:
        return []

def _season_avg(pid: int, season: int) -> Optional[float]:
    """
    Return season AVG as float or None.
    """
    try:
        data = _http_get_json(
            f"{_STATSAPI_BASE}/api/v1/people/{pid}/stats",
            {"stats": "season", "group": "hitting", "season": season},
        )
        splits = ((data.get("stats") or [{}])[0].get("splits")) or []
        if not splits:
            return None
        avg_str = (splits[0].get("stat") or {}).get("avg")
        if not isinstance(avg_str, str):
            return None
        s = avg_str.strip()
        if s.startswith("."):
            s = "0" + s
        return round(float(s), 3)
    except Exception:
        return None

def _compute_hitless_streak(pid: int, up_to_date: date_cls, last_n: int) -> int:
    """
    Count consecutive games (up to last_n) on/before up_to_date with AB>0 and H==0.
    Ignore games with AB==0. Stop at first game with a hit.
    """
    season = up_to_date.year
    splits = _game_log_splits(pid, season)
    streak = 0
    for s in splits:
        d = s.get("date") or s.get("gameDate")
        dt = _parse_iso(d)
        if not dt or dt > up_to_date:
            continue  # ignore future or same-day in-progress
        stat = s.get("stat") or {}
        try:
            ab = int(stat.get("atBats") or 0)
            h = int(stat.get("hits") or 0)
        except Exception:
            ab, h = 0, 0
        if ab <= 0:
            continue
        if h == 0:
            streak += 1
            if streak >= last_n:
                return streak
        else:
            break
    return streak

# ---------- Route ----------

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
    For the supplied names, return those whose season AVG >= min_season_avg AND whose
    current hitless-streak (considering only completed games up to 'date') >= min_hitless_games.
    """
    the_date: date_cls = parse_date(date)
    season = the_date.year

    # Parse names list
    if not names:
        return {"date": the_date.isoformat(), "season": season, "items": [], "debug": [{"note": "no names provided"}]}
    raw_names = [n.strip() for n in names.split(",") if n.strip()]
    if not raw_names:
        return {"date": the_date.isoformat(), "season": season, "items": [], "debug": [{"note": "no names provided"}]}

    # Soft verify (does not filter names; returns context only)
    filtered_names = raw_names
    verify_ctx: Dict[str, Any] | None = None
    if verify == 1:
        try:
            filtered_names, verify_ctx = verify_and_filter_names_soft(
                the_date=the_date,
                provider=request.app.state.provider,
                input_names=raw_names,
                cutoffs={
                    "min_season_avg": min_season_avg,
                    "min_hitless_games": min_hitless_games,
                    "last_n": last_n,
                },
                debug_flag=bool(debug),
            )
        except Exception as e:
            verify_ctx = {"error": f"{type(e).__name__}: {e}", "names_checked": len(raw_names)}

    items: List[Dict[str, Any]] = []
    debugs: List[Dict[str, Any]] = []

    for nm in filtered_names:
        pid = _people_search(nm)
        if pid is None:
            debugs.append({"name": nm, "error": "not found in people search"})
            continue

        avg = _season_avg(pid, season)
        if avg is None:
            debugs.append({"name": nm, "error": "season average unavailable"})
            continue
        if avg < min_season_avg:
            debugs.append({"name": nm, "team": "", "skip": f"season_avg {avg:.3f} < min {min_season_avg:.3f}"})
            continue

        streak = _compute_hitless_streak(pid, the_date, last_n)
        if streak < min_hitless_games:
            debugs.append({"name": nm, "team": "", "skip": f"hitless_streak {streak} < min {min_hitless_games}"})
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

    out: Dict[str, Any] = {"date": the_date.isoformat(), "season": season, "items": items}
    if debugs or debug == 1:
        out["debug"] = debugs
    if verify_ctx is not None:
        out["verify_context"] = verify_ctx
    return out
