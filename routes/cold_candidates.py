# routes/cold_candidates.py

from __future__ import annotations

import re
import requests
from datetime import datetime, date as date_cls
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query, Request

# avoid circular import with main.py
from services.dates import parse_date

# Try optional soft-verify helper; fall back safely if missing
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

_STATSAPI_BASE = "https://statsapi.mlb.com"
_TIMEOUT = 12

def _http_get_json(url: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    r = requests.get(
        url,
        params=params or {},
        headers={
            "User-Agent": "mlb-analyzer-api/1.0 (cold_candidates)",
            "Accept": "application/json",
        },
        timeout=_TIMEOUT,
    )
    r.raise_for_status()
    return r.json()

# --- name normalization & matching -------------------------------------------------

_name_cleaner = re.compile(r"[^a-z]+")

def _norm_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = _name_cleaner.sub(" ", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def _split_first_last(q_norm: str) -> Tuple[str, str]:
    parts = q_norm.split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[-1]

def _candidate_norm_names(c: Dict[str, Any]) -> Dict[str, str]:
    # normalize several candidate name fields
    return {
        "full": _norm_name(c.get("fullName", "")),
        "use": _norm_name(c.get("useName", "")),
        "last_first": _norm_name((c.get("lastFirstName") or "").replace(",", " ")),
        "first": _norm_name(c.get("firstName", "")),
        "last": _norm_name(c.get("lastName", "")),
    }

def _is_exact_match(q_norm: str, cand_norm: Dict[str, str]) -> bool:
    if q_norm in (cand_norm["full"], cand_norm["use"], cand_norm["last_first"]):
        return True
    qf, ql = _split_first_last(q_norm)
    if qf and ql and cand_norm["first"] == qf and cand_norm["last"] == ql:
        return True
    return False

def _soft_match_score(q_norm: str, cand_norm: Dict[str, str]) -> int:
    # token overlap with fullName/useName; use as *tie-breaker only*
    q_tokens = set(q_norm.split())
    pool = set((cand_norm["full"] + " " + cand_norm["use"] + " " + cand_norm["last_first"]).split())
    return len(q_tokens & pool)

def _choose_person_id(q: str, people: List[Dict[str, Any]]) -> Optional[int]:
    """
    Choose a single correct person id from a list of candidates for query 'q'.
    Rules:
      - Accept ONLY exact matches (normalized) on full/use/lastFirst or exact first+last.
      - If multiple exact matches, prefer one with currentTeam present, then active=True.
      - If there are NO exact matches, return None (do NOT guess on token overlap).
    """
    qn = _norm_name(q)
    exacts: List[Tuple[int, bool, bool]] = []  # (pid, has_team, is_active)
    for c in people:
        pid = c.get("id") or (c.get("person") or {}).get("id")
        if not isinstance(pid, int):
            continue
        cn = _candidate_norm_names(c)
        if _is_exact_match(qn, cn):
            has_team = bool((c.get("currentTeam") or c.get("team") or {}).get("id"))
            is_active = bool(c.get("active"))
            exacts.append((pid, has_team, is_active))

    if not exacts:
        return None

    # sort: has_team desc, is_active desc
    exacts.sort(key=lambda t: (t[1], t[2]), reverse=True)
    return exacts[0][0]

# --- people lookup ----------------------------------------------------------------

def _people_search(full_name: str) -> Optional[int]:
    """
    Robust player lookup via /people/search.
    Returns the chosen player's id or None.
    """
    q = full_name.strip()
    if not q:
        return None

    # Primary: /people/search
    try:
        data = _http_get_json(
            f"{_STATSAPI_BASE}/api/v1/people/search",
            {"query": q, "sportId": 1},
        )
        people = (data.get("people") or data.get("results") or []) if isinstance(data, dict) else []
        pid = _choose_person_id(q, people)
        if pid is not None:
            return pid
    except Exception:
        pass

    # Fallback: /people?search=...
    try:
        data = _http_get_json(
            f"{_STATSAPI_BASE}/api/v1/people",
            {"search": q, "sportId": 1},
        )
        people = data.get("people") or []
        pid = _choose_person_id(q, people)
        if pid is not None:
            return pid
    except Exception:
        pass

    return None

def _person_team(pid: int) -> str:
    try:
        data = _http_get_json(
            f"{_STATSAPI_BASE}/api/v1/people/{pid}",
            {"hydrate": "currentTeam"},
        )
        ppl = data.get("people") or []
        if not ppl:
            return ""
        p0 = ppl[0] or {}
        team = (p0.get("currentTeam") or {}).get("name") or ""
        return team if isinstance(team, str) else ""
    except Exception:
        return ""

# --- stats helpers ----------------------------------------------------------------

def _parse_iso(d: str) -> Optional[date_cls]:
    try:
        return datetime.fromisoformat(d.replace("Z", "+00:00")).date()
    except Exception:
        try:
            return datetime.strptime(d, "%Y-%m-%d").date()
        except Exception:
            return None

def _game_log_splits(pid: int, season: int) -> List[Dict[str, Any]]:
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

# --- route ------------------------------------------------------------------------

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
    Return players (from 'names') with season AVG >= min_season_avg AND a hitless streak
    (completed games only, up to 'date') >= min_hitless_games. Name lookup requires an
    exact match; if no exact match is found, that name is skipped (no guessing).
    """
    the_date: date_cls = parse_date(date)
    season = the_date.year

    if not names:
        return {"date": the_date.isoformat(), "season": season, "items": [], "debug": [{"note": "no names provided"}]}
    raw_names = [n.strip() for n in names.split(",") if n.strip()]
    if not raw_names:
        return {"date": the_date.isoformat(), "season": season, "items": [], "debug": [{"note": "no names provided"}]}

    # soft verify (doesn't filter names, provides context)
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
            debugs.append({"name": nm, "error": "player not found (no exact match)"})
            continue

        avg = _season_avg(pid, season)
        team_name = _person_team(pid)  # fetch current team name

        if avg is None:
            debugs.append({"name": nm, "pid": pid, "team": team_name or "", "error": "season average unavailable"})
            continue

        if avg < min_season_avg:
            debugs.append({
                "name": nm,
                "pid": pid,
                "team": team_name or "",
                "skip": f"season_avg {avg:.3f} < min {min_season_avg:.3f}",
            })
            continue

        streak = _compute_hitless_streak(pid, the_date, last_n)
        if streak < min_hitless_games:
            debugs.append({
                "name": nm,
                "pid": pid,
                "team": team_name or "",
                "skip": f"hitless_streak {streak} < min {min_hitless_games}",
            })
            continue

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
