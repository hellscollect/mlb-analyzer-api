# routes/cold_candidates.py
from fastapi import APIRouter, Query
from fastapi import HTTPException
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import unicodedata
import httpx

router = APIRouter(prefix="", tags=["cold_candidates"])

STATS_BASE = "https://statsapi.mlb.com/api/v1"

def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )

def _norm(s: str) -> str:
    return _strip_accents(s or "").lower().strip()

def _today_str_yyyy_mm_dd() -> str:
    # local NY time semantics live elsewhere; here we accept date from query
    return datetime.utcnow().date().isoformat()

def _is_not_started(status: Dict[str, Any]) -> bool:
    # Treat Preview / Pre-Game / Warmup as "not started"
    # From MLB schedule payload: abstractGameCode: "P" for not-started, "L" live, "F"/"O" finished
    agc = (status or {}).get("abstractGameCode", "")
    detailed = (status or {}).get("detailedState", "")
    return agc == "P" or detailed in ("Preview", "Pre-Game", "Warmup")

async def _get_not_started_team_ids(client: httpx.AsyncClient, date_str: str) -> Tuple[List[int], Dict[str, Any]]:
    q = {"sportId": 1, "date": date_str}
    r = await client.get(f"{STATS_BASE}/schedule", params=q, timeout=15)
    r.raise_for_status()
    data = r.json()
    team_ids: List[int] = []
    debug = {"games": []}
    for d in data.get("dates", []):
        for g in d.get("games", []):
            status = g.get("status", {})
            if _is_not_started(status):
                away = g.get("teams", {}).get("away", {}).get("team", {})
                home = g.get("teams", {}).get("home", {}).get("team", {})
                aid = away.get("id")
                hid = home.get("id")
                if isinstance(aid, int):
                    team_ids.append(aid)
                if isinstance(hid, int):
                    team_ids.append(hid)
            debug["games"].append({
                "gamePk": g.get("gamePk"),
                "status": status.get("detailedState"),
                "abstractGameCode": status.get("abstractGameCode"),
            })
    team_ids = sorted(set(team_ids))
    return team_ids, debug

async def _people_search(client: httpx.AsyncClient, name: str) -> List[Dict[str, Any]]:
    """
    Robust search: try full name; if empty, try last-name-only and filter.
    """
    name = (name or "").strip()
    if not name:
        return []
    # 1) direct
    r = await client.get(f"{STATS_BASE}/people", params={"search": name}, timeout=15)
    if r.status_code == 400:
        # retry with ascii
        r = await client.get(f"{STATS_BASE}/people", params={"search": _strip_accents(name)}, timeout=15)
    r.raise_for_status()
    data = r.json() if r.content else {}
    people = data.get("people", []) or []

    if people:
        return people

    # 2) last-name fallback
    parts = [p for p in name.split() if p]
    if len(parts) >= 2:
        last = parts[-1]
        r2 = await client.get(f"{STATS_BASE}/people", params={"search": last}, timeout=15)
        if r2.status_code == 400:
            r2 = await client.get(f"{STATS_BASE}/people", params={"search": _strip_accents(last)}, timeout=15)
        r2.raise_for_status()
        d2 = r2.json() if r2.content else {}
        cand = d2.get("people", []) or []
        if cand:
            # prefer exact-ish last-name matches and same first initial
            first_initial = _norm(parts[0])[:1]
            last_norm = _norm(last)
            filtered = []
            for p in cand:
                full = _norm(p.get("fullName", ""))
                tokens = [t for t in full.split() if t]
                if not tokens:
                    continue
                last_tok = tokens[-1]
                fi = tokens[0][:1] if tokens else ""
                if last_tok == last_norm and (not first_initial or fi == first_initial):
                    filtered.append(p)
            if filtered:
                return filtered
            return cand  # fall back to any last-name match
    return []

async def _season_avg(client: httpx.AsyncClient, person_id: int, season: int) -> Optional[float]:
    q = {"stats": "season", "group": "hitting", "season": season}
    r = await client.get(f"{STATS_BASE}/people/{person_id}/stats", params=q, timeout=15)
    r.raise_for_status()
    data = r.json()
    for s in (data.get("stats") or []):
        for sp in (s.get("splits") or []):
            stat = sp.get("stat", {})
            avg = stat.get("avg")
            if avg is not None:
                try:
                    return float(avg)
                except Exception:
                    pass
    return None

async def _hitless_streak(client: httpx.AsyncClient, person_id: int, season: int, last_n: int) -> int:
    """
    Count **consecutive** games (most recent backwards) with AB>0 and H==0.
    """
    q = {"stats": "gameLog", "group": "hitting", "season": season}
    r = await client.get(f"{STATS_BASE}/people/{person_id}/stats", params=q, timeout=15)
    r.raise_for_status()
    data = r.json()
    splits = []
    for s in (data.get("stats") or []):
        splits.extend(s.get("splits") or [])
    # Ensure most-recent first: game logs usually are newest first, but be safe:
    # Each split may have "date" YYYY-MM-DD
    splits.sort(key=lambda x: x.get("date", ""), reverse=True)

    streak = 0
    checked = 0
    for sp in splits:
        if checked >= max(last_n, 1):
            break
        stat = sp.get("stat", {})
        try:
            ab = int(stat.get("atBats") or 0)
            hits = int(stat.get("hits") or 0)
        except Exception:
            ab = 0
            hits = 0
        if ab <= 0:
            # skip games without an AB for streak purposes
            continue
        checked += 1
        if hits == 0:
            streak += 1
        else:
            break
    return streak

@router.get("/cold_candidates", name="cold_candidates")
async def cold_candidates(
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    names: Optional[str] = Query(None, description="Comma-separated full names"),
    min_season_avg: float = Query(0.260, ge=0.0, le=1.0),
    min_hitless_games: int = Query(1, ge=0),
    last_n: int = Query(7, ge=1, le=30),
    limit: int = Query(50, ge=1, le=200),
    verify: int = Query(0, ge=0, le=1),
    debug: int = Query(0, ge=0, le=1),
):
    """
    Logic:
      1) Get NOT-STARTED team IDs for the given date.
      2) Resolve each provided name -> player(s) (robust search).
      3) Keep only players whose currentTeam.id is in NOT-STARTED set.
      4) Compute season AVG and hitless STREAK(last_n).
      5) Filter by min_season_avg & min_hitless_games.
    """
    # Resolve 'date'
    when = (date or "today").lower().strip()
    if when in ("today", ""):
        date_str = _today_str_yyyy_mm_dd()
    else:
        date_str = when

    items: List[Dict[str, Any]] = []
    dbg: List[Dict[str, Any]] = []

    if not names:
        return {"date": date_str, "season": int(date_str.split("-")[0]), "items": [], "debug": [{"note": "no names provided"}]}

    # Collect not-started teams
    async with httpx.AsyncClient() as client:
        try:
            not_started_team_ids, sched_dbg = await _get_not_started_team_ids(client, date_str)
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"schedule fetch failed: {type(e).__name__}: {e}") from e

        season = int(date_str.split("-")[0])
        # Process each name
        raw_names = [n.strip() for n in names.split(",") if n.strip()]
        for name in raw_names:
            try:
                people = await _people_search(client, name)
            except httpx.HTTPError as e:
                dbg.append({"name": name, "error": f"HTTPError: {e}"})
                continue

            if not people:
                dbg.append({"name": name, "error": "not found in people search"})
                continue

            # choose the best candidate: prefer exact (accents-insensitive) full match if available
            target = None
            name_norm = _norm(name)
            for p in people:
                if _norm(p.get("fullName", "")) == name_norm:
                    target = p
                    break
            if target is None:
                target = people[0]

            pid = target.get("id")
            cteam = (target.get("currentTeam") or {}).get("id")
            cteam_name = (target.get("currentTeam") or {}).get("name") or ""

            if not isinstance(pid, int):
                dbg.append({"name": name, "error": "bad/unknown player id"})
                continue

            if not isinstance(cteam, int):
                dbg.append({"name": name, "skip": "no current team on record"})
                continue

            if cteam not in not_started_team_ids:
                dbg.append({"name": name, "skip": "no not-started game today (not found on any active roster of a not-started team)"})
                continue

            # stats
            avg = await _season_avg(client, pid, season)
            if avg is None:
                dbg.append({"name": name, "team": cteam_name, "skip": "no season avg"})
                continue

            streak = await _hitless_streak(client, pid, season, last_n)
            # Filter per thresholds
            if avg < min_season_avg:
                dbg.append({"name": name, "team": cteam_name, "skip": f"season_avg {avg:.3f} < min {min_season_avg:.3f}"})
                continue
            if streak < min_hitless_games:
                dbg.append({"name": name, "team": cteam_name, "skip": f"hitless_streak {streak} < min {min_hitless_games}"})
                continue

            items.append({
                "name": target.get("fullName") or name,
                "team": cteam_name,
                "season_avg": round(avg, 3),
                "hitless_streak": streak,
            })

    # enforce limit
    items = items[:limit]
    out = {"date": date_str, "season": season, "items": items, "debug": dbg if debug or verify else []}
    return out
