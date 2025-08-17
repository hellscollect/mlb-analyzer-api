# routes/cold_candidates.py
from fastapi import APIRouter, Query, HTTPException
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import unicodedata
import httpx

router = APIRouter(prefix="", tags=["cold_candidates"])

STATS_BASE = "https://statsapi.mlb.com/api/v1"

def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def _norm(s: str) -> str:
    return _strip_accents((s or "").lower().strip())

def _today_str() -> str:
    # keep simple; upstream date parsing handled elsewhere in app
    return datetime.utcnow().date().isoformat()

def _is_not_started(status: Dict[str, Any]) -> bool:
    # Not started: abstractGameCode == "P" or detailedState in these values
    agc = (status or {}).get("abstractGameCode", "")
    detailed = (status or {}).get("detailedState", "")
    return agc == "P" or detailed in ("Preview", "Pre-Game", "Warmup")

async def _get_not_started_team_ids(client: httpx.AsyncClient, date_str: str) -> Tuple[List[int], Dict[str, Any]]:
    q = {"sportId": 1, "date": date_str}
    r = await client.get(f"{STATS_BASE}/schedule", params=q, timeout=15)
    r.raise_for_status()
    data = r.json()
    team_ids: List[int] = []
    dbg = {"games": []}
    for d in data.get("dates", []):
        for g in d.get("games", []):
            status = g.get("status", {})
            dbg["games"].append({
                "gamePk": g.get("gamePk"),
                "abstractGameCode": status.get("abstractGameCode"),
                "detailedState": status.get("detailedState"),
            })
            if _is_not_started(status):
                away = g.get("teams", {}).get("away", {}).get("team", {})
                home = g.get("teams", {}).get("home", {}).get("team", {})
                if isinstance(away.get("id"), int):
                    team_ids.append(away["id"])
                if isinstance(home.get("id"), int):
                    team_ids.append(home["id"])
    return sorted(set(team_ids)), dbg

async def _fetch_active_roster(client: httpx.AsyncClient, team_id: int, season: int) -> Dict[str, Any]:
    # Active roster endpoint
    params = {"season": season}
    r = await client.get(f"{STATS_BASE}/teams/{team_id}/roster/Active", params=params, timeout=15)
    r.raise_for_status()
    return r.json()

async def _build_roster_index(client: httpx.AsyncClient, team_ids: List[int], season: int) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    """
    Return:
      by_full_name_norm: fullName_norm -> player dict {id, fullName, teamId, teamName}
      by_last_name_norm: lastName_norm -> [player dicts ...]
    """
    by_full: Dict[str, Dict[str, Any]] = {}
    by_last: Dict[str, List[Dict[str, Any]]] = {}
    for tid in team_ids:
        try:
            data = await _fetch_active_roster(client, tid, season)
        except httpx.HTTPError:
            continue
        roster = data.get("roster") or []
        team_name = (data.get("teamName") or "") or (data.get("teams", [{}])[0].get("name") if data.get("teams") else "")
        for row in roster:
            person = row.get("person") or {}
            pid = person.get("id")
            full = person.get("fullName") or ""
            if not isinstance(pid, int) or not full:
                continue
            entry = {"id": pid, "fullName": full, "teamId": tid, "teamName": team_name}
            by_full[_norm(full)] = entry
            parts = [p for p in full.split() if p]
            if parts:
                last_norm = _norm(parts[-1])
                by_last.setdefault(last_norm, []).append(entry)
    return by_full, by_last

async def _season_avg(client: httpx.AsyncClient, person_id: int, season: int) -> Optional[float]:
    params = {"stats": "season", "group": "hitting", "season": season}
    r = await client.get(f"{STATS_BASE}/people/{person_id}/stats", params=params, timeout=15)
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
    params = {"stats": "gameLog", "group": "hitting", "season": season}
    r = await client.get(f"{STATS_BASE}/people/{person_id}/stats", params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    splits = []
    for s in (data.get("stats") or []):
        splits.extend(s.get("splits") or [])
    # Ensure newest first by date
    splits.sort(key=lambda x: x.get("date", ""), reverse=True)

    streak = 0
    checked = 0
    for sp in splits:
        if checked >= max(last_n, 1):
            break
        stat = sp.get("stat", {}) or {}
        try:
            ab = int(stat.get("atBats") or 0)
            hits = int(stat.get("hits") or 0)
        except Exception:
            ab, hits = 0, 0
        if ab <= 0:
            # skip games without AB for streak purposes
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
    Resolution flow (no /people?search):
      1) Find today's NOT-STARTED team IDs.
      2) Build an index of active rosters for those teams.
      3) Resolve each requested name by full match; else last-name (+first initial) within those rosters.
      4) For resolved players, fetch season AVG and consecutive hitless streak (last_n).
      5) Apply min_season_avg AND min_hitless_games filters.
    """
    when = (date or "today").strip().lower()
    date_str = _today_str() if when in ("", "today") else when
    season = int(date_str.split("-")[0])

    if not names:
        return {"date": date_str, "season": season, "items": [], "debug": [{"note": "no names provided"}]}

    raw_names = [n.strip() for n in names.split(",") if n.strip()]
    items: List[Dict[str, Any]] = []
    dbg: List[Dict[str, Any]] = []

    async with httpx.AsyncClient() as client:
        try:
            not_started_team_ids, sched_dbg = await _get_not_started_team_ids(client, date_str)
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"schedule fetch failed: {type(e).__name__}: {e}") from e

        if not not_started_team_ids:
            # nothing left today that hasn't started
            return {"date": date_str, "season": season, "items": [], "debug": [{"note": "no not-started teams today"}]}

        by_full, by_last = await _build_roster_index(client, not_started_team_ids, season)

        for name in raw_names:
            nn = _norm(name)
            candidate = by_full.get(nn)
            if not candidate:
                parts = [p for p in name.split() if p]
                last = _norm(parts[-1]) if parts else ""
                pool = by_last.get(last, [])
                if pool:
                    if len(parts) >= 1:
                        first_initial = _norm(parts[0])[:1]
                        narrowed = [p for p in pool if _norm(p["fullName"])[:1] == first_initial]
                        candidate = (narrowed or pool)[0]
                    else:
                        candidate = pool[0]

            if not candidate:
                dbg.append({"name": name, "skip": "not on any active roster of a not-started team today"})
                continue

            pid = candidate["id"]
            team_name = candidate["teamName"] or ""
            try:
                avg = await _season_avg(client, pid, season)
            except httpx.HTTPError as e:
                dbg.append({"name": name, "team": team_name, "error": f"HTTPError fetching season stats: {e}"})
                continue
            if avg is None:
                dbg.append({"name": name, "team": team_name, "skip": "no season avg"})
                continue

            try:
                streak = await _hitless_streak(client, pid, season, last_n)
            except httpx.HTTPError as e:
                dbg.append({"name": name, "team": team_name, "error": f"HTTPError fetching game logs: {e}"})
                continue

            # thresholds
            if avg < min_season_avg:
                dbg.append({"name": candidate["fullName"], "team": team_name, "skip": f"season_avg {avg:.3f} < min {min_season_avg:.3f}"})
                continue
            if streak < min_hitless_games:
                dbg.append({"name": candidate["fullName"], "team": team_name, "skip": f"hitless_streak {streak} < min {min_hitless_games}"})
                continue

            items.append({
                "name": candidate["fullName"],
                "team": team_name,
                "season_avg": round(avg, 3),
                "hitless_streak": streak,
            })

    items = items[:limit]
    out = {"date": date_str, "season": season, "items": items}
    if debug or verify:
        out["debug"] = dbg
    return out
