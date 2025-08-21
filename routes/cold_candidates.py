# routes/cold_candidates.py
from __future__ import annotations

from fastapi import APIRouter, Query
from typing import Dict, List, Optional, Iterable, Any
from datetime import datetime, timezone
import unicodedata
import httpx
import pytz

router = APIRouter()
MLB_BASE = "https://statsapi.mlb.com/api/v1"

# ---------- time & utils ----------
def _eastern_today_str() -> str:
    tz = pytz.timezone("US/Eastern")
    return datetime.now(tz).date().isoformat()

def _normalize(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower().strip()

def _fetch_json(client: httpx.Client, url: str, params: Optional[Dict] = None) -> Dict:
    r = client.get(url, params=params)
    r.raise_for_status()
    return r.json()

def _parse_dt(maybe: Optional[str]) -> Optional[datetime]:
    """Parse MLB date strings robustly; return timezone-aware UTC if possible."""
    if not maybe:
        return None
    s = str(maybe)
    # Primary: MLB gameDate is ISO like "2025-08-21T23:10:00Z"
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        pass
    # Fallbacks
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None

# ---------- schedule helpers ----------
def _schedule_for_date(client: httpx.Client, date_str: str) -> Dict:
    return _fetch_json(client, f"{MLB_BASE}/schedule", params={"sportId": 1, "date": date_str})

def _not_started_team_ids_for_date(schedule_json: Dict) -> set[int]:
    ns_ids: set[int] = set()
    for d in schedule_json.get("dates", []):
        for g in d.get("games", []):
            code = (g.get("status", {}) or {}).get("statusCode", "")
            if code in ("P", "S"):  # Preview / Scheduled (not started)
                try:
                    ns_ids.add(int(g["teams"]["home"]["team"]["id"]))
                    ns_ids.add(int(g["teams"]["away"]["team"]["id"]))
                except Exception:
                    pass
    return ns_ids

def _team_ids_from_schedule(schedule_json: Dict) -> List[int]:
    ids: set[int] = set()
    for d in schedule_json.get("dates", []):
        for g in d.get("games", []):
            try:
                ids.add(int(g["teams"]["home"]["team"]["id"]))
                ids.add(int(g["teams"]["away"]["team"]["id"]))
            except Exception:
                pass
    return sorted(ids)

def _all_mlb_team_ids(client: httpx.Client, season: int) -> List[int]:
    data = _fetch_json(client, f"{MLB_BASE}/teams", params={"sportId": 1, "season": season})
    teams = data.get("teams", []) or []
    out: List[int] = []
    for t in teams:
        try:
            out.append(int(t["id"]))
        except Exception:
            pass
    return sorted(out)

def _team_active_roster_people(client: httpx.Client, team_id: int, season: int) -> List[Dict]:
    data = _fetch_json(client, f"{MLB_BASE}/teams/{team_id}/roster", params={"rosterType": "active", "season": season})
    return data.get("roster", []) or []

def _iter_league_player_names_for_scan(client: httpx.Client, season: int, date_str: str) -> Iterable[str]:
    # Prefer today's scheduled teams; fallback to all teams if schedule missing.
    sched = _schedule_for_date(client, date_str)
    team_ids = _team_ids_from_schedule(sched) or _all_mlb_team_ids(client, season)
    for tid in team_ids:
        roster = _team_active_roster_people(client, tid, season)
        for r in roster:
            person = r.get("person") or {}
            full = person.get("fullName")
            if full:
                yield full

# ---------- player helpers ----------
def _search_player(client: httpx.Client, name: str) -> Optional[Dict]:
    data = _fetch_json(client, f"{MLB_BASE}/people/search", params={"names": name})
    people = data.get("people", []) or []
    if not people:
        return None
    target = _normalize(name)
    for p in people:
        full = p.get("fullName") or ""
        if _normalize(full) == target:
            return p
    return people[0]

def _person_with_stats(client: httpx.Client, pid: int, season: int) -> Dict:
    hydrate = f"team,stats(group=hitting,type=season,season={season})"
    data = _fetch_json(client, f"{MLB_BASE}/people/{pid}", params={"hydrate": hydrate})
    people = data.get("people", []) or [{}]
    return people[0]

def _season_avg_from_people(people_entry: Dict) -> Optional[float]:
    stats = (people_entry.get("stats") or [])
    for block in stats:
        if (block.get("group", {}).get("displayName") == "hitting" and
            block.get("type", {}).get("displayName", "").lower() == "season"):
            splits = block.get("splits") or []
            if splits:
                avg_str = (splits[0].get("stat") or {}).get("avg")
                try:
                    return float(avg_str)
                except (TypeError, ValueError):
                    return None
    return None

def _game_log_newest_first_regular_season(client: httpx.Client, pid: int, season: int, max_entries: int = 120) -> List[Dict]:
    """
    Fetch hitting game logs and return REGULAR SEASON games only, newest first,
    sorted by parsed 'gameDate' (fallback to 'date').
    """
    data = _fetch_json(
        client,
        f"{MLB_BASE}/people/{pid}/stats",
        params={"stats": "gameLog", "group": "hitting", "season": season, "sportIds": 1},
    )
    splits = ((data.get("stats") or [{}])[0].get("splits")) or []

    # Filter to regular season when we can detect it
    filtered: List[Dict] = []
    for s in splits:
        gt = s.get("gameType")
        if gt is not None and gt != "R":
            continue
        filtered.append(s)

    def sort_key(s: Dict[str, Any]) -> tuple:
        dt = _parse_dt(s.get("gameDate") or s.get("date"))
        # Use negative timestamp for descending; None should be very old
        ts = dt.timestamp() if dt else -1.0
        # GamePk as tie-breaker (higher = newer in practice)
        pk = s.get("game", {}).get("gamePk") or s.get("gamePk") or 0
        try:
            pk = int(pk)
        except Exception:
            pk = 0
        return (ts, pk)

    filtered.sort(key=sort_key, reverse=True)
    return filtered[:max_entries]

def _current_hitless_streak_ab_gt0(game_splits: List[Dict], debug_capture: Optional[List[Dict]] = None) -> int:
    """
    Count consecutive MOST-RECENT games with AB>0 and H==0; skip 0-AB/DNP entirely.
    If debug_capture is provided, append the first 3 AB>0 games encountered with (date, ab, hits).
    """
    streak = 0
    captured = 0
    for s in game_splits:
        stat = s.get("stat") or {}
        ab = int(stat.get("atBats") or 0)
        if ab <= 0:
            continue
        hits = int(stat.get("hits") or 0)
        if debug_capture is not None and captured < 3:
            debug_capture.append({
                "date": s.get("gameDate") or s.get("date"),
                "ab": ab,
                "hits": hits
            })
            captured += 1
        if hits == 0:
            streak += 1
        else:
            break
    return streak

# ---------- route ----------
@router.get("/cold_candidates")
def cold_candidates(
    date: str = Query("today", description="YYYY-MM-DD or 'today' (US/Eastern)"),
    season: int = Query(2025, ge=1900, le=2100),
    names: Optional[str] = Query(None, description="Optional comma-separated player names. If omitted, scans league rosters."),
    min_season_avg: float = Query(0.26, ge=0.0, le=1.0, description="Only include hitters with season AVG ≥ this (default .260)."),
    min_hitless_games: int = Query(1, ge=1, description="Current hitless streak (AB>0) must be ≥ this."),
    limit: int = Query(30, ge=1, le=1000),
    verify: int = Query(1, ge=0, le=1, description="1 = only include players on teams that have NOT started yet today. If empty after filter, we auto-fallback to include all teams."),
    # Back-compat: accept but ignore last_n so older Action calls don't break.
    last_n: Optional[int] = Query(None, description="Ignored. Accepted for backward compatibility."),
    debug: int = Query(0, ge=0, le=1),
):
    """
    Returns players with season AVG ≥ min_season_avg AND a current hitless streak ≥ min_hitless_games.
    - Streak = consecutive most-recent AB>0 games with 0 hits (DNP/0-AB ignored).
    - Uses robust 'gameDate' ordering and filters to regular season games (gameType 'R' when present).
    - If names omitted, scans today's scheduled teams (fallback: all MLB).
    - If verify=1, tries pregame-only; if that yields empty, auto-fallback to all teams.
    Response: { "date": "...", "candidates": [...], "debug": [...]? }
    """
    date_str = _eastern_today_str() if _normalize(date) == "today" else date

    with httpx.Client(timeout=25) as client:
        sched = _schedule_for_date(client, date_str)
        ns_team_ids = _not_started_team_ids_for_date(sched) if (verify or debug) else set()

        # Build scan list
        if names:
            requested = [n.strip() for n in names.split(",") if n.strip()]
        else:
            requested = list(_iter_league_player_names_for_scan(client, season, date_str))

        pre_candidates: List[Dict] = []
        debug_list: Optional[List[Dict]] = [] if debug else None
        seen: set[str] = set()

        for name in requested:
            key = _normalize(name)
            if key in seen:
                continue
            seen.add(key)

            try:
                p = _search_player(client, name)
                if not p:
                    if debug_list is not None:
                        debug_list.append({"name": name, "skip": "player not found"})
                    continue

                pid = int(p["id"])
                person = _person_with_stats(client, pid, season)
                full = person.get("fullName") or p.get("fullName") or name
                team_info = person.get("currentTeam") or {}
                team_id = team_info.get("id")
                team_name = (team_info.get("name") or "").strip()

                # Season AVG gate
                season_avg = _season_avg_from_people(person)
                if season_avg is None or season_avg < min_season_avg:
                    if debug_list is not None:
                        reason = "no season stats" if season_avg is None else f"season_avg {season_avg:.3f} < {min_season_avg:.3f}"
                        debug_list.append({"name": full, "team": team_name, "skip": reason})
                    continue

                # Current hitless streak (AB>0 only), using robust-ordered regular-season logs
                last_ab_samples: List[Dict] = [] if debug else None  # type: ignore
                logs = _game_log_newest_first_regular_season(client, pid, season, max_entries=120)
                streak = _current_hitless_streak_ab_gt0(logs, debug_capture=last_ab_samples)

                if streak < min_hitless_games:
                    if debug_list is not None:
                        dbg = {"name": full, "team": team_name, "skip": f"hitless_streak {streak} < {min_hitless_games}"}
                        if last_ab_samples:
                            dbg["last_ab_samples"] = last_ab_samples
                        debug_list.append(dbg)
                    continue

                cand = {
                    "name": full,
                    "team": team_name,
                    "season_avg": round(season_avg, 3),
                    "hitless_streak": streak,
                    "_team_id": team_id,
                }
                if debug_list is not None and last_ab_samples:
                    cand["_last_ab_samples"] = last_ab_samples  # candidate-level audit

                pre_candidates.append(cand)

                if len(pre_candidates) >= max(limit * 3, 60):  # reasonable cap during scans
                    break

            except Exception as e:
                if debug_list is not None:
                    debug_list.append({"name": name, "error": f"{type(e).__name__}: {e}"})

        # Apply verify filter if requested (pregame), with auto-fallback if empty
        def _filter_by_pregame(cands: List[Dict]) -> List[Dict]:
            out: List[Dict] = []
            for it in cands:
                tid = it.get("_team_id")
                try:
                    tid_int = int(tid) if tid is not None else None
                except Exception:
                    tid_int = None
                if (tid_int is not None) and (tid_int in ns_team_ids):
                    out.append(it)
            return out

        candidates = list(pre_candidates)
        if verify:
            filtered = _filter_by_pregame(pre_candidates)
            if filtered:
                candidates = filtered
            else:
                # auto-fallback
                if debug_list is not None:
                    reason = "no candidates after pregame filter" if ns_team_ids else "no not-started teams"
                    debug_list.append({"note": f"verify auto-fallback to include all teams ({reason})"})
                candidates = pre_candidates

        # Strip internals
        for it in candidates:
            it.pop("_team_id", None)
            it.pop("_last_ab_samples", None)  # keep debug list only in debug section

        # Sort: season_avg DESC, then hitless_streak DESC
        candidates.sort(key=lambda x: (x.get("season_avg", 0.0), x.get("hitless_streak", 0)), reverse=True)

        # Limit
        candidates = candidates[:limit]

        # Response
        resp: Dict = {"date": date_str, "candidates": candidates}
        if debug_list is not None:
            resp["debug"] = debug_list
        return resp
