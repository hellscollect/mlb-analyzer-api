# routes/cold_candidates.py
from fastapi import APIRouter, Query, Request, HTTPException
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, date as date_cls
import unicodedata
import httpx
import pytz

router = APIRouter()

MLB_API = "https://statsapi.mlb.com/api/v1"

# ---- local date parsing (don’t import from main to avoid circulars)
def parse_date(d: Optional[str]) -> date_cls:
    tz = pytz.timezone("America/New_York")
    today = datetime.now(tz).date()
    if not d or d.lower() == "today":
        return today
    s = d.lower()
    if s == "yesterday":
        return today - timedelta(days=1)
    if s == "tomorrow":
        return today + timedelta(days=1)
    return datetime.strptime(d, "%Y-%m-%d").date()

# ---- name helpers
def _normalize_name(s: str) -> str:
    """lowercase, remove accents/punctuation/spaces differences."""
    if not s:
        return s
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace(".", "").replace(",", "").replace("-", " ").strip().lower()
    s = " ".join(s.split())
    return s

def _name_keys(full_name: str) -> List[str]:
    """generate a couple of tolerant keys (handles Jr, periods)."""
    base = _normalize_name(full_name)
    keys = {base}
    # allow/no 'jr'
    if base.endswith(" jr"):
        keys.add(base[:-3].strip())
    else:
        keys.add((base + " jr").strip())
    return list(keys)

# ---- schedule helpers
def _collect_not_started_team_ids(schedule: Dict[str, Any]) -> Tuple[List[int], Dict[int, str]]:
    """
    Return team IDs that have NOT started yet (strict).
    We consider NOT started only when status.detailedState in {'Preview','Pre-Game'}.
    Anything else ('Warmup','In Progress','Final','Game Over', etc.) is treated as started.
    """
    not_started_ids: List[int] = []
    id_to_name: Dict[int, str] = {}
    dates = schedule.get("dates") or []
    if not dates:
        return not_started_ids, id_to_name

    for d in dates:
        for g in d.get("games", []):
            st = (g.get("status") or {}).get("detailedState", "")
            home = (g.get("teams", {}).get("home", {}) or {}).get("team", {}) or {}
            away = (g.get("teams", {}).get("away", {}) or {}).get("team", {}) or {}
            home_id, home_name = home.get("id"), home.get("name")
            away_id, away_name = away.get("id"), away.get("name")

            started = st not in ("Preview", "Pre-Game")
            if not started:
                if isinstance(home_id, int):
                    not_started_ids.append(home_id)
                    if home_name:
                        id_to_name[home_id] = home_name
                if isinstance(away_id, int):
                    not_started_ids.append(away_id)
                    if away_name:
                        id_to_name[away_id] = away_name

    # de-dupe, preserve order
    seen = set()
    ordered = []
    for tid in not_started_ids:
        if tid not in seen:
            seen.add(tid)
            ordered.append(tid)
    return ordered, id_to_name

# ---- roster + logs
def _roster_url(team_id: int, season: int) -> str:
    return f"{MLB_API}/teams/{team_id}/roster?rosterType=active&season={season}"

def _gamelog_url(person_id: int, season: int) -> str:
    return f"{MLB_API}/people/{person_id}/stats?stats=gameLog&group=hitting&gameType=R&season={season}"

def _opponent_from_split(split: Dict[str, Any]) -> str:
    team = (split.get("team") or {}).get("name")
    opp = (split.get("opponent") or {}).get("name")
    home_away = split.get("isHome", False)
    vs = "vs" if home_away else "@"
    if team and opp:
        return f"{team} {vs} {opp}"
    return opp or ""

def _compute_season_avg_and_streak(splits: List[Dict[str, Any]], cutoff: date_cls) -> Tuple[float, int, List[Dict[str, Any]]]:
    """
    splits: MLB 'gameLog' splits for the season.
    cutoff: ONLY count games with date < cutoff (ignore today).
    Returns (season_avg_through_yesterday, hitless_streak, recent_for_debug)
    - streak: consecutive games (backwards) with AB>0 and H==0 until a hit.
    - season avg is computed from ALL completed games before cutoff (H/AB).
    - recent_for_debug: up to 5 most recent completed games (AB and H echoed).
    """
    # filter completed prior to cutoff
    parsed: List[Tuple[date_cls, Dict[str, Any]]] = []
    for s in splits:
        try:
            d = datetime.strptime(s.get("date"), "%Y-%m-%d").date()
        except Exception:
            # older payloads sometimes use 'gameDate'
            try:
                d = datetime.fromisoformat(s.get("gameDate", "")[:10]).date()
            except Exception:
                continue
        if d >= cutoff:
            continue
        parsed.append((d, s))

    # sort newest first
    parsed.sort(key=lambda x: x[0], reverse=True)

    # season totals (through yesterday)
    tot_ab = 0
    tot_h = 0
    for _, s in parsed:
        st = s.get("stat") or {}
        ab = int(st.get("atBats") or 0)
        h = int(st.get("hits") or 0)
        tot_ab += ab
        tot_h += h
    season_avg = round((tot_h / tot_ab), 3) if tot_ab > 0 else 0.0

    # hitless streak (newest backwards), skip AB=0 games
    streak = 0
    for _, s in parsed:
        st = s.get("stat") or {}
        ab = int(st.get("atBats") or 0)
        h = int(st.get("hits") or 0)
        if ab == 0:
            continue
        if h == 0:
            streak += 1
            continue
        # first game with a hit ends the streak
        break

    # recent for debug (show last 5 completed games regardless of AB)
    recent = []
    for d, s in parsed[:5]:
        st = s.get("stat") or {}
        recent.append({
            "date": d.isoformat(),
            "opp": _opponent_from_split(s),
            "AB": int(st.get("atBats") or 0),
            "H": int(st.get("hits") or 0),
        })

    return season_avg, streak, recent

def _build_name_index(rosters: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    rosters: list of {'teamId','teamName','roster':[{'person':{'id','fullName'}, ...}]}
    returns: map of name-key -> {'id', 'teamId', 'teamName', 'fullName'}
    """
    out: Dict[str, Dict[str, Any]] = {}
    for r in rosters:
        tid = r["teamId"]
        tname = r["teamName"]
        for entry in r.get("roster", []):
            p = entry.get("person") or {}
            pid = p.get("id")
            fname = p.get("fullName") or ""
            if not pid or not fname:
                continue
            for k in _name_keys(fname):
                out[k] = {"id": pid, "teamId": tid, "teamName": tname, "fullName": fname}
    return out

@router.get("/cold_candidates", operation_id="cold_candidates")
def cold_candidates(
    request: Request,
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    names: Optional[str] = Query(None, description="Comma-separated full names"),
    min_season_avg: float = Query(0.260),
    min_hitless_games: int = Query(1, ge=0, description="Minimum consecutive hitless games (last completed games only)"),
    last_n: int = Query(7, ge=1, le=30, description="For debug preview only; streak is NOT capped by this"),
    limit: int = Query(50, ge=1, le=200),
    verify: int = Query(0, ge=0, le=1),
    debug: int = Query(0, ge=0, le=1),
):
    """
    Finds cold-hitter candidates for the given date.
    Rules:
      - Consider ONLY teams whose game has NOT started yet (status in {'Preview','Pre-Game'} at query time).
      - Ignore today's live/in-progress stats completely.
      - Compute season AVG from all completed games before the date.
      - Compute hitless streak by walking backward through completed games with AB>0 until first game with H>0.
    """
    the_date = parse_date(date)
    season = the_date.year

    provider = getattr(request.app.state, "provider", None)
    sched_fn = getattr(provider, "schedule_for_date", None) if provider else None
    if not sched_fn or not callable(sched_fn):
        raise HTTPException(status_code=501, detail="Provider does not implement schedule_for_date()")

    # schedule + not-started teams
    schedule = sched_fn(date_str=the_date.isoformat(), date=the_date, debug=bool(debug))
    not_started_ids, id_to_name = _collect_not_started_team_ids(schedule)

    result_items: List[Dict[str, Any]] = []
    dbg: List[Dict[str, Any]] = []

    # If there are no not-started teams, short-circuit
    if not not_started_ids:
        out = {"date": the_date.isoformat(), "season": season, "items": [], "debug": []}
        if verify == 1:
            out["verify"] = {
                "not_started_team_ids": [],
                "not_started_team_names": [],
            }
        return out

    # Fetch active rosters for ALL not-started teams (single client for speed)
    rosters_payload: List[Dict[str, Any]] = []
    with httpx.Client(timeout=15.0) as client:
        for tid in not_started_ids:
            try:
                r = client.get(_roster_url(tid, season))
                r.raise_for_status()
                rosters_payload.append({
                    "teamId": tid,
                    "teamName": id_to_name.get(tid, f"Team {tid}"),
                    "roster": r.json().get("roster") or [],
                })
            except httpx.HTTPError as e:
                # If roster fails, skip this team
                dbg.append({"team_id": tid, "error": f"roster fetch failed: {type(e).__name__}: {e}"})

    name_index = _build_name_index(rosters_payload)

    # Decide candidates:
    requested_names: List[str] = []
    if names:
        requested_names = [n.strip() for n in names.split(",") if n.strip()]
    else:
        # If no names provided, scan everyone on these rosters (can be large)
        requested_names = [rec["fullName"] for rec in name_index.values()]

    # Resolve names to player IDs using the roster index
    resolved: List[Tuple[str, Dict[str, Any]]] = []
    for nm in requested_names:
        key = _normalize_name(nm)
        hit = name_index.get(key)
        if not hit:
            # try tolerant keys (Jr variants)
            for alt in _name_keys(nm):
                if alt in name_index:
                    hit = name_index[alt]
                    break
        if not hit:
            dbg.append({"name": nm, "skip": "no not-started game today (not found on any active roster of a not-started team)"})
            continue
        resolved.append((nm, hit))

    # Fetch game logs and compute metrics
    with httpx.Client(timeout=20.0) as client:
        for display_name, rec in resolved:
            pid = rec["id"]
            team_name = rec["teamName"]

            try:
                resp = client.get(_gamelog_url(pid, season))
                resp.raise_for_status()
                data = resp.json()
                splits = (((data.get("stats") or [])[:1] or [{}])[0].get("splits")) or []
            except httpx.HTTPError as e:
                dbg.append({"name": display_name, "team": team_name, "error": f"game logs fetch failed: {type(e).__name__}: {e}"})
                continue

            season_avg, streak, recent = _compute_season_avg_and_streak(splits, cutoff=the_date)

            # thresholds
            if season_avg < float(min_season_avg):
                dbg.append({"name": display_name, "team": team_name, "skip": f"season_avg {season_avg:.3f} < min {float(min_season_avg):.3f}"})
                continue
            if streak < int(min_hitless_games):
                dbg.append({"name": display_name, "team": team_name, "skip": f"hitless_streak {streak} < min {int(min_hitless_games)}"})
                continue

            row = {
                "name": display_name,
                "team": team_name,
                "season_avg": round(season_avg, 3),
                "hitless_streak": streak,
            }
            result_items.append(row)

            if debug == 1:
                # attach recent breakdown for easy human verification
                row_dbg = {"name": display_name, "recent": recent}
                dbg.append(row_dbg)

    # de-dupe & sort (longest streak first, then highest AVG)
    deduped: Dict[str, Dict[str, Any]] = {}
    for r in result_items:
        deduped[r["name"]] = r
    result_items = list(deduped.values())
    result_items.sort(key=lambda x: (-x["hitless_streak"], -x["season_avg"]))

    # apply limit
    result_items = result_items[:limit]

    out: Dict[str, Any] = {
        "date": the_date.isoformat(),
        "season": season,
        "items": result_items,
        "debug": dbg if debug == 1 else [],
    }

    if verify == 1:
        out["verify"] = {
            "not_started_team_ids": not_started_ids[:10],
            "not_started_team_names": [id_to_name.get(tid, str(tid)) for tid in not_started_ids[:10]],
        }

    return out
