# routes/cold_candidates.py (FULL OVERWRITE, context-aware + real Statcast)
from __future__ import annotations

from fastapi import APIRouter, Query, HTTPException
from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime, date as date_cls, timezone, timedelta
import unicodedata
import httpx
import pytz
import math
import statistics

# --- Optional Statcast wiring ---
_STATCAST_OK = False
try:
    # pybaseball 2.2.x
    from pybaseball import statcast_batter
    _STATCAST_OK = True
except Exception:
    _STATCAST_OK = False

router = APIRouter()
MLB_BASE = "https://statsapi.mlb.com/api/v1"
_EASTERN = pytz.timezone("US/Eastern")

# ----------------- time & utils -----------------
def _eastern_today_str() -> str:
    return datetime.now(_EASTERN).date().isoformat()

def _is_today_et(ymd: str) -> bool:
    try:
        return _eastern_today_str() == ymd
    except Exception:
        return False

def _parse_ymd(s: str) -> date_cls:
    return datetime.strptime(s, "%Y-%m-%d").date()

def _next_ymd_str(s: str) -> str:
    return (_parse_ymd(s) + timedelta(days=1)).isoformat()

def _normalize(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii").lower().strip()

def _parse_dt_utc(maybe: Optional[str]) -> Optional[datetime]:
    if not maybe:
        return None
    s = str(maybe)
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None

def _fetch_json(client: httpx.Client, url: str, params: Optional[Dict] = None) -> Dict:
    r = client.get(url, params=params)
    r.raise_for_status()
    return r.json()

def _fetch_json_safe(client: httpx.Client, url: str, params: Optional[Dict], dbg: Optional[List[Dict]], label: str) -> Dict:
    try:
        return _fetch_json(client, url, params=params)
    except Exception as e:
        if dbg is not None:
            dbg.append({"fetch_error": label, "error": f"{type(e).__name__}: {e}"})
        return {}

# ----------------- schedule helpers -----------------
def _schedule_for_date(client: httpx.Client, date_str: str, dbg: Optional[List[Dict]]) -> Dict:
    return _fetch_json_safe(client, f"{MLB_BASE}/schedule", {"sportId": 1, "date": date_str}, dbg, f"schedule:{date_str}")

def _not_started_team_ids_for_date(schedule_json: Dict) -> Set[int]:
    """
    STRICT pregame set: treat P=Preview, S=Scheduled, PW=Pre-Game Warmup as NOT started.
    """
    ns_ids: Set[int] = set()
    for d in schedule_json.get("dates", []) or []:
        for g in d.get("games", []) or []:
            code = (g.get("status", {}) or {}).get("statusCode", "")
            if code in ("P", "S", "PW"):
                try:
                    ns_ids.add(int(g["teams"]["home"]["team"]["id"]))
                    ns_ids.add(int(g["teams"]["away"]["team"]["id"]))
                except Exception:
                    pass
    return ns_ids

def _team_ids_from_schedule(schedule_json: Dict) -> List[int]:
    ids: Set[int] = set()
    for d in schedule_json.get("dates", []) or []:
        for g in d.get("games", []) or []:
            try:
                ids.add(int(g["teams"]["home"]["team"]["id"]))
                ids.add(int(g["teams"]["away"]["team"]["id"]))
            except Exception:
                pass
    return sorted(ids)

def _schedule_rows(schedule_json: Dict) -> List[Tuple[str,str,int,str]]:
    """
    Returns (away@home, statusCode, gamePk, statusText) for footer.
    """
    out: List[Tuple[str,str,int,str]] = []
    for d in schedule_json.get("dates", []) or []:
        for g in d.get("games", []) or []:
            home = (((g.get("teams") or {}).get("home") or {}).get("team") or {}).get("name") or "?"
            away = (((g.get("teams") or {}).get("away") or {}).get("team") or {}).get("name") or "?"
            st = g.get("status", {}) or {}
            code = st.get("statusCode", "")
            text = st.get("detailedState") or st.get("abstractGameState") or ""
            pk = g.get("gamePk") or 0
            try: pk = int(pk)
            except: pk = 0
            out.append((f"{away} @ {home}", code, pk, text))
    return out

def _game_pks_for_date(schedule_json: Dict) -> Set[int]:
    pks: Set[int] = set()
    for d in schedule_json.get("dates", []) or []:
        for g in d.get("games", []) or []:
            pk = g.get("gamePk")
            try:
                if pk is not None:
                    pks.add(int(pk))
            except Exception:
                continue
    return pks

def _probable_pitcher_for_team(game: Dict, team_side: str) -> Optional[Dict]:
    """
    team_side: 'home' or 'away'
    """
    try:
        return ((game.get("teams") or {}).get(team_side) or {}).get("probablePitcher") or None
    except Exception:
        return None

def _all_mlb_team_ids(client: httpx.Client, season: int, dbg: Optional[List[Dict]]) -> List[int]:
    data = _fetch_json_safe(client, f"{MLB_BASE}/teams", {"sportId": 1, "season": season}, dbg, f"teams:{season}")
    teams = data.get("teams", []) or []
    out: List[int] = []
    for t in teams:
        try:
            out.append(int(t["id"]))
        except Exception:
            pass
    return sorted(out)

# ----------------- stats & logs -----------------
def _choose_best_mlb_season_split(splits: List[Dict]) -> Optional[Dict]:
    if not splits:
        return None
    def score(sp: Dict) -> Tuple[int, int]:
        league_id = (((sp.get("league") or {}) or {}).get("id"))
        sport_id = (((sp.get("sport") or {}) or {}).get("id"))
        if sport_id is None:
            sport_id = (((sp.get("team") or {}).get("sport") or {}) or {}).get("id")
        try:
            ab = int((sp.get("stat") or {}).get("atBats") or 0)
        except Exception:
            ab = 0
        pri = 2 if league_id in (103, 104) else (1 if sport_id == 1 else 0)
        return (pri, ab)
    best, key = None, (-1, -1)
    for sp in splits:
        k = score(sp)
        if k > key:
            key = k
            best = sp
    return best

def _season_avg_from_people_like(obj: Dict) -> Optional[float]:
    stats = (obj.get("stats") or [])
    for block in stats:
        gdn = (block.get("group") or {}).get("displayName", "")
        gcd = (block.get("group") or {}).get("code", "")
        tdn = (block.get("type") or {}).get("displayName", "")
        tcd = (block.get("type") or {}).get("code", "")
        if (gdn == "hitting" or gcd == "hitting") and (tdn.lower() == "season" or tcd == "season"):
            splits = block.get("splits") or []
            chosen = _choose_best_mlb_season_split(splits)
            if not chosen:
                continue
            try:
                return float(str((chosen.get("stat") or {}).get("avg")))
            except Exception:
                return None
    return None

def _season_ab_gp_from_people_like(obj: Dict) -> Tuple[Optional[int], Optional[int]]:
    stats = (obj.get("stats") or [])
    for block in stats:
        gdn = (block.get("group") or {}).get("displayName", "")
        gcd = (block.get("group") or {}).get("code", "")
        tdn = (block.get("type") or {}).get("displayName", "")
        tcd = (block.get("type") or {}).get("code", "")
        if (gdn == "hitting" or gcd == "hitting") and (tdn.lower() == "season" or tcd == "season"):
            splits = block.get("splits") or []
            chosen = _choose_best_mlb_season_split(splits)
            if not chosen:
                continue
            st = chosen.get("stat") or {}
            try:
                ab = int(st.get("atBats") or 0)
            except Exception:
                ab = None
            try:
                gp = int(st.get("gamesPlayed") or st.get("games") or 0)
            except Exception:
                gp = None
            return ab, gp
    return None, None

def _expected_abs_from_person(obj: Dict) -> float:
    ab, gp = _season_ab_gp_from_people_like(obj)
    if ab is not None and gp and gp > 0:
        try:
            v = float(ab) / float(gp)
            return max(2.0, min(5.5, v))
        except Exception:
            pass
    return 4.0

def _break_prob_from_avg_and_ab(avg: float, expected_abs: float) -> float:
    p_no_hit = (1.0 - max(0.0, min(1.0, float(avg)))) ** max(0.0, float(expected_abs))
    return 1.0 - p_no_hit

def _game_log_regular_season_desc(client: httpx.Client, pid: int, season: int, max_entries: int, dbg: Optional[List[Dict]]) -> List[Dict]:
    data = _fetch_json_safe(
        client,
        f"{MLB_BASE}/people/{pid}/stats",
        {"stats": "gameLog", "group": "hitting", "season": season, "sportIds": 1},
        dbg, f"gameLog:{pid}:{season}"
    )
    splits = ((data.get("stats") or [{}])[0].get("splits")) or []

    def is_regular(s: Dict) -> bool:
        gt = s.get("gameType")
        return (gt is None) or (gt == "R")

    def sort_key(s: Dict[str, Any]) -> tuple:
        dt = _parse_dt_utc(s.get("gameDate") or s.get("date"))
        ts = dt.timestamp() if dt else -1.0
        pk = s.get("game", {}).get("gamePk") or s.get("gamePk") or 0
        try:
            pk = int(pk)
        except Exception:
            pk = 0
        return (ts, pk)

    filtered = [s for s in splits if is_regular(s)]
    filtered.sort(key=sort_key, reverse=True)
    return filtered[:max_entries]

def _date_in_eastern(dt_utc: datetime) -> date_cls:
    return dt_utc.astimezone(_EASTERN).date()

def _extract_team_name_from_person_or_logs(
    person_like: Dict,
    team_map: Optional[Dict[int, Tuple[int, str]]] = None,
    pid: Optional[int] = None,
    logs: Optional[List[Dict]] = None,
    slate_date_ymd: Optional[str] = None,
) -> str:
    team_info = (person_like.get("currentTeam") or {}) if isinstance(person_like, dict) else {}
    name = (team_info.get("name") or "").strip()
    if name:
        return name
    t2 = (person_like.get("team") or {})
    name2 = (t2.get("name") or "").strip()
    if name2:
        return name2
    if logs and slate_date_ymd:
        slate_date = _parse_ymd(slate_date_ymd)
        for s in logs:
            dt_utc = _parse_dt_utc(s.get("gameDate") or s.get("date"))
            if not dt_utc:
                continue
            if _date_in_eastern(dt_utc) >= slate_date:
                continue
            t = (s.get("team") or {})
            nm = (t.get("name") or "").strip()
            if nm:
                return nm
    if pid is not None and team_map and pid in team_map:
        return (team_map[pid][1] or "").strip() or "N/A"
    return "N/A"

def _current_hitless_streak_before_slate(
    game_splits: List[Dict],
    slate_date_ymd: str,
    exclude_game_pks: Optional[Set[int]] = None
) -> int:
    slate_date = _parse_ymd(slate_date_ymd)
    exclude_game_pks = exclude_game_pks or set()
    streak = 0
    for s in game_splits:
        pk = s.get("game", {}).get("gamePk") or s.get("gamePk")
        try:
            if pk is not None and int(pk) in exclude_game_pks:
                continue
        except Exception:
            pass

        dt_utc = _parse_dt_utc(s.get("gameDate") or s.get("date"))
        if not dt_utc:
            continue
        if _date_in_eastern(dt_utc) >= slate_date:
            continue  # exclude same-day/future

        stat = s.get("stat") or {}
        try:
            ab = int(stat.get("atBats") or 0)
            hits = int(stat.get("hits") or 0)
        except Exception:
            ab, hits = 0, 0
        if ab <= 0:
            continue
        if hits == 0:
            streak += 1
        else:
            break
    return streak

def _average_hitless_streak_before_slate(
    game_splits: List[Dict],
    slate_date_ymd: str,
    exclude_game_pks: Optional[Set[int]] = None
) -> Optional[float]:
    slate_date = _parse_ymd(slate_date_ymd)
    exclude_game_pks = exclude_game_pks or set()

    prior: List[Dict] = []
    for s in game_splits:
        pk = s.get("game", {}).get("gamePk") or s.get("gamePk")
        try:
            if pk is not None and int(pk) in exclude_game_pks:
                continue
        except Exception:
            pass

        dt_utc = _parse_dt_utc(s.get("gameDate") or s.get("date"))
        if not dt_utc:
            continue
        if _date_in_eastern(dt_utc) >= slate_date:
            continue
        stat = s.get("stat") or {}
        try:
            ab = int(stat.get("atBats") or 0)
        except Exception:
            ab = 0
        if ab > 0:
            prior.append(s)
    prior.reverse()  # oldest -> newest

    streaks: List[int] = []
    run = 0
    for s in prior:
        stat = s.get("stat") or {}
        try:
            hits = int(stat.get("hits") or 0)
        except Exception:
            hits = 0
        if hits == 0:
            run += 1
        else:
            if run > 0:
                streaks.append(run)
                run = 0

    if not streaks:
        return None
    return sum(streaks) / len(streaks)

# ----------------- roster collection -----------------
def _team_roster_ids_multi(client: httpx.Client, team_id: int, season: int, dbg: Optional[List[Dict]]) -> List[int]:
    attempts = [
        ("Active", {"rosterType": "Active"}),
        ("active", {"rosterType": "active"}),
        ("40Man", {"rosterType": "40Man"}),
        ("fullSeason", {"rosterType": "fullSeason", "season": season}),
        ("season", {"season": season}),
    ]
    ids: List[int] = []
    for label, params in attempts:
        try:
            data = _fetch_json(client, f"{MLB_BASE}/teams/{team_id}/roster", params=params)
            roster = data.get("roster", []) or []
            got = 0
            for r in roster:
                person = r.get("person") or {}
                pid = person.get("id")
                try:
                    if pid is not None:
                        ids.append(int(pid)); got += 1
                except Exception:
                    continue
            if dbg is not None:
                dbg.append({"team_id": team_id, "roster_source": label, "count": got})
            if got > 0:
                break
        except Exception as e:
            if dbg is not None:
                dbg.append({"team_id": team_id, "roster_source": label, "error": f"{type(e).__name__}: {e}"})
            continue
    return ids

def _hydrate_team_roster_people(client: httpx.Client, team_ids: List[int], season: int, dbg: Optional[List[Dict]]) -> Tuple[Set[int], Dict[int, Tuple[int, str]]]:
    person_ids: Set[int] = set()
    team_map: Dict[int, Tuple[int, str]] = {}
    for i in range(0, len(team_ids), 8):
        sub = team_ids[i:i+8]
        params = {
            "teamIds": ",".join(str(t) for t in sub),
            "sportId": 1,
            "season": season,
            "hydrate": f"roster(person,person.stats(group=hitting,type=season,season={season}))"
        }
        try:
            data = _fetch_json(client, f"{MLB_BASE}/teams", params=params)
            for t in data.get("teams", []) or []:
                tid = t.get("id")
                tname = t.get("name", "")
                roster_container = t.get("roster")
                entries = (roster_container.get("roster", []) if isinstance(roster_container, dict) else roster_container) or []
                for entry in entries:
                    person = entry.get("person") or {}
                    pid = person.get("id")
                    if pid is None:
                        continue
                    try:
                        pid = int(pid)
                    except Exception:
                        continue
                    person_ids.add(pid)
                    team_map[pid] = (int(tid) if tid is not None else None, tname)
        except Exception as e:
            if dbg is not None:
                dbg.append({"teams_hydrate_chunk": sub, "error": f"{type(e).__name__}: {e}"})
    return person_ids, team_map

def _collect_union_player_ids(
    client: httpx.Client,
    team_ids: List[int],
    season: int,
    dbg: Optional[List[Dict]]
) -> Tuple[List[int], Dict[int, Tuple[int, str]]]:
    ids: Set[int] = set()
    team_map: Dict[int, Tuple[int, str]] = {}

    for tid in team_ids:
        got = _team_roster_ids_multi(client, tid, season, dbg)
        for pid in got:
            ids.add(pid)
            team_map.setdefault(pid, (tid, ""))

    hydrate_ids, hydrate_map = _hydrate_team_roster_people(client, team_ids, season, dbg)
    for pid in hydrate_ids:
        ids.add(pid)
        if pid in hydrate_map:
            team_map[pid] = hydrate_map[pid]

    out_ids = sorted(ids)
    if dbg is not None:
        dbg.append({"union_player_ids": len(out_ids)})
    return out_ids, team_map

def _batch_people_with_stats(client: httpx.Client, ids: List[int], season: int, dbg: Optional[List[Dict]]) -> List[Dict]:
    out: List[Dict] = []
    for i in range(0, len(ids), 100):
        sub = ids[i:i+100]
        try:
            params = {
                "personIds": ",".join(str(x) for x in sub),
                "hydrate": f"team,stats(group=hitting,type=season,season={season})"
            }
            data = _fetch_json(client, f"{MLB_BASE}/people", params=params)
            ppl = data.get("people", []) or []
            out.extend(ppl)
            if dbg is not None:
                dbg.append({"people_batch_chunk": len(sub), "returned": len(ppl)})
        except Exception as e:
            if dbg is not None:
                dbg.append({"people_batch_chunk": len(sub), "error": f"{type(e).__name__}: {e}"})
    return out

# ----------------- Statcast enrichment -----------------
def _statcast_window(effective_date: str, lookback_days: int) -> Tuple[str, str]:
    end_dt = _parse_ymd(effective_date)
    start_dt = end_dt - timedelta(days=max(1, lookback_days))
    return (start_dt.isoformat(), end_dt.isoformat())

def _compute_statcast_metrics(df) -> Tuple[Optional[float], Optional[float]]:
    """
    Returns (hh_percent_14d, xba_delta_14d) as floats or None.
    - HH%: % of balls in play with launch_speed >=95
    - xBA delta: mean(estimated_ba_using_speedangle) - actual BA over the rows
    """
    try:
        if df is None or len(df) == 0:
            return None, None
        # Filter to ball events
        # pybaseball statcast frames typically have columns:
        # 'launch_speed', 'events', 'description', 'estimated_ba_using_speedangle', 'bb_type', 'type', etc.
        # We'll treat any row with a non-null 'launch_speed' as a batted ball.
        bbe = df[df["launch_speed"].notna()]
        if len(bbe) == 0:
            hh = None
        else:
            hard = bbe[bbe["launch_speed"] >= 95.0]
            hh = 100.0 * len(hard) / len(bbe)

        # Actual BA on these rows: hits / at-bats
        # A rough proxy: rows with events in ('single','double','triple','home_run') as hits.
        hits = df["events"].isin(["single", "double", "triple", "home_run"]).sum()
        # Approx AB proxy: PA minus walks HBP Sac? Statcast tables may not have AB flag; we approximate by counting
        # rows with type == 'X' (ball in play) or events that are outs.
        # Simpler: use columns 'bb_type' notna as in-play; else fall back to all rows.
        if "type" in df.columns:
            ab_rows = df[df["type"] == "X"]
            ab = max(1, len(ab_rows))  # avoid div/0
        elif "bb_type" in df.columns:
            ab_rows = df[df["bb_type"].notna()]
            ab = max(1, len(ab_rows))
        else:
            ab = max(1, len(df))
        actual_ba = hits / ab if ab > 0 else 0.0

        if "estimated_ba_using_speedangle" in df.columns:
            xba = float(df["estimated_ba_using_speedangle"].fillna(0.0).mean())
        else:
            xba = None

        if xba is None:
            xba_delta = None
        else:
            xba_delta = xba - actual_ba

        return (float(hh) if hh is not None else None,
                float(xba_delta) if xba_delta is not None else None)
    except Exception:
        return None, None

def _get_statcast_recent(pid: int, effective_date: str, lookback_days: int, dbg: Optional[List[Dict]]) -> Dict:
    if not _STATCAST_OK:
        if dbg is not None:
            dbg.append({"statcast": {"pid": pid, "wired": False, "why": "pybaseball missing"}})
        return {"hh_percent_14d": None, "xba_delta_14d": None, "has_signal": False, "wired": False}
    try:
        start, end = _statcast_window(effective_date, lookback_days)
        df = statcast_batter(start_dt=start, end_dt=end, player_id=pid)
        hh, dx = _compute_statcast_metrics(df)
        has = False
        why = []
        if hh is not None and hh >= 40.0:
            has = True; why.append(f"HH% (14d) {hh:.1f}")
        if dx is not None and dx >= 0.030:
            has = True; why.append(f"xBA–BA (14d) +{dx:.3f}")
        if dbg is not None:
            dbg.append({"statcast": {"pid": pid, "wired": True, "rows": 0 if df is None else len(df), "hh_percent_14d": hh, "xba_delta_14d": dx}})
        return {
            "hh_percent_14d": hh,
            "xba_delta_14d": dx,
            "has_signal": has,
            "why": "; ".join(why) if why else "no recent Statcast signal",
            "wired": True
        }
    except Exception as e:
        if dbg is not None:
            dbg.append({"statcast_error": {"pid": pid, "error": f"{type(e).__name__}: {e}"}})
        return {"hh_percent_14d": None, "xba_delta_14d": None, "has_signal": False, "why": "statcast_error", "wired": True}

def _statcast_signal(stat: Dict, min_hh: float, min_delta: float) -> Tuple[bool, str]:
    hh = stat.get("hh_percent_14d")
    dx = stat.get("xba_delta_14d")
    has = False
    reasons = []
    try:
        if hh is not None and float(hh) >= float(min_hh):
            has = True
            reasons.append(f"HH% (14d) {float(hh):.1f}")
    except Exception:
        pass
    try:
        if dx is not None and float(dx) >= float(min_delta):
            has = True
            reasons.append(f"xBA–BA (14d) +{float(dx):.3f}")
    except Exception:
        pass
    return has, "; ".join(reasons) if reasons else "no recent Statcast signal"

# ----------------- context: park & pitcher & platoon -----------------
# Simple park factor (hits) index ~100 = neutral, >100 hitter friendly (illustrative defaults).
_PARK_FACTOR_HITS = {
    # Team name -> index (you can refine these from public tables)
    "Colorado Rockies": 112, "Boston Red Sox": 106, "Los Angeles Dodgers": 104,
    "New York Yankees": 103, "Cincinnati Reds": 103, "Philadelphia Phillies": 102,
    "Texas Rangers": 102, "Chicago Cubs": 101, "Atlanta Braves": 101,
    # Neutral baseline fallback
}

def _team_name_from_id(client: httpx.Client, tid: int, season: int, dbg: Optional[List[Dict]]) -> Optional[str]:
    data = _fetch_json_safe(client, f"{MLB_BASE}/teams/{tid}", {"season": season}, dbg, f"team_name:{tid}")
    t = (data.get("teams") or [{}])[0]
    return t.get("name")

def _park_factor_for_matchup(home_name: Optional[str]) -> float:
    if not home_name:
        return 100.0
    return float(_PARK_FACTOR_HITS.get(home_name, 100))

def _probable_pitcher_info(client: httpx.Client, gamePk: int, dbg: Optional[List[Dict]]) -> Dict:
    game = _fetch_json_safe(client, f"{MLB_BASE}/game/{gamePk}/feed/live", None, dbg, f"live:{gamePk}")
    allp = (((game.get("gameData") or {}).get("probablePitchers")) or {})
    # structure: {"home": {...}, "away": {...}}
    out = {}
    for side in ("home", "away"):
        p = allp.get(side) or {}
        out[side] = {
            "id": p.get("id"),
            "fullName": p.get("fullName"),
            "pitchHand": ((p.get("pitchHand") or {}).get("code") or "").upper(),  # R/L
        }
    # Basic ERA pull from live box if available
    try:
        box = (game.get("liveData") or {}).get("boxscore") or {}
        for side in ("home", "away"):
            pmap = ((box.get("teams") or {}).get(side) or {}).get("players") or {}
            pid = out[side].get("id")
            if pid is None: 
                continue
            key = f"ID{pid}"
            if key in pmap:
                stats = ((pmap[key] or {}).get("seasonStats") or {}).get("pitching") or {}
                era = stats.get("era")
                try:
                    out[side]["era"] = float(era) if era is not None else None
                except Exception:
                    out[side]["era"] = None
    except Exception:
        pass
    return out

def _platoon_bonus(h_bats: Optional[str], p_hand: Optional[str]) -> float:
    # +1.0 if advantage, 0 neutral (just a scaler we'll later map to 0..100)
    if not h_bats or not p_hand:
        return 0.0
    hb = h_bats.upper()
    ph = p_hand.upper()
    # Simplistic: R vs L or L vs R is advantage
    if (hb == "R" and ph == "L") or (hb == "L" and ph == "R"):
        return 1.0
    return 0.0

# ----------------- bookmaker/composite -----------------
def _decorate_candidate_with_base_scores(base: Dict, person_like: Dict) -> None:
    season_avg = float(base.get("season_avg", 0.0))
    expected_abs = _expected_abs_from_person(person_like)  # 2..5.5 typical
    break_prob = _break_prob_from_avg_and_ab(season_avg, expected_abs)  # 0..1

    current = int(base.get("hitless_streak", 0))
    avg_streak = base.get("avg_hitless_streak_season", None)
    try:
        avg_streak_f = float(avg_streak) if avg_streak is not None else 1.0
    except Exception:
        avg_streak_f = 1.0
    denom = max(0.5, avg_streak_f)
    pressure = (float(current) / denom) if denom > 0 else float(current)

    score = break_prob * pressure

    base["expected_abs"] = round(expected_abs, 2)
    base["break_prob_next"] = round(break_prob * 100.0, 1)  # percent
    base["pressure"] = round(pressure, 3)
    base["score"] = round(score * 100.0, 1)
    base["hit_chance_pct"] = base["break_prob_next"]
    base["overdue_ratio"] = base["pressure"]
    base["ranking_score"] = base["score"]

def _scale_0_100(val: float, lo: float, hi: float) -> float:
    if val <= lo: return 0.0
    if val >= hi: return 100.0
    return (val - lo) / (hi - lo) * 100.0

def _compose_composite(
    cand: Dict,
    stat: Dict,
    context: Dict,
    w_hit_chance: float,
    w_overdue: float,
    w_elite_avg: float,
    w_statcast: float,
    w_pitcher: float,
    w_platoon: float,
    w_park: float
) -> float:
    # Base signals
    hit_chance = float(cand["hit_chance_pct"])  # already 0..100
    overdue = float(cand["overdue_ratio"])      # ~0..n; scale to 0..100 with 3.0≈100
    overdue_scaled = max(0.0, min(100.0, overdue * 33.33))

    avg = float(cand["season_avg"])
    # Elite AVG bump (0 @ .260, 100 @ .340+)
    elite_scaled = _scale_0_100(avg, 0.260, 0.340)

    # Statcast
    hh = stat.get("hh_percent_14d")
    dx = stat.get("xba_delta_14d")
    stat_scaled_parts = []
    if isinstance(hh, (int, float)):
        stat_scaled_parts.append(_scale_0_100(hh, 30.0, 50.0))   # 30→0, 50→100
    if isinstance(dx, (int, float)):
        stat_scaled_parts.append(_scale_0_100(dx, 0.00, 0.08))  # 0.00→0, 0.08→100
    stat_scaled = statistics.mean(stat_scaled_parts) if stat_scaled_parts else 0.0

    # Context: pitcher softness (ERA), platoon, park
    era = context.get("opp_sp_era")
    pitcher_soft_scaled = _scale_0_100(era, 3.0, 6.0) if isinstance(era, (int, float)) else 0.0
    platoon_scaled = 100.0 if context.get("platoon_advantage") else 0.0
    park_idx = context.get("park_index_hits", 100.0)
    park_scaled = _scale_0_100(park_idx, 95.0, 110.0)  # 95→0, 110→100

    composite = (
        w_hit_chance * (hit_chance / 100.0) +
        w_overdue    * (overdue_scaled / 100.0) +
        w_elite_avg  * (elite_scaled / 100.0) +
        w_statcast   * (stat_scaled / 100.0) +
        w_pitcher    * (pitcher_soft_scaled / 100.0) +
        w_platoon    * (platoon_scaled / 100.0) +
        w_park       * (park_scaled / 100.0)
    )
    return composite

# ----------------- sorting helpers -----------------
_VALID_SORT_KEYS = {
    "hitless_streak", "season_avg", "avg_hitless_streak_season", "break_prob_next",
    "pressure", "score", "hit_chance_pct", "overdue_ratio",
    "ranking_score", "score_plus", "composite"
}

def _parse_sort_by(sort_by: Optional[str]) -> List[Tuple[str, bool]]:
    default = [("hitless_streak", True), ("season_avg", True), ("avg_hitless_streak_season", True)]
    if not sort_by:
        return default
    out: List[Tuple[str, bool]] = []
    for raw in sort_by.split(","):
        k = raw.strip()
        if not k:
            continue
        desc = k.startswith("-")
        field = k[1:] if desc else k
        if field in _VALID_SORT_KEYS:
            out.append((field, desc))
    return out or default

def _apply_sort(candidates: List[Dict], sort_spec: List[Tuple[str, bool]]) -> List[Dict]:
    def key_fn(item: Dict):
        keys = []
        for field, desc in sort_spec:
            v = item.get(field, 0)
            try:
                v = float(v)
            except Exception:
                v = 0.0
            keys.append(-v if desc else v)
        return tuple(keys)
    return sorted(candidates, key=key_fn)

# ----------------- main route -----------------
@router.get("/cold_candidates")
def cold_candidates(
    date: str = Query("today", description="YYYY-MM-DD or 'today' (US/Eastern)"),
    season: int = Query(2025, ge=1900, le=2100),
    names: Optional[str] = Query(None, description="Optional comma-separated player names. If omitted, scans slate rosters."),
    min_season_avg: float = Query(0.26, ge=0.0, le=1.0, description="Only include hitters with season AVG ≥ this."),
    min_hitless_games: int = Query(1, ge=1, description="Current hitless streak (AB>0) must be ≥ this."),
    min_season_ab: int = Query(100, ge=0, description="Minimum season AB to qualify."),
    min_season_gp: int = Query(40, ge=0, description="Minimum season games played to qualify."),
    limit: int = Query(30, ge=1, le=1000),
    verify: int = Query(1, ge=0, le=1, description="1 = STRICT pregame only (teams not started yet). 0 = include all teams."),
    roll_to_next_slate_if_empty: int = Query(1, ge=0, le=1, description="If verify=1 and there are ZERO pregame teams today, roll to NEXT day."),
    scan_multiplier: int = Query(8, ge=1, le=40, description="How many logs to check: limit × scan_multiplier (cap applies)."),
    max_log_checks: Optional[int] = Query(None, ge=1, le=5000, description="Hard cap for log checks; overrides scan_multiplier."),
    debug: int = Query(0, ge=0, le=1),

    # aliases / presentation
    mode: Optional[str] = Query(None, description="Alias for verify. 'pregame' -> verify=1, 'all' -> verify=0."),
    as_of: Optional[str] = Query(None, description="YYYY-MM-DD snapshot. If not today ET, verification disabled."),
    group_by: str = Query("streak", description="Grouping preset: 'streak' (default) or 'none'."),
    sort_by: Optional[str] = Query(None, description="When group_by='none', comma-separated fields with optional '-' for DESC."),

    # Statcast knobs
    require_statcast_for_tiers: int = Query(1, ge=0, le=1, description="If 1, Tier S/A only shown when Statcast signal exists."),
    hh_recent_days: int = Query(14, ge=7, le=28, description="Statcast lookback window in days."),
    statcast_min_hh_14d: float = Query(40.0, description="HH%% (14d) threshold to treat as positive Statcast."),
    statcast_min_xba_delta_14d: float = Query(0.03, description="xBA–BA (14d) threshold to treat as positive Statcast."),

    # composite weights (retain your spirit; add context weights)
    w_hit_chance: float = Query(45.0),
    w_overdue: float = Query(17.5),
    w_elite_avg: float = Query(12.5),
    w_statcast: float = Query(15.0),
    w_pitcher: float = Query(6.0),
    w_platoon: float = Query(2.0),
    w_park: float = Query(2.0),

    # tier thresholds (unchanged from your stricter settings)
    tier_s_min_composite: float = Query(70.0),
    tier_s_min_hit_chance: float = Query(67.0),
    tier_s_min_overdue: float = Query(2.0),

    tier_a_min_composite: float = Query(55.0),
    tier_a_min_hit_chance: float = Query(62.0),
):
    """
    VERIFIED cold-hitter candidates with real Statcast and context (pitcher/park/platoon).
    """
    requested_date = _eastern_today_str() if _normalize(date) == "today" else date
    effective_date = requested_date

    # alias mapping
    mode_norm = (mode or "").strip().lower() if mode else None
    if mode_norm in ("pregame", "pre", "strict"):
        verify_effective = 1
    elif mode_norm in ("all", "any"):
        verify_effective = 0
    else:
        verify_effective = 1 if int(verify) == 1 else 0

    # as_of snapshot handling
    as_of_norm = (as_of or "").strip()
    historical_mode = False
    if as_of_norm:
        try:
            _ = _parse_ymd(as_of_norm)
            effective_date = as_of_norm
            if not _is_today_et(as_of_norm):
                historical_mode = True
        except Exception:
            pass

    if historical_mode:
        verify_effective = 0
        roll_enabled = False
    else:
        roll_enabled = bool(roll_to_next_slate_if_empty)

    # scan budget
    DEFAULT_MULT = max(1, int(scan_multiplier))
    computed_max = min(3000, limit * DEFAULT_MULT)
    MAX_LOG_CHECKS = max_log_checks if max_log_checks is not None else computed_max

    if verify_effective == 0:
        if max_log_checks is None:
            MAX_LOG_CHECKS = min(MAX_LOG_CHECKS, 400)
        if "limit" in cold_candidates.__signature__.parameters:
            if limit > 30:
                limit = 30

    group_mode = (group_by or "").strip().lower()
    sort_spec = _parse_sort_by(sort_by) if group_mode == "none" else []

    with httpx.Client(timeout=45) as client:
        debug_list: Optional[List[Dict]] = [] if debug else None

        # schedule for date
        sched = _schedule_for_date(client, effective_date, debug_list)
        ns_team_ids_today = _not_started_team_ids_for_date(sched) if (verify_effective == 1) else set()
        slate_team_ids_today = _team_ids_from_schedule(sched) if sched else _all_mlb_team_ids(client, season, debug_list)
        exclude_pks_for_date = _game_pks_for_date(sched) if sched else set()
        sched_rows = _schedule_rows(sched)

        # build map: gamePk -> probable pitchers, home name
        game_meta: Dict[int, Dict[str, Any]] = {}
        for d in sched.get("dates", []) or []:
            for g in d.get("games", []) or []:
                pk = g.get("gamePk")
                try:
                    pk = int(pk)
                except Exception:
                    continue
                home_team_name = (((g.get("teams") or {}).get("home") or {}).get("team") or {}).get("name")
                try:
                    pp = _probable_pitcher_info(client, pk, debug_list)
                except Exception:
                    pp = {}
                game_meta[pk] = {
                    "home_name": home_team_name,
                    "probable": pp
                }

        rolled = False
        if (verify_effective == 1) and roll_enabled and len(ns_team_ids_today) == 0:
            effective_date = _next_ymd_str(effective_date)
            sched = _schedule_for_date(client, effective_date, debug_list)
            ns_team_ids_today = _not_started_team_ids_for_date(sched)
            slate_team_ids_today = _team_ids_from_schedule(sched) or slate_team_ids_today
            exclude_pks_for_date = _game_pks_for_date(sched)
            sched_rows = _schedule_rows(sched)
            # refresh game meta
            game_meta = {}
            for d in sched.get("dates", []) or []:
                for g in d.get("games", []) or []:
                    pk = g.get("gamePk")
                    try:
                        pk = int(pk)
                    except Exception:
                        continue
                    home_team_name = (((g.get("teams") or {}).get("home") or {}).get("team") or {}).get("name")
                    try:
                        pp = _probable_pitcher_info(client, pk, debug_list)
                    except Exception:
                        pp = {}
                    game_meta[pk] = {
                        "home_name": home_team_name,
                        "probable": pp
                    }
            rolled = True

        # gather people
        candidates: List[Dict] = []

        def _qualify_by_ab_gp(person_like: Dict) -> bool:
            ab, gp = _season_ab_gp_from_people_like(person_like)
            if ab is None or gp is None:
                return False
            return (ab >= min_season_ab) and (gp >= min_season_gp)

        def _decor_context(pid: int, person: Dict, target_date: str, logs: List[Dict]) -> Dict[str, Any]:
            """
            Determine opponent SP ERA (best effort), platoon, park index.
            """
            bats_code = (((person.get("batSide") or {}).get("code")) or ((person.get("batSide") or {}).get("batSideCode")) or (person.get("batSideCode"))) or None
            # pick the most recent prior game (before slate) to infer the scheduled gamePk; if none, context will be best effort neutral
            opp_sp_era = None
            platoon_adv = False
            park_idx = 100.0

            # Attempt to find today's game pk by scanning schedule team ids mapping
            # If logs include the game after slate (none), fallback to probable pitchers by team matchup
            # Best-effort: iterate schedule rows and find a game where player's current team matches and status is pregame.
            # To know player's team id:
            team_info = person.get("currentTeam") or {}
            tid = team_info.get("id")
            # Find any game with this team id
            target_gpk = None
            for d in sched.get("dates", []) or []:
                for g in d.get("games", []) or []:
                    try:
                        home_id = int(((g.get("teams") or {}).get("home") or {}).get("team", {}).get("id"))
                        away_id = int(((g.get("teams") or {}).get("away") or {}).get("team", {}).get("id"))
                        if tid is not None and int(tid) in (home_id, away_id):
                            # Check status pregame-like
                            code = (g.get("status", {}) or {}).get("statusCode", "")
                            if code in ("P", "S", "PW"):
                                target_gpk = int(g.get("gamePk"))
                                home_name = (((g.get("teams") or {}).get("home") or {}).get("team") or {}).get("name")
                                park_idx = _park_factor_for_matchup(home_name)
                                # determine opposing pitcher hand/era
                                pp = game_meta.get(target_gpk, {}).get("probable") or {}
                                # if player's team is away, opposing is home probable; vice versa
                                opp_side = "home" if int(tid) == away_id else "away"
                                opp = pp.get(opp_side) or {}
                                p_hand = opp.get("pitchHand")  # 'R' or 'L'
                                era = opp.get("era")
                                opp_sp_era = era if isinstance(era, (int, float)) else None
                                # platoon
                                platoon_adv = bool(_platoon_bonus(bats_code, p_hand))
                                raise StopIteration
                    except Exception:
                        continue
            # If none found, leave neutrals (will not sink scoring)
            return {
                "opp_sp_era": opp_sp_era,                # None allowed
                "platoon_advantage": platoon_adv,        # bool
                "park_index_hits": park_idx              # float index
            }

        def _decorate_and_add(person: Dict, pid: int, target_date: str, team_map: Optional[Dict[int, Tuple[int, str]]] = None):
            logs = _game_log_regular_season_desc(client, pid, season, max_entries=160, dbg=debug_list)
            streak = _current_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)
            if streak < min_hitless_games:
                return
            avg_season_hitless = _average_hitless_streak_before_slate(logs, target_date, exclude_pks_for_date)
            team_name = _extract_team_name_from_person_or_logs(person, team_map, pid, logs, target_date)
            season_avg = _season_avg_from_people_like(person)
            if season_avg is None or season_avg < min_season_avg:
                return

            cand = {
                "name": person.get("fullName") or "",
                "team": team_name,
                "season_avg": round(float(season_avg), 3),
                "hitless_streak": int(streak),
                "avg_hitless_streak_season": round(avg_season_hitless, 2) if avg_season_hitless is not None else 0.0,
            }
            _decorate_candidate_with_base_scores(cand, person)

            # Statcast overlay fetch + signal
            stat = _get_statcast_recent(pid, target_date, hh_recent_days, debug_list)
            has_sig, why = _statcast_signal(stat, statcast_min_hh_14d, statcast_min_xba_delta_14d)

            # Context (pitcher/park/platoon)
            ctx = _decor_context(pid, person, target_date, logs)

            # Composite with context
            composite = _compose_composite(
                cand, stat, ctx,
                w_hit_chance, w_overdue, w_elite_avg, w_statcast,
                w_pitcher, w_platoon, w_park
            )

            cand["score_plus"] = round(float(cand["score"]), 1)  # placeholder = score; keep until you wire markets
            cand["composite"] = round(composite, 1)
            cand["_statcast"] = {
                "has_signal": has_sig,
                "why": why,
                "hh_percent_14d": stat.get("hh_percent_14d"),
                "xba_delta_14d": stat.get("xba_delta_14d"),
                "wired": stat.get("wired", False)
            }
            # Add lightweight context echoes (helps debugging & ranking interpretability)
            cand["_context"] = {
                "opp_sp_era": ctx.get("opp_sp_era"),
                "platoon_adv": ctx.get("platoon_advantage"),
                "park_idx_hits": ctx.get("park_index_hits")
            }
            candidates.append(cand)

        # league mode or explicit names
        if names:
            requested = [n.strip() for n in names.split(",") if n.strip()]
            for name in requested:
                try:
                    data = _fetch_json(client, f"{MLB_BASE}/people/search", params={"names": name})
                    people = data.get("people", []) or []
                    if not people:
                        if debug_list is not None:
                            debug_list.append({"name": name, "skip": "player not found"})
                        continue
                    norm_target = _normalize(name)
                    p0 = next((p for p in people if _normalize(p.get("fullName","")) == norm_target), people[0])
                    pid = int(p0["id"])
                    pdata = _fetch_json(client, f"{MLB_BASE}/people/{pid}", params={"hydrate": f"team,stats(group=hitting,type=season,season={season})"})
                    person = (pdata.get("people") or [{}])[0]
                    if not _qualify_by_ab_gp(person):
                        continue
                    if verify_effective == 1:
                        team_info = person.get("currentTeam") or {}
                        try:
                            team_id = int(team_info.get("id")) if team_info.get("id") is not None else None
                        except Exception:
                            team_id = None
                        if team_id is None or team_id not in ns_team_ids_today:
                            continue
                    _decorate_and_add(person, pid, effective_date)
                    if len(candidates) >= limit:
                        break
                except Exception as e:
                    if debug_list is not None:
                        debug_list.append({"name": name, "error": f"{type(e).__name__}: {e}"})
        else:
            # league scan
            scan_team_ids = sorted(ns_team_ids_today) if verify_effective == 1 else slate_team_ids_today
            union_ids, team_map = _collect_union_player_ids(client, scan_team_ids, season, debug_list)
            people = _batch_people_with_stats(client, union_ids, season, debug_list)

            # normalize team
            for p in people:
                if not p.get("currentTeam"):
                    pid = p.get("id")
                    if isinstance(pid, int) and pid in team_map:
                        tid, tname = team_map[pid]
                        p["currentTeam"] = {"id": tid, "name": tname}

            prospects: List[Tuple[float, Dict]] = []
            for p in people:
                season_avg = _season_avg_from_people_like(p)
                if season_avg is None or season_avg < min_season_avg:
                    continue
                ab, gp = _season_ab_gp_from_people_like(p)
                if ab is None or gp is None or ab < min_season_ab or gp < min_season_gp:
                    continue
                try:
                    pid = int(p.get("id"))
                except Exception:
                    pid = None
                if pid is None:
                    continue
                if verify_effective == 1:
                    team_info = p.get("currentTeam") or {}
                    try:
                        team_id = int(team_info.get("id")) if team_info.get("id") is not None else None
                    except Exception:
                        team_id = None
                    if team_id is None or team_id not in ns_team_ids_today:
                        continue
                prospects.append((float(season_avg), {"pid": pid, "person": p}))

            prospects.sort(key=lambda x: x[0], reverse=True)

            checks = 0
            for _, meta in prospects:
                if checks >= MAX_LOG_CHECKS or len(candidates) >= limit:
                    break
                checks += 1
                try:
                    _decorate_and_add(meta["person"], meta["pid"], effective_date, team_map)
                except Exception as e:
                    if debug_list is not None:
                        dbg_name = (meta["person"] or {}).get("fullName","")
                        debug_list.append({"name": dbg_name, "error": f"{type(e).__name__}: {e}"})

        # group/sort
        if group_mode == "streak":
            buckets: Dict[int, List[Dict]] = {}
            for c in candidates:
                buckets.setdefault(int(c.get("hitless_streak", 0)), []).append(c)
            out_list: List[Dict] = []
            for k in sorted(buckets.keys(), reverse=True):
                grp = sorted(buckets[k], key=lambda x: float(x.get("ranking_score", x.get("score", 0.0))), reverse=True)
                out_list.extend(grp)
            candidates = out_list[:limit]
        else:
            if sort_spec:
                candidates = _apply_sort(candidates, sort_spec)
            else:
                candidates = sorted(candidates, key=lambda x: (float(x.get("ranking_score", x.get("score", 0.0))), float(x.get("season_avg", 0.0))), reverse=True)
            candidates = candidates[:limit]

        # Build Tier S / A — Statcast gate enforced if require_statcast_for_tiers=1
        best_targets_s: List[Dict] = []
        best_targets_a: List[Dict] = []
        for c in candidates:
            hit_ch = float(c.get("hit_chance_pct", 0.0))
            overdue = float(c.get("overdue_ratio", 0.0))
            comp = float(c.get("composite", 0.0))
            sc = c.get("_statcast", {}) or {}
            has_sig = bool(sc.get("has_signal"))

            # enforce gate
            if require_statcast_for_tiers == 1 and not has_sig:
                continue

            if comp >= tier_s_min_composite and (hit_ch >= tier_s_min_hit_chance or overdue >= tier_s_min_overdue):
                best_targets_s.append(c)
            elif comp >= tier_a_min_composite and hit_ch >= tier_a_min_hit_chance:
                best_targets_a.append(c)

        response: Dict[str, Any] = {"date": effective_date, "candidates": candidates}
        response["schedule"] = [{"matchup": row[0], "statusCode": row[1], "statusText": row[3], "gamePk": row[2]} for row in sched_rows]
        response["pregame_counts"] = {
            "pregame_teams": len(ns_team_ids_today),
            "slate_teams": len(slate_team_ids_today),
        }
        response["best_targets"] = {
            "tier_s": best_targets_s,
            "tier_a": best_targets_a,
        }

        if debug_list is not None:
            stamp = {
                "requested_date": requested_date,
                "effective_date": effective_date,
                "verify": int(verify_effective),
                "rolled_to_next_slate": bool(rolled),
                "pregame_team_count": len(ns_team_ids_today),
                "slate_team_count": len(slate_team_ids_today),
                "cutoffs": {
                    "min_season_avg": min_season_avg,
                    "min_hitless_games": min_hitless_games,
                    "min_season_ab": min_season_ab,
                    "min_season_gp": min_season_gp,
                    "limit": limit,
                    "scan_multiplier": DEFAULT_MULT,
                    "max_log_checks": MAX_LOG_CHECKS,
                },
                "statcast": {
                    "wired": _STATCAST_OK,
                    "require_statcast_for_tiers": require_statcast_for_tiers,
                    "hh_recent_days": hh_recent_days,
                    "statcast_min_hh_14d": statcast_min_hh_14d,
                    "statcast_min_xba_delta_14d": statcast_min_xba_delta_14d,
                },
                "weights": {
                    "w_hit_chance": w_hit_chance,
                    "w_overdue": w_overdue,
                    "w_elite_avg": w_elite_avg,
                    "w_statcast": w_statcast,
                    "w_pitcher": w_pitcher,
                    "w_platoon": w_platoon,
                    "w_park": w_park,
                },
                "params": {
                    "mode": mode_norm or None,
                    "as_of": as_of_norm or None,
                    "group_by": group_mode,
                    "sort_by": sort_by or None,
                }
            }
            response["debug"] = [stamp] + (debug_list or [])
        return response
