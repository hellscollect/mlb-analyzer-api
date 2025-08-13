# providers/statsapi_provider.py
from __future__ import annotations
import os
from datetime import date as _date
from typing import Dict, List, Any, Iterable, Optional, Tuple
import requests
from models import Hitter, Pitcher

# ----- tiny helpers -----
def _safe_float(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None

def _to_dict(x: Any) -> Dict[str, Any]:
    if hasattr(x, "model_dump"):  # pydantic v2
        return x.model_dump()
    if hasattr(x, "dict"):        # pydantic v1
        return x.dict()
    return dict(x)

def _team_code(team_obj: Dict[str, Any]) -> str:
    return (
        team_obj.get("abbreviation")
        or team_obj.get("teamCode")
        or team_obj.get("teamName")
        or team_obj.get("name")
        or ""
    )

class StatsApiProvider:
    """
    Provider backed by MLB's public StatsAPI (no auth).
    Env (optional):
      STATSAPI_BASE = https://statsapi.mlb.com/api/v1    (default)
      STATSAPI_SEASON = 2025                             (defaults to date.year)
      STATSAPI_HITTERS_PER_TEAM = 3                      (# of position players sampled per team)
      STATSAPI_TIMEOUT = 10
      STATSAPI_GAME_TYPE = R                             (regular season)
    """

    def __init__(self):
        self.base = (os.getenv("STATSAPI_BASE") or "https://statsapi.mlb.com/api/v1").rstrip("/")
        self.season_override = os.getenv("STATSAPI_SEASON")
        self.hitters_per_team = int(os.getenv("STATSAPI_HITTERS_PER_TEAM", "3"))
        self.timeout = float(os.getenv("STATSAPI_TIMEOUT", "10"))
        self.game_type = os.getenv("STATSAPI_GAME_TYPE", "R")
        self.key = ""  # for /provider_raw debug parity

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "mlb-analyzer/1.0"})

    # ---------- Public methods used by main.py ----------
    def hot_streak_hitters(self, date: _date, min_avg: float = 0.280, games: int = 3,
                           require_hit_each: bool = True, debug: bool = False):
        hitters = self.get_hitters(date)
        out: List[Dict[str, Any]] = []
        for h in hitters:
            if (h.avg or 0.0) < min_avg:
                continue
            seq = list(h.last_n_hits_each_game or [])
            if len(seq) < games:
                continue
            if require_hit_each and not all((x or 0) >= 1 for x in seq[:games]):
                continue
            out.append(_to_dict(h))
        return {"items": out, "meta": {"count": len(out), "min_avg": min_avg, "games": games, "require_hit_each": require_hit_each}} if debug else out

    def cold_streak_hitters(self, date: _date, min_avg: float = 0.275, games: int = 2,
                            require_zero_hit_each: bool = True, debug: bool = False):
        hitters = self.get_hitters(date)
        out: List[Dict[str, Any]] = []
        for h in hitters:
            if (h.avg or 0.0) < min_avg:
                continue
            seq = list(h.last_n_hits_each_game or [])
            if len(seq) < games:
                continue
            if require_zero_hit_each and not all((x or 0) == 0 for x in seq[:games]):
                continue
            if require_zero_hit_each and (h.last_n_hitless_games or 0) < games:
                continue
            out.append(_to_dict(h))
        return {"items": out, "meta": {"count": len(out), "min_avg": min_avg, "games": games, "require_zero_hit_each": require_zero_hit_each}} if debug else out

    def pitcher_streaks(self, date: _date, hot_max_era: float = 4.00, hot_min_ks_each: int = 6, hot_last_starts: int = 3,
                        cold_min_era: float = 4.60, cold_min_runs_each: int = 3, cold_last_starts: int = 2,
                        debug: bool = False):
        pitchers = self.get_pitchers(date)
        hot: List[Dict[str, Any]] = []
        cold: List[Dict[str, Any]] = []
        for p in pitchers:
            ks = list(p.k_per_start_last_n or [])
            ra = list(p.runs_allowed_last_n or [])
            if (p.era or 99.9) <= hot_max_era and len(ks) >= hot_last_starts and all((k or 0) >= hot_min_ks_each for k in ks[:hot_last_starts]):
                hot.append(_to_dict(p))
            if (p.era or 0.0) >= cold_min_era and len(ra) >= cold_last_starts and all((r or 0) >= cold_min_runs_each for r in ra[:cold_last_starts]):
                cold.append(_to_dict(p))
        resp = {"hot_pitchers": hot, "cold_pitchers": cold}
        if debug:
            resp["meta"] = {"counts": {"hot": len(hot), "cold": len(cold)}}
        return resp

    def cold_pitchers(self, date: _date, min_era: float = 4.60, min_runs_each: int = 3, last_starts: int = 2, debug: bool = False):
        pitchers = self.get_pitchers(date)
        out: List[Dict[str, Any]] = []
        for p in pitchers:
            ra = list(p.runs_allowed_last_n or [])
            if (p.era or 0.0) >= min_era and len(ra) >= last_starts and all((r or 0) >= min_runs_each for r in ra[:last_starts]):
                out.append(_to_dict(p))
        return {"items": out, "meta": {"count": len(out), "min_era": min_era, "min_runs_each": min_runs_each, "last_starts": last_starts}} if debug else out

    def slate_scan(self, date: _date, debug: bool = False):
        hot_hitters = self.hot_streak_hitters(date, debug=False)
        cold_hitters = self.cold_streak_hitters(date, debug=False)
        streaks = self.pitcher_streaks(date, debug=False)
        hot_pitchers = streaks.get("hot_pitchers", [])
        cold_pitchers = streaks.get("cold_pitchers", [])
        pid_index = {p["player_id"]: p for p in (hot_pitchers + cold_pitchers)}
        matchups: List[Dict[str, Any]] = []
        for h in (hot_hitters if isinstance(hot_hitters, list) else hot_hitters.get("items", [])):
            pid = h.get("probable_pitcher_id")
            if pid and pid in pid_index:
                p = pid_index[pid]
                matchups.append({
                    "hitter_id": h["player_id"],
                    "hitter_name": h["name"],
                    "hitter_team": h["team"],
                    "pitcher_id": p["player_id"],
                    "pitcher_name": p["name"],
                    "pitcher_team": p["team"],
                    "opponent_team": h.get("opponent_team"),
                    "note": "Hot hitter vs probable pitcher",
                })
        out = {
            "hot_hitters": hot_hitters if isinstance(hot_hitters, list) else hot_hitters.get("items", []),
            "cold_hitters": cold_hitters if isinstance(cold_hitters, list) else cold_hitters.get("items", []),
            "hot_pitchers": hot_pitchers,
            "cold_pitchers": cold_pitchers,
            "matchups": matchups,
        }
        if debug:
            out["debug"] = {"counts": {k: len(out[k]) for k in out}}
        return out

    # ---------- Internal helpers ----------
    def _season_of(self, d: _date) -> int:
        if self.season_override and self.season_override.isdigit():
            return int(self.season_override)
        return d.year

    def _get(self, path: str, params: Dict[str, Any]) -> Any:
        url = f"{self.base}{path}"
        try:
            r = self._session.get(url, params=params, timeout=self.timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"[statsapi] GET {url} params={params} -> {type(e).__name__}: {e}")
            return {}

    # ---------- Raw fetches (StatsAPI) ----------
    def _fetch_schedule(self, game_date: _date) -> Dict[str, Any]:
        return self._get("/schedule", {"sportId": 1, "date": game_date.isoformat(), "hydrate": "probablePitcher"})

    def _person_stats(self, pid: int, group: str, season: int) -> Dict[str, Any]:
        """
        Use the dedicated /people/{id}/stats endpoint to avoid hydrate quirks.
        Example:
          /people/545361/stats?stats=gameLog,season&group=hitting&season=2025&gameType=R
        """
        params = {
            "stats": "gameLog,season",
            "group": group,
            "season": season,
            "gameType": self.game_type,
        }
        return self._get(f"/people/{pid}/stats", params)

    def _fetch_pitcher_rows(self, game_date: _date, limit: Optional[int] = None, team: Optional[str] = None) -> Iterable[Dict[str, Any]]:
        season = self._season_of(game_date)
        sched = self._fetch_schedule(game_date)
        entries: List[Tuple[int, str, str]] = []
        for d in sched.get("dates", []):
            for g in d.get("games", []):
                away = g.get("teams", {}).get("away", {})
                home = g.get("teams", {}).get("home", {})
                away_code = _team_code(away.get("team", {}) or {})
                home_code = _team_code(home.get("team", {}) or {})
                ap = (away.get("probablePitcher") or {}).get("id")
                hp = (home.get("probablePitcher") or {}).get("id")
                if ap and (not team or team == away_code):
                    entries.append((int(ap), away_code, home_code))
                if hp and (not team or team == home_code):
                    entries.append((int(hp), home_code, away_code))
        if limit:
            entries = entries[:limit]

        rows: List[Dict[str, Any]] = []
        for pid, tcode, opp in entries:
            pdata = self._person_stats(pid, "pitching", season)
            # parse name
            name = str(pid)
            # Some responses include people array with person name in "note", but with /stats it’s nested differently.
            # Fallback: we can fetch /people/{pid} if name missing, but usually splits include player name.
            try:
                name = pdata.get("people", [{}])[0].get("fullName") or name
            except Exception:
                pass

            era = None
            ks_seq: List[int] = []
            ra_seq: List[int] = []
            for block in pdata.get("stats", []):
                stype = (block.get("type") or {}).get("displayName", "").lower()
                group = (block.get("group") or {}).get("displayName", "").lower()
                if group != "pitching":
                    continue
                splits = block.get("splits") or []
                if stype == "season":
                    if splits:
                        era = _safe_float((splits[0].get("stat") or {}).get("era"))
                elif stype == "gamelog":
                    for sp in splits[:5]:
                        stat = sp.get("stat", {})
                        ks_seq.append(int(stat.get("strikeOuts", 0)))
                        ra_seq.append(int(stat.get("earnedRuns", 0)))

            rows.append({
                "player_id": str(pid),
                "name": name,
                "team": tcode,
                "opponent_team": opp,
                "era": era if era is not None else 0.0,
                "kbb": None,
                "k_per_start_last_n": ks_seq,
                "runs_allowed_last_n": ra_seq,
                "is_probable": True,
            })
        return rows

    def _fetch_hitter_rows(self, game_date: _date, limit: Optional[int] = None, team: Optional[str] = None) -> Iterable[Dict[str, Any]]:
        season = self._season_of(game_date)
        sched = self._fetch_schedule(game_date)

        # Map team -> opponent probable pitcher id
        opp_prob_by_team: Dict[str, int] = {}
        for d in sched.get("dates", []):
            for g in d.get("games", []):
                away = g.get("teams", {}).get("away", {})
                home = g.get("teams", {}).get("home", {})
                away_code = _team_code(away.get("team", {}) or {})
                home_code = _team_code(home.get("team", {}) or {})
                ap = (away.get("probablePitcher") or {}).get("id")
                hp = (home.get("probablePitcher") or {}).get("id")
                if away_code and hp:
                    opp_prob_by_team[away_code] = int(hp)
                if home_code and ap:
                    opp_prob_by_team[home_code] = int(ap)

        # Build team list on slate
        teams: List[Tuple[int, str, str]] = []
        for d in sched.get("dates", []):
            for g in d.get("games", []):
                away = g.get("teams", {}).get("away", {})
                home = g.get("teams", {}).get("home", {})
                away_id = (away.get("team") or {}).get("id")
                home_id = (home.get("team") or {}).get("id")
                away_code = _team_code(away.get("team", {}) or {})
                home_code = _team_code(home.get("team", {}) or {})
                if away_id and (not team or team == away_code):
                    teams.append((int(away_id), away_code, home_code))
                if home_id and (not team or team == home_code):
                    teams.append((int(home_id), home_code, away_code))

        rows: List[Dict[str, Any]] = []
        for tid, tcode, opp_code in teams:
            roster = self._get(f"/teams/{tid}/roster", {"rosterType": "active", "season": season}).get("roster") or []
            picked = 0
            for r in roster:
                if picked >= self.hitters_per_team:
                    break
                pos_abbrev = ((r.get("position") or {}).get("abbreviation") or "").upper()
                if pos_abbrev in ("P", "SP", "RP"):
                    continue
                person = r.get("person") or {}
                pid = person.get("id")
                if not pid:
                    continue

                hdata = self._person_stats(pid, "hitting", season)
                # try to get name
                name = str(pid)
                try:
                    name = hdata.get("people", [{}])[0].get("fullName") or name
                except Exception:
                    pass

                avg = None
                hits_each: List[int] = []
                hitless_streak = 0
                for block in hdata.get("stats", []):
                    stype = (block.get("type") or {}).get("displayName", "").lower()
                    group = (block.get("group") or {}).get("displayName", "").lower()
                    if group != "hitting":
                        continue
                    splits = block.get("splits") or []
                    if stype == "season":
                        if splits:
                            avg = _safe_float((splits[0].get("stat") or {}).get("avg"))
                    elif stype == "gamelog":
                        for sp in splits[:5]:
                            stat = sp.get("stat", {})
                            hits = int(stat.get("hits", 0))
                            hits_each.append(hits)
                        # leading-zero run = hitless streak
                        for h in hits_each:
                            if h == 0:
                                hitless_streak += 1
                            else:
                                break

                rows.append({
                    "player_id": str(pid),
                    "name": name,
                    "team": tcode,
                    "opponent_team": opp_code,
                    "probable_pitcher_id": str(opp_prob_by_team.get(tcode)) if opp_prob_by_team.get(tcode) else None,
                    "avg": avg if avg is not None else 0.0,
                    "obp": None,
                    "slg": None,
                    "last_n_games": len(hits_each),
                    "last_n_hits_each_game": hits_each,
                    "last_n_hitless_games": hitless_streak,
                })
                picked += 1

            if limit and len(rows) >= limit:
                break

        if limit:
            rows = rows[:limit]
        return rows

    # ---------- Map to Pydantic ----------
    def get_hitters(self, game_date: _date) -> List[Hitter]:
        return [self._map_hitter(r) for r in self._fetch_hitter_rows(game_date)]

    def get_pitchers(self, game_date: _date) -> List[Pitcher]:
        return [self._map_pitcher(r) for r in self._fetch_pitcher_rows(game_date)]

    def _map_hitter(self, r: Dict[str, Any]) -> Hitter:
        return Hitter(
            player_id=str(r["player_id"]),
            name=r["name"],
            team=r["team"],
            opponent_team=r.get("opponent_team"),
            probable_pitcher_id=r.get("probable_pitcher_id"),
            avg=float(r["avg"]),
            obp=_safe_float(r.get("obp")),
            slg=_safe_float(r.get("slg")),
            last_n_games=int(r.get("last_n_games", 0)),
            last_n_hits_each_game=list(r.get("last_n_hits_each_game", [])),
            last_n_hitless_games=int(r.get("last_n_hitless_games", 0)),
        )

    def _map_pitcher(self, r: Dict[str, Any]) -> Pitcher:
        return Pitcher(
            player_id=str(r["player_id"]),
            name=r["name"],
            team=r["team"],
            opponent_team=r.get("opponent_team"),
            era=float(r.get("era") or 0.0),
            kbb=_safe_float(r.get("kbb")),
            k_per_start_last_n=list(r.get("k_per_start_last_n", [])),
            runs_allowed_last_n=list(r.get("runs_allowed_last_n", [])),
            is_probable=bool(r.get("is_probable", False)),
        )
