# providers/statsapi_provider.py

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional
from datetime import date as date_cls

import httpx

# Public MLB Stats API base; overridable via env
DEFAULT_BASE = "https://statsapi.mlb.com"

class StatsApiProvider:
    """
    Minimal real-data provider using MLB Stats API.

    - _fetch_hitter_rows / _fetch_pitcher_rows:
        Returns player rows (name, team, season stats) for teams scheduled that date.
    - hot/cold endpoints:
        Basic heuristics using season averages/ERA to produce usable lists.

    NOTE: This is intentionally simplified so you can see real data today.
    You can evolve streak logic later with game logs.
    """

    def __init__(self):
        self.base: str = (
            os.getenv("STATS_API_BASE")
            or os.getenv("DATA_API_BASE")
            or DEFAULT_BASE
        ).rstrip("/")

        # API key not required for public endpoints; present for parity with debug
        self.key: Optional[str] = os.getenv("STATS_API_KEY") or os.getenv("DATA_API_KEY") or None

        # HTTP client config
        self._timeout = float(os.getenv("HTTP_TIMEOUT_SEC", "15"))
        self._limits = httpx.Limits(max_keepalive_connections=8, max_connections=16)

    # ------------- HTTP helpers -------------

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base}{path}"
        headers = {}
        if self.key:
            headers["Authorization"] = f"Bearer {self.key}"
        with httpx.Client(timeout=self._timeout, limits=self._limits, headers=headers) as client:
            r = client.get(url, params=params)
            # Treat 400/404 as "no data" instead of an error
            if r.status_code in (400, 404):
                return {}
            r.raise_for_status()
            return r.json()

    # ------------- Core fetches -------------

    def _teams_playing_on(self, d: date_cls) -> List[Dict[str, Any]]:
        # MLB StatsAPI schedule needs sportId=1 for MLB
        try:
            sch = self._get("/api/v1/schedule", {"date": d.isoformat(), "sportId": 1})
        except httpx.HTTPError:
            # Network hiccup or other non-HTTP error: treat as no games to avoid 500s.
            return []
        dates = sch.get("dates") or []
        if not dates:
            return []
        games = []
        for date_block in dates:
            games.extend(date_block.get("games", []) or [])
        teams = []
        for g in games:
            for side in ("home", "away"):
                t = g.get(f"{side}Team") or {}
                if t:
                    teams.append({"id": t.get("id"), "name": t.get("name")})
        # de-dup by id
        uniq = {}
        for t in teams:
            if t.get("id"):
                uniq[t["id"]] = t
        return list(uniq.values())

    def _team_roster(self, team_id: int) -> List[Dict[str, Any]]:
        data = self._get(f"/api/v1/teams/{team_id}/roster", {"rosterType": "active"})
        return data.get("roster", []) or []

    def _player_season_stats(self, player_id: int, season_year: int, group: str) -> Dict[str, Any]:
        # group: "hitting" or "pitching"
        data = self._get(f"/api/v1/people/{player_id}/stats", {"stats": "season", "group": group, "season": season_year})
        splits = (data.get("stats") or [{}])[0].get("splits") or []
        return (splits[0].get("stat") if splits else {}) or {}

    # ------------- Rows for provider_raw -------------

    def _fetch_hitter_rows(self, date: date_cls, limit: Optional[int] = None, team: Optional[str] = None) -> List[Dict[str, Any]]:
        year = date.year
        rows: List[Dict[str, Any]] = []
        teams = self._teams_playing_on(date)
        for t in teams:
            if team and team.lower() not in (t.get("name") or "").lower():
                continue
            roster = self._team_roster(t["id"])
            for r in roster:
                p = r.get("person") or {}
                pid = p.get("id")
                pname = p.get("fullName")
                if not pid:
                    continue
                # get hitting stats
                try:
                    stat = self._player_season_stats(pid, year, "hitting")
                except Exception:
                    stat = {}
                row = {
                    "player_id": pid,
                    "player_name": pname,
                    "team_id": t["id"],
                    "team_name": t["name"],
                    "avg": _safe_float(stat.get("avg")),
                    "ops": _safe_float(stat.get("ops")),
                    "hr": _safe_int(stat.get("homeRuns")),
                    "rbi": _safe_int(stat.get("rbi")),
                    "gamesPlayed": _safe_int(stat.get("gamesPlayed")),
                }
                rows.append(row)
                if limit and len(rows) >= limit:
                    return rows
        return rows

    def _fetch_pitcher_rows(self, date: date_cls, limit: Optional[int] = None, team: Optional[str] = None) -> List[Dict[str, Any]]:
        year = date.year
        rows: List[Dict[str, Any]] = []
        teams = self._teams_playing_on(date)
        for t in teams:
            if team and team.lower() not in (t.get("name") or "").lower():
                continue
            roster = self.__team_roster(t["id"])
            for r in roster:
                p = r.get("person") or {}
                pid = p.get("id")
                pname = p.get("fullName")
                primary = (r.get("position") or {}).get("abbreviation", "")
                if not pid:
                    continue
                # only pitchers if position is P (heuristic)
                if primary != "P":
                    continue
                # get pitching stats
                try:
                    stat = self._player_season_stats(pid, year, "pitching")
                except Exception:
                    stat = {}
                row = {
                    "player_id": pid,
                    "player_name": pname,
                    "team_id": t["id"],
                    "team_name": t["name"],
                    "era": _safe_float(stat.get("era")),
                    "so": _safe_int(stat.get("strikeOuts")),
                    "whip": _safe_float(stat.get("whip")),
                    "gamesStarted": _safe_int(stat.get("gamesStarted")),
                }
                rows.append(row)
                if limit and len(rows) >= limit:
                    return rows
        return rows

    # ------------- Public endpoints -------------

    def hot_streak_hitters(self, *, date: date_cls, min_avg: float, games: int, require_hit_each: bool, debug: bool) -> Dict[str, Any]:
        # Simplified heuristic: season AVG >= min_avg
        hitters = self._fetch_hitter_rows(date, limit=None, team=None)
        hot = [h for h in hitters if (h.get("avg") or 0.0) >= float(min_avg)]
        out = {"items": hot}
        if debug:
            out["debug"] = {"note": "Heuristic based on season AVG; per-game streaks not computed in this minimal version."}
        return out

    def cold_streak_hitters(self, *, date: date_cls, min_avg: float, games: int, require_zero_hit_each: bool, debug: bool) -> Dict[str, Any]:
        # Simplified heuristic: season AVG < min_avg
        hitters = self._fetch_hitter_rows(date, limit=None, team=None)
        cold = [h for h in hitters if (h.get("avg") or 1.0) < float(min_avg)]
        out = {"items": cold}
        if debug:
            out["debug"] = {"note": "Heuristic based on season AVG; zero-hit-per-game logic not computed in this minimal version."}
        return out

    def pitcher_streaks(self, *, date: date_cls, hot_max_era: float, hot_min_ks_each: int, hot_last_starts: int,
                        cold_min_era: float, cold_min_runs_each: int, cold_last_starts: int, debug: bool) -> Dict[str, Any]:
        # Simplified heuristic: hot ERA <= hot_max_era, cold ERA >= cold_min_era
        pitchers = self._fetch_pitcher_rows(date, limit=None, team=None)
        hot = [p for p in pitchers if (p.get("era") or 99.9) <= float(hot_max_era)]
        cold = [p for p in pitchers if (p.get("era") or 0.0) >= float(cold_min_era)]
        out = {"hot": hot, "cold": cold}
        if debug:
            out["debug"] = {"note": "Heuristic based on season ERA; per-start streaks not computed in this minimal version."}
        return out

    def cold_pitchers(self, *, date: date_cls, min_era: float, min_runs_each: int, last_starts: int, debug: bool) -> Dict[str, Any]:
        pitchers = self._fetch_pitcher_rows(date, limit=None, team=None)
        cold = [p for p in pitchers if (p.get("era") or 0.0) >= float(min_era)]
        out = {"items": cold}
        if debug:
            out["debug"] = {"note": "Heuristic based on season ERA; run-allowed streaks not computed in this minimal version."}
        return out

    def slate_scan(self, *, date: date_cls, debug: bool) -> Dict[str, Any]:
        # Build via simple heuristics so you get usable lists
        hot_hitters = self.hot_streak_hitters(date=date, min_avg=0.300, games=3, require_hit_each=True, debug=False)["items"]
        cold_hitters = self.cold_streak_hitters(date=date, min_avg=0.220, games=2, require_zero_hit_each=True, debug=False)["items"]
        hot_pitchers = self.pitcher_streaks(date=date, hot_max_era=3.50, hot_min_ks_each=6, hot_last_starts=3,
                                            cold_min_era=4.60, cold_min_runs_each=3, cold_last_starts=2, debug=False)["hot"]
        cold_pitchers = self.cold_pitchers(date=date, min_era=4.60, min_runs_each=3, last_starts=2, debug=False)["items"]

        out = {
            "hot_hitters": hot_hitters,
            "cold_hitters": cold_hitters,
            "hot_pitchers": hot_pitchers,
            "cold_pitchers": cold_pitchers,
            "matchups": [],  # You can populate this later
        }
        if debug:
            out["debug"] = {"source": "statsapi", "base": self.base}
        return out


# --------- helpers ---------
def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None

def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(v)
    except Exception:
        return None
