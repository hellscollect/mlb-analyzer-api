# providers/statsapi_provider.py

from __future__ import annotations

import os
import sys
from typing import Any, Dict, List, Optional
from datetime import date as date_cls

import httpx

DEFAULT_BASE = "https://statsapi.mlb.com"


class StatsApiProvider:
    """
    Real-data provider using MLB StatsAPI.

    Endpoints used:
      - /api/v1/schedule?date=YYYY-MM-DD&sportId=1
      - /api/v1/teams/{team_id}/roster?rosterType=active
      - /api/v1/people/{player_id}/stats?stats=season&group=hitting|pitching&season=YYYY
    """

    def __init__(self):
        self.base: str = (
            os.getenv("STATS_API_BASE")
            or os.getenv("DATA_API_BASE")
            or DEFAULT_BASE
        ).rstrip("/")

        self.key: Optional[str] = os.getenv("STATS_API_KEY") or os.getenv("DATA_API_KEY") or None

        self._timeout = float(os.getenv("HTTP_TIMEOUT_SEC", "15"))
        self._limits = httpx.Limits(max_keepalive_connections=8, max_connections=16)

        self._debug = (os.getenv("STATS_DEBUG", "0") in ("1", "true", "True", "YES", "yes"))

        self._last_schedule_status: Optional[int] = None
        self._last_error: Optional[str] = None

    # -------- logging --------
    def _log(self, *args: Any) -> None:
        if self._debug:
            print("[StatsApiProvider]", *args, file=sys.stderr)

    # -------- HTTP helper --------
    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base}{path}"
        headers = {}
        if self.key:
            headers["Authorization"] = f"Bearer {self.key}"
        self._log("GET", url, "params=", params)
        with httpx.Client(timeout=self._timeout, limits=self._limits, headers=headers) as client:
            r = client.get(url, params=params)
            self._log("HTTP", r.status_code, "for", url)
            if path.startswith("/api/v1/schedule"):
                self._last_schedule_status = r.status_code
            if r.status_code in (400, 404):
                return {}
            r.raise_for_status()
            return r.json()

    # -------- core fetches --------
    def _teams_playing_on(self, d: date_cls) -> List[Dict[str, Any]]:
        """
        FIXED: read teams from game['teams']['home']['team'] / ['away']['team'].
        """
        try:
            sch = self._get("/api/v1/schedule", {"date": d.isoformat(), "sportId": 1})
        except httpx.HTTPError as e:
            self._last_error = f"schedule_error: {e}"
            self._log(self._last_error)
            return []

        dates = sch.get("dates") or []
        if not dates:
            return []

        games: List[Dict[str, Any]] = []
        for block in dates:
            games.extend(block.get("games", []) or [])

        teams: List[Dict[str, Any]] = []
        for g in games:
            teams_node = g.get("teams") or {}
            home_team = ((teams_node.get("home") or {}).get("team")) or {}
            away_team = ((teams_node.get("away") or {}).get("team")) or {}
            if home_team:
                teams.append({"id": home_team.get("id"), "name": home_team.get("name")})
            if away_team:
                teams.append({"id": away_team.get("id"), "name": away_team.get("name")})

        uniq: Dict[int, Dict[str, Any]] = {}
        for t in teams:
            tid = t.get("id")
            if tid:
                uniq[tid] = t
        out = list(uniq.values())
        self._log(f"{len(out)} teams scheduled on {d.isoformat()}")
        return out

    def _team_roster(self, team_id: int) -> List[Dict[str, Any]]:
        data = self._get(f"/api/v1/teams/{team_id}/roster", {"rosterType": "active"})
        roster = data.get("roster", []) or []
        self._log(f"team {team_id} roster size:", len(roster))
        return roster

    def _player_season_stats(self, player_id: int, season_year: int, group: str) -> Dict[str, Any]:
        data = self._get(
            f"/api/v1/people/{player_id}/stats",
            {"stats": "season", "group": group, "season": season_year},
        )
        stats = data.get("stats") or []
        if not stats:
            return {}
        splits = stats[0].get("splits") or []
        return (splits[0].get("stat") if splits else {}) or {}

    # -------- rows for provider_raw --------
    def _fetch_hitter_rows(self, date: date_cls, limit: Optional[int] = None, team: Optional[str] = None) -> List[Dict[str, Any]]:
        year = date.year
        rows: List[Dict[str, Any]] = []
        for t in self._teams_playing_on(date):
            if team and team.lower() not in (t.get("name") or "").lower():
                continue
            roster = self._team_roster(t["id"])
            for r in roster:
                p = r.get("person") or {}
                pid = p.get("id")
                pname = p.get("fullName")
                if not pid:
                    continue
                try:
                    stat = self._player_season_stats(pid, year, "hitting")
                except Exception as e:
                    self._log("hitting stat error:", e)
                    stat = {}
                rows.append({
                    "player_id": pid,
                    "player_name": pname,
                    "team_id": t["id"],
                    "team_name": t["name"],
                    "avg": _safe_float(stat.get("avg")),
                    "ops": _safe_float(stat.get("ops")),
                    "hr": _safe_int(stat.get("homeRuns")),
                    "rbi": _safe_int(stat.get("rbi")),
                    "gamesPlayed": _safe_int(stat.get("gamesPlayed")),
                })
                if limit and len(rows) >= limit:
                    self._log("hitter rows limited to", limit)
                    return rows
        self._log("total hitter rows:", len(rows))
        return rows

    def _fetch_pitcher_rows(self, date: date_cls, limit: Optional[int] = None, team: Optional[str] = None) -> List[Dict[str, Any]]:
        year = date.year
        rows: List[Dict[str, Any]] = []
        for t in self._teams_playing_on(date):
            if team and team.lower() not in (t.get("name") or "").lower():
                continue
            roster = self._team_roster(t["id"])
            for r in roster:
                p = r.get("person") or {}
                pid = p.get("id")
                pname = p.get("fullName")
                if not pid:
                    continue
                pos_abbr = (r.get("position") or {}).get("abbreviation", "")
                if pos_abbr != "P":
                    continue
                try:
                    stat = self._player_season_stats(pid, year, "pitching")
                except Exception as e:
                    self._log("pitching stat error:", e)
                    stat = {}
                rows.append({
                    "player_id": pid,
                    "player_name": pname,
                    "team_id": t["id"],
                    "team_name": t["name"],
                    "era": _safe_float(stat.get("era")),
                    "so": _safe_int(stat.get("strikeOuts")),
                    "whip": _safe_float(stat.get("whip")),
                    "gamesStarted": _safe_int(stat.get("gamesStarted")),
                })
                if limit and len(rows) >= limit:
                    self._log("pitcher rows limited to", limit)
                    return rows
        self._log("total pitcher rows:", len(rows))
        return rows

    # -------- public endpoints (simple heuristics) --------
    def hot_streak_hitters(self, *, date: date_cls, min_avg: float, games: int, require_hit_each: bool, debug: bool) -> Dict[str, Any]:
        hitters = self._fetch_hitter_rows(date, limit=None, team=None)
        hot = [h for h in hitters if (h.get("avg") or 0.0) >= float(min_avg)]
        out: Dict[str, Any] = {"items": hot}
        if debug:
            out["debug"] = {"note": "Heuristic = season AVG >= min_avg",
                            "last_schedule_status": self._last_schedule_status, "error": self._last_error}
        return out

    def cold_streak_hitters(self, *, date: date_cls, min_avg: float, games: int, require_zero_hit_each: bool, debug: bool) -> Dict[str, Any]:
        hitters = self._fetch_hitter_rows(date, limit=None, team=None)
        cold = [h for h in hitters if (h.get("avg") or 1.0) < float(min_avg)]
        out: Dict[str, Any] = {"items": cold}
        if debug:
            out["debug"] = {"note": "Heuristic = season AVG < min_avg",
                            "last_schedule_status": self._last_schedule_status, "error": self._last_error}
        return out

    def pitcher_streaks(self, *, date: date_cls, hot_max_era: float, hot_min_ks_each: int, hot_last_starts: int,
                        cold_min_era: float, cold_min_runs_each: int, cold_last_starts: int, debug: bool) -> Dict[str, Any]:
        pitchers = self._fetch_pitcher_rows(date, limit=None, team=None)
        hot = [p for p in pitchers if (p.get("era") or 99.9) <= float(hot_max_era)]
        cold = [p for p in pitchers if (p.get("era") or 0.0) >= float(cold_min_era)]
        out: Dict[str, Any] = {"hot": hot, "cold": cold}
        if debug:
            out["debug"] = {"note": "Heuristic = hot ERA ≤ hot_max_era; cold ERA ≥ cold_min_era",
                            "last_schedule_status": self._last_schedule_status, "error": self._last_error}
        return out

    def cold_pitchers(self, *, date: date_cls, min_era: float, min_runs_each: int, last_starts: int, debug: bool) -> Dict[str, Any]:
        pitchers = self._fetch_pitcher_rows(date, limit=None, team=None)
        cold = [p for p in pitchers if (p.get("era") or 0.0) >= float(min_era)]
        out: Dict[str, Any] = {"items": cold}
        if debug:
            out["debug"] = {"note": "Heuristic = season ERA ≥ min_era",
                            "last_schedule_status": self._last_schedule_status, "error": self._last_error}
        return out

    def slate_scan(self, *, date: date_cls, debug: bool) -> Dict[str, Any]:
        hot_hitters = self.hot_streak_hitters(date=date, min_avg=0.300, games=3, require_hit_each=True, debug=False)["items"]
        cold_hitters = self.cold_streak_hitters(date=date, min_avg=0.220, games=2, require_zero_hit_each=True, debug=False)["items"]
        ps = self.pitcher_streaks(date=date, hot_max_era=3.50, hot_min_ks_each=6, hot_last_starts=3,
                                  cold_min_era=4.60, cold_min_runs_each=3, cold_last_starts=2, debug=False)
        hot_pitchers = ps["hot"]
        cold_pitchers = ps["cold"]
        out: Dict[str, Any] = {
            "hot_hitters": hot_hitters,
            "cold_hitters": cold_hitters,
            "hot_pitchers": hot_pitchers,
            "cold_pitchers": cold_pitchers,
            "matchups": [],  # expand later if you want
        }
        if debug:
            out["debug"] = {"source": "statsapi", "base": self.base,
                            "last_schedule_status": self._last_schedule_status, "error": self._last_error}
        return out

    # -------- diagnostics --------
    def debug_schedule(self, *, date: date_cls) -> Dict[str, Any]:
        payload = self._get("/api/v1/schedule", {"date": date.isoformat(), "sportId": 1})
        dates = payload.get("dates") or []
        games = []
        for dblock in dates:
            games.extend(dblock.get("games", []) or [])
        teams_sample = []
        for g in games[:5]:
            teams_node = g.get("teams") or {}
            home = ((teams_node.get("home") or {}).get("team") or {}).get("name")
            away = ((teams_node.get("away") or {}).get("team") or {}).get("name")
            teams_sample.append({"home": home, "away": away})
        return {
            "status": self._last_schedule_status,
            "date": date.isoformat(),
            "games_count": len(games),
            "teams_sample": teams_sample,
            "raw": payload,
        }


# -------- helpers --------
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
