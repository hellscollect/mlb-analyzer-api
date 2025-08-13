# providers/statsapi_provider.py
from __future__ import annotations
import os, sys
from typing import Any, Dict, List, Optional, Tuple
from datetime import date as date_cls
import httpx

DEFAULT_BASE = "https://statsapi.mlb.com"

class StatsApiProvider:
    def __init__(self):
        self.base: str = (os.getenv("STATS_API_BASE") or os.getenv("DATA_API_BASE") or DEFAULT_BASE).rstrip("/")
        self.key: Optional[str] = os.getenv("STATS_API_KEY") or os.getenv("DATA_API_KEY") or None
        self._timeout = float(os.getenv("HTTP_TIMEOUT_SEC", "12"))
        self._limits = httpx.Limits(max_keepalive_connections=6, max_connections=12)
        self._debug = (os.getenv("STATS_DEBUG", "0") in ("1","true","True","YES","yes"))
        self._last_schedule_status: Optional[int] = None
        self._last_error: Optional[str] = None

    def _log(self, *a: Any) -> None:
        if self._debug: print("[StatsApiProvider]", *a, file=sys.stderr)

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base}{path}"
        headers = {}
        if self.key: headers["Authorization"] = f"Bearer {self.key}"
        with httpx.Client(timeout=self._timeout, limits=self._limits, headers=headers) as client:
            r = client.get(url, params=params)
            if path.startswith("/api/v1/schedule"): self._last_schedule_status = r.status_code
            if r.status_code in (400,404): return {}
            r.raise_for_status()
            return r.json()

    # ---------- schedule ----------
    def _schedule_games(self, d: date_cls) -> List[Dict[str, Any]]:
        try:
            sch = self._get("/api/v1/schedule", {"date": d.isoformat(), "sportId": 1})
        except httpx.HTTPError as e:
            self._last_error = f"schedule_error: {e}"
            return []
        dates = sch.get("dates") or []
        games: List[Dict[str, Any]] = []
        for blk in dates: games.extend(blk.get("games", []) or [])
        return games

    def _teams_playing_on(self, d: date_cls) -> List[Dict[str, Any]]:
        teams: List[Dict[str, Any]] = []
        for g in self._schedule_games(d):
            t = g.get("teams") or {}
            h = (t.get("home") or {}).get("team") or {}
            a = (t.get("away") or {}).get("team") or {}
            if h: teams.append({"id": h.get("id"), "name": h.get("name")})
            if a: teams.append({"id": a.get("id"), "name": a.get("name")})
        uniq: Dict[int, Dict[str, Any]] = {}
        for t in teams:
            tid = t.get("id")
            if tid: uniq[tid] = t
        return list(uniq.values())

    def _build_matchups(self, d: date_cls) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for g in self._schedule_games(d):
            t = g.get("teams") or {}
            home = (t.get("home") or {})
            away = (t.get("away") or {})
            hteam = (home.get("team") or {})
            ateam = (away.get("team") or {})
            hprob = (home.get("probablePitcher") or {})
            aprob = (away.get("probablePitcher") or {})
            out.append({
                "game_pk": g.get("gamePk"),
                "game_date_utc": g.get("gameDate"),
                "status": (g.get("status") or {}).get("abstractGameState"),
                "venue": (g.get("venue") or {}).get("name"),
                "home": {
                    "team_id": hteam.get("id"), "team_name": hteam.get("name"),
                    "probable_pitcher_id": hprob.get("id"), "probable_pitcher_name": hprob.get("fullName"),
                },
                "away": {
                    "team_id": ateam.get("id"), "team_name": ateam.get("name"),
                    "probable_pitcher_id": aprob.get("id"), "probable_pitcher_name": aprob.get("fullName"),
                },
            })
        return out

    # ---------- roster + stats ----------
    def _team_roster(self, team_id: int) -> List[Dict[str, Any]]:
        data = self._get(f"/api/v1/teams/{team_id}/roster", {"rosterType": "active"})
        return data.get("roster", []) or []

    def _player_season_stats(self, pid: int, season_year: int, group: str) -> Dict[str, Any]:
        data = self._get(f"/api/v1/people/{pid}/stats", {"stats": "season", "group": group, "season": season_year})
        stats = data.get("stats") or []
        if not stats: return {}
        splits = stats[0].get("splits") or []
        return (splits[0].get("stat") if splits else {}) or {}

    # ---------- bounded rows ----------
    def _sampled_roster_rows(self, *, date: date_cls, max_teams: int, per_team: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        year = date.year
        hitters_rows: List[Dict[str, Any]] = []
        pitchers_rows: List[Dict[str, Any]] = []
        for t in self._teams_playing_on(date)[:max_teams]:
            roster = self._team_roster(t["id"])
            hitters = [r for r in roster if ((r.get("position") or {}).get("abbreviation")) != "P"][:per_team]
            pitchers = [r for r in roster if ((r.get("position") or {}).get("abbreviation")) == "P"][:per_team]
            for r in hitters:
                p = r.get("person") or {}; pid = p.get("id"); pname = p.get("fullName")
                if not pid: continue
                stat = self._player_season_stats(pid, year, "hitting") or {}
                hitters_rows.append({
                    "player_id": pid, "player_name": pname, "team_id": t["id"], "team_name": t["name"],
                    "avg": _safe_float(stat.get("avg")), "ops": _safe_float(stat.get("ops")),
                    "hr": _safe_int(stat.get("homeRuns")), "rbi": _safe_int(stat.get("rbi")),
                    "gamesPlayed": _safe_int(stat.get("gamesPlayed")),
                })
            for r in pitchers:
                p = r.get("person") or {}; pid = p.get("id"); pname = p.get("fullName")
                if not pid: continue
                stat = self._player_season_stats(pid, year, "pitching") or {}
                pitchers_rows.append({
                    "player_id": pid, "player_name": pname, "team_id": t["id"], "team_name": t["name"],
                    "era": _safe_float(stat.get("era")), "so": _safe_int(stat.get("strikeOuts")),
                    "whip": _safe_float(stat.get("whip")), "gamesStarted": _safe_int(stat.get("gamesStarted")),
                })
        return hitters_rows, pitchers_rows

    # ---------- public API ----------
    def hot_streak_hitters(self, *, date: date_cls, min_avg: float, games: int, require_hit_each: bool, debug: bool) -> Dict[str, Any]:
        hit, _ = self._sampled_roster_rows(date=date, max_teams=16, per_team=8)
        hot = [h for h in hit if (h.get("avg") or 0.0) >= float(min_avg)]
        return {"items": hot, **({"debug": {"last_schedule_status": self._last_schedule_status, "error": self._last_error}} if debug else {})}

    def cold_streak_hitters(self, *, date: date_cls, min_avg: float, games: int, require_zero_hit_each: bool, debug: bool) -> Dict[str, Any]:
        hit, _ = self._sampled_roster_rows(date=date, max_teams=16, per_team=8)
        cold = [h for h in hit if (h.get("avg") or 1.0) < float(min_avg)]
        return {"items": cold, **({"debug": {"last_schedule_status": self._last_schedule_status, "error": self._last_error}} if debug else {})}

    def pitcher_streaks(self, *, date: date_cls, hot_max_era: float, hot_min_ks_each: int, hot_last_starts: int,
                        cold_min_era: float, cold_min_runs_each: int, cold_last_starts: int, debug: bool) -> Dict[str, Any]:
        _, pit = self._sampled_roster_rows(date=date, max_teams=16, per_team=8)
        hot = [p for p in pit if (p.get("era") or 99.9) <= float(hot_max_era)]
        cold = [p for p in pit if (p.get("era") or 0.0) >= float(cold_min_era)]
        return {"hot": hot, "cold": cold, **({"debug": {"last_schedule_status": self._last_schedule_status, "error": self._last_error}} if debug else {})}

    def cold_pitchers(self, *, date: date_cls, min_era: float, min_runs_each: int, last_starts: int, debug: bool) -> Dict[str, Any]:
        _, pit = self._sampled_roster_rows(date=date, max_teams=16, per_team=8)
        cold = [p for p in pit if (p.get("era") or 0.0) >= float(min_era)]
        return {"items": cold, **({"debug": {"last_schedule_status": self._last_schedule_status, "error": self._last_error}} if debug else {})}

    def slate_scan(self, *, date: date_cls, max_teams: int = 16, per_team: int = 8, debug: bool = False) -> Dict[str, Any]:
        per_team = max(1, min(15, per_team)); max_teams = max(2, min(30, max_teams))
        hitters_rows, pitchers_rows = self._sampled_roster_rows(date=date, max_teams=max_teams, per_team=per_team)
        hot_hitters = [h for h in hitters_rows if (h.get("avg") or 0.0) >= 0.300]
        cold_hitters = [h for h in hitters_rows if (h.get("avg") or 1.0) < 0.220]
        hot_pitchers = [p for p in pitchers_rows if (p.get("era") or 99.9) <= 3.50]
        cold_pitchers = [p for p in pitchers_rows if (p.get("era") or 0.0) >= 4.60]
        matchups = self._build_matchups(date)
        out = {
            "hot_hitters": hot_hitters, "cold_hitters": cold_hitters,
            "hot_pitchers": hot_pitchers, "cold_pitchers": cold_pitchers,
            "matchups": matchups,
        }
        if debug:
            out["debug"] = {
                "source": "statsapi", "base": self.base, "last_schedule_status": self._last_schedule_status,
                "error": self._last_error, "limits": {"max_teams": max_teams, "per_team": per_team},
                "counts": {"hitters_rows": len(hitters_rows), "pitchers_rows": len(pitchers_rows), "matchups": len(matchups)},
            }
        return out

    def light_slate(self, *, date: date_cls, max_teams: int = 6, per_team: int = 2, debug: bool = True) -> Dict[str, Any]:
        # unchanged; used for smoke tests if needed
        teams = self._teams_playing_on(date); games_count = len(teams)//2 if teams else 0; teams = teams[:max_teams]
        year = date.year; hitters_samples: List[str]=[]; pitchers_samples: List[str]=[]
        try:
            for t in teams:
                roster = self._team_roster(t["id"])
                hitters = [r for r in roster if ((r.get("position") or {}).get("abbreviation")) != "P"][:per_team]
                pitchers = [r for r in roster if ((r.get("position") or {}).get("abbreviation")) == "P"][:per_team]
                for r in hitters:
                    p=r.get("person") or {}; pid=p.get("id"); pname=p.get("fullName") or "Unknown"
                    if pid: self._player_season_stats(pid, year, "hitting"); hitters_samples.append(pname)
                for r in pitchers:
                    p=r.get("person") or {}; pid=p.get("id"); pname=p.get("fullName") or "Unknown"
                    if pid: self._player_season_stats(pid, year, "pitching"); pitchers_samples.append(pname)
        except Exception as e:
            self._last_error=f"light_slate_error: {e}"
        result = {"date": date.isoformat(), "games_count_est": games_count,
                  "counts":{"sampled_teams": len(teams), "sampled_hitters": len(hitters_samples), "sampled_pitchers": len(pitchers_samples)},
                  "samples":{"hitters": hitters_samples[:10], "pitchers": pitchers_samples[:10]}}
        if debug: result["debug"]={"base": self.base, "last_schedule_status": self._last_schedule_status, "error": self._last_error}
        return result

    def debug_schedule(self, *, date: date_cls) -> Dict[str, Any]:
        payload = self._get("/api/v1/schedule", {"date": date.isoformat(), "sportId": 1})
        dates = payload.get("dates") or []; games=[]
        for blk in dates: games.extend(blk.get("games", []) or [])
        teams_sample=[]
        for g in games[:5]:
            t=g.get("teams") or {}; home=((t.get("home") or {}).get("team") or {}).get("name"); away=((t.get("away") or {}).get("team") or {}).get("name")
            teams_sample.append({"home": home, "away": away})
        return {"status": self._last_schedule_status, "date": date.isoformat(), "games_count": len(games), "teams_sample": teams_sample, "raw": payload}

def _safe_float(v: Any) -> Optional[float]:
    try:
        if v in (None,""): return None
        return float(v)
    except Exception:
        return None

def _safe_int(v: Any) -> Optional[int]:
    try:
        if v in (None,""): return None
        return int(v)
    except Exception:
        return None
