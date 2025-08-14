# providers/statsapi_provider.py
from __future__ import annotations

import os, sys
from typing import Any, Dict, List, Optional, Tuple
from datetime import date as date_cls, datetime, timezone
from statistics import mean
from zoneinfo import ZoneInfo
import httpx

DEFAULT_BASE = "https://statsapi.mlb.com"
ET_ZONE = ZoneInfo("America/New_York")


class StatsApiProvider:
    """
    Real-data provider using MLB StatsAPI.

    Endpoints used:
      - /api/v1/schedule?date=YYYY-MM-DD&sportId=1&hydrate=probablePitcher
      - /api/v1/teams/{team_id}/roster?rosterType=active
      - /api/v1/people/{player_id}/stats?stats=season|gameLog&group=hitting|pitching&season=YYYY
    """

    def __init__(self):
        self.base: str = (
            os.getenv("STATS_API_BASE")
            or os.getenv("DATA_API_BASE")
            or DEFAULT_BASE
        ).rstrip("/")

        self.key: Optional[str] = os.getenv("STATS_API_KEY") or os.getenv("DATA_API_KEY") or None

        # Conservative timeouts/limits to play nice with Actions
        self._timeout = float(os.getenv("HTTP_TIMEOUT_SEC", "12"))
        self._limits = httpx.Limits(max_keepalive_connections=6, max_connections=12)

        self._debug = (os.getenv("STATS_DEBUG", "0") in ("1","true","True","YES","yes"))

        self._last_schedule_status: Optional[int] = None
        self._last_error: Optional[str] = None

    # -------- logging --------
    def _log(self, *a: Any) -> None:
        if self._debug:
            print("[StatsApiProvider]", *a, file=sys.stderr)

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

    # -------- time helpers --------
    def _to_et_str(self, game_date_utc: Optional[str]) -> Optional[str]:
        if not game_date_utc:
            return None
        try:
            if game_date_utc.endswith("Z"):
                dt_utc = datetime.fromisoformat(game_date_utc.replace("Z", "+00:00"))
            else:
                dt_utc = datetime.fromisoformat(game_date_utc)
                if dt_utc.tzinfo is None:
                    dt_utc = dt_utc.replace(tzinfo=timezone.utc)
            dt_et = dt_utc.astimezone(ET_ZONE)
            return dt_et.strftime("%Y-%m-%d %H:%M ET")
        except Exception as e:
            self._log("time parse error:", e, "raw:", game_date_utc)
            return None

    # -------- schedule helpers --------
    def _schedule_games(self, d: date_cls) -> List[Dict[str, Any]]:
        try:
            sch = self._get(
                "/api/v1/schedule",
                {
                    "date": d.isoformat(),
                    "sportId": 1,
                    "hydrate": "probablePitcher",
                },
            )
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
        return games

    def _teams_playing_on(self, d: date_cls) -> List[Dict[str, Any]]:
        games = self._schedule_games(d)
        teams: List[Dict[str, Any]] = []
        for g in games:
            tnode = g.get("teams") or {}
            home_team = ((tnode.get("home") or {}).get("team")) or {}
            away_team = ((tnode.get("away") or {}).get("team")) or {}
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

    def _build_matchups(self, d: date_cls) -> List[Dict[str, Any]]:
        games = self._schedule_games(d)
        matchups: List[Dict[str, Any]] = []
        for g in games:
            tnode = g.get("teams") or {}
            home = (tnode.get("home") or {})
            away = (tnode.get("away") or {})
            home_team = (home.get("team") or {})
            away_team = (away.get("team") or {})
            home_prob = (home.get("probablePitcher") or {})
            away_prob = (away.get("probablePitcher") or {})

            venue_name = ((g.get("venue") or {}).get("name")) or None
            status = ((g.get("status") or {}).get("abstractGameState")) or None
            game_date_utc = g.get("gameDate")
            et_time = self._to_et_str(game_date_utc)

            matchups.append({
                "game_pk": g.get("gamePk"),
                "game_date_utc": game_date_utc,
                "et_time": et_time,
                "status": status,
                "venue": venue_name,
                "home": {
                    "team_id": home_team.get("id"),
                    "team_name": home_team.get("name"),
                    "probable_pitcher_id": home_prob.get("id"),
                    "probable_pitcher_name": home_prob.get("fullName"),
                },
                "away": {
                    "team_id": away_team.get("id"),
                    "team_name": away_team.get("name"),
                    "probable_pitcher_id": away_prob.get("id"),
                    "probable_pitcher_name": away_prob.get("fullName"),
                },
            })
        return matchups

    # -------- roster & stats --------
    def _team_roster(self, team_id: int) -> List[Dict[str, Any]]:
        data = self._get(f"/api/v1/teams/{team_id}/roster", {"rosterType": "active"})
        return data.get("roster", []) or []

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

    def _player_game_logs(self, player_id: int, season_year: int, group: str, limit: int = 30) -> List[Dict[str, Any]]:
        data = self._get(
            f"/api/v1/people/{player_id}/stats",
            {"stats": "gameLog", "group": group, "season": season_year},
        )
        stats = data.get("stats") or []
        if not stats:
            return []
        splits = (stats[0].get("splits") or [])[:limit]
        logs: List[Dict[str, Any]] = []
        for s in splits:
            stat = s.get("stat") or {}
            logs.append({
                "date": s.get("date"),
                "hits": _safe_int(stat.get("hits")) or 0,
                "atBats": _safe_int(stat.get("atBats")) or 0,
            })
        logs.reverse()  # oldest -> newest
        return logs

    # -------- helpers to bound work (used by old endpoints) --------
    def _sampled_roster_rows(
        self,
        *,
        date: date_cls,
        max_teams: int,
        per_team: int
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        year = date.year
        hitters_rows: List[Dict[str, Any]] = []
        pitchers_rows: List[Dict[str, Any]] = []

        teams = self._teams_playing_on(date)[:max_teams]
        for t in teams:
            roster = self._team_roster(t["id"])

            hitters = [r for r in roster if ((r.get("position") or {}).get("abbreviation")) != "P"][:per_team]
            pitchers = [r for r in roster if ((r.get("position") or {}).get("abbreviation")) == "P"][:per_team]

            for r in hitters:
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
                hitters_rows.append({
                    "player_id": pid,
                    "player_name": pname,
                    "team_id": t["id"],
                    "team_name": t["name"],
                    "avg": _safe_float(stat.get("avg")),
                    "ops": _safe_float(stat.get("ops")),
                    "hr": _safe_int(stat.get("homeRuns")),
                    "rbi": _safe_int(stat.get("rbi")),
                    "pa": _safe_int(stat.get("plateAppearances")),
                    "gamesPlayed": _safe_int(stat.get("gamesPlayed")),
                })

            for r in pitchers:
                p = r.get("person") or {}
                pid = p.get("id")
                pname = p.get("fullName")
                if not pid:
                    continue
                try:
                    stat = self._player_season_stats(pid, year, "pitching")
                except Exception as e:
                    self._log("pitching stat error:", e)
                    stat = {}
                pitchers_rows.append({
                    "player_id": pid,
                    "player_name": pname,
                    "team_id": t["id"],
                    "team_name": t["name"],
                    "era": _safe_float(stat.get("era")),
                    "so": _safe_int(stat.get("strikeOuts")),
                    "whip": _safe_float(stat.get("whip")),
                    "gamesStarted": _safe_int(stat.get("gamesStarted")),
                })

        return hitters_rows, pitchers_rows

    # -------- slump & surge analytics (hitters) --------
    @staticmethod
    def _current_hitless_streak(logs: List[Dict[str, Any]]) -> int:
        streak = 0
        for g in reversed(logs):
            if (g.get("hits") or 0) == 0 and (g.get("atBats") or 0) > 0:
                streak += 1
            else:
                break
        return streak

    @staticmethod
    def _hitless_run_lengths(logs: List[Dict[str, Any]]) -> List[int]:
        runs: List[int] = []
        cur = 0
        for g in logs:
            ab = g.get("atBats") or 0
            hits = g.get("hits") or 0
            if ab == 0:
                if cur > 0:
                    runs.append(cur)
                    cur = 0
                continue
            if hits == 0:
                cur += 1
            else:
                if cur > 0:
                    runs.append(cur)
                    cur = 0
        if cur > 0:
            runs.append(cur)
        return runs

    @staticmethod
    def _base_slump_weight(n: int) -> int:
        if n <= 0: return 0
        if n == 1: return 1
        if n == 2: return 3
        if n == 3: return 5
        return 7

    @staticmethod
    def _quality_multiplier(season_avg: Optional[float]) -> float:
        if not season_avg:
            return 1.0
        a = season_avg
        if a <= 0.270: return 1.0
        if a >= 0.330: return 1.30
        return 1.0 + (a - 0.270) * (0.30 / 0.060)

    def _cold_hitter_score(self, season_avg: Optional[float], logs: List[Dict[str, Any]]) -> Dict[str, Any]:
        ch = self._current_hitless_streak(logs)
        runs = self._hitless_run_lengths(logs)
        avg_run = mean(runs) if runs else 0.7
        rarity = ch / max(0.75, avg_run)
        rarity = max(0.5, min(2.0, rarity))
        base = self._base_slump_weight(ch)
        qual = self._quality_multiplier(season_avg)
        score = base * rarity * qual
        return {
            "current_hitless_streak": ch,
            "avg_hitless_run": round(avg_run, 2),
            "rarity_index": round(rarity, 2),
            "slump_score": round(score, 2),
        }

    def _hot_hitter_score(self, season_avg: Optional[float], logs: List[Dict[str, Any]]) -> Dict[str, Any]:
        recent = logs[-5:] if len(logs) >= 5 else logs[:]
        ab = sum((g.get("atBats") or 0) for g in recent)
        hits = sum((g.get("hits") or 0) for g in recent)
        recent_avg = (hits / ab) if ab > 0 else 0.0
        uplift = (recent_avg - (season_avg or 0.0))
        return {
            "recent_avg_5": round(recent_avg, 3),
            "season_avg": round((season_avg or 0.0), 3),
            "avg_uplift": round(uplift, 3),
            "hot_score": round(max(0.0, uplift) * 100, 1),
        }

    # -------- public endpoints (existing) --------
    def hot_streak_hitters(self, *, date: date_cls, min_avg: float, games: int, require_hit_each: bool, debug: bool) -> Dict[str, Any]:
        year = date.year
        hit_rows, _ = self._sampled_roster_rows(date=date, max_teams=16, per_team=8)
        out_items: List[Dict[str, Any]] = []
        for h in hit_rows:
            season_avg = h.get("avg") or 0.0
            if season_avg < 0.250:
                continue
            pid = h["player_id"]
            try:
                logs = self._player_game_logs(pid, year, "hitting", limit=30)
            except Exception as e:
                self._log("hot logs error:", pid, e)
                logs = []
            hot = self._hot_hitter_score(season_avg, logs)
            if hot["avg_uplift"] <= 0.0:
                continue
            out_items.append({**h, **hot})
        out_items.sort(key=lambda x: (x.get("hot_score") or 0.0, x.get("recent_avg_5") or 0.0), reverse=True)
        result: Dict[str, Any] = {"items": out_items}
        if debug:
            result["debug"] = {"note": "Hot = recent 5G AVG above season AVG", "count": len(out_items)}
        return result

    def cold_streak_hitters(self, *, date: date_cls, min_avg: float, games: int, require_zero_hit_each: bool, debug: bool) -> Dict[str, Any]:
        year = date.year
        hit_rows, _ = self._sampled_roster_rows(date=date, max_teams=16, per_team=8)
        out_items: List[Dict[str, Any]] = []
        for h in hit_rows:
            season_avg = h.get("avg") or 0.0
            pa = h.get("pa") or 0
            if season_avg < 0.270 or pa < 50:
                continue
            pid = h["player_id"]
            try:
                logs = self._player_game_logs(pid, year, "hitting", limit=30)
            except Exception as e:
                self._log("cold logs error:", pid, e)
                logs = []
            cold = self._cold_hitter_score(season_avg, logs)
            if cold["current_hitless_streak"] <= 0:
                continue
            out_items.append({**h, **cold, "last5_hits": [(g.get("hits") or 0) for g in logs[-5:]]})
        out_items.sort(key=lambda x: (x.get("slump_score") or 0.0, x.get("avg") or 0.0), reverse=True)
        result: Dict[str, Any] = {"items": out_items}
        if debug:
            result["debug"] = {"note": "Cold = hitless streak weighted by rarity among good hitters", "count": len(out_items)}
        return result

    def pitcher_streaks(self, *, date: date_cls, hot_max_era: float, hot_min_ks_each: int, hot_last_starts: int,
                        cold_min_era: float, cold_min_runs_each: int, cold_last_starts: int, debug: bool) -> Dict[str, Any]:
        _, pit_rows = self._sampled_roster_rows(date=date, max_teams=16, per_team=8)
        hot = [p for p in pit_rows if (p.get("era") or 99.9) <= float(hot_max_era)]
        cold = [p for p in pit_rows if (p.get("era") or 0.0) >= float(cold_min_era)]
        out: Dict[str, Any] = {"hot": hot, "cold": cold}
        if debug:
            out["debug"] = {"note": "Heuristic = hot ERA ≤ hot_max_era; cold ERA ≥ cold_min_era"}
        return out

    # -------- NEW: full-league compact scan (for GPT) --------
    def league_scan(self, *, date: date_cls, top_n: int = 15, debug: bool = False) -> Dict[str, Any]:
        """
        Scan ALL scheduled MLB teams and ALL active hitters server-side,
        compute hot/cold via game logs and season baselines,
        and return COMPACT top-N lists with counts and matchups.
        """
        year = date.year
        teams = self._teams_playing_on(date)
        hitters_rows: List[Dict[str, Any]] = []
        pitchers_rows: List[Dict[str, Any]] = []

        # Pull full active rosters (hitters + pitchers) for every scheduled team
        for t in teams:
            roster = self._team_roster(t["id"])
            for r in roster:
                p = r.get("person") or {}
                pid = p.get("id")
                if not pid:
                    continue
                pos = (r.get("position") or {}).get("abbreviation", "")
                if pos == "P":
                    try:
                        stat = self._player_season_stats(pid, year, "pitching")
                    except Exception as e:
                        self._log("pitch season stat err:", pid, e); stat = {}
                    pitchers_rows.append({
                        "player_id": pid,
                        "player_name": p.get("fullName"),
                        "team_id": t["id"],
                        "team_name": t["name"],
                        "era": _safe_float(stat.get("era")),
                        "so": _safe_int(stat.get("strikeOuts")),
                        "whip": _safe_float(stat.get("whip")),
                        "gamesStarted": _safe_int(stat.get("gamesStarted")),
                    })
                else:
                    try:
                        stat = self._player_season_stats(pid, year, "hitting")
                    except Exception as e:
                        self._log("hit season stat err:", pid, e); stat = {}
                    hitters_rows.append({
                        "player_id": pid,
                        "player_name": p.get("fullName"),
                        "team_id": t["id"],
                        "team_name": t["name"],
                        "avg": _safe_float(stat.get("avg")),
                        "ops": _safe_float(stat.get("ops")),
                        "hr": _safe_int(stat.get("homeRuns")),
                        "rbi": _safe_int(stat.get("rbi")),
                        "pa": _safe_int(stat.get("plateAppearances")),
                        "gamesPlayed": _safe_int(stat.get("gamesPlayed")),
                    })

        # Build hot/cold hitters (game logs)
        hot_hitters: List[Dict[str, Any]] = []
        cold_hitters: List[Dict[str, Any]] = []
        for h in hitters_rows:
            pid = h["player_id"]
            season_avg = h.get("avg") or 0.0
            pa = h.get("pa") or 0
            try:
                logs = self._player_game_logs(pid, year, "hitting", limit=30)
            except Exception as e:
                self._log("league logs err:", pid, e)
                logs = []

            # HOT: recent 5 above season, baseline >= .250
            if season_avg >= 0.250:
                hot = self._hot_hitter_score(season_avg, logs)
                if hot["avg_uplift"] > 0.0:
                    hot_hitters.append({**h, **hot})

            # COLD: good hitter baseline and positive hitless streak
            if season_avg >= 0.270 and pa >= 50:
                cold = self._cold_hitter_score(season_avg, logs)
                if cold["current_hitless_streak"] > 0:
                    cold_hitters.append({**h, **cold})

        hot_hitters.sort(key=lambda x: (x.get("hot_score") or 0.0, x.get("recent_avg_5") or 0.0), reverse=True)
        cold_hitters.sort(key=lambda x: (x.get("slump_score") or 0.0, x.get("avg") or 0.0), reverse=True)

        # Pitchers (season heuristics; compact top-N too)
        hot_pitchers = [p for p in pitchers_rows if (p.get("era") or 99.9) <= 3.50]
        cold_pitchers = [p for p in pitchers_rows if (p.get("era") or 0.0) >= 4.60]
        hot_pitchers.sort(key=lambda x: (-(x.get("era") or 99.9), x.get("so") or 0.0))
        cold_pitchers.sort(key=lambda x: (x.get("era") or 0.0, -(x.get("so") or 0.0)))

        matchups = self._build_matchups(date)

        # COMPACT response: counts + top-N lists + matchups (not all rows)
        out: Dict[str, Any] = {
            "date": date.isoformat(),
            "counts": {
                "teams": len(teams),
                "hitters_total": len(hitters_rows),
                "pitchers_total": len(pitchers_rows),
                "hot_hitters": len(hot_hitters),
                "cold_hitters": len(cold_hitters),
                "hot_pitchers": len(hot_pitchers),
                "cold_pitchers": len(cold_pitchers),
                "matchups": len(matchups),
            },
            "top": {
                "hot_hitters": hot_hitters[:top_n],
                "cold_hitters": cold_hitters[:top_n],
                "hot_pitchers": hot_pitchers[:top_n],
                "cold_pitchers": cold_pitchers[:top_n],
            },
            "matchups": matchups,
        }
        if debug:
            out["debug"] = {
                "source": "statsapi",
                "base": self.base,
                "last_schedule_status": self._last_schedule_status,
                "error": self._last_error,
                "top_n": top_n,
            }
        return out

    # -------- diagnostics --------
    def debug_schedule(self, *, date: date_cls) -> Dict[str, Any]:
        payload = self._get("/api/v1/schedule", {"date": date.isoformat(), "sportId": 1, "hydrate": "probablePitcher"})
        dates = payload.get("dates") or []
        games = []
        for dblock in dates:
            games.extend(dblock.get("games", []) or [])
        teams_sample = []
        for g in games[:5]:
            tnode = g.get("teams") or {}
            home = ((tnode.get("home") or {}).get("team") or {}).get("name")
            away = ((tnode.get("away") or {}).get("team") or {}).get("name")
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
