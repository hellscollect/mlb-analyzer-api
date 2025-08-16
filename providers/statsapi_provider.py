import requests
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional

class StatsApiProvider:
    """
    Business logic lives here.
    - schedule_for_date
    - cold_candidates (STRICT: only AB>0 & H==0, regular season)
    - league_hot_hitters / league_cold_hitters (placeholders)
    - pitcher_streaks (empty lists)
    - cold_pitchers (empty list)
    - _fetch_hitter_rows / _fetch_pitcher_rows (stubs for /provider_raw probes)
    """
    BASE_URL = "https://statsapi.mlb.com/api/v1"
    base = "mlb-statsapi"
    key = None

    # ------------------------
    # HTTP helper
    # ------------------------
    def _fetch(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.BASE_URL}/{endpoint}"
        r = requests.get(url, params=params, timeout=25)
        r.raise_for_status()
        return r.json()

    # ------------------------
    # Public methods used by your API
    # ------------------------
    def schedule_for_date(self, date: str) -> Dict[str, Any]:
        return self._fetch("schedule", params={"sportId": 1, "date": date})

    def league_hot_hitters(self, date: Optional[str] = None, top_n: int = 10) -> List[Dict[str, Any]]:
        return []

    def league_cold_hitters(self, date: Optional[str] = None, top_n: int = 10) -> List[Dict[str, Any]]:
        return []

    def pitcher_streaks(
        self,
        date: Optional[str] = None,
        hot_max_era: float = 4.0,
        hot_min_ks_each: int = 6,
        hot_last_starts: int = 3,
        cold_min_era: float = 4.6,
        cold_min_runs_each: int = 3,
        cold_last_starts: int = 2,
        debug: bool = False,
    ) -> Dict[str, Any]:
        return {"hot_pitchers": [], "cold_pitchers": [], "debug": {"note": "not implemented"}}

    def cold_pitchers(
        self,
        date: Optional[str] = None,
        min_era: float = 4.6,
        min_runs_each: int = 3,
        last_starts: int = 2,
        debug: bool = False,
    ) -> List[Dict[str, Any]]:
        return []

    # ------------------------
    # STRICT cold candidate scan
    # ------------------------
    def cold_candidates(
        self,
        date: str = "today",
        min_season_avg: float = 0.26,
        last_n: int = 7,
        min_hitless_games: int = 3,
        limit: int = 30,
        verify: int = 1,   # reserved; future boxscore re-check
        debug: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        League-wide scan for cold hitters with strict slump rules:
        - Only regular-season games (type 'R')
        - Count consecutive games with AB > 0 and H == 0
        - Ignore games with AB == 0
        - Min season AVG filter
        - Consider up to `last_n` qualifying games (AB>0 & 'R'), newest→oldest
        """
        as_of = self._resolve_date(date)
        season = as_of.year

        results: List[Dict[str, Any]] = []
        for team in self._teams_mlb():
            tid = team.get("id")
            tname = team.get("name")
            for r in self._team_roster_active(tid):
                pos = (r.get("position") or {}).get("abbreviation", "")
                if pos == "P":
                    continue
                person = r.get("person") or {}
                pid = person.get("id")
                pname = person.get("fullName")

                s_avg = self._player_season_avg(pid, season)
                if s_avg is None or s_avg < float(min_season_avg):
                    continue

                logs = self._player_game_logs(pid, season, as_of)
                streak = self._compute_hitless_streak_from_gamelog(logs, last_n)

                if streak >= int(min_hitless_games):
                    row: Dict[str, Any] = {
                        "playerId": pid,
                        "name": pname,
                        "teamId": tid,
                        "teamName": tname,
                        "season": season,
                        "asOfDate": as_of.isoformat(),
                        "seasonAVG": round(float(s_avg), 3),
                        "hitlessStreak": int(streak),
                        "lastNConsidered": int(last_n),
                    }
                    if debug:
                        row["debugSample"] = logs[:max(1, min(int(last_n), 5))]
                    results.append(row)

                if len(results) >= int(limit):
                    break
            if len(results) >= int(limit):
                break

        results.sort(key=lambda r: (r["hitlessStreak"], r["seasonAVG"]), reverse=True)
        return results

    # ------------------------
    # Helpers
    # ------------------------
    def _resolve_date(self, date_str: Optional[str]):
        today = datetime.now(timezone.utc).astimezone().date()
        if not date_str or date_str.lower() == "today":
            return today
        if date_str.lower() == "yesterday":
            return today - timedelta(days=1)
        if date_str.lower() == "tomorrow":
            return today + timedelta(days=1)
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    @lru_cache(maxsize=1)
    def _teams_mlb(self) -> List[Dict[str, Any]]:
        data = self._fetch("teams", params={"sportId": 1, "activeStatus": "yes"})
        return data.get("teams", []) or []

    @lru_cache(maxsize=256)
    def _team_roster_active(self, team_id: int) -> List[Dict[str, Any]]:
        data = self._fetch(f"teams/{team_id}/roster", params={"rosterType": "active"})
        return data.get("roster", []) or []

    @lru_cache(maxsize=4096)
    def _player_season_avg(self, player_id: int, season: int) -> Optional[float]:
        q = {"hydrate": f"stats(group=hitting,type=season,season={season})"}
        data = self._fetch(f"people/{player_id}", params=q)
        ppl = data.get("people", []) or []
        if not ppl:
            return None
        stats = (ppl[0].get("stats") or [])
        if not stats:
            return None
        splits = (stats[0].get("splits") or [])
        if not splits:
            return None
        stat = splits[0].get("stat") or {}
        avg = stat.get("avg")
        try:
            return float(avg)
        except Exception:
            return None

    @lru_cache(maxsize=4096)
    def _player_game_logs(self, player_id: int, season: int, as_of_date) -> List[Dict[str, Any]]:
        """
        Fetch full-season game logs and trim to games on/before as_of_date.
        Return newest→oldest simplified rows for streak computation.
        """
        q = {"hydrate": f"stats(group=hitting,type=gameLog,season={season})"}
        data = self._fetch(f"people/{player_id}", params=q)
        ppl = data.get("people", []) or []
        if not ppl:
            return []
        stats = (ppl[0].get("stats") or [])
        game_log_entry = None
        for s in stats:
            t = s.get("type", {})
            if t.get("type") == "gameLog" or t.get("displayName", "").lower() == "gamelog":
                game_log_entry = s
                break
        if not game_log_entry:
            return []

        rows: List[Dict[str, Any]] = []
        for sp in (game_log_entry.get("splits") or []):
            game = sp.get("game", {}) or {}
            gdate_raw = game.get("gameDate")
            if not gdate_raw:
                continue
            try:
                gdate = datetime.fromisoformat(gdate_raw.replace("Z", "+00:00")).date()
            except Exception:
                continue
            if gdate > as_of_date:
                continue
            gtype = (game.get("type") or "").upper()
            stat = sp.get("stat", {}) or {}
            ab = int(stat.get("atBats") or 0)
            h = int(stat.get("hits") or 0)
            rows.append(
                {
                    "gameDate": gdate_raw,
                    "gameType": gtype,
                    "AB": ab,
                    "H": h,
                    "opponent": (sp.get("team") or {}).get("name"),
                }
            )

        rows.sort(key=lambda r: r["gameDate"], reverse=True)
        return rows

    def _compute_hitless_streak_from_gamelog(self, logs: List[Dict[str, Any]], last_n: int) -> int:
        streak = 0
        considered = 0
        for g in logs:
            if considered >= int(last_n):
                break
            if g.get("gameType") != "R":
                continue
            ab = int(g.get("AB") or 0)
            h = int(g.get("H") or 0)
            if ab == 0:
                continue
            considered += 1
            if h == 0:
                streak += 1
            else:
                break
        return streak

    # ------------------------
    # Private fetch stubs for /provider_raw probes in main.py
    # ------------------------
    def _fetch_hitter_rows(self, date, limit=None, team: Optional[str] = None) -> List[Dict[str, Any]]:
        return []

    def _fetch_pitcher_rows(self, date, limit=None, team: Optional[str] = None) -> List[Dict[str, Any]]:
        return []
