import requests
from datetime import datetime, timezone
from functools import lru_cache

class StatsApiProvider:
    BASE_URL = "https://statsapi.mlb.com/api/v1"

    # --------- HTTP ----------
    def _fetch(self, endpoint, params=None):
        url = f"{self.BASE_URL}/{endpoint}"
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json()

    # --------- Public API used by routes ----------
    def schedule_for_date(self, date_str):
        return self._fetch("schedule", params={"sportId": 1, "date": date_str})

    def league_hot_hitters(self, date=None, top_n=10):
        # Not implemented yet
        return []

    def cold_candidates(
        self,
        date="today",
        min_season_avg=0.26,
        last_n=7,
        min_hitless_games=3,
        limit=30,
        verify=1,
        debug=0,
    ):
        """
        League-wide cold pool with STRICT slump definition:
        - Only count consecutive games with AB>0, H==0
        - Ignore non-regular-season games (gameType != 'R')
        - Only include hitters with season AVG >= min_season_avg
        - Up to `limit` results sorted by longest streak desc
        """
        as_of = self._resolve_date(date)
        season = as_of.year

        # Build league player list (active roster hitters)
        teams = self._teams_mlb()
        players = []
        for t in teams:
            tid = t.get("id")
            roster = self._team_roster_active(tid)
            for r in roster:
                p = r.get("person", {})
                pos = r.get("position", {}).get("abbreviation", "")
                # Filter to likely hitters (exclude Ps); MLB uses "P" for pitchers
                if pos == "P":
                    continue
                players.append(
                    {
                        "playerId": p.get("id"),
                        "playerFullName": p.get("fullName"),
                        "primaryPosition": pos,
                        "teamId": tid,
                        "teamName": t.get("name"),
                    }
                )

        results = []
        for p in players:
            pid = p["playerId"]
            # Quick season AVG screen
            season_avg = self._player_season_avg(pid, season)
            if season_avg is None or season_avg < float(min_season_avg):
                continue

            # Game logs (last ~30 to be safe; streak calc itself will use last_n)
            logs = self._player_game_logs(pid, season, as_of)
            streak = self._compute_hitless_streak_from_gamelog(logs, last_n)

            if streak >= int(min_hitless_games):
                row = {
                    "playerId": pid,
                    "name": p["playerFullName"],
                    "teamId": p["teamId"],
                    "teamName": p["teamName"],
                    "season": season,
                    "asOfDate": as_of.strftime("%Y-%m-%d"),
                    "seasonAVG": round(season_avg, 3),
                    "hitlessStreak": streak,
                    "lastNConsidered": int(last_n),
                }
                if debug:
                    row["debugSample"] = logs[:last_n]
                results.append(row)

            if len(results) >= int(limit):
                break

        # Sort by streak desc, then by season AVG desc (tie-breaker)
        results.sort(key=lambda r: (r["hitlessStreak"], r["seasonAVG"]), reverse=True)
        return results

    # --------- Helpers ----------
    def _resolve_date(self, date_str):
        if not date_str or date_str == "today":
            return datetime.now(timezone.utc).astimezone().date()
        # Accept YYYY-MM-DD
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    @lru_cache(maxsize=1)
    def _teams_mlb(self):
        data = self._fetch("teams", params={"sportId": 1, "activeStatus": "yes"})
        return data.get("teams", [])

    @lru_cache(maxsize=256)
    def _team_roster_active(self, team_id):
        data = self._fetch(f"teams/{team_id}/roster", params={"rosterType": "active"})
        return data.get("roster", [])

    @lru_cache(maxsize=4096)
    def _player_season_avg(self, player_id, season):
        # Hydrate season stats
        q = {
            "hydrate": f"stats(group=hitting,type=season,season={season})"
        }
        data = self._fetch(f"people/{player_id}", params=q)
        people = data.get("people", [])
        if not people:
            return None
        stats = (people[0].get("stats") or [])
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
    def _player_game_logs(self, player_id, season, as_of_date):
        """
        Uses stats hydrate: gameLog for the season; filters to games on/before as_of_date.
        Returns newest→oldest list of simplified rows for streak calc.
        """
        q = {
            "hydrate": f"stats(group=hitting,type=gameLog,season={season})"
        }
        data = self._fetch(f"people/{player_id}", params=q)
        people = data.get("people", [])
        if not people:
            return []

        stats = (people[0].get("stats") or [])
        if not stats:
            return []

        # Find the gameLog entry
        game_log_entry = None
        for s in stats:
            if s.get("type", {}).get("displayName", "").lower() == "gamelog" or s.get("type", {}).get("type") == "gameLog":
                game_log_entry = s
                break
        if not game_log_entry:
            return []

        splits = (game_log_entry.get("splits") or [])
        rows = []
        for sp in splits:
            game = sp.get("game", {})  # has gameDate and type
            gdate_raw = game.get("gameDate")  # ISO string
            if not gdate_raw:
                continue
            try:
                gdate = datetime.fromisoformat(gdate_raw.replace("Z", "+00:00")).date()
            except Exception:
                continue
            if gdate > as_of_date:
                continue

            stat = sp.get("stat", {})
            ab = int(stat.get("atBats") or 0)
            h = int(stat.get("hits") or 0)
            gtype = (game.get("type") or "").upper()
            rows.append(
                {
                    "gameDate": gdate_raw,
                    "gameType": gtype,
                    "AB": ab,
                    "H": h,
                    "opponent": sp.get("team", {}).get("name"),
                }
            )

        # newest → oldest
        rows.sort(key=lambda r: r["gameDate"], reverse=True)
        return rows

    def _compute_hitless_streak_from_gamelog(self, logs, last_n):
        """
        Logs are newest→oldest. Count consecutive H==0 for games with AB>0 and type 'R'.
        Stop on first game with a hit. Consider only last_n regular-season games in the scan window.
        """
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
                # ignored; does not advance 'considered'
                continue
            considered += 1
            if h == 0:
                streak += 1
            else:
                break
        return streak
