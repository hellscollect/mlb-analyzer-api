import requests
from datetime import datetime, timedelta
import math
import os

DATA_API_BASE = os.getenv("DATA_API_BASE", "https://statsapi.mlb.com")
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SEC", "20"))
STATS_DEBUG = int(os.getenv("STATS_DEBUG", "0"))

class StatsApiProvider:
    def __init__(self):
        pass

    def _get(self, path, params=None):
        url = f"{DATA_API_BASE}{path}"
        if STATS_DEBUG:
            print(f"DEBUG: GET {url} params={params}")
        resp = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def schedule_for_date(self, date_str):
        data = self._get(f"/api/v1/schedule", {
            "sportId": 1,
            "date": date_str
        })
        games = []
        for date in data.get("dates", []):
            for game in date.get("games", []):
                away = game["teams"]["away"]["team"]["name"]
                home = game["teams"]["home"]["team"]["name"]
                venue = game.get("venue", {}).get("name", "")
                probables = {
                    "away_pitcher": game["teams"]["away"].get("probablePitcher", {}).get("fullName", ""),
                    "home_pitcher": game["teams"]["home"].get("probablePitcher", {}).get("fullName", "")
                }
                games.append({
                    "away": away,
                    "home": home,
                    "et_time": game.get("gameDate", ""),
                    "venue": venue,
                    "probables": probables
                })
        return games

    def league_hot_hitters(self, last_n=5):
        # placeholder implementation
        return []

    def league_cold_hitters(self, last_n=5):
        # placeholder implementation
        return []

    def cold_candidates(self, date_str, min_season_avg=0.26, last_n=7, min_hitless_games=3, limit=30, verify=True):
        # Step 1: Get all players & stats from MLB API
        season_year = datetime.strptime(date_str, "%Y-%m-%d").year
        standings = self._get(f"/api/v1/stats/leaders", {
            "leaderCategories": "battingAverage",
            "season": season_year,
            "sportId": 1,
            "limit": 5000
        })
        players = []
        for row in standings.get("leagueLeaders", [])[0].get("leaders", []):
            player = row.get("person", {})
            stats = row.get("value", 0)
            team = row.get("team", {}).get("name", "")
            players.append({
                "id": player.get("id"),
                "name": player.get("fullName"),
                "team": team,
                "season_avg": float(stats)
            })

        # Step 2: Filter by season avg
        players = [p for p in players if p["season_avg"] >= min_season_avg]

        results = []
        for p in players:
            hitless_streak = self._calculate_hitless_streak(p["id"], date_str)

            # Debug trace for Carlos Correa
            if p["name"].lower() == "carlos correa":
                print(f"TRACE_CORREA: checking games for {p['name']} ({p['team']}) as of {date_str}")
                game_logs = self._get(f"/api/v1/people/{p['id']}/stats/game", {
                    "stats": "gameLog",
                    "season": season_year
                })
                for split in game_logs.get("stats", [])[0].get("splits", []):
                    game_date = split.get("date", "")
                    ab = split.get("stat", {}).get("atBats", 0)
                    hits = split.get("stat", {}).get("hits", 0)
                    print(f"TRACE_CORREA: {game_date} AB={ab} H={hits}")

            if hitless_streak >= min_hitless_games:
                rarity_index = self._calculate_rarity_index(hitless_streak, p["season_avg"])
                slump_score = rarity_index * 27.8  # arbitrary weight for now
                results.append({
                    "player_name": p["name"],
                    "team_name": p["team"],
                    "season_avg": p["season_avg"],
                    "current_hitless_streak": hitless_streak,
                    "avg_hitless_run": self._estimate_avg_hitless_run(p["season_avg"]),
                    "rarity_index": rarity_index,
                    "slump_score": round(slump_score, 1)
                })

        results.sort(key=lambda x: x["slump_score"], reverse=True)
        return results[:limit]

    def _calculate_hitless_streak(self, player_id, date_str):
        # Walk backwards in game logs until a hit is found, skip games with AB=0
        season_year = datetime.strptime(date_str, "%Y-%m-%d").year
        data = self._get(f"/api/v1/people/{player_id}/stats/game", {
            "stats": "gameLog",
            "season": season_year
        })
        streak = 0
        for split in data.get("stats", [])[0].get("splits", []):
            ab = split.get("stat", {}).get("atBats", 0)
            hits = split.get("stat", {}).get("hits", 0)
            if ab == 0:
                continue
            if hits == 0:
                streak += 1
            else:
                break
        return streak

    def _calculate_rarity_index(self, hitless_streak, season_avg):
        if season_avg == 0:
            return 0
        expected_prob = (1 - season_avg) ** hitless_streak
        rarity = -math.log(expected_prob + 1e-9)
        return rarity

    def _estimate_avg_hitless_run(self, season_avg):
        if season_avg == 0:
            return 0
        return round((1 - season_avg) / season_avg, 3)
