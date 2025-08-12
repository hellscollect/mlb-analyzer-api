from flask import Flask, jsonify, request
import requests
from datetime import datetime

# ZoneInfo fallback for older Python versions
try:
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:
    from backports.zoneinfo import ZoneInfo  # type: ignore

app = Flask(__name__)

MLB_API = "https://statsapi.mlb.com/api/v1"

def today_str_et():
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

def get_schedule(date_str):
    url = f"{MLB_API}/schedule"
    params = {"sportId": 1, "date": date_str, "hydrate": "probablePitcher"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def get_team_roster(team_id):
    url = f"{MLB_API}/teams/{team_id}/roster"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json().get("roster", [])

def get_player_season_avg(player_id, season=None):
    if season is None:
        season = datetime.now(ZoneInfo("America/New_York")).year
    url = f"{MLB_API}/people/{player_id}/stats"
    params = {"stats": "season", "group": "hitting", "season": season}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    try:
        splits = data["stats"][0]["splits"]
        if not splits:
            return None
        avg_str = splits[0]["stat"]["avg"]
        if not avg_str or avg_str == ".---":
            return None
        return float(avg_str)
    except Exception:
        return None

def get_player_last_n_games(player_id, n=2, season=None):
    if season is None:
        season = datetime.now(ZoneInfo("America/New_York")).year
    url = f"{MLB_API}/people/{player_id}/stats"
    params = {"stats": "gameLog", "group": "hitting", "season": season}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    try:
        return data["stats"][0]["splits"][:n]
    except Exception:
        return []

def is_hitless(game_splits):
    if not game_splits:
        return False
    return all(int(g["stat"].get("hits", 0)) == 0 for g in game_splits)

@app.get("/")
def root():
    return jsonify({"ok": True, "message": "API is running", "today_et": today_str_et()})

@app.get("/health")
def health():
    return jsonify({"ok": True})

@app.get("/cold_streak_hitters")
def cold_streak_hitters():
    """
    Optional query params:
      - date: YYYY-MM-DD (default: today ET)
      - min_avg: float (default: 0.275)
      - last_n: int (default: 2)
    """
    date_str = request.args.get("date") or today_str_et()
    min_avg = float(request.args.get("min_avg", 0.275))
    last_n = int(request.args.get("last_n", 2))

    sched = get_schedule(date_str)
    games = [g for d in sched.get("dates", []) for g in d.get("games", [])]

    results = []
    season_year = datetime.now(ZoneInfo("America/New_York")).year

    for g in games:
        teams = g.get("teams", {})
        for side in ("home", "away"):
            t = teams.get(side, {})
            team = t.get("team", {})
            team_id = team.get("id")
            team_name = team.get("name")
            if not team_id:
                continue

            roster = get_team_roster(team_id)
            for p in roster:
                pos_type = p.get("position", {}).get("type", "")
if pos_type == "Pitcher":
    continue
                pid = p.get("person", {}).get("id")
                pname = p.get("person", {}).get("fullName")
                if not pid:
                    continue

                avg = get_player_season_avg(pid, season_year)
                if avg is None or avg < min_avg:
                    continue

                glast = get_player_last_n_games(pid, n=last_n, season=season_year)
                if not is_hitless(glast):
                    continue

                results.append({
                    "player": pname,
                    "team": team_name,
                    "avg": avg,
                    "recent_games": [{"date": gi["date"], "hits": int(gi["stat"].get("hits", 0))} for gi in glast]
                })

    return jsonify(results)

if __name__ == "__main__":
    # On Render, you typically bind to 0.0.0.0 and a provided $PORT
    import os
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
