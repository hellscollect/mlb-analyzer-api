from flask import Flask, jsonify, request
import requests
from datetime import datetime
import os

# ZoneInfo fallback for older Python versions
try:
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore

app = Flask(__name__)

MLB_API = "https://statsapi.mlb.com/api/v1"


# ---------------------------
# Helpers
# ---------------------------

def today_str_et():
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")


def _get(url, params=None, timeout=30):
    r = requests.get(url, params=params or {}, timeout=timeout)
    r.raise_for_status()
    return r.json()


def get_schedule(date_str):
    # include probablePitcher just for context/possible future use
    return _get(f"{MLB_API}/schedule", {
        "sportId": 1,
        "date": date_str,
        "hydrate": "probablePitcher"
    })


def get_team_roster(team_id):
    data = _get(f"{MLB_API}/teams/{team_id}/roster")
    return data.get("roster", [])


def get_player_season_avg(player_id, season=None):
    if season is None:
        season = datetime.now(ZoneInfo("America/New_York")).year
    data = _get(f"{MLB_API}/people/{player_id}/stats", {
        "stats": "season",
        "group": "hitting",
        "season": season
    })
    try:
        splits = data["stats"][0]["splits"]
        if not splits:
            return None
        avg_str = splits[0]["stat"].get("avg")
        if not avg_str or avg_str in (".---",):
            return None
        return float(avg_str)
    except Exception:
        return None


def get_player_last_n_games(player_id, n=2, season=None):
    if season is None:
        season = datetime.now(ZoneInfo("America/New_York")).year
    data = _get(f"{MLB_API}/people/{player_id}/stats", {
        "stats": "gameLog",
        "group": "hitting",
        "season": season
    })
    try:
        splits = data["stats"][0]["splits"]
        return splits[:max(0, n)]
    except Exception:
        return []


def is_hitless(game_splits):
    """True if 0 hits in each game in the provided list (non-empty)."""
    if not game_splits:
        return False
    for g in game_splits:
        hits = int(g.get("stat", {}).get("hits", 0))
        if hits != 0:
            return False
    return True


# ---------------------------
# Routes
# ---------------------------

@app.get("/")
def root():
    return jsonify({"ok": True, "message": "API is running", "today_et": today_str_et()})


@app.get("/health")
def health():
    return jsonify({"ok": True})


@app.get("/cold_streak_hitters")
def cold_streak_hitters():
    """
    Identify hitters who meet:
      - season AVG >= min_avg (default: 0.275)
      - hitless in last_n most recent games (default: 2)
      - playing on 'date' (default: today ET)

    Query params:
      - date: YYYY-MM-DD (optional, default: today ET)
      - min_avg: float (optional, default: 0.275)
      - last_n: int   (optional, default: 2)
      - debug: 1 to return counters/diagnostics
    """
    date_str = request.args.get("date") or today_str_et()
    try:
        min_avg = float(request.args.get("min_avg", 0.275))
    except Exception:
        min_avg = 0.275
    try:
        last_n = int(request.args.get("last_n", 2))
    except Exception:
        last_n = 2
    debug = request.args.get("debug") == "1"

    # counters for debug
    counters = {
        "games_total": 0,
        "teams_total": 0,
        "roster_players_total": 0,
        "non_pitchers": 0,
        "season_stat_available": 0,
        "passed_avg": 0,
        "had_recent_gamelogs": 0,
        "passed_hitless": 0
    }

    sched = get_schedule(date_str)
    games = [g for d in sched.get("dates", []) for g in d.get("games", [])]
    counters["games_total"] = len(games)

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
            counters["teams_total"] += 1

            roster = get_team_roster(team_id)
            counters["roster_players_total"] += len(roster)

            for p in roster:
                # Include ALL non-pitchers: Catcher/Infielder/Outfielder etc.
                pos_type = p.get("position", {}).get("type", "")
                if pos_type == "Pitcher":
                    continue
                counters["non_pitchers"] += 1

                pid = p.get("person", {}).get("id")
                pname = p.get("person", {}).get("fullName")
                if not pid:
                    continue

                avg = get_player_season_avg(pid, season_year)
                if avg is None:
                    continue
                counters["season_stat_available"] += 1

                if avg < min_avg:
                    continue
                counters["passed_avg"] += 1

                glast = get_player_last_n_games(pid, n=last_n, season=season_year)
                if glast:
                    counters["had_recent_gamelogs"] += 1
                if not is_hitless(glast):
                    continue
                counters["passed_hitless"] += 1

                results.append({
                    "player": pname,
                    "team": team_name,
                    "avg": avg,
                    "recent_games": [
                        {"date": gi.get("date"), "hits": int(gi.get("stat", {}).get("hits", 0))}
                        for gi in glast
                    ]
                })

    if debug:
        return jsonify({"date": date_str, "min_avg": min_avg, "last_n": last_n,
                        "counters": counters, "results": results})

    return jsonify(results)


if __name__ == "__main__":
    # Render sets $PORT; default to 10000 if not present.
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
