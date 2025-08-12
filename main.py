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
    # include probablePitcher for both hitter and pitcher endpoints
    return _get(f"{MLB_API}/schedule", {
        "sportId": 1,
        "date": date_str,
        "hydrate": "probablePitcher"
    })


def get_team_roster(team_id):
    data = _get(f"{MLB_API}/teams/{team_id}/roster")
    return data.get("roster", [])


def get_person_name(person_id):
    data = _get(f"{MLB_API}/people/{person_id}")
    try:
        return data["people"][0]["fullName"]
    except Exception:
        return str(person_id)


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


def get_pitcher_season_era(player_id, season=None):
    if season is None:
        season = datetime.now(ZoneInfo("America/New_York")).year
    data = _get(f"{MLB_API}/people/{player_id}/stats", {
        "stats": "season",
        "group": "pitching",
        "season": season
    })
    try:
        splits = data["stats"][0]["splits"]
        if not splits:
            return None
        era_str = splits[0]["stat"].get("era")
        if not era_str or era_str in ("", "--"):
            return None
        return float(era_str)
    except Exception:
        return None


# ---- date parsing helper for game logs ----
def _parse_ymd(dstr: str):
    """Parse 'YYYY-MM-DD' into a date object."""
    return datetime.strptime(dstr, "%Y-%m-%d").date()


# ---- hitting game logs (respect as_of_date and sort newest->oldest) ----
def get_player_last_n_games(player_id, n=2, season=None, as_of_date=None):
    if season is None:
        season = datetime.now(ZoneInfo("America/New_York")).year
    data = _get(f"{MLB_API}/people/{player_id}/stats", {
        "stats": "gameLog",
        "group": "hitting",
        "season": season
    })
    try:
        splits = data["stats"][0]["splits"]
    except Exception:
        return []

    if as_of_date:
        cutoff = _parse_ymd(as_of_date)
        splits = [s for s in splits if _parse_ymd(s.get("date")) <= cutoff]

    splits.sort(key=lambda s: s.get("date", ""), reverse=True)
    return splits[:max(0, n)]


# ---- pitching game logs (starts only; respect as_of_date and sort newest->oldest) ----
def get_pitcher_last_n_starts(player_id, n=2, season=None, as_of_date=None):
    if season is None:
        season = datetime.now(ZoneInfo("America/New_York")).year
    data = _get(f"{MLB_API}/people/{player_id}/stats", {
        "stats": "gameLog",
        "group": "pitching",
        "season": season
    })
    try:
        splits = data["stats"][0]["splits"]
    except Exception:
        return []

    # keep only starts
    starts = [s for s in splits if int(s.get("stat", {}).get("gamesStarted", 0)) == 1]

    if as_of_date:
        cutoff = _parse_ymd(as_of_date)
        starts = [s for s in starts if _parse_ymd(s.get("date")) <= cutoff]

    starts.sort(key=lambda s: s.get("date", ""), reverse=True)
    return starts[:max(0, n)]


def is_hitless(game_splits):
    if not game_splits:
        return False
    return all(int(g.get("stat", {}).get("hits", 0)) == 0 for g in game_splits)


def is_hit_in_each(game_splits):
    if not game_splits:
        return False
    return all(int(g.get("stat", {}).get("hits", 0)) >= 1 for g in game_splits)


# ---------------------------
# Routes
# ---------------------------

@app.get("/")
def root():
    return jsonify({"ok": True, "message": "API is running", "today_et": today_str_et()})


@app.get("/health")
def health():
    return jsonify({"ok": True})


def _opposing_probable_pitcher_id(game_obj, side):
    """Return the opposing team's probable pitcher id if present."""
    opp_side = "home" if side == "away" else "away"
    opp = game_obj.get("teams", {}).get(opp_side, {})
    pp = opp.get("probablePitcher") or {}
    return pp.get("id")


def _team_probable_pitcher_id(game_obj, side):
    """Return this side's probable pitcher id if present."""
    t = game_obj.get("teams", {}).get(side, {})
    pp = t.get("probablePitcher") or {}
    return pp.get("id")


def _passes_pitcher_era_filter(game_obj, side, min_pitcher_era, season_year):
    if min_pitcher_era is None:
        return True
    pid = _opposing_probable_pitcher_id(game_obj, side)
    if not pid:
        return False  # if ERA filter requested but no probable pitcher, skip
    era = get_pitcher_season_era(pid, season_year)
    return (era is not None) and (era >= min_pitcher_era)


@app.get("/cold_streak_hitters")
def cold_streak_hitters():
    """
    Hitters with season AVG >= min_avg and 0 hits in each of last_n games as-of 'date'.
    Query params:
      - date (YYYY-MM-DD, default today ET)
      - min_avg (default 0.275)
      - last_n (default 2)
      - min_pitcher_era (optional: opposing probable pitcher ERA must be >= this)
      - debug=1 to include counters
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
    mpe_raw = request.args.get("min_pitcher_era")
    min_pitcher_era = float(mpe_raw) if mpe_raw not in (None, "",) else None
    debug = request.args.get("debug") == "1"

    counters = {
        "games_total": 0, "teams_total": 0, "roster_players_total": 0,
        "non_pitchers": 0, "season_stat_available": 0, "passed_avg": 0,
        "had_recent_gamelogs": 0, "passed_hitless": 0, "passed_pitcher_era": 0
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

            if not _passes_pitcher_era_filter(g, side, min_pitcher_era, season_year):
                continue
            counters["passed_pitcher_era"] += 1

            roster = get_team_roster(team_id)
            counters["roster_players_total"] += len(roster)

            for p in roster:
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

                glast = get_player_last_n_games(pid, n=last_n, season=season_year, as_of_date=date_str)
                if glast:
                    counters["had_recent_gamelogs"] += 1
                if not is_hitless(glast):
                    continue
                counters["passed_hitless"] += 1

                results.append({
                    "player": pname, "team": team_name, "avg": avg,
                    "recent_games": [
                        {"date": gi.get("date"), "hits": int(gi.get("stat", {}).get("hits", 0))}
                        for gi in glast
                    ]
                })

    if debug:
        return jsonify({"date": date_str, "min_avg": min_avg, "last_n": last_n,
                        "min_pitcher_era": min_pitcher_era,
                        "counters": counters, "results": results})
    return jsonify(results)


@app.get("/hot_streak_hitters")
def hot_streak_hitters():
    """
    Hitters with season AVG >= min_avg and >=1 hit in each of last_n games as-of 'date'.
    Query params:
      - date (YYYY-MM-DD, default today ET)
      - min_avg (default 0.275)
      - last_n (default 2)
      - min_pitcher_era (optional: opposing probable pitcher ERA must be >= this)
      - debug=1 to include counters
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
    mpe_raw = request.args.get("min_pitcher_era")
    min_pitcher_era = float(mpe_raw) if mpe_raw not in (None, "",) else None
    debug = request.args.get("debug") == "1"

    counters = {
        "games_total": 0, "teams_total": 0, "roster_players_total": 0,
        "non_pitchers": 0, "season_stat_available": 0, "passed_avg": 0,
        "had_recent_gamelogs": 0, "passed_hot": 0, "passed_pitcher_era": 0
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

            if not _passes_pitcher_era_filter(g, side, min_pitcher_era, season_year):
                continue
            counters["passed_pitcher_era"] += 1

            roster = get_team_roster(team_id)
            counters["roster_players_total"] += len(roster)

            for p in roster:
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

                glast = get_player_last_n_games(pid, n=last_n, season=season_year, as_of_date=date_str)
                if glast:
                    counters["had_recent_gamelogs"] += 1
                if not is_hit_in_each(glast):
                    continue
                counters["passed_hot"] += 1

                results.append({
                    "player": pname, "team": team_name, "avg": avg,
                    "recent_games": [
                        {"date": gi.get("date"), "hits": int(gi.get("stat", {}).get("hits", 0))}
                        for gi in glast
                    ]
                })

    if debug:
        return jsonify({"date": date_str, "min_avg": min_avg, "last_n": last_n,
                        "min_pitcher_era": min_pitcher_era,
                        "counters": counters, "results": results})
    return jsonify(results)


@app.get("/pitcher_streaks")
def pitcher_streaks():
    """
    Probable starters matching ERA and K filters over their last N starts as-of 'date'.
    Query params:
      - date (YYYY-MM-DD, default today ET)
      - max_era (optional: require season ERA <= this value)
      - min_strikeouts (optional int: each start must have >= this K)
      - last_n (default 2)
      - debug=1 to include counters
    """
    date_str = request.args.get("date") or today_str_et()
    as_of = date_str
    try:
        last_n = int(request.args.get("last_n", 2))
    except Exception:
        last_n = 2
    me_raw = request.args.get("max_era")
    max_era = float(me_raw) if me_raw not in (None, "",) else None
    try:
        min_k = int(request.args.get("min_strikeouts", 0))
    except Exception:
        min_k = 0
    debug = request.args.get("debug") == "1"

    counters = {
        "games_total": 0, "probables_checked": 0, "season_era_available": 0,
        "passed_season_era": 0, "had_recent_starts": 0, "passed_min_k": 0
    }

    sched = get_schedule(date_str)
    games = [g for d in sched.get("dates", []) for g in d.get("games", [])]
    counters["games_total"] = len(games)

    results = []
    season_year = datetime.now(ZoneInfo("America/New_York")).year

    for g in games:
        for side in ("home", "away"):
            pid = _team_probable_pitcher_id(g, side)
            if not pid:
                continue
            counters["probables_checked"] += 1

            era = get_pitcher_season_era(pid, season_year)
            if era is None:
                continue
            counters["season_era_available"] += 1
            if (max_era is not None) and (era > max_era):
                continue
            counters["passed_season_era"] += 1

            starts = get_pitcher_last_n_starts(pid, n=last_n, season=season_year, as_of_date=as_of)
            if starts:
                counters["had_recent_starts"] += 1

            # Must have at least last_n starts to evaluate strictly
            if len(starts) < max(1, last_n):
                continue

            # Min K per start (if requested)
            if min_k > 0:
                if not all(int(s.get("stat", {}).get("strikeOuts", 0)) >= min_k for s in starts):
                    continue
                counters["passed_min_k"] += 1

            # Build output
            team_name = g.get("teams", {}).get(side, {}).get("team", {}).get("name")
            pname = get_person_name(pid)
            recent = []
            for s in starts:
                stat = s.get("stat", {})
                recent.append({
                    "date": s.get("date"),
                    "inningsPitched": stat.get("inningsPitched"),
                    "strikeOuts": int(stat.get("strikeOuts", 0)),
                    "runs": int(stat.get("runs", stat.get("earnedRuns", 0)))
                })

            results.append({
                "player": pname, "team": team_name, "era": era,
                "recent_starts": recent
            })

    if debug:
        return jsonify({"date": date_str, "last_n": last_n, "max_era": max_era,
                        "min_strikeouts": min_k, "counters": counters, "results": results})
    return jsonify(results)


if __name__ == "__main__":
    # Render sets $PORT; default to 10000 if not present.
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
