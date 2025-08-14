# providers/statsapi_provider.py
import requests
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from zoneinfo import ZoneInfo

BASE = "https://statsapi.mlb.com/api/v1"


def _log(msg: str) -> None:
    # Keep logs consistent with your Render logs
    print(f"[StatsApiProvider] {msg}", flush=True)


def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{BASE}{path}"
    _log(f"GET {url} params= {params}")
    r = requests.get(url, params=params or {}, timeout=30)
    _log(f"HTTP {r.status_code} for {url}")
    r.raise_for_status()
    return r.json()


def _season_from_date(date_str: str) -> int:
    # date_str is YYYY-MM-DD
    try:
        return int(date_str[:4])
    except Exception:
        return datetime.now().year


def _to_et_str(iso_dt: str) -> str:
    # StatsAPI gives ISO UTC ("Z"); convert to ET and pretty-print.
    try:
        dt = datetime.fromisoformat(iso_dt.replace("Z", "+00:00"))
        dt_et = dt.astimezone(ZoneInfo("America/New_York"))
        s = dt_et.strftime("%I:%M %p ET")
        return s.lstrip("0")
    except Exception:
        return ""


def _safe_float(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        if isinstance(v, str):
            v = v.strip()
            if v == "":
                return 0.0
        return float(v)
    except Exception:
        return 0.0


def _avg_from_stat(stat: Dict[str, Any]) -> float:
    """
    Prefer 'avg' if present; otherwise compute H/AB.
    """
    if not stat:
        return 0.0
    if "avg" in stat and stat["avg"] not in (None, "", ".---"):
        try:
            return float(stat["avg"])
        except Exception:
            # some APIs return ".250" as string; float(".250") works, but be safe:
            return _safe_float(stat["avg"])
    h = _safe_float(stat.get("hits"))
    ab = _safe_float(stat.get("atBats"))
    if ab > 0:
        return h / ab
    return 0.0


def _extract_season_avg(stats_json: Dict[str, Any]) -> float:
    try:
        splits = stats_json.get("stats", [])[0].get("splits", [])
        if not splits:
            return 0.0
        stat = splits[0].get("stat", {})
        return _avg_from_stat(stat)
    except Exception:
        return 0.0


def _extract_game_splits(stats_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        return stats_json.get("stats", [])[0].get("splits", [])
    except Exception:
        return []


def _recent_avg_from_gamelog(splits: List[Dict[str, Any]], n: int = 5) -> float:
    """
    Compute last-n games batting average (H/AB), skipping games with AB=0.
    """
    games: List[Tuple[int, int]] = []  # (H, AB)
    for sp in splits:
        stat = sp.get("stat", {})
        h = int(_safe_float(stat.get("hits")))
        ab = int(_safe_float(stat.get("atBats")))
        if ab > 0:
            games.append((h, ab))
    if not games:
        return 0.0
    # Most recent first in StatsAPI gameLog; just take the first n with AB>0
    games = games[:n]
    total_h = sum(h for h, _ in games)
    total_ab = sum(ab for _, ab in games)
    return (total_h / total_ab) if total_ab > 0 else 0.0


def _current_hitless_streak(splits: List[Dict[str, Any]]) -> int:
    """
    Count consecutive most-recent games with 0 hits (AB>0).
    """
    streak = 0
    for sp in splits:
        st = sp.get("stat", {})
        ab = int(_safe_float(st.get("atBats")))
        h = int(_safe_float(st.get("hits")))
        if ab == 0:
            # ignore games without AB
            continue
        if h == 0:
            streak += 1
        else:
            break
    return streak


def _avg_hitless_run(splits: List[Dict[str, Any]]) -> float:
    """
    Average length of hitless runs across the season, ignoring games with AB=0.
    """
    runs: List[int] = []
    cur = 0
    for sp in splits:
        st = sp.get("stat", {})
        ab = int(_safe_float(st.get("atBats")))
        h = int(_safe_float(st.get("hits")))
        if ab == 0:
            continue
        if h == 0:
            cur += 1
        else:
            if cur > 0:
                runs.append(cur)
            cur = 0
    if cur > 0:
        runs.append(cur)
    if not runs:
        return 0.0
    return sum(runs) / len(runs)


class StatsApiProvider:
    """
    Provider that:
      • Builds the slate and probables from /schedule
      • Scans hitters on active rosters for teams in the slate
      • Computes HOT and COLD lists with the exact fields your router expects
    """

    # ---------------- Schedule ----------------

    def schedule_for_date(self, date_str: str) -> List[Dict[str, Any]]:
        data = _get("/schedule", {
            "date": date_str,
            "sportId": 1,
            "hydrate": "probablePitcher"
        })

        out: List[Dict[str, Any]] = []
        dates = data.get("dates") or []
        if not dates:
            _log(f"0 games scheduled on {date_str}")
            return out

        games = dates[0].get("games", [])
        _log(f"{len(games) * 2} teams scheduled on {date_str}")

        for g in games:
            away_team = (((g.get("teams") or {}).get("away") or {}).get("team") or {}).get("name")
            home_team = (((g.get("teams") or {}).get("home") or {}).get("team") or {}).get("name")
            away_pitcher = (((g.get("teams") or {}).get("away") or {}).get("probablePitcher") or {}).get("fullName")
            home_pitcher = (((g.get("teams") or {}).get("home") or {}).get("probablePitcher") or {}).get("fullName")
            venue = (g.get("venue") or {}).get("name")
            game_date = g.get("gameDate") or ""
            et_time = _to_et_str(game_date) if game_date else ""

            out.append({
                "away": away_team or "",
                "home": home_team or "",
                "et_time": et_time,
                "venue": venue or "",
                "probables": {
                    "away_pitcher": away_pitcher or "",
                    "home_pitcher": home_pitcher or ""
                }
            })
        return out

    # ---------------- Hot / Cold ----------------

    def _teams_in_slate(self, date_str: str) -> List[Dict[str, Any]]:
        data = _get("/schedule", {
            "date": date_str,
            "sportId": 1
        })
        teams: Dict[int, str] = {}
        dates = data.get("dates") or []
        if not dates:
            return []
        for g in dates[0].get("games", []):
            away = (((g.get("teams") or {}).get("away") or {}).get("team") or {})
            home = (((g.get("teams") or {}).get("home") or {}).get("team") or {})
            if "id" in away and "name" in away:
                teams[away["id"]] = away["name"]
            if "id" in home and "name" in home:
                teams[home["id"]] = home["name"]
        return [{"id": k, "name": v} for k, v in teams.items()]

    def _active_roster(self, team_id: int) -> List[Dict[str, Any]]:
        js = _get(f"/teams/{team_id}/roster", {"rosterType": "active"})
        return js.get("roster") or []

    def _player_season_stats(self, player_id: int, season: int) -> Dict[str, Any]:
        return _get(f"/people/{player_id}/stats", {
            "stats": "season",
            "group": "hitting",
            "season": season
        })

    def _player_gamelog(self, player_id: int, season: int) -> Dict[str, Any]:
        return _get(f"/people/{player_id}/stats", {
            "stats": "gameLog",
            "group": "hitting",
            "season": season
        })

    def _scan_hitters_for_teams(self, date_str: str) -> List[Dict[str, Any]]:
        """
        Return a normalized list of hitter rows across all teams in the slate:
        {
          "player_id": int,
          "player_name": str,
          "team_name": str,
          "season_avg": float,
          "recent_avg_5": float,
          "hitless_streak": int,
          "avg_hitless_run": float
        }
        """
        season = _season_from_date(date_str)
        teams = self._teams_in_slate(date_str)
        hitters: List[Dict[str, Any]] = []

        for t in teams:
            team_id = t["id"]
            team_name = t["name"]

            roster = self._active_roster(team_id)
            for r in roster:
                try:
                    person = r.get("person") or {}
                    pid = person.get("id")
                    pname = person.get("fullName") or person.get("lastFirstName") or ""
                    pos = (r.get("position") or {}).get("abbreviation")
                    # Skip pitchers for hitter lists
                    if pos == "P":
                        continue
                    if not pid:
                        continue

                    # season
                    season_stats = self._player_season_stats(pid, season)
                    s_avg = _extract_season_avg(season_stats)

                    # gamelog
                    gamelog = self._player_gamelog(pid, season)
                    splits = _extract_game_splits(gamelog)
                    r5 = _recent_avg_from_gamelog(splits, 5)
                    cur0 = _current_hitless_streak(splits)
                    avg0 = _avg_hitless_run(splits)

                    hitters.append({
                        "player_id": pid,
                        "player_name": pname,
                        "team_name": team_name,
                        "season_avg": round(s_avg, 3),
                        "recent_avg_5": round(r5, 3),
                        "hitless_streak": int(cur0),
                        "avg_hitless_run": round(avg0, 3),
                    })
                except Exception as ex:
                    _log(f"scan_hitter_error:{type(ex).__name__}")
                    continue

        return hitters

    def league_hot_hitters(self, date_str: str, top_n: int) -> List[Dict[str, Any]]:
        """
        Return: list of dicts with keys:
          player_name, team_name, recent_avg_5, season_avg, avg_uplift
        """
        rows = self._scan_hitters_for_teams(date_str)
        # compute uplift
        out: List[Dict[str, Any]] = []
        for r in rows:
            uplift = float(r["recent_avg_5"]) - float(r["season_avg"])
            out.append({
                "player_name": r["player_name"],
                "team_name": r["team_name"],
                "recent_avg_5": round(float(r["recent_avg_5"]), 3),
                "season_avg": round(float(r["season_avg"]), 3),
                "avg_uplift": round(uplift, 3),
            })
        out.sort(key=lambda x: x["avg_uplift"], reverse=True)
        return out[:max(0, int(top_n))]

    def league_cold_hitters(self, date_str: str, top_n: int) -> List[Dict[str, Any]]:
        """
        Return: list of dicts with keys:
          player_name, team_name, season_avg, current_hitless_streak, avg_hitless_run, rarity_index, slump_score
        """
        rows = self._scan_hitters_for_teams(date_str)
        out: List[Dict[str, Any]] = []

        for r in rows:
            cur0 = int(r["hitless_streak"])
            avg0 = float(r["avg_hitless_run"])
            season_avg = float(r["season_avg"])
            recent = float(r["recent_avg_5"])

            rarity_index = (cur0 / avg0) if avg0 > 0 else float(cur0)
            # emphasize "true slump": underperforming vs season baseline
            delta = max(0.0, season_avg - recent)
            slump_score = rarity_index * (delta * 100.0)

            out.append({
                "player_name": r["player_name"],
                "team_name": r["team_name"],
                "season_avg": round(season_avg, 3),
                "current_hitless_streak": cur0,
                "avg_hitless_run": round(avg0, 3),
                "rarity_index": round(rarity_index, 3),
                "slump_score": round(slump_score, 1),
            })

        # Sort by slump_score desc, then current_hitless_streak desc
        out.sort(key=lambda x: (x["slump_score"], x["current_hitless_streak"]), reverse=True)
        return out[:max(0, int(top_n))]
