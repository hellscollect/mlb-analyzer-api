# providers/statsapi_provider.py
from __future__ import annotations

import unicodedata
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, date as date_cls
from zoneinfo import ZoneInfo

from .statsapi_client import StatsApiClient

def _log(msg: str) -> None:
    print(f"[StatsApiProvider] {msg}", flush=True)


def _season_from_date(date_str: str) -> int:
    try:
        return int(str(date_str)[:4])
    except Exception:
        return datetime.now().year


def _to_et_str(iso_dt: str) -> str:
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
    if not stat:
        return 0.0
    if "avg" in stat and stat["avg"] not in (None, "", ".---"):
        try:
            return float(stat["avg"])
        except Exception:
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
    games: List[Tuple[int, int]] = []
    for sp in splits:
        stat = sp.get("stat", {})
        h = int(_safe_float(stat.get("hits")))
        ab = int(_safe_float(stat.get("atBats")))
        if ab > 0:
            games.append((h, ab))
    if not games:
        return 0.0
    games = games[:n]
    total_h = sum(h for h, _ in games)
    total_ab = sum(ab for _, ab in games)
    return (total_h / total_ab) if total_ab > 0 else 0.0


def _current_hitless_streak(splits: List[Dict[str, Any]]) -> int:
    streak = 0
    for sp in splits:
        st = sp.get("stat", {})
        ab = int(_safe_float(st.get("atBats")))
        h = int(_safe_float(st.get("hits")))
        if ab == 0:
            continue
        if h == 0:
            streak += 1
        else:
            break
    return streak


def _avg_hitless_run(splits: List[Dict[str, Any]]) -> float:
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


def _norm_name(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return (
        s.lower()
        .replace(".", "")
        .replace("-", " ")
        .replace("'", "")
        .replace("  ", " ")
        .strip()
    )


def _same_player(a: str, b: str) -> bool:
    return _norm_name(a) == _norm_name(b)


class StatsApiProvider:
    """
    Provider that:
      • Builds the slate and probables from /schedule
      • Scans hitters on active rosters for teams in the slate
      • Computes HOT and COLD lists
      • Boxscore-based hitless streak verification via /game/{gamePk}/boxscore
    """

    def __init__(self, client: Optional[StatsApiClient] = None):
        self.client = client or StatsApiClient()

    # ---------------- Schedule ----------------

    def schedule_for_date(self, date_str: str) -> List[Dict[str, Any]]:
        data = self.client.schedule(str(date_str), hydrate="probablePitcher")

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
        data = self.client.schedule(str(date_str))
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
        js = self.client.team_roster(team_id, "active")
        return js.get("roster") or []

    def _player_season_stats(self, player_id: int, season: int) -> Dict[str, Any]:
        return self.client.player_stats(player_id, int(season), "season")

    def _player_gamelog(self, player_id: int, season: int) -> Dict[str, Any]:
        return self.client.player_stats(player_id, int(season), "gameLog")

    def _scan_hitters_for_teams(self, date_str: str) -> List[Dict[str, Any]]:
        season = _season_from_date(str(date_str))
        teams = self._teams_in_slate(str(date_str))
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
                        "team_name": team_name,  # roster-derived (current team for that slate)
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
        rows = self._scan_hitters_for_teams(str(date_str))
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
        rows = self._scan_hitters_for_teams(str(date_str))
        out: List[Dict[str, Any]] = []
        for r in rows:
            cur0 = int(r["hitless_streak"])
            avg0 = float(r["avg_hitless_run"])
            season_avg = float(r["season_avg"])
            recent = float(r["recent_avg_5"])
            rarity_index = (cur0 / avg0) if avg0 > 0 else float(cur0)
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
        out.sort(key=lambda x: (x["slump_score"], x["current_hitless_streak"]), reverse=True)
        return out[:max(0, int(top_n))]

    # ---------------- Boxscore verification ----------------
    def boxscore_hitless_streak(
        self,
        player_name: str,
        team_name: Optional[str] = None,
        end_date: Optional[date_cls] = None,
        max_lookback: int = 300,
        debug: bool = False,
    ) -> int:
        """
        Current-season AB>0-only consecutive hitless games for `player_name`.
        Notes:
          • Team name is treated as a HINT ONLY; player matching is by full name from boxscore.
          • Season is hard-limited to end_date.year (starts Mar 1).
        """
        tz = ZoneInfo("America/New_York")
        if end_date is None:
            end_date = datetime.now(tz).date()
        season_year = end_date.year
        season_start = date_cls(season_year, 3, 1)  # safe early bound

        target = _norm_name(player_name)

        cursor = end_date
        looked = 0
        streak = 0

        while cursor >= season_start and looked <= max_lookback:
            day = cursor.strftime("%Y-%m-%d")
            try:
                sched = self.client.schedule(day)
            except Exception:
                sched = {}

            dates = sched.get("dates") or []
            games = dates[0].get("games", []) if dates else []

            for g in games:
                game_pk = g.get("gamePk") or g.get("game_pk") or g.get("game_id")
                if not game_pk:
                    continue

                # Pull boxscore
                try:
                    box = self.client.boxscore(int(game_pk))
                except Exception:
                    continue

                batter_rows: List[Dict[str, Any]] = []

                teams_blob = box.get("teams") or {}
                for side in ("home", "away"):
                    side_blob = teams_blob.get(side)
                    if isinstance(side_blob, dict):
                        players = side_blob.get("players")
                        if isinstance(players, dict):
                            for pdata in players.values():
                                person = pdata.get("person") or {}
                                full_name = person.get("fullName") or pdata.get("name") or ""
                                stats = pdata.get("stats", {})
                                batting = stats.get("batting") or {}
                                ab = batting.get("atBats")
                                h = batting.get("hits")
                                if full_name:
                                    batter_rows.append({"name": full_name, "ab": ab, "h": h})

                if not batter_rows:
                    for key in ("batters", "hitters"):
                        maybe = box.get(key)
                        if isinstance(maybe, list):
                            for row in maybe:
                                if isinstance(row, dict):
                                    full_name = row.get("name") or row.get("fullName") or ""
                                    batting = row.get("batting") or row
                                    ab = batting.get("ab") or batting.get("atBats")
                                    h = batting.get("h") or batting.get("hits")
                                    if full_name:
                                        batter_rows.append({"name": full_name, "ab": ab, "h": h})

                matched_any = False
                hit_found = False
                counted_hitless = False

                for br in batter_rows:
                    if _same_player(br.get("name", ""), target):
                        matched_any = True
                        ab = br.get("ab") or 0
                        h = br.get("h") or 0
                        try:
                            ab = int(ab)
                        except Exception:
                            ab = 0
                        try:
                            h = int(h)
                        except Exception:
                            h = 0

                        if ab == 0:
                            continue
                        if h > 0:
                            hit_found = True
                            break
                        else:
                            counted_hitless = True  # AB>0 and H==0

                if matched_any:
                    if hit_found:
                        return streak
                    if counted_hitless:
                        streak += 1

            cursor = cursor - timedelta(days=1)
            looked += 1

        return streak
