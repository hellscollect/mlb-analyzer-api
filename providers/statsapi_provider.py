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
        return int(date_str[:4])
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
    """
    Prefer 'avg' if present; otherwise compute H/AB.
    """
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
    """
    Compute last-n games batting average (H/AB), skipping games with AB=0.
    Splits are most-recent first.
    """
    games: List[Tuple[int, int]] = []  # (H, AB)
    for sp in splits:
        stat = sp.get("stat", {})
        h = int(_safe_float(stat.get("hits")))
        ab = int(_safe_float(stat.get("atBats")))
        if ab > 0:
            games.append((h, ab))
        if len(games) >= n:
            break
    if not games:
        return 0.0
    total_h = sum(h for h, _ in games)
    total_ab = sum(ab for _, ab in games)
    return (total_h / total_ab) if total_ab > 0 else 0.0


def _ab0_hitless_streak_from_splits(
    splits: List[Dict[str, Any]],
    end_date: Optional[date_cls] = None,
) -> int:
    """
    AB>0-only *consecutive hitless games* walking most-recent -> older.
    Stops at first game with a hit. 0-AB games do not count and do not break.
    If end_date is provided, only consider splits with date <= end_date.
    """
    streak = 0
    for sp in splits:
        # split date (formats vary a bit across endpoints)
        dstr = (sp.get("date") or sp.get("gameDate") or sp.get("gameDateTime") or "")
        if "T" in dstr:
            dstr = dstr.split("T")[0]
        try:
            gdate = datetime.strptime(dstr, "%Y-%m-%d").date() if dstr else None
        except Exception:
            gdate = None
        if end_date and gdate and gdate > end_date:
            # skip future games relative to end_date
            continue

        st = sp.get("stat", {})
        ab = int(_safe_float(st.get("atBats")))
        h = int(_safe_float(st.get("hits")))
        if ab == 0:
            # ignore this game entirely
            continue
        if h == 0:
            streak += 1
        else:
            break
    return streak


def _avg_hitless_run_ab_only(splits: List[Dict[str, Any]]) -> float:
    """
    Average length of AB>0-only hitless runs across the season.
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


# ----------------------------
# Name normalization helpers
# ----------------------------
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


# ======================================================================
# Provider
# ======================================================================
class StatsApiProvider:
    """
    Provider that:
      • Builds the slate and probables from /schedule
      • Scans hitters on active rosters for teams in the slate
      • Computes HOT and COLD lists
      • Enforces AB>0-only streak logic via each player's gameLog (fast)
    """

    def __init__(self, client: Optional[StatsApiClient] = None):
        self.client = client or StatsApiClient()

    # ---------------- Schedule ----------------

    def schedule_for_date(self, date_str: str) -> List[Dict[str, Any]]:
        data = self.client.schedule(date_str, hydrate="probablePitcher")

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

    # ---------------- League scans (AB>0-only using gameLog) ----------------

    def _teams_in_slate(self, date_str: str) -> List[Dict[str, Any]]:
        data = self.client.schedule(date_str)
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
        return self.client.player_stats(player_id, season, "season")

    def _player_gamelog(self, player_id: int, season: int) -> Dict[str, Any]:
        return self.client.player_stats(player_id, season, "gameLog")

    def _scan_hitters_for_teams(self, date_str: str) -> List[Dict[str, Any]]:
        """
        Returns per-hitter rows for the slate with AB>0-only hitless streak computed from gameLog.
        Keys:
          player_id, player_name, team_name, season_avg, recent_avg_5,
          hitless_streak (AB>0-only), avg_hitless_run
        """
        try:
            end_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except Exception:
            end_date = datetime.now(ZoneInfo("America/New_York")).date()

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
                    if pos == "P" or not pid:
                        continue

                    # season avg
                    season_stats = self._player_season_stats(pid, season)
                    s_avg = _extract_season_avg(season_stats)

                    # game log
                    gamelog = self._player_gamelog(pid, season)
                    splits = _extract_game_splits(gamelog)

                    r5 = _recent_avg_from_gamelog(splits, 5)
                    verified_streak = _ab0_hitless_streak_from_splits(splits, end_date=end_date)
                    avg0 = _avg_hitless_run_ab_only(splits)

                    hitters.append({
                        "player_id": pid,
                        "player_name": pname,
                        "team_name": team_name,
                        "season_avg": round(s_avg, 3),
                        "recent_avg_5": round(r5, 3),
                        "hitless_streak": int(verified_streak),   # AB>0-only
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
        All streak values are AB>0-only and computed from the player’s gameLog.
        """
        rows = self._scan_hitters_for_teams(date_str)
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

    # ---------------- Verification endpoint helper (fast) ----------------
    def boxscore_hitless_streak(
        self,
        player_name: str,
        team_name: Optional[str] = None,
        end_date: Optional[date_cls] = None,
        max_lookback: int = 300,   # kept for signature compatibility; not used
        debug: bool = False,
    ) -> int:
        """
        AB>0-only *consecutive hitless games* for `player_name` based on the player’s gameLog.
        We locate the player from today’s slate rosters (team hint helps if a player was traded).
        """
        tz = ZoneInfo("America/New_York")
        if end_date is None:
            end_date = datetime.now(tz).date()
        date_str = end_date.strftime("%Y-%m-%d")
        season = end_date.year

        # Find the player by scanning active rosters for the slate
        target = _norm_name(player_name)
        teams = self._teams_in_slate(date_str)
        # If team_name is provided, bias the search order
        if team_name:
            tn = _norm_name(team_name)
            teams.sort(key=lambda t: 0 if _norm_name(t["name"]) == tn else 1)

        for t in teams:
            roster = self._active_roster(t["id"])
            for r in roster:
                person = r.get("person") or {}
                pname = person.get("fullName") or person.get("lastFirstName") or ""
                if not pname:
                    continue
                if _same_player(pname, target):
                    pid = person.get("id")
                    if not pid:
                        continue
                    stats = self._player_stats_for_gamelog(pid, season)
                    splits = _extract_game_splits(stats)
                    return _ab0_hitless_streak_from_splits(splits, end_date=end_date)

        # Not found in slate rosters ➜ conservative 0
        return 0

    def _player_stats_for_gamelog(self, player_id: int, season: int) -> Dict[str, Any]:
        return self.client.player_stats(player_id, season, "gameLog")
