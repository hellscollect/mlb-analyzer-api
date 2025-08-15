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
    games = games[:n]
    total_h = sum(h for h, _ in games)
    total_ab = sum(ab for _, ab in games)
    return (total_h / total_ab) if total_ab > 0 else 0.0


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
      • ALWAYS enforces AB>0-only streak logic using boxscores
    """

    def __init__(self, client: Optional[StatsApiClient] = None):
        # small TTL cache + retries client so we don't hammer the public API
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

    # ---------------- League scans (with enforced AB>0 verification) ----------------

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
        Returns per-hitter rows for the slate with AB>0-only hitless streak verified by boxscores.
        Keys:
          player_id, player_name, team_name, season_avg, recent_avg_5,
          hitless_streak (AB>0-only, verified), avg_hitless_run
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

                    # season + gamelog (for season_avg and recent_avg_5)
                    season_stats = self._player_season_stats(pid, season)
                    s_avg = _extract_season_avg(season_stats)

                    gamelog = self._player_gamelog(pid, season)
                    splits = _extract_game_splits(gamelog)
                    r5 = _recent_avg_from_gamelog(splits, 5)

                    # AB>0-only streak — always boxscore-verified
                    verified_streak = self.boxscore_hitless_streak(
                        player_name=pname,
                        team_name=team_name,      # helps keep trades straight most of the time
                        end_date=end_date,
                        max_lookback=300,
                        debug=False,
                    )

                    # Approximate “avg hitless run” from gamelog (AB>0 only already in helper)
                    avg0 = self._avg_hitless_run_ab_only(splits)

                    hitters.append({
                        "player_id": pid,
                        "player_name": pname,
                        "team_name": team_name,
                        "season_avg": round(s_avg, 3),
                        "recent_avg_5": round(r5, 3),
                        "hitless_streak": int(verified_streak),   # <- enforced AB>0
                        "avg_hitless_run": round(avg0, 3),
                    })
                except Exception as ex:
                    _log(f"scan_hitter_error:{type(ex).__name__}")
                    continue

        return hitters

    @staticmethod
    def _avg_hitless_run_ab_only(splits: List[Dict[str, Any]]) -> float:
        """
        Average length of hitless runs across the season, counting only AB>0 games.
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
        All streak values are AB>0-only and boxscore-verified.
        """
        rows = self._scan_hitters_for_teams(date_str)
        out: List[Dict[str, Any]] = []

        for r in rows:
            cur0 = int(r["hitless_streak"])            # already verified via boxscores
            avg0 = float(r["avg_hitless_run"])
            season_avg = float(r["season_avg"])
            recent = float(r["recent_avg_5"])

            rarity_index = (cur0 / avg0) if avg0 > 0 else float(cur0)
            delta = max(0.0, season_avg - recent)      # “true slump” weighting
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

    # ---------------- Boxscore verification (AB>0-only) ----------------
    def boxscore_hitless_streak(
        self,
        player_name: str,
        team_name: Optional[str] = None,
        end_date: Optional[date_cls] = None,
        max_lookback: int = 300,
        debug: bool = False,
    ) -> int:
        """
        AB>0-only *consecutive hitless games* for `player_name`, scanning backward from `end_date`
        until a game with a hit occurs (or season start). 0-AB games never count and never break.
        """
        tz = ZoneInfo("America/New_York")
        if end_date is None:
            end_date = datetime.now(tz).date()
        season_year = end_date.year
        season_start = date_cls(season_year, 3, 1)  # safe early bound

        target = _norm_name(player_name)
        team_norm = _norm_name(team_name) if team_name else None

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

                # If team filter provided, use as a hint to skip unrelated games
                if team_norm:
                    home_name = (((g.get("teams") or {}).get("home") or {}).get("team") or {}).get("name") or ""
                    away_name = (((g.get("teams") or {}).get("away") or {}).get("team") or {}).get("name") or ""
                    if _norm_name(home_name) != team_norm and _norm_name(away_name) != team_norm:
                        # not hard exclusion (trades), but helps most paths
                        pass

                try:
                    box = self.client.boxscore(int(game_pk))
                except Exception:
                    continue

                batter_rows: List[Dict[str, Any]] = []

                # Standard shape
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

                # Fallback shapes
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
                            continue  # 0-AB never counts, never breaks
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
                    # if matched but AB==0 only -> ignore and continue scanning older dates

            cursor = cursor - timedelta(days=1)
            looked += 1

        return streak
