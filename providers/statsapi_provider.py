# providers/statsapi_provider.py
from __future__ import annotations

import unicodedata
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, date as date_cls
from zoneinfo import ZoneInfo

from .statsapi_client import StatsApiClient


def _log(msg: str) -> None:
    # Keep logs consistent with your Render logs
    print(f"[StatsApiProvider] {msg}", flush=True)


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


# ----------------------------
# Param normalization helpers
# ----------------------------
def _coerce_date_str(
    date_str: Optional[str] = None,
    date: Optional[date_cls] = None,
) -> str:
    """
    Accept either a YYYY-MM-DD string or a date object; return YYYY-MM-DD.
    """
    if date_str and isinstance(date_str, str):
        return date_str
    if date and isinstance(date, date_cls):
        return date.isoformat()
    # last resort: today ET
    tz = ZoneInfo("America/New_York")
    return datetime.now(tz).date().isoformat()


def _coerce_top_n(top_n: Optional[int] = None, n: Optional[int] = None, default: int = 15) -> int:
    v = top_n if top_n is not None else n
    try:
        v = int(v) if v is not None else default
    except Exception:
        v = default
    return max(1, min(200, v))


# ======================================================================
# Provider
# ======================================================================
class StatsApiProvider:
    """
    Provider that:
      • Builds the slate and probables from /schedule
      • Scans hitters on active rosters for teams in the slate
      • Computes HOT and COLD lists with the exact fields your router expects
      • Boxscore-based hitless streak verification via /game/{gamePk}/boxscore
    """

    def __init__(self, client: Optional[StatsApiClient] = None):
        # A small TTL cache + retry client so we don't hammer the public API.
        self.client = client or StatsApiClient()

    # ---------------- Schedule ----------------
    def schedule_for_date(self, date_str: Optional[str] = None, date: Optional[date_cls] = None, **_: Any) -> List[Dict[str, Any]]:
        d = _coerce_date_str(date_str=date_str, date=date)
        data = self.client.schedule(d, hydrate="probablePitcher")

        out: List[Dict[str, Any]] = []
        dates = data.get("dates") or []
        if not dates:
            _log(f"0 games scheduled on {d}")
            return out

        games = dates[0].get("games", [])
        _log(f"{len(games) * 2} teams scheduled on {d}")

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

    def league_hot_hitters(
        self,
        date_str: Optional[str] = None,
        date: Optional[date_cls] = None,
        top_n: int = 15,
        n: Optional[int] = None,
        **_: Any,
    ) -> List[Dict[str, Any]]:
        """
        Return: list of dicts with keys:
          player_name, team_name, recent_avg_5, season_avg, avg_uplift
        """
        d = _coerce_date_str(date_str=date_str, date=date)
        k = _coerce_top_n(top_n=top_n, n=n, default=15)

        rows = self._scan_hitters_for_teams(d)
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
        return out[:k]

    def league_cold_hitters(
        self,
        date_str: Optional[str] = None,
        date: Optional[date_cls] = None,
        top_n: int = 15,
        n: Optional[int] = None,
        **_: Any,
    ) -> List[Dict[str, Any]]:
        """
        Return: list of dicts with keys:
          player_name, team_name, season_avg, current_hitless_streak, avg_hitless_run, rarity_index, slump_score
        """
        d = _coerce_date_str(date_str=date_str, date=date)
        k = _coerce_top_n(top_n=top_n, n=n, default=15)

        rows = self._scan_hitters_for_teams(d)
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
        return out[:k]

    # ---------------- Boxscore verification ----------------
    def boxscore_hitless_streak(
        self,
        player_name: str,
        team_name: Optional[str] = None,
        end_date: Optional[date_cls] = None,
        max_lookback: int = 300,
        debug: bool = False,
        **_: Any,
    ) -> int:
        """
        Compute current-season, AB>0-only *consecutive hitless games* for `player_name`,
        scanning backward from `end_date` until a game with a hit occurs (or season start).

        Rules:
          • Count a game as hitless ONLY if AB > 0 and H == 0.
          • 0-for-0 (BB/HBP/SF-only) does NOT count and does NOT break the streak.
          • Any hit (H > 0) immediately BREAKS the streak.
          • Current season only (based on end_date.year).
        Returns:
          int -> consecutive hitless games (AB>0 only)
        """
        # Establish time bounds
        tz = ZoneInfo("America/New_York")
        if end_date is None:
            end_date = datetime.now(tz).date()
        season_year = end_date.year
        season_start = date_cls(season_year, 3, 1)  # safe early bound

        target = _norm_name(player_name)
        team_norm = _norm_name(team_name) if team_name else None

        # Walk back day-by-day
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

                # If a team filter is provided, skip games that clearly don't involve it (best-effort)
                if team_norm:
                    home_name = (((g.get("teams") or {}).get("home") or {}).get("team") or {}).get("name") or ""
                    away_name = (((g.get("teams") or {}).get("away") or {}).get("team") or {}).get("name") or ""
                    if _norm_name(home_name) != team_norm and _norm_name(away_name) != team_norm:
                        # Not a hard exclusion—player may have been traded—but this speeds most paths.
                        pass

                # Pull boxscore
                try:
                    box = self.client.boxscore(int(game_pk))
                except Exception:
                    continue

                batter_rows: List[Dict[str, Any]] = []

                # Standard shape: box['teams']['home'/'away']['players'][<id>]['stats']['batting']
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

                # Fallback shapes occasionally seen
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
                            # ignore this game
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
                    # if matched but AB==0 only -> ignore and continue scanning older dates

            cursor = cursor - timedelta(days=1)
            looked += 1

        return streak
