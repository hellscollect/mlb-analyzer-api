# providers/statsapi_provider.py
import requests
from datetime import datetime, timedelta, timezone, date as date_cls
from typing import Any, Dict, List, Optional, Tuple

class StatsApiProvider:
    BASE_URL = "https://statsapi.mlb.com/api/v1"

    # ------------- HTTP -------------
    def _fetch(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        r = requests.get(url, params=params or {}, timeout=20)
        r.raise_for_status()
        return r.json()

    # ------------- Dates -------------
    def _parse_date(self, d: Optional[Any]) -> date_cls:
        today = datetime.now(timezone.utc).date()
        if d is None:
            return today
        if isinstance(d, date_cls):
            return d
        if isinstance(d, str):
            s = d.lower()
            if s == "today":
                return today
            if s == "yesterday":
                return today - timedelta(days=1)
            if s == "tomorrow":
                return today + timedelta(days=1)
            return datetime.strptime(d, "%Y-%m-%d").date()
        raise ValueError("Invalid date")

    # ------------- Schedule -------------
    def schedule_for_date(self, date: Optional[Any] = None) -> Dict[str, Any]:
        d = self._parse_date(date)
        return self._fetch("schedule", {"sportId": 1, "date": d.isoformat()})

    def _games_for_date(self, date: date_cls) -> List[Dict[str, Any]]:
        sched = self.schedule_for_date(date)
        out: List[Dict[str, Any]] = []
        for dt in sched.get("dates", []):
            out.extend(dt.get("games", []))
        return out

    def _game_start_dt_utc(self, game: Dict[str, Any]) -> Optional[datetime]:
        gd = (game.get("gameDate") or "").replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(gd)
        except Exception:
            return None

    def _not_started_games(self, date: date_cls) -> List[Dict[str, Any]]:
        now_utc = datetime.now(timezone.utc)
        games = self._games_for_date(date)
        out = []
        for g in games:
            status = g.get("status", {})
            code = status.get("codedGameState")
            # 'S' Scheduled, 'I' In Progress, 'F' Final, etc.
            start_dt = self._game_start_dt_utc(g)
            if code == "S" and (start_dt is None or start_dt > now_utc):
                out.append(g)
        return out

    def _team_ids_with_not_started_games(self, date: date_cls) -> Tuple[set, Dict[int, str]]:
        ids = set()
        id_to_name: Dict[int, str] = {}
        for g in self._not_started_games(date):
            home = g.get("teams", {}).get("home", {}).get("team", {})
            away = g.get("teams", {}).get("away", {}).get("team", {})
            hid, aid = home.get("id"), away.get("id")
            if isinstance(hid, int):
                ids.add(hid); id_to_name[hid] = home.get("name", "")
            if isinstance(aid, int):
                ids.add(aid); id_to_name[aid] = away.get("name", "")
        return ids, id_to_name

    # ------------- People / Rosters / Stats -------------
    def _people_search(self, name: str) -> List[Dict[str, Any]]:
        j = self._fetch("people", {"search": name})
        return j.get("people", []) or []

    def _person_detail(self, person_id: int) -> Dict[str, Any]:
        j = self._fetch(f"people/{person_id}", {})
        people = j.get("people", []) or []
        return people[0] if people else {}

    def _team_active_roster(self, team_id: int) -> List[Dict[str, Any]]:
        j = self._fetch(f"teams/{team_id}/roster", {"rosterType": "active"})
        return j.get("roster", []) or []

    def _season_avg(self, person_id: int, season: int) -> float:
        j = self._fetch(f"people/{person_id}/stats", {
            "stats": "season",
            "group": "hitting",
            "season": str(season),
            "gameType": "R",
        })
        splits = (j.get("stats", []) or [{}])[0].get("splits", []) or []
        if not splits:
            return 0.0
        stat = splits[0].get("stat", {}) or {}
        try:
            return float(stat.get("avg") or 0.0)
        except Exception:
            return 0.0

    def _gamelogs(self, person_id: int, season: int) -> List[Dict[str, Any]]:
        j = self._fetch(f"people/{person_id}/stats", {
            "stats": "gameLog",
            "group": "hitting",
            "season": str(season),
            "gameType": "R",
        })
        return (j.get("stats", []) or [{}])[0].get("splits", []) or []

    # ------------- Streak helpers -------------
    def _split_game_dt(self, split: Dict[str, Any]) -> datetime:
        gd = (split.get("date") or split.get("game", {}).get("gameDate") or "").replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(gd)
        except Exception:
            return datetime(1900, 1, 1, tzinfo=timezone.utc)

    def _hitless_streak_from_splits(self, splits: List[Dict[str, Any]]) -> int:
        # Sort newest -> oldest
        logs = sorted(splits, key=self._split_game_dt, reverse=True)
        streak = 0
        for s in logs:
            # only regular season
            gtype = (s.get("game", {}) or {}).get("type", "R")
            if str(gtype).upper() != "R":
                continue
            stat = s.get("stat", {}) or {}
            ab = int(stat.get("atBats") or 0)
            h = int(stat.get("hits") or 0)
            if ab == 0:
                continue
            if h == 0:
                streak += 1
            else:
                break
        return streak

    def _hit_streak_from_splits(self, splits: List[Dict[str, Any]]) -> int:
        logs = sorted(splits, key=self._split_game_dt, reverse=True)
        streak = 0
        for s in logs:
            gtype = (s.get("game", {}) or {}).get("type", "R")
            if str(gtype).upper() != "R":
                continue
            stat = s.get("stat", {}) or {}
            ab = int(stat.get("atBats") or 0)
            h = int(stat.get("hits") or 0)
            if ab == 0:
                continue
            if h >= 1:
                streak += 1
            else:
                break
        return streak

    # ------------- Public: names-driven cold candidates -------------
    def cold_candidates(
        self,
        date: Optional[Any] = None,
        names: Optional[List[str]] = None,
        min_season_avg: float = 0.26,
        last_n: int = 7,
        min_hitless_games: int = 1,
        limit: int = 30,
        verify: bool = True,
        debug: bool = False,
    ) -> Dict[str, Any]:
        """
        Names-only helper used by /routes/cold_candidates.py
        - Only returns players whose team has a NOT-YET-STARTED game today.
        - hitless_streak counts consecutive AB>0, H==0 games (regular season only).
        """
        d = self._parse_date(date)
        season = d.year
        not_started_games = self._not_started_games(d)
        eligible_team_ids, team_name_map = self._team_ids_with_not_started_games(d)

        items: List[Dict[str, Any]] = []
        dbg: List[Any] = []

        if not names:
            return {"date": d.isoformat(), "season": season, "items": [], "debug": dbg}

        for raw_name in names:
            name = (raw_name or "").strip()
            if not name:
                continue
            try:
                people = self._people_search(name)
                if not people:
                    if debug: dbg.append({"name": name, "error": "no match"})
                    continue
                p = people[0]
                pid = int(p.get("id"))
                full_name = p.get("fullName") or name

                # Determine team
                team = (p.get("currentTeam") or {})
                team_id = team.get("id")
                team_name = team.get("name") or team_name_map.get(team_id, "")

                # Must have a not-started game today for inclusion
                if isinstance(team_id, int) and team_id not in eligible_team_ids:
                    if debug: dbg.append({"name": full_name, "skip": "no not-started game today"})
                    continue

                splits = self._gamelogs(pid, season)[:max(1, int(last_n))]
                streak = self._hitless_streak_from_splits(splits)

                # Require last AB>0 game to be hitless -> implied by streak>=1
                if streak < max(1, int(min_hitless_games)):
                    if debug: dbg.append({"name": full_name, "streak": streak, "skip": "below min_hitless_games"})
                    continue

                avg = self._season_avg(pid, season)
                if avg < float(min_season_avg):
                    if debug: dbg.append({"name": full_name, "avg": avg, "skip": "below min_season_avg"})
                    continue

                items.append({
                    "name": full_name,
                    "team": team_name or "",
                    "season_avg": round(avg, 3),
                    "hitless_streak": streak,
                })
            except requests.HTTPError as e:
                if debug: dbg.append({"name": name, "error": f"HTTPError: {e}"})
            except Exception as e:
                if debug: dbg.append({"name": name, "error": f"{type(e).__name__}: {e}"})

        # Sort: longer streak first, then higher season avg
        items.sort(key=lambda x: (-x["hitless_streak"], -x["season_avg"]))
        if isinstance(limit, int) and limit > 0:
            items = items[:limit]

        return {"date": d.isoformat(), "season": season, "items": items, "debug": dbg}

    # ------------- Public: league scan for cold streak hitters (current day only) -------------
    def cold_streak_hitters(
        self,
        date: Optional[Any] = None,
        min_avg: float = 0.26,
        games: int = 1,
        require_zero_hit_each: bool = True,
        top_n: int = 25,
        debug: bool = False,
    ) -> Dict[str, Any]:
        """
        Current-day scan across ACTIVE rosters of teams with NOT-YET-STARTED games.
        Filters to competent hitters (min_avg) whose last `games` AB>0 games were 0-H hits.
        """
        d = self._parse_date(date)
        season = d.year
        team_ids, team_name_map = self._team_ids_with_not_started_games(d)
        hitter_positions = {"C","1B","2B","3B","SS","LF","CF","RF","DH","OF"}

        out: List[Dict[str, Any]] = []
        dbg: List[Any] = []

        for tid in sorted(team_ids):
            try:
                roster = self._team_active_roster(tid)
            except Exception as e:
                if debug: dbg.append({"team_id": tid, "error": f"roster: {e}"})
                continue

            for r in roster:
                try:
                    pos = (r.get("position") or {}).get("abbreviation") or ""
                    if pos.upper() not in hitter_positions:
                        continue
                    person = r.get("person") or {}
                    pid = int(person.get("id"))
                    name = person.get("fullName") or ""

                    splits = self._gamelogs(pid, season)
                    if not splits:
                        continue
                    # consecutive hitless games
                    streak = self._hitless_streak_from_splits(splits)
                    if require_zero_hit_each:
                        if streak < max(1, int(games)):
                            continue
                    else:
                        # if not requiring every game 0H, still require last game 0H
                        if streak < 1:
                            continue

                    avg = self._season_avg(pid, season)
                    if avg < float(min_avg):
                        continue

                    out.append({
                        "name": name,
                        "team": team_name_map.get(tid, ""),
                        "season_avg": round(avg, 3),
                        "hitless_streak": streak,
                    })
                except Exception as e:
                    if debug: dbg.append({"team_id": tid, "player_err": f"{type(e).__name__}: {e}"})

        out.sort(key=lambda x: (-x["hitless_streak"], -x["season_avg"]))
        if isinstance(top_n, int) and top_n > 0:
            out = out[:top_n]

        return {"date": d.isoformat(), "season": season, "cold_hitters": out, "debug": dbg}

    # ------------- Optional: hot streak (to keep self-test happy) -------------
    def hot_streak_hitters(
        self,
        date: Optional[Any] = None,
        min_avg: float = 0.26,
        games: int = 3,
        require_hit_each: bool = True,
        top_n: int = 25,
        debug: bool = False,
    ) -> Dict[str, Any]:
        d = self._parse_date(date)
        season = d.year
        team_ids, team_name_map = self._team_ids_with_not_started_games(d)
        hitter_positions = {"C","1B","2B","3B","SS","LF","CF","RF","DH","OF"}
        out: List[Dict[str, Any]] = []
        for tid in sorted(team_ids):
            try:
                roster = self._team_active_roster(tid)
            except Exception:
                continue
            for r in roster:
                pos = (r.get("position") or {}).get("abbreviation") or ""
                if pos.upper() not in hitter_positions:
                    continue
                person = r.get("person") or {}
                pid = int(person.get("id"))
                name = person.get("fullName") or ""
                splits = self._gamelogs(pid, season)
                if not splits:
                    continue
                hit_streak = self._hit_streak_from_splits(splits)
                if require_hit_each and hit_streak < max(1, int(games)):
                    continue
                avg = self._season_avg(pid, season)
                if avg < float(min_avg):
                    continue
                out.append({
                    "name": name,
                    "team": team_name_map.get(tid, ""),
                    "season_avg": round(avg, 3),
                    "hit_streak": hit_streak,
                })
        out.sort(key=lambda x: (-x["hit_streak"], -x["season_avg"]))
        if isinstance(top_n, int) and top_n > 0:
            out = out[:top_n]
        return {"date": d.isoformat(), "season": season, "hot_hitters": out}
