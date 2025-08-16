import requests
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Union

class StatsApiProvider:
    BASE_URL = "https://statsapi.mlb.com/api/v1"

    def _fetch(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        r = requests.get(url, params=params or {}, timeout=20)
        r.raise_for_status()
        return r.json()

    # ---------- Helpers ----------
    @staticmethod
    def _to_datestr(date_obj_or_str: Union[str, datetime, None]) -> str:
        if isinstance(date_obj_or_str, str):
            return date_obj_or_str
        if isinstance(date_obj_or_str, datetime):
            return date_obj_or_str.strftime("%Y-%m-%d")
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def _parse_game_date(g: Dict[str, Any]) -> datetime:
        d = g.get("date") or g.get("game", {}).get("gameDate") or g.get("gameDate")
        if not d:
            return datetime.min.replace(tzinfo=timezone.utc)
        try:
            return datetime.fromisoformat(d.replace("Z", "+00:00"))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    def schedule_for_date(self, date: Union[str, datetime]) -> Dict[str, Any]:
        # Keep existing signature; wrapper may pass str or date
        date_str = self._to_datestr(date)
        return self._fetch("schedule", params={"sportId": 1, "date": date_str})

    # --- Stats lookups ---
    def _season_avg(self, person_id: int, season: int) -> float:
        data = self._fetch(
            f"people/{person_id}/stats",
            params={"stats": "season", "group": "hitting", "season": season, "gameType": "R"},
        )
        try:
            splits = data["stats"][0]["splits"]
            if not splits:
                return 0.0
            avg = splits[0]["stat"].get("avg") or "0.000"
            return float(avg)
        except Exception:
            return 0.0

    def _game_logs(self, person_id: int, season: int) -> List[Dict[str, Any]]:
        data = self._fetch(
            f"people/{person_id}/stats",
            params={"stats": "gameLog", "group": "hitting", "season": season, "gameType": "R"},
        )
        try:
            return data["stats"][0]["splits"]
        except Exception:
            return []

    def _compute_hitless_streak_from_gamelog(self, game_logs: List[Dict[str, Any]]) -> int:
        # Newest → oldest; only Regular season; count consecutive (AB>0 and H==0)
        logs = sorted(game_logs, key=self._parse_game_date, reverse=True)
        streak = 0
        for g in logs:
            gtype = (g.get("game", {}).get("type") or g.get("gameType") or "").upper()
            if gtype != "R":
                continue
            stat = g.get("stat", {})
            ab = int(stat.get("atBats") or 0)
            h = int(stat.get("hits") or 0)
            if ab == 0:
                continue
            if h == 0:
                streak += 1
            else:
                break
        return streak

    def _search_people(self, name: str) -> List[Dict[str, Any]]:
        data = self._fetch("people", params={"search": name})
        return data.get("people", []) or []

    # ---------- Targeted cold-candidates by names (fast path for verification) ----------
    def cold_candidates_by_names(
        self,
        names: List[str],
        date: Optional[Union[str, datetime]] = None,
        min_season_avg: float = 0.26,
        last_n: int = 7,
        min_hitless_games: int = 1,
        limit: int = 30,
        verify: int = 1,
        debug: int = 0,
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Evaluate only the provided names.
        Business rules:
         - Regular season only (gameType=R)
         - Streak = consecutive games with AB>0 and H==0, newest→oldest
         - Ignore games with AB==0
         - Filter by season AVG >= min_season_avg
        """
        # Season/year from date (fallback to current year)
        try:
            dstr = self._to_datestr(date)
            season = datetime.fromisoformat(dstr).year
        except Exception:
            season = datetime.now().year

        out: List[Dict[str, Any]] = []
        notes: List[Dict[str, Any]] = []

        for raw in names:
            q = (raw or "").strip()
            if not q:
                continue
            try:
                matches = self._search_people(q)
                if not matches:
                    if debug:
                        notes.append({"name": q, "note": "not_found"})
                    continue
                person = matches[0]
                pid = person["id"]
                fullname = person.get("fullName") or q
                team_name = (person.get("currentTeam") or {}).get("name") or ""

                avg = self._season_avg(pid, season)
                if avg < min_season_avg:
                    if debug:
                        notes.append({"name": fullname, "avg": avg, "skip": "below_min_avg"})
                    continue

                logs = self._game_logs(pid, season)
                # Count consecutive hitless AB>0 from the front; last_n bounds the context we care about
                # (we still allow longer consecutive runs if present)
                streak = 0
                considered = 0
                for g in sorted(logs, key=self._parse_game_date, reverse=True):
                    gtype = (g.get("game", {}).get("type") or g.get("gameType") or "").upper()
                    if gtype != "R":
                        continue
                    stat = g.get("stat", {})
                    ab = int(stat.get("atBats") or 0)
                    h = int(stat.get("hits") or 0)
                    if ab == 0:
                        continue
                    considered += 1
                    if h == 0:
                        streak += 1
                    else:
                        break
                    # do not hard-cap streak by last_n; last_n just defines window minimums

                if streak >= min_hitless_games and streak > 0:
                    out.append({
                        "name": fullname,
                        "team": team_name,
                        "season_avg": round(avg, 3),
                        "hitless_streak": streak,
                    })
                elif debug:
                    notes.append({"name": fullname, "avg": avg, "streak": streak})

            except Exception as e:
                if debug:
                    notes.append({"name": q, "error": f"{type(e).__name__}: {e}"})

        out = sorted(out, key=lambda x: x["hitless_streak"], reverse=True)[: max(1, int(limit))]
        return {"date": self._to_datestr(date), "season": season, "items": out, "debug": notes} if debug else out

    # ---------- Placeholders (unchanged) ----------
    def league_hot_hitters(self, date=None, top_n: int = 10):
        return []

    def league_cold_hitters(self, date=None, top_n: int = 10):
        return []
