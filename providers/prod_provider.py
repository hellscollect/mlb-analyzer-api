# providers/prod_provider.py
from __future__ import annotations
import os
from datetime import date as _date
from typing import Dict, List, Any, Iterable, Optional
from models import Hitter, Pitcher  # avoid circular import with main.py

_FAKE_ON = os.getenv("PROD_USE_FAKE", "0") in ("1", "true", "True", "YES", "yes")

class ProdProvider:
    """
    Replace the TODO sections with your real data pulls.
    While wiring, you can set env PROD_USE_FAKE=1 to return seeded rows.
    Must return Hitter/Pitcher Pydantic objects (not dicts) from the public methods.
    """

    def __init__(self):
        # TODO: init your real data clients here (DB/API/etc)
        pass

    # ---------- Public API expected by main.py ----------
    def get_hitters(self, game_date: _date) -> List[Hitter]:
        rows = self._fetch_hitter_rows(game_date)
        return [self._map_hitter(row) for row in rows if row]

    def get_pitchers(self, game_date: _date) -> List[Pitcher]:
        rows = self._fetch_pitcher_rows(game_date)
        return [self._map_pitcher(row) for row in rows if row]

    def get_probable_pitchers_by_team(self, game_date: _date) -> Dict[str, Pitcher]:
        pitchers = self.get_pitchers(game_date)
        return {p.team: p for p in pitchers if p.is_probable}

    # ---------- Replace these with your REAL fetches ----------
    def _fetch_hitter_rows(
        self,
        game_date: _date,
        limit: Optional[int] = None,
        team: Optional[str] = None,
    ) -> Iterable[Dict[str, Any]]:
        """
        Return iterable of dict-like rows describing hitters for the given date.
        Required keys for mapping: player_id, name, team, avg
        """
        if _FAKE_ON:
            rows = _fake_hitter_rows(game_date)
            if team:
                rows = [r for r in rows if r.get("team") == team]
            if limit:
                rows = rows[:limit]
            return rows

        # TODO: replace with real data source (DB/API)
        # Example:
        # return self.api.get_hitters(game_date.isoformat(), team=team, limit=limit)
        return []

    def _fetch_pitcher_rows(
        self,
        game_date: _date,
        limit: Optional[int] = None,
        team: Optional[str] = None,
    ) -> Iterable[Dict[str, Any]]:
        """
        Return iterable of dict-like rows describing pitchers for the given date.
        Required keys for mapping: player_id, name, team, era
        """
        if _FAKE_ON:
            rows = _fake_pitcher_rows(game_date)
            if team:
                rows = [r for r in rows if r.get("team") == team]
            if limit:
                rows = rows[:limit]
            return rows

        # TODO: replace with real data source (DB/API)
        # Example:
        # return self.api.get_pitchers(game_date.isoformat(), team=team, limit=limit)
        return []

    # ---------- Map raw rows to API models ----------
    def _map_hitter(self, r: Dict[str, Any]) -> Hitter:
        return Hitter(
            player_id=str(r["player_id"]),
            name=r["name"],
            team=r["team"],
            opponent_team=r.get("opponent_team"),
            probable_pitcher_id=r.get("probable_pitcher_id"),
            avg=float(r["avg"]),
            obp=_safe_float(r.get("obp")),
            slg=_safe_float(r.get("slg")),
            last_n_games=int(r.get("last_n_games", 0)),
            last_n_hits_each_game=list(r.get("last_n_hits_each_game", [])),
            last_n_hitless_games=int(r.get("last_n_hitless_games", 0)),
        )

    def _map_pitcher(self, r: Dict[str, Any]) -> Pitcher:
        return Pitcher(
            player_id=str(r["player_id"]),
            name=r["name"],
            team=r["team"],
            opponent_team=r.get("opponent_team"),
            era=float(r["era"]),
            kbb=_safe_float(r.get("kbb")),
            k_per_start_last_n=list(r.get("k_per_start_last_n", [])),
            runs_allowed_last_n=list(r.get("runs_allowed_last_n", [])),
            is_probable=bool(r.get("is_probable", False)),
        )


# -------- helpers & fake data --------
def _safe_float(x: Optional[Any]) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except Exception:
        return None

def _fake_hitter_rows(game_date: _date) -> List[Dict[str, Any]]:
    # Minimal but realistic sample rows; adjust as needed
    return [
        {
            "player_id": "h_1001",
            "name": "Aaron Example",
            "team": "NYY",
            "opponent_team": "BOS",
            "probable_pitcher_id": "p_9001",
            "avg": 0.312,
            "obp": 0.389,
            "slg": 0.571,
            "last_n_games": 3,
            "last_n_hits_each_game": [2, 1, 1],
            "last_n_hitless_games": 0,
        },
        {
            "player_id": "h_1002",
            "name": "Mookie Sample",
            "team": "LAD",
            "opponent_team": "SF",
            "probable_pitcher_id": "p_9002",
            "avg": 0.298,
            "obp": 0.364,
            "slg": 0.520,
            "last_n_games": 3,
            "last_n_hits_each_game": [1, 2, 0],
            "last_n_hitless_games": 1,
        },
    ]

def _fake_pitcher_rows(game_date: _date) -> List[Dict[str, Any]]:
    return [
        {
            "player_id": "p_9001",
            "name": "Gerrit Sample",
            "team": "BOS",
            "opponent_team": "NYY",
            "era": 3.21,
            "kbb": 4.1,
            "k_per_start_last_n": [7, 8, 6],
            "runs_allowed_last_n": [1, 3, 2],
            "is_probable": True,
        },
        {
            "player_id": "p_9002",
            "name": "Logan Demo",
            "team": "SF",
            "opponent_team": "LAD",
            "era": 3.85,
            "kbb": 3.6,
            "k_per_start_last_n": [6, 6, 5],
            "runs_allowed_last_n": [2, 1, 4],
            "is_probable": False,
        },
    ]
