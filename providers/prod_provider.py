# providers/prod_provider.py
from __future__ import annotations
from datetime import date as _date
from typing import Dict, List, Any, Iterable, Optional
from main import Hitter, Pitcher

class ProdProvider:
    """
    Replace the TODO sections with your real data pulls.
    Must return Hitter/Pitcher Pydantic objects (not dicts).
    """

    # ---------- OPTIONAL: lightweight init (APIs/DB clients, etc.) ----------
    def __init__(self):
        # TODO: init your data clients here (e.g., DB pool, API client, etc.)
        # self.db = connect(...)
        # self.api = MyApi(token=os.getenv("MY_API_TOKEN"))
        pass

    # ---------- Public API expected by main.py ----------
    def get_hitters(self, game_date: _date) -> List[Hitter]:
        rows = self._fetch_hitter_rows(game_date)
        return [self._map_hitter(row) for row in rows if row]  # filter Nones defensively

    def get_pitchers(self, game_date: _date) -> List[Pitcher]:
        rows = self._fetch_pitcher_rows(game_date)
        return [self._map_pitcher(row) for row in rows if row]

    def get_probable_pitchers_by_team(self, game_date: _date) -> Dict[str, Pitcher]:
        """Optional but helps matchups; if your source marks probables, return them here."""
        pitchers = self.get_pitchers(game_date)
        return {p.team: p for p in pitchers if p.is_probable}

    # ---------- Replace these with your real fetches ----------
    def _fetch_hitter_rows(self, game_date: _date) -> Iterable[Dict[str, Any]]:
        """
        TODO: return an iterable of dict-like rows for the date.
        Each row must contain fields enough to build Hitter (see _map_hitter).
        """
        # Example skeletons:
        # return self.db.query("SELECT ... WHERE game_date = %s", (game_date,))
        # return self.api.get_hitters(game_date.isoformat())
        return []

    def _fetch_pitcher_rows(self, game_date: _date) -> Iterable[Dict[str, Any]]:
        """
        TODO: return an iterable of dict-like rows for the date.
        Each row must contain fields enough to build Pitcher (see _map_pitcher).
        """
        # Example skeletons:
        # return self.db.query("SELECT ... WHERE game_date = %s", (game_date,))
        # return self.api.get_pitchers(game_date.isoformat())
        return []

    # ---------- Map raw rows to API models (edit these to match your schema) ----------
    def _map_hitter(self, r: Dict[str, Any]) -> Hitter:
        """
        Map your row -> Hitter.
        REQUIRED: player_id, name, team, avg
        Optional fields should be None if missing.
        """
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
        """
        Map your row -> Pitcher.
        REQUIRED: player_id, name, team, era
        """
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

# -------- helpers --------
def _safe_float(x: Optional[Any]) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except Exception:
        return None
