# providers/prod_provider.py
from __future__ import annotations
import os
from datetime import date as _date
from typing import Dict, List, Any, Iterable, Optional, Tuple
from models import Hitter, Pitcher  # avoid circular import with main.py

_FAKE_ON = os.getenv("PROD_USE_FAKE", "0") in ("1", "true", "True", "YES", "yes")

def _to_dict(x: Any) -> Dict[str, Any]:
    # pydantic v2 uses model_dump; v1 uses dict()
    if hasattr(x, "model_dump"):
        return x.model_dump()
    if hasattr(x, "dict"):
        return x.dict()
    return dict(x)

class ProdProvider:
    """
    Replace the TODO sections with your real data pulls.
    While wiring, set env PROD_USE_FAKE=1 to return seeded rows.
    Public methods return JSON-serializable dicts (not Pydantic objects).
    """

    def __init__(self):
        # TODO: init your real data clients here (DB/API/etc)
        pass

    # ---------- Public API expected by main.py (implementations added) ----------
    def hot_streak_hitters(
        self,
        date: _date,
        min_avg: float = 0.280,
        games: int = 3,
        require_hit_each: bool = True,
        debug: bool = False,
    ) -> Dict[str, Any]:
        hitters = self.get_hitters(date)
        out: List[Dict[str, Any]] = []
        for h in hitters:
            if h.avg < min_avg:
                continue
            # we only know about last_n via last_n_hits_each_game length
            seq = list(h.last_n_hits_each_game or [])
            if len(seq) < games:
                continue
            window = seq[:games]  # assume most-recent-first; adjust if needed
            if require_hit_each and not all((hits or 0) >= 1 for hits in window):
                continue
            out.append(_to_dict(h))
        resp: Dict[str, Any] = out
        if debug:
            return {"items": out, "meta": {"count": len(out), "min_avg": min_avg, "games": games, "require_hit_each": require_hit_each}}
        return out

    def cold_streak_hitters(
        self,
        date: _date,
        min_avg: float = 0.275,
        games: int = 2,
        require_zero_hit_each: bool = True,
        debug: bool = False,
    ) -> Dict[str, Any]:
        hitters = self.get_hitters(date)
        out: List[Dict[str, Any]] = []
        for h in hitters:
            # treat min_avg as "capable hitter" filter: keep if season AVG >= min_avg
            if h.avg < min_avg:
                continue
            seq = list(h.last_n_hits_each_game or [])
            if len(seq) < games:
                continue
            window = seq[:games]
            if require_zero_hit_each and not all((hits or 0) == 0 for hits in window):
                continue
            # also allow provided last_n_hitless_games as a secondary check
            if require_zero_hit_each and h.last_n_hitless_games < games:
                # if explicit counter says fewer, skip (keeps it strict)
                continue
            out.append(_to_dict(h))
        if debug:
            return {"items": out, "meta": {"count": len(out), "min_avg": min_avg, "games": games, "require_zero_hit_each": require_zero_hit_each}}
        return out

    def pitcher_streaks(
        self,
        date: _date,
        hot_max_era: float = 4.00,
        hot_min_ks_each: int = 6,
        hot_last_starts: int = 3,
        cold_min_era: float = 4.60,
        cold_min_runs_each: int = 3,
        cold_last_starts: int = 2,
        debug: bool = False,
    ) -> Dict[str, Any]:
        pitchers = self.get_pitchers(date)
        hot: List[Dict[str, Any]] = []
        cold: List[Dict[str, Any]] = []
        for p in pitchers:
            # HOT: ERA ≤ hot_max_era & Ks each of last N starts ≥ hot_min_ks_each
            ks_seq = list(p.k_per_start_last_n or [])
            if p.era <= hot_max_era and len(ks_seq) >= hot_last_starts:
                if all((k or 0) >= hot_min_ks_each for k in ks_seq[:hot_last_starts]):
                    hot.append(_to_dict(p))

            # COLD: ERA ≥ cold_min_era & runs allowed each of last N starts ≥ cold_min_runs_each
            ra_seq = list(p.runs_allowed_last_n or [])
            if p.era >= cold_min_era and len(ra_seq) >= cold_last_starts:
                if all((r or 0) >= cold_min_runs_each for r in ra_seq[:cold_last_starts]):
                    cold.append(_to_dict(p))

        resp = {"hot_pitchers": hot, "cold_pitchers": cold}
        if debug:
            resp["meta"] = {
                "counts": {"hot": len(hot), "cold": len(cold)},
                "params": {
                    "hot_max_era": hot_max_era,
                    "hot_min_ks_each": hot_min_ks_each,
                    "hot_last_starts": hot_last_starts,
                    "cold_min_era": cold_min_era,
                    "cold_min_runs_each": cold_min_runs_each,
                    "cold_last_starts": cold_last_starts,
                },
            }
        return resp

    def cold_pitchers(
        self,
        date: _date,
        min_era: float = 4.60,
        min_runs_each: int = 3,
        last_starts: int = 2,
        debug: bool = False,
    ) -> Dict[str, Any]:
        pitchers = self.get_pitchers(date)
        out: List[Dict[str, Any]] = []
        for p in pitchers:
            ra_seq = list(p.runs_allowed_last_n or [])
            if p.era >= min_era and len(ra_seq) >= last_starts:
                if all((r or 0) >= min_runs_each for r in ra_seq[:last_starts]):
                    out.append(_to_dict(p))
        if debug:
            return {"items": out, "meta": {"count": len(out), "min_era": min_era, "min_runs_each": min_runs_each, "last_starts": last_starts}}
        return out

    def slate_scan(self, date: _date, debug: bool = False) -> Dict[str, Any]:
        # Reuse the logic above to produce the four buckets
        hot_hitters = self.hot_streak_hitters(date, debug=False)
        cold_hitters = self.cold_streak_hitters(date, debug=False)
        streaks = self.pitcher_streaks(date, debug=False)
        hot_pitchers = streaks.get("hot_pitchers", [])
        cold_pitchers = streaks.get("cold_pitchers", [])

        # Simple matchup ideas: link hitter.probable_pitcher_id to pitcher.player_id
        pid_index: Dict[str, Dict[str, Any]] = {p["player_id"]: p for p in (hot_pitchers + cold_pitchers)}
        matchups: List[Dict[str, Any]] = []
        for h in hot_hitters:
            pid = h.get("probable_pitcher_id")
            if pid and pid in pid_index:
                p = pid_index[pid]
                matchups.append({
                    "hitter_id": h["player_id"],
                    "hitter_name": h["name"],
                    "hitter_team": h["team"],
                    "pitcher_id": p["player_id"],
                    "pitcher_name": p["name"],
                    "pitcher_team": p["team"],
                    "opponent_team": h.get("opponent_team"),
                    "note": "Hot hitter vs probable pitcher",
                })

        out = {
            "hot_hitters": hot_hitters if isinstance(hot_hitters, list) else hot_hitters.get("items", []),
            "cold_hitters": cold_hitters if isinstance(cold_hitters, list) else cold_hitters.get("items", []),
            "hot_pitchers": hot_pitchers,
            "cold_pitchers": cold_pitchers,
            "matchups": matchups,
        }
        if debug:
            out["debug"] = {"counts": {k: len(out[k]) for k in ["hot_hitters", "cold_hitters", "hot_pitchers", "cold_pitchers", "matchups"]}}
        return out

    # ---------- Internal helpers ----------
    def get_hitters(self, game_date: _date) -> List[Hitter]:
        rows = self._fetch_hitter_rows(game_date)
        return [self._map_hitter(row) for row in rows if row]

    def get_pitchers(self, game_date: _date) -> List[Pitcher]:
        rows = self._fetch_pitcher_rows(game_date)
        return [self._map_pitcher(row) for row in rows if row]

    # ---------- Replace these with your REAL fetches ----------
    def _fetch_hitter_rows(
        self,
        game_date: _date,
        limit: Optional[int] = None,
        team: Optional[str] = None,
    ) -> Iterable[Dict[str, Any]]:
        if _FAKE_ON:
            rows = _fake_hitter_rows(game_date)
            if team:
                rows = [r for r in rows if r.get("team") == team]
            if limit:
                rows = rows[:limit]
            return rows
        # TODO: replace with real data source (DB/API)
        return []

    def _fetch_pitcher_rows(
        self,
        game_date: _date,
        limit: Optional[int] = None,
        team: Optional[str] = None,
    ) -> Iterable[Dict[str, Any]]:
        if _FAKE_ON:
            rows = _fake_pitcher_rows(game_date)
            if team:
                rows = [r for r in rows if r.get("team") == team]
            if limit:
                rows = rows[:limit]
            return rows
        # TODO: replace with real data source (DB/API)
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
