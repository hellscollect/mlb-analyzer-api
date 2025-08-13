# providers/prod_provider.py
from __future__ import annotations
import os
from datetime import date as _date
from typing import Dict, List, Any, Iterable, Optional
import requests  # ← real HTTP fetch
from models import Hitter, Pitcher  # avoid circular import with main.py

# Toggle seeded fake rows for quick testing
_FAKE_ON = os.getenv("PROD_USE_FAKE", "0") in ("1", "true", "True", "YES", "yes")

def _to_dict(x: Any) -> Dict[str, Any]:
    if hasattr(x, "model_dump"):  # pydantic v2
        return x.model_dump()
    if hasattr(x, "dict"):        # pydantic v1
        return x.dict()
    return dict(x)

class ProdProvider:
    """
    Real data provider with optional fake-data mode.
    Env:
      - PROD_USE_FAKE=1 (optional) to return seeded rows
      - DATA_API_BASE=https://your-data-api.example.com (no trailing slash)
      - DATA_API_KEY=... (optional; sent as Bearer token)
    Endpoints (assumed):
      GET {DATA_API_BASE}/hitters?date=YYYY-MM-DD&team=XXX&limit=N
      GET {DATA_API_BASE}/pitchers?date=YYYY-MM-DD&team=XXX&limit=N
    Shapes may vary; mapping is tolerant and tries multiple aliases.
    """

    def __init__(self):
        self.base = (os.getenv("DATA_API_BASE") or "").rstrip("/")
        self.key = os.getenv("DATA_API_KEY") or ""
        self._session = requests.Session()
        if self.key:
            self._session.headers.update({"Authorization": f"Bearer {self.key}"})
        self._session.headers.update({"User-Agent": "mlb-analyzer/1.0"})

    # ------------ Public methods used by main.py ------------
    def hot_streak_hitters(
        self,
        date: _date,
        min_avg: float = 0.280,
        games: int = 3,
        require_hit_each: bool = True,
        debug: bool = False,
    ):
        hitters = self.get_hitters(date)
        out: List[Dict[str, Any]] = []
        for h in hitters:
            if (h.avg or 0.0) < min_avg:
                continue
            seq = list(h.last_n_hits_each_game or [])
            if len(seq) < games:
                continue
            if require_hit_each and not all((hits or 0) >= 1 for hits in seq[:games]):
                continue
            out.append(_to_dict(h))
        return {"items": out, "meta": {"count": len(out), "min_avg": min_avg, "games": games, "require_hit_each": require_hit_each}} if debug else out

    def cold_streak_hitters(
        self,
        date: _date,
        min_avg: float = 0.275,
        games: int = 2,
        require_zero_hit_each: bool = True,
        debug: bool = False,
    ):
        hitters = self.get_hitters(date)
        out: List[Dict[str, Any]] = []
        for h in hitters:
            if (h.avg or 0.0) < min_avg:
                continue
            seq = list(h.last_n_hits_each_game or [])
            if len(seq) < games:
                continue
            if require_zero_hit_each and not all((hits or 0) == 0 for hits in seq[:games]):
                continue
            if require_zero_hit_each and (h.last_n_hitless_games or 0) < games:
                continue
            out.append(_to_dict(h))
        return {"items": out, "meta": {"count": len(out), "min_avg": min_avg, "games": games, "require_zero_hit_each": require_zero_hit_each}} if debug else out

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
    ):
        pitchers = self.get_pitchers(date)
        hot: List[Dict[str, Any]] = []
        cold: List[Dict[str, Any]] = []
        for p in pitchers:
            ks = list(p.k_per_start_last_n or [])
            ra = list(p.runs_allowed_last_n or [])
            if (p.era or 99.9) <= hot_max_era and len(ks) >= hot_last_starts and all((k or 0) >= hot_min_ks_each for k in ks[:hot_last_starts]):
                hot.append(_to_dict(p))
            if (p.era or 0.0) >= cold_min_era and len(ra) >= cold_last_starts and all((r or 0) >= cold_min_runs_each for r in ra[:cold_last_starts]):
                cold.append(_to_dict(p))
        resp = {"hot_pitchers": hot, "cold_pitchers": cold}
        if debug:
            resp["meta"] = {"counts": {"hot": len(hot), "cold": len(cold)}}
        return resp

    def cold_pitchers(self, date: _date, min_era: float = 4.60, min_runs_each: int = 3, last_starts: int = 2, debug: bool = False):
        pitchers = self.get_pitchers(date)
        out: List[Dict[str, Any]] = []
        for p in pitchers:
            ra = list(p.runs_allowed_last_n or [])
            if (p.era or 0.0) >= min_era and len(ra) >= last_starts and all((r or 0) >= min_runs_each for r in ra[:last_starts]):
                out.append(_to_dict(p))
        return {"items": out, "meta": {"count": len(out), "min_era": min_era, "min_runs_each": min_runs_each, "last_starts": last_starts}} if debug else out

    def slate_scan(self, date: _date, debug: bool = False):
        hot_hitters = self.hot_streak_hitters(date, debug=False)
        cold_hitters = self.cold_streak_hitters(date, debug=False)
        streaks = self.pitcher_streaks(date, debug=False)
        hot_pitchers = streaks.get("hot_pitchers", [])
        cold_pitchers = streaks.get("cold_pitchers", [])
        pid_index = {p["player_id"]: p for p in (hot_pitchers + cold_pitchers)}
        matchups: List[Dict[str, Any]] = []
        for h in (hot_hitters if isinstance(hot_hitters, list) else hot_hitters.get("items", [])):
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
            out["debug"] = {"counts": {k: len(out[k]) for k in out}}
        return out

    # ------------ Internal helpers ------------
    def _api_get(self, path: str, params: Dict[str, Any]) -> Any:
        if not self.base:
            # If base is missing, behave like "no data"
            return []
        url = f"{self.base}{path}"
        try:
            r = self._session.get(url, params=params, timeout=10)
            r.raise_for_status()
            data = r.json()
            # Some APIs wrap results in {"data": [...]}
            return data.get("data", data)
        except Exception as e:
            print(f"[prod_provider] GET {url} params={params} -> {type(e).__name__}: {e}")
            return []

    # ------------ Raw fetches (fake or real) ------------
    def _fetch_hitter_rows(self, game_date: _date, limit: Optional[int] = None, team: Optional[str] = None) -> Iterable[Dict[str, Any]]:
        if _FAKE_ON:
            rows = _fake_hitter_rows(game_date)
            if team: rows = [r for r in rows if (r.get("team") == team)]
            if limit: rows = rows[:limit]
            return rows
        params = {"date": game_date.isoformat()}
        if team: params["team"] = team
        if limit: params["limit"] = limit
        return self._api_get("/hitters", params)

    def _fetch_pitcher_rows(self, game_date: _date, limit: Optional[int] = None, team: Optional[str] = None) -> Iterable[Dict[str, Any]]:
        if _FAKE_ON:
            rows = _fake_pitcher_rows(game_date)
            if team: rows = [r for r in rows if (r.get("team") == team)]
            if limit: rows = rows[:limit]
            return rows
        params = {"date": game_date.isoformat()}
        if team: params["team"] = team
        if limit: params["limit"] = limit
        return self._api_get("/pitchers", params)

    # ------------ Mapping (tolerant of aliases) ------------
    def _map_hitter(self, r: Dict[str, Any]) -> Hitter:
        pid = _first(r, "player_id", "playerId", "id")
        name = _first(r, "name", "player_name", "full_name")
        team = _first(r, "team", "team_abbr", "team_code")
        opp  = _first(r, "opponent_team", "opponent", "opp", "opp_team")
        prob = _first(r, "probable_pitcher_id", "probablePitcherId", "probable_pitcher")

        avg  = _as_float(_first(r, "avg", "batting_avg", "BA"))
        obp  = _as_float(_first(r, "obp", "on_base_pct", "OBP"))
        slg  = _as_float(_first(r, "slg", "slugging", "SLG"))

        hits_each = _as_int_list(_first(r, "last_n_hits_each_game", "hits_last_n"))
        if not hits_each:
            # Try to compute from logs if present
            logs = _first(r, "game_logs", "recentGames", "recent_games") or []
            hits_each = _extract_ints_from_logs(logs, keys=("hits", "H"))

        hitless_n = _as_int(_first(r, "last_n_hitless_games", "hitless_streak")) or 0

        return Hitter(
            player_id=str(pid),
            name=name,
            team=team,
            opponent_team=opp,
            probable_pitcher_id=(str(prob) if prob is not None else None),
            avg=float(avg if avg is not None else 0.0),
            obp=obp,
            slg=slg,
            last_n_games=len(hits_each),
            last_n_hits_each_game=hits_each,
            last_n_hitless_games=hitless_n,
        )

    def _map_pitcher(self, r: Dict[str, Any]) -> Pitcher:
        pid = _first(r, "player_id", "playerId", "id")
        name = _first(r, "name", "player_name", "full_name")
        team = _first(r, "team", "team_abbr", "team_code")
        opp  = _first(r, "opponent_team", "opponent", "opp", "opp_team")

        era = _as_float(_first(r, "era", "ERA"))
        kbb = _as_float(_first(r, "kbb", "KBB"))

        ks_seq = _as_int_list(_first(r, "k_per_start_last_n", "last_n_ks", "ks_last_n"))
        ra_seq = _as_int_list(_first(r, "runs_allowed_last_n", "last_n_runs", "runs_last_n"))

        if not (ks_seq and ra_seq):
            logs = _first(r, "game_logs", "recentStarts", "recent_starts") or []
            if not ks_seq:
                ks_seq = _extract_ints_from_logs(logs, keys=("strikeouts", "SO", "k"))
            if not ra_seq:
                ra_seq = _extract_ints_from_logs(logs, keys=("earned_runs", "ER", "runs"))

        probable = bool(_first(r, "is_probable", "probable", "isProbable", "status") in (True, "Probable", "PROBABLE", "probable"))

        return Pitcher(
            player_id=str(pid),
            name=name,
            team=team,
            opponent_team=opp,
            era=float(era if era is not None else 0.0),
            kbb=kbb,
            k_per_start_last_n=ks_seq or [],
            runs_allowed_last_n=ra_seq or [],
            is_probable=probable,
        )

# -------- helpers & fake data --------
def _first(r: Dict[str, Any], *keys: str):
    for k in keys:
        if k in r and r[k] is not None:
            return r[k]
    return None

def _as_float(x: Any) -> Optional[float]:
    try:
        if x is None: return None
        if isinstance(x, (int, float)): return float(x)
        s = str(x).strip().replace("%", "")
        return float(s)
    except Exception:
        return None

def _as_int(x: Any) -> Optional[int]:
    try:
        return int(x) if x is not None else None
    except Exception:
        return None

def _as_int_list(x: Any) -> List[int]:
    if x is None: return []
    if isinstance(x, list): 
        out = []
        for v in x:
            try: out.append(int(v))
            except Exception: out.append(0)
        return out
    return []

def _extract_ints_from_logs(logs: List[Dict[str, Any]], keys: Iterable[str]) -> List[int]:
    out: List[int] = []
    for row in logs[:10]:  # cap to last ~10 entries
        val = None
        for k in keys:
            if k in row:
                val = row.get(k)
                break
        try:
            out.append(int(val if val is not None else 0))
        except Exception:
            out.append(0)
    return out

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
