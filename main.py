from __future__ import annotations
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta, date as _date
import importlib
import os

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

try:
    # Python 3.9+ standard library
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # Fallback handled below

app = FastAPI(
    title="MLB Analyzer API",
    version="1.6.3",
    description="Hot/Cold hitters & pitchers with slate scan, matchup filters, relative dates, advanced filters, and timezone-aware dates."
)

# =========================
# Timezone + Date Handling
# =========================

DEFAULT_TZ = "America/New_York"

def _safe_zoneinfo(tz_name: str):
    if ZoneInfo is None:
        # Should not happen on modern Python, but guard anyway
        raise RuntimeError("zoneinfo not available in this Python runtime.")
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo(DEFAULT_TZ)

def _today_in_tz(tz: str) -> _date:
    z = _safe_zoneinfo(tz)
    return datetime.now(z).date()

def _parse_date(d: Optional[str], tz: str) -> _date:
    """
    Accepts: None, 'today', 'yesterday', 'tomorrow', or 'YYYY-MM-DD'.
    Interprets relative words in the provided IANA timezone (default America/New_York).
    Falls back to 'today' in tz on invalid input.
    """
    base_today = _today_in_tz(tz)
    if not d:
        return base_today
    s = d.strip().lower()
    if s in ("today",):
        return base_today
    if s in ("yesterday", "yday"):
        return base_today - timedelta(days=1)
    if s in ("tomorrow", "tmrw", "tmmr"):
        return base_today + timedelta(days=1)
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return base_today

# =========================
# Data Models
# =========================

class Hitter(BaseModel):
    player_id: str
    name: str
    team: str
    opponent_team: Optional[str] = None
    probable_pitcher_id: Optional[str] = None
    avg: float
    obp: Optional[float] = None
    slg: Optional[float] = None
    last_n_games: int = 0
    last_n_hits_each_game: List[int] = Field(default_factory=list)
    last_n_hitless_games: int = 0

class Pitcher(BaseModel):
    player_id: str
    name: str
    team: str
    opponent_team: Optional[str] = None
    era: float
    kbb: Optional[float] = None
    k_per_start_last_n: List[int] = Field(default_factory=list)
    runs_allowed_last_n: List[int] = Field(default_factory=list)
    is_probable: bool = False

class StreakResult(BaseModel):
    debug: Dict[str, Any] = Field(default_factory=dict)
    results: List[Dict[str, Any]] = Field(default_factory=list)

class SlateScanResponse(BaseModel):
    date: str
    tz: str
    debug: Dict[str, Any] = Field(default_factory=dict)
    hot_hitters: List[Dict[str, Any]] = Field(default_factory=list)
    cold_hitters: List[Dict[str, Any]] = Field(default_factory=list)
    hot_pitchers: List[Dict[str, Any]] = Field(default_factory=list)
    cold_pitchers: List[Dict[str, Any]] = Field(default_factory=list)
    matchups: Dict[str, List[Dict[str, Any]]] = Field(default_factory=dict)

class HealthResponse(BaseModel):
    server_time_utc: str
    date_in_tz: str
    tz: str
    provider_loaded: bool
    provider_class: Optional[str] = None
    note: Optional[str] = None

# =================================
# Data Provider (pluggable)
# =================================

class DataProvider:
    """Interface / default no-op provider."""
    def get_hitters(self, game_date: _date) -> List[Hitter]: return []
    def get_pitchers(self, game_date: _date) -> List[Pitcher]: return []
    def get_probable_pitchers_by_team(self, game_date: _date) -> Dict[str, Pitcher]:
        return {p.team: p for p in self.get_pitchers(game_date) if p.is_probable}

def _load_provider_from_env() -> Tuple[DataProvider, Dict[str, Any]]:
    """
    Dynamically load a provider using MLB_PROVIDER env var formatted as 'module.path:ClassName'.
    Example: MLB_PROVIDER='myapp.data_provider:ProdProvider'
    """
    meta = {"env": os.environ.get("MLB_PROVIDER"), "loaded": False, "error": None, "class": None}
    env_val = os.environ.get("MLB_PROVIDER")
    if not env_val:
        return DataProvider(), meta
    try:
        if ":" not in env_val:
            raise ValueError("MLB_PROVIDER must be 'module.path:ClassName'")
        module_name, class_name = env_val.split(":", 1)
        mod = importlib.import_module(module_name)
        cls = getattr(mod, class_name)
        inst = cls()  # must be compatible with DataProvider interface
        if not hasattr(inst, "get_hitters") or not hasattr(inst, "get_pitchers"):
            raise TypeError("Loaded provider does not implement required methods.")
        meta["loaded"] = True
        meta["class"] = f"{module_name}:{class_name}"
        return inst, meta
    except Exception as e:
        meta["error"] = repr(e)
        return DataProvider(), meta

_provider, _provider_meta = _load_provider_from_env()

# =========================
# Filtering Helpers
# =========================

def _passes_min(value: Optional[float], threshold: Optional[float]) -> bool:
    return threshold is None or (value is not None and value >= threshold)

def _passes_max(value: Optional[float], threshold: Optional[float]) -> bool:
    return threshold is None or (value is not None and value <= threshold)

def _serialize_hitter(h: Hitter) -> Dict[str, Any]:
    return h.dict()

def _serialize_pitcher(p: Pitcher) -> Dict[str, Any]:
    return p.dict()

# =========================
# Business Logic
# =========================

def find_hot_hitters(
    hitters: List[Hitter],
    *,
    avg_min: float,
    last_n: int,
    require_hits_each_game: bool = True,
    obp_min: Optional[float] = None,
    slg_min: Optional[float] = None,
    debug: bool = False
) -> Tuple[List[Hitter], Dict[str, Any]]:
    dbg = {"scanned": len(hitters)}
    pool = [h for h in hitters if h.avg >= avg_min]
    dbg["after_avg"] = len(pool)
    if require_hits_each_game and last_n > 0:
        pool = [h for h in pool
                if len(h.last_n_hits_each_game) >= last_n
                and all(x >= 1 for x in h.last_n_hits_each_game[:last_n])]
    dbg["after_hits"] = len(pool)
    pool = [h for h in pool if _passes_min(h.obp, obp_min) and _passes_min(h.slg, slg_min)]
    dbg["after_adv"] = len(pool)
    return pool, dbg if debug else {}

def find_cold_hitters(
    hitters: List[Hitter], *,
    avg_min: float,
    last_n_hitless: int,
    obp_max: Optional[float] = None,
    slg_max: Optional[float] = None,
    debug: bool = False
) -> Tuple[List[Hitter], Dict[str, Any]]:
    dbg = {"scanned": len(hitters)}
    pool = [h for h in hitters if h.avg >= avg_min]
    dbg["after_avg"] = len(pool)
    if last_n_hitless > 0:
        pool = [h for h in pool if h.last_n_hitless_games >= last_n_hitless]
    dbg["after_hitless"] = len(pool)
    pool = [h for h in pool if _passes_max(h.obp, obp_max) and _passes_max(h.slg, slg_max)]
    dbg["after_adv"] = len(pool)
    return pool, dbg if debug else {}

def find_hot_pitchers(
    pitchers: List[Pitcher], *,
    era_max: float,
    strikeouts_each_last_n: int,
    kbb_min: Optional[float] = None,
    debug: bool = False
) -> Tuple[List[Pitcher], Dict[str, Any]]:
    dbg = {"scanned": len(pitchers)}
    pool = [p for p in pitchers if p.era <= era_max]
    dbg["after_era"] = len(pool)
    if strikeouts_each_last_n > 0:
        pool = [p for p in pool
                if len(p.k_per_start_last_n) >= strikeouts_each_last_n
                and all(k >= 6 for k in p.k_per_start_last_n[:strikeouts_each_last_n])]
    dbg["after_ks"] = len(pool)
    pool = [p for p in pool if _passes_min(p.kbb, kbb_min)]
    dbg["after_kbb"] = len(pool)
    return pool, dbg if debug else {}

def find_cold_pitchers(
    pitchers: List[Pitcher], *,
    era_min: float,
    runs_allowed_each_last_n: int,
    kbb_max: Optional[float] = None,
    debug: bool = False
) -> Tuple[List[Pitcher], Dict[str, Any]]:
    dbg = {"scanned": len(pitchers)}
    pool = [p for p in pitchers if p.era >= era_min]
    dbg["after_era"] = len(pool)
    if runs_allowed_each_last_n > 0:
        pool = [p for p in pool
                if len(p.runs_allowed_last_n) >= runs_allowed_each_last_n
                and all(r >= 3 for r in p.runs_allowed_last_n[:runs_allowed_each_last_n])]
    dbg["after_runs"] = len(pool)
    pool = [p for p in pool if _passes_max(p.kbb, kbb_max)]
    dbg["after_kbb"] = len(pool)
    return pool, dbg if debug else {}

# =========================
# Matchup Builder
# =========================

def build_matchups(
    *, date_obj: _date,
    hot_hitters: List[Hitter],
    cold_hitters: List[Hitter],
    hot_pitchers: List[Pitcher],
    cold_pitchers: List[Pitcher],
) -> Dict[str, List[Dict[str, Any]]]:
    cold_pitchers_by_id = {p.player_id: p for p in cold_pitchers}
    hot_pitchers_by_id = {p.player_id: p for p in hot_pitchers}
    probables_by_team = _provider.get_probable_pitchers_by_team(date_obj)

    hh_vs_cp = []
    for h in hot_hitters:
        mp = None
        if h.probable_pitcher_id and h.probable_pitcher_id in cold_pitchers_by_id:
            mp = cold_pitchers_by_id[h.probable_pitcher_id]
        elif h.opponent_team:
            p = probables_by_team.get(h.opponent_team)
            if p and p.player_id in cold_pitchers_by_id:
                mp = cold_pitchers_by_id[p.player_id]
        if mp:
            hh_vs_cp.append({"hitter": _serialize_hitter(h), "pitcher": _serialize_pitcher(mp)})

    hp_vs_ch = []
    for h in cold_hitters:
        mp = None
        if h.probable_pitcher_id and h.probable_pitcher_id in hot_pitchers_by_id:
            mp = hot_pitchers_by_id[h.probable_pitcher_id]
        elif h.opponent_team:
            p = probables_by_team.get(h.opponent_team)
            if p and p.player_id in hot_pitchers_by_id:
                mp = hot_pitchers_by_id[p.player_id]
        if mp:
            hp_vs_ch.append({"pitcher": _serialize_pitcher(mp), "hitter": _serialize_hitter(h)})

    return {
        "hot_hitters_vs_cold_pitchers": hh_vs_cp,
        "hot_pitchers_vs_cold_hitters": hp_vs_ch
    }

# =========================
# Endpoints
# =========================

@app.get("/health", response_model=HealthResponse, operation_id="getHealth")
def health(tz: str = Query(DEFAULT_TZ, description="IANA timezone used for relative dates")):
    try:
        date_tz = _today_in_tz(tz)
    except Exception:
        tz = DEFAULT_TZ
        date_tz = _today_in_tz(tz)
    return HealthResponse(
        server_time_utc=datetime.utcnow().isoformat() + "Z",
        date_in_tz=date_tz.isoformat(),
        tz=tz,
        provider_loaded=_provider_meta.get("loaded", False),
        provider_class=_provider_meta.get("class"),
        note=("Using default no-op provider; set MLB_PROVIDER='module:Class' to load your data layer."
              if not _provider_meta.get("loaded") else None)
    )

@app.get("/hot_streak_hitters", response_model=StreakResult, operation_id="getHotStreakHitters")
def hot_streak_hitters(
    avg_min: float = Query(0.280, description="Minimum batting average"),
    last_n: int = Query(3, description="Lookback games requiring ≥1 hit each"),
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    obp_min: Optional[float] = Query(None, description="Minimum OBP (optional)"),
    slg_min: Optional[float] = Query(None, description="Minimum SLG (optional)"),
    tz: str = Query(DEFAULT_TZ, description="IANA timezone for relative dates"),
    debug: Optional[int] = Query(0, description="Set to 1 to return counters")
):
    d = _parse_date(date, tz)
    hitters = _provider.get_hitters(d)
    filtered, dbg = find_hot_hitters(
        hitters, avg_min=avg_min, last_n=last_n,
        require_hits_each_game=True, obp_min=obp_min, slg_min=slg_min, debug=bool(debug)
    )
    out_dbg = {"counters": dbg}
    if not _provider_meta.get("loaded"):
        out_dbg["provider"] = _provider_meta
    return StreakResult(debug=out_dbg if debug else {}, results=[_serialize_hitter(h) for h in filtered])

@app.get("/cold_streak_hitters", response_model=StreakResult, operation_id="getColdStreakHitters")
def cold_streak_hitters(
    avg_min: float = Query(0.275, description="Minimum batting average"),
    last_n_hitless: int = Query(2, description="Number of most recent games with 0 hits"),
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    obp_max: Optional[float] = Query(None, description="Maximum OBP (optional)"),
    slg_max: Optional[float] = Query(None, description="Maximum SLG (optional)"),
    tz: str = Query(DEFAULT_TZ, description="IANA timezone for relative dates"),
    debug: Optional[int] = Query(0, description="Set to 1 to return counters")
):
    d = _parse_date(date, tz)
    hitters = _provider.get_hitters(d)
    filtered, dbg = find_cold_hitters(
        hitters, avg_min=avg_min, last_n_hitless=last_n_hitless,
        obp_max=obp_max, slg_max=slg_max, debug=bool(debug)
    )
    out_dbg = {"counters": dbg}
    if not _provider_meta.get("loaded"):
        out_dbg["provider"] = _provider_meta
    return StreakResult(debug=out_dbg if debug else {}, results=[_serialize_hitter(h) for h in filtered])

@app.get("/pitcher_streaks", response_model=StreakResult, operation_id="getPitcherStreaks")
def pitcher_streaks(
    era_max: float = Query(4.00, description="Maximum ERA to qualify as 'hot'"),
    strikeouts_each_last_n: int = Query(3, description="Require ≥6 K in each of the last N starts"),
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    kbb_min: Optional[float] = Query(None, description="Minimum K/BB ratio (optional)"),
    tz: str = Query(DEFAULT_TZ, description="IANA timezone for relative dates"),
    debug: Optional[int] = Query(0, description="Set to 1 to return counters")
):
    d = _parse_date(date, tz)
    pitchers = _provider.get_pitchers(d)
    filtered, dbg = find_hot_pitchers(
        pitchers, era_max=era_max, strikeouts_each_last_n=strikeouts_each_last_n,
        kbb_min=kbb_min, debug=bool(debug)
    )
    out_dbg = {"counters": dbg}
    if not _provider_meta.get("loaded"):
        out_dbg["provider"] = _provider_meta
    return StreakResult(debug=out_dbg if debug else {}, results=[_serialize_pitcher(p) for p in filtered])

@app.get("/cold_pitchers", response_model=StreakResult, operation_id="getColdPitchers")
def cold_pitchers(
    era_min: float = Query(4.60, description="Minimum ERA to qualify as 'cold'"),
    runs_allowed_each_last_n: int = Query(2, description="Require ≥3 ER in each of the last N starts"),
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    kbb_max: Optional[float] = Query(None, description="Maximum K/BB ratio (optional)"),
    tz: str = Query(DEFAULT_TZ, description="IANA timezone for relative dates"),
    debug: Optional[int] = Query(0, description="Set to 1 to return counters")
):
    d = _parse_date(date, tz)
    pitchers = _provider.get_pitchers(d)
    filtered, dbg = find_cold_pitchers(
        pitchers, era_min=era_min, runs_allowed_each_last_n=runs_allowed_each_last_n,
        kbb_max=kbb_max, debug=bool(debug)
    )
    out_dbg = {"counters": dbg}
    if not _provider_meta.get("loaded"):
        out_dbg["provider"] = _provider_meta
    return StreakResult(debug=out_dbg if debug else {}, results=[_serialize_pitcher(p) for p in filtered])

@app.get("/slate_scan", response_model=SlateScanResponse, operation_id="getSlateScan")
def slate_scan(
    hot_avg_min: float = Query(0.280, description="Hot hitters: minimum AVG"),
    hot_last_n: int = Query(3, description="Hot hitters: require ≥1 hit each of last N"),
    hot_obp_min: Optional[float] = Query(None, description="Hot hitters: minimum OBP"),
    hot_slg_min: Optional[float] = Query(None, description="Hot hitters: minimum SLG"),
    cold_avg_min: float = Query(0.275, description="Cold hitters: minimum AVG"),
    cold_last_n_hitless: int = Query(2, description="Cold hitters: consecutive 0-hit games (N)"),
    cold_obp_max: Optional[float] = Query(None, description="Cold hitters: maximum OBP"),
    cold_slg_max: Optional[float] = Query(None, description="Cold hitters: maximum SLG"),
    hot_era_max: float = Query(4.00, description="Hot pitchers: maximum ERA"),
    hot_ks_each_last_n: int = Query(3, description="Hot pitchers: require ≥6 K each of last N starts"),
    hot_kbb_min: Optional[float] = Query(None, description="Hot pitchers: minimum K/BB"),
    cold_era_min: float = Query(4.60, description="Cold pitchers: minimum ERA"),
    cold_runs_each_last_n: int = Query(2, description="Cold pitchers: require ≥3 ER each of last N starts"),
    cold_kbb_max: Optional[float] = Query(None, description="Cold pitchers: maximum K/BB"),
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    tz: str = Query(DEFAULT_TZ, description="IANA timezone for relative dates"),
    debug: Optional[int] = Query(0, description="Set to 1 to include counters")
):
    d = _parse_date(date, tz)
    hitters = _provider.get_hitters(d)
    pitchers = _provider.get_pitchers(d)

    hh, dbg_hh = find_hot_hitters(
        hitters, avg_min=hot_avg_min, last_n=hot_last_n,
        require_hits_each_game=True, obp_min=hot_obp_min, slg_min=hot_slg_min, debug=bool(debug)
    )
    ch, dbg_ch = find_cold_hitters(
        hitters, avg_min=cold_avg_min, last_n_hitless=cold_last_n_hitless,
        obp_max=cold_obp_max, slg_max=cold_slg_max, debug=bool(debug)
    )
    hp, dbg_hp = find_hot_pitchers(
        pitchers, era_max=hot_era_max, strikeouts_each_last_n=hot_ks_each_last_n,
        kbb_min=hot_kbb_min, debug=bool(debug)
    )
    cp, dbg_cp = find_cold_pitchers(
        pitchers, era_min=cold_era_min, runs_allowed_each_last_n=cold_runs_each_last_n,
        kbb_max=cold_kbb_max, debug=bool(debug)
    )

    matchups = build_matchups(
        date_obj=d, hot_hitters=hh, cold_hitters=ch, hot_pitchers=hp, cold_pitchers=cp
    )

    dbg_payload: Dict[str, Any] = {}
    if debug:
        dbg_payload = {
            "hot_hitters": dbg_hh,
            "cold_hitters": dbg_ch,
            "hot_pitchers": dbg_hp,
            "cold_pitchers": dbg_cp
        }
        if not _provider_meta.get("loaded"):
            dbg_payload["provider"] = _provider_meta

    return SlateScanResponse(
        date=d.isoformat(),
        tz=tz,
        debug=dbg_payload,
        hot_hitters=[_serialize_hitter(x) for x in hh],
        cold_hitters=[_serialize_hitter(x) for x in ch],
        hot_pitchers=[_serialize_pitcher(x) for x in hp],
        cold_pitchers=[_serialize_pitcher(x) for x in cp],
        matchups=matchups
    )

# =========================
# Local / Render entrypoint
# =========================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
