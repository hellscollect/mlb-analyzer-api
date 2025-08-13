from __future__ import annotations
from typing import List, Optional, Dict, Any, Tuple
from datetime import date as _date, datetime, timedelta
import os

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

# =====================================
# App Setup
# =====================================
app = FastAPI(
    title="MLB Analyzer API",
    version="1.6.1",
    description="Hot/Cold hitters & pitchers with slate scan, matchup filters, relative dates, and advanced stats."
)

# =====================================
# Utilities: Date Handling
# =====================================
def _parse_date(d: Optional[str]) -> _date:
    if not d:
        return _date.today()
    s = d.strip().lower()
    today = _date.today()
    if s == "today":
        return today
    if s in ("yesterday", "yday"):
        return today - timedelta(days=1)
    if s in ("tomorrow", "tmrw"):
        return today + timedelta(days=1)
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return today

# =====================================
# Models
# =====================================
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
    debug: Dict[str, Any] = Field(default_factory=dict)
    hot_hitters: List[Dict[str, Any]] = Field(default_factory=list)
    cold_hitters: List[Dict[str, Any]] = Field(default_factory=list)
    hot_pitchers: List[Dict[str, Any]] = Field(default_factory=list)
    cold_pitchers: List[Dict[str, Any]] = Field(default_factory=list)
    matchups: Dict[str, List[Dict[str, Any]]] = Field(default_factory=dict)

# =====================================
# Dummy Data Provider (replace later)
# =====================================
class DataProvider:
    def get_hitters(self, game_date: _date) -> List[Hitter]:
        return []
    def get_pitchers(self, game_date: _date) -> List[Pitcher]:
        return []
    def get_probable_pitchers_by_team(self, game_date: _date) -> Dict[str, Pitcher]:
        return {p.team: p for p in self.get_pitchers(game_date) if p.is_probable}

_provider = DataProvider()

# =====================================
# Filter helpers
# =====================================
def _passes_min(value, threshold): return threshold is None or (value is not None and value >= threshold)
def _passes_max(value, threshold): return threshold is None or (value is not None and value <= threshold)

def _serialize_hitter(h: Hitter) -> Dict[str, Any]: return h.dict()
def _serialize_pitcher(p: Pitcher) -> Dict[str, Any]: return p.dict()

# =====================================
# Core filters (hot/cold hitters & pitchers)
# =====================================
def find_hot_hitters(hitters, avg_min, last_n, obp_min=None, slg_min=None, debug=False):
    dbg = {"scanned": len(hitters)}
    pool = [h for h in hitters if h.avg >= avg_min]
    dbg["after_avg"] = len(pool)
    pool = [h for h in pool if len(h.last_n_hits_each_game) >= last_n and all(x >= 1 for x in h.last_n_hits_each_game[:last_n])]
    dbg["after_hits"] = len(pool)
    pool = [h for h in pool if _passes_min(h.obp, obp_min) and _passes_min(h.slg, slg_min)]
    dbg["after_adv"] = len(pool)
    return pool, dbg if debug else {}

def find_cold_hitters(hitters, avg_min, last_n_hitless, obp_max=None, slg_max=None, debug=False):
    dbg = {"scanned": len(hitters)}
    pool = [h for h in hitters if h.avg >= avg_min]
    dbg["after_avg"] = len(pool)
    pool = [h for h in pool if h.last_n_hitless_games >= last_n_hitless]
    dbg["after_hitless"] = len(pool)
    pool = [h for h in pool if _passes_max(h.obp, obp_max) and _passes_max(h.slg, slg_max)]
    dbg["after_adv"] = len(pool)
    return pool, dbg if debug else {}

def find_hot_pitchers(pitchers, era_max, ks_each_last_n, kbb_min=None, debug=False):
    dbg = {"scanned": len(pitchers)}
    pool = [p for p in pitchers if p.era <= era_max]
    dbg["after_era"] = len(pool)
    pool = [p for p in pool if len(p.k_per_start_last_n) >= ks_each_last_n and all(k >= 6 for k in p.k_per_start_last_n[:ks_each_last_n])]
    dbg["after_ks"] = len(pool)
    pool = [p for p in pool if _passes_min(p.kbb, kbb_min)]
    dbg["after_kbb"] = len(pool)
    return pool, dbg if debug else {}

def find_cold_pitchers(pitchers, era_min, runs_each_last_n, kbb_max=None, debug=False):
    dbg = {"scanned": len(pitchers)}
    pool = [p for p in pitchers if p.era >= era_min]
    dbg["after_era"] = len(pool)
    pool = [p for p in pool if len(p.runs_allowed_last_n) >= runs_each_last_n and all(r >= 3 for r in p.runs_allowed_last_n[:runs_each_last_n])]
    dbg["after_runs"] = len(pool)
    pool = [p for p in pool if _passes_max(p.kbb, kbb_max)]
    dbg["after_kbb"] = len(pool)
    return pool, dbg if debug else {}

# =====================================
# Matchups
# =====================================
def build_matchups(hh, ch, hp, cp, provider, date_obj):
    probables_by_team = provider.get_probable_pitchers_by_team(date_obj)
    cold_pitchers_by_id = {p.player_id: p for p in cp}
    hot_pitchers_by_id = {p.player_id: p for p in hp}
    hh_vs_cp = []
    for h in hh:
        pid = h.probable_pitcher_id
        if pid and pid in cold_pitchers_by_id:
            hh_vs_cp.append({"hitter": _serialize_hitter(h), "pitcher": _serialize_pitcher(cold_pitchers_by_id[pid])})
    hp_vs_ch = []
    for h in ch:
        pid = h.probable_pitcher_id
        if pid and pid in hot_pitchers_by_id:
            hp_vs_ch.append({"pitcher": _serialize_pitcher(hot_pitchers_by_id[pid]), "hitter": _serialize_hitter(h)})
    return {"hot_hitters_vs_cold_pitchers": hh_vs_cp, "hot_pitchers_vs_cold_hitters": hp_vs_ch}

# =====================================
# Endpoints
# =====================================
@app.get("/slate_scan", response_model=SlateScanResponse)
def slate_scan(
    hot_avg_min: float = 0.280,
    hot_last_n: int = 3,
    hot_obp_min: Optional[float] = None,
    hot_slg_min: Optional[float] = None,
    cold_avg_min: float = 0.275,
    cold_last_n_hitless: int = 2,
    cold_obp_max: Optional[float] = None,
    cold_slg_max: Optional[float] = None,
    hot_era_max: float = 4.00,
    hot_ks_each_last_n: int = 3,
    hot_kbb_min: Optional[float] = None,
    cold_era_min: float = 4.60,
    cold_runs_each_last_n: int = 2,
    cold_kbb_max: Optional[float] = None,
    date: Optional[str] = None,
    debug: Optional[int] = 0
):
    d = _parse_date(date)
    hitters = _provider.get_hitters(d)
    pitchers = _provider.get_pitchers(d)
    hh, dbg_hh = find_hot_hitters(hitters, hot_avg_min, hot_last_n, hot_obp_min, hot_slg_min, debug=bool(debug))
    ch, dbg_ch = find_cold_hitters(hitters, cold_avg_min, cold_last_n_hitless, cold_obp_max, cold_slg_max, debug=bool(debug))
    hp, dbg_hp = find_hot_pitchers(pitchers, hot_era_max, hot_ks_each_last_n, hot_kbb_min, debug=bool(debug))
    cp, dbg_cp = find_cold_pitchers(pitchers, cold_era_min, cold_runs_each_last_n, cold_kbb_max, debug=bool(debug))
    matchups = build_matchups(hh, ch, hp, cp, _provider, d)
    return SlateScanResponse(
        date=d.isoformat(),
        debug={"hot_hitters": dbg_hh, "cold_hitters": dbg_ch, "hot_pitchers": dbg_hp, "cold_pitchers": dbg_cp} if debug else {},
        hot_hitters=[_serialize_hitter(x) for x in hh],
        cold_hitters=[_serialize_hitter(x) for x in ch],
        hot_pitchers=[_serialize_pitcher(x) for x in hp],
        cold_pitchers=[_serialize_pitcher(x) for x in cp],
        matchups=matchups
    )

# =====================================
# Run for local dev
# =====================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
