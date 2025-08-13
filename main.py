# main.py
from __future__ import annotations
from typing import List, Optional, Dict, Any, Tuple
from datetime import date as _date, datetime, timedelta
import os

from fastapi import FastAPI, Query
from pydantic import BaseModel, Field

app = FastAPI(
    title="MLB Analyzer API",
    version="1.6.0",
    description="Hot/Cold hitters & pitchers with slate scan and matchup filters. Supports relative dates and advanced filters."
)

# =========================
# Utilities: Date Handling
# =========================

def _parse_date(d: Optional[str]) -> _date:
    """
    Accepts: None, 'today', 'yesterday', 'tomorrow', or 'YYYY-MM-DD'.
    Falls back to today if None/empty/invalid.
    """
    if not d:
        return _date.today()
    s = d.strip().lower()
    today = _date.today()
    if s in ("today",):
        return today
    if s in ("yesterday", "yday"):
        return today - timedelta(days=1)
    if s in ("tomorrow", "tmmr", "tmrw"):
        return today + timedelta(days=1)
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return today


# =========================
# Data Models
# =========================

class Hitter(BaseModel):
    player_id: str
    name: str
    team: str
    opponent_team: Optional[str] = None
    probable_pitcher_id: Optional[str] = None  # if known pregame
    avg: float
    obp: Optional[float] = None
    slg: Optional[float] = None
    last_n_games: int = 0
    last_n_hits_each_game: List[int] = Field(default_factory=list)  # hits per game for N most recent games
    last_n_hitless_games: int = 0  # consecutive recent hitless games

class Pitcher(BaseModel):
    player_id: str
    name: str
    team: str
    opponent_team: Optional[str] = None
    k_per_start_last_n: List[int] = Field(default_factory=list)
    runs_allowed_last_n: List[int] = Field(default_factory=list)
    era: float
    kbb: Optional[float] = None  # K/BB ratio
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
    # New matchup buckets
    matchups: Dict[str, List[Dict[str, Any]]] = Field(default_factory=dict)


# =================================
# Data Provider (pluggable / safe)
# =================================

class DataProvider:
    """
    Replace these methods with your production data layer.
    Returning empty lists is intentional to avoid deployment crashes if not wired.
    """
    def get_hitters(self, game_date: _date) -> List[Hitter]:
        # TODO: Integrate your real data fetch. Must fill fields used above.
        return []

    def get_pitchers(self, game_date: _date) -> List[Pitcher]:
        # TODO: Integrate your real data fetch. Must fill fields used above.
        return []

    def get_probable_pitchers_by_team(self, game_date: _date) -> Dict[str, Pitcher]:
        """
        Convenience map of probable starting pitchers keyed by team.
        If your data source already annotates Pitcher.is_probable/opponent_team, just build from that.
        """
        probables = {p.team: p for p in self.get_pitchers(game_date) if getattr(p, "is_probable", False)}
        return probables


_provider = DataProvider()


# =========================
# Filtering Helpers
# =========================

def _passes_min(value: Optional[float], threshold: Optional[float]) -> bool:
    if threshold is None or threshold == 0:
        return True
    if value is None:
        return False
    return value >= threshold

def _passes_max(value: Optional[float], threshold: Optional[float]) -> bool:
    if threshold is None or threshold == 0:
        return True
    if value is None:
        return False
    return value <= threshold

def _serialize_hitter(h: Hitter) -> Dict[str, Any]:
    return {
        "player_id": h.player_id,
        "name": h.name,
        "team": h.team,
        "opponent_team": h.opponent_team,
        "probable_pitcher_id": h.probable_pitcher_id,
        "avg": h.avg,
        "obp": h.obp,
        "slg": h.slg,
        "last_n_games": h.last_n_games,
        "last_n_hits_each_game": h.last_n_hits_each_game,
        "last_n_hitless_games": h.last_n_hitless_games,
    }

def _serialize_pitcher(p: Pitcher) -> Dict[str, Any]:
    return {
        "player_id": p.player_id,
        "name": p.name,
        "team": p.team,
        "opponent_team": p.opponent_team,
        "era": p.era,
        "kbb": p.kbb,
        "k_per_start_last_n": p.k_per_start_last_n,
        "runs_allowed_last_n": p.runs_allowed_last_n,
        "is_probable": p.is_probable,
    }


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
    dbg = {"scanned": len(hitters), "kept_after_avg": 0, "kept_after_hits": 0, "kept_after_adv": 0}
    pool = [h for h in hitters if h.avg >= avg_min]
    dbg["kept_after_avg"] = len(pool)

    if require_hits_each_game and last_n > 0:
        tmp = []
        for h in pool:
            if len(h.last_n_hits_each_game) >= last_n and all(x >= 1 for x in h.last_n_hits_each_game[:last_n]):
                tmp.append(h)
        pool = tmp
    dbg["kept_after_hits"] = len(pool)

    tmp = []
    for h in pool:
        if not _passes_min(h.obp, obp_min): 
            continue
        if not _passes_min(h.slg, slg_min):
            continue
        tmp.append(h)
    pool = tmp
    dbg["kept_after_adv"] = len(pool)

    return pool, dbg if debug else {}

def find_cold_hitters(
    hitters: List[Hitter],
    *,
    avg_min: float,
    last_n_hitless: int,
    obp_max: Optional[float] = None,
    slg_max: Optional[float] = None,
    debug: bool = False
) -> Tuple[List[Hitter], Dict[str, Any]]:
    dbg = {"scanned": len(hitters), "kept_after_avg": 0, "kept_after_hitless": 0, "kept_after_adv": 0}
    pool = [h for h in hitters if h.avg >= avg_min]
    dbg["kept_after_avg"] = len(pool)

    if last_n_hitless > 0:
        pool = [h for h in pool if h.last_n_hitless_games >= last_n_hitless]
    dbg["kept_after_hitless"] = len(pool)

    tmp = []
    for h in pool:
        if not _passes_max(h.obp, obp_max):
            continue
        if not _passes_max(h.slg, slg_max):
            continue
        tmp.append(h)
    pool = tmp
    dbg["kept_after_adv"] = len(pool)

    return pool, dbg if debug else {}

def find_hot_pitchers(
    pitchers: List[Pitcher],
    *,
    era_max: float,
    strikeouts_each_last_n: int,
    kbb_min: Optional[float] = None,
    debug: bool = False
) -> Tuple[List[Pitcher], Dict[str, Any]]:
    dbg = {"scanned": len(pitchers), "kept_after_era": 0, "kept_after_ks": 0, "kept_after_kbb": 0}
    pool = [p for p in pitchers if p.era <= era_max]
    dbg["kept_after_era"] = len(pool)

    if strikeouts_each_last_n > 0:
        tmp = []
        for p in pool:
            if len(p.k_per_start_last_n) >= strikeouts_each_last_n and all(
                k >= 6 for k in p.k_per_start_last_n[:strikeouts_each_last_n]
            ):
                tmp.append(p)
        pool = tmp
    dbg["kept_after_ks"] = len(pool)

    tmp = []
    for p in pool:
        if not _passes_min(p.kbb, kbb_min):
            continue
        tmp.append(p)
    pool = tmp
    dbg["kept_after_kbb"] = len(pool)

    return pool, dbg if debug else {}

def find_cold_pitchers(
    pitchers: List[Pitcher],
    *,
    era_min: float,
    runs_allowed_each_last_n: int,
    kbb_max: Optional[float] = None,
    debug: bool = False
) -> Tuple[List[Pitcher], Dict[str, Any]]:
    dbg = {"scanned": len(pitchers), "kept_after_era": 0, "kept_after_runs": 0, "kept_after_kbb": 0}
    pool = [p for p in pitchers if p.era >= era_min]
    dbg["kept_after_era"] = len(pool)

    if runs_allowed_each_last_n > 0:
        tmp = []
        for p in pool:
            if len(p.runs_allowed_last_n) >= runs_allowed_each_last_n and all(
                r >= 3 for r in p.runs_allowed_last_n[:runs_allowed_each_last_n]
            ):
                tmp.append(p)
        pool = tmp
    dbg["kept_after_runs"] = len(pool)

    tmp = []
    for p in pool:
        if not _passes_max(p.kbb, kbb_max):
            continue
        tmp.append(p)
    pool = tmp
    dbg["kept_after_kbb"] = len(pool)

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
    provider: DataProvider
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Produces two buckets:
      - hot_hitters_vs_cold_pitchers
      - hot_pitchers_vs_cold_hitters
    Matching rules:
      1) Prefer explicit probable pitcher IDs on hitters (probable_pitcher_id).
      2) Else match by hitter.opponent_team -> provider.get_probable_pitchers_by_team().
      3) Else best-effort by team/opponent_team if present on pitchers.
    """
    probables_by_team = provider.get_probable_pitchers_by_team(date_obj)
    cold_pitchers_by_id = {p.player_id: p for p in cold_pitchers}
    cold_pitchers_by_team = {p.team: p for p in cold_pitchers if p.is_probable}
    hot_pitchers_by_team = {p.team: p for p in hot_pitchers if p.is_probable}

    hh_vs_cp = []
    for h in hot_hitters:
        matched_pitcher: Optional[Pitcher] = None
        # 1) direct probable pitcher id
        if h.probable_pitcher_id and h.probable_pitcher_id in cold_pitchers_by_id:
            matched_pitcher = cold_pitchers_by_id[h.probable_pitcher_id]
        # 2) opponent team + probables
        elif h.opponent_team and h.opponent_team in probables_by_team:
            p = probables_by_team[h.opponent_team]
            if p.player_id in cold_pitchers_by_id:
                matched_pitcher = cold_pitchers_by_id[p.player_id]
        # 3) fallback: match by team maps
        elif h.opponent_team and h.opponent_team in cold_pitchers_by_team:
            matched_pitcher = cold_pitchers_by_team[h.opponent_team]

        if matched_pitcher:
            hh_vs_cp.append({
                "hitter": _serialize_hitter(h),
                "pitcher": _serialize_pitcher(matched_pitcher)
            })

    hp_vs_ch = []
    # Inverse: find cold hitters facing hot probable pitchers
    for h in cold_hitters:
        matched_pitcher: Optional[Pitcher] = None
        if h.probable_pitcher_id and h.probable_pitcher_id in {p.player_id for p in hot_pitchers}:
            matched_pitcher = next((p for p in hot_pitchers if p.player_id == h.probable_pitcher_id), None)
        elif h.opponent_team and h.opponent_team in probables_by_team:
            p = probables_by_team[h.opponent_team]
            if p.player_id in {hp.player_id for hp in hot_pitchers}:
                matched_pitcher = next((hp for hp in hot_pitchers if hp.player_id == p.player_id), None)
        elif h.opponent_team and h.opponent_team in hot_pitchers_by_team:
            matched_pitcher = hot_pitchers_by_team[h.opponent_team]

        if matched_pitcher:
            hp_vs_ch.append({
                "pitcher": _serialize_pitcher(matched_pitcher),
                "hitter": _serialize_hitter(h)
            })

    return {
        "hot_hitters_vs_cold_pitchers": hh_vs_cp,
        "hot_pitchers_vs_cold_hitters": hp_vs_ch
    }


# =========================
# Endpoints
# =========================

@app.get("/hot_streak_hitters", response_model=StreakResult)
def hot_streak_hitters(
    avg_min: float = Query(0.280, description="Minimum batting average"),
    last_n: int = Query(3, description="Lookback games requiring ≥1 hit each"),
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    obp_min: Optional[float] = Query(None, description="Minimum OBP (optional)"),
    slg_min: Optional[float] = Query(None, description="Minimum SLG (optional)"),
    debug: Optional[int] = Query(0, description="Set to 1 to return counters")
):
    d = _parse_date(date)
    hitters = _provider.get_hitters(d)
    filtered, dbg = find_hot_hitters(
        hitters,
        avg_min=avg_min,
        last_n=last_n,
        require_hits_each_game=True,
        obp_min=obp_min,
        slg_min=slg_min,
        debug=bool(debug)
    )
    return StreakResult(
        debug=dbg,
        results=[_serialize_hitter(h) for h in filtered]
    )

@app.get("/cold_streak_hitters", response_model=StreakResult)
def cold_streak_hitters(
    avg_min: float = Query(0.275, description="Minimum batting average"),
    last_n_hitless: int = Query(2, description="Number of most recent games with 0 hits"),
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    obp_max: Optional[float] = Query(None, description="Maximum OBP (optional)"),
    slg_max: Optional[float] = Query(None, description="Maximum SLG (optional)"),
    debug: Optional[int] = Query(0, description="Set to 1 to return counters")
):
    d = _parse_date(date)
    hitters = _provider.get_hitters(d)
    filtered, dbg = find_cold_hitters(
        hitters,
        avg_min=avg_min,
        last_n_hitless=last_n_hitless,
        obp_max=obp_max,
        slg_max=slg_max,
        debug=bool(debug)
    )
    return StreakResult(
        debug=dbg,
        results=[_serialize_hitter(h) for h in filtered]
    )

@app.get("/pitcher_streaks", response_model=StreakResult)
def pitcher_streaks(
    era_max: float = Query(4.00, description="Maximum ERA to qualify as 'hot'"),
    strikeouts_each_last_n: int = Query(3, description="Require ≥6 K in each of the last N starts"),
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    kbb_min: Optional[float] = Query(None, description="Minimum K/BB ratio (optional)"),
    debug: Optional[int] = Query(0, description="Set to 1 to return counters")
):
    d = _parse_date(date)
    pitchers = _provider.get_pitchers(d)
    filtered, dbg = find_hot_pitchers(
        pitchers,
        era_max=era_max,
        strikeouts_each_last_n=strikeouts_each_last_n,
        kbb_min=kbb_min,
        debug=bool(debug)
    )
    return StreakResult(
        debug=dbg,
        results=[_serialize_pitcher(p) for p in filtered]
    )

@app.get("/cold_pitchers", response_model=StreakResult)
def cold_pitchers(
    era_min: float = Query(4.60, description="Minimum ERA to qualify as 'cold'"),
    runs_allowed_each_last_n: int = Query(2, description="Require ≥3 ER in each of the last N starts"),
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    kbb_max: Optional[float] = Query(None, description="Maximum K/BB ratio (optional)"),
    debug: Optional[int] = Query(0, description="Set to 1 to return counters")
):
    d = _parse_date(date)
    pitchers = _provider.get_pitchers(d)
    filtered, dbg = find_cold_pitchers(
        pitchers,
        era_min=era_min,
        runs_allowed_each_last_n=runs_allowed_each_last_n,
        kbb_max=kbb_max,
        debug=bool(debug)
    )
    return StreakResult(
        debug=dbg,
        results=[_serialize_pitcher(p) for p in filtered]
    )

@app.get("/slate_scan", response_model=SlateScanResponse)
def slate_scan(
    # HITTERS (hot)
    hot_avg_min: float = Query(0.280, description="Hot hitters: minimum AVG"),
    hot_last_n: int = Query(3, description="Hot hitters: require ≥1 hit each of last N"),
    hot_obp_min: Optional[float] = Query(None, description="Hot hitters: minimum OBP"),
    hot_slg_min: Optional[float] = Query(None, description="Hot hitters: minimum SLG"),
    # HITTERS (cold)
    cold_avg_min: float = Query(0.275, description="Cold hitters: minimum AVG"),
    cold_last_n_hitless: int = Query(2, description="Cold hitters: consecutive 0-hit games (N)"),
    cold_obp_max: Optional[float] = Query(None, description="Cold hitters: maximum OBP"),
    cold_slg_max: Optional[float] = Query(None, description="Cold hitters: maximum SLG"),
    # PITCHERS (hot)
    hot_era_max: float = Query(4.00, description="Hot pitchers: maximum ERA"),
    hot_ks_each_last_n: int = Query(3, description="Hot pitchers: require ≥6 K each of last N starts"),
    hot_kbb_min: Optional[float] = Query(None, description="Hot pitchers: minimum K/BB"),
    # PITCHERS (cold)
    cold_era_min: float = Query(4.60, description="Cold pitchers: minimum ERA"),
    cold_runs_each_last_n: int = Query(2, description="Cold pitchers: require ≥3 ER each of last N starts"),
    cold_kbb_max: Optional[float] = Query(None, description="Cold pitchers: maximum K/BB"),
    # Global
    date: Optional[str] = Query(None, description="today|yesterday|tomorrow|YYYY-MM-DD"),
    debug: Optional[int] = Query(0, description="Set to 1 to include counters")
):
    d = _parse_date(date)
    hitters = _provider.get_hitters(d)
    pitchers = _provider.get_pitchers(d)

    # hot hitters
    hh, dbg_hh = find_hot_hitters(
        hitters,
        avg_min=hot_avg_min,
        last_n=hot_last_n,
        require_hits_each_game=True,
        obp_min=hot_obp_min,
        slg_min=hot_slg_min,
        debug=bool(debug)
    )

    # cold hitters
    ch, dbg_ch = find_cold_hitters(
        hitters,
        avg_min=cold_avg_min,
        last_n_hitless=cold_last_n_hitless,
        obp_max=cold_obp_max,
        slg_max=cold_slg_max,
        debug=bool(debug)
    )

    # hot pitchers
    hp, dbg_hp = find_hot_pitchers(
        pitchers,
        era_max=hot_era_max,
        strikeouts_each_last_n=hot_ks_each_last_n,
        kbb_min=hot_kbb_min,
        debug=bool(debug)
    )

    # cold pitchers
    cp, dbg_cp = find_cold_pitchers(
        pitchers,
        era_min=cold_era_min,
        runs_allowed_each_last_n=cold_runs_each_last_n,
        kbb_max=cold_kbb_max,
        debug=bool(debug)
    )

    # Matchups
    matchups = build_matchups(
        date_obj=d,
        hot_hitters=hh,
        cold_hitters=ch,
        hot_pitchers=hp,
        cold_pitchers=cp,
        provider=_provider
    )

    return SlateScanResponse(
        date=d.isoformat(),
        debug={
            "hot_hitters": dbg_hh,
            "cold_hitters": dbg_ch,
            "hot_pitchers": dbg_hp,
            "cold_pitchers": dbg_cp
        } if debug else {},
        hot_hitters=[_serialize_hitter(x) for x in hh],
        cold_hitters=[_serialize_hitter(x) for x in ch],
        hot_pitchers=[_serialize_pitcher(x) for x in hp],
        cold_pitchers=[_serialize_pitcher(x) for x in cp],
        matchups=matchups
    )
