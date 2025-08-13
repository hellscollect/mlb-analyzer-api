from typing import Any, Dict, List, Optional
from datetime import date

class SimpleProvider:
    """
    Minimal provider that satisfies the exact contracts used by main.py.
    Returns empty-but-valid structures so your API never 500's while you wire up a real source.
    """

    # Optional knobs your main.py inspects in /provider_raw?debug=1
    base: Optional[str] = None    # upstream base URL (unset here)
    key: Optional[str] = None     # API key (unset here)

    def __init__(self) -> None:
        # Do NOT do anything that can crash here.
        pass

    # ---- private fetchers used by /provider_raw ----
    # NOTE: names and params must match what _smart_call_fetch probes for
    def _fetch_hitter_rows(self, date: date, limit: Optional[int] = None, team: Optional[str] = None) -> List[Dict[str, Any]]:
        return []  # stub: no external calls

    def _fetch_pitcher_rows(self, date: date, limit: Optional[int] = None, team: Optional[str] = None) -> List[Dict[str, Any]]:
        return []  # stub: no external calls

    # ---- public methods used by your endpoints ----
    def hot_streak_hitters(self, *, date: date, min_avg: float, games: int,
                           require_hit_each: bool, debug: bool) -> Dict[str, Any]:
        return {"items": [], "debug": {"stub": True} if debug else None}

    def cold_streak_hitters(self, *, date: date, min_avg: float, games: int,
                            require_zero_hit_each: bool, debug: bool) -> Dict[str, Any]:
        return {"items": [], "debug": {"stub": True} if debug else None}

    def pitcher_streaks(self, *, date: date, hot_max_era: float, hot_min_ks_each: int, hot_last_starts: int,
                        cold_min_era: float, cold_min_runs_each: int, cold_last_starts: int, debug: bool) -> Dict[str, Any]:
        return {
            "hot": [],
            "cold": [],
            "debug": {"stub": True} if debug else None
        }

    def cold_pitchers(self, *, date: date, min_era: float, min_runs_each: int,
                      last_starts: int, debug: bool) -> Dict[str, Any]:
        return {"items": [], "debug": {"stub": True} if debug else None}

    def slate_scan(self, *, date: date, debug: bool) -> Dict[str, Any]:
        # IMPORTANT: keys + types exactly as main.py reads
        out = {
            "hot_hitters": [],
            "cold_hitters": [],
            "hot_pitchers": [],
            "cold_pitchers": [],
            "matchups": [],
        }
        if debug:
            out["debug"] = {"stub": True}
        return out
