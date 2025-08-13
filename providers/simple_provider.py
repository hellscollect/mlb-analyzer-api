from typing import Any, Dict, List, Optional
from datetime import date

class SimpleProvider:
    """
    Minimal provider that satisfies main.py and returns valid empty data.
    This keeps the API stable while you wire a real data source later.
    """

    base: Optional[str] = None  # upstream base URL (unset here)
    key: Optional[str] = None   # API key (unset here)

    def __init__(self) -> None:
        # Do not crash here.
        pass

    # ---- private fetchers used by /provider_raw via _smart_call_fetch ----
    def _fetch_hitter_rows(self, date: date, limit: Optional[int] = None, team: Optional[str] = None) -> List[Dict[str, Any]]:
        return []

    def _fetch_pitcher_rows(self, date: date, limit: Optional[int] = None, team: Optional[str] = None) -> List[Dict[str, Any]]:
        return []

    # ---- public methods used by endpoints ----
    def hot_streak_hitters(self, *, date: date, min_avg: float, games: int, require_hit_each: bool, debug: bool) -> Dict[str, Any]:
        return {"items": [], "debug": {"stub": True} if debug else None}

    def cold_streak_hitters(self, *, date: date, min_avg: float, games: int, require_zero_hit_each: bool, debug: bool) -> Dict[str, Any]:
        return {"items": [], "debug": {"stub": True} if debug else None}

    def pitcher_streaks(self, *, date: date, hot_max_era: float, hot_min_ks_each: int, hot_last_starts: int,
                        cold_min_era: float, cold_min_runs_each: int, cold_last_starts: int, debug: bool) -> Dict[str, Any]:
        return {"hot": [], "cold": [], "debug": {"stub": True} if debug else None}

    def cold_pitchers(self, *, date: date, min_era: float, min_runs_each: int, last_starts: int, debug: bool) -> Dict[str, Any]:
        return {"items": [], "debug": {"stub": True} if debug else None}

    def slate_scan(self, *, date: date, debug: bool) -> Dict[str, Any]:
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
