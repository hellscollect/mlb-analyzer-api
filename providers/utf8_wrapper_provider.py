# providers/utf8_wrapper_provider.py
from __future__ import annotations

from datetime import date as date_cls
from typing import Any, Dict, List, Optional, Tuple, Union

# your actual provider
from providers.statsapi_provider import StatsApiProvider


# ---------------------------
# UTF-8 washing helpers
# ---------------------------
def _looks_mojibake(s: str) -> bool:
    return any(seq in s for seq in ("Ã", "�"))

def _fix_text(s: str) -> str:
    if not s or not _looks_mojibake(s):
        return s
    try:
        return s.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
    except Exception:
        return s

def _wash_utf8(obj: Any) -> Any:
    if isinstance(obj, str):
        return _fix_text(obj)
    if isinstance(obj, dict):
        return {k: _wash_utf8(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_wash_utf8(v) for v in obj]
    if isinstance(obj, tuple):
        return tuple(_wash_utf8(v) for v in obj)
    return obj


# ---------------------------
# Argument adapters
# ---------------------------
def _date_to_str(d: Union[str, date_cls, None]) -> Optional[str]:
    if d is None:
        return None
    if isinstance(d, str):
        return d
    if isinstance(d, date_cls):
        return d.isoformat()
    return str(d)

def _attempt_calls(calls: List[Tuple[Any, tuple, dict]]):
    """
    Try a sequence of call variants until one works. Only swallows TypeError
    raised by signature mismatch; other exceptions bubble up to show real errors.
    """
    last_te: Optional[TypeError] = None
    for fn, args, kwargs in calls:
        try:
            return fn(*args, **kwargs)
        except TypeError as te:
            last_te = te
            continue
    if last_te:
        raise last_te
    raise RuntimeError("No callable variants attempted")

class Utf8WrapperProvider:
    """
    Wraps StatsApiProvider to:
      - Fix mojibake in all returned strings
      - Adapt differing method signatures & binding styles
        (instance, classmethod-ish, staticmethod-ish, positional/keyword).
    """

    def __init__(self) -> None:
        self.inner = StatsApiProvider()
        # surface typical attrs for diagnostics
        for attr in ("base", "key"):
            if hasattr(self.inner, attr):
                setattr(self, attr, getattr(self.inner, attr))

    # ---------------------------
    # Adapters used by routes/league_scan
    # ---------------------------
    def league_hot_hitters(self, date, limit: int = 15, **kwargs) -> List[Dict[str, Any]]:
        ds = _date_to_str(date)

        calls: List[Tuple[Any, tuple, dict]] = []
        # 1) bound method attempts
        if hasattr(self.inner, "league_hot_hitters"):
            f = getattr(self.inner, "league_hot_hitters")
            calls += [
                (f, (), {"date": date, "limit": limit, **kwargs}),
                (f, (), {"date_str": ds, "top_n": limit, **kwargs}),
                (f, (), {"date_str": ds, "limit": limit, **kwargs}),
                (f, (date, limit), {**kwargs}),
                (f, (ds, limit), {**kwargs}),
                (f, (), {"game_date": date, "top_n": limit, **kwargs}),
                (f, (), {"game_date": ds, "top_n": limit, **kwargs}),
            ]
        # 2) class-level function attempts (handles odd definitions)
        cls_f = getattr(type(self.inner), "league_hot_hitters", None)
        if callable(cls_f):
            calls += [
                (cls_f, (ds, limit), {**kwargs}),                       # staticmethod-ish
                (cls_f, (), {"date_str": ds, "top_n": limit, **kwargs}),
                (cls_f, (self.inner, ds, limit), {**kwargs}),           # instance method via class
                (cls_f, (self.inner,), {"date_str": ds, "top_n": limit, **kwargs}),
            ]

        out = _attempt_calls(calls)
        return _wash_utf8(out)

    def league_cold_hitters(self, date, limit: int = 15, **kwargs) -> List[Dict[str, Any]]:
        ds = _date_to_str(date)

        calls: List[Tuple[Any, tuple, dict]] = []
        if hasattr(self.inner, "league_cold_hitters"):
            f = getattr(self.inner, "league_cold_hitters")
            calls += [
                (f, (), {"date": date, "limit": limit, **kwargs}),
                (f, (), {"date_str": ds, "top_n": limit, **kwargs}),
                (f, (), {"date_str": ds, "limit": limit, **kwargs}),
                (f, (date, limit), {**kwargs}),
                (f, (ds, limit), {**kwargs}),
                (f, (), {"game_date": date, "top_n": limit, **kwargs}),
                (f, (), {"game_date": ds, "top_n": limit, **kwargs}),
            ]
        cls_f = getattr(type(self.inner), "league_cold_hitters", None)
        if callable(cls_f):
            calls += [
                (cls_f, (ds, limit), {**kwargs}),
                (cls_f, (), {"date_str": ds, "top_n": limit, **kwargs}),
                (cls_f, (self.inner, ds, limit), {**kwargs}),
                (cls_f, (self.inner,), {"date_str": ds, "top_n": limit, **kwargs}),
            ]

        out = _attempt_calls(calls)
        return _wash_utf8(out)

    def schedule_for_date(self, date, **kwargs) -> Dict[str, Any]:
        ds = _date_to_str(date)

        calls: List[Tuple[Any, tuple, dict]] = []
        if hasattr(self.inner, "schedule_for_date"):
            f = getattr(self.inner, "schedule_for_date")
            calls += [
                (f, (), {"date": date, **kwargs}),
                (f, (), {"date_str": ds, **kwargs}),
                (f, (ds,), {**kwargs}),
                (f, (date,), {**kwargs}),
                (f, (), {"game_date": ds, **kwargs}),
            ]
        cls_f = getattr(type(self.inner), "schedule_for_date", None)
        if callable(cls_f):
            calls += [
                (cls_f, (ds,), {**kwargs}),
                (cls_f, (), {"date_str": ds, **kwargs}),
                (cls_f, (self.inner, ds), {**kwargs}),
                (cls_f, (self.inner,), {"date_str": ds, **kwargs}),
            ]

        out = _attempt_calls(calls)
        return _wash_utf8(out)

    # ---------------------------
    # Adapters used by main.py streak endpoints
    # ---------------------------
    def hot_streak_hitters(
        self,
        date,
        min_avg: float = 0.280,
        games: int = 3,
        require_hit_each: bool = True,
        debug: bool = False,
        **kwargs,
    ):
        ds = _date_to_str(date)
        calls: List[Tuple[Any, tuple, dict]] = []

        if hasattr(self.inner, "hot_streak_hitters"):
            f = getattr(self.inner, "hot_streak_hitters")
            calls += [
                (f, (), {"date": date, "min_avg": min_avg, "games": games,
                         "require_hit_each": require_hit_each, "debug": debug, **kwargs}),
                (f, (), {"date": ds, "min_avg": min_avg, "games": games,
                         "require_hit_each": require_hit_each, "debug": debug, **kwargs}),
                (f, (ds,), {"min_avg": min_avg, "games": games,
                            "require_hit_each": require_hit_each, "debug": debug, **kwargs}),
                (f, (date,), {"min_avg": min_avg, "games": games,
                              "require_hit_each": require_hit_each, "debug": debug, **kwargs}),
            ]
        out = _attempt_calls(calls)
        return _wash_utf8(out)

    def cold_streak_hitters(
        self,
        date,
        min_avg: float = 0.275,
        games: int = 2,
        require_zero_hit_each: bool = True,
        debug: bool = False,
        **kwargs,
    ):
        ds = _date_to_str(date)
        calls: List[Tuple[Any, tuple, dict]] = []

        if hasattr(self.inner, "cold_streak_hitters"):
            f = getattr(self.inner, "cold_streak_hitters")
            calls += [
                (f, (), {"date": date, "min_avg": min_avg, "games": games,
                         "require_zero_hit_each": require_zero_hit_each, "debug": debug, **kwargs}),
                (f, (), {"date": ds, "min_avg": min_avg, "games": games,
                         "require_zero_hit_each": require_zero_hit_each, "debug": debug, **kwargs}),
                (f, (ds,), {"min_avg": min_avg, "games": games,
                            "require_zero_hit_each": require_zero_hit_each, "debug": debug, **kwargs}),
                (f, (date,), {"min_avg": min_avg, "games": games,
                              "require_zero_hit_each": require_zero_hit_each, "debug": debug, **kwargs}),
            ]
        out = _attempt_calls(calls)
        return _wash_utf8(out)

    def pitcher_streaks(
        self,
        date,
        hot_max_era: float = 4.0,
        hot_min_ks_each: int = 6,
        hot_last_starts: int = 3,
        cold_min_era: float = 4.6,
        cold_min_runs_each: int = 3,
        cold_last_starts: int = 2,
        debug: bool = False,
        **kwargs,
    ):
        if not hasattr(self.inner, "pitcher_streaks"):
            return {
                "hot_pitchers": [],
                "cold_pitchers": [],
                "debug": {
                    "note": "pitcher_streaks not implemented by provider; returning empty lists",
                    "provider_module": "providers.statsapi_provider",
                    "provider_class": "StatsApiProvider",
                },
            }

        ds = _date_to_str(date)
        f = getattr(self.inner, "pitcher_streaks")

        calls: List[Tuple[Any, tuple, dict]] = [
            (f, (), {"date": date, "hot_max_era": hot_max_era, "hot_min_ks_each": hot_min_ks_each,
                     "hot_last_starts": hot_last_starts, "cold_min_era": cold_min_era,
                     "cold_min_runs_each": cold_min_runs_each, "cold_last_starts": cold_last_starts,
                     "debug": debug, **kwargs}),
            (f, (), {"date": ds, "hot_max_era": hot_max_era, "hot_min_ks_each": hot_min_ks_each,
                     "hot_last_starts": hot_last_starts, "cold_min_era": cold_min_era,
                     "cold_min_runs_each": cold_min_runs_each, "cold_last_starts": cold_last_starts,
                     "debug": debug, **kwargs}),
            (f, (ds,), {"hot_max_era": hot_max_era, "hot_min_ks_each": hot_min_ks_each,
                        "hot_last_starts": hot_last_starts, "cold_min_era": cold_min_era,
                        "cold_min_runs_each": cold_min_runs_each, "cold_last_starts": cold_last_starts,
                        "debug": debug, **kwargs}),
            (f, (date,), {"hot_max_era": hot_max_era, "hot_min_ks_each": hot_min_ks_each,
                          "hot_last_starts": hot_last_starts, "cold_min_era": cold_min_era,
                          "cold_min_runs_each": cold_min_runs_each, "cold_last_starts": cold_last_starts,
                          "debug": debug, **kwargs}),
        ]

        out = _attempt_calls(calls)
        return _wash_utf8(out)

    # ---------------------------
    # Optional debug passthroughs for /provider_raw
    # ---------------------------
    def _fetch_hitter_rows(self, date, limit: Optional[int] = None, team: Optional[str] = None, **kwargs):
        if not hasattr(self.inner, "_fetch_hitter_rows"):
            raise NotImplementedError("_fetch_hitter_rows not available in inner provider")
        ds = _date_to_str(date)
        f = getattr(self.inner, "_fetch_hitter_rows")
        calls = [
            (f, (), {"date": date, "limit": limit, "team": team, **kwargs}),
            (f, (), {"date": ds, "limit": limit, "team": team, **kwargs}),
            (f, (ds,), {"limit": limit, "team": team, **kwargs}),
        ]
        out = _attempt_calls(calls)
        return _wash_utf8(out)

    def _fetch_pitcher_rows(self, date, limit: Optional[int] = None, team: Optional[str] = None, **kwargs):
        if not hasattr(self.inner, "_fetch_pitcher_rows"):
            raise NotImplementedError("_fetch_pitcher_rows not available in inner provider")
        ds = _date_to_str(date)
        f = getattr(self.inner, "_fetch_pitcher_rows")
        calls = [
            (f, (), {"date": date, "limit": limit, "team": team, **kwargs}),
            (f, (), {"date": ds, "limit": limit, "team": team, **kwargs}),
            (f, (ds,), {"limit": limit, "team": team, **kwargs}),
        ]
        out = _attempt_calls(calls)
        return _wash_utf8(out)
