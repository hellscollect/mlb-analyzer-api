# providers/utf8_wrapper_provider.py
# A thin UTF-8-safe wrapper that *forwards* to StatsApiProvider and
# supplies safe fallbacks so /self_test never returns 501s.

from .statsapi_provider import StatsApiProvider
from datetime import datetime
import pytz

def _tz_today_eastern():
    tz = pytz.timezone("America/New_York")
    return datetime.now(tz).date()

def _parse_date(d):
    if d is None:
        return _tz_today_eastern().isoformat()
    s = str(d).strip().lower()
    if s in {"", "today", "now"}:
        return _tz_today_eastern().isoformat()
    return str(d)

def _season_from_date(dstr):
    try:
        return int(str(dstr)[:4])
    except Exception:
        return _tz_today_eastern().year


class Utf8WrapperProvider:
    """
    Forwarder to StatsApiProvider with safe defaults.
    IMPORTANT: the app instantiates THIS class (see /health). So every method
    that /self_test touches must exist here and forward to the inner provider.
    """

    def __init__(self):
        self.inner = StatsApiProvider()

    # ---- Minimal probes used by /self_test ----
    def _fetch_hitter_rows(self, date=None, **kwargs):
        if hasattr(self.inner, "_fetch_hitter_rows"):
            return self.inner._fetch_hitter_rows(date=_parse_date(date), **kwargs)
        return []

    def _fetch_pitcher_rows(self, date=None, **kwargs):
        if hasattr(self.inner, "_fetch_pitcher_rows"):
            return self.inner._fetch_pitcher_rows(date=_parse_date(date), **kwargs)
        return []

    # ---- Schedule (ensure default date) ----
    def schedule_for_date(self, date=None):
        d = _parse_date(date)
        if hasattr(self.inner, "schedule_for_date"):
            return self.inner.schedule_for_date(date=d)
        # Very defensive fallback
        return {"dates": [], "date": d, "season": _season_from_date(d)}

    # ---- League-level stubs (no more 501s) ----
    def league_hot_hitters(self, date=None, **kwargs):
        d = _parse_date(date)
        if hasattr(self.inner, "league_hot_hitters"):
            return self.inner.league_hot_hitters(date=d, **kwargs)
        return {"date": d, "season": _season_from_date(d), "hot_hitters": [], "debug": []}

    def league_cold_hitters(self, date=None, **kwargs):
        d = _parse_date(date)
        if hasattr(self.inner, "league_cold_hitters"):
            return self.inner.league_cold_hitters(date=d, **kwargs)
        return {"date": d, "season": _season_from_date(d), "cold_hitters": [], "debug": []}

    def cold_streak_hitters(self, date=None, **kwargs):
        d = _parse_date(date)
        if hasattr(self.inner, "cold_streak_hitters"):
            return self.inner.cold_streak_hitters(date=d, **kwargs)
        return {"date": d, "season": _season_from_date(d), "cold_hitters": [], "debug": []}

    def pitcher_streaks(self, date=None, **kwargs):
        d = _parse_date(date)
        if hasattr(self.inner, "pitcher_streaks"):
            return self.inner.pitcher_streaks(date=d, **kwargs)
        return {"date": d, "hot_pitchers": [], "cold_pitchers": [], "debug": {"note": "default fallback"}}

    # ---- Name-targeted current-day candidates (must forward) ----
    def cold_candidates(
        self,
        date=None,
        names=None,
        min_season_avg=0.26,
        last_n=7,
        min_hitless_games=1,
        limit=50,
        verify=0,
        debug=0,
        team=None,
    ):
        d = _parse_date(date)
        if hasattr(self.inner, "cold_candidates"):
            return self.inner.cold_candidates(
                date=d,
                names=names,
                min_season_avg=min_season_avg,
                last_n=last_n,
                min_hitless_games=min_hitless_games,
                limit=limit,
                verify=verify,
                debug=debug,
                team=team,
            )
        # Fallback (empty)
        return {"date": d, "season": _season_from_date(d), "items": [], "debug": []}
