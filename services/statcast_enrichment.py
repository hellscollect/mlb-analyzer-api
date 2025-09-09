# services/statcast_enrichment.py
from __future__ import annotations

import csv
import io
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

try:
    # Optional but best way to resolve MLBAM ids.
    from pybaseball import playerid_lookup  # type: ignore
except Exception:  # pragma: no cover
    playerid_lookup = None  # type: ignore


STATCAST_DAYS_DEFAULT = 14
STATCAST_SEARCH_CSV_URL = "https://baseballsavant.mlb.com/statcast_search/csv"

_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 ProblemChildAI/1.0"
)

_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_TTL_SEC = 600  # 10 minutes


@dataclass
class StatcastSignal:
    hard_hit_pct: Optional[float]  # 0..100
    xba_gap: Optional[float]       # xBA − BA
    has_signal: bool               # meets thresholds
    why: str                       # human summary


def _cache_key(mbam: int, start: str, end: str) -> str:
    return f"bam:{mbam}:{start}:{end}"

def _now_sec() -> float:
    return time.time()

def _from_cache(key: str) -> Optional[Dict[str, Any]]:
    item = _CACHE.get(key)
    if not item:
        return None
    if _now_sec() - item["t"] > _CACHE_TTL_SEC:
        _CACHE.pop(key, None)
        return None
    return item["v"]

def _to_cache(key: str, value: Dict[str, Any]) -> None:
    _CACHE[key] = {"v": value, "t": _now_sec()}

def _daterange_recent(days: int) -> Tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=max(1, int(days)))
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

def _split_name(nm: str) -> Tuple[str, str]:
    nm = (nm or "").strip()
    if not nm:
        return "", ""
    parts = nm.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[-1], " ".join(parts[:-1])

def _lookup_bamid(name: str) -> Optional[int]:
    """Resolve MLBAM id via pybaseball if available."""
    if playerid_lookup is None:
        return None
    try:
        last, first = _split_name(name)
        df = playerid_lookup(last, first)  # type: ignore
        if df is None or df.empty:
            return None
        for _, row in df.iterrows():
            bam = row.get("key_mlbam")
            if bam and str(bam).strip().isdigit():
                return int(bam)
    except Exception:
        return None
    return None


def _fetch_statcast_csv(
    mbam: int,
    start_dt: str,
    end_dt: str,
    *,
    timeout: float = 8.0,
    max_retries: int = 2,
    retry_backoff: float = 0.6,
) -> List[Dict[str, str]]:
    """
    Fetch raw Statcast rows for a given batter over [start_dt, end_dt] from Baseball Savant CSV.
    """
    params = {
        "player_type": "batter",
        "player_lookup": "true",
        "type": "details",
        "player": str(mbam),
        "start_date": start_dt,
        "end_date": end_dt,
        # Everything else left blank intentionally (all pitch types, counts, etc.)
        "hfPT": "", "hfAB": "", "hfBBT": "", "hfPR": "", "hfZ": "", "stadium": "", "hfBBL": "",
        "hfNewZones": "", "hfGT": "", "hfC": "", "hfSea": "", "hfSit": "", "hfOuts": "",
        "opponent": "", "pitcher_throws": "", "batter_stands": "", "metric_1": "", "rehab": "",
    }

    headers = {"User-Agent": _UA, "Accept": "text/csv"}

    attempt = 0
    while True:
        attempt += 1
        try:
            r = requests.get(STATCAST_SEARCH_CSV_URL, params=params, headers=headers, timeout=timeout)
            r.raise_for_status()
            buff = io.StringIO(r.text)
            reader = csv.DictReader(buff)
            return list(reader)
        except Exception:
            if attempt <= max_retries:
                time.sleep(retry_backoff * attempt)
                continue
            return []


def _calc_signal(rows: Iterable[Dict[str, str]]) -> Tuple[Optional[float], Optional[float]]:
    """
    Compute (Hard-Hit %, xBA gap) over the window.
    - hard_hit_pct: EV >= 95 mph over batted balls
    - xba_gap: mean(estimated_ba_using_speedangle) − BA (hits/AB)
    """
    evs: List[float] = []
    xbas: List[float] = []
    events: List[str] = []

    for row in rows:
        # batted ball speed
        ls = row.get("launch_speed")
        if ls:
            try:
                evs.append(float(ls))
            except Exception:
                pass

        # estimated BA per batted ball
        xba_str = row.get("estimated_ba_using_speedangle")
        if xba_str:
            try:
                xbas.append(float(xba_str))
            except Exception:
                pass

        evname = (row.get("events") or "").strip().lower()
        events.append(evname)

    # Hard-Hit%
    hard_hit_pct = None
    if evs:
        hard = sum(1 for ev in evs if ev >= 95.0)
        hard_hit_pct = 100.0 * hard / len(evs)

    # BA from events (AB subset)
    AB_EVENTS = {
        "single", "double", "triple", "home_run",
        "strikeout", "field_out", "grounded_into_double_play", "force_out",
        "field_error", "double_play", "fielders_choice", "fielders_choice_out",
        "pop_out", "flyout", "lineout", "strikeout_double_play", "other_out"
    }
    HIT_EVENTS = {"single", "double", "triple", "home_run"}

    ab = sum(1 for e in events if e in AB_EVENTS)
    hits = sum(1 for e in events if e in HIT_EVENTS)
    ba = (hits / ab) if ab > 0 else 0.0

    xba = (sum(xbas) / len(xbas)) if xbas else None
    xba_gap = (xba - ba) if (xba is not None) else None

    return hard_hit_pct, xba_gap


def _meets_thresholds(
    hard_hit_pct: Optional[float],
    xba_gap: Optional[float],
    *,
    hh_min: float,
    xba_delta_min: float,
) -> Tuple[bool, str]:
    reasons: List[str] = []
    ok = False
    if hard_hit_pct is not None and hard_hit_pct >= hh_min:
        ok = True
        reasons.append(f"HH% {hard_hit_pct:.1f}≥{hh_min:.0f}")
    if xba_gap is not None and xba_gap >= xba_delta_min:
        ok = True
        reasons.append(f"xBA–BA +{xba_gap:.3f}≥+{xba_delta_min:.3f}")
    return ok, "; ".join(reasons)


def fetch_statcast_overlays(
    names: List[str],
    *,
    recent_days: int = STATCAST_DAYS_DEFAULT,
    statcast_min_hh_14d: float = 40.0,
    statcast_min_xba_delta_14d: float = 0.030,
) -> Dict[str, Dict[str, Any]]:
    """
    Return { name: { has_signal, why, hh_percent_14d, xba_delta_14d } }.
    Fail-soft per player on errors/missing data.
    """
    out: Dict[str, Dict[str, Any]] = {}
    if not names:
        return out

    start, end = _daterange_recent(recent_days)

    for nm in names:
        try:
            bam = _lookup_bamid(nm)
            if not bam:
                continue

            ck = _cache_key(bam, start, end)
            cached = _from_cache(ck)
            if cached is not None:
                out[nm] = cached
                continue

            rows = _fetch_statcast_csv(bam, start, end)
            if not rows:
                miss = {"has_signal": False, "why": "", "hh_percent_14d": None, "xba_delta_14d": None}
                _to_cache(ck, miss)
                out[nm] = miss
                continue

            hh, xgap = _calc_signal(rows)
            has, why = _meets_thresholds(hh, xgap, hh_min=statcast_min_hh_14d, xba_delta_min=statcast_min_xba_delta_14d)

            result = {
                "has_signal": bool(has),
                "why": why,
                "hh_percent_14d": round(hh, 1) if hh is not None else None,
                "xba_delta_14d": round(xgap, 3) if xgap is not None else None,
            }
            _to_cache(ck, result)
            out[nm] = result

            # polite pacing
            time.sleep(0.08)

        except Exception:
            out[nm] = {"has_signal": False, "why": "", "hh_percent_14d": None, "xba_delta_14d": None}

    return out
