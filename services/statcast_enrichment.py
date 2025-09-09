# services/statcast_enrichment.py
from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple
from datetime import date, timedelta
import pandas as pd

# pybaseball imports (already in requirements.txt)
from pybaseball import statcast_batter, playerid_lookup

# Basic mapping from string "events" to whether it's an official AB and whether it's a hit
# This is a simplification but works well for recent windows
_AB_EVENTS = {
    "single", "double", "triple", "home_run", "strikeout", "field_out", "grounded_into_double_play",
    "force_out", "field_error", "double_play", "fielders_choice", "fielders_choice_out", "pop_out",
    "flyout", "lineout", "strikeout_double_play", "caught_stealing_2b", "other_out"
}
_HIT_EVENTS = {"single", "double", "triple", "home_run"}

def _split_name(nm: str) -> Tuple[str, str]:
    nm = (nm or "").strip()
    if not nm:
        return "", ""
    parts = nm.split()
    if len(parts) == 1:
        return parts[0], ""
    # best-effort: last token as last name
    return parts[-1], " ".join(parts[:-1])

def _lookup_bamid(name: str) -> Optional[int]:
    last, first = _split_name(name)
    try:
        df = playerid_lookup(last, first)  # returns DataFrame with "key_mlbam"
        if df is None or df.empty:
            return None
        # Prefer currently active / most recent id
        # playerid_lookup may return multiple rows; take the first with a key_mlbam
        for _, row in df.iterrows():
            bam = row.get("key_mlbam")
            if pd.notna(bam):
                try:
                    return int(bam)
                except Exception:
                    continue
    except Exception:
        return None
    return None

def _daterange_recent(days: int) -> Tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=max(1, int(days)))
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

def _calc_rates(df: pd.DataFrame) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Returns (hard_hit_pct, barrel_pct, xba_gap) for the dataframe window.
    - hard_hit_pct: % of batted balls with launch_speed >= 95 mph
    - barrel_pct: naive: % with "barreled" flag if present else high EV + sweet spot proxy
    - xba_gap: mean(estimated_ba_using_speedangle) - recent BA (hits/AB)
    """
    if df is None or df.empty:
        return None, None, None

    # Batted balls subset
    batted = df[df["launch_speed"].notna()]
    hard_hit_pct = None
    if not batted.empty:
        hard = (batted["launch_speed"] >= 95).sum()
        hard_hit_pct = 100.0 * float(hard) / float(len(batted))

    # Barrel pct (pybaseball has "barrel" column in some versions; fallback proxy)
    barrel_pct = None
    if "barrel" in df.columns:
        try:
            barrel_pct = 100.0 * float((df["barrel"] == 1).sum()) / float(len(df))
        except Exception:
            barrel_pct = None
    if barrel_pct is None and not batted.empty:
        # proxy: EV>=98 and 26<=LA<=30-ish sweet-spot window
        proxy = batted[(batted["launch_speed"] >= 98) & (batted["launch_angle"].between(26, 30, inclusive="both"))]
        barrel_pct = 100.0 * float(len(proxy)) / float(len(batted))

    # BA
    ev = df["events"].astype(str).str.lower().fillna("")
    ab_mask = ev.isin(_AB_EVENTS)
    ab = int(ab_mask.sum())
    hits = int(ev.isin(_HIT_EVENTS).sum())
    ba = (hits / ab) if ab > 0 else 0.0

    # xBA
    xba_col = "estimated_ba_using_speedangle"
    if xba_col in df.columns:
        xba_vals = df[xba_col].dropna()
        xba = float(xba_vals.mean()) if not xba_vals.empty else None
    else:
        xba = None

    xba_gap = (xba - ba) if (xba is not None) else None
    return hard_hit_pct, barrel_pct, xba_gap

def fetch_statcast_overlays(
    names: List[str],
    *,
    recent_days: int = 14,
    timeout_s: float = 2.5,  # retained for signature compatibility; not used by pybaseball
) -> Dict[str, Dict[str, Any]]:
    """
    Return a dict keyed by player name with optional fields:
      hard_hit_pct_recent (0..100), barrel_pct_recent (0..100),
      xba_gap_recent (xBA - BA as decimal), pitcher_fit ('good'/'neutral'/'bad'),
      park_flag (True if hitter-friendly), lineup_slot (int if known).

    Fail-soft: if data unavailable, return {} or omit keys.
    """
    out: Dict[str, Dict[str, Any]] = {}
    if not names:
        return out

    start, end = _daterange_recent(int(recent_days))

    for nm in names:
        try:
            pid = _lookup_bamid(nm)
            if not pid:
                continue
            df = statcast_batter(start_dt=start, end_dt=end, player_id=pid)
            if df is None or df.empty:
                continue
            hard, barrel, xgap = _calc_rates(df)
            row: Dict[str, Any] = {}
            if hard is not None:
                row["hard_hit_pct_recent"] = float(round(hard, 2))
            if barrel is not None:
                row["barrel_pct_recent"] = float(round(barrel, 2))
            if xgap is not None:
                row["xba_gap_recent"] = float(round(xgap, 4))

            # Placeholders for future enrichments (kept for stability with the existing code)
            row["pitcher_fit"] = "neutral"
            row["park_flag"] = False
            row["lineup_slot"] = None

            out[nm] = row
        except Exception:
            # fail-soft per player
            continue

    return out
