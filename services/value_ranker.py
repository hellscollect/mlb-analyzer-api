# services/value_ranker.py

from typing import List, Dict, Any, Optional
import math

def _safe(val, default=None):
    return default if val is None else val

def _rank_index(sorted_list: List[Dict[str, Any]], name_key: str, target_name: str) -> Optional[int]:
    for i, row in enumerate(sorted_list):
        if row.get(name_key) == target_name:
            return i
    return None

def _normalize_rank(idx: Optional[int], n: int) -> float:
    # Lower rank index is better. Map to 0..1 where 1 is best.
    if idx is None or n <= 1:
        return 0.0
    return 1.0 - (idx / (n - 1))

def _min_max_norm(vals: List[float]) -> List[float]:
    if not vals:
        return []
    vmin, vmax = min(vals), max(vals)
    if vmax <= vmin:
        return [0.5 for _ in vals]
    return [(v - vmin) / (vmax - vmin) for v in vals]

def _cap(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _implied_prob_from_american(odds: Optional[float]) -> Optional[float]:
    if odds is None:
        return None
    try:
        o = float(odds)
    except Exception:
        return None
    if o >= 100:
        return 100.0 / (o + 100.0)
    if o <= -100:
        return (-o) / ((-o) + 100.0)
    return None

def aggregate_and_score(
    candidates: List[Dict[str, Any]],
    *,
    name_key: str = "name",
    savant: Optional[Dict[str, Dict[str, Any]]] = None,  # name -> {hard_hit_pct_recent, barrel_pct_recent, xba_gap_recent, pitcher_fit, park_flag, lineup_slot}
    odds: Optional[Dict[str, Dict[str, Any]]] = None,    # name -> {odds_hit_1plus}
    # Tunables (bookmaker mindset)
    elite_avg_anchor: float = 0.300,   # push elite bats up even at 1-game streaks
    streak_saturation: int = 3,        # diminishing returns after 3+
    hard_hit_thresh: float = 40.0,
    xba_gap_thresh: float = 0.05,
) -> List[Dict[str, Any]]:
    """
    Returns a list with added keys:
      - crossref_rank (0..1, higher is better) from triple sort consensus
      - bookmaker_score (0..1 composite)
      - score_plus (float) = scaled version of 'score'
      - filters_applied (list[str]) optional
      - value_note (str) optional
    Sorting is by score_plus (fallback to score).
    """
    n = len(candidates)
    if n == 0:
        return candidates

    # Triple sort
    by_score   = sorted(candidates, key=lambda r: _safe(r.get("score"), -1e9), reverse=True)
    by_hit     = sorted(candidates, key=lambda r: _safe(r.get("break_prob_next"), -1e9), reverse=True)
    by_overdue = sorted(candidates, key=lambda r: _safe(r.get("pressure"), -1e9), reverse=True)

    # Crossref ranks
    crossref_map: Dict[str, float] = {}
    for row in candidates:
        nm = row.get(name_key)
        i_score = _rank_index(by_score,   name_key, nm)
        i_hit   = _rank_index(by_hit,     name_key, nm)
        i_over  = _rank_index(by_overdue, name_key, nm)
        r_score = _normalize_rank(i_score, n)
        r_hit   = _normalize_rank(i_hit, n)
        r_over  = _normalize_rank(i_over, n)
        crossref_map[nm] = (r_score + r_hit + r_over) / 3.0

    # Normalize within-list season AVG to emphasize elite hitters
    avgs = [float(_safe(r.get("season_avg"), 0.0)) for r in candidates]
    avgs_norm = _min_max_norm(avgs)

    out: List[Dict[str, Any]] = []
    for idx, row in enumerate(candidates):
        nm = row.get(name_key)
        base_score = float(_safe(row.get("score"), 0.0))
        hit_p = float(_safe(row.get("break_prob_next"), 0.0)) / 100.0
        pressure = float(_safe(row.get("pressure"), 0.0))
        season_avg = float(_safe(row.get("season_avg"), 0.0))
        streak = int(_safe(row.get("hitless_streak"), 0))

        # Components (each 0..1-ish)
        c_cross  = crossref_map.get(nm, 0.0)                     # consensus across your three sorts
        c_avg    = avgs_norm[idx]                                # skill anchor
        c_elite  = _cap((season_avg - elite_avg_anchor) / 0.030, 0.0, 1.0)  # extra push for truly elite bats
        c_hit    = _cap(hit_p, 0.0, 1.0)                         # your model hit chance
        c_over   = _cap(pressure / 2.5, 0.0, 1.0)                # overdue scaled; caps extreme tails
        c_strk   = math.log(1 + min(streak, streak_saturation)) / math.log(1 + streak_saturation)  # diminishing returns

        filters_applied: List[str] = []
        value_note = None

        # Statcast overlays (fail-soft)
        c_hard = 0.0
        c_xgap = 0.0
        c_pfit = 0.0
        c_park = 0.0
        c_line = 0.0
        if savant and nm in savant:
            sv = savant[nm]
            hh = sv.get("hard_hit_pct_recent")
            xgap = sv.get("xba_gap_recent")
            pfit = sv.get("pitcher_fit")     # good/neutral/bad
            park = sv.get("park_flag")       # True/False
            lslot = sv.get("lineup_slot")    # int

            if isinstance(hh, (int, float)) and hh >= hard_hit_thresh:
                c_hard = 1.0
                filters_applied.append(f"Hard-Hit%≥{hard_hit_thresh:g}")
            if isinstance(xgap, (int, float)) and xgap >= xba_gap_thresh:
                c_xgap = _cap(xgap / 0.10, 0.0, 1.0)  # scale 0–.10
                filters_applied.append(f"xBA-BA≥{xba_gap_thresh:.3f}")
            if pfit == "good":
                c_pfit = 1.0
                filters_applied.append("Pitcher fit")
            if park is True:
                c_park = 1.0
                filters_applied.append("Hitter park")
            if isinstance(lslot, int) and lslot <= 5:
                c_line = 1.0
                filters_applied.append("Lineup≤5")

        # Odds (fail-soft)
        c_value = 0.0
        if odds and nm in odds:
            quoted = odds[nm].get("odds_hit_1plus")
            implied = _implied_prob_from_american(quoted)
            if implied is not None and hit_p > 0.0:
                edge = hit_p - implied       # positive = we like it more than the book
                if edge > 0:
                    c_value = _cap(edge / 0.10, 0.0, 1.0)  # cap at +10pp
                    value_note = f"Implied {implied*100:.1f}%, model {hit_p*100:.1f}%"

        # Bookmaker composite (0..1). Weights chosen to emphasize SKILL + CONSENSUS,
        # then add supporting context (statcast/odds) as tie-breakers.
        bookmaker_score = (
            0.25 * c_avg   +   # skill baseline within the slate
            0.15 * c_elite +   # extra push for true elites (Judge-types)
            0.20 * c_cross +   # consensus of Score/Hit/Overdue
            0.15 * c_hit   +   # your model's break prob
            0.10 * c_over  +   # overdue (pressure)
            0.05 * c_strk  +   # streak length with diminishing returns
            0.03 * c_hard  +   # quality of contact
            0.03 * c_xgap  +   # expected vs actual (unlucky)
            0.02 * c_pfit  +   # pitch-type fit
            0.01 * c_park  +   # hitter-friendly park
            0.01 * c_line  +   # lineup slot
            0.10 * c_value     # value vs odds (if edge positive)
        )
        bookmaker_score = _cap(bookmaker_score, 0.0, 1.0)

        # Turn composite into a gentle multiplier on your Score
        m = 1.0 + 0.25 * bookmaker_score  # up to +25% on extreme cases
        score_plus = round(base_score * m, 1)

        new_row = dict(row)
        new_row["crossref_rank"] = round(crossref_map.get(nm, 0.0), 4)
        new_row["bookmaker_score"] = round(bookmaker_score, 4)
        new_row["score_plus"] = score_plus
        if filters_applied:
            new_row["filters_applied"] = filters_applied
        if value_note:
            new_row["value_note"] = value_note
        out.append(new_row)

    out.sort(key=lambda r: (_safe(r.get("score_plus"), _safe(r.get("score"), -1e9))), reverse=True)
    return out
