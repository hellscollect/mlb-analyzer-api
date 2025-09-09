# services/value_ranker.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class RankWeights:
    """
    All weights are expressed in 'points' that should sum roughly near 100,
    but they don't have to. They are normalized inside the scorer.

    These map to your API query knobs:
      - w_hit_chance
      - w_overdue
      - w_elite_avg
      - w_statcast
    """
    w_hit_chance: float = 50.0   # break_prob_next (Hit chance)
    w_overdue: float   = 17.5    # pressure (Overdue)
    w_elite_avg: float = 12.5    # season batting average anchor
    w_statcast: float  = 20.0    # Statcast signal contribution

    def normalized(self) -> "RankWeights":
        total = max(1e-9, self.w_hit_chance + self.w_overdue + self.w_elite_avg + self.w_statcast)
        return RankWeights(
            w_hit_chance=self.w_hit_chance / total,
            w_overdue=self.w_overdue / total,
            w_elite_avg=self.w_elite_avg / total,
            w_statcast=self.w_statcast / total,
        )


@dataclass
class RankInputs:
    """
    Inputs for a single candidate row.
    Missing values are handled graceully (treated as neutral/zero where appropriate).
    """
    season_avg: Optional[float] = None          # e.g. .321
    break_prob_next: Optional[float] = None     # 0..1 (we'll clamp)
    pressure: Optional[float] = None            # continuous overdue metric
    hard_hit_pct_recent: Optional[float] = None # 0..100
    xba_gap_recent: Optional[float] = None      # xBA - BA (decimal)
    statcast_positive: bool = False             # gate flag used for tiers (handled upstream)


class ValueRanker:
    """
    Computes 'score' and 'bookmaker' components, then 'composite'.

    NOTE:
    - elite_avg_anchor is restored to 0.300 per your original baseline.
    - No implicit "extra boost" for AVG beyond what w_elite_avg contributes.
    - The final composite is a convex combination of category sub-scores using runtime weights.
    """

    elite_avg_anchor: float = 0.300

    def __init__(self, weights: RankWeights):
        # Normalize once; avoids surprises if user passes large numbers.
        self.w = weights.normalized()

    @staticmethod
    def _safe_float(x: Optional[float], default: float = 0.0) -> float:
        try:
            if x is None:
                return default
            return float(x)
        except Exception:
            return default

    @staticmethod
    def _clamp(x: float, lo: float, hi: float) -> float:
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _score_hit_chance(self, p: Optional[float]) -> float:
        """
        Input p expected in 0..1. Convert to 0..100 points.
        """
        p = self._clamp(self._safe_float(p, 0.0), 0.0, 1.0)
        return 100.0 * p

    def _score_overdue(self, pressure: Optional[float]) -> float:
        """
        Pressure is open-ended. Map a reasonable window into 0..100.
        We use a soft ramp: 0 at <=0.5, 100 at >=3.0, linear in between.
        """
        x = self._safe_float(pressure, 0.0)
        if x <= 0.5:
            return 0.0
        if x >= 3.0:
            return 100.0
        # linear ramp between 0.5 and 3.0
        return (x - 0.5) / (3.0 - 0.5) * 100.0

    def _score_elite_avg(self, avg: Optional[float]) -> float:
        """
        Compare season AVG to anchor (0.300).
        0 points at <=0.240, 100 points at >=0.340, linear in between, then
        add a mild bonus if >= anchor.
        """
        a = self._safe_float(avg, 0.0)
        if a <= 0.240:
            base = 0.0
        elif a >= 0.340:
            base = 100.0
        else:
            base = (a - 0.240) / (0.340 - 0.240) * 100.0

        # Mild bonus for AVG above the anchor (kept small; this is not a hard boost)
        if a >= self.elite_avg_anchor:
            bonus = min(10.0, (a - self.elite_avg_anchor) * 1000.0)  # e.g., .320 → +20, capped at +10
        else:
            bonus = 0.0

        return self._clamp(base + bonus, 0.0, 110.0)  # allow small cap

    def _score_statcast(self, hard_hit_pct: Optional[float], xba_gap: Optional[float]) -> float:
        """
        Blend quality-of-contact (0..100) with xBA gap (unlucky contact).
        - HardHit% mapped directly 0..100.
        - xBA gap in [-0.10, +0.10] maps to [0, 100] with 50 at 0.
        """
        hh = self._clamp(self._safe_float(hard_hit_pct, 0.0), 0.0, 100.0)

        xgap = self._safe_float(xba_gap, 0.0)
        xgap = self._clamp(xgap, -0.10, 0.10)
        xgap_score = (xgap + 0.10) / 0.20 * 100.0  # -0.10→0 ; +0.10→100 ; 0→50

        # 70/30 blend: prioritize real HH% but keep xBA gap influence
        return 0.70 * hh + 0.30 * xgap_score

    def score_row(self, ri: RankInputs) -> Dict[str, float]:
        """
        Returns a dict with:
          - score         : raw composite (0..100-ish)
          - bookmaker     : bookmaker-oriented subscore (0..1 range)
          - score_plus    : alias to score (kept for compatibility)
          - components_*  : sub-scores for transparency
        """
        # Components as 0..100 scores
        s_hit = self._score_hit_chance(ri.break_prob_next)
        s_ovr = self._score_overdue(ri.pressure)
        s_avg = self._score_elite_avg(ri.season_avg)
        s_stat = self._score_statcast(ri.hard_hit_pct_recent, ri.xba_gap_recent)

        # Normalized weights (sum ~ 1.0)
        w = self.w

        # Composite as convex combination
        composite = (
            w.w_hit_chance * s_hit +
            w.w_overdue   * s_ovr +
            w.w_elite_avg * s_avg +
            w.w_statcast  * s_stat
        )

        # Bookmaker sub-score scaled to 0..1 (keep simple/monotonic)
        bookmaker = self._clamp(composite / 100.0, 0.0, 1.0)

        return {
            "score": composite,
            "bookmaker": bookmaker,
            "score_plus": composite,
            "components_hit": s_hit,
            "components_overdue": s_ovr,
            "components_avg": s_avg,
            "components_statcast": s_stat,
        }


def compute_scores(row: Dict[str, Any], weights: RankWeights) -> Dict[str, float]:
    """
    Adapter entry point. Pass a candidate row (as dict) and RankWeights (from query params).
    Expected keys in row (all optional):
      - 'season_avg'
      - 'break_prob_next'
      - 'pressure'
      - '_statcast': {'hh_percent_14d' or 'hard_hit_pct_recent', 'xba_delta_14d' or 'xba_gap_recent'}
    """
    # Pull statcast from either the new keys or fallback keys
    stat = row.get("_statcast") or {}
    hard = stat.get("hh_percent_14d")
    if hard is None:
        hard = stat.get("hard_hit_pct_recent")
    xgap = stat.get("xba_delta_14d")
    if xgap is None:
        xgap = stat.get("xba_gap_recent")

    ri = RankInputs(
        season_avg=row.get("season_avg"),
        break_prob_next=row.get("break_prob_next"),
        pressure=row.get("pressure"),
        hard_hit_pct_recent=hard,
        xba_gap_recent=xgap,
        statcast_positive=bool(stat.get("has_signal", False)),
    )
    vr = ValueRanker(weights)
    return vr.score_row(ri)
