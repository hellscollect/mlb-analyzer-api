
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any, Optional

@dataclass
class RankWeights:
    w_hit_chance: float = 50.0
    w_overdue: float   = 17.5
    w_elite_avg: float = 12.5
    w_statcast: float  = 20.0
    def normalized(self) -> "RankWeights":
        total = max(1e-9, self.w_hit_chance + self.w_overdue + self.w_elite_avg + self.w_statcast)
        return RankWeights(
            w_hit_chance=self.w_hit_chance/total,
            w_overdue=self.w_overdue/total,
            w_elite_avg=self.w_elite_avg/total,
            w_statcast=self.w_statcast/total,
        )

@dataclass
class RankInputs:
    season_avg: Optional[float] = None
    break_prob_next: Optional[float] = None
    pressure: Optional[float] = None
    hard_hit_pct_recent: Optional[float] = None
    xba_gap_recent: Optional[float] = None

class ValueRanker:
    elite_avg_anchor: float = 0.300
    def __init__(self, weights: RankWeights):
        self.w = weights.normalized()
    @staticmethod
    def _safe_float(x: Optional[float], default: float = 0.0) -> float:
        try:
            if x is None: return default
            return float(x)
        except Exception:
            return default
    @staticmethod
    def _clamp(x: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, x))
    def _score_hit_chance(self, p: Optional[float]) -> float:
        p = self._clamp(self._safe_float(p, 0.0), 0.0, 1.0); return 100.0 * p
    def _score_overdue(self, pressure: Optional[float]) -> float:
        x = self._safe_float(pressure, 0.0)
        if x <= 0.5: return 0.0
        if x >= 3.0: return 100.0
        return (x - 0.5) / (3.0 - 0.5) * 100.0
    def _score_elite_avg(self, avg: Optional[float]) -> float:
        a = self._safe_float(avg, 0.0)
        if a <= 0.240: base = 0.0
        elif a >= 0.340: base = 100.0
        else: base = (a - 0.240) / (0.340 - 0.240) * 100.0
        bonus = 0.0
        if a >= self.elite_avg_anchor:
            bonus = min(10.0, (a - self.elite_avg_anchor) * 1000.0)
        return self._clamp(base + bonus, 0.0, 110.0)
    def _score_statcast(self, hard_hit_pct: Optional[float], xba_gap: Optional[float]) -> float:
        hh = self._clamp(self._safe_float(hard_hit_pct, 0.0), 0.0, 100.0)
        xgap = self._safe_float(xba_gap, 0.0); xgap = self._clamp(xgap, -0.10, 0.10)
        xgap_score = (xgap + 0.10)/0.20*100.0
        return 0.70*hh + 0.30*xgap_score
    def score_row(self, ri: RankInputs) -> Dict[str, float]:
        s_hit = self._score_hit_chance(ri.break_prob_next)
        s_ovr = self._score_overdue(ri.pressure)
        s_avg = self._score_elite_avg(ri.season_avg)
        s_stat = self._score_statcast(ri.hard_hit_pct_recent, ri.xba_gap_recent)
        w = self.w
        composite = w.w_hit_chance*s_hit + w.w_overdue*s_ovr + w.w_elite_avg*s_avg + w.w_statcast*s_stat
        return {"score": composite, "score_plus": composite, "components_hit": s_hit, "components_overdue": s_ovr, "components_avg": s_avg, "components_statcast": s_stat}

def compute_scores(row: Dict[str, Any], weights: RankWeights) -> Dict[str, float]:
    stat = row.get("_statcast") or {}
    hard = stat.get("hh_percent_14d") if stat.get("hh_percent_14d") is not None else stat.get("hard_hit_pct_recent")
    xgap = stat.get("xba_delta_14d") if stat.get("xba_delta_14d") is not None else stat.get("xba_gap_recent")
    ri = RankInputs(season_avg=row.get("season_avg"), break_prob_next=row.get("break_prob_next"), pressure=row.get("pressure"), hard_hit_pct_recent=hard, xba_gap_recent=xgap)
    return ValueRanker(weights).score_row(ri)
