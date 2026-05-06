"""Anomaly detection for pipeline metrics using z-score analysis."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional
import math

from pipewatch.aggregator import AggregationResult


@dataclass
class AnomalyResult:
    source: str
    metric_name: str
    observed: float
    expected: float
    z_score: float
    is_anomaly: bool
    reason: str = ""

    def __str__(self) -> str:
        flag = "ANOMALY" if self.is_anomaly else "ok"
        return (
            f"[{flag}] {self.source}/{self.metric_name}: "
            f"observed={self.observed:.4g}, expected={self.expected:.4g}, "
            f"z={self.z_score:.2f}"
        )


def _z_score(observed: float, mean: float, stddev: float) -> float:
    """Return z-score; 0.0 when stddev is zero (no variance)."""
    if stddev == 0.0:
        return 0.0
    return (observed - mean) / stddev


def detect_anomaly(
    source: str,
    metric_name: str,
    observed: float,
    agg: AggregationResult,
    threshold: float = 3.0,
) -> AnomalyResult:
    """Detect whether *observed* is anomalous given historical *agg* stats.

    Args:
        source: Source name the metric belongs to.
        metric_name: Name of the metric being evaluated.
        observed: The current metric value.
        agg: Aggregation stats computed from recent history.
        threshold: Number of standard deviations that constitutes an anomaly.

    Returns:
        An :class:`AnomalyResult` describing the outcome.
    """
    if agg is None or agg.count < 2:
        return AnomalyResult(
            source=source,
            metric_name=metric_name,
            observed=observed,
            expected=agg.mean if agg else observed,
            z_score=0.0,
            is_anomaly=False,
            reason="insufficient history",
        )

    z = _z_score(observed, agg.mean, agg.stddev)
    is_anomaly = abs(z) >= threshold
    reason = f"|z|={abs(z):.2f} >= threshold={threshold}" if is_anomaly else ""
    return AnomalyResult(
        source=source,
        metric_name=metric_name,
        observed=observed,
        expected=agg.mean,
        z_score=z,
        is_anomaly=is_anomaly,
        reason=reason,
    )


def detect_all(
    source: str,
    observations: dict,
    aggregations: dict,
    threshold: float = 3.0,
) -> List[AnomalyResult]:
    """Run anomaly detection for every metric in *observations*."""
    results = []
    for name, value in observations.items():
        agg = aggregations.get(name)
        results.append(detect_anomaly(source, name, value, agg, threshold))
    return results
