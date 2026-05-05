"""Metric aggregation utilities for summarising values across a time window."""

from __future__ import annotations

from dataclasses import dataclass, field
from statistics import mean, median, stdev
from typing import List, Optional, Sequence

from pipewatch.history import HistoryEntry


@dataclass
class AggregationResult:
    """Statistical summary of a metric's values over a window."""

    source: str
    metric_name: str
    count: int
    minimum: float
    maximum: float
    average: float
    median: float
    stddev: Optional[float]
    values: List[float] = field(repr=False)

    def __str__(self) -> str:
        stddev_str = f"{self.stddev:.4f}" if self.stddev is not None else "n/a"
        return (
            f"{self.source}/{self.metric_name}: "
            f"n={self.count} min={self.minimum} max={self.maximum} "
            f"avg={self.average:.4f} median={self.median:.4f} stddev={stddev_str}"
        )


def aggregate(entries: Sequence[HistoryEntry]) -> Optional[AggregationResult]:
    """Compute aggregation statistics from a sequence of history entries.

    Returns *None* when *entries* is empty.
    """
    if not entries:
        return None

    values = [e.value for e in entries]
    first = entries[0]

    return AggregationResult(
        source=first.source,
        metric_name=first.metric_name,
        count=len(values),
        minimum=min(values),
        maximum=max(values),
        average=mean(values),
        median=median(values),
        stddev=stdev(values) if len(values) >= 2 else None,
        values=list(values),
    )


def aggregate_from_history(history, source: str, metric_name: str, limit: int = 0) -> Optional[AggregationResult]:
    """Convenience wrapper that reads entries from a *MetricHistory* instance.

    Args:
        history: A :class:`~pipewatch.history.MetricHistory` instance.
        source: Source name to query.
        metric_name: Metric name to query.
        limit: When > 0, only the most recent *limit* entries are used.
    """
    entries = history.get(source, metric_name)
    if limit > 0:
        entries = entries[-limit:]
    return aggregate(entries)
