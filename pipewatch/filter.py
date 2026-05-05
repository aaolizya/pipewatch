"""Filtering utilities for pipeline metrics and alert results."""

from __future__ import annotations

from typing import Iterable, List, Optional

from pipewatch.metrics import AlertResult, Metric
from pipewatch.summary import SourceSummary


def filter_by_status(
    results: Iterable[AlertResult],
    statuses: Iterable[str],
) -> List[AlertResult]:
    """Return only results whose status matches one of *statuses*.

    Args:
        results:  Iterable of :class:`~pipewatch.metrics.AlertResult` objects.
        statuses: Collection of status strings, e.g. ``["warning", "critical"]``.

    Returns:
        Filtered list preserving original order.
    """
    allowed = {s.lower() for s in statuses}
    return [r for r in results if r.status.lower() in allowed]


def filter_by_source(
    summaries: Iterable[SourceSummary],
    source_names: Iterable[str],
) -> List[SourceSummary]:
    """Return only summaries whose source name is in *source_names*.

    Args:
        summaries:    Iterable of :class:`~pipewatch.summary.SourceSummary` objects.
        source_names: Collection of source name strings to keep.

    Returns:
        Filtered list preserving original order.
    """
    allowed = {n.lower() for n in source_names}
    return [s for s in summaries if s.source.name.lower() in allowed]


def filter_metrics_by_name(
    metrics: Iterable[Metric],
    pattern: str,
) -> List[Metric]:
    """Return metrics whose name contains *pattern* (case-insensitive substring).

    Args:
        metrics: Iterable of :class:`~pipewatch.metrics.Metric` objects.
        pattern: Substring to search for within each metric name.

    Returns:
        Filtered list preserving original order.
    """
    lower = pattern.lower()
    return [m for m in metrics if lower in m.name.lower()]


def filter_alerts_above_value(
    results: Iterable[AlertResult],
    threshold: float,
) -> List[AlertResult]:
    """Return alert results whose metric value exceeds *threshold*.

    Args:
        results:   Iterable of :class:`~pipewatch.metrics.AlertResult` objects.
        threshold: Numeric lower bound (exclusive).

    Returns:
        Filtered list preserving original order.
    """
    return [r for r in results if r.metric.value > threshold]
