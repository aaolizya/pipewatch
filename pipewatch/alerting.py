"""Alert evaluation and formatting for collected metrics."""

from __future__ import annotations

import logging
from typing import List, Tuple

from pipewatch.config import SourceConfig
from pipewatch.metrics import AlertResult, Metric, evaluate_metric

logger = logging.getLogger(__name__)


def build_source_map(sources: List[SourceConfig]) -> dict[str, SourceConfig]:
    """Return a mapping of source name → SourceConfig for quick lookup."""
    return {s.name: s for s in sources}


def evaluate_all(
    metrics: List[Metric],
    sources: List[SourceConfig],
) -> List[Tuple[Metric, AlertResult]]:
    """Evaluate every collected metric against its source thresholds.

    Args:
        metrics: Collected metric values.
        sources: Source configurations that carry threshold information.

    Returns:
        List of (metric, alert_result) pairs for all metrics.
    """
    source_map = build_source_map(sources)
    results: List[Tuple[Metric, AlertResult]] = []

    for metric in metrics:
        source = source_map.get(metric.name)
        if source is None:
            logger.warning(
                "No source config found for metric '%s'; skipping evaluation.",
                metric.name,
            )
            continue
        result = evaluate_metric(
            metric,
            warning_threshold=source.warning_threshold,
            critical_threshold=source.critical_threshold,
        )
        results.append((metric, result))
        if result.is_alert():
            logger.warning(
                "ALERT [%s] %s = %s %s",
                result.level.upper(),
                metric.name,
                metric.value,
                metric.unit or "",
            )
        else:
            logger.debug("OK %s", metric.name)

    return results


def format_report(results: List[Tuple[Metric, AlertResult]]) -> str:
    """Produce a human-readable summary of all evaluated metrics.

    Args:
        results: Pairs returned by :func:`evaluate_all`.

    Returns:
        Multi-line string report.
    """
    if not results:
        return "No metrics collected."

    lines = []
    for metric, alert in results:
        unit_str = f" {metric.unit}" if metric.unit else ""
        lines.append(
            f"[{alert.level.upper():8s}] {metric.name}: {metric.value}{unit_str}"
        )
    return "\n".join(lines)
