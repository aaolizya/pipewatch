"""Replay historical metric data through the alerting pipeline for debugging and testing."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.history import HistoryEntry, MetricHistory
from pipewatch.metrics import Metric, evaluate_metric
from pipewatch.alerting import AlertResult


@dataclass
class ReplayEvent:
    """A single replayed metric evaluation."""
    source_name: str
    metric: Metric
    result: AlertResult

    def __str__(self) -> str:
        return f"[{self.source_name}] {self.metric.name}={self.metric.value} -> {self.result.status}"


@dataclass
class ReplayReport:
    """Aggregated results from a replay run."""
    events: List[ReplayEvent] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.events)

    @property
    def alert_count(self) -> int:
        return sum(1 for e in self.events if e.result.is_alert)

    def __str__(self) -> str:
        return (
            f"ReplayReport: {self.total} events, "
            f"{self.alert_count} alerts"
        )


def replay_history(
    source_name: str,
    history: MetricHistory,
    metric_name: str,
    warn_threshold: Optional[float] = None,
    crit_threshold: Optional[float] = None,
    limit: Optional[int] = None,
) -> ReplayReport:
    """Replay stored history entries for a single metric through evaluate_metric."""
    entries: List[HistoryEntry] = history.get(source_name, metric_name)
    if limit is not None:
        entries = entries[-limit:]

    report = ReplayReport()
    for entry in entries:
        metric = Metric(
            name=metric_name,
            value=entry.value,
            timestamp=entry.timestamp,
            unit=entry.unit,
            warn_threshold=warn_threshold,
            crit_threshold=crit_threshold,
        )
        result = evaluate_metric(metric)
        report.events.append(ReplayEvent(
            source_name=source_name,
            metric=metric,
            result=result,
        ))
    return report
