"""Metrics collection and evaluation for pipeline health monitoring."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class Metric:
    """Represents a single collected metric from a pipeline source."""

    source_name: str
    value: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    unit: Optional[str] = None

    def __repr__(self) -> str:
        ts = self.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        unit_str = f" {self.unit}" if self.unit else ""
        return f"Metric(source={self.source_name!r}, value={self.value}{unit_str}, ts={ts})"


@dataclass
class AlertResult:
    """Result of evaluating a metric against its configured thresholds."""

    metric: Metric
    threshold_warning: Optional[float]
    threshold_critical: Optional[float]
    level: str  # "ok", "warning", or "critical"

    @property
    def is_alert(self) -> bool:
        return self.level in ("warning", "critical")

    def __repr__(self) -> str:
        return (
            f"AlertResult(source={self.metric.source_name!r}, "
            f"value={self.metric.value}, level={self.level!r})"
        )


def evaluate_metric(
    metric: Metric,
    threshold_warning: Optional[float] = None,
    threshold_critical: Optional[float] = None,
) -> AlertResult:
    """Evaluate a metric against warning and critical thresholds.

    Critical threshold takes precedence over warning when both are set.
    Returns an AlertResult with the appropriate alert level.
    """
    level = "ok"

    if threshold_warning is not None and metric.value >= threshold_warning:
        level = "warning"

    if threshold_critical is not None and metric.value >= threshold_critical:
        level = "critical"

    return AlertResult(
        metric=metric,
        threshold_warning=threshold_warning,
        threshold_critical=threshold_critical,
        level=level,
    )
