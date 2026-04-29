"""Summary reporting for pipewatch pipeline health metrics."""

from dataclasses import dataclass, field
from typing import List, Dict

from pipewatch.metrics import AlertResult, Metric
from pipewatch.trend import TrendResult, TrendDirection


@dataclass
class SourceSummary:
    """Aggregated health summary for a single source."""

    source_name: str
    total: int = 0
    ok: int = 0
    warnings: int = 0
    criticals: int = 0
    errors: int = 0
    trend_results: List[TrendResult] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return self.criticals == 0 and self.errors == 0

    @property
    def status(self) -> str:
        if self.errors > 0 or self.criticals > 0:
            return "CRITICAL"
        if self.warnings > 0:
            return "WARNING"
        return "OK"

    def __str__(self) -> str:
        trend_summary = ""
        rising = sum(1 for t in self.trend_results if t.direction == TrendDirection.RISING)
        falling = sum(1 for t in self.trend_results if t.direction == TrendDirection.FALLING)
        if rising or falling:
            trend_summary = f" | trends: +{rising}/-{falling}"
        return (
            f"[{self.status}] {self.source_name}: "
            f"{self.ok} ok, {self.warnings} warn, {self.criticals} crit, {self.errors} err"
            f"{trend_summary}"
        )


@dataclass
class PipelineSummary:
    """Aggregated health summary across all sources."""

    sources: Dict[str, SourceSummary] = field(default_factory=dict)

    @property
    def overall_status(self) -> str:
        statuses = [s.status for s in self.sources.values()]
        if "CRITICAL" in statuses:
            return "CRITICAL"
        if "WARNING" in statuses:
            return "WARNING"
        return "OK"

    def __str__(self) -> str:
        lines = [f"=== Pipeline Summary [{self.overall_status}] ==="]
        for summary in self.sources.values():
            lines.append(f"  {summary}")
        return "\n".join(lines)


def build_summary(
    alert_results: List[AlertResult],
    trend_results: Dict[str, TrendResult] = None,
) -> PipelineSummary:
    """Build a PipelineSummary from a list of AlertResults and optional trends."""
    trend_results = trend_results or {}
    pipeline = PipelineSummary()

    for result in alert_results:
        name = result.metric.source_name
        if name not in pipeline.sources:
            pipeline.sources[name] = SourceSummary(source_name=name)
        summary = pipeline.sources[name]
        summary.total += 1

        level = result.level
        if level == "ok":
            summary.ok += 1
        elif level == "warning":
            summary.warnings += 1
        elif level == "critical":
            summary.criticals += 1
        else:
            summary.errors += 1

    for key, trend in trend_results.items():
        source_name = key.split(":")[0] if ":" in key else key
        if source_name in pipeline.sources:
            pipeline.sources[source_name].trend_results.append(trend)

    return pipeline
