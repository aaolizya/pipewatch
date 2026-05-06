"""Endpoint-style health check for pipewatch pipeline status."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.summary import PipelineSummary


@dataclass
class HealthCheckResult:
    """Result of a pipeline health check."""

    healthy: bool
    status: str
    total_sources: int
    ok_count: int
    warning_count: int
    critical_count: int
    critical_sources: List[str] = field(default_factory=list)
    message: Optional[str] = None

    def __str__(self) -> str:
        parts = [
            f"status={self.status}",
            f"sources={self.total_sources}",
            f"ok={self.ok_count}",
            f"warning={self.warning_count}",
            f"critical={self.critical_count}",
        ]
        if self.message:
            parts.append(f"message={self.message!r}")
        return "HealthCheckResult(" + ", ".join(parts) + ")"


def run_healthcheck(
    summary: PipelineSummary,
    allow_warnings: bool = True,
) -> HealthCheckResult:
    """Evaluate overall pipeline health from a PipelineSummary.

    Args:
        summary: The current pipeline summary.
        allow_warnings: If True, WARNING status does not mark the check unhealthy.

    Returns:
        A HealthCheckResult describing the current state.
    """
    ok_count = 0
    warning_count = 0
    critical_count = 0
    critical_sources: List[str] = []

    for src in summary.sources:
        s = src.status.upper()
        if s == "OK":
            ok_count += 1
        elif s == "WARNING":
            warning_count += 1
        elif s == "CRITICAL":
            critical_count += 1
            critical_sources.append(src.source_name)

    total = len(summary.sources)
    is_critical = critical_count > 0
    is_warning = warning_count > 0 and not allow_warnings

    healthy = not is_critical and not is_warning

    if is_critical:
        status = "CRITICAL"
        message = f"{critical_count} source(s) critical: {', '.join(critical_sources)}"
    elif is_warning and not allow_warnings:
        status = "WARNING"
        message = f"{warning_count} source(s) in warning state"
    else:
        status = "OK"
        message = "All sources healthy" if ok_count == total else "No critical issues"

    return HealthCheckResult(
        healthy=healthy,
        status=status,
        total_sources=total,
        ok_count=ok_count,
        warning_count=warning_count,
        critical_count=critical_count,
        critical_sources=critical_sources,
        message=message,
    )
