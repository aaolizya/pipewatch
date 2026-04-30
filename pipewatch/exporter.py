"""Export pipeline summary data to various output formats."""

from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from typing import Any

from pipewatch.summary import PipelineSummary, SourceSummary


def _source_to_dict(source: SourceSummary) -> dict[str, Any]:
    """Serialize a SourceSummary to a plain dictionary."""
    return {
        "source": source.name,
        "status": source.status,
        "healthy": source.healthy,
        "metric_count": len(source.results),
        "warning_count": sum(
            1 for r in source.results if r.alert_result and r.alert_result.level == "warning"
        ),
        "critical_count": sum(
            1 for r in source.results if r.alert_result and r.alert_result.level == "critical"
        ),
    }


def export_json(summary: PipelineSummary, *, indent: int = 2) -> str:
    """Serialize a PipelineSummary to a JSON string."""
    payload: dict[str, Any] = {
        "exported_at": datetime.now(tz=timezone.utc).isoformat(),
        "overall_status": summary.overall_status,
        "sources": [_source_to_dict(s) for s in summary.sources],
    }
    return json.dumps(payload, indent=indent)


def export_csv(summary: PipelineSummary) -> str:
    """Serialize a PipelineSummary to a CSV string."""
    fieldnames = [
        "source",
        "status",
        "healthy",
        "metric_count",
        "warning_count",
        "critical_count",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for source in summary.sources:
        writer.writerow(_source_to_dict(source))
    return buf.getvalue()


def export_text(summary: PipelineSummary) -> str:
    """Serialize a PipelineSummary to a human-readable text block."""
    lines = [
        f"Pipeline Status: {summary.overall_status.upper()}",
        "-" * 40,
    ]
    for source in summary.sources:
        d = _source_to_dict(source)
        lines.append(
            f"  [{d['status'].upper():8}] {d['source']}  "
            f"(metrics={d['metric_count']}, "
            f"warn={d['warning_count']}, "
            f"crit={d['critical_count']})"
        )
    return "\n".join(lines)
