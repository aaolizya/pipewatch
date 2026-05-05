"""Output formatters for pipeline metric reports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from pipewatch.summary import PipelineSummary, SourceSummary

_STATUS_COLORS = {
    "ok": "\033[32m",       # green
    "warning": "\033[33m",  # yellow
    "critical": "\033[31m", # red
    "unknown": "\033[90m",  # dark grey
}
_RESET = "\033[0m"
_BOLD = "\033[1m"


@dataclass
class FormatOptions:
    color: bool = True
    show_ok: bool = True
    compact: bool = False
    timestamp_format: str = "%Y-%m-%d %H:%M:%S"


def _colorize(text: str, status: str, options: FormatOptions) -> str:
    if not options.color:
        return text
    code = _STATUS_COLORS.get(status.lower(), "")
    return f"{code}{text}{_RESET}" if code else text


def _format_source(source: SourceSummary, options: FormatOptions) -> List[str]:
    lines: List[str] = []
    status = source.status()
    if not options.show_ok and status == "ok":
        return lines

    header = f"  [{status.upper()}] {source.source_name}"
    lines.append(_colorize(header, status, options))

    if options.compact:
        return lines

    for result in source.results:
        alert = result.alert
        level = alert.level if alert else "ok"
        metric_line = (
            f"    {result.metric.name}: {result.metric.value}"
            f"{' ' + result.metric.unit if result.metric.unit else ''}"
            f" [{level.upper()}]"
        )
        lines.append(_colorize(metric_line, level, options))

    return lines


def format_pipeline_summary(
    summary: PipelineSummary,
    options: Optional[FormatOptions] = None,
) -> str:
    """Render a PipelineSummary as a human-readable string."""
    if options is None:
        options = FormatOptions()

    overall = summary.overall_status()
    lines: List[str] = []

    title = f"{_BOLD}PipeWatch Report{_RESET}" if options.color else "PipeWatch Report"
    lines.append(title)
    lines.append(
        _colorize(f"Overall status: {overall.upper()}", overall, options)
    )
    lines.append("")

    for src in summary.sources:
        lines.extend(_format_source(src, options))

    return "\n".join(lines)
