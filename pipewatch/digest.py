"""Periodic digest reports summarising pipeline health over a time window."""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.aggregator import AggregationResult, aggregate_from_history
from pipewatch.history import MetricHistory
from pipewatch.config import SourceConfig


@dataclass
class DigestEntry:
    source_name: str
    metric_name: str
    aggregation: AggregationResult

    def __str__(self) -> str:
        return (
            f"{self.source_name}/{self.metric_name}: "
            f"min={self.aggregation.min:.3g} "
            f"max={self.aggregation.max:.3g} "
            f"mean={self.aggregation.mean:.3g} "
            f"samples={self.aggregation.count}"
        )


@dataclass
class DigestReport:
    generated_at: datetime.datetime
    window_seconds: int
    entries: List[DigestEntry] = field(default_factory=list)

    @property
    def source_names(self) -> List[str]:
        return sorted({e.source_name for e in self.entries})

    def entries_for_source(self, source_name: str) -> List[DigestEntry]:
        return [e for e in self.entries if e.source_name == source_name]

    def __str__(self) -> str:
        lines = [
            f"=== Digest Report ({self.generated_at.strftime('%Y-%m-%d %H:%M:%S')}) ===",
            f"Window: {self.window_seconds}s | Sources: {len(self.source_names)} | Metrics: {len(self.entries)}",
        ]
        for src in self.source_names:
            lines.append(f"  [{src}]")
            for entry in self.entries_for_source(src):
                lines.append(f"    {entry}")
        return "\n".join(lines)


def build_digest(
    sources: List[SourceConfig],
    history: MetricHistory,
    window_seconds: int = 3600,
    now: Optional[datetime.datetime] = None,
) -> DigestReport:
    """Build a DigestReport from recorded history within *window_seconds*."""
    if now is None:
        now = datetime.datetime.utcnow()

    cutoff = now - datetime.timedelta(seconds=window_seconds)
    entries: List[DigestEntry] = []

    for source in sources:
        metric_names = {
            e.metric_name
            for e in history._store.values()
            for e in e  # type: ignore[attr-defined]
            if e.source_name == source.name
        } if hasattr(history, '_store') else set()

        # Collect unique metric names from history keys
        seen: set = set()
        for key in list(getattr(history, '_store', {})):
            s, m = key
            if s == source.name:
                seen.add(m)

        for metric_name in seen:
            result = aggregate_from_history(
                history, source.name, metric_name,
                since=cutoff, now=now,
            )
            if result is not None:
                entries.append(DigestEntry(
                    source_name=source.name,
                    metric_name=metric_name,
                    aggregation=result,
                ))

    return DigestReport(
        generated_at=now,
        window_seconds=window_seconds,
        entries=entries,
    )
