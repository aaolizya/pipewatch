"""Sliding window metric aggregation over a fixed time range."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional

from pipewatch.history import HistoryEntry


@dataclass
class WindowStats:
    source: str
    metric_name: str
    window_seconds: int
    count: int
    mean: Optional[float]
    min_val: Optional[float]
    max_val: Optional[float]
    oldest: Optional[datetime]
    newest: Optional[datetime]

    def __str__(self) -> str:
        if self.mean is None:
            return f"{self.source}/{self.metric_name} [{self.window_seconds}s] no data"
        return (
            f"{self.source}/{self.metric_name} [{self.window_seconds}s] "
            f"n={self.count} mean={self.mean:.4g} "
            f"min={self.min_val:.4g} max={self.max_val:.4g}"
        )


def _entries_in_window(
    entries: List[HistoryEntry], window_seconds: int, now: Optional[datetime] = None
) -> List[HistoryEntry]:
    """Return entries whose timestamp falls within the window ending at *now*."""
    if now is None:
        now = datetime.utcnow()
    cutoff = now - timedelta(seconds=window_seconds)
    return [e for e in entries if e.timestamp >= cutoff]


def compute_window(
    source: str,
    metric_name: str,
    entries: List[HistoryEntry],
    window_seconds: int,
    now: Optional[datetime] = None,
) -> WindowStats:
    """Compute sliding-window statistics for *entries* over *window_seconds*."""
    if window_seconds <= 0:
        raise ValueError(f"window_seconds must be positive, got {window_seconds}")

    recent = _entries_in_window(entries, window_seconds, now)
    if not recent:
        return WindowStats(
            source=source,
            metric_name=metric_name,
            window_seconds=window_seconds,
            count=0,
            mean=None,
            min_val=None,
            max_val=None,
            oldest=None,
            newest=None,
        )

    values = [e.value for e in recent]
    timestamps = [e.timestamp for e in recent]
    mean = sum(values) / len(values)
    return WindowStats(
        source=source,
        metric_name=metric_name,
        window_seconds=window_seconds,
        count=len(recent),
        mean=mean,
        min_val=min(values),
        max_val=max(values),
        oldest=min(timestamps),
        newest=max(timestamps),
    )
