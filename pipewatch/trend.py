"""Trend analysis helpers built on top of MetricHistory."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from pipewatch.history import HistoryEntry


class TrendDirection(str, Enum):
    RISING = "rising"
    FALLING = "falling"
    STABLE = "stable"
    INSUFFICIENT_DATA = "insufficient_data"


@dataclass
class TrendResult:
    direction: TrendDirection
    change_pct: Optional[float]
    window: int

    def __str__(self) -> str:
        if self.direction == TrendDirection.INSUFFICIENT_DATA:
            return f"trend=insufficient_data (need {self.window} points)"
        sign = "+" if (self.change_pct or 0) >= 0 else ""
        return f"trend={self.direction.value} ({sign}{self.change_pct:.1f}% over {self.window} samples)"


def compute_trend(
    entries: List[HistoryEntry],
    window: int = 5,
    rising_threshold_pct: float = 10.0,
    falling_threshold_pct: float = -10.0,
) -> TrendResult:
    """Compute trend direction from the most recent *window* history entries.

    Args:
        entries: Ordered list of history entries (oldest first).
        window: Number of recent entries to consider.
        rising_threshold_pct: Percentage change above which trend is RISING.
        falling_threshold_pct: Percentage change below which trend is FALLING.

    Returns:
        A TrendResult describing the direction and magnitude of change.
    """
    recent = entries[-window:] if len(entries) >= window else entries

    if len(recent) < 2:
        return TrendResult(
            direction=TrendDirection.INSUFFICIENT_DATA,
            change_pct=None,
            window=window,
        )

    first_val = recent[0].value
    last_val = recent[-1].value

    if first_val == 0:
        change_pct = 0.0 if last_val == 0 else float("inf")
    else:
        change_pct = ((last_val - first_val) / abs(first_val)) * 100.0

    if change_pct > rising_threshold_pct:
        direction = TrendDirection.RISING
    elif change_pct < falling_threshold_pct:
        direction = TrendDirection.FALLING
    else:
        direction = TrendDirection.STABLE

    return TrendResult(direction=direction, change_pct=change_pct, window=len(recent))
