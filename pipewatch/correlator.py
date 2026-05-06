"""Metric correlation: detect when two metrics move together or inversely."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional, Sequence

from pipewatch.history import HistoryEntry


@dataclass
class CorrelationResult:
    source_a: str
    metric_a: str
    source_b: str
    metric_b: str
    coefficient: float  # Pearson r in [-1, 1]
    sample_size: int

    def __str__(self) -> str:
        direction = "positive" if self.coefficient >= 0 else "negative"
        strength = (
            "strong" if abs(self.coefficient) >= 0.7
            else "moderate" if abs(self.coefficient) >= 0.4
            else "weak"
        )
        return (
            f"{self.source_a}/{self.metric_a} <-> "
            f"{self.source_b}/{self.metric_b}: "
            f"{strength} {direction} correlation "
            f"(r={self.coefficient:.3f}, n={self.sample_size})"
        )


def _pearson(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    """Return Pearson r for two equal-length sequences, or None if undefined."""
    n = len(xs)
    if n < 2:
        return None
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    den_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
    den_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))
    if den_x == 0 or den_y == 0:
        return None
    return num / (den_x * den_y)


def _align_by_timestamp(
    entries_a: List[HistoryEntry],
    entries_b: List[HistoryEntry],
    tolerance: float = 5.0,
) -> tuple[List[float], List[float]]:
    """Pair values whose timestamps are within *tolerance* seconds."""
    xs: List[float] = []
    ys: List[float] = []
    used: set[int] = set()
    for ea in entries_a:
        for i, eb in enumerate(entries_b):
            if i in used:
                continue
            if abs(ea.timestamp - eb.timestamp) <= tolerance:
                xs.append(ea.value)
                ys.append(eb.value)
                used.add(i)
                break
    return xs, ys


def correlate(
    source_a: str,
    metric_a: str,
    entries_a: List[HistoryEntry],
    source_b: str,
    metric_b: str,
    entries_b: List[HistoryEntry],
    timestamp_tolerance: float = 5.0,
) -> Optional[CorrelationResult]:
    """Compute Pearson correlation between two metric histories.

    Returns None if there are fewer than 2 aligned data points.
    """
    xs, ys = _align_by_timestamp(entries_a, entries_b, timestamp_tolerance)
    r = _pearson(xs, ys)
    if r is None:
        return None
    return CorrelationResult(
        source_a=source_a,
        metric_a=metric_a,
        source_b=source_b,
        metric_b=metric_b,
        coefficient=round(r, 6),
        sample_size=len(xs),
    )
