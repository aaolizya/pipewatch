"""Metric value profiler: tracks min/max/avg over a rolling window per source+metric."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ProfileStats:
    name: str
    source: str
    count: int
    minimum: float
    maximum: float
    mean: float
    p50: float
    p95: float

    def __str__(self) -> str:
        return (
            f"{self.source}/{self.name}: "
            f"n={self.count} min={self.minimum:.3g} "
            f"max={self.maximum:.3g} mean={self.mean:.3g} "
            f"p50={self.p50:.3g} p95={self.p95:.3g}"
        )


def _percentile(sorted_values: List[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    idx = (len(sorted_values) - 1) * pct / 100.0
    lo = int(idx)
    hi = min(lo + 1, len(sorted_values) - 1)
    frac = idx - lo
    return sorted_values[lo] + frac * (sorted_values[hi] - sorted_values[lo])


class MetricProfiler:
    """Accumulates raw values per (source, metric_name) and computes profile stats."""

    def __init__(self, window: int = 100) -> None:
        if window < 1:
            raise ValueError("window must be >= 1")
        self._window = window
        self._data: Dict[str, List[float]] = {}

    def _key(self, source: str, name: str) -> str:
        return f"{source}\x00{name}"

    def record(self, source: str, name: str, value: float) -> None:
        k = self._key(source, name)
        buf = self._data.setdefault(k, [])
        buf.append(value)
        if len(buf) > self._window:
            del buf[0]

    def profile(self, source: str, name: str) -> Optional[ProfileStats]:
        k = self._key(source, name)
        values = self._data.get(k)
        if not values:
            return None
        sv = sorted(values)
        count = len(sv)
        return ProfileStats(
            name=name,
            source=source,
            count=count,
            minimum=sv[0],
            maximum=sv[-1],
            mean=sum(sv) / count,
            p50=_percentile(sv, 50),
            p95=_percentile(sv, 95),
        )

    def all_profiles(self) -> List[ProfileStats]:
        results = []
        for k in self._data:
            source, name = k.split("\x00", 1)
            p = self.profile(source, name)
            if p is not None:
                results.append(p)
        return results

    def clear(self, source: str, name: str) -> None:
        self._data.pop(self._key(source, name), None)
