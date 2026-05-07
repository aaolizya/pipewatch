"""Metric value sampler: down-samples a stream of metric values to a
fixed-size reservoir using reservoir sampling (Algorithm R).
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.metrics import Metric


@dataclass
class SamplerStats:
    """Summary statistics produced from the current reservoir."""

    source: str
    metric_name: str
    sample_size: int
    total_seen: int
    min_value: float
    max_value: float
    mean_value: float

    def __str__(self) -> str:
        return (
            f"[{self.source}/{self.metric_name}] "
            f"sample={self.sample_size}/{self.total_seen} "
            f"min={self.min_value:.4g} max={self.max_value:.4g} "
            f"mean={self.mean_value:.4g}"
        )


@dataclass
class MetricSampler:
    """Reservoir sampler for a single (source, metric_name) pair."""

    source: str
    metric_name: str
    capacity: int = 100
    _reservoir: List[float] = field(default_factory=list, init=False, repr=False)
    _total_seen: int = field(default=0, init=False, repr=False)
    _rng: random.Random = field(default_factory=random.Random, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.capacity < 1:
            raise ValueError(f"capacity must be >= 1, got {self.capacity}")

    def record(self, value: float) -> None:
        """Add a value to the reservoir using Algorithm R."""
        self._total_seen += 1
        n = self._total_seen
        if len(self._reservoir) < self.capacity:
            self._reservoir.append(value)
        else:
            j = self._rng.randint(0, n - 1)
            if j < self.capacity:
                self._reservoir[j] = value

    def record_metric(self, metric: Metric) -> None:
        """Convenience wrapper that extracts the numeric value from a Metric."""
        self.record(metric.value)

    def stats(self) -> Optional[SamplerStats]:
        """Return summary statistics, or None if no values have been recorded."""
        if not self._reservoir:
            return None
        values = self._reservoir
        return SamplerStats(
            source=self.source,
            metric_name=self.metric_name,
            sample_size=len(values),
            total_seen=self._total_seen,
            min_value=min(values),
            max_value=max(values),
            mean_value=sum(values) / len(values),
        )

    def reset(self) -> None:
        """Clear the reservoir and counter."""
        self._reservoir.clear()
        self._total_seen = 0
