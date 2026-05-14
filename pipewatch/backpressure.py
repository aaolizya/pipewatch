"""Backpressure controller for pipewatch.

Tracks queue depth or lag metrics per source and signals whether the
pipeline should slow down or pause collection to avoid overwhelming
downstream systems.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class BackpressureEntry:
    source_name: str
    current_depth: float = 0.0
    high_watermark: float = 1000.0
    low_watermark: float = 500.0
    throttled: bool = False

    def update(self, depth: float) -> None:
        """Update the current queue depth and recalculate throttle state."""
        if depth < 0:
            raise ValueError("depth must be non-negative")
        self.current_depth = depth
        if self.throttled:
            # Only release backpressure once depth drops below low watermark
            if depth <= self.low_watermark:
                self.throttled = False
        else:
            if depth >= self.high_watermark:
                self.throttled = True

    @property
    def pressure_ratio(self) -> float:
        """Return depth as a fraction of the high watermark (0.0–1.0+)."""
        if self.high_watermark == 0:
            return 0.0
        return self.current_depth / self.high_watermark

    def __str__(self) -> str:
        state = "THROTTLED" if self.throttled else "OK"
        return (
            f"BackpressureEntry({self.source_name} depth={self.current_depth:.1f} "
            f"hwm={self.high_watermark:.1f} [{state}])"
        )


@dataclass
class BackpressureController:
    high_watermark: float = 1000.0
    low_watermark: float = 500.0
    _entries: Dict[str, BackpressureEntry] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        if self.high_watermark <= 0:
            raise ValueError("high_watermark must be positive")
        if self.low_watermark < 0:
            raise ValueError("low_watermark must be non-negative")
        if self.low_watermark >= self.high_watermark:
            raise ValueError("low_watermark must be less than high_watermark")

    def _entry(self, source_name: str) -> BackpressureEntry:
        if source_name not in self._entries:
            self._entries[source_name] = BackpressureEntry(
                source_name=source_name,
                high_watermark=self.high_watermark,
                low_watermark=self.low_watermark,
            )
        return self._entries[source_name]

    def record(self, source_name: str, depth: float) -> None:
        """Record a new queue depth observation for *source_name*."""
        self._entry(source_name).update(depth)

    def is_throttled(self, source_name: str) -> bool:
        """Return True if *source_name* is currently under backpressure."""
        return self._entry(source_name).throttled

    def pressure_ratio(self, source_name: str) -> float:
        """Return the pressure ratio for *source_name*."""
        return self._entry(source_name).pressure_ratio

    def throttled_sources(self) -> list[str]:
        """Return a list of all currently throttled source names."""
        return [name for name, e in self._entries.items() if e.throttled]

    def reset(self, source_name: str) -> None:
        """Remove tracking state for *source_name*."""
        self._entries.pop(source_name, None)
