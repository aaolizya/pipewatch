"""Adaptive polling scheduler for pipewatch sources."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class ScheduleEntry:
    source_name: str
    base_interval: float
    last_run: float = field(default_factory=lambda: 0.0)
    jitter: float = 0.0

    @property
    def next_run(self) -> float:
        return self.last_run + self.base_interval + self.jitter

    def is_due(self, now: Optional[float] = None) -> bool:
        if now is None:
            now = time.monotonic()
        return now >= self.next_run

    def mark_ran(self, now: Optional[float] = None, jitter: float = 0.0) -> None:
        self.last_run = now if now is not None else time.monotonic()
        self.jitter = jitter


class Scheduler:
    """Tracks per-source poll schedules and yields sources that are due."""

    def __init__(self, default_interval: float = 60.0) -> None:
        if default_interval <= 0:
            raise ValueError("default_interval must be positive")
        self.default_interval = default_interval
        self._entries: Dict[str, ScheduleEntry] = {}

    def register(self, source_name: str, interval: Optional[float] = None) -> None:
        """Register a source with an optional per-source interval."""
        effective = interval if interval is not None else self.default_interval
        if effective <= 0:
            raise ValueError(f"interval for '{source_name}' must be positive")
        self._entries[source_name] = ScheduleEntry(
            source_name=source_name,
            base_interval=effective,
        )

    def due(self, now: Optional[float] = None) -> list[str]:
        """Return names of sources whose next_run time has passed."""
        ts = now if now is not None else time.monotonic()
        return [
            name
            for name, entry in self._entries.items()
            if entry.is_due(ts)
        ]

    def mark_ran(self, source_name: str, now: Optional[float] = None, jitter: float = 0.0) -> None:
        """Record that a source was just collected."""
        if source_name not in self._entries:
            raise KeyError(f"Unknown source: '{source_name}'")
        self._entries[source_name].mark_ran(now=now, jitter=jitter)

    def entry(self, source_name: str) -> ScheduleEntry:
        return self._entries[source_name]
