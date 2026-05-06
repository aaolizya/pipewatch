"""Watchdog: detects sources that have stopped reporting metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional


@dataclass
class WatchdogEntry:
    source_name: str
    last_seen: Optional[datetime] = None
    stale: bool = False

    def update(self, ts: datetime) -> None:
        self.last_seen = ts
        self.stale = False


@dataclass
class StaleSource:
    source_name: str
    last_seen: Optional[datetime]
    silence_seconds: float

    def __str__(self) -> str:
        seen = self.last_seen.isoformat() if self.last_seen else "never"
        return (
            f"[STALE] {self.source_name} — last seen: {seen} "
            f"(silent for >{self.silence_seconds}s)"
        )


class Watchdog:
    """Tracks last-seen timestamps for each source and flags stale ones."""

    def __init__(self, stale_after_seconds: float = 300.0) -> None:
        if stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")
        self.stale_after = stale_after_seconds
        self._entries: Dict[str, WatchdogEntry] = {}

    def record(self, source_name: str, ts: Optional[datetime] = None) -> None:
        """Record a successful metric collection for a source."""
        ts = ts or datetime.utcnow()
        if source_name not in self._entries:
            self._entries[source_name] = WatchdogEntry(source_name=source_name)
        self._entries[source_name].update(ts)

    def check(self, now: Optional[datetime] = None) -> List[StaleSource]:
        """Return sources that have not reported within the stale window."""
        now = now or datetime.utcnow()
        threshold = timedelta(seconds=self.stale_after)
        stale: List[StaleSource] = []
        for name, entry in self._entries.items():
            if entry.last_seen is None or (now - entry.last_seen) > threshold:
                entry.stale = True
                stale.append(
                    StaleSource(
                        source_name=name,
                        last_seen=entry.last_seen,
                        silence_seconds=self.stale_after,
                    )
                )
        return stale

    def register(self, source_name: str) -> None:
        """Pre-register a source without marking it as seen."""
        if source_name not in self._entries:
            self._entries[source_name] = WatchdogEntry(source_name=source_name)

    def reset(self, source_name: str) -> None:
        """Remove tracking entry for a source."""
        self._entries.pop(source_name, None)

    @property
    def tracked_sources(self) -> List[str]:
        return list(self._entries.keys())
