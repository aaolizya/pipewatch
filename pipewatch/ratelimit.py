"""Rate limiting for metric collection requests."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class RateLimitEntry:
    source_name: str
    last_collected: float = field(default_factory=lambda: 0.0)
    request_count: int = 0

    def update(self) -> None:
        self.last_collected = time.monotonic()
        self.request_count += 1


class RateLimiter:
    """Enforces a minimum interval between collections per source."""

    def __init__(self, min_interval: float = 1.0) -> None:
        if min_interval < 0:
            raise ValueError("min_interval must be non-negative")
        self.min_interval = min_interval
        self._entries: Dict[str, RateLimitEntry] = {}

    def _entry(self, source_name: str) -> RateLimitEntry:
        if source_name not in self._entries:
            self._entries[source_name] = RateLimitEntry(source_name=source_name)
        return self._entries[source_name]

    def is_allowed(self, source_name: str) -> bool:
        """Return True if enough time has elapsed since the last collection."""
        entry = self._entry(source_name)
        if entry.last_collected == 0.0:
            return True
        elapsed = time.monotonic() - entry.last_collected
        return elapsed >= self.min_interval

    def record(self, source_name: str) -> None:
        """Record that a collection just occurred for *source_name*."""
        self._entry(source_name).update()

    def time_until_allowed(self, source_name: str) -> float:
        """Return seconds until the source is allowed to be collected again."""
        entry = self._entry(source_name)
        if entry.last_collected == 0.0:
            return 0.0
        elapsed = time.monotonic() - entry.last_collected
        remaining = self.min_interval - elapsed
        return max(0.0, remaining)

    def request_count(self, source_name: str) -> int:
        """Return the total number of collections recorded for *source_name*."""
        return self._entry(source_name).request_count

    def reset(self, source_name: Optional[str] = None) -> None:
        """Reset state for one source, or all sources if *source_name* is None."""
        if source_name is None:
            self._entries.clear()
        else:
            self._entries.pop(source_name, None)
