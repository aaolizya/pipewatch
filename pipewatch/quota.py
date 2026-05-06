"""Request quota tracking for pipeline sources."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional


@dataclass
class QuotaEntry:
    limit: int
    used: int = 0
    window_start: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    window_seconds: int = 3600

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.used)

    @property
    def exhausted(self) -> bool:
        return self.used >= self.limit

    def window_expired(self, now: Optional[datetime] = None) -> bool:
        now = now or datetime.now(timezone.utc)
        elapsed = (now - self.window_start).total_seconds()
        return elapsed >= self.window_seconds

    def reset(self, now: Optional[datetime] = None) -> None:
        now = now or datetime.now(timezone.utc)
        self.used = 0
        self.window_start = now

    def consume(self, amount: int = 1) -> bool:
        """Consume *amount* quota units. Returns True if allowed."""
        if self.exhausted:
            return False
        self.used += amount
        return True


class QuotaManager:
    """Tracks per-source request quotas with rolling time windows."""

    def __init__(self, default_limit: int = 1000, window_seconds: int = 3600) -> None:
        if default_limit < 0:
            raise ValueError("default_limit must be non-negative")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self._default_limit = default_limit
        self._window_seconds = window_seconds
        self._entries: Dict[str, QuotaEntry] = {}

    def _entry(self, source: str) -> QuotaEntry:
        if source not in self._entries:
            self._entries[source] = QuotaEntry(
                limit=self._default_limit,
                window_seconds=self._window_seconds,
            )
        return self._entries[source]

    def check_and_consume(self, source: str, now: Optional[datetime] = None) -> bool:
        """Return True and consume one unit if quota remains; False otherwise."""
        entry = self._entry(source)
        if entry.window_expired(now):
            entry.reset(now)
        return entry.consume()

    def remaining(self, source: str) -> int:
        entry = self._entry(source)
        return entry.remaining

    def set_limit(self, source: str, limit: int) -> None:
        if limit < 0:
            raise ValueError("limit must be non-negative")
        entry = self._entry(source)
        entry.limit = limit

    def reset(self, source: str) -> None:
        self._entry(source).reset()
