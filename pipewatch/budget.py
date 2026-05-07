"""Alert budget tracking: limit how many alerts fire per source per window."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional


@dataclass
class BudgetEntry:
    source: str
    limit: int
    window_seconds: float
    fired: int = 0
    window_start: Optional[datetime] = None

    def _reset_if_expired(self, now: datetime) -> None:
        if self.window_start is None:
            self.window_start = now
            return
        elapsed = (now - self.window_start).total_seconds()
        if elapsed >= self.window_seconds:
            self.fired = 0
            self.window_start = now

    def consume(self, now: Optional[datetime] = None) -> bool:
        """Try to consume one alert slot. Returns True if allowed."""
        now = now or datetime.utcnow()
        self._reset_if_expired(now)
        if self.fired < self.limit:
            self.fired += 1
            return True
        return False

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.fired)

    def exhausted(self) -> bool:
        return self.fired >= self.limit


@dataclass
class AlertBudget:
    default_limit: int = 10
    default_window_seconds: float = 3600.0
    _entries: Dict[str, BudgetEntry] = field(default_factory=dict)

    def set_limit(self, source: str, limit: int, window_seconds: Optional[float] = None) -> None:
        ws = window_seconds if window_seconds is not None else self.default_window_seconds
        self._entries[source] = BudgetEntry(
            source=source, limit=limit, window_seconds=ws
        )

    def _entry(self, source: str) -> BudgetEntry:
        if source not in self._entries:
            self._entries[source] = BudgetEntry(
                source=source,
                limit=self.default_limit,
                window_seconds=self.default_window_seconds,
            )
        return self._entries[source]

    def consume(self, source: str, now: Optional[datetime] = None) -> bool:
        """Consume one alert slot for *source*. Returns True if the alert is allowed."""
        return self._entry(source).consume(now=now)

    def remaining(self, source: str) -> int:
        return self._entry(source).remaining

    def exhausted(self, source: str) -> bool:
        return self._entry(source).exhausted()

    def reset(self, source: str) -> None:
        """Force-reset a source's budget window."""
        if source in self._entries:
            e = self._entries[source]
            e.fired = 0
            e.window_start = None
