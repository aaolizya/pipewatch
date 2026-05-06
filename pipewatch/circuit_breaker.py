"""Circuit breaker for protecting against repeated collection failures."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, TypeVar

from pipewatch.collector import CollectionError

T = TypeVar("T")


class BreakerState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing; requests blocked
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class BreakerEntry:
    state: BreakerState = BreakerState.CLOSED
    failure_count: int = 0
    last_failure_time: float = 0.0
    last_success_time: float = 0.0


class CircuitBreaker:
    """Per-source circuit breaker that opens after repeated failures."""

    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_timeout: float = 60.0,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be positive")
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._entries: dict[str, BreakerEntry] = {}

    def _entry(self, source_name: str) -> BreakerEntry:
        if source_name not in self._entries:
            self._entries[source_name] = BreakerEntry()
        return self._entries[source_name]

    def state(self, source_name: str) -> BreakerState:
        entry = self._entry(source_name)
        if entry.state == BreakerState.OPEN:
            if time.monotonic() - entry.last_failure_time >= self.recovery_timeout:
                entry.state = BreakerState.HALF_OPEN
        return entry.state

    def is_allowed(self, source_name: str) -> bool:
        return self.state(source_name) != BreakerState.OPEN

    def record_success(self, source_name: str) -> None:
        entry = self._entry(source_name)
        entry.failure_count = 0
        entry.state = BreakerState.CLOSED
        entry.last_success_time = time.monotonic()

    def record_failure(self, source_name: str) -> None:
        entry = self._entry(source_name)
        entry.failure_count += 1
        entry.last_failure_time = time.monotonic()
        if entry.failure_count >= self.failure_threshold:
            entry.state = BreakerState.OPEN

    def call(
        self,
        source_name: str,
        fn: Callable[[], T],
    ) -> T:
        """Execute *fn* under the circuit breaker for *source_name*."""
        if not self.is_allowed(source_name):
            raise CollectionError(
                f"Circuit breaker OPEN for source '{source_name}'; "
                "skipping collection"
            )
        try:
            result = fn()
        except Exception:
            self.record_failure(source_name)
            raise
        self.record_success(source_name)
        return result

    def reset(self, source_name: str) -> None:
        """Manually reset the breaker for *source_name* to CLOSED."""
        self._entries.pop(source_name, None)
