"""Retry logic for transient collection failures."""

from __future__ import annotations

import time
import logging
from dataclasses import dataclass, field
from typing import Callable, TypeVar, Optional

log = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class RetryPolicy:
    """Configuration for retry behaviour."""

    max_attempts: int = 3
    base_delay: float = 1.0
    backoff_factor: float = 2.0
    max_delay: float = 30.0
    retryable_exceptions: tuple = field(
        default_factory=lambda: (Exception,)
    )

    def delay_for(self, attempt: int) -> float:
        """Return the sleep duration before *attempt* (0-indexed)."""
        if attempt == 0:
            return 0.0
        raw = self.base_delay * (self.backoff_factor ** (attempt - 1))
        return min(raw, self.max_delay)


@dataclass
class RetryResult:
    """Outcome of a retried call."""

    value: object
    attempts: int
    succeeded: bool
    last_exception: Optional[BaseException] = None


def with_retry(
    fn: Callable[[], T],
    policy: Optional[RetryPolicy] = None,
    *,
    _sleep: Callable[[float], None] = time.sleep,
) -> RetryResult:
    """Call *fn* up to *policy.max_attempts* times, retrying on transient errors.

    Returns a :class:`RetryResult`.  Raises the last exception when all
    attempts are exhausted.
    """
    if policy is None:
        policy = RetryPolicy()

    last_exc: Optional[BaseException] = None

    for attempt in range(policy.max_attempts):
        delay = policy.delay_for(attempt)
        if delay > 0:
            log.debug("Retry attempt %d/%d — sleeping %.2fs", attempt + 1, policy.max_attempts, delay)
            _sleep(delay)

        try:
            result = fn()
            if attempt > 0:
                log.info("Call succeeded on attempt %d", attempt + 1)
            return RetryResult(value=result, attempts=attempt + 1, succeeded=True)
        except policy.retryable_exceptions as exc:  # type: ignore[misc]
            last_exc = exc
            log.warning(
                "Attempt %d/%d failed: %s",
                attempt + 1,
                policy.max_attempts,
                exc,
            )

    raise last_exc  # type: ignore[misc]
