"""Alert escalation policy: upgrade severity after repeated failures."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class EscalationEntry:
    source: str
    metric: str
    consecutive_failures: int = 0
    first_failure_at: Optional[float] = None
    escalated: bool = False

    def _key(self) -> str:
        return f"{self.source}::{self.metric}"


@dataclass
class EscalationPolicy:
    """Escalate an alert to critical after *threshold* consecutive failures
    that span at least *min_duration_seconds*."""

    threshold: int = 3
    min_duration_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.threshold < 1:
            raise ValueError("threshold must be >= 1")
        if self.min_duration_seconds < 0:
            raise ValueError("min_duration_seconds must be >= 0")


class AlertEscalator:
    """Tracks repeated alert states and decides when to escalate."""

    def __init__(self, policy: Optional[EscalationPolicy] = None) -> None:
        self._policy = policy or EscalationPolicy()
        self._entries: Dict[str, EscalationEntry] = {}

    def _entry(self, source: str, metric: str) -> EscalationEntry:
        key = f"{source}::{metric}"
        if key not in self._entries:
            self._entries[key] = EscalationEntry(source=source, metric=metric)
        return self._entries[key]

    def record_alert(self, source: str, metric: str, now: Optional[float] = None) -> bool:
        """Record an alert occurrence. Returns True if the alert should be escalated."""
        now = now if now is not None else time.time()
        entry = self._entry(source, metric)
        entry.consecutive_failures += 1
        if entry.first_failure_at is None:
            entry.first_failure_at = now
        elapsed = now - entry.first_failure_at
        if (
            entry.consecutive_failures >= self._policy.threshold
            and elapsed >= self._policy.min_duration_seconds
        ):
            entry.escalated = True
        return entry.escalated

    def record_recovery(self, source: str, metric: str) -> None:
        """Reset tracking for a metric that has recovered."""
        key = f"{source}::{metric}"
        self._entries.pop(key, None)

    def is_escalated(self, source: str, metric: str) -> bool:
        return self._entry(source, metric).escalated

    def failure_count(self, source: str, metric: str) -> int:
        return self._entry(source, metric).consecutive_failures
