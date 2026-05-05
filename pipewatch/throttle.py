"""Alert throttling to suppress repeated notifications within a cooldown window."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class ThrottleEntry:
    """Tracks the last notification time for a given alert key."""

    last_notified_at: float
    count: int = 1


class AlertThrottle:
    """Suppresses duplicate alert notifications within a configurable cooldown period.

    Args:
        cooldown_seconds: Minimum seconds between repeated notifications for the
            same source/metric combination. Defaults to 300 (5 minutes).
    """

    def __init__(self, cooldown_seconds: float = 300.0) -> None:
        self.cooldown_seconds = cooldown_seconds
        self._state: Dict[str, ThrottleEntry] = {}

    def _key(self, source_name: str, metric_name: str) -> str:
        return f"{source_name}:{metric_name}"

    def should_notify(self, source_name: str, metric_name: str) -> bool:
        """Return True if a notification should be sent for this alert.

        A notification is allowed when no prior notification has been recorded
        or when the cooldown window has elapsed since the last one.
        """
        key = self._key(source_name, metric_name)
        entry = self._state.get(key)
        if entry is None:
            return True
        return (time.monotonic() - entry.last_notified_at) >= self.cooldown_seconds

    def record(self, source_name: str, metric_name: str) -> None:
        """Mark that a notification was just sent for this alert."""
        key = self._key(source_name, metric_name)
        existing = self._state.get(key)
        if existing is None:
            self._state[key] = ThrottleEntry(last_notified_at=time.monotonic())
        else:
            existing.last_notified_at = time.monotonic()
            existing.count += 1

    def reset(self, source_name: str, metric_name: Optional[str] = None) -> None:
        """Clear throttle state for a source, or a specific metric within it."""
        if metric_name is not None:
            self._state.pop(self._key(source_name, metric_name), None)
        else:
            prefix = f"{source_name}:"
            keys_to_remove = [k for k in self._state if k.startswith(prefix)]
            for k in keys_to_remove:
                del self._state[k]

    def notification_count(self, source_name: str, metric_name: str) -> int:
        """Return how many times a notification has been sent for this alert."""
        entry = self._state.get(self._key(source_name, metric_name))
        return entry.count if entry else 0
