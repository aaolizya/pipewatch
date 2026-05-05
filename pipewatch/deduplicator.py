"""Alert deduplication: suppress repeated alerts for the same metric/status."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple


@dataclass
class DedupeEntry:
    """Tracks the last-seen alert status for a metric."""

    source: str
    metric_name: str
    last_status: str
    last_value: float

    @property
    def key(self) -> Tuple[str, str]:
        return (self.source, self.metric_name)


class AlertDeduplicator:
    """Suppresses alerts that repeat the same source/metric/status combination.

    Only forwards an alert when the status *changes* relative to the last
    time that metric was seen, preventing notification storms during
    sustained pipeline outages.
    """

    def __init__(self) -> None:
        self._seen: Dict[Tuple[str, str], DedupeEntry] = {}

    def is_new(self, source: str, metric_name: str, status: str, value: float) -> bool:
        """Return True if this alert represents a status change (or is unseen)."""
        key = (source, metric_name)
        entry = self._seen.get(key)
        if entry is None or entry.last_status != status:
            self._seen[key] = DedupeEntry(
                source=source,
                metric_name=metric_name,
                last_status=status,
                last_value=value,
            )
            return True
        # Same status — update value but suppress notification
        entry.last_value = value
        return False

    def reset(self, source: str, metric_name: str) -> None:
        """Clear deduplication state for a specific metric."""
        self._seen.pop((source, metric_name), None)

    def reset_all(self) -> None:
        """Clear all tracked state."""
        self._seen.clear()

    def last_entry(self, source: str, metric_name: str) -> Optional[DedupeEntry]:
        """Return the stored entry for a metric, or None."""
        return self._seen.get((source, metric_name))

    def __len__(self) -> int:
        return len(self._seen)
