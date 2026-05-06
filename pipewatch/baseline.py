"""Baseline tracking for pipeline metrics.

Allows recording an expected 'normal' value for each metric and computing
the deviation of observed values from that baseline.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class BaselineEntry:
    source: str
    metric_name: str
    expected_value: float
    tolerance_pct: float = 10.0  # percent deviation considered acceptable

    def deviation_pct(self, observed: float) -> float:
        """Return the percentage deviation of *observed* from the baseline."""
        if self.expected_value == 0:
            return 0.0 if observed == 0 else float("inf")
        return abs(observed - self.expected_value) / abs(self.expected_value) * 100.0

    def is_within_tolerance(self, observed: float) -> bool:
        """Return True when *observed* is within the configured tolerance band."""
        return self.deviation_pct(observed) <= self.tolerance_pct


@dataclass
class BaselineStore:
    _entries: Dict[str, BaselineEntry] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Mutation helpers
    # ------------------------------------------------------------------

    def set(self, source: str, metric_name: str, expected_value: float,
            tolerance_pct: float = 10.0) -> None:
        """Register or overwrite a baseline entry."""
        key = _key(source, metric_name)
        self._entries[key] = BaselineEntry(
            source=source,
            metric_name=metric_name,
            expected_value=expected_value,
            tolerance_pct=tolerance_pct,
        )

    def get(self, source: str, metric_name: str) -> Optional[BaselineEntry]:
        """Return the baseline entry for *source*/*metric_name*, or None."""
        return self._entries.get(_key(source, metric_name))

    def remove(self, source: str, metric_name: str) -> bool:
        """Delete a baseline entry. Returns True if it existed."""
        key = _key(source, metric_name)
        if key in self._entries:
            del self._entries[key]
            return True
        return False

    def all_entries(self) -> list[BaselineEntry]:
        return list(self._entries.values())

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: str) -> None:
        data = [
            {
                "source": e.source,
                "metric_name": e.metric_name,
                "expected_value": e.expected_value,
                "tolerance_pct": e.tolerance_pct,
            }
            for e in self._entries.values()
        ]
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)

    @classmethod
    def load(cls, path: str) -> "BaselineStore":
        store = cls()
        if not os.path.exists(path):
            return store
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        for item in data:
            store.set(
                item["source"],
                item["metric_name"],
                item["expected_value"],
                item.get("tolerance_pct", 10.0),
            )
        return store


def _key(source: str, metric_name: str) -> str:
    return f"{source}::{metric_name}"
