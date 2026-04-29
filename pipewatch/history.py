"""Metric history tracking for trend detection and persistence."""

from __future__ import annotations

import json
import os
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Dict, List, Optional

from pipewatch.metrics import Metric

DEFAULT_HISTORY_DIR = Path.home() / ".pipewatch" / "history"
DEFAULT_MAX_ENTRIES = 100


@dataclass
class HistoryEntry:
    source_name: str
    metric_name: str
    value: float
    unit: Optional[str]
    timestamp: str

    @classmethod
    def from_metric(cls, metric: Metric) -> "HistoryEntry":
        return cls(
            source_name=metric.source_name,
            metric_name=metric.name,
            value=metric.value,
            unit=metric.unit,
            timestamp=metric.timestamp or datetime.now(timezone.utc).isoformat(),
        )


class MetricHistory:
    """Stores and retrieves recent metric values per source/metric key."""

    def __init__(
        self,
        history_dir: Path = DEFAULT_HISTORY_DIR,
        max_entries: int = DEFAULT_MAX_ENTRIES,
    ) -> None:
        self.history_dir = history_dir
        self.max_entries = max_entries
        self._cache: Dict[str, Deque[HistoryEntry]] = {}

    def _key(self, source_name: str, metric_name: str) -> str:
        return f"{source_name}__{metric_name}"

    def _file_path(self, key: str) -> Path:
        safe = key.replace("/", "_").replace(" ", "_")
        return self.history_dir / f"{safe}.json"

    def record(self, metric: Metric) -> None:
        """Add a metric reading to history."""
        key = self._key(metric.source_name, metric.name)
        if key not in self._cache:
            self._cache[key] = deque(self._load(key), maxlen=self.max_entries)
        self._cache[key].append(HistoryEntry.from_metric(metric))
        self._persist(key)

    def get(self, source_name: str, metric_name: str) -> List[HistoryEntry]:
        """Return recorded history for a given source/metric pair."""
        key = self._key(source_name, metric_name)
        if key not in self._cache:
            self._cache[key] = deque(self._load(key), maxlen=self.max_entries)
        return list(self._cache[key])

    def _load(self, key: str) -> List[HistoryEntry]:
        path = self._file_path(key)
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text())
            return [HistoryEntry(**entry) for entry in data]
        except (json.JSONDecodeError, TypeError, KeyError):
            return []

    def _persist(self, key: str) -> None:
        self.history_dir.mkdir(parents=True, exist_ok=True)
        path = self._file_path(key)
        entries = [asdict(e) for e in self._cache[key]]
        path.write_text(json.dumps(entries, indent=2))
