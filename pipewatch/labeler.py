"""Metric labeler: attach free-form key/value labels to metrics for richer
filtering and reporting downstream."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, Optional


@dataclass
class LabelSet:
    """An immutable-ish collection of string key/value labels."""

    _labels: Dict[str, str] = field(default_factory=dict, repr=False)

    def set(self, key: str, value: str) -> None:
        if not key:
            raise ValueError("Label key must not be empty")
        self._labels[key] = value

    def get(self, key: str) -> Optional[str]:
        return self._labels.get(key)

    def matches(self, key: str, value: str) -> bool:
        """Return True when the label exists and equals *value*."""
        return self._labels.get(key) == value

    def as_dict(self) -> Dict[str, str]:
        return dict(self._labels)

    def __len__(self) -> int:
        return len(self._labels)

    def __repr__(self) -> str:  # pragma: no cover
        return f"LabelSet({self._labels!r})"


class MetricLabeler:
    """Attaches and queries labels for (source, metric_name) pairs."""

    def __init__(self) -> None:
        self._store: Dict[tuple, LabelSet] = {}

    def _key(self, source: str, metric_name: str) -> tuple:
        return (source, metric_name)

    def label(self, source: str, metric_name: str, key: str, value: str) -> None:
        """Add or update a single label for the given source/metric pair."""
        k = self._key(source, metric_name)
        if k not in self._store:
            self._store[k] = LabelSet()
        self._store[k].set(key, value)

    def labels_for(self, source: str, metric_name: str) -> LabelSet:
        """Return the LabelSet for a source/metric pair (empty if unknown)."""
        return self._store.get(self._key(source, metric_name), LabelSet())

    def find_by_label(
        self, key: str, value: str
    ) -> Iterable[tuple]:
        """Yield (source, metric_name) pairs whose labels match key=value."""
        for (src, name), ls in self._store.items():
            if ls.matches(key, value):
                yield (src, name)

    def remove(self, source: str, metric_name: str) -> None:
        """Drop all labels for a source/metric pair (no-op if absent)."""
        self._store.pop(self._key(source, metric_name), None)

    def all_pairs(self) -> Iterable[tuple]:
        """Yield all registered (source, metric_name) pairs."""
        return list(self._store.keys())
