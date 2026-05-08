"""Eviction policy for bounded in-memory caches used across pipewatch modules."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Generic, Hashable, List, Optional, Tuple, TypeVar

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


@dataclass
class EvictionPolicy:
    """Configuration for a bounded LRU cache."""

    max_size: int = 256
    evict_ratio: float = 0.25  # fraction of entries removed on overflow

    def __post_init__(self) -> None:
        if self.max_size < 1:
            raise ValueError("max_size must be >= 1")
        if not (0.0 < self.evict_ratio <= 1.0):
            raise ValueError("evict_ratio must be in (0, 1]")

    @property
    def evict_count(self) -> int:
        """Number of entries to drop when the cache is full."""
        return max(1, int(self.max_size * self.evict_ratio))


class LRUCache(Generic[K, V]):
    """Simple LRU cache backed by an insertion-ordered dict."""

    def __init__(self, policy: Optional[EvictionPolicy] = None) -> None:
        self._policy = policy or EvictionPolicy()
        self._store: Dict[K, V] = {}

    # ------------------------------------------------------------------
    def __len__(self) -> int:
        return len(self._store)

    def __contains__(self, key: object) -> bool:
        return key in self._store

    # ------------------------------------------------------------------
    def get(self, key: K) -> Optional[V]:
        """Return value for *key*, promoting it to most-recently-used."""
        if key not in self._store:
            return None
        value = self._store.pop(key)
        self._store[key] = value
        return value

    def put(self, key: K, value: V) -> None:
        """Insert or update *key* and evict oldest entries if over capacity."""
        if key in self._store:
            self._store.pop(key)
        self._store[key] = value
        if len(self._store) > self._policy.max_size:
            self._evict()

    def evicted_keys(self) -> List[K]:
        """Return keys that would be evicted next (oldest first)."""
        n = self._policy.evict_count
        return list(self._store.keys())[:n]

    def clear(self) -> None:
        self._store.clear()

    # ------------------------------------------------------------------
    def _evict(self) -> None:
        for key in self.evicted_keys():
            del self._store[key]

    def items(self) -> List[Tuple[K, V]]:
        return list(self._store.items())
