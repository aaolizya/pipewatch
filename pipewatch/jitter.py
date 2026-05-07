"""Jitter utilities for spreading poll intervals to avoid thundering herd."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class JitterPolicy:
    """Controls how random jitter is applied to a base interval."""

    max_jitter: float  # seconds
    strategy: str = "uniform"  # "uniform" | "proportional"
    proportion: float = 0.2  # used when strategy == "proportional"
    seed: Optional[int] = None
    _rng: random.Random = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.max_jitter < 0:
            raise ValueError("max_jitter must be >= 0")
        if self.strategy not in ("uniform", "proportional"):
            raise ValueError(f"Unknown jitter strategy: {self.strategy!r}")
        if not (0.0 <= self.proportion <= 1.0):
            raise ValueError("proportion must be between 0.0 and 1.0")
        self._rng = random.Random(self.seed)

    def apply(self, base_interval: float) -> float:
        """Return base_interval with jitter added.

        Args:
            base_interval: The nominal poll interval in seconds.

        Returns:
            Jittered interval (always >= 0).
        """
        if base_interval < 0:
            raise ValueError("base_interval must be >= 0")

        if self.strategy == "uniform":
            offset = self._rng.uniform(0.0, self.max_jitter)
        else:  # proportional
            cap = base_interval * self.proportion
            offset = self._rng.uniform(0.0, cap)

        return max(0.0, base_interval + offset)


def apply_jitter(base_interval: float, max_jitter: float, seed: Optional[int] = None) -> float:
    """Convenience wrapper: apply uniform jitter to *base_interval*."""
    policy = JitterPolicy(max_jitter=max_jitter, strategy="uniform", seed=seed)
    return policy.apply(base_interval)
