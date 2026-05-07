"""Capacity planning module: tracks metric headroom relative to a defined ceiling."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class CapacityResult:
    source: str
    metric_name: str
    observed: float
    ceiling: float
    headroom: float
    headroom_pct: float
    at_risk: bool

    def __str__(self) -> str:
        status = "AT RISK" if self.at_risk else "OK"
        return (
            f"[{status}] {self.source}/{self.metric_name}: "
            f"{self.observed:.2f} / {self.ceiling:.2f} "
            f"({self.headroom_pct:.1f}% headroom)"
        )


def compute_capacity(
    source: str,
    metric_name: str,
    observed: float,
    ceiling: float,
    risk_threshold_pct: float = 20.0,
) -> CapacityResult:
    """Compute headroom between observed value and a defined ceiling.

    Args:
        source: Name of the data source.
        metric_name: Name of the metric being evaluated.
        observed: Current observed value.
        ceiling: Maximum allowable value (capacity ceiling).
        risk_threshold_pct: Headroom percentage below which the metric is at risk.

    Returns:
        A CapacityResult describing current headroom and risk status.

    Raises:
        ValueError: If ceiling is not positive.
    """
    if ceiling <= 0:
        raise ValueError(f"ceiling must be positive, got {ceiling}")
    if risk_threshold_pct < 0 or risk_threshold_pct > 100:
        raise ValueError(
            f"risk_threshold_pct must be between 0 and 100, got {risk_threshold_pct}"
        )

    headroom = ceiling - observed
    headroom_pct = max((headroom / ceiling) * 100.0, 0.0)
    at_risk = headroom_pct < risk_threshold_pct

    return CapacityResult(
        source=source,
        metric_name=metric_name,
        observed=observed,
        ceiling=ceiling,
        headroom=headroom,
        headroom_pct=headroom_pct,
        at_risk=at_risk,
    )


@dataclass
class CapacityStore:
    """Holds ceiling definitions and produces CapacityResults on demand."""

    _ceilings: Dict[str, float] = field(default_factory=dict)
    risk_threshold_pct: float = 20.0

    def set_ceiling(self, source: str, metric_name: str, ceiling: float) -> None:
        """Register or update a ceiling for a source/metric pair."""
        key = f"{source}::{metric_name}"
        self._ceilings[key] = ceiling

    def evaluate(
        self, source: str, metric_name: str, observed: float
    ) -> Optional[CapacityResult]:
        """Return a CapacityResult if a ceiling is registered, else None."""
        key = f"{source}::{metric_name}"
        ceiling = self._ceilings.get(key)
        if ceiling is None:
            return None
        return compute_capacity(
            source, metric_name, observed, ceiling, self.risk_threshold_pct
        )

    def registered_keys(self) -> list:
        return list(self._ceilings.keys())
