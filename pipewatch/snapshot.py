"""Snapshot: capture and persist a point-in-time view of pipeline state."""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from pipewatch.summary import PipelineSummary


@dataclass
class Snapshot:
    """Immutable point-in-time record of overall pipeline health."""

    captured_at: str
    overall_status: str
    source_count: int
    healthy_count: int
    warning_count: int
    critical_count: int
    sources: list[dict[str, Any]] = field(default_factory=list)

    @staticmethod
    def from_summary(summary: PipelineSummary) -> "Snapshot":
        """Build a Snapshot from a live PipelineSummary."""
        sources = []
        for src in summary.sources:
            sources.append(
                {
                    "name": src.name,
                    "status": src.status,
                    "metric_count": len(src.results),
                }
            )

        statuses = [s.status for s in summary.sources]
        return Snapshot(
            captured_at=datetime.now(timezone.utc).isoformat(),
            overall_status=summary.overall_status,
            source_count=len(summary.sources),
            healthy_count=statuses.count("ok"),
            warning_count=statuses.count("warning"),
            critical_count=statuses.count("critical"),
            sources=sources,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def save_snapshot(snapshot: Snapshot, path: str) -> None:
    """Persist a snapshot to a JSON file, creating parent dirs as needed."""
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(snapshot.to_dict(), fh, indent=2)


def load_snapshot(path: str) -> Snapshot:
    """Load a previously saved snapshot from disk.

    Raises:
        FileNotFoundError: If the file at *path* does not exist.
        ValueError: If the file contents cannot be parsed as a valid Snapshot.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Snapshot file not found: {path}")
    with open(path, "r", encoding="utf-8") as fh:
        try:
            data = json.load(fh)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in snapshot file '{path}': {exc}") from exc
    try:
        return Snapshot(**data)
    except TypeError as exc:
        raise ValueError(f"Snapshot data in '{path}' has unexpected fields: {exc}") from exc
