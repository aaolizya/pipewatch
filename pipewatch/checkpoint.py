"""Checkpoint module for persisting and resuming pipeline run state."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional


@dataclass
class CheckpointEntry:
    source_name: str
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    consecutive_failures: int = 0

    def record_success(self, ts: Optional[datetime] = None) -> None:
        self.last_success = ts or datetime.now(timezone.utc)
        self.consecutive_failures = 0

    def record_failure(self, ts: Optional[datetime] = None) -> None:
        self.last_failure = ts or datetime.now(timezone.utc)
        self.consecutive_failures += 1

    def to_dict(self) -> dict:
        return {
            "source_name": self.source_name,
            "last_success": self.last_success.isoformat() if self.last_success else None,
            "last_failure": self.last_failure.isoformat() if self.last_failure else None,
            "consecutive_failures": self.consecutive_failures,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CheckpointEntry":
        return cls(
            source_name=data["source_name"],
            last_success=datetime.fromisoformat(data["last_success"]) if data.get("last_success") else None,
            last_failure=datetime.fromisoformat(data["last_failure"]) if data.get("last_failure") else None,
            consecutive_failures=data.get("consecutive_failures", 0),
        )


@dataclass
class Checkpoint:
    entries: Dict[str, CheckpointEntry] = field(default_factory=dict)

    def get(self, source_name: str) -> CheckpointEntry:
        if source_name not in self.entries:
            self.entries[source_name] = CheckpointEntry(source_name=source_name)
        return self.entries[source_name]

    def to_dict(self) -> dict:
        return {name: entry.to_dict() for name, entry in self.entries.items()}

    @classmethod
    def from_dict(cls, data: dict) -> "Checkpoint":
        entries = {name: CheckpointEntry.from_dict(v) for name, v in data.items()}
        return cls(entries=entries)


def save_checkpoint(checkpoint: Checkpoint, path: Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(checkpoint.to_dict(), indent=2))
    os.replace(tmp, path)


def load_checkpoint(path: Path) -> Checkpoint:
    path = Path(path)
    if not path.exists():
        return Checkpoint()
    data = json.loads(path.read_text())
    return Checkpoint.from_dict(data)
