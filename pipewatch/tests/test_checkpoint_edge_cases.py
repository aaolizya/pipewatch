"""Edge-case tests for checkpoint serialization and concurrent-write safety."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.checkpoint import (
    Checkpoint,
    CheckpointEntry,
    load_checkpoint,
    save_checkpoint,
)


def test_save_is_atomic_tmp_removed(tmp_path):
    """After save, no .tmp file should remain."""
    p = tmp_path / "cp.json"
    cp = Checkpoint()
    cp.get("svc").record_success()
    save_checkpoint(cp, p)
    tmp = p.with_suffix(".tmp")
    assert not tmp.exists()


def test_saved_file_is_valid_json(tmp_path):
    p = tmp_path / "cp.json"
    cp = Checkpoint()
    cp.get("x").record_failure()
    save_checkpoint(cp, p)
    data = json.loads(p.read_text())
    assert isinstance(data, dict)


def test_load_empty_json_returns_empty_checkpoint(tmp_path):
    p = tmp_path / "cp.json"
    p.write_text("{}")
    cp = load_checkpoint(p)
    assert cp.entries == {}


def test_entry_from_dict_missing_optional_fields():
    data = {"source_name": "db"}  # no timestamps or failure count
    entry = CheckpointEntry.from_dict(data)
    assert entry.consecutive_failures == 0
    assert entry.last_success is None


def test_checkpoint_from_dict_multiple_sources():
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat()
    data = {
        "a": {"source_name": "a", "last_success": ts, "last_failure": None, "consecutive_failures": 0},
        "b": {"source_name": "b", "last_success": None, "last_failure": ts, "consecutive_failures": 4},
    }
    cp = Checkpoint.from_dict(data)
    assert len(cp.entries) == 2
    assert cp.get("b").consecutive_failures == 4


def test_overwrite_existing_checkpoint(tmp_path):
    p = tmp_path / "cp.json"
    cp1 = Checkpoint()
    cp1.get("db").record_failure()
    save_checkpoint(cp1, p)

    cp2 = Checkpoint()
    cp2.get("db").record_success()
    save_checkpoint(cp2, p)

    loaded = load_checkpoint(p)
    assert loaded.get("db").consecutive_failures == 0


def test_nested_directory_created_automatically(tmp_path):
    p = tmp_path / "a" / "b" / "c" / "cp.json"
    cp = Checkpoint()
    cp.get("svc").record_success()
    save_checkpoint(cp, p)
    assert p.exists()
