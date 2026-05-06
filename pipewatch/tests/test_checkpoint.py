"""Unit tests for pipewatch.checkpoint."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipewatch.checkpoint import (
    Checkpoint,
    CheckpointEntry,
    load_checkpoint,
    save_checkpoint,
)


# ---------------------------------------------------------------------------
# CheckpointEntry
# ---------------------------------------------------------------------------

def test_entry_defaults():
    entry = CheckpointEntry(source_name="db")
    assert entry.last_success is None
    assert entry.last_failure is None
    assert entry.consecutive_failures == 0


def test_record_success_clears_failures():
    entry = CheckpointEntry(source_name="db", consecutive_failures=3)
    entry.record_success()
    assert entry.consecutive_failures == 0
    assert entry.last_success is not None


def test_record_failure_increments():
    entry = CheckpointEntry(source_name="db")
    entry.record_failure()
    entry.record_failure()
    assert entry.consecutive_failures == 2
    assert entry.last_failure is not None


def test_record_success_uses_provided_timestamp():
    ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    entry = CheckpointEntry(source_name="db")
    entry.record_success(ts)
    assert entry.last_success == ts


def test_entry_round_trip():
    ts = datetime(2024, 6, 1, 8, 30, 0, tzinfo=timezone.utc)
    entry = CheckpointEntry(source_name="svc", last_success=ts, consecutive_failures=2)
    restored = CheckpointEntry.from_dict(entry.to_dict())
    assert restored.source_name == "svc"
    assert restored.last_success == ts
    assert restored.consecutive_failures == 2


def test_entry_round_trip_none_timestamps():
    entry = CheckpointEntry(source_name="svc")
    restored = CheckpointEntry.from_dict(entry.to_dict())
    assert restored.last_success is None
    assert restored.last_failure is None


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

def test_get_creates_missing_entry():
    cp = Checkpoint()
    entry = cp.get("new_source")
    assert isinstance(entry, CheckpointEntry)
    assert entry.source_name == "new_source"


def test_get_returns_same_entry():
    cp = Checkpoint()
    e1 = cp.get("src")
    e2 = cp.get("src")
    assert e1 is e2


def test_checkpoint_to_dict_includes_all_sources():
    cp = Checkpoint()
    cp.get("a").record_success()
    cp.get("b").record_failure()
    d = cp.to_dict()
    assert "a" in d and "b" in d


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def test_save_creates_file(tmp_path):
    cp = Checkpoint()
    cp.get("db").record_success()
    p = tmp_path / "checkpoints" / "state.json"
    save_checkpoint(cp, p)
    assert p.exists()


def test_load_nonexistent_returns_empty(tmp_path):
    cp = load_checkpoint(tmp_path / "missing.json")
    assert isinstance(cp, Checkpoint)
    assert cp.entries == {}


def test_save_load_round_trip(tmp_path):
    ts = datetime(2024, 3, 10, 9, 0, 0, tzinfo=timezone.utc)
    cp = Checkpoint()
    cp.get("api").record_success(ts)
    cp.get("api").record_failure()
    p = tmp_path / "cp.json"
    save_checkpoint(cp, p)
    loaded = load_checkpoint(p)
    entry = loaded.get("api")
    assert entry.last_success == ts
    assert entry.consecutive_failures == 1
