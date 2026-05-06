"""Integration tests: checkpoint wired into a simulated collection cycle."""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.checkpoint import Checkpoint, load_checkpoint, save_checkpoint
from pipewatch.collector import CollectionError


def _make_source(name: str) -> MagicMock:
    src = MagicMock()
    src.name = name
    src.url = "http://example.com/metrics"
    return src


def _simulate_cycle(sources, checkpoint, fail_sources=()):
    """Fake one collection cycle, updating checkpoint accordingly."""
    for src in sources:
        entry = checkpoint.get(src.name)
        if src.name in fail_sources:
            entry.record_failure()
        else:
            entry.record_success()


def test_success_cycle_resets_failures(tmp_path):
    cp = Checkpoint()
    cp.get("db").consecutive_failures = 5
    sources = [_make_source("db")]
    _simulate_cycle(sources, cp)
    assert cp.get("db").consecutive_failures == 0


def test_failure_cycle_accumulates(tmp_path):
    cp = Checkpoint()
    sources = [_make_source("api")]
    _simulate_cycle(sources, cp, fail_sources=("api",))
    _simulate_cycle(sources, cp, fail_sources=("api",))
    assert cp.get("api").consecutive_failures == 2


def test_checkpoint_persists_across_restarts(tmp_path):
    p = tmp_path / "cp.json"
    sources = [_make_source("svc")]

    cp1 = Checkpoint()
    _simulate_cycle(sources, cp1, fail_sources=("svc",))
    save_checkpoint(cp1, p)

    cp2 = load_checkpoint(p)
    assert cp2.get("svc").consecutive_failures == 1


def test_mixed_sources_tracked_independently(tmp_path):
    p = tmp_path / "cp.json"
    sources = [_make_source("a"), _make_source("b")]

    cp = Checkpoint()
    _simulate_cycle(sources, cp, fail_sources=("b",))
    save_checkpoint(cp, p)

    loaded = load_checkpoint(p)
    assert loaded.get("a").consecutive_failures == 0
    assert loaded.get("b").consecutive_failures == 1


def test_recovery_after_failures(tmp_path):
    p = tmp_path / "cp.json"
    sources = [_make_source("db")]

    cp = Checkpoint()
    _simulate_cycle(sources, cp, fail_sources=("db",))
    _simulate_cycle(sources, cp, fail_sources=("db",))
    _simulate_cycle(sources, cp)  # recovery
    save_checkpoint(cp, p)

    loaded = load_checkpoint(p)
    assert loaded.get("db").consecutive_failures == 0
    assert loaded.get("db").last_success is not None
