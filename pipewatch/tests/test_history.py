"""Tests for pipewatch.history module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipewatch.history import HistoryEntry, MetricHistory
from pipewatch.metrics import Metric


@pytest.fixture
def tmp_history(tmp_path: Path) -> MetricHistory:
    return MetricHistory(history_dir=tmp_path / "history", max_entries=10)


@pytest.fixture
def sample_metric() -> Metric:
    return Metric(
        source_name="db_primary",
        name="query_latency_ms",
        value=42.5,
        unit="ms",
        timestamp="2024-01-15T12:00:00+00:00",
    )


def test_history_entry_from_metric(sample_metric: Metric) -> None:
    entry = HistoryEntry.from_metric(sample_metric)
    assert entry.source_name == "db_primary"
    assert entry.metric_name == "query_latency_ms"
    assert entry.value == 42.5
    assert entry.unit == "ms"
    assert entry.timestamp == "2024-01-15T12:00:00+00:00"


def test_record_adds_entry(tmp_history: MetricHistory, sample_metric: Metric) -> None:
    tmp_history.record(sample_metric)
    entries = tmp_history.get("db_primary", "query_latency_ms")
    assert len(entries) == 1
    assert entries[0].value == 42.5


def test_record_multiple_entries(tmp_history: MetricHistory, sample_metric: Metric) -> None:
    for val in [10.0, 20.0, 30.0]:
        m = Metric(
            source_name="db_primary",
            name="query_latency_ms",
            value=val,
            unit="ms",
            timestamp="2024-01-15T12:00:00+00:00",
        )
        tmp_history.record(m)
    entries = tmp_history.get("db_primary", "query_latency_ms")
    assert len(entries) == 3
    assert [e.value for e in entries] == [10.0, 20.0, 30.0]


def test_record_respects_max_entries(tmp_path: Path, sample_metric: Metric) -> None:
    history = MetricHistory(history_dir=tmp_path / "h", max_entries=3)
    for i in range(5):
        m = Metric(
            source_name="db_primary",
            name="query_latency_ms",
            value=float(i),
            unit="ms",
            timestamp="2024-01-15T12:00:00+00:00",
        )
        history.record(m)
    entries = history.get("db_primary", "query_latency_ms")
    assert len(entries) == 3
    assert entries[0].value == 2.0


def test_get_returns_empty_for_unknown(tmp_history: MetricHistory) -> None:
    entries = tmp_history.get("nonexistent", "no_metric")
    assert entries == []


def test_persist_and_reload(tmp_path: Path, sample_metric: Metric) -> None:
    h1 = MetricHistory(history_dir=tmp_path / "h", max_entries=10)
    h1.record(sample_metric)

    h2 = MetricHistory(history_dir=tmp_path / "h", max_entries=10)
    entries = h2.get("db_primary", "query_latency_ms")
    assert len(entries) == 1
    assert entries[0].value == 42.5


def test_corrupted_file_returns_empty(tmp_path: Path) -> None:
    history_dir = tmp_path / "h"
    history_dir.mkdir()
    bad_file = history_dir / "db_primary__query_latency_ms.json"
    bad_file.write_text("not valid json {{")

    history = MetricHistory(history_dir=history_dir, max_entries=10)
    entries = history.get("db_primary", "query_latency_ms")
    assert entries == []


def test_record_creates_history_dir_if_missing(tmp_path: Path, sample_metric: Metric) -> None:
    """Ensure MetricHistory creates the history directory on first record if it does not exist."""
    history_dir = tmp_path / "nested" / "history"
    assert not history_dir.exists()

    history = MetricHistory(history_dir=history_dir, max_entries=10)
    history.record(sample_metric)

    assert history_dir.exists()
    entries = history.get("db_primary", "query_latency_ms")
    assert len(entries) == 1
    assert entries[0].value == 42.5
