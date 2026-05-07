"""Integration tests: window module working with real MetricHistory."""
from datetime import datetime, timedelta
from pathlib import Path
import tempfile

import pytest

from pipewatch.history import MetricHistory
from pipewatch.metrics import Metric
from pipewatch.window import compute_window


_NOW = datetime(2024, 6, 1, 12, 0, 0)


def _metric(name: str, value: float, seconds_ago: float) -> Metric:
    return Metric(
        source="svc",
        name=name,
        value=value,
        timestamp=_NOW - timedelta(seconds=seconds_ago),
        unit="ms",
    )


@pytest.fixture()
def history(tmp_path):
    return MetricHistory(path=tmp_path / "history.json")


def test_window_uses_recorded_history(history):
    for v, ago in [(1.0, 5), (3.0, 20), (5.0, 50), (99.0, 200)]:
        history.record(_metric("latency", v, ago))

    entries = history.entries_for("svc", "latency")
    stats = compute_window("svc", "latency", entries, 60, now=_NOW)

    assert stats.count == 3
    assert stats.mean == pytest.approx((1.0 + 3.0 + 5.0) / 3)


def test_window_empty_history_no_crash(history):
    entries = history.entries_for("svc", "missing_metric")
    stats = compute_window("svc", "missing_metric", entries, 60, now=_NOW)
    assert stats.count == 0
    assert stats.mean is None


def test_window_per_metric_isolation(history):
    history.record(_metric("cpu", 80.0, 10))
    history.record(_metric("mem", 50.0, 10))
    history.record(_metric("cpu", 90.0, 20))

    cpu_entries = history.entries_for("svc", "cpu")
    mem_entries = history.entries_for("svc", "mem")

    cpu_stats = compute_window("svc", "cpu", cpu_entries, 60, now=_NOW)
    mem_stats = compute_window("svc", "mem", mem_entries, 60, now=_NOW)

    assert cpu_stats.count == 2
    assert mem_stats.count == 1
    assert mem_stats.mean == pytest.approx(50.0)


def test_window_narrow_vs_wide(history):
    for v, ago in [(1.0, 10), (2.0, 70), (3.0, 130)]:
        history.record(_metric("rps", v, ago))

    entries = history.entries_for("svc", "rps")
    narrow = compute_window("svc", "rps", entries, 60, now=_NOW)
    wide = compute_window("svc", "rps", entries, 180, now=_NOW)

    assert narrow.count == 1
    assert wide.count == 3
