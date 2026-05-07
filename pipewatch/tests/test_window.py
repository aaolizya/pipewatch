"""Tests for pipewatch.window sliding-window aggregation."""
from datetime import datetime, timedelta

import pytest

from pipewatch.history import HistoryEntry
from pipewatch.window import WindowStats, compute_window, _entries_in_window


SRC = "db"
METRIC = "query_time"
_NOW = datetime(2024, 6, 1, 12, 0, 0)


def _entry(value: float, seconds_ago: float) -> HistoryEntry:
    return HistoryEntry(
        source=SRC,
        metric_name=METRIC,
        value=value,
        timestamp=_NOW - timedelta(seconds=seconds_ago),
    )


# ---------------------------------------------------------------------------
# _entries_in_window
# ---------------------------------------------------------------------------

def test_entries_in_window_includes_recent():
    entries = [_entry(1.0, 30), _entry(2.0, 90)]
    result = _entries_in_window(entries, 60, now=_NOW)
    assert len(result) == 1
    assert result[0].value == 1.0


def test_entries_in_window_includes_boundary():
    entries = [_entry(5.0, 60)]
    result = _entries_in_window(entries, 60, now=_NOW)
    assert len(result) == 1


def test_entries_in_window_empty_input():
    assert _entries_in_window([], 60, now=_NOW) == []


def test_entries_in_window_all_old():
    entries = [_entry(1.0, 200), _entry(2.0, 300)]
    assert _entries_in_window(entries, 60, now=_NOW) == []


# ---------------------------------------------------------------------------
# compute_window
# ---------------------------------------------------------------------------

def test_compute_window_no_entries_returns_nones():
    stats = compute_window(SRC, METRIC, [], 60, now=_NOW)
    assert stats.count == 0
    assert stats.mean is None
    assert stats.min_val is None
    assert stats.max_val is None


def test_compute_window_single_entry():
    stats = compute_window(SRC, METRIC, [_entry(7.5, 10)], 60, now=_NOW)
    assert stats.count == 1
    assert stats.mean == pytest.approx(7.5)
    assert stats.min_val == pytest.approx(7.5)
    assert stats.max_val == pytest.approx(7.5)


def test_compute_window_multiple_entries_mean():
    entries = [_entry(10.0, 5), _entry(20.0, 15), _entry(30.0, 25)]
    stats = compute_window(SRC, METRIC, entries, 60, now=_NOW)
    assert stats.count == 3
    assert stats.mean == pytest.approx(20.0)


def test_compute_window_filters_old_entries():
    entries = [_entry(100.0, 5), _entry(999.0, 120)]
    stats = compute_window(SRC, METRIC, entries, 60, now=_NOW)
    assert stats.count == 1
    assert stats.mean == pytest.approx(100.0)


def test_compute_window_min_max():
    entries = [_entry(3.0, 10), _entry(9.0, 20), _entry(6.0, 30)]
    stats = compute_window(SRC, METRIC, entries, 60, now=_NOW)
    assert stats.min_val == pytest.approx(3.0)
    assert stats.max_val == pytest.approx(9.0)


def test_compute_window_invalid_window_raises():
    with pytest.raises(ValueError, match="window_seconds must be positive"):
        compute_window(SRC, METRIC, [], 0, now=_NOW)


def test_compute_window_negative_raises():
    with pytest.raises(ValueError):
        compute_window(SRC, METRIC, [], -10, now=_NOW)


def test_window_stats_str_with_data():
    stats = compute_window(SRC, METRIC, [_entry(4.0, 5)], 60, now=_NOW)
    text = str(stats)
    assert SRC in text
    assert METRIC in text
    assert "mean=" in text


def test_window_stats_str_no_data():
    stats = compute_window(SRC, METRIC, [], 60, now=_NOW)
    assert "no data" in str(stats)


def test_window_stats_oldest_newest():
    e1 = _entry(1.0, 50)
    e2 = _entry(2.0, 10)
    stats = compute_window(SRC, METRIC, [e1, e2], 60, now=_NOW)
    assert stats.oldest == e1.timestamp
    assert stats.newest == e2.timestamp
