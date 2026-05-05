"""Tests for pipewatch.aggregator."""

from __future__ import annotations

import pytest

from pipewatch.aggregator import AggregationResult, aggregate, aggregate_from_history
from pipewatch.history import HistoryEntry, MetricHistory


def _entry(value: float, source: str = "db", metric: str = "latency") -> HistoryEntry:
    return HistoryEntry(source=source, metric_name=metric, value=value, timestamp=0.0)


# ---------------------------------------------------------------------------
# aggregate()
# ---------------------------------------------------------------------------

def test_aggregate_returns_none_for_empty():
    assert aggregate([]) is None


def test_aggregate_single_entry():
    result = aggregate([_entry(42.0)])
    assert isinstance(result, AggregationResult)
    assert result.count == 1
    assert result.minimum == 42.0
    assert result.maximum == 42.0
    assert result.average == 42.0
    assert result.median == 42.0
    assert result.stddev is None  # not enough data points


def test_aggregate_multiple_entries():
    entries = [_entry(v) for v in [10.0, 20.0, 30.0]]
    result = aggregate(entries)
    assert result.count == 3
    assert result.minimum == 10.0
    assert result.maximum == 30.0
    assert pytest.approx(result.average, rel=1e-6) == 20.0
    assert result.median == 20.0
    assert result.stddev is not None


def test_aggregate_stddev_two_values():
    entries = [_entry(0.0), _entry(10.0)]
    result = aggregate(entries)
    assert result.stddev is not None
    assert result.stddev > 0


def test_aggregate_preserves_source_and_metric():
    entries = [_entry(1.0, source="api", metric="error_rate")]
    result = aggregate(entries)
    assert result.source == "api"
    assert result.metric_name == "error_rate"


def test_aggregate_values_list_matches():
    vals = [5.0, 15.0, 25.0]
    result = aggregate([_entry(v) for v in vals])
    assert result.values == vals


# ---------------------------------------------------------------------------
# AggregationResult.__str__()
# ---------------------------------------------------------------------------

def test_aggregation_result_str_contains_source():
    result = aggregate([_entry(1.0)])
    assert "db" in str(result)
    assert "latency" in str(result)


def test_aggregation_result_str_no_stddev():
    result = aggregate([_entry(1.0)])
    assert "n/a" in str(result)


# ---------------------------------------------------------------------------
# aggregate_from_history()
# ---------------------------------------------------------------------------

def _make_history(*values) -> MetricHistory:
    h = MetricHistory()
    for v in values:
        entry = _entry(v)
        h.record(entry)
    return h


def test_aggregate_from_history_basic():
    h = _make_history(1.0, 2.0, 3.0)
    result = aggregate_from_history(h, "db", "latency")
    assert result is not None
    assert result.count == 3


def test_aggregate_from_history_limit():
    h = _make_history(1.0, 2.0, 3.0, 4.0, 5.0)
    result = aggregate_from_history(h, "db", "latency", limit=3)
    assert result.count == 3
    assert result.values == [3.0, 4.0, 5.0]


def test_aggregate_from_history_unknown_metric_returns_none():
    h = MetricHistory()
    result = aggregate_from_history(h, "db", "nonexistent")
    assert result is None
