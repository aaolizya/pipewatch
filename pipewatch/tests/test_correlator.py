"""Unit tests for pipewatch.correlator."""
import math
import time

import pytest

from pipewatch.correlator import CorrelationResult, _pearson, correlate
from pipewatch.history import HistoryEntry


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _entry(value: float, ts: float) -> HistoryEntry:
    return HistoryEntry(value=value, timestamp=ts, source="s", metric="m")


def _entries(values, base_ts=1_000_000.0, step=1.0):
    return [_entry(v, base_ts + i * step) for i, v in enumerate(values)]


# ---------------------------------------------------------------------------
# _pearson
# ---------------------------------------------------------------------------

def test_pearson_perfect_positive():
    r = _pearson([1, 2, 3, 4, 5], [2, 4, 6, 8, 10])
    assert r is not None
    assert math.isclose(r, 1.0, abs_tol=1e-9)


def test_pearson_perfect_negative():
    r = _pearson([1, 2, 3], [3, 2, 1])
    assert r is not None
    assert math.isclose(r, -1.0, abs_tol=1e-9)


def test_pearson_zero_variance_returns_none():
    assert _pearson([5, 5, 5], [1, 2, 3]) is None


def test_pearson_single_point_returns_none():
    assert _pearson([1], [1]) is None


def test_pearson_empty_returns_none():
    assert _pearson([], []) is None


# ---------------------------------------------------------------------------
# correlate
# ---------------------------------------------------------------------------

def test_correlate_returns_result():
    a = _entries([1, 2, 3, 4, 5])
    b = _entries([2, 4, 6, 8, 10])
    result = correlate("src", "m1", a, "src", "m2", b)
    assert isinstance(result, CorrelationResult)
    assert math.isclose(result.coefficient, 1.0, abs_tol=1e-6)


def test_correlate_sample_size():
    a = _entries([1, 2, 3])
    b = _entries([3, 2, 1])
    result = correlate("s", "a", a, "s", "b", b)
    assert result is not None
    assert result.sample_size == 3


def test_correlate_no_overlap_returns_none():
    a = _entries([1, 2, 3], base_ts=1_000_000.0)
    b = _entries([1, 2, 3], base_ts=9_000_000.0)
    result = correlate("s", "a", a, "s", "b", b, timestamp_tolerance=1.0)
    assert result is None


def test_correlate_partial_overlap():
    base = 1_000_000.0
    a = [_entry(v, base + i) for i, v in enumerate([1, 2, 3, 4])]
    # b only matches first 2 timestamps
    b = [_entry(v, base + i) for i, v in enumerate([10, 20])]
    result = correlate("s", "a", a, "s", "b", b)
    assert result is not None
    assert result.sample_size == 2


def test_correlate_stores_names():
    a = _entries([1, 2, 3])
    b = _entries([1, 2, 3])
    result = correlate("src_a", "metric_a", a, "src_b", "metric_b", b)
    assert result.source_a == "src_a"
    assert result.metric_a == "metric_a"
    assert result.source_b == "src_b"
    assert result.metric_b == "metric_b"


def test_correlation_result_str_strong_positive():
    r = CorrelationResult("s", "m", "s2", "m2", 0.85, 10)
    assert "strong" in str(r)
    assert "positive" in str(r)


def test_correlation_result_str_weak_negative():
    r = CorrelationResult("s", "m", "s2", "m2", -0.2, 5)
    assert "weak" in str(r)
    assert "negative" in str(r)


def test_correlate_tolerance_respected():
    base = 1_000_000.0
    a = [_entry(1.0, base), _entry(2.0, base + 10)]
    b = [_entry(1.0, base + 3), _entry(2.0, base + 13)]
    result_tight = correlate("s", "a", a, "s", "b", b, timestamp_tolerance=2.0)
    result_loose = correlate("s", "a", a, "s", "b", b, timestamp_tolerance=5.0)
    assert result_tight is None
    assert result_loose is not None
