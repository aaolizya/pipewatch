"""Unit tests for pipewatch.profiler."""
import pytest
from pipewatch.profiler import MetricProfiler, ProfileStats, _percentile


# ---------------------------------------------------------------------------
# _percentile helpers
# ---------------------------------------------------------------------------

def test_percentile_single_value():
    assert _percentile([5.0], 50) == 5.0


def test_percentile_p50_even_list():
    result = _percentile([1.0, 2.0, 3.0, 4.0], 50)
    assert 2.0 <= result <= 3.0


def test_percentile_p95_ten_values():
    values = sorted(float(i) for i in range(1, 11))
    result = _percentile(values, 95)
    assert result >= 9.0


def test_percentile_empty_returns_zero():
    assert _percentile([], 50) == 0.0


# ---------------------------------------------------------------------------
# MetricProfiler construction
# ---------------------------------------------------------------------------

def test_invalid_window_raises():
    with pytest.raises(ValueError):
        MetricProfiler(window=0)


def test_default_window_accepted():
    p = MetricProfiler()
    assert p._window == 100


# ---------------------------------------------------------------------------
# record / profile
# ---------------------------------------------------------------------------

def test_profile_none_before_any_records():
    p = MetricProfiler()
    assert p.profile("src", "latency") is None


def test_profile_returns_stats_after_record():
    p = MetricProfiler()
    p.record("src", "latency", 42.0)
    stats = p.profile("src", "latency")
    assert isinstance(stats, ProfileStats)
    assert stats.count == 1
    assert stats.minimum == 42.0
    assert stats.maximum == 42.0
    assert stats.mean == 42.0


def test_profile_multiple_values():
    p = MetricProfiler()
    for v in [10.0, 20.0, 30.0]:
        p.record("src", "rate", v)
    stats = p.profile("src", "rate")
    assert stats.count == 3
    assert stats.minimum == 10.0
    assert stats.maximum == 30.0
    assert abs(stats.mean - 20.0) < 1e-9


def test_window_evicts_oldest():
    p = MetricProfiler(window=3)
    for v in [1.0, 2.0, 3.0, 4.0]:  # 4th evicts 1st
        p.record("s", "m", v)
    stats = p.profile("s", "m")
    assert stats.count == 3
    assert stats.minimum == 2.0


def test_different_sources_independent():
    p = MetricProfiler()
    p.record("a", "x", 1.0)
    p.record("b", "x", 99.0)
    assert p.profile("a", "x").maximum == 1.0
    assert p.profile("b", "x").minimum == 99.0


def test_profile_str_contains_source_and_name():
    p = MetricProfiler()
    p.record("mydb", "query_time", 5.5)
    s = str(p.profile("mydb", "query_time"))
    assert "mydb" in s
    assert "query_time" in s


def test_all_profiles_returns_all_keys():
    p = MetricProfiler()
    p.record("s1", "m1", 1.0)
    p.record("s1", "m2", 2.0)
    p.record("s2", "m1", 3.0)
    profiles = p.all_profiles()
    assert len(profiles) == 3


def test_clear_removes_entry():
    p = MetricProfiler()
    p.record("s", "m", 7.0)
    p.clear("s", "m")
    assert p.profile("s", "m") is None


def test_clear_nonexistent_is_noop():
    p = MetricProfiler()
    p.clear("ghost", "metric")  # should not raise
