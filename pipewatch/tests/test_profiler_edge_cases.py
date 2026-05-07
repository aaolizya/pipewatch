"""Edge-case tests for pipewatch.profiler."""
import math
import pytest
from pipewatch.profiler import MetricProfiler, _percentile


def test_percentile_100_returns_max():
    values = [1.0, 5.0, 10.0]
    assert _percentile(values, 100) == 10.0


def test_percentile_0_returns_min():
    values = [3.0, 7.0, 15.0]
    assert _percentile(values, 0) == 3.0


def test_record_negative_values():
    p = MetricProfiler()
    p.record("s", "m", -10.0)
    p.record("s", "m", -5.0)
    stats = p.profile("s", "m")
    assert stats.minimum == -10.0
    assert stats.maximum == -5.0
    assert abs(stats.mean - (-7.5)) < 1e-9


def test_record_zero_value():
    p = MetricProfiler()
    p.record("s", "m", 0.0)
    stats = p.profile("s", "m")
    assert stats.minimum == 0.0
    assert stats.mean == 0.0


def test_record_large_float():
    p = MetricProfiler()
    big = 1e15
    p.record("s", "m", big)
    stats = p.profile("s", "m")
    assert stats.maximum == big


def test_all_profiles_empty_profiler():
    p = MetricProfiler()
    assert p.all_profiles() == []


def test_profile_str_contains_numeric_fields():
    p = MetricProfiler()
    p.record("src", "val", 3.14159)
    s = str(p.profile("src", "val"))
    assert "n=1" in s


def test_window_one_keeps_only_latest():
    p = MetricProfiler(window=1)
    p.record("s", "m", 99.0)
    p.record("s", "m", 1.0)
    stats = p.profile("s", "m")
    assert stats.count == 1
    assert stats.maximum == 1.0


def test_same_name_different_sources_no_bleed():
    p = MetricProfiler()
    p.record("sourceA", "cpu", 10.0)
    p.record("sourceB", "cpu", 90.0)
    assert p.profile("sourceA", "cpu").mean == 10.0
    assert p.profile("sourceB", "cpu").mean == 90.0


def test_p50_two_values_is_midpoint():
    p = MetricProfiler()
    p.record("s", "m", 0.0)
    p.record("s", "m", 10.0)
    stats = p.profile("s", "m")
    assert abs(stats.p50 - 5.0) < 1e-9
