"""Tests for pipewatch.sampler."""
from __future__ import annotations

import pytest

from pipewatch.sampler import MetricSampler, SamplerStats
from pipewatch.metrics import Metric


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_metric(value: float, name: str = "latency", source: str = "db") -> Metric:
    return Metric(source=source, name=name, value=value, unit="ms")


# ---------------------------------------------------------------------------
# MetricSampler construction
# ---------------------------------------------------------------------------

def test_invalid_capacity_raises():
    with pytest.raises(ValueError, match="capacity"):
        MetricSampler(source="s", metric_name="m", capacity=0)


def test_default_capacity_is_100():
    s = MetricSampler(source="s", metric_name="m")
    assert s.capacity == 100


# ---------------------------------------------------------------------------
# record / stats
# ---------------------------------------------------------------------------

def test_stats_empty_returns_none():
    s = MetricSampler(source="src", metric_name="bytes")
    assert s.stats() is None


def test_stats_single_value():
    s = MetricSampler(source="src", metric_name="bytes")
    s.record(42.0)
    result = s.stats()
    assert isinstance(result, SamplerStats)
    assert result.min_value == 42.0
    assert result.max_value == 42.0
    assert result.mean_value == 42.0
    assert result.sample_size == 1
    assert result.total_seen == 1


def test_stats_multiple_values():
    s = MetricSampler(source="src", metric_name="m", capacity=50)
    for v in [1.0, 2.0, 3.0, 4.0]:
        s.record(v)
    result = s.stats()
    assert result.min_value == 1.0
    assert result.max_value == 4.0
    assert abs(result.mean_value - 2.5) < 1e-9
    assert result.total_seen == 4


def test_reservoir_bounded_by_capacity():
    capacity = 10
    s = MetricSampler(source="src", metric_name="m", capacity=capacity)
    for i in range(200):
        s.record(float(i))
    result = s.stats()
    assert result.sample_size == capacity
    assert result.total_seen == 200


def test_record_metric_uses_value():
    s = MetricSampler(source="db", metric_name="latency")
    m = _make_metric(99.5)
    s.record_metric(m)
    result = s.stats()
    assert result.min_value == 99.5


# ---------------------------------------------------------------------------
# reset
# ---------------------------------------------------------------------------

def test_reset_clears_reservoir():
    s = MetricSampler(source="src", metric_name="m")
    s.record(1.0)
    s.reset()
    assert s.stats() is None
    assert s._total_seen == 0


# ---------------------------------------------------------------------------
# SamplerStats __str__
# ---------------------------------------------------------------------------

def test_stats_str_contains_source_and_metric():
    s = MetricSampler(source="pipeline_a", metric_name="row_count", capacity=5)
    for v in [10.0, 20.0, 30.0]:
        s.record(v)
    text = str(s.stats())
    assert "pipeline_a" in text
    assert "row_count" in text
    assert "3/3" in text
