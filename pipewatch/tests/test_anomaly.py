"""Unit tests for pipewatch.anomaly."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from pipewatch.anomaly import AnomalyResult, detect_anomaly, detect_all, _z_score


def _agg(mean: float, stddev: float, count: int = 10):
    agg = MagicMock()
    agg.mean = mean
    agg.stddev = stddev
    agg.count = count
    return agg


# --- _z_score ---

def test_z_score_zero_stddev_returns_zero():
    assert _z_score(5.0, 3.0, 0.0) == 0.0


def test_z_score_positive():
    assert _z_score(6.0, 3.0, 1.5) == pytest.approx(2.0)


def test_z_score_negative():
    assert _z_score(0.0, 3.0, 1.5) == pytest.approx(-2.0)


# --- detect_anomaly ---

def test_detect_anomaly_ok_within_threshold():
    agg = _agg(mean=100.0, stddev=5.0)
    result = detect_anomaly("src", "latency", 102.0, agg, threshold=3.0)
    assert not result.is_anomaly
    assert result.z_score == pytest.approx(0.4)


def test_detect_anomaly_flags_outlier():
    agg = _agg(mean=100.0, stddev=5.0)
    result = detect_anomaly("src", "latency", 120.0, agg, threshold=3.0)
    assert result.is_anomaly
    assert result.z_score == pytest.approx(4.0)


def test_detect_anomaly_negative_outlier():
    agg = _agg(mean=100.0, stddev=5.0)
    result = detect_anomaly("src", "latency", 80.0, agg, threshold=3.0)
    assert result.is_anomaly
    assert result.z_score == pytest.approx(-4.0)


def test_detect_anomaly_exactly_at_threshold_is_anomaly():
    agg = _agg(mean=0.0, stddev=1.0)
    result = detect_anomaly("src", "m", 3.0, agg, threshold=3.0)
    assert result.is_anomaly


def test_detect_anomaly_none_agg_not_anomaly():
    result = detect_anomaly("src", "m", 42.0, None, threshold=3.0)
    assert not result.is_anomaly
    assert result.reason == "insufficient history"


def test_detect_anomaly_insufficient_count_not_anomaly():
    agg = _agg(mean=100.0, stddev=5.0, count=1)
    result = detect_anomaly("src", "m", 999.0, agg, threshold=3.0)
    assert not result.is_anomaly
    assert result.reason == "insufficient history"


def test_detect_anomaly_result_fields():
    agg = _agg(mean=50.0, stddev=10.0)
    result = detect_anomaly("pipeline_a", "error_rate", 80.0, agg)
    assert result.source == "pipeline_a"
    assert result.metric_name == "error_rate"
    assert result.observed == 80.0
    assert result.expected == 50.0


def test_anomaly_result_str_anomaly():
    r = AnomalyResult("s", "m", 9.0, 3.0, 4.0, True, "reason")
    assert "ANOMALY" in str(r)


def test_anomaly_result_str_ok():
    r = AnomalyResult("s", "m", 3.0, 3.0, 0.0, False)
    assert "ok" in str(r)


# --- detect_all ---

def test_detect_all_returns_one_per_metric():
    obs = {"latency": 120.0, "error_rate": 0.01}
    aggs = {
        "latency": _agg(mean=100.0, stddev=5.0),
        "error_rate": _agg(mean=0.01, stddev=0.001),
    }
    results = detect_all("src", obs, aggs)
    assert len(results) == 2


def test_detect_all_missing_agg_handled():
    obs = {"latency": 120.0}
    results = detect_all("src", obs, {})
    assert len(results) == 1
    assert not results[0].is_anomaly
