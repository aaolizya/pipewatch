"""Edge-case tests for pipewatch.anomaly."""
from __future__ import annotations

import pytest

from pipewatch.anomaly import detect_anomaly, _z_score


def _agg(mean, stddev, count=10):
    from unittest.mock import MagicMock
    a = MagicMock()
    a.mean = mean
    a.stddev = stddev
    a.count = count
    return a


def test_zero_stddev_never_anomaly():
    """Constant history → stddev=0, z=0, never anomalous."""
    agg = _agg(mean=42.0, stddev=0.0)
    result = detect_anomaly("s", "m", 9999.0, agg, threshold=3.0)
    assert not result.is_anomaly
    assert result.z_score == 0.0


def test_negative_observed_value():
    agg = _agg(mean=0.0, stddev=1.0)
    result = detect_anomaly("s", "m", -10.0, agg, threshold=3.0)
    assert result.is_anomaly
    assert result.z_score == pytest.approx(-10.0)


def test_very_small_threshold_flags_minor_deviation():
    agg = _agg(mean=100.0, stddev=1.0)
    result = detect_anomaly("s", "m", 100.5, agg, threshold=0.1)
    assert result.is_anomaly


def test_threshold_zero_always_anomaly_when_any_deviation():
    agg = _agg(mean=100.0, stddev=1.0)
    result = detect_anomaly("s", "m", 100.001, agg, threshold=0.0)
    assert result.is_anomaly


def test_exact_mean_is_not_anomaly():
    agg = _agg(mean=55.0, stddev=2.0)
    result = detect_anomaly("s", "m", 55.0, agg, threshold=3.0)
    assert not result.is_anomaly
    assert result.z_score == 0.0


def test_count_exactly_two_is_sufficient():
    agg = _agg(mean=10.0, stddev=1.0, count=2)
    result = detect_anomaly("s", "m", 99.0, agg, threshold=3.0)
    # count=2 is sufficient; should evaluate normally
    assert result.is_anomaly
    assert result.reason != "insufficient history"


def test_reason_empty_when_not_anomaly():
    agg = _agg(mean=10.0, stddev=1.0)
    result = detect_anomaly("s", "m", 10.5, agg, threshold=3.0)
    assert result.reason == ""


def test_reason_populated_when_anomaly():
    agg = _agg(mean=10.0, stddev=1.0)
    result = detect_anomaly("s", "m", 20.0, agg, threshold=3.0)
    assert result.reason != ""
    assert "z" in result.reason.lower() or "|" in result.reason


def test_z_score_symmetry():
    assert _z_score(5.0, 3.0, 2.0) == pytest.approx(-_z_score(1.0, 3.0, 2.0))
