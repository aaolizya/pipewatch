"""Tests for pipewatch.metrics module."""

from datetime import datetime

import pytest

from pipewatch.metrics import AlertResult, Metric, evaluate_metric


@pytest.fixture
def sample_metric():
    return Metric(
        source_name="orders_db",
        value=42.0,
        timestamp=datetime(2024, 1, 15, 12, 0, 0),
        unit="rows",
    )


def test_metric_repr(sample_metric):
    result = repr(sample_metric)
    assert "orders_db" in result
    assert "42.0" in result
    assert "rows" in result


def test_metric_repr_no_unit():
    m = Metric(source_name="pipeline_a", value=5.0)
    assert "pipeline_a" in repr(m)
    assert "unit" not in repr(m)


def test_evaluate_metric_ok(sample_metric):
    result = evaluate_metric(sample_metric, threshold_warning=50.0, threshold_critical=90.0)
    assert result.level == "ok"
    assert not result.is_alert


def test_evaluate_metric_warning(sample_metric):
    result = evaluate_metric(sample_metric, threshold_warning=40.0, threshold_critical=90.0)
    assert result.level == "warning"
    assert result.is_alert


def test_evaluate_metric_critical(sample_metric):
    result = evaluate_metric(sample_metric, threshold_warning=40.0, threshold_critical=42.0)
    assert result.level == "critical"
    assert result.is_alert


def test_evaluate_metric_critical_without_warning(sample_metric):
    result = evaluate_metric(sample_metric, threshold_critical=10.0)
    assert result.level == "critical"


def test_evaluate_metric_no_thresholds(sample_metric):
    result = evaluate_metric(sample_metric)
    assert result.level == "ok"
    assert not result.is_alert


def test_evaluate_metric_exact_warning_boundary(sample_metric):
    """Value equal to warning threshold should trigger warning."""
    result = evaluate_metric(sample_metric, threshold_warning=42.0)
    assert result.level == "warning"


def test_evaluate_metric_exact_critical_boundary(sample_metric):
    """Value equal to critical threshold should trigger critical."""
    result = evaluate_metric(sample_metric, threshold_warning=30.0, threshold_critical=42.0)
    assert result.level == "critical"
    assert result.is_alert


def test_alert_result_repr(sample_metric):
    result = evaluate_metric(sample_metric, threshold_warning=40.0)
    text = repr(result)
    assert "orders_db" in text
    assert "warning" in text


def test_alert_result_stores_thresholds(sample_metric):
    result = evaluate_metric(sample_metric, threshold_warning=30.0, threshold_critical=80.0)
    assert result.threshold_warning == 30.0
    assert result.threshold_critical == 80.0
    assert result.metric is sample_metric
