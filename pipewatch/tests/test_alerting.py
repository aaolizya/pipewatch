"""Tests for pipewatch.alerting."""

from __future__ import annotations

import pytest

from pipewatch.alerting import build_source_map, evaluate_all, format_report
from pipewatch.config import SourceConfig
from pipewatch.metrics import Metric


@pytest.fixture()
def source_ok() -> SourceConfig:
    return SourceConfig(
        name="lag",
        url="http://example.com",
        warning_threshold=100.0,
        critical_threshold=500.0,
    )


@pytest.fixture()
def metric_ok() -> Metric:
    return Metric(name="lag", value=10.0, unit="messages", timestamp=0.0)


@pytest.fixture()
def metric_warning() -> Metric:
    return Metric(name="lag", value=200.0, unit="messages", timestamp=0.0)


@pytest.fixture()
def metric_critical() -> Metric:
    return Metric(name="lag", value=600.0, unit="messages", timestamp=0.0)


def test_build_source_map(source_ok):
    mapping = build_source_map([source_ok])
    assert "lag" in mapping
    assert mapping["lag"] is source_ok


def test_evaluate_all_ok(source_ok, metric_ok):
    results = evaluate_all([metric_ok], [source_ok])
    assert len(results) == 1
    _, alert = results[0]
    assert alert.level == "ok"


def test_evaluate_all_warning(source_ok, metric_warning):
    results = evaluate_all([metric_warning], [source_ok])
    _, alert = results[0]
    assert alert.level == "warning"


def test_evaluate_all_critical(source_ok, metric_critical):
    results = evaluate_all([metric_critical], [source_ok])
    _, alert = results[0]
    assert alert.level == "critical"


def test_evaluate_all_unknown_metric_skipped(source_ok):
    orphan = Metric(name="unknown", value=1.0, unit=None, timestamp=0.0)
    results = evaluate_all([orphan], [source_ok])
    assert results == []


def test_format_report_ok(source_ok, metric_ok):
    results = evaluate_all([metric_ok], [source_ok])
    report = format_report(results)
    assert "lag" in report
    assert "OK" in report
    assert "10.0" in report


def test_format_report_no_metrics():
    report = format_report([])
    assert report == "No metrics collected."


def test_format_report_includes_unit(source_ok, metric_ok):
    results = evaluate_all([metric_ok], [source_ok])
    report = format_report(results)
    assert "messages" in report


def test_format_report_no_unit(source_ok):
    metric = Metric(name="lag", value=5.0, unit=None, timestamp=0.0)
    results = evaluate_all([metric], [source_ok])
    report = format_report(results)
    assert "lag" in report
