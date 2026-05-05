"""Tests for pipewatch.filter."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipewatch.filter import (
    filter_alerts_above_value,
    filter_by_source,
    filter_by_status,
    filter_metrics_by_name,
)
from pipewatch.metrics import AlertResult, Metric


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_metric(name: str = "lag", value: float = 0.0) -> Metric:
    return Metric(source="src", name=name, value=value, unit="ms", timestamp=0.0)


def _make_result(status: str, value: float = 0.0, name: str = "lag") -> AlertResult:
    metric = _make_metric(name=name, value=value)
    return AlertResult(metric=metric, status=status, message="")


def _make_summary(source_name: str) -> MagicMock:
    src = MagicMock()
    src.name = source_name
    summary = MagicMock()
    summary.source = src
    return summary


# ---------------------------------------------------------------------------
# filter_by_status
# ---------------------------------------------------------------------------

def test_filter_by_status_keeps_matching():
    results = [_make_result("ok"), _make_result("warning"), _make_result("critical")]
    out = filter_by_status(results, ["warning", "critical"])
    assert len(out) == 2
    assert all(r.status in {"warning", "critical"} for r in out)


def test_filter_by_status_case_insensitive():
    results = [_make_result("OK"), _make_result("Warning")]
    out = filter_by_status(results, ["ok"])
    assert len(out) == 1
    assert out[0].status == "OK"


def test_filter_by_status_empty_result():
    results = [_make_result("ok")]
    assert filter_by_status(results, ["critical"]) == []


# ---------------------------------------------------------------------------
# filter_by_source
# ---------------------------------------------------------------------------

def test_filter_by_source_keeps_matching():
    summaries = [_make_summary("db"), _make_summary("kafka"), _make_summary("s3")]
    out = filter_by_source(summaries, ["kafka", "s3"])
    assert len(out) == 2
    names = {s.source.name for s in out}
    assert names == {"kafka", "s3"}


def test_filter_by_source_case_insensitive():
    summaries = [_make_summary("Kafka")]
    out = filter_by_source(summaries, ["kafka"])
    assert len(out) == 1


def test_filter_by_source_no_match_returns_empty():
    summaries = [_make_summary("db")]
    assert filter_by_source(summaries, ["redis"]) == []


# ---------------------------------------------------------------------------
# filter_metrics_by_name
# ---------------------------------------------------------------------------

def test_filter_metrics_by_name_substring_match():
    metrics = [_make_metric("consumer_lag"), _make_metric("throughput"), _make_metric("lag_p99")]
    out = filter_metrics_by_name(metrics, "lag")
    assert len(out) == 2


def test_filter_metrics_by_name_case_insensitive():
    metrics = [_make_metric("ConsumerLag")]
    out = filter_metrics_by_name(metrics, "consumerlAg")
    assert len(out) == 1


def test_filter_metrics_by_name_no_match():
    metrics = [_make_metric("throughput")]
    assert filter_metrics_by_name(metrics, "lag") == []


# ---------------------------------------------------------------------------
# filter_alerts_above_value
# ---------------------------------------------------------------------------

def test_filter_alerts_above_value_keeps_exceeding():
    results = [_make_result("warning", value=5.0), _make_result("critical", value=15.0)]
    out = filter_alerts_above_value(results, 10.0)
    assert len(out) == 1
    assert out[0].metric.value == 15.0


def test_filter_alerts_above_value_excludes_equal():
    results = [_make_result("warning", value=10.0)]
    assert filter_alerts_above_value(results, 10.0) == []


def test_filter_alerts_above_value_empty_input():
    assert filter_alerts_above_value([], 0.0) == []
