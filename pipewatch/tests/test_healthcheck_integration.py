"""Integration tests: healthcheck driven from real summary/alerting objects."""

from __future__ import annotations

from pipewatch.alerting import build_source_map
from pipewatch.healthcheck import run_healthcheck
from pipewatch.metrics import AlertResult, Metric
from pipewatch.summary import PipelineSummary, SourceSummary

import datetime


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _metric(name: str, value: float) -> Metric:
    return Metric(
        source="integration",
        name=name,
        value=value,
        unit="ms",
        timestamp=datetime.datetime.utcnow(),
    )


def _result(metric: Metric, level: str) -> AlertResult:
    return AlertResult(metric=metric, level=level, threshold=None)


def _source_summary(name: str, results) -> SourceSummary:
    return SourceSummary(source_name=name, results=results, trends=[])


def _pipeline(*sources) -> PipelineSummary:
    return PipelineSummary(sources=list(sources))


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

def test_all_ok_pipeline_healthy():
    s1 = _source_summary("db", [_result(_metric("latency", 10), "ok")])
    s2 = _source_summary("api", [_result(_metric("error_rate", 0.1), "ok")])
    summary = _pipeline(s1, s2)
    hc = run_healthcheck(summary)
    assert hc.healthy is True
    assert hc.status == "OK"
    assert hc.total_sources == 2


def test_single_critical_source_fails_check():
    s1 = _source_summary("db", [_result(_metric("latency", 9000), "critical")])
    s2 = _source_summary("api", [_result(_metric("error_rate", 0.0), "ok")])
    summary = _pipeline(s1, s2)
    hc = run_healthcheck(summary)
    assert hc.healthy is False
    assert hc.status == "CRITICAL"
    assert "db" in hc.critical_sources
    assert hc.critical_count == 1
    assert hc.ok_count == 1


def test_warning_does_not_fail_by_default():
    s1 = _source_summary("queue", [_result(_metric("depth", 500), "warning")])
    summary = _pipeline(s1)
    hc = run_healthcheck(summary)
    assert hc.healthy is True
    assert hc.warning_count == 1


def test_strict_mode_fails_on_warning():
    s1 = _source_summary("queue", [_result(_metric("depth", 500), "warning")])
    summary = _pipeline(s1)
    hc = run_healthcheck(summary, allow_warnings=False)
    assert hc.healthy is False
    assert hc.status == "WARNING"


def test_mixed_statuses_critical_dominates():
    sources = [
        _source_summary("a", [_result(_metric("x", 1), "ok")]),
        _source_summary("b", [_result(_metric("x", 2), "warning")]),
        _source_summary("c", [_result(_metric("x", 3), "critical")]),
    ]
    hc = run_healthcheck(_pipeline(*sources))
    assert hc.status == "CRITICAL"
    assert hc.healthy is False
    assert hc.ok_count == 1
    assert hc.warning_count == 1
    assert hc.critical_count == 1
