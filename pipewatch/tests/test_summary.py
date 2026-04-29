"""Tests for pipewatch.summary module."""

import pytest
from unittest.mock import MagicMock

from pipewatch.metrics import AlertResult, Metric
from pipewatch.trend import TrendResult, TrendDirection
from pipewatch.summary import SourceSummary, PipelineSummary, build_summary


def _make_metric(source_name: str, name: str = "lag") -> Metric:
    m = MagicMock(spec=Metric)
    m.source_name = source_name
    m.name = name
    return m


def _make_result(source_name: str, level: str) -> AlertResult:
    r = MagicMock(spec=AlertResult)
    r.metric = _make_metric(source_name)
    r.level = level
    return r


def _make_trend(direction: TrendDirection) -> TrendResult:
    t = MagicMock(spec=TrendResult)
    t.direction = direction
    return t


# --- SourceSummary ---

def test_source_summary_status_ok():
    s = SourceSummary(source_name="src", total=3, ok=3)
    assert s.status == "OK"
    assert s.healthy is True


def test_source_summary_status_warning():
    s = SourceSummary(source_name="src", total=2, ok=1, warnings=1)
    assert s.status == "WARNING"
    assert s.healthy is True


def test_source_summary_status_critical():
    s = SourceSummary(source_name="src", total=2, ok=1, criticals=1)
    assert s.status == "CRITICAL"
    assert s.healthy is False


def test_source_summary_str_contains_name():
    s = SourceSummary(source_name="my_pipeline", total=1, ok=1)
    assert "my_pipeline" in str(s)


def test_source_summary_str_includes_trend_info():
    trend = _make_trend(TrendDirection.RISING)
    s = SourceSummary(source_name="src", total=1, ok=1, trend_results=[trend])
    result = str(s)
    assert "+1" in result


# --- PipelineSummary ---

def test_pipeline_summary_overall_ok():
    p = PipelineSummary(sources={"a": SourceSummary("a", ok=1, total=1)})
    assert p.overall_status == "OK"


def test_pipeline_summary_overall_critical_if_any_critical():
    p = PipelineSummary(sources={
        "a": SourceSummary("a", ok=1, total=1),
        "b": SourceSummary("b", criticals=1, total=1),
    })
    assert p.overall_status == "CRITICAL"


def test_pipeline_summary_str_contains_header():
    p = PipelineSummary()
    assert "Pipeline Summary" in str(p)


# --- build_summary ---

def test_build_summary_groups_by_source():
    results = [
        _make_result("src_a", "ok"),
        _make_result("src_a", "warning"),
        _make_result("src_b", "critical"),
    ]
    summary = build_summary(results)
    assert "src_a" in summary.sources
    assert "src_b" in summary.sources
    assert summary.sources["src_a"].warnings == 1
    assert summary.sources["src_b"].criticals == 1


def test_build_summary_attaches_trends():
    results = [_make_result("src_a", "ok")]
    trend = _make_trend(TrendDirection.FALLING)
    summary = build_summary(results, trend_results={"src_a:lag": trend})
    assert len(summary.sources["src_a"].trend_results) == 1


def test_build_summary_empty_results():
    summary = build_summary([])
    assert summary.overall_status == "OK"
    assert len(summary.sources) == 0
