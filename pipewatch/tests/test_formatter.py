"""Tests for pipewatch.formatter."""

from __future__ import annotations

import pytest

from pipewatch.alerting import AlertResult
from pipewatch.formatter import FormatOptions, format_pipeline_summary
from pipewatch.metrics import Metric
from pipewatch.summary import PipelineSummary, SourceSummary


def _make_metric(name: str, value: float, unit: str = "") -> Metric:
    return Metric(name=name, value=value, unit=unit, source="src", timestamp=0.0)


def _make_result(metric: Metric, level: str = "ok") -> AlertResult:
    alert = None
    if level != "ok":
        from pipewatch.metrics import AlertResult as AR
        alert = type("FakeAlert", (), {"level": level})()
    return AlertResult(metric=metric, alert=alert)


def _make_source(name: str, results) -> SourceSummary:
    return SourceSummary(source_name=name, results=results)


def _make_summary(*sources: SourceSummary) -> PipelineSummary:
    return PipelineSummary(sources=list(sources))


@pytest.fixture
def ok_summary():
    m = _make_metric("latency", 42.0, "ms")
    r = _make_result(m, "ok")
    src = _make_source("db", [r])
    return _make_summary(src)


@pytest.fixture
def critical_summary():
    m = _make_metric("error_rate", 0.9)
    r = _make_result(m, "critical")
    src = _make_source("api", [r])
    return _make_summary(src)


def test_format_contains_report_header(ok_summary):
    out = format_pipeline_summary(ok_summary, FormatOptions(color=False))
    assert "PipeWatch Report" in out


def test_format_contains_overall_status(ok_summary):
    out = format_pipeline_summary(ok_summary, FormatOptions(color=False))
    assert "Overall status:" in out


def test_format_shows_source_name(ok_summary):
    out = format_pipeline_summary(ok_summary, FormatOptions(color=False))
    assert "db" in out


def test_format_shows_metric_name_and_value(ok_summary):
    out = format_pipeline_summary(ok_summary, FormatOptions(color=False))
    assert "latency" in out
    assert "42.0" in out


def test_format_shows_unit(ok_summary):
    out = format_pipeline_summary(ok_summary, FormatOptions(color=False))
    assert "ms" in out


def test_format_critical_status_shown(critical_summary):
    out = format_pipeline_summary(critical_summary, FormatOptions(color=False))
    assert "CRITICAL" in out


def test_format_hide_ok_sources(ok_summary):
    opts = FormatOptions(color=False, show_ok=False)
    out = format_pipeline_summary(ok_summary, opts)
    assert "db" not in out


def test_format_compact_omits_metrics(ok_summary):
    opts = FormatOptions(color=False, compact=True)
    out = format_pipeline_summary(ok_summary, opts)
    assert "latency" not in out


def test_format_color_escape_codes_present(ok_summary):
    opts = FormatOptions(color=True)
    out = format_pipeline_summary(ok_summary, opts)
    assert "\033[" in out


def test_format_no_color_no_escape_codes(ok_summary):
    opts = FormatOptions(color=False)
    out = format_pipeline_summary(ok_summary, opts)
    assert "\033[" not in out


def test_format_default_options_does_not_raise(ok_summary):
    out = format_pipeline_summary(ok_summary)
    assert out
