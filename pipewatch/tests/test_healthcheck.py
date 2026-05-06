"""Tests for pipewatch.healthcheck."""

from __future__ import annotations

import pytest

from pipewatch.healthcheck import HealthCheckResult, run_healthcheck
from pipewatch.summary import PipelineSummary, SourceSummary


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_source(name: str, status: str) -> SourceSummary:
    src = SourceSummary(source_name=name, results=[], trends=[])
    src._status_override = status.upper()  # patch for test isolation
    src.status = property(lambda self: self._status_override)  # type: ignore
    # Directly set attribute since SourceSummary.status is a property
    object.__setattr__(src, "_test_status", status.upper())
    return src


class _FakeSource:
    def __init__(self, name: str, status: str):
        self.source_name = name
        self.status = status.upper()
        self.results = []
        self.trends = []


class _FakeSummary:
    def __init__(self, sources):
        self.sources = sources


def _summary(*pairs) -> _FakeSummary:
    """Build a fake PipelineSummary from (name, status) pairs."""
    return _FakeSummary([_FakeSource(n, s) for n, s in pairs])


# ---------------------------------------------------------------------------
# HealthCheckResult
# ---------------------------------------------------------------------------

def test_healthcheck_result_str_includes_status():
    r = HealthCheckResult(
        healthy=True, status="OK", total_sources=3,
        ok_count=3, warning_count=0, critical_count=0,
    )
    assert "status=OK" in str(r)


def test_healthcheck_result_str_includes_message():
    r = HealthCheckResult(
        healthy=False, status="CRITICAL", total_sources=1,
        ok_count=0, warning_count=0, critical_count=1,
        message="db is down",
    )
    assert "db is down" in str(r)


# ---------------------------------------------------------------------------
# run_healthcheck
# ---------------------------------------------------------------------------

def test_all_ok_is_healthy():
    result = run_healthcheck(_summary(("src1", "ok"), ("src2", "ok")))
    assert result.healthy is True
    assert result.status == "OK"
    assert result.ok_count == 2


def test_critical_source_marks_unhealthy():
    result = run_healthcheck(_summary(("src1", "ok"), ("src2", "critical")))
    assert result.healthy is False
    assert result.status == "CRITICAL"
    assert "src2" in result.critical_sources


def test_warning_allowed_by_default_is_healthy():
    result = run_healthcheck(_summary(("src1", "warning")))
    assert result.healthy is True
    assert result.status == "OK"
    assert result.warning_count == 1


def test_warning_disallowed_marks_unhealthy():
    result = run_healthcheck(_summary(("src1", "warning")), allow_warnings=False)
    assert result.healthy is False
    assert result.status == "WARNING"


def test_multiple_critical_sources_all_listed():
    result = run_healthcheck(
        _summary(("a", "critical"), ("b", "critical"), ("c", "ok"))
    )
    assert set(result.critical_sources) == {"a", "b"}
    assert result.critical_count == 2


def test_empty_summary_is_healthy():
    result = run_healthcheck(_FakeSummary([]))
    assert result.healthy is True
    assert result.total_sources == 0


def test_counts_are_accurate():
    result = run_healthcheck(
        _summary(("a", "ok"), ("b", "warning"), ("c", "critical"), ("d", "ok"))
    )
    assert result.ok_count == 2
    assert result.warning_count == 1
    assert result.critical_count == 1
    assert result.total_sources == 4
