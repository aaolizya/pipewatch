"""Integration tests: Snapshot.from_summary wired to real summary objects."""
from unittest.mock import MagicMock

import pytest

from pipewatch.metrics import AlertResult, Metric
from pipewatch.snapshot import Snapshot
from pipewatch.summary import PipelineSummary, SourceSummary
from pipewatch.trend import TrendDirection, TrendResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_result(status: str) -> AlertResult:
    metric = Metric(source="src", name="m", value=1.0, unit=None, timestamp=0.0)
    return AlertResult(metric=metric, status=status, threshold=None)


def _make_source(name: str, *statuses: str) -> SourceSummary:
    results = [_make_result(s) for s in statuses]
    trend = TrendResult(direction=TrendDirection.STABLE, delta=0.0)
    return SourceSummary(name=name, results=results, trend=trend)


# ---------------------------------------------------------------------------
# Snapshot.from_summary
# ---------------------------------------------------------------------------

def test_from_summary_source_count():
    summary = PipelineSummary(
        sources=[
            _make_source("db", "ok", "ok"),
            _make_source("api", "warning"),
        ]
    )
    snap = Snapshot.from_summary(summary)
    assert snap.source_count == 2


def test_from_summary_counts_statuses():
    summary = PipelineSummary(
        sources=[
            _make_source("a", "ok"),
            _make_source("b", "warning"),
            _make_source("c", "critical"),
        ]
    )
    snap = Snapshot.from_summary(summary)
    assert snap.healthy_count == 1
    assert snap.warning_count == 1
    assert snap.critical_count == 1


def test_from_summary_source_names():
    summary = PipelineSummary(
        sources=[
            _make_source("db", "ok"),
            _make_source("queue", "ok"),
        ]
    )
    snap = Snapshot.from_summary(summary)
    names = [s["name"] for s in snap.sources]
    assert names == ["db", "queue"]


def test_from_summary_captured_at_set():
    summary = PipelineSummary(sources=[_make_source("db", "ok")])
    snap = Snapshot.from_summary(summary)
    assert snap.captured_at  # non-empty ISO timestamp
    assert "+" in snap.captured_at or snap.captured_at.endswith("Z")


def test_from_summary_overall_status_propagated():
    summary = PipelineSummary(
        sources=[_make_source("db", "critical")]
    )
    snap = Snapshot.from_summary(summary)
    assert snap.overall_status == summary.overall_status
