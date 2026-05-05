"""Integration tests: aggregator working with real MetricHistory and Metric objects."""

from __future__ import annotations

import time

import pytest

from pipewatch.aggregator import aggregate_from_history
from pipewatch.history import HistoryEntry, MetricHistory
from pipewatch.metrics import Metric


def _make_metric(name: str, value: float, source: str = "pipeline") -> Metric:
    return Metric(name=name, value=value, source=source, unit="ms", timestamp=time.time())


def _history_from_metrics(*metrics: Metric) -> MetricHistory:
    """Build a MetricHistory by recording HistoryEntry objects derived from Metrics."""
    h = MetricHistory()
    for m in metrics:
        entry = HistoryEntry(
            source=m.source,
            metric_name=m.name,
            value=m.value,
            timestamp=m.timestamp,
        )
        h.record(entry)
    return h


def test_full_pipeline_aggregation():
    """Record several real Metric values and verify aggregation statistics."""
    metrics = [
        _make_metric("latency", 100.0),
        _make_metric("latency", 200.0),
        _make_metric("latency", 300.0),
    ]
    h = _history_from_metrics(*metrics)
    result = aggregate_from_history(h, "pipeline", "latency")

    assert result is not None
    assert result.count == 3
    assert result.minimum == 100.0
    assert result.maximum == 300.0
    assert pytest.approx(result.average) == 200.0


def test_aggregation_isolated_per_metric_name():
    """Aggregation for one metric name must not include values from another."""
    metrics = [
        _make_metric("latency", 50.0),
        _make_metric("latency", 150.0),
        _make_metric("error_rate", 999.0),
    ]
    h = _history_from_metrics(*metrics)
    result = aggregate_from_history(h, "pipeline", "latency")

    assert result.count == 2
    assert 999.0 not in result.values


def test_limit_reflects_most_recent_values():
    """When limit is applied, the most recent entries should be used."""
    metrics = [_make_metric("latency", float(v)) for v in range(1, 11)]
    h = _history_from_metrics(*metrics)
    result = aggregate_from_history(h, "pipeline", "latency", limit=5)

    assert result.count == 5
    assert result.minimum == 6.0
    assert result.maximum == 10.0
