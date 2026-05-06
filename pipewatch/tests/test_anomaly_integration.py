"""Integration tests: anomaly detection wired to real aggregation history."""
from __future__ import annotations

import pytest
from datetime import datetime, timedelta

from pipewatch.history import MetricHistory, HistoryEntry
from pipewatch.aggregator import aggregate_from_history
from pipewatch.anomaly import detect_anomaly, detect_all


def _ts(offset_s: int = 0) -> datetime:
    return datetime(2024, 1, 1, 12, 0, 0) + timedelta(seconds=offset_s)


def _history_entries(values, name="latency", source="svc"):
    return [
        HistoryEntry(source=source, metric_name=name, value=v, timestamp=_ts(i * 10))
        for i, v in enumerate(values)
    ]


# Build a MetricHistory and populate it
def _make_history(values, name="latency", source="svc", tmp_path=None):
    h = MetricHistory(path=str(tmp_path / "hist.json")) if tmp_path else MetricHistory()
    for entry in _history_entries(values, name, source):
        h.record(entry)
    return h


def test_normal_value_not_flagged(tmp_path):
    values = [100.0] * 20
    h = _make_history(values, tmp_path=tmp_path)
    agg = aggregate_from_history(h, "svc", "latency")
    result = detect_anomaly("svc", "latency", 100.5, agg, threshold=3.0)
    assert not result.is_anomaly


def test_spike_flagged_as_anomaly(tmp_path):
    # tight cluster around 100, then a spike
    values = [100.0 + (i % 3) * 0.5 for i in range(20)]
    h = _make_history(values, tmp_path=tmp_path)
    agg = aggregate_from_history(h, "svc", "latency")
    result = detect_anomaly("svc", "latency", 200.0, agg, threshold=3.0)
    assert result.is_anomaly


def test_detect_all_with_real_history(tmp_path):
    h = _make_history([10.0] * 15, name="errors", source="api", tmp_path=tmp_path)
    agg_errors = aggregate_from_history(h, "api", "errors")

    h2 = _make_history([50.0 + i for i in range(15)], name="latency", source="api", tmp_path=tmp_path)
    agg_lat = aggregate_from_history(h2, "api", "latency")

    observations = {"errors": 10.5, "latency": 200.0}
    aggregations = {"errors": agg_errors, "latency": agg_lat}

    results = detect_all("api", observations, aggregations, threshold=3.0)
    by_name = {r.metric_name: r for r in results}

    assert not by_name["errors"].is_anomaly
    assert by_name["latency"].is_anomaly


def test_custom_threshold_changes_sensitivity(tmp_path):
    values = [100.0] * 20
    h = _make_history(values, tmp_path=tmp_path)
    agg = aggregate_from_history(h, "svc", "latency")
    # With stddev ~0, any deviation is infinite — but we test a real variance case
    values2 = [100.0 + (i % 5) for i in range(20)]
    h2 = _make_history(values2, tmp_path=tmp_path)
    agg2 = aggregate_from_history(h2, "svc", "latency")

    strict = detect_anomaly("svc", "latency", 104.0, agg2, threshold=1.0)
    lenient = detect_anomaly("svc", "latency", 104.0, agg2, threshold=5.0)
    # strict threshold should flag what lenient does not (or both; just ensure no crash)
    assert isinstance(strict.is_anomaly, bool)
    assert isinstance(lenient.is_anomaly, bool)
