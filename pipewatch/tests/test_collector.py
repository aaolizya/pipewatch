"""Tests for pipewatch.collector."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest
import requests

from pipewatch.collector import CollectionError, collect_all, collect_metric
from pipewatch.config import SourceConfig
from pipewatch.metrics import Metric


@pytest.fixture()
def source() -> SourceConfig:
    return SourceConfig(
        name="lag",
        url="http://example.com/metrics",
        unit="messages",
        warning_threshold=100.0,
        critical_threshold=500.0,
    )


def _mock_response(value) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {"value": value}
    resp.raise_for_status.return_value = None
    return resp


@patch("pipewatch.collector.requests.get")
def test_collect_metric_returns_metric(mock_get, source):
    mock_get.return_value = _mock_response(42.0)
    metric = collect_metric(source)
    assert isinstance(metric, Metric)
    assert metric.name == "lag"
    assert metric.value == 42.0
    assert metric.unit == "messages"


@patch("pipewatch.collector.requests.get")
def test_collect_metric_timestamp_set(mock_get, source):
    before = time.time()
    mock_get.return_value = _mock_response(1.0)
    metric = collect_metric(source)
    assert metric.timestamp >= before


@patch("pipewatch.collector.requests.get")
def test_collect_metric_request_error_raises(mock_get, source):
    mock_get.side_effect = requests.ConnectionError("refused")
    with pytest.raises(CollectionError, match="lag"):
        collect_metric(source)


@patch("pipewatch.collector.requests.get")
def test_collect_metric_missing_value_key(mock_get, source):
    resp = MagicMock()
    resp.json.return_value = {"other": 1}
    resp.raise_for_status.return_value = None
    mock_get.return_value = resp
    with pytest.raises(CollectionError, match="missing 'value'"):
        collect_metric(source)


@patch("pipewatch.collector.requests.get")
def test_collect_metric_non_numeric_value(mock_get, source):
    mock_get.return_value = _mock_response("not-a-number")
    with pytest.raises(CollectionError, match="Non-numeric"):
        collect_metric(source)


@patch("pipewatch.collector.collect_metric")
def test_collect_all_returns_successful(mock_collect, source):
    metric = Metric(name="lag", value=10.0, unit="messages", timestamp=0.0)
    mock_collect.return_value = metric
    results = collect_all([source])
    assert results == [metric]


@patch("pipewatch.collector.collect_metric")
def test_collect_all_skips_failed_sources(mock_collect, source):
    mock_collect.side_effect = CollectionError("boom")
    results = collect_all([source])
    assert results == []


@patch("pipewatch.collector.collect_metric")
def test_collect_all_partial_failure(mock_collect):
    good = SourceConfig(name="ok", url="http://a", warning_threshold=1, critical_threshold=2)
    bad = SourceConfig(name="bad", url="http://b", warning_threshold=1, critical_threshold=2)
    good_metric = Metric(name="ok", value=0.0, unit=None, timestamp=0.0)

    def side_effect(src, **kw):
        if src.name == "bad":
            raise CollectionError("fail")
        return good_metric

    mock_collect.side_effect = side_effect
    results = collect_all([good, bad])
    assert results == [good_metric]
