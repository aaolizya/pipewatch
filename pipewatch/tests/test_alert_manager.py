"""Tests for pipewatch.alert_manager."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alert_manager import AlertManager
from pipewatch.config import PipewatchConfig, SourceConfig
from pipewatch.history import MetricHistory
from pipewatch.metrics import Metric


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_source(name="svc", warning=80.0, critical=90.0):
    return SourceConfig(
        name=name,
        url="http://example.com/metrics",
        metric="latency_ms",
        warning=warning,
        critical=critical,
    )


def _make_config(sources=None):
    return PipewatchConfig(
        sources=sources or [_make_source()],
        poll_interval=30,
    )


def _make_metric(name="latency_ms", value=50.0, source="svc"):
    return Metric(name=name, value=value, source=source, unit="ms")


# ---------------------------------------------------------------------------
# from_config
# ---------------------------------------------------------------------------

def test_from_config_creates_instance():
    cfg = _make_config()
    manager = AlertManager.from_config(cfg)
    assert isinstance(manager, AlertManager)
    assert isinstance(manager.history, MetricHistory)


def test_from_config_accepts_existing_history():
    cfg = _make_config()
    hist = MetricHistory()
    manager = AlertManager.from_config(cfg, history=hist)
    assert manager.history is hist


# ---------------------------------------------------------------------------
# process — normal (no alerts)
# ---------------------------------------------------------------------------

def test_process_records_history():
    cfg = _make_config()
    manager = AlertManager.from_config(cfg)
    metric = _make_metric(value=50.0)
    manager.process([metric])
    assert len(manager.history.get(metric.name, metric.source)) == 1


def test_process_returns_pipeline_summary():
    from pipewatch.summary import PipelineSummary
    cfg = _make_config()
    manager = AlertManager.from_config(cfg)
    summary = manager.process([_make_metric(value=50.0)])
    assert isinstance(summary, PipelineSummary)


# ---------------------------------------------------------------------------
# process — alert triggering
# ---------------------------------------------------------------------------

def test_process_dispatches_on_new_critical():
    cfg = _make_config()
    manager = AlertManager.from_config(cfg)
    mock_notifier = MagicMock()
    manager.notifiers = [mock_notifier]

    manager.process([_make_metric(value=95.0)])  # critical
    mock_notifier.send.assert_called_once()


def test_process_no_dispatch_when_already_critical():
    cfg = _make_config()
    manager = AlertManager.from_config(cfg)
    mock_notifier = MagicMock()
    manager.notifiers = [mock_notifier]

    manager.process([_make_metric(value=95.0)])  # first — triggers
    manager.process([_make_metric(value=95.0)])  # second — already known
    assert mock_notifier.send.call_count == 1


def test_process_no_dispatch_when_ok():
    cfg = _make_config()
    manager = AlertManager.from_config(cfg)
    mock_notifier = MagicMock()
    manager.notifiers = [mock_notifier]

    manager.process([_make_metric(value=50.0)])
    mock_notifier.send.assert_not_called()
