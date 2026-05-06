"""Unit tests for pipewatch.digest_scheduler."""
import datetime
import pytest

from pipewatch.digest_scheduler import DigestScheduler
from pipewatch.history import MetricHistory
from pipewatch.config import SourceConfig
from pipewatch.metrics import Metric


class _CapturingNotifier:
    def __init__(self):
        self.calls = []

    def send(self, subject: str, body: str) -> None:
        self.calls.append({"subject": subject, "body": body})


def _make_source(name: str = "svc") -> SourceConfig:
    return SourceConfig(name=name, url="http://x", metrics=[])


def _make_metric(source: str, name: str, value: float, ts: datetime.datetime) -> Metric:
    return Metric(source_name=source, name=name, value=value, unit="ms", timestamp=ts)


def test_invalid_interval_raises():
    with pytest.raises(ValueError, match="interval_seconds"):
        DigestScheduler(interval_seconds=0, window_seconds=3600)


def test_invalid_window_raises():
    with pytest.raises(ValueError, match="window_seconds"):
        DigestScheduler(interval_seconds=3600, window_seconds=-1)


def test_is_due_initially_true():
    ds = DigestScheduler(interval_seconds=3600, window_seconds=3600)
    assert ds.is_due() is True


def test_is_not_due_immediately_after_send():
    ds = DigestScheduler(interval_seconds=3600, window_seconds=3600)
    notifier = _CapturingNotifier()
    ds.notifiers.append(notifier)
    now = datetime.datetime(2024, 6, 1, 8, 0, 0)
    ds.maybe_send([], MetricHistory(), now=now)
    assert ds.is_due(now) is False


def test_is_due_after_interval_elapses():
    ds = DigestScheduler(interval_seconds=3600, window_seconds=3600)
    t0 = datetime.datetime(2024, 6, 1, 8, 0, 0)
    ds.maybe_send([], MetricHistory(), now=t0)
    t1 = t0 + datetime.timedelta(seconds=3601)
    assert ds.is_due(t1) is True


def test_maybe_send_returns_none_when_not_due():
    ds = DigestScheduler(interval_seconds=3600, window_seconds=3600)
    t0 = datetime.datetime(2024, 6, 1, 8, 0, 0)
    ds.maybe_send([], MetricHistory(), now=t0)
    result = ds.maybe_send([], MetricHistory(), now=t0 + datetime.timedelta(seconds=10))
    assert result is None


def test_notifier_receives_message():
    ds = DigestScheduler(interval_seconds=3600, window_seconds=3600)
    notifier = _CapturingNotifier()
    ds.notifiers.append(notifier)
    h = MetricHistory()
    now = datetime.datetime(2024, 6, 1, 8, 0, 0)
    h.record(_make_metric("svc", "cpu", 42.0, now - datetime.timedelta(minutes=5)))
    ds.maybe_send([_make_source("svc")], h, now=now)
    assert len(notifier.calls) == 1
    assert "Digest" in notifier.calls[0]["subject"]


def test_multiple_notifiers_all_called():
    ds = DigestScheduler(interval_seconds=60, window_seconds=300)
    n1, n2 = _CapturingNotifier(), _CapturingNotifier()
    ds.notifiers.extend([n1, n2])
    now = datetime.datetime(2024, 6, 1, 9, 0, 0)
    ds.maybe_send([], MetricHistory(), now=now)
    assert len(n1.calls) == 1
    assert len(n2.calls) == 1
