"""Integration tests for replay using real history and metric evaluation."""
from datetime import datetime, timezone, timedelta
import pytest

from pipewatch.history import HistoryEntry, MetricHistory
from pipewatch.replay import replay_history


def _ts(offset_seconds: int = 0) -> datetime:
    base = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    return base + timedelta(seconds=offset_seconds)


@pytest.fixture
def history(tmp_path):
    h = MetricHistory(path=tmp_path / "h.json")
    values = [5.0, 15.0, 25.0, 75.0, 95.0]
    for i, v in enumerate(values):
        entry = HistoryEntry(timestamp=_ts(i * 60), value=v, unit="pct")
        h.record("pipeline_a", "queue_depth", entry)
    return h


def test_full_replay_returns_all_events(history):
    report = replay_history("pipeline_a", history, "queue_depth")
    assert report.total == 5


def test_critical_alerts_detected(history):
    report = replay_history(
        "pipeline_a", history, "queue_depth",
        warn_threshold=50.0, crit_threshold=80.0,
    )
    critical_events = [
        e for e in report.events if e.result.status == "critical"
    ]
    assert len(critical_events) == 1
    assert critical_events[0].metric.value == 95.0


def test_warning_alerts_detected(history):
    report = replay_history(
        "pipeline_a", history, "queue_depth",
        warn_threshold=50.0, crit_threshold=80.0,
    )
    warning_events = [
        e for e in report.events if e.result.status == "warning"
    ]
    assert len(warning_events) == 1
    assert warning_events[0].metric.value == 75.0


def test_ok_events_have_no_alert(history):
    report = replay_history(
        "pipeline_a", history, "queue_depth",
        warn_threshold=50.0, crit_threshold=80.0,
    )
    ok_events = [e for e in report.events if e.result.status == "ok"]
    assert all(not e.result.is_alert for e in ok_events)


def test_limit_one_returns_last_entry(history):
    report = replay_history(
        "pipeline_a", history, "queue_depth",
        crit_threshold=80.0, limit=1
    )
    assert report.total == 1
    assert report.events[0].metric.value == 95.0
