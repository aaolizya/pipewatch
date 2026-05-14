"""Unit tests for pipewatch.replay."""
from datetime import datetime, timezone
import pytest

from pipewatch.history import HistoryEntry, MetricHistory
from pipewatch.replay import (
    ReplayEvent,
    ReplayReport,
    replay_history,
)


DT = datetime(2024, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def populated_history(tmp_path):
    h = MetricHistory(path=tmp_path / "hist.json")
    for v in [10.0, 50.0, 90.0]:
        entry = HistoryEntry(timestamp=DT, value=v, unit="pct")
        h.record("src", "load", entry)
    return h


def test_replay_report_total(populated_history):
    report = replay_history("src", populated_history, "load")
    assert report.total == 3


def test_replay_report_no_thresholds_no_alerts(populated_history):
    report = replay_history("src", populated_history, "load")
    assert report.alert_count == 0


def test_replay_report_crit_threshold_catches_high_value(populated_history):
    report = replay_history(
        "src", populated_history, "load",
        warn_threshold=40.0, crit_threshold=80.0
    )
    # 90.0 should be critical
    assert report.alert_count >= 1


def test_replay_event_str(populated_history):
    report = replay_history("src", populated_history, "load")
    event = report.events[0]
    assert "src" in str(event)
    assert "load" in str(event)


def test_replay_report_str(populated_history):
    report = replay_history("src", populated_history, "load")
    s = str(report)
    assert "ReplayReport" in s
    assert "3" in s


def test_replay_limit_restricts_events(populated_history):
    report = replay_history("src", populated_history, "load", limit=2)
    assert report.total == 2


def test_replay_limit_takes_most_recent(populated_history):
    report = replay_history(
        "src", populated_history, "load",
        crit_threshold=80.0, limit=1
    )
    # Last entry is 90.0 which exceeds crit_threshold
    assert report.alert_count == 1


def test_replay_empty_history(tmp_path):
    h = MetricHistory(path=tmp_path / "empty.json")
    report = replay_history("src", h, "load")
    assert report.total == 0
    assert report.alert_count == 0


def test_replay_event_metric_value_preserved(populated_history):
    report = replay_history("src", populated_history, "load")
    values = [e.metric.value for e in report.events]
    assert 10.0 in values
    assert 50.0 in values
    assert 90.0 in values


def test_replay_report_alert_count_property():
    from pipewatch.metrics import Metric
    from pipewatch.alerting import AlertResult
    report = ReplayReport()
    m = Metric(name="x", value=1.0, timestamp=DT)
    ok_result = AlertResult(status="ok", is_alert=False, message="")
    crit_result = AlertResult(status="critical", is_alert=True, message="high")
    report.events.append(ReplayEvent(source_name="s", metric=m, result=ok_result))
    report.events.append(ReplayEvent(source_name="s", metric=m, result=crit_result))
    assert report.alert_count == 1
