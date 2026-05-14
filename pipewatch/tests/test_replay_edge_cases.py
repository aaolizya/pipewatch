"""Edge-case tests for pipewatch.replay."""
from datetime import datetime, timezone
import pytest

from pipewatch.history import HistoryEntry, MetricHistory
from pipewatch.replay import ReplayReport, replay_history


DT = datetime(2024, 3, 15, tzinfo=timezone.utc)


def _history_with_single_entry(tmp_path, value: float) -> MetricHistory:
    h = MetricHistory(path=tmp_path / "h.json")
    h.record("src", "m", HistoryEntry(timestamp=DT, value=value, unit=None))
    return h


def test_limit_larger_than_history_returns_all(tmp_path):
    h = MetricHistory(path=tmp_path / "h.json")
    for v in [1.0, 2.0]:
        h.record("src", "m", HistoryEntry(timestamp=DT, value=v, unit=None))
    report = replay_history("src", h, "m", limit=100)
    assert report.total == 2


def test_limit_zero_returns_empty(tmp_path):
    h = _history_with_single_entry(tmp_path, 42.0)
    report = replay_history("src", h, "m", limit=0)
    assert report.total == 0


def test_unknown_source_returns_empty_report(tmp_path):
    h = MetricHistory(path=tmp_path / "h.json")
    report = replay_history("nonexistent", h, "m")
    assert report.total == 0


def test_unit_preserved_on_replayed_metric(tmp_path):
    h = MetricHistory(path=tmp_path / "h.json")
    h.record("src", "m", HistoryEntry(timestamp=DT, value=1.0, unit="MB/s"))
    report = replay_history("src", h, "m")
    assert report.events[0].metric.unit == "MB/s"


def test_warn_only_threshold_no_crit(tmp_path):
    h = MetricHistory(path=tmp_path / "h.json")
    for v in [1.0, 60.0]:
        h.record("src", "m", HistoryEntry(timestamp=DT, value=v, unit=None))
    report = replay_history("src", h, "m", warn_threshold=50.0)
    statuses = {e.result.status for e in report.events}
    assert "warning" in statuses
    assert "critical" not in statuses


def test_replay_report_empty_alert_count_is_zero():
    report = ReplayReport()
    assert report.alert_count == 0
    assert report.total == 0


def test_replay_source_name_in_event_str(tmp_path):
    h = _history_with_single_entry(tmp_path, 5.0)
    report = replay_history("my_source", h, "m")
    assert "my_source" in str(report.events[0])
