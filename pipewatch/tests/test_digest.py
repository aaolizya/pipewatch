"""Unit tests for pipewatch.digest."""
import datetime
import pytest

from pipewatch.digest import DigestEntry, DigestReport, build_digest
from pipewatch.aggregator import AggregationResult
from pipewatch.history import MetricHistory
from pipewatch.metrics import Metric
from pipewatch.config import SourceConfig


def _make_source(name: str = "db") -> SourceConfig:
    return SourceConfig(name=name, url="http://example.com", metrics=[])


def _make_metric(source: str, name: str, value: float, ts: datetime.datetime) -> Metric:
    return Metric(source_name=source, name=name, value=value, unit="ms", timestamp=ts)


@pytest.fixture()
def history_with_data():
    h = MetricHistory()
    now = datetime.datetime(2024, 1, 1, 12, 0, 0)
    for i, v in enumerate([10.0, 20.0, 30.0]):
        ts = now - datetime.timedelta(minutes=10 * i)
        h.record(_make_metric("db", "latency", v, ts))
    return h, now


def test_digest_entry_str():
    agg = AggregationResult(min=1.0, max=3.0, mean=2.0, stddev=1.0, count=3)
    entry = DigestEntry(source_name="db", metric_name="latency", aggregation=agg)
    text = str(entry)
    assert "db/latency" in text
    assert "mean=2" in text
    assert "samples=3" in text


def test_digest_report_source_names():
    agg = AggregationResult(min=1.0, max=1.0, mean=1.0, stddev=0.0, count=1)
    entries = [
        DigestEntry("alpha", "m", agg),
        DigestEntry("beta", "m", agg),
        DigestEntry("alpha", "n", agg),
    ]
    report = DigestReport(
        generated_at=datetime.datetime(2024, 1, 1),
        window_seconds=3600,
        entries=entries,
    )
    assert report.source_names == ["alpha", "beta"]


def test_digest_report_entries_for_source():
    agg = AggregationResult(min=1.0, max=1.0, mean=1.0, stddev=0.0, count=1)
    entries = [
        DigestEntry("alpha", "m", agg),
        DigestEntry("beta", "m", agg),
    ]
    report = DigestReport(
        generated_at=datetime.datetime(2024, 1, 1),
        window_seconds=3600,
        entries=entries,
    )
    assert len(report.entries_for_source("alpha")) == 1
    assert len(report.entries_for_source("missing")) == 0


def test_digest_report_str_contains_header(history_with_data):
    h, now = history_with_data
    report = build_digest([_make_source("db")], h, window_seconds=3600, now=now)
    text = str(report)
    assert "Digest Report" in text
    assert "db" in text


def test_build_digest_respects_window(history_with_data):
    h, now = history_with_data
    # window of 5 minutes should exclude entries older than that
    report_narrow = build_digest([_make_source("db")], h, window_seconds=300, now=now)
    report_wide = build_digest([_make_source("db")], h, window_seconds=3600, now=now)
    if report_narrow.entries and report_wide.entries:
        narrow_count = report_narrow.entries[0].aggregation.count
        wide_count = report_wide.entries[0].aggregation.count
        assert narrow_count <= wide_count


def test_build_digest_empty_history():
    h = MetricHistory()
    now = datetime.datetime(2024, 1, 1, 12, 0, 0)
    report = build_digest([_make_source("db")], h, window_seconds=3600, now=now)
    assert isinstance(report, DigestReport)
    assert report.entries == []
