"""Integration tests: full digest pipeline from history to dispatched report."""
import datetime

from pipewatch.digest import build_digest
from pipewatch.digest_scheduler import DigestScheduler
from pipewatch.history import MetricHistory
from pipewatch.metrics import Metric
from pipewatch.config import SourceConfig


class _Cap:
    def __init__(self):
        self.bodies = []

    def send(self, subject: str, body: str) -> None:
        self.bodies.append(body)


def _src(name: str) -> SourceConfig:
    return SourceConfig(name=name, url="http://x", metrics=[])


def _m(src: str, name: str, value: float, ts: datetime.datetime) -> Metric:
    return Metric(source_name=src, name=name, value=value, unit="", timestamp=ts)


def test_full_digest_pipeline_two_sources():
    now = datetime.datetime(2024, 3, 15, 10, 0, 0)
    h = MetricHistory()
    for v in [1.0, 2.0, 3.0]:
        h.record(_m("alpha", "rps", v, now - datetime.timedelta(minutes=5)))
    for v in [10.0, 20.0]:
        h.record(_m("beta", "errors", v, now - datetime.timedelta(minutes=3)))

    report = build_digest([_src("alpha"), _src("beta")], h, window_seconds=3600, now=now)

    assert "alpha" in report.source_names
    assert "beta" in report.source_names
    alpha_entries = report.entries_for_source("alpha")
    assert len(alpha_entries) == 1
    assert alpha_entries[0].aggregation.count == 3
    assert abs(alpha_entries[0].aggregation.mean - 2.0) < 1e-9


def test_digest_scheduler_fires_once_per_interval():
    cap = _Cap()
    ds = DigestScheduler(interval_seconds=600, window_seconds=1800)
    ds.notifiers.append(cap)
    h = MetricHistory()
    now = datetime.datetime(2024, 3, 15, 10, 0, 0)
    h.record(_m("svc", "latency", 5.0, now - datetime.timedelta(minutes=1)))

    r1 = ds.maybe_send([_src("svc")], h, now=now)
    r2 = ds.maybe_send([_src("svc")], h, now=now + datetime.timedelta(seconds=300))
    r3 = ds.maybe_send([_src("svc")], h, now=now + datetime.timedelta(seconds=601))

    assert r1 is not None
    assert r2 is None
    assert r3 is not None
    assert len(cap.bodies) == 2


def test_digest_body_contains_metric_stats():
    cap = _Cap()
    ds = DigestScheduler(interval_seconds=60, window_seconds=600)
    ds.notifiers.append(cap)
    h = MetricHistory()
    now = datetime.datetime(2024, 3, 15, 10, 0, 0)
    for v in [100.0, 200.0, 300.0]:
        h.record(_m("db", "query_time", v, now - datetime.timedelta(seconds=30)))

    ds.maybe_send([_src("db")], h, now=now)

    body = cap.bodies[0]
    assert "db" in body
    assert "query_time" in body
    assert "mean" in body
