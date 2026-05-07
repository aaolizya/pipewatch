"""Integration tests: profiler fed from real Metric objects via history."""
import pytest
from datetime import datetime, timezone

from pipewatch.metrics import Metric
from pipewatch.profiler import MetricProfiler


def _ts() -> datetime:
    return datetime.now(tz=timezone.utc)


def _metric(source: str, name: str, value: float) -> Metric:
    return Metric(source=source, name=name, value=value, timestamp=_ts())


@pytest.fixture()
def profiler() -> MetricProfiler:
    return MetricProfiler(window=50)


def test_feed_metrics_and_retrieve_profile(profiler):
    metrics = [_metric("db", "latency_ms", float(v)) for v in range(1, 11)]
    for m in metrics:
        profiler.record(m.source, m.name, m.value)
    stats = profiler.profile("db", "latency_ms")
    assert stats.count == 10
    assert stats.minimum == 1.0
    assert stats.maximum == 10.0
    assert abs(stats.mean - 5.5) < 1e-9


def test_p95_reasonable_for_skewed_data(profiler):
    # 99 low values + 1 spike
    for _ in range(99):
        profiler.record("api", "resp", 1.0)
    profiler.record("api", "resp", 1000.0)
    stats = profiler.profile("api", "resp")
    # p95 should still be close to 1.0 since spike is at the very top
    assert stats.p95 < 1000.0
    assert stats.maximum == 1000.0


def test_window_limits_memory(profiler):
    p = MetricProfiler(window=5)
    for i in range(20):
        p.record("s", "m", float(i))
    stats = p.profile("s", "m")
    assert stats.count == 5
    # Only last 5 values: 15,16,17,18,19
    assert stats.minimum == 15.0


def test_multiple_sources_all_profiled(profiler):
    sources = ["alpha", "beta", "gamma"]
    for src in sources:
        for v in [1.0, 2.0, 3.0]:
            profiler.record(src, "throughput", v)
    all_p = profiler.all_profiles()
    profiled_sources = {p.source for p in all_p}
    assert profiled_sources == set(sources)


def test_clear_and_re_record(profiler):
    profiler.record("svc", "errors", 100.0)
    profiler.clear("svc", "errors")
    profiler.record("svc", "errors", 5.0)
    stats = profiler.profile("svc", "errors")
    assert stats.count == 1
    assert stats.maximum == 5.0
