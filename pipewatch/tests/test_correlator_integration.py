"""Integration tests: correlator works end-to-end with MetricHistory."""
import math

from pipewatch.correlator import correlate
from pipewatch.history import MetricHistory
from pipewatch.metrics import Metric


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_metric(source: str, name: str, value: float) -> Metric:
    return Metric(source=source, name=name, value=value, unit="")


def _populate(history: MetricHistory, source: str, name: str, values, base_ts=1_000_000.0):
    """Record a sequence of values at 1-second intervals."""
    for i, v in enumerate(values):
        m = _make_metric(source, name, v)
        history.record(m, timestamp=base_ts + i)


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_positive_correlation_detected(tmp_path):
    h = MetricHistory(path=tmp_path / "hist.json")
    _populate(h, "svc", "cpu", [10, 20, 30, 40, 50])
    _populate(h, "svc", "load", [1, 2, 3, 4, 5])

    entries_cpu = h.get("svc", "cpu")
    entries_load = h.get("svc", "load")

    result = correlate("svc", "cpu", entries_cpu, "svc", "load", entries_load)
    assert result is not None
    assert math.isclose(result.coefficient, 1.0, abs_tol=1e-6)
    assert result.sample_size == 5


def test_negative_correlation_detected(tmp_path):
    h = MetricHistory(path=tmp_path / "hist.json")
    _populate(h, "svc", "free_mem", [100, 80, 60, 40, 20])
    _populate(h, "svc", "rss", [10, 30, 50, 70, 90])

    a = h.get("svc", "free_mem")
    b = h.get("svc", "rss")

    result = correlate("svc", "free_mem", a, "svc", "rss", b)
    assert result is not None
    assert result.coefficient < -0.99


def test_uncorrelated_sources_coefficient_near_zero(tmp_path):
    h = MetricHistory(path=tmp_path / "hist.json")
    _populate(h, "svc", "alpha", [1, 2, 3, 4, 5])
    # constant series — pearson undefined → None
    _populate(h, "svc", "beta", [7, 7, 7, 7, 7])

    a = h.get("svc", "alpha")
    b = h.get("svc", "beta")

    result = correlate("svc", "alpha", a, "svc", "beta", b)
    assert result is None  # zero variance in beta


def test_cross_source_correlation(tmp_path):
    h = MetricHistory(path=tmp_path / "hist.json")
    _populate(h, "db", "query_time", [5, 10, 15, 20, 25])
    _populate(h, "api", "latency", [50, 100, 150, 200, 250])

    a = h.get("db", "query_time")
    b = h.get("api", "latency")

    result = correlate("db", "query_time", a, "api", "latency", b)
    assert result is not None
    assert result.source_a == "db"
    assert result.source_b == "api"
    assert math.isclose(result.coefficient, 1.0, abs_tol=1e-6)
