"""Integration tests for QuotaManager used alongside the collector."""
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

from pipewatch.quota import QuotaManager
from pipewatch.config import SourceConfig


def _make_source(name: str = "db") -> SourceConfig:
    return SourceConfig(
        name=name,
        url="http://example.com/metrics",
        metric_name="row_count",
        warning_threshold=100.0,
        critical_threshold=200.0,
    )


def test_quota_gates_collection_loop():
    """Simulate a tight collection loop where quota runs out mid-way."""
    mgr = QuotaManager(default_limit=3, window_seconds=3600)
    source = _make_source()
    collected = 0
    blocked = 0

    for _ in range(6):
        if mgr.check_and_consume(source.name):
            collected += 1
        else:
            blocked += 1

    assert collected == 3
    assert blocked == 3


def test_quota_resets_after_window_allows_new_cycle():
    mgr = QuotaManager(default_limit=2, window_seconds=60)
    source = _make_source()
    t0 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)

    mgr.check_and_consume(source.name, now=t0)
    mgr.check_and_consume(source.name, now=t0)
    assert mgr.check_and_consume(source.name, now=t0) is False

    t1 = t0 + timedelta(seconds=61)
    assert mgr.check_and_consume(source.name, now=t1) is True
    assert mgr.remaining(source.name) == 1


def test_per_source_quotas_do_not_interfere():
    mgr = QuotaManager(default_limit=2, window_seconds=3600)
    sources = [_make_source(f"src_{i}") for i in range(3)]

    for s in sources:
        mgr.check_and_consume(s.name)
        mgr.check_and_consume(s.name)

    for s in sources:
        assert mgr.remaining(s.name) == 0
        assert mgr.check_and_consume(s.name) is False


def test_set_limit_per_source_overrides_default():
    mgr = QuotaManager(default_limit=100, window_seconds=3600)
    source = _make_source("limited")
    mgr.set_limit(source.name, 2)

    mgr.check_and_consume(source.name)
    mgr.check_and_consume(source.name)
    assert mgr.check_and_consume(source.name) is False
    # other sources still have the default limit
    other = _make_source("other")
    assert mgr.remaining(other.name) == 100
