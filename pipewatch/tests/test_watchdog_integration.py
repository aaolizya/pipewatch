"""Integration tests: Watchdog wired into a simulated collection loop."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock

from pipewatch.watchdog import Watchdog


def _make_source(name: str) -> MagicMock:
    src = MagicMock()
    src.name = name
    return src


def test_all_sources_healthy_after_collection():
    dog = Watchdog(stale_after_seconds=120)
    sources = [_make_source(n) for n in ("db", "api", "queue")]
    now = datetime(2024, 6, 1, 10, 0, 0)

    for src in sources:
        dog.record(src.name, ts=now)

    stale = dog.check(now=now + timedelta(seconds=60))
    assert stale == []


def test_missing_source_flagged_after_one_cycle():
    dog = Watchdog(stale_after_seconds=30)
    now = datetime(2024, 6, 1, 10, 0, 0)

    dog.record("api", ts=now)
    dog.record("db", ts=now)
    # "queue" never reports
    dog.register("queue")

    stale = dog.check(now=now + timedelta(seconds=31))
    stale_names = {s.source_name for s in stale}
    assert "queue" in stale_names
    assert "api" not in stale_names
    assert "db" not in stale_names


def test_recovered_source_no_longer_stale():
    dog = Watchdog(stale_after_seconds=30)
    t0 = datetime(2024, 6, 1, 10, 0, 0)

    dog.record("db", ts=t0)
    # goes stale
    assert len(dog.check(now=t0 + timedelta(seconds=60))) == 1

    # recovers
    dog.record("db", ts=t0 + timedelta(seconds=65))
    assert dog.check(now=t0 + timedelta(seconds=70)) == []


def test_multiple_stale_sources_all_reported():
    dog = Watchdog(stale_after_seconds=10)
    t0 = datetime(2024, 6, 1, 8, 0, 0)

    for name in ("a", "b", "c"):
        dog.record(name, ts=t0)

    stale = dog.check(now=t0 + timedelta(seconds=20))
    assert len(stale) == 3


def test_partial_staleness_mixed_results():
    dog = Watchdog(stale_after_seconds=60)
    t0 = datetime(2024, 6, 1, 9, 0, 0)

    dog.record("fresh", ts=t0 + timedelta(seconds=50))
    dog.record("stale", ts=t0)

    stale = dog.check(now=t0 + timedelta(seconds=90))
    stale_names = {s.source_name for s in stale}
    assert "stale" in stale_names
    assert "fresh" not in stale_names
