"""Unit tests for pipewatch.watchdog."""

from datetime import datetime, timedelta

import pytest

from pipewatch.watchdog import StaleSource, Watchdog


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def dog() -> Watchdog:
    return Watchdog(stale_after_seconds=60.0)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

def test_invalid_stale_after_raises():
    with pytest.raises(ValueError):
        Watchdog(stale_after_seconds=0)


def test_invalid_stale_after_negative_raises():
    with pytest.raises(ValueError):
        Watchdog(stale_after_seconds=-10)


# ---------------------------------------------------------------------------
# register / tracked_sources
# ---------------------------------------------------------------------------

def test_register_adds_source(dog):
    dog.register("db")
    assert "db" in dog.tracked_sources


def test_register_idempotent(dog):
    dog.register("db")
    dog.register("db")
    assert dog.tracked_sources.count("db") == 1


# ---------------------------------------------------------------------------
# record
# ---------------------------------------------------------------------------

def test_record_marks_seen(dog):
    now = datetime(2024, 1, 1, 12, 0, 0)
    dog.record("api", ts=now)
    assert "api" in dog.tracked_sources


def test_record_auto_registers(dog):
    dog.record("queue")
    assert "queue" in dog.tracked_sources


# ---------------------------------------------------------------------------
# check — no stale sources
# ---------------------------------------------------------------------------

def test_check_fresh_source_not_stale(dog):
    now = datetime(2024, 1, 1, 12, 0, 0)
    dog.record("api", ts=now)
    result = dog.check(now=now + timedelta(seconds=30))
    assert result == []


def test_check_exactly_at_boundary_not_stale(dog):
    now = datetime(2024, 1, 1, 12, 0, 0)
    dog.record("api", ts=now)
    result = dog.check(now=now + timedelta(seconds=60))
    assert result == []


# ---------------------------------------------------------------------------
# check — stale sources
# ---------------------------------------------------------------------------

def test_check_stale_source_returned(dog):
    now = datetime(2024, 1, 1, 12, 0, 0)
    dog.record("api", ts=now)
    result = dog.check(now=now + timedelta(seconds=61))
    assert len(result) == 1
    assert result[0].source_name == "api"


def test_check_never_seen_source_is_stale(dog):
    dog.register("ghost")
    result = dog.check(now=datetime(2024, 1, 1, 12, 0, 0))
    assert any(s.source_name == "ghost" for s in result)


def test_check_stale_source_has_correct_last_seen(dog):
    ts = datetime(2024, 1, 1, 11, 0, 0)
    dog.record("db", ts=ts)
    result = dog.check(now=ts + timedelta(seconds=120))
    assert result[0].last_seen == ts


# ---------------------------------------------------------------------------
# reset
# ---------------------------------------------------------------------------

def test_reset_removes_source(dog):
    dog.record("api")
    dog.reset("api")
    assert "api" not in dog.tracked_sources


def test_reset_nonexistent_source_no_error(dog):
    dog.reset("nonexistent")  # should not raise


# ---------------------------------------------------------------------------
# StaleSource.__str__
# ---------------------------------------------------------------------------

def test_stale_source_str_includes_name():
    s = StaleSource(source_name="db", last_seen=None, silence_seconds=60.0)
    assert "db" in str(s)
    assert "STALE" in str(s)


def test_stale_source_str_never_when_no_last_seen():
    s = StaleSource(source_name="db", last_seen=None, silence_seconds=60.0)
    assert "never" in str(s)
