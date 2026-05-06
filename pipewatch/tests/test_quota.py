"""Unit tests for pipewatch.quota."""
from datetime import datetime, timezone, timedelta

import pytest

from pipewatch.quota import QuotaEntry, QuotaManager


# ---------------------------------------------------------------------------
# QuotaEntry
# ---------------------------------------------------------------------------

def test_entry_defaults():
    entry = QuotaEntry(limit=100)
    assert entry.used == 0
    assert entry.remaining == 100
    assert not entry.exhausted


def test_consume_decrements_remaining():
    entry = QuotaEntry(limit=5)
    assert entry.consume() is True
    assert entry.remaining == 4
    assert entry.used == 1


def test_consume_returns_false_when_exhausted():
    entry = QuotaEntry(limit=2)
    entry.consume()
    entry.consume()
    assert entry.exhausted
    assert entry.consume() is False
    assert entry.used == 2  # not incremented further


def test_window_expired_false_immediately():
    entry = QuotaEntry(limit=10, window_seconds=3600)
    assert not entry.window_expired()


def test_window_expired_true_after_window():
    past = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    entry = QuotaEntry(limit=10, window_seconds=60, window_start=past)
    now = past + timedelta(seconds=120)
    assert entry.window_expired(now)


def test_reset_clears_used():
    entry = QuotaEntry(limit=10)
    entry.consume()
    entry.consume()
    entry.reset()
    assert entry.used == 0
    assert entry.remaining == 10


# ---------------------------------------------------------------------------
# QuotaManager
# ---------------------------------------------------------------------------

def _mgr(limit: int = 10, window: int = 3600) -> QuotaManager:
    return QuotaManager(default_limit=limit, window_seconds=window)


def test_check_and_consume_first_call_allowed():
    mgr = _mgr(limit=5)
    assert mgr.check_and_consume("src") is True


def test_remaining_decrements_after_consume():
    mgr = _mgr(limit=3)
    mgr.check_and_consume("src")
    assert mgr.remaining("src") == 2


def test_quota_exhausted_blocks():
    mgr = _mgr(limit=2)
    mgr.check_and_consume("src")
    mgr.check_and_consume("src")
    assert mgr.check_and_consume("src") is False


def test_window_reset_restores_quota():
    past = datetime(2024, 1, 1, tzinfo=timezone.utc)
    mgr = _mgr(limit=2, window=60)
    mgr.check_and_consume("src", now=past)
    mgr.check_and_consume("src", now=past)
    assert mgr.check_and_consume("src", now=past) is False
    future = past + timedelta(seconds=61)
    assert mgr.check_and_consume("src", now=future) is True


def test_multiple_sources_tracked_independently():
    mgr = _mgr(limit=1)
    assert mgr.check_and_consume("a") is True
    assert mgr.check_and_consume("b") is True
    assert mgr.check_and_consume("a") is False


def test_set_limit_changes_limit():
    mgr = _mgr(limit=5)
    mgr.set_limit("src", 1)
    mgr.check_and_consume("src")
    assert mgr.check_and_consume("src") is False


def test_negative_default_limit_raises():
    with pytest.raises(ValueError):
        QuotaManager(default_limit=-1)


def test_zero_window_raises():
    with pytest.raises(ValueError):
        QuotaManager(window_seconds=0)


def test_set_negative_limit_raises():
    mgr = _mgr()
    with pytest.raises(ValueError):
        mgr.set_limit("src", -5)


def test_reset_clears_used_for_source():
    mgr = _mgr(limit=1)
    mgr.check_and_consume("src")
    assert mgr.remaining("src") == 0
    mgr.reset("src")
    assert mgr.remaining("src") == 1
