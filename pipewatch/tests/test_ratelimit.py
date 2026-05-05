"""Tests for pipewatch.ratelimit."""

from __future__ import annotations

import time

import pytest

from pipewatch.ratelimit import RateLimitEntry, RateLimiter


# ---------------------------------------------------------------------------
# RateLimitEntry
# ---------------------------------------------------------------------------

def test_entry_defaults():
    entry = RateLimitEntry(source_name="src")
    assert entry.last_collected == 0.0
    assert entry.request_count == 0


def test_entry_update_increments_count():
    entry = RateLimitEntry(source_name="src")
    entry.update()
    assert entry.request_count == 1
    entry.update()
    assert entry.request_count == 2


def test_entry_update_sets_last_collected():
    entry = RateLimitEntry(source_name="src")
    before = time.monotonic()
    entry.update()
    after = time.monotonic()
    assert before <= entry.last_collected <= after


# ---------------------------------------------------------------------------
# RateLimiter construction
# ---------------------------------------------------------------------------

def test_negative_interval_raises():
    with pytest.raises(ValueError):
        RateLimiter(min_interval=-1.0)


def test_zero_interval_always_allowed():
    limiter = RateLimiter(min_interval=0.0)
    limiter.record("src")
    assert limiter.is_allowed("src") is True


# ---------------------------------------------------------------------------
# is_allowed / record
# ---------------------------------------------------------------------------

def test_is_allowed_first_time():
    limiter = RateLimiter(min_interval=60.0)
    assert limiter.is_allowed("new_source") is True


def test_is_allowed_false_immediately_after_record():
    limiter = RateLimiter(min_interval=60.0)
    limiter.record("src")
    assert limiter.is_allowed("src") is False


def test_is_allowed_true_after_interval(monkeypatch):
    limiter = RateLimiter(min_interval=1.0)
    base = time.monotonic()
    monkeypatch.setattr("pipewatch.ratelimit.time.monotonic", lambda: base)
    limiter.record("src")
    monkeypatch.setattr("pipewatch.ratelimit.time.monotonic", lambda: base + 1.5)
    assert limiter.is_allowed("src") is True


def test_different_sources_are_independent():
    limiter = RateLimiter(min_interval=60.0)
    limiter.record("src_a")
    assert limiter.is_allowed("src_a") is False
    assert limiter.is_allowed("src_b") is True


# ---------------------------------------------------------------------------
# time_until_allowed
# ---------------------------------------------------------------------------

def test_time_until_allowed_zero_before_first_record():
    limiter = RateLimiter(min_interval=10.0)
    assert limiter.time_until_allowed("src") == 0.0


def test_time_until_allowed_positive_after_record(monkeypatch):
    limiter = RateLimiter(min_interval=10.0)
    base = time.monotonic()
    monkeypatch.setattr("pipewatch.ratelimit.time.monotonic", lambda: base)
    limiter.record("src")
    monkeypatch.setattr("pipewatch.ratelimit.time.monotonic", lambda: base + 3.0)
    remaining = limiter.time_until_allowed("src")
    assert 6.5 < remaining <= 7.0


# ---------------------------------------------------------------------------
# request_count / reset
# ---------------------------------------------------------------------------

def test_request_count_increments():
    limiter = RateLimiter(min_interval=0.0)
    limiter.record("src")
    limiter.record("src")
    assert limiter.request_count("src") == 2


def test_reset_single_source():
    limiter = RateLimiter(min_interval=60.0)
    limiter.record("src")
    limiter.reset("src")
    assert limiter.is_allowed("src") is True
    assert limiter.request_count("src") == 0


def test_reset_all_sources():
    limiter = RateLimiter(min_interval=60.0)
    limiter.record("a")
    limiter.record("b")
    limiter.reset()
    assert limiter.is_allowed("a") is True
    assert limiter.is_allowed("b") is True
