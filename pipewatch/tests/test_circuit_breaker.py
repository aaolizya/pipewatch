"""Tests for pipewatch.circuit_breaker."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from pipewatch.circuit_breaker import BreakerState, CircuitBreaker
from pipewatch.collector import CollectionError


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

def test_invalid_failure_threshold_raises():
    with pytest.raises(ValueError, match="failure_threshold"):
        CircuitBreaker(failure_threshold=0)


def test_invalid_recovery_timeout_raises():
    with pytest.raises(ValueError, match="recovery_timeout"):
        CircuitBreaker(recovery_timeout=0)


# ---------------------------------------------------------------------------
# Initial state
# ---------------------------------------------------------------------------

def test_new_source_is_closed():
    cb = CircuitBreaker()
    assert cb.state("src") == BreakerState.CLOSED


def test_new_source_is_allowed():
    cb = CircuitBreaker()
    assert cb.is_allowed("src") is True


# ---------------------------------------------------------------------------
# Failure recording
# ---------------------------------------------------------------------------

def test_failures_below_threshold_stays_closed():
    cb = CircuitBreaker(failure_threshold=3)
    cb.record_failure("src")
    cb.record_failure("src")
    assert cb.state("src") == BreakerState.CLOSED


def test_failures_at_threshold_opens_breaker():
    cb = CircuitBreaker(failure_threshold=3)
    for _ in range(3):
        cb.record_failure("src")
    assert cb.state("src") == BreakerState.OPEN


def test_open_breaker_blocks_is_allowed():
    cb = CircuitBreaker(failure_threshold=1)
    cb.record_failure("src")
    assert cb.is_allowed("src") is False


# ---------------------------------------------------------------------------
# Success recording
# ---------------------------------------------------------------------------

def test_success_resets_failure_count():
    cb = CircuitBreaker(failure_threshold=3)
    cb.record_failure("src")
    cb.record_failure("src")
    cb.record_success("src")
    assert cb._entry("src").failure_count == 0


def test_success_closes_breaker():
    cb = CircuitBreaker(failure_threshold=1)
    cb.record_failure("src")
    cb.record_success("src")
    assert cb.state("src") == BreakerState.CLOSED


# ---------------------------------------------------------------------------
# Half-open transition
# ---------------------------------------------------------------------------

def test_open_transitions_to_half_open_after_timeout():
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=30.0)
    cb.record_failure("src")
    future = time.monotonic() + 31.0
    with patch("pipewatch.circuit_breaker.time.monotonic", return_value=future):
        assert cb.state("src") == BreakerState.HALF_OPEN
        assert cb.is_allowed("src") is True


# ---------------------------------------------------------------------------
# call() helper
# ---------------------------------------------------------------------------

def test_call_returns_result_on_success():
    cb = CircuitBreaker()
    result = cb.call("src", lambda: 42)
    assert result == 42


def test_call_records_failure_on_exception():
    cb = CircuitBreaker(failure_threshold=1)
    with pytest.raises(RuntimeError):
        cb.call("src", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    assert cb.state("src") == BreakerState.OPEN


def test_call_raises_collection_error_when_open():
    cb = CircuitBreaker(failure_threshold=1)
    cb.record_failure("src")
    with pytest.raises(CollectionError, match="Circuit breaker OPEN"):
        cb.call("src", lambda: None)


# ---------------------------------------------------------------------------
# reset()
# ---------------------------------------------------------------------------

def test_reset_closes_open_breaker():
    cb = CircuitBreaker(failure_threshold=1)
    cb.record_failure("src")
    cb.reset("src")
    assert cb.state("src") == BreakerState.CLOSED


def test_reset_unknown_source_is_safe():
    cb = CircuitBreaker()
    cb.reset("nonexistent")  # should not raise


# ---------------------------------------------------------------------------
# Source isolation
# ---------------------------------------------------------------------------

def test_different_sources_are_independent():
    cb = CircuitBreaker(failure_threshold=1)
    cb.record_failure("src_a")
    assert cb.state("src_a") == BreakerState.OPEN
    assert cb.state("src_b") == BreakerState.CLOSED
