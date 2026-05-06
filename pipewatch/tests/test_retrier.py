"""Tests for pipewatch.retrier."""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from pipewatch.retrier import RetryPolicy, RetryResult, with_retry


# ---------------------------------------------------------------------------
# RetryPolicy.delay_for
# ---------------------------------------------------------------------------

def test_delay_for_first_attempt_is_zero():
    policy = RetryPolicy(base_delay=2.0)
    assert policy.delay_for(0) == 0.0


def test_delay_for_second_attempt_is_base():
    policy = RetryPolicy(base_delay=1.0, backoff_factor=2.0)
    assert policy.delay_for(1) == 1.0


def test_delay_for_applies_backoff():
    policy = RetryPolicy(base_delay=1.0, backoff_factor=2.0)
    assert policy.delay_for(2) == 2.0
    assert policy.delay_for(3) == 4.0


def test_delay_for_capped_at_max_delay():
    policy = RetryPolicy(base_delay=10.0, backoff_factor=10.0, max_delay=15.0)
    assert policy.delay_for(3) == 15.0


# ---------------------------------------------------------------------------
# with_retry — success paths
# ---------------------------------------------------------------------------

def test_with_retry_succeeds_first_attempt():
    fn = MagicMock(return_value=42)
    result = with_retry(fn, _sleep=lambda _: None)
    assert result.succeeded is True
    assert result.value == 42
    assert result.attempts == 1
    fn.assert_called_once()


def test_with_retry_succeeds_on_second_attempt():
    fn = MagicMock(side_effect=[ValueError("boom"), 99])
    policy = RetryPolicy(max_attempts=3, base_delay=0.0)
    result = with_retry(fn, policy, _sleep=lambda _: None)
    assert result.succeeded is True
    assert result.value == 99
    assert result.attempts == 2


def test_with_retry_returns_retry_result_type():
    result = with_retry(lambda: "ok", _sleep=lambda _: None)
    assert isinstance(result, RetryResult)


# ---------------------------------------------------------------------------
# with_retry — failure paths
# ---------------------------------------------------------------------------

def test_with_retry_raises_after_max_attempts():
    fn = MagicMock(side_effect=RuntimeError("always fails"))
    policy = RetryPolicy(max_attempts=3, base_delay=0.0)
    with pytest.raises(RuntimeError, match="always fails"):
        with_retry(fn, policy, _sleep=lambda _: None)
    assert fn.call_count == 3


def test_with_retry_non_retryable_exception_propagates_immediately():
    fn = MagicMock(side_effect=TypeError("type error"))
    policy = RetryPolicy(
        max_attempts=5,
        base_delay=0.0,
        retryable_exceptions=(ValueError,),
    )
    with pytest.raises(TypeError):
        with_retry(fn, policy, _sleep=lambda _: None)
    fn.assert_called_once()


def test_with_retry_uses_default_policy_when_none():
    calls = []
    def fn():
        calls.append(1)
        raise OSError("fail")

    with pytest.raises(OSError):
        with_retry(fn, _sleep=lambda _: None)
    assert len(calls) == 3  # default max_attempts


def test_with_retry_sleep_called_with_correct_delays():
    slept: list[float] = []
    fn = MagicMock(side_effect=[IOError(), IOError(), 1])
    policy = RetryPolicy(max_attempts=3, base_delay=2.0, backoff_factor=2.0)
    with_retry(fn, policy, _sleep=slept.append)
    # attempt 0 → 0s (not called), attempt 1 → 2s, attempt 2 → 4s
    assert slept == [2.0, 4.0]
