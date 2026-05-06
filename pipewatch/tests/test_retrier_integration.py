"""Integration tests: retrier wired with collector-style callables."""

from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock

from pipewatch.retrier import RetryPolicy, with_retry
from pipewatch.collector import CollectionError


def _make_flaky_collector(fail_times: int, final_value: float):
    """Return a callable that raises CollectionError *fail_times* then returns *final_value*."""
    calls = []

    def collect():
        calls.append(1)
        if len(calls) <= fail_times:
            raise CollectionError(f"transient failure #{len(calls)}")
        return final_value

    return collect


def test_flaky_collector_recovers_within_attempts():
    collector = _make_flaky_collector(fail_times=2, final_value=3.14)
    policy = RetryPolicy(
        max_attempts=5,
        base_delay=0.0,
        retryable_exceptions=(CollectionError,),
    )
    result = with_retry(collector, policy, _sleep=lambda _: None)
    assert result.succeeded is True
    assert result.value == pytest.approx(3.14)
    assert result.attempts == 3


def test_flaky_collector_exhausts_all_attempts():
    collector = _make_flaky_collector(fail_times=10, final_value=0.0)
    policy = RetryPolicy(
        max_attempts=3,
        base_delay=0.0,
        retryable_exceptions=(CollectionError,),
    )
    with pytest.raises(CollectionError):
        with_retry(collector, policy, _sleep=lambda _: None)


def test_non_collection_error_not_retried():
    """A ValueError (not CollectionError) should propagate on the first attempt."""
    fn = MagicMock(side_effect=ValueError("unexpected"))
    policy = RetryPolicy(
        max_attempts=5,
        base_delay=0.0,
        retryable_exceptions=(CollectionError,),
    )
    with pytest.raises(ValueError):
        with_retry(fn, policy, _sleep=lambda _: None)
    fn.assert_called_once()


def test_sleep_not_called_on_immediate_success():
    slept: list[float] = []
    with_retry(lambda: 1, _sleep=slept.append)
    assert slept == []


def test_total_delay_respects_max_delay_cap():
    slept: list[float] = []
    fn = MagicMock(side_effect=[OSError(), OSError(), OSError(), 0])
    policy = RetryPolicy(
        max_attempts=4,
        base_delay=5.0,
        backoff_factor=10.0,
        max_delay=8.0,
    )
    with_retry(fn, policy, _sleep=slept.append)
    assert all(d <= 8.0 for d in slept)
    assert len(slept) == 3  # attempts 1, 2, 3 each sleep before running
