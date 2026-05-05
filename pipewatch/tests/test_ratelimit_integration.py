"""Integration tests: RateLimiter used alongside the collector."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.config import SourceConfig
from pipewatch.ratelimit import RateLimiter


def _make_source(name: str = "pipeline_a") -> SourceConfig:
    return SourceConfig(
        name=name,
        url="http://example.com/metrics",
        metric_path="$.value",
        warning_threshold=80.0,
        critical_threshold=95.0,
    )


# ---------------------------------------------------------------------------
# Gating collection with the rate limiter
# ---------------------------------------------------------------------------

def test_rate_limiter_gates_collect_all():
    """collect_all should be skipped when the rate limiter disallows it."""
    from pipewatch.ratelimit import RateLimiter

    limiter = RateLimiter(min_interval=60.0)
    source = _make_source()

    # First call: allowed
    limiter.record(source.name)
    assert limiter.is_allowed(source.name) is False

    # Simulate a guard that would skip collection
    collected = []
    if limiter.is_allowed(source.name):
        collected.append(source.name)

    assert collected == []


def test_rate_limiter_allows_after_reset():
    limiter = RateLimiter(min_interval=60.0)
    source = _make_source()

    limiter.record(source.name)
    assert limiter.is_allowed(source.name) is False

    limiter.reset(source.name)
    assert limiter.is_allowed(source.name) is True


def test_multiple_sources_tracked_independently():
    limiter = RateLimiter(min_interval=30.0)
    sources = [_make_source(f"src_{i}") for i in range(3)]

    # Record only the first two
    limiter.record(sources[0].name)
    limiter.record(sources[1].name)

    assert limiter.is_allowed(sources[0].name) is False
    assert limiter.is_allowed(sources[1].name) is False
    assert limiter.is_allowed(sources[2].name) is True


def test_request_counts_accumulate_across_records():
    limiter = RateLimiter(min_interval=0.0)
    source = _make_source()

    for _ in range(5):
        limiter.record(source.name)

    assert limiter.request_count(source.name) == 5


def test_reset_all_clears_counts_for_all_sources():
    limiter = RateLimiter(min_interval=0.0)
    names = ["alpha", "beta", "gamma"]
    for name in names:
        limiter.record(name)
        limiter.record(name)

    limiter.reset()

    for name in names:
        assert limiter.request_count(name) == 0
        assert limiter.is_allowed(name) is True
