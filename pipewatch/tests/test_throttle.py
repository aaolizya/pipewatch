"""Tests for pipewatch.throttle."""

import time

import pytest

from pipewatch.throttle import AlertThrottle, ThrottleEntry


# ---------------------------------------------------------------------------
# ThrottleEntry
# ---------------------------------------------------------------------------

def test_throttle_entry_defaults():
    entry = ThrottleEntry(last_notified_at=1000.0)
    assert entry.count == 1
    assert entry.last_notified_at == 1000.0


# ---------------------------------------------------------------------------
# AlertThrottle.should_notify
# ---------------------------------------------------------------------------

def test_should_notify_first_time():
    throttle = AlertThrottle(cooldown_seconds=60)
    assert throttle.should_notify("src", "latency") is True


def test_should_notify_false_within_cooldown():
    throttle = AlertThrottle(cooldown_seconds=300)
    throttle.record("src", "latency")
    assert throttle.should_notify("src", "latency") is False


def test_should_notify_true_after_cooldown(monkeypatch):
    throttle = AlertThrottle(cooldown_seconds=1)
    throttle.record("src", "latency")

    # Advance monotonic time beyond cooldown
    original = time.monotonic
    monkeypatch.setattr(time, "monotonic", lambda: original() + 2)

    assert throttle.should_notify("src", "latency") is True


def test_should_notify_different_metrics_independent():
    throttle = AlertThrottle(cooldown_seconds=300)
    throttle.record("src", "latency")
    assert throttle.should_notify("src", "error_rate") is True


# ---------------------------------------------------------------------------
# AlertThrottle.record
# ---------------------------------------------------------------------------

def test_record_increments_count():
    throttle = AlertThrottle(cooldown_seconds=0)
    throttle.record("src", "latency")
    throttle.record("src", "latency")
    assert throttle.notification_count("src", "latency") == 2


def test_notification_count_zero_before_record():
    throttle = AlertThrottle()
    assert throttle.notification_count("src", "latency") == 0


# ---------------------------------------------------------------------------
# AlertThrottle.reset
# ---------------------------------------------------------------------------

def test_reset_specific_metric():
    throttle = AlertThrottle(cooldown_seconds=300)
    throttle.record("src", "latency")
    throttle.reset("src", "latency")
    assert throttle.should_notify("src", "latency") is True


def test_reset_source_clears_all_metrics():
    throttle = AlertThrottle(cooldown_seconds=300)
    throttle.record("src", "latency")
    throttle.record("src", "error_rate")
    throttle.reset("src")
    assert throttle.should_notify("src", "latency") is True
    assert throttle.should_notify("src", "error_rate") is True


def test_reset_source_does_not_affect_other_sources():
    throttle = AlertThrottle(cooldown_seconds=300)
    throttle.record("src_a", "latency")
    throttle.record("src_b", "latency")
    throttle.reset("src_a")
    assert throttle.should_notify("src_b", "latency") is False


def test_reset_nonexistent_key_is_safe():
    throttle = AlertThrottle()
    throttle.reset("ghost", "metric")  # should not raise
