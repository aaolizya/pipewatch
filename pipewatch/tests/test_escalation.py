"""Tests for pipewatch.escalation."""
import pytest
from pipewatch.escalation import AlertEscalator, EscalationPolicy


# ---------------------------------------------------------------------------
# EscalationPolicy validation
# ---------------------------------------------------------------------------

def test_policy_invalid_threshold_raises():
    with pytest.raises(ValueError, match="threshold"):
        EscalationPolicy(threshold=0)


def test_policy_invalid_duration_raises():
    with pytest.raises(ValueError, match="min_duration_seconds"):
        EscalationPolicy(min_duration_seconds=-1)


def test_policy_defaults_are_sensible():
    p = EscalationPolicy()
    assert p.threshold >= 1
    assert p.min_duration_seconds >= 0


# ---------------------------------------------------------------------------
# AlertEscalator – basic recording
# ---------------------------------------------------------------------------

def test_first_alert_not_escalated():
    esc = AlertEscalator(EscalationPolicy(threshold=3, min_duration_seconds=0))
    result = esc.record_alert("src", "lag", now=0.0)
    assert result is False


def test_escalates_after_threshold_and_duration():
    policy = EscalationPolicy(threshold=3, min_duration_seconds=60)
    esc = AlertEscalator(policy)
    esc.record_alert("src", "lag", now=0.0)
    esc.record_alert("src", "lag", now=30.0)
    result = esc.record_alert("src", "lag", now=90.0)
    assert result is True


def test_no_escalation_before_duration():
    policy = EscalationPolicy(threshold=2, min_duration_seconds=120)
    esc = AlertEscalator(policy)
    esc.record_alert("src", "lag", now=0.0)
    result = esc.record_alert("src", "lag", now=10.0)  # only 10 s elapsed
    assert result is False


def test_no_escalation_before_threshold():
    policy = EscalationPolicy(threshold=5, min_duration_seconds=0)
    esc = AlertEscalator(policy)
    for i in range(4):
        result = esc.record_alert("src", "lag", now=float(i * 10))
    assert result is False


# ---------------------------------------------------------------------------
# Recovery resets state
# ---------------------------------------------------------------------------

def test_recovery_resets_failure_count():
    esc = AlertEscalator(EscalationPolicy(threshold=2, min_duration_seconds=0))
    esc.record_alert("src", "lag", now=0.0)
    esc.record_alert("src", "lag", now=1.0)
    esc.record_recovery("src", "lag")
    assert esc.failure_count("src", "lag") == 0


def test_recovery_clears_escalation_flag():
    esc = AlertEscalator(EscalationPolicy(threshold=1, min_duration_seconds=0))
    esc.record_alert("src", "lag", now=0.0)
    assert esc.is_escalated("src", "lag") is True
    esc.record_recovery("src", "lag")
    assert esc.is_escalated("src", "lag") is False


# ---------------------------------------------------------------------------
# Multiple metrics are tracked independently
# ---------------------------------------------------------------------------

def test_different_metrics_tracked_independently():
    policy = EscalationPolicy(threshold=2, min_duration_seconds=0)
    esc = AlertEscalator(policy)
    esc.record_alert("src", "lag", now=0.0)
    esc.record_alert("src", "lag", now=1.0)
    # second metric has only one failure
    result = esc.record_alert("src", "error_rate", now=0.0)
    assert result is False
    assert esc.is_escalated("src", "lag") is True
    assert esc.is_escalated("src", "error_rate") is False


def test_failure_count_increments():
    esc = AlertEscalator()
    for i in range(5):
        esc.record_alert("src", "m", now=float(i))
    assert esc.failure_count("src", "m") == 5
