"""Integration tests: AlertEscalator wired to realistic alert sequences."""
import time
from pipewatch.escalation import AlertEscalator, EscalationPolicy


def _run_alerts(esc, source, metric, count, start=0.0, step=30.0):
    """Fire *count* alerts spaced *step* seconds apart, return last result."""
    result = False
    for i in range(count):
        result = esc.record_alert(source, metric, now=start + i * step)
    return result


def test_escalation_then_recovery_then_re_escalation():
    """A metric that recovers and then fails again should re-escalate cleanly."""
    policy = EscalationPolicy(threshold=3, min_duration_seconds=60)
    esc = AlertEscalator(policy)

    escalated = _run_alerts(esc, "pipe-a", "lag", count=3, start=0.0, step=30.0)
    assert escalated is True

    esc.record_recovery("pipe-a", "lag")
    assert esc.is_escalated("pipe-a", "lag") is False

    # Re-fail: need threshold + duration again
    not_yet = _run_alerts(esc, "pipe-a", "lag", count=2, start=200.0, step=10.0)
    assert not_yet is False

    re_escalated = esc.record_alert("pipe-a", "lag", now=300.0)
    assert re_escalated is True


def test_multiple_sources_escalate_independently():
    policy = EscalationPolicy(threshold=2, min_duration_seconds=0)
    esc = AlertEscalator(policy)

    _run_alerts(esc, "source-1", "error_rate", count=2, start=0.0, step=1.0)
    _run_alerts(esc, "source-2", "error_rate", count=1, start=0.0, step=1.0)

    assert esc.is_escalated("source-1", "error_rate") is True
    assert esc.is_escalated("source-2", "error_rate") is False


def test_zero_duration_policy_escalates_quickly():
    """With min_duration_seconds=0 any threshold count triggers escalation."""
    policy = EscalationPolicy(threshold=2, min_duration_seconds=0)
    esc = AlertEscalator(policy)
    esc.record_alert("s", "m", now=0.0)
    result = esc.record_alert("s", "m", now=0.0)  # same timestamp is fine
    assert result is True


def test_recovery_of_unknown_metric_is_noop():
    """Calling record_recovery on a metric never seen should not raise."""
    esc = AlertEscalator()
    esc.record_recovery("ghost-source", "ghost-metric")  # must not raise
    assert esc.failure_count("ghost-source", "ghost-metric") == 0


def test_escalation_state_persists_across_many_alerts():
    """Once escalated, further alerts keep the escalated flag set."""
    policy = EscalationPolicy(threshold=2, min_duration_seconds=0)
    esc = AlertEscalator(policy)
    _run_alerts(esc, "s", "m", count=2, start=0.0, step=1.0)
    assert esc.is_escalated("s", "m") is True
    for i in range(10):
        esc.record_alert("s", "m", now=float(100 + i))
    assert esc.is_escalated("s", "m") is True
    assert esc.failure_count("s", "m") == 12
