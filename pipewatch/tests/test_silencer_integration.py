"""Integration tests: AlertSilencer interacting with AlertManager-style flow."""

from datetime import datetime, timedelta, timezone

from pipewatch.silencer import AlertSilencer, SilenceRule


UTC = timezone.utc
NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)


def _make_silencer(*rules: SilenceRule) -> AlertSilencer:
    s = AlertSilencer()
    for r in rules:
        s.add_rule(r)
    return s


def test_maintenance_window_suppresses_all_metrics_for_source():
    """A broad rule silences every metric on a source during a window."""
    silencer = _make_silencer(
        SilenceRule(
            source_pattern="warehouse",
            metric_pattern="*",
            start=NOW - timedelta(hours=1),
            end=NOW + timedelta(hours=1),
            reason="scheduled maintenance",
        )
    )
    assert silencer.is_silenced("warehouse", "row_count", now=NOW)
    assert silencer.is_silenced("warehouse", "latency_ms", now=NOW)
    assert not silencer.is_silenced("postgres", "row_count", now=NOW)


def test_multiple_rules_any_match_silences():
    """If any active rule matches, the alert is silenced."""
    silencer = _make_silencer(
        SilenceRule(source_pattern="cache", end=NOW - timedelta(seconds=1)),  # expired
        SilenceRule(source_pattern="cache", metric_pattern="hit_rate"),  # active, no window
    )
    assert silencer.is_silenced("cache", "hit_rate", now=NOW)
    assert not silencer.is_silenced("cache", "miss_rate", now=NOW)


def test_purge_then_check_no_longer_silenced():
    """After purging expired rules, previously silenced alerts are no longer suppressed."""
    silencer = _make_silencer(
        SilenceRule(
            source_pattern="kafka",
            end=NOW - timedelta(minutes=5),
        )
    )
    # Before purge: rule exists but is expired — already inactive
    assert not silencer.is_silenced("kafka", "lag", now=NOW)
    removed = silencer.purge_expired(now=NOW)
    assert removed == 1
    assert silencer.rules == []


def test_reason_preserved_on_rule():
    """Reason string is stored and accessible for reporting."""
    rule = SilenceRule(
        source_pattern="db*",
        reason="DB upgrade in progress",
    )
    silencer = _make_silencer(rule)
    active = silencer.active_rules(now=NOW)
    assert len(active) == 1
    assert active[0].reason == "DB upgrade in progress"


def test_empty_silencer_never_silences():
    silencer = AlertSilencer()
    assert not silencer.is_silenced("any_source", "any_metric", now=NOW)
    assert silencer.active_rules(now=NOW) == []
    assert silencer.purge_expired(now=NOW) == 0
