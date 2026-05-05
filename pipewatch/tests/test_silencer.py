"""Tests for pipewatch.silencer."""

from datetime import datetime, timedelta, timezone

import pytest

from pipewatch.silencer import AlertSilencer, SilenceRule


UTC = timezone.utc
NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
PAST = NOW - timedelta(hours=2)
FUTURE = NOW + timedelta(hours=2)


# ---------------------------------------------------------------------------
# SilenceRule.is_active
# ---------------------------------------------------------------------------

def test_rule_active_no_window():
    rule = SilenceRule(source_pattern="db*")
    assert rule.is_active(NOW) is True


def test_rule_active_within_window():
    rule = SilenceRule(source_pattern="db*", start=PAST, end=FUTURE)
    assert rule.is_active(NOW) is True


def test_rule_inactive_before_start():
    rule = SilenceRule(source_pattern="db*", start=FUTURE)
    assert rule.is_active(NOW) is False


def test_rule_inactive_after_end():
    rule = SilenceRule(source_pattern="db*", end=PAST)
    assert rule.is_active(NOW) is False


# ---------------------------------------------------------------------------
# SilenceRule.matches
# ---------------------------------------------------------------------------

def test_rule_matches_exact_source():
    rule = SilenceRule(source_pattern="postgres", metric_pattern="*")
    assert rule.matches("postgres", "row_count") is True


def test_rule_matches_wildcard_source():
    rule = SilenceRule(source_pattern="db_*")
    assert rule.matches("db_primary", "latency") is True
    assert rule.matches("cache", "latency") is False


def test_rule_matches_metric_pattern():
    rule = SilenceRule(source_pattern="*", metric_pattern="lag_*")
    assert rule.matches("kafka", "lag_consumer") is True
    assert rule.matches("kafka", "throughput") is False


# ---------------------------------------------------------------------------
# AlertSilencer.is_silenced
# ---------------------------------------------------------------------------

def test_is_silenced_matching_active_rule():
    silencer = AlertSilencer()
    silencer.add_rule(SilenceRule(source_pattern="db*", start=PAST, end=FUTURE))
    assert silencer.is_silenced("db_primary", "row_count", now=NOW) is True


def test_is_silenced_no_matching_rule():
    silencer = AlertSilencer()
    silencer.add_rule(SilenceRule(source_pattern="cache"))
    assert silencer.is_silenced("db_primary", "row_count", now=NOW) is False


def test_is_silenced_inactive_rule_not_applied():
    silencer = AlertSilencer()
    silencer.add_rule(SilenceRule(source_pattern="db*", end=PAST))
    assert silencer.is_silenced("db_primary", "row_count", now=NOW) is False


# ---------------------------------------------------------------------------
# AlertSilencer.active_rules
# ---------------------------------------------------------------------------

def test_active_rules_filters_expired():
    silencer = AlertSilencer()
    silencer.add_rule(SilenceRule(source_pattern="a", end=PAST))
    silencer.add_rule(SilenceRule(source_pattern="b", end=FUTURE))
    active = silencer.active_rules(now=NOW)
    assert len(active) == 1
    assert active[0].source_pattern == "b"


# ---------------------------------------------------------------------------
# AlertSilencer.purge_expired
# ---------------------------------------------------------------------------

def test_purge_expired_removes_old_rules():
    silencer = AlertSilencer()
    silencer.add_rule(SilenceRule(source_pattern="a", end=PAST))
    silencer.add_rule(SilenceRule(source_pattern="b", end=FUTURE))
    removed = silencer.purge_expired(now=NOW)
    assert removed == 1
    assert len(silencer.rules) == 1


def test_purge_expired_keeps_open_ended_rules():
    silencer = AlertSilencer()
    silencer.add_rule(SilenceRule(source_pattern="*"))  # no end
    removed = silencer.purge_expired(now=NOW)
    assert removed == 0
    assert len(silencer.rules) == 1
