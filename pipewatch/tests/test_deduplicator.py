"""Tests for pipewatch.deduplicator."""

import pytest

from pipewatch.deduplicator import AlertDeduplicator, DedupeEntry


# ---------------------------------------------------------------------------
# DedupeEntry
# ---------------------------------------------------------------------------

def test_dedupe_entry_key():
    entry = DedupeEntry(source="db", metric_name="lag", last_status="warning", last_value=1.5)
    assert entry.key == ("db", "lag")


# ---------------------------------------------------------------------------
# AlertDeduplicator.is_new
# ---------------------------------------------------------------------------

def test_is_new_first_time_returns_true():
    d = AlertDeduplicator()
    assert d.is_new("db", "lag", "warning", 1.5) is True


def test_is_new_same_status_returns_false():
    d = AlertDeduplicator()
    d.is_new("db", "lag", "warning", 1.5)
    assert d.is_new("db", "lag", "warning", 2.0) is False


def test_is_new_status_change_returns_true():
    d = AlertDeduplicator()
    d.is_new("db", "lag", "warning", 1.5)
    assert d.is_new("db", "lag", "critical", 5.0) is True


def test_is_new_recovery_to_ok_returns_true():
    d = AlertDeduplicator()
    d.is_new("db", "lag", "critical", 9.0)
    assert d.is_new("db", "lag", "ok", 0.1) is True


def test_is_new_different_metrics_are_independent():
    d = AlertDeduplicator()
    d.is_new("db", "lag", "warning", 1.5)
    # Different metric on same source should be treated as new
    assert d.is_new("db", "row_count", "warning", 100.0) is True


def test_is_new_different_sources_are_independent():
    d = AlertDeduplicator()
    d.is_new("db", "lag", "warning", 1.5)
    assert d.is_new("cache", "lag", "warning", 1.5) is True


def test_is_new_updates_value_on_duplicate():
    d = AlertDeduplicator()
    d.is_new("db", "lag", "warning", 1.5)
    d.is_new("db", "lag", "warning", 3.0)
    entry = d.last_entry("db", "lag")
    assert entry is not None
    assert entry.last_value == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# reset / reset_all / last_entry / __len__
# ---------------------------------------------------------------------------

def test_reset_clears_single_metric():
    d = AlertDeduplicator()
    d.is_new("db", "lag", "warning", 1.5)
    d.reset("db", "lag")
    assert d.last_entry("db", "lag") is None


def test_reset_unknown_key_is_safe():
    d = AlertDeduplicator()
    d.reset("nonexistent", "metric")  # should not raise


def test_reset_all_clears_everything():
    d = AlertDeduplicator()
    d.is_new("db", "lag", "warning", 1.5)
    d.is_new("cache", "hit_rate", "critical", 0.2)
    d.reset_all()
    assert len(d) == 0


def test_len_reflects_tracked_metrics():
    d = AlertDeduplicator()
    assert len(d) == 0
    d.is_new("db", "lag", "warning", 1.5)
    d.is_new("db", "row_count", "ok", 500.0)
    assert len(d) == 2
