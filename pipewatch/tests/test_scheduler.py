"""Tests for pipewatch.scheduler."""

import pytest
from pipewatch.scheduler import Scheduler, ScheduleEntry


# ---------------------------------------------------------------------------
# ScheduleEntry
# ---------------------------------------------------------------------------

def test_entry_is_due_at_zero():
    entry = ScheduleEntry(source_name="s", base_interval=60.0, last_run=0.0)
    assert entry.is_due(now=0.0) is True


def test_entry_not_due_before_interval():
    entry = ScheduleEntry(source_name="s", base_interval=60.0, last_run=1000.0)
    assert entry.is_due(now=1050.0) is False


def test_entry_due_exactly_at_interval():
    entry = ScheduleEntry(source_name="s", base_interval=60.0, last_run=1000.0)
    assert entry.is_due(now=1060.0) is True


def test_entry_mark_ran_updates_last_run():
    entry = ScheduleEntry(source_name="s", base_interval=30.0)
    entry.mark_ran(now=500.0)
    assert entry.last_run == 500.0


def test_entry_mark_ran_stores_jitter():
    entry = ScheduleEntry(source_name="s", base_interval=30.0)
    entry.mark_ran(now=500.0, jitter=5.0)
    assert entry.jitter == 5.0
    assert entry.next_run == pytest.approx(535.0)


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

def test_scheduler_default_interval_stored():
    sched = Scheduler(default_interval=120.0)
    assert sched.default_interval == 120.0


def test_scheduler_negative_interval_raises():
    with pytest.raises(ValueError):
        Scheduler(default_interval=-1.0)


def test_register_uses_default_interval():
    sched = Scheduler(default_interval=45.0)
    sched.register("db")
    assert sched.entry("db").base_interval == 45.0


def test_register_uses_custom_interval():
    sched = Scheduler(default_interval=60.0)
    sched.register("api", interval=10.0)
    assert sched.entry("api").base_interval == 10.0


def test_register_zero_interval_raises():
    sched = Scheduler()
    with pytest.raises(ValueError):
        sched.register("bad", interval=0.0)


def test_due_returns_all_at_start():
    sched = Scheduler(default_interval=60.0)
    sched.register("a")
    sched.register("b")
    due = sched.due(now=0.0)
    assert set(due) == {"a", "b"}


def test_due_excludes_recently_ran():
    sched = Scheduler(default_interval=60.0)
    sched.register("a")
    sched.register("b")
    sched.mark_ran("a", now=1000.0)
    due = sched.due(now=1030.0)
    assert "a" not in due
    assert "b" in due


def test_mark_ran_unknown_source_raises():
    sched = Scheduler()
    with pytest.raises(KeyError):
        sched.mark_ran("nonexistent", now=0.0)


def test_due_after_full_interval():
    sched = Scheduler(default_interval=60.0)
    sched.register("x")
    sched.mark_ran("x", now=1000.0)
    assert "x" not in sched.due(now=1059.0)
    assert "x" in sched.due(now=1060.0)


def test_multiple_sources_independent_schedules():
    sched = Scheduler(default_interval=60.0)
    sched.register("fast", interval=10.0)
    sched.register("slow", interval=120.0)
    sched.mark_ran("fast", now=1000.0)
    sched.mark_ran("slow", now=1000.0)
    due = sched.due(now=1015.0)
    assert "fast" in due
    assert "slow" not in due
