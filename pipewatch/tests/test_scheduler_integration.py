"""Integration tests: Scheduler wired with SourceConfig intervals."""

from unittest.mock import MagicMock
from pipewatch.scheduler import Scheduler
from pipewatch.config import SourceConfig


def _make_source(name: str, poll_interval: float = 60.0) -> SourceConfig:
    src = MagicMock(spec=SourceConfig)
    src.name = name
    src.poll_interval = poll_interval
    return src


def _build_scheduler(sources: list, default_interval: float = 60.0) -> Scheduler:
    sched = Scheduler(default_interval=default_interval)
    for src in sources:
        sched.register(src.name, interval=getattr(src, "poll_interval", None))
    return sched


def test_all_sources_due_on_first_tick():
    sources = [_make_source("db"), _make_source("api"), _make_source("cache")]
    sched = _build_scheduler(sources)
    due = sched.due(now=0.0)
    assert set(due) == {"db", "api", "cache"}


def test_per_source_interval_respected():
    sources = [
        _make_source("fast", poll_interval=15.0),
        _make_source("slow", poll_interval=300.0),
    ]
    sched = _build_scheduler(sources)
    for src in sources:
        sched.mark_ran(src.name, now=0.0)

    due_at_20 = sched.due(now=20.0)
    assert "fast" in due_at_20
    assert "slow" not in due_at_20

    due_at_300 = sched.due(now=300.0)
    assert "fast" in due_at_300
    assert "slow" in due_at_300


def test_simulate_two_poll_cycles():
    sources = [_make_source("m", poll_interval=30.0)]
    sched = _build_scheduler(sources)

    # cycle 1
    assert "m" in sched.due(now=0.0)
    sched.mark_ran("m", now=0.0)

    # between cycles
    assert "m" not in sched.due(now=15.0)

    # cycle 2
    assert "m" in sched.due(now=30.0)
    sched.mark_ran("m", now=30.0)

    # after cycle 2
    assert "m" not in sched.due(now=45.0)
    assert "m" in sched.due(now=60.0)


def test_jitter_delays_next_run():
    sources = [_make_source("j", poll_interval=60.0)]
    sched = _build_scheduler(sources)
    sched.mark_ran("j", now=0.0, jitter=10.0)

    assert "j" not in sched.due(now=60.0)
    assert "j" in sched.due(now=70.0)
