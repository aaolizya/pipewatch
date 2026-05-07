"""Unit tests for pipewatch.budget."""
from datetime import datetime, timedelta

import pytest

from pipewatch.budget import AlertBudget, BudgetEntry


NOW = datetime(2024, 6, 1, 12, 0, 0)


# ---------------------------------------------------------------------------
# BudgetEntry
# ---------------------------------------------------------------------------

def test_entry_defaults():
    e = BudgetEntry(source="s", limit=5, window_seconds=60.0)
    assert e.fired == 0
    assert e.remaining == 5
    assert not e.exhausted()


def test_consume_allows_up_to_limit():
    e = BudgetEntry(source="s", limit=3, window_seconds=60.0)
    assert e.consume(now=NOW) is True
    assert e.consume(now=NOW) is True
    assert e.consume(now=NOW) is True
    assert e.remaining == 0


def test_consume_blocks_over_limit():
    e = BudgetEntry(source="s", limit=2, window_seconds=60.0)
    e.consume(now=NOW)
    e.consume(now=NOW)
    assert e.consume(now=NOW) is False


def test_consume_resets_after_window():
    e = BudgetEntry(source="s", limit=2, window_seconds=60.0)
    e.consume(now=NOW)
    e.consume(now=NOW)
    assert e.exhausted()
    later = NOW + timedelta(seconds=61)
    assert e.consume(now=later) is True
    assert e.fired == 1


def test_consume_does_not_reset_before_window():
    e = BudgetEntry(source="s", limit=1, window_seconds=120.0)
    e.consume(now=NOW)
    almost = NOW + timedelta(seconds=119)
    assert e.consume(now=almost) is False


# ---------------------------------------------------------------------------
# AlertBudget
# ---------------------------------------------------------------------------

def test_budget_default_limit():
    b = AlertBudget(default_limit=5, default_window_seconds=60.0)
    for _ in range(5):
        assert b.consume("src", now=NOW) is True
    assert b.consume("src", now=NOW) is False


def test_budget_set_limit_overrides_default():
    b = AlertBudget(default_limit=100)
    b.set_limit("src", limit=2, window_seconds=3600.0)
    assert b.consume("src", now=NOW) is True
    assert b.consume("src", now=NOW) is True
    assert b.consume("src", now=NOW) is False


def test_budget_remaining_decrements():
    b = AlertBudget(default_limit=4, default_window_seconds=60.0)
    assert b.remaining("src") == 4
    b.consume("src", now=NOW)
    assert b.remaining("src") == 3


def test_budget_exhausted_flag():
    b = AlertBudget(default_limit=1, default_window_seconds=60.0)
    assert not b.exhausted("src")
    b.consume("src", now=NOW)
    assert b.exhausted("src")


def test_budget_reset_clears_fired():
    b = AlertBudget(default_limit=1, default_window_seconds=60.0)
    b.consume("src", now=NOW)
    assert b.exhausted("src")
    b.reset("src")
    assert not b.exhausted("src")
    assert b.remaining("src") == 1


def test_budget_sources_independent():
    b = AlertBudget(default_limit=2, default_window_seconds=60.0)
    b.consume("a", now=NOW)
    b.consume("a", now=NOW)
    assert b.exhausted("a")
    assert not b.exhausted("b")
