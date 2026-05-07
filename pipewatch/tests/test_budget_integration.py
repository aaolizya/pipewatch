"""Integration tests: AlertBudget interacting with alerting pipeline."""
from datetime import datetime, timedelta

from pipewatch.budget import AlertBudget
from pipewatch.metrics import Metric, AlertResult


NOW = datetime(2024, 6, 1, 12, 0, 0)


def _make_result(source: str, status: str = "critical") -> AlertResult:
    m = Metric(
        source=source,
        name="queue_depth",
        value=99.0,
        unit="items",
        timestamp=NOW,
    )
    return AlertResult(metric=m, status=status, threshold=50.0)


def _fire_if_allowed(budget: AlertBudget, results, now: datetime):
    fired = []
    for r in results:
        if r.status != "ok" and budget.consume(r.metric.source, now=now):
            fired.append(r)
    return fired


def test_budget_limits_alert_volume():
    budget = AlertBudget(default_limit=3, default_window_seconds=3600.0)
    results = [_make_result("pipe-a") for _ in range(10)]
    fired = _fire_if_allowed(budget, results, NOW)
    assert len(fired) == 3
    assert budget.exhausted("pipe-a")


def test_budget_resets_allow_next_window():
    budget = AlertBudget(default_limit=2, default_window_seconds=60.0)
    results = [_make_result("pipe-b") for _ in range(5)]
    _fire_if_allowed(budget, results, NOW)
    assert budget.exhausted("pipe-b")

    next_window = NOW + timedelta(seconds=61)
    fired2 = _fire_if_allowed(budget, results, next_window)
    assert len(fired2) == 2


def test_per_source_budgets_independent():
    budget = AlertBudget(default_limit=2, default_window_seconds=3600.0)
    r_a = [_make_result("src-a") for _ in range(5)]
    r_b = [_make_result("src-b") for _ in range(5)]

    fired_a = _fire_if_allowed(budget, r_a, NOW)
    fired_b = _fire_if_allowed(budget, r_b, NOW)

    assert len(fired_a) == 2
    assert len(fired_b) == 2
    assert budget.exhausted("src-a")
    assert budget.exhausted("src-b")


def test_ok_results_do_not_consume_budget():
    budget = AlertBudget(default_limit=2, default_window_seconds=3600.0)
    ok_results = [_make_result("pipe-c", status="ok") for _ in range(10)]
    fired = _fire_if_allowed(budget, ok_results, NOW)
    assert fired == []
    assert budget.remaining("pipe-c") == 2
