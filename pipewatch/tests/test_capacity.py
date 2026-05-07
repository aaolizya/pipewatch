"""Tests for pipewatch.capacity."""
import pytest

from pipewatch.capacity import (
    CapacityResult,
    CapacityStore,
    compute_capacity,
)


# ---------------------------------------------------------------------------
# compute_capacity
# ---------------------------------------------------------------------------

def test_compute_capacity_returns_result():
    result = compute_capacity("db", "connections", 70.0, 100.0)
    assert isinstance(result, CapacityResult)


def test_compute_capacity_headroom_value():
    result = compute_capacity("db", "connections", 70.0, 100.0)
    assert result.headroom == pytest.approx(30.0)


def test_compute_capacity_headroom_pct():
    result = compute_capacity("db", "connections", 70.0, 100.0)
    assert result.headroom_pct == pytest.approx(30.0)


def test_compute_capacity_not_at_risk_above_threshold():
    result = compute_capacity("db", "connections", 70.0, 100.0, risk_threshold_pct=20.0)
    assert result.at_risk is False


def test_compute_capacity_at_risk_below_threshold():
    result = compute_capacity("db", "connections", 90.0, 100.0, risk_threshold_pct=20.0)
    assert result.at_risk is True


def test_compute_capacity_exactly_at_threshold_is_not_at_risk():
    # headroom_pct == risk_threshold_pct means we are exactly at the boundary
    result = compute_capacity("db", "connections", 80.0, 100.0, risk_threshold_pct=20.0)
    assert result.at_risk is False


def test_compute_capacity_over_ceiling_clamped_headroom_pct():
    result = compute_capacity("db", "connections", 110.0, 100.0)
    assert result.headroom_pct == pytest.approx(0.0)
    assert result.at_risk is True


def test_compute_capacity_zero_ceiling_raises():
    with pytest.raises(ValueError, match="ceiling must be positive"):
        compute_capacity("db", "connections", 10.0, 0.0)


def test_compute_capacity_negative_ceiling_raises():
    with pytest.raises(ValueError, match="ceiling must be positive"):
        compute_capacity("db", "connections", 10.0, -5.0)


def test_compute_capacity_invalid_threshold_raises():
    with pytest.raises(ValueError, match="risk_threshold_pct"):
        compute_capacity("db", "connections", 10.0, 100.0, risk_threshold_pct=150.0)


def test_capacity_result_str_ok():
    result = compute_capacity("db", "connections", 60.0, 100.0)
    assert "OK" in str(result)
    assert "db/connections" in str(result)


def test_capacity_result_str_at_risk():
    result = compute_capacity("db", "connections", 95.0, 100.0)
    assert "AT RISK" in str(result)


# ---------------------------------------------------------------------------
# CapacityStore
# ---------------------------------------------------------------------------

def test_store_evaluate_returns_none_without_ceiling():
    store = CapacityStore()
    assert store.evaluate("db", "connections", 50.0) is None


def test_store_evaluate_returns_result_after_set():
    store = CapacityStore()
    store.set_ceiling("db", "connections", 100.0)
    result = store.evaluate("db", "connections", 50.0)
    assert isinstance(result, CapacityResult)
    assert result.headroom_pct == pytest.approx(50.0)


def test_store_set_ceiling_overrides_previous():
    store = CapacityStore()
    store.set_ceiling("db", "connections", 100.0)
    store.set_ceiling("db", "connections", 200.0)
    result = store.evaluate("db", "connections", 100.0)
    assert result.ceiling == pytest.approx(200.0)


def test_store_registered_keys_returns_all():
    store = CapacityStore()
    store.set_ceiling("db", "connections", 100.0)
    store.set_ceiling("cache", "memory_mb", 512.0)
    keys = store.registered_keys()
    assert "db::connections" in keys
    assert "cache::memory_mb" in keys


def test_store_custom_risk_threshold_propagated():
    store = CapacityStore(risk_threshold_pct=50.0)
    store.set_ceiling("db", "connections", 100.0)
    result = store.evaluate("db", "connections", 60.0)
    # 40% headroom < 50% threshold => at risk
    assert result.at_risk is True
