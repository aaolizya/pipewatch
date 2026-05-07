"""Tests for pipewatch.jitter."""

from __future__ import annotations

import pytest

from pipewatch.jitter import JitterPolicy, apply_jitter


# ---------------------------------------------------------------------------
# JitterPolicy construction
# ---------------------------------------------------------------------------

def test_invalid_max_jitter_raises():
    with pytest.raises(ValueError, match="max_jitter"):
        JitterPolicy(max_jitter=-1.0)


def test_invalid_strategy_raises():
    with pytest.raises(ValueError, match="strategy"):
        JitterPolicy(max_jitter=5.0, strategy="gaussian")


def test_invalid_proportion_raises():
    with pytest.raises(ValueError, match="proportion"):
        JitterPolicy(max_jitter=5.0, strategy="proportional", proportion=1.5)


# ---------------------------------------------------------------------------
# Uniform strategy
# ---------------------------------------------------------------------------

def test_uniform_jitter_within_bounds():
    policy = JitterPolicy(max_jitter=10.0, seed=42)
    for _ in range(50):
        result = policy.apply(30.0)
        assert 30.0 <= result <= 40.0


def test_zero_max_jitter_returns_base():
    policy = JitterPolicy(max_jitter=0.0, seed=0)
    assert policy.apply(60.0) == pytest.approx(60.0)


def test_uniform_jitter_deterministic_with_seed():
    p1 = JitterPolicy(max_jitter=5.0, seed=7)
    p2 = JitterPolicy(max_jitter=5.0, seed=7)
    assert p1.apply(20.0) == pytest.approx(p2.apply(20.0))


def test_negative_base_interval_raises():
    policy = JitterPolicy(max_jitter=5.0)
    with pytest.raises(ValueError, match="base_interval"):
        policy.apply(-1.0)


# ---------------------------------------------------------------------------
# Proportional strategy
# ---------------------------------------------------------------------------

def test_proportional_jitter_within_proportion():
    policy = JitterPolicy(max_jitter=999.0, strategy="proportional", proportion=0.1, seed=1)
    base = 100.0
    for _ in range(50):
        result = policy.apply(base)
        assert base <= result <= base + base * 0.1 + 1e-9


def test_proportional_zero_proportion_returns_base():
    policy = JitterPolicy(max_jitter=0.0, strategy="proportional", proportion=0.0, seed=0)
    assert policy.apply(45.0) == pytest.approx(45.0)


# ---------------------------------------------------------------------------
# Convenience wrapper
# ---------------------------------------------------------------------------

def test_apply_jitter_returns_float():
    result = apply_jitter(30.0, max_jitter=5.0, seed=99)
    assert isinstance(result, float)


def test_apply_jitter_within_range():
    result = apply_jitter(30.0, max_jitter=5.0, seed=3)
    assert 30.0 <= result <= 35.0


def test_apply_jitter_zero_max_returns_base():
    assert apply_jitter(60.0, max_jitter=0.0) == pytest.approx(60.0)
