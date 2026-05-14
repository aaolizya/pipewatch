"""Tests for pipewatch.backpressure."""
import pytest

from pipewatch.backpressure import BackpressureController, BackpressureEntry


# ---------------------------------------------------------------------------
# BackpressureEntry
# ---------------------------------------------------------------------------

def test_entry_defaults():
    e = BackpressureEntry(source_name="src")
    assert e.current_depth == 0.0
    assert e.throttled is False


def test_entry_str_ok():
    e = BackpressureEntry(source_name="src")
    assert "OK" in str(e)
    assert "src" in str(e)


def test_entry_str_throttled():
    e = BackpressureEntry(source_name="src", high_watermark=100.0, low_watermark=50.0)
    e.update(100.0)
    assert "THROTTLED" in str(e)


def test_entry_negative_depth_raises():
    e = BackpressureEntry(source_name="src")
    with pytest.raises(ValueError):
        e.update(-1.0)


def test_entry_throttles_at_high_watermark():
    e = BackpressureEntry(source_name="src", high_watermark=100.0, low_watermark=50.0)
    e.update(100.0)
    assert e.throttled is True


def test_entry_not_throttled_below_high_watermark():
    e = BackpressureEntry(source_name="src", high_watermark=100.0, low_watermark=50.0)
    e.update(99.9)
    assert e.throttled is False


def test_entry_releases_at_low_watermark():
    e = BackpressureEntry(source_name="src", high_watermark=100.0, low_watermark=50.0)
    e.update(100.0)
    assert e.throttled is True
    e.update(50.0)
    assert e.throttled is False


def test_entry_stays_throttled_between_watermarks():
    e = BackpressureEntry(source_name="src", high_watermark=100.0, low_watermark=50.0)
    e.update(100.0)
    e.update(75.0)  # between watermarks — should remain throttled
    assert e.throttled is True


def test_pressure_ratio():
    e = BackpressureEntry(source_name="src", high_watermark=200.0, low_watermark=100.0)
    e.update(100.0)
    assert e.pressure_ratio == pytest.approx(0.5)


def test_pressure_ratio_zero_hwm():
    e = BackpressureEntry(source_name="src", high_watermark=0.0)
    assert e.pressure_ratio == 0.0


# ---------------------------------------------------------------------------
# BackpressureController
# ---------------------------------------------------------------------------

def test_controller_invalid_high_watermark_raises():
    with pytest.raises(ValueError):
        BackpressureController(high_watermark=0)


def test_controller_invalid_low_watermark_raises():
    with pytest.raises(ValueError):
        BackpressureController(high_watermark=100.0, low_watermark=-1.0)


def test_controller_low_ge_high_raises():
    with pytest.raises(ValueError):
        BackpressureController(high_watermark=100.0, low_watermark=100.0)


def test_controller_new_source_not_throttled():
    ctrl = BackpressureController()
    assert ctrl.is_throttled("src") is False


def test_controller_record_triggers_throttle():
    ctrl = BackpressureController(high_watermark=100.0, low_watermark=50.0)
    ctrl.record("src", 100.0)
    assert ctrl.is_throttled("src") is True


def test_controller_throttled_sources_returns_list():
    ctrl = BackpressureController(high_watermark=100.0, low_watermark=50.0)
    ctrl.record("a", 100.0)
    ctrl.record("b", 10.0)
    assert ctrl.throttled_sources() == ["a"]


def test_controller_pressure_ratio_delegated():
    ctrl = BackpressureController(high_watermark=200.0, low_watermark=50.0)
    ctrl.record("src", 100.0)
    assert ctrl.pressure_ratio("src") == pytest.approx(0.5)


def test_controller_reset_clears_state():
    ctrl = BackpressureController(high_watermark=100.0, low_watermark=50.0)
    ctrl.record("src", 100.0)
    assert ctrl.is_throttled("src") is True
    ctrl.reset("src")
    assert ctrl.is_throttled("src") is False


def test_controller_multiple_sources_independent():
    ctrl = BackpressureController(high_watermark=100.0, low_watermark=50.0)
    ctrl.record("x", 100.0)
    ctrl.record("y", 10.0)
    assert ctrl.is_throttled("x") is True
    assert ctrl.is_throttled("y") is False
