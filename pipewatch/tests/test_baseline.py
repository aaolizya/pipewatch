"""Tests for pipewatch.baseline."""

import json
import os
import pytest

from pipewatch.baseline import BaselineEntry, BaselineStore


# ---------------------------------------------------------------------------
# BaselineEntry
# ---------------------------------------------------------------------------

def test_deviation_pct_exact_match():
    entry = BaselineEntry(source="s", metric_name="m", expected_value=100.0)
    assert entry.deviation_pct(100.0) == pytest.approx(0.0)


def test_deviation_pct_above():
    entry = BaselineEntry(source="s", metric_name="m", expected_value=100.0)
    assert entry.deviation_pct(115.0) == pytest.approx(15.0)


def test_deviation_pct_below():
    entry = BaselineEntry(source="s", metric_name="m", expected_value=200.0)
    assert entry.deviation_pct(180.0) == pytest.approx(10.0)


def test_deviation_pct_zero_expected_nonzero_observed():
    entry = BaselineEntry(source="s", metric_name="m", expected_value=0.0)
    assert entry.deviation_pct(5.0) == float("inf")


def test_deviation_pct_both_zero():
    entry = BaselineEntry(source="s", metric_name="m", expected_value=0.0)
    assert entry.deviation_pct(0.0) == pytest.approx(0.0)


def test_within_tolerance_true():
    entry = BaselineEntry(source="s", metric_name="m", expected_value=100.0, tolerance_pct=10.0)
    assert entry.is_within_tolerance(105.0) is True


def test_within_tolerance_false():
    entry = BaselineEntry(source="s", metric_name="m", expected_value=100.0, tolerance_pct=10.0)
    assert entry.is_within_tolerance(120.0) is False


def test_within_tolerance_exact_boundary():
    entry = BaselineEntry(source="s", metric_name="m", expected_value=100.0, tolerance_pct=10.0)
    assert entry.is_within_tolerance(110.0) is True


# ---------------------------------------------------------------------------
# BaselineStore – in-memory operations
# ---------------------------------------------------------------------------

def test_set_and_get():
    store = BaselineStore()
    store.set("pipeline_a", "row_count", 1000.0)
    entry = store.get("pipeline_a", "row_count")
    assert entry is not None
    assert entry.expected_value == 1000.0


def test_get_missing_returns_none():
    store = BaselineStore()
    assert store.get("pipeline_a", "missing") is None


def test_remove_existing_returns_true():
    store = BaselineStore()
    store.set("src", "metric", 50.0)
    assert store.remove("src", "metric") is True
    assert store.get("src", "metric") is None


def test_remove_missing_returns_false():
    store = BaselineStore()
    assert store.remove("src", "ghost") is False


def test_all_entries_count():
    store = BaselineStore()
    store.set("s1", "m1", 10.0)
    store.set("s1", "m2", 20.0)
    store.set("s2", "m1", 30.0)
    assert len(store.all_entries()) == 3


def test_overwrite_updates_value():
    store = BaselineStore()
    store.set("src", "metric", 100.0)
    store.set("src", "metric", 200.0)
    assert store.get("src", "metric").expected_value == 200.0


# ---------------------------------------------------------------------------
# BaselineStore – persistence
# ---------------------------------------------------------------------------

def test_save_creates_valid_json(tmp_path):
    path = str(tmp_path / "baselines.json")
    store = BaselineStore()
    store.set("src", "m", 42.0, tolerance_pct=5.0)
    store.save(path)
    assert os.path.exists(path)
    with open(path) as fh:
        data = json.load(fh)
    assert len(data) == 1
    assert data[0]["expected_value"] == 42.0
    assert data[0]["tolerance_pct"] == 5.0


def test_load_round_trips(tmp_path):
    path = str(tmp_path / "baselines.json")
    store = BaselineStore()
    store.set("pipeline_x", "latency", 250.0, tolerance_pct=15.0)
    store.save(path)
    loaded = BaselineStore.load(path)
    entry = loaded.get("pipeline_x", "latency")
    assert entry is not None
    assert entry.expected_value == 250.0
    assert entry.tolerance_pct == 15.0


def test_load_missing_file_returns_empty(tmp_path):
    path = str(tmp_path / "nonexistent.json")
    store = BaselineStore.load(path)
    assert store.all_entries() == []
