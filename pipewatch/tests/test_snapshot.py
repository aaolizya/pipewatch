"""Tests for pipewatch.snapshot."""
import json
import os

import pytest

from pipewatch.snapshot import Snapshot, load_snapshot, save_snapshot


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_snapshot(**overrides) -> Snapshot:
    defaults = dict(
        captured_at="2024-01-15T12:00:00+00:00",
        overall_status="ok",
        source_count=3,
        healthy_count=2,
        warning_count=1,
        critical_count=0,
        sources=[
            {"name": "db", "status": "ok", "metric_count": 4},
            {"name": "api", "status": "warning", "metric_count": 2},
            {"name": "queue", "status": "ok", "metric_count": 1},
        ],
    )
    defaults.update(overrides)
    return Snapshot(**defaults)


# ---------------------------------------------------------------------------
# Snapshot dataclass
# ---------------------------------------------------------------------------

def test_snapshot_to_dict_keys():
    snap = _make_snapshot()
    d = snap.to_dict()
    assert set(d.keys()) == {
        "captured_at", "overall_status", "source_count",
        "healthy_count", "warning_count", "critical_count", "sources",
    }


def test_snapshot_to_dict_values():
    snap = _make_snapshot(overall_status="critical", critical_count=1)
    d = snap.to_dict()
    assert d["overall_status"] == "critical"
    assert d["critical_count"] == 1


def test_snapshot_sources_preserved():
    snap = _make_snapshot()
    assert len(snap.to_dict()["sources"]) == 3
    assert snap.to_dict()["sources"][0]["name"] == "db"


# ---------------------------------------------------------------------------
# save / load round-trip
# ---------------------------------------------------------------------------

def test_save_creates_file(tmp_path):
    snap = _make_snapshot()
    dest = str(tmp_path / "snapshots" / "snap.json")
    save_snapshot(snap, dest)
    assert os.path.isfile(dest)


def test_save_valid_json(tmp_path):
    snap = _make_snapshot()
    dest = str(tmp_path / "snap.json")
    save_snapshot(snap, dest)
    with open(dest) as fh:
        data = json.load(fh)
    assert data["source_count"] == 3


def test_load_snapshot_round_trip(tmp_path):
    snap = _make_snapshot(overall_status="warning", warning_count=2)
    dest = str(tmp_path / "snap.json")
    save_snapshot(snap, dest)
    loaded = load_snapshot(dest)
    assert loaded.overall_status == "warning"
    assert loaded.warning_count == 2
    assert loaded.source_count == 3


def test_load_snapshot_sources(tmp_path):
    snap = _make_snapshot()
    dest = str(tmp_path / "snap.json")
    save_snapshot(snap, dest)
    loaded = load_snapshot(dest)
    assert isinstance(loaded.sources, list)
    assert loaded.sources[1]["name"] == "api"


def test_load_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_snapshot(str(tmp_path / "nonexistent.json"))
