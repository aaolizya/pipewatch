"""Tests for pipewatch.exporter."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from pipewatch.exporter import export_csv, export_json, export_text
from pipewatch.metrics import AlertResult, Metric
from pipewatch.summary import PipelineSummary, SourceSummary


def _make_alert(level: str) -> AlertResult:
    ar = MagicMock(spec=AlertResult)
    ar.level = level
    return ar


def _make_result(level: str | None = None):
    r = MagicMock()
    r.alert_result = _make_alert(level) if level else None
    return r


def _make_source(name: str, *levels: str | None) -> SourceSummary:
    source = MagicMock(spec=SourceSummary)
    source.name = name
    source.results = [_make_result(lv) for lv in levels]
    source.healthy = all(lv is None for lv in levels)
    source.status = "ok" if source.healthy else ("critical" if "critical" in levels else "warning")
    return source


@pytest.fixture()
def summary() -> PipelineSummary:
    s1 = _make_source("db", None, None)
    s2 = _make_source("api", "warning", "critical")
    ps = MagicMock(spec=PipelineSummary)
    ps.sources = [s1, s2]
    ps.overall_status = "critical"
    return ps


# --- JSON ---

def test_export_json_is_valid_json(summary):
    result = export_json(summary)
    data = json.loads(result)  # must not raise
    assert isinstance(data, dict)


def test_export_json_contains_overall_status(summary):
    data = json.loads(export_json(summary))
    assert data["overall_status"] == "critical"


def test_export_json_source_counts(summary):
    data = json.loads(export_json(summary))
    api = next(s for s in data["sources"] if s["source"] == "api")
    assert api["warning_count"] == 1
    assert api["critical_count"] == 1


def test_export_json_has_exported_at(summary):
    data = json.loads(export_json(summary))
    assert "exported_at" in data


# --- CSV ---

def test_export_csv_has_header(summary):
    result = export_csv(summary)
    assert result.startswith("source,status")


def test_export_csv_row_count(summary):
    lines = [l for l in export_csv(summary).strip().splitlines() if l]
    assert len(lines) == 3  # header + 2 sources


def test_export_csv_contains_source_names(summary):
    result = export_csv(summary)
    assert "db" in result
    assert "api" in result


# --- Text ---

def test_export_text_contains_status(summary):
    result = export_text(summary)
    assert "CRITICAL" in result


def test_export_text_lists_sources(summary):
    result = export_text(summary)
    assert "db" in result
    assert "api" in result


def test_export_text_shows_counts(summary):
    result = export_text(summary)
    assert "warn=1" in result
    assert "crit=1" in result
