"""Integration tests: exporter round-trips through real summary objects."""

from __future__ import annotations

import csv
import io
import json

import pytest

from pipewatch.exporter import export_csv, export_json, export_text
from pipewatch.metrics import AlertResult, Metric
from pipewatch.summary import PipelineSummary, SourceSummary


def _metric(name: str, value: float) -> Metric:
    return Metric(source="svc", name=name, value=value, unit="ms")


@pytest.fixture()
def real_summary() -> PipelineSummary:
    """Build a PipelineSummary using real dataclasses."""
    from pipewatch.alerting import evaluate_all
    from pipewatch.config import SourceConfig

    cfg = SourceConfig(
        name="svc",
        url="http://example.com/metrics",
        metrics={"latency": {"warning": 200, "critical": 500}},
    )
    metrics = [
        _metric("latency", 150.0),
    ]
    results = evaluate_all(metrics, cfg)
    source_summary = SourceSummary(name="svc", results=results)
    return PipelineSummary(sources=[source_summary])


def test_json_round_trip(real_summary):
    raw = export_json(real_summary)
    data = json.loads(raw)
    assert data["sources"][0]["source"] == "svc"
    assert data["sources"][0]["metric_count"] == 1


def test_csv_parseable(real_summary):
    raw = export_csv(real_summary)
    reader = csv.DictReader(io.StringIO(raw))
    rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["source"] == "svc"


def test_text_overall_status_present(real_summary):
    result = export_text(real_summary)
    assert "Pipeline Status:" in result


def test_json_healthy_source(real_summary):
    data = json.loads(export_json(real_summary))
    assert data["sources"][0]["healthy"] is True
    assert data["sources"][0]["critical_count"] == 0
