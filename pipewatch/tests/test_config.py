"""Tests for pipewatch configuration loading."""

import textwrap
import pytest

from pipewatch.config import (
    PipewatchConfig,
    SourceConfig,
    load_config,
    _parse_config,
)


SAMPLE_RAW_CONFIG = {
    "poll_interval": 30,
    "sources": [
        {
            "name": "orders_db",
            "type": "postgres",
            "connection": {"host": "localhost", "port": 5432},
            "thresholds": {"row_count_min": 100},
        },
        {
            "name": "events_kafka",
            "type": "kafka",
            "connection": {"bootstrap_servers": "kafka:9092"},
        },
    ],
    "alert_channels": [{"type": "slack", "webhook": "https://hooks.slack.com/xxx"}],
}


def test_parse_config_returns_pipewatch_config():
    config = _parse_config(SAMPLE_RAW_CONFIG)
    assert isinstance(config, PipewatchConfig)


def test_parse_config_sources_count():
    config = _parse_config(SAMPLE_RAW_CONFIG)
    assert len(config.sources) == 2


def test_parse_config_source_fields():
    config = _parse_config(SAMPLE_RAW_CONFIG)
    src = config.sources[0]
    assert isinstance(src, SourceConfig)
    assert src.name == "orders_db"
    assert src.type == "postgres"
    assert src.connection["port"] == 5432
    assert src.thresholds["row_count_min"] == 100


def test_parse_config_source_defaults():
    config = _parse_config(SAMPLE_RAW_CONFIG)
    kafka_src = config.sources[1]
    assert kafka_src.thresholds == {}


def test_parse_config_poll_interval():
    config = _parse_config(SAMPLE_RAW_CONFIG)
    assert config.poll_interval == 30


def test_parse_config_default_poll_interval():
    config = _parse_config({})
    assert config.poll_interval == 60


def test_parse_config_empty():
    config = _parse_config({})
    assert config.sources == []
    assert config.alert_channels == []


def test_load_config_from_file(tmp_path):
    config_file = tmp_path / "pipewatch.yml"
    config_file.write_text(
        textwrap.dedent("""\
            poll_interval: 45
            sources:
              - name: test_src
                type: postgres
                connection:
                  host: db.example.com
        """)
    )
    config = load_config(str(config_file))
    assert config.poll_interval == 45
    assert config.sources[0].name == "test_src"


def test_load_config_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/pipewatch.yml")
