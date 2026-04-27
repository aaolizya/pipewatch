"""Configuration loading and validation for pipewatch."""

import os
from dataclasses import dataclass, field
from typing import Any

import yaml


@dataclass
class SourceConfig:
    name: str
    type: str
    connection: dict = field(default_factory=dict)
    thresholds: dict = field(default_factory=dict)


@dataclass
class PipewatchConfig:
    sources: list[SourceConfig] = field(default_factory=list)
    alert_channels: list[dict] = field(default_factory=list)
    poll_interval: int = 60


DEFAULT_CONFIG_PATHS = [
    "pipewatch.yml",
    "pipewatch.yaml",
    os.path.expanduser("~/.config/pipewatch/config.yml"),
]


def load_config(path: str | None = None) -> PipewatchConfig:
    """Load configuration from a YAML file.

    Args:
        path: Explicit path to config file. Falls back to default locations.

    Returns:
        Parsed PipewatchConfig instance.

    Raises:
        FileNotFoundError: If no config file is found.
        ValueError: If the config file is malformed.
    """
    config_path = path or _find_config_file()

    with open(config_path, "r") as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}

    return _parse_config(raw)


def _find_config_file() -> str:
    for candidate in DEFAULT_CONFIG_PATHS:
        if os.path.isfile(candidate):
            return candidate
    raise FileNotFoundError(
        "No pipewatch config found. Checked: " + ", ".join(DEFAULT_CONFIG_PATHS)
    )


def _parse_config(raw: dict[str, Any]) -> PipewatchConfig:
    sources = [
        SourceConfig(
            name=s["name"],
            type=s["type"],
            connection=s.get("connection", {}),
            thresholds=s.get("thresholds", {}),
        )
        for s in raw.get("sources", [])
    ]

    return PipewatchConfig(
        sources=sources,
        alert_channels=raw.get("alert_channels", []),
        poll_interval=int(raw.get("poll_interval", 60)),
    )
