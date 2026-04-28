"""Metric collection from configured pipeline sources."""

from __future__ import annotations

import logging
import time
from typing import List, Optional

import requests

from pipewatch.config import SourceConfig
from pipewatch.metrics import Metric

logger = logging.getLogger(__name__)


class CollectionError(Exception):
    """Raised when a metric cannot be collected from a source."""


def collect_metric(source: SourceConfig, timeout: int = 10) -> Metric:
    """Fetch the current metric value from a single source.

    Args:
        source: The source configuration to collect from.
        timeout: HTTP request timeout in seconds.

    Returns:
        A :class:`~pipewatch.metrics.Metric` with the current value.

    Raises:
        CollectionError: If the request fails or the response is malformed.
    """
    try:
        response = requests.get(source.url, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        raise CollectionError(
            f"Failed to reach source '{source.name}': {exc}"
        ) from exc
    except ValueError as exc:
        raise CollectionError(
            f"Invalid JSON from source '{source.name}': {exc}"
        ) from exc

    raw_value = payload.get("value")
    if raw_value is None:
        raise CollectionError(
            f"Response from '{source.name}' missing 'value' key."
        )

    try:
        value = float(raw_value)
    except (TypeError, ValueError) as exc:
        raise CollectionError(
            f"Non-numeric value from '{source.name}': {raw_value!r}"
        ) from exc

    return Metric(
        name=source.name,
        value=value,
        unit=source.unit,
        timestamp=time.time(),
    )


def collect_all(
    sources: List[SourceConfig],
    timeout: int = 10,
) -> List[Metric]:
    """Collect metrics from every configured source.

    Sources that fail are skipped and a warning is logged.

    Args:
        sources: List of source configurations.
        timeout: Per-request timeout in seconds.

    Returns:
        List of successfully collected :class:`~pipewatch.metrics.Metric` objects.
    """
    metrics: List[Metric] = []
    for source in sources:
        try:
            metric = collect_metric(source, timeout=timeout)
            logger.debug("Collected %s", metric)
            metrics.append(metric)
        except CollectionError as exc:
            logger.warning("Skipping source '%s': %s", source.name, exc)
    return metrics
