"""Poll loop that ties collection and alerting together."""

from __future__ import annotations

import logging
import time
from typing import Optional

from pipewatch.alerting import evaluate_all, format_report
from pipewatch.collector import collect_all
from pipewatch.config import PipewatchConfig

logger = logging.getLogger(__name__)


def run_once(config: PipewatchConfig) -> str:
    """Collect metrics and evaluate alerts for a single poll cycle.

    Args:
        config: Loaded pipewatch configuration.

    Returns:
        Human-readable report string.
    """
    logger.info("Collecting metrics from %d source(s)...", len(config.sources))
    metrics = collect_all(config.sources)
    results = evaluate_all(metrics, config.sources)
    report = format_report(results)
    return report


def run_loop(
    config: PipewatchConfig,
    poll_interval: Optional[int] = None,
    max_iterations: Optional[int] = None,
) -> None:
    """Continuously poll all sources and print reports.

    Args:
        config: Loaded pipewatch configuration.
        poll_interval: Seconds between polls; defaults to ``config.poll_interval``.
        max_iterations: Stop after this many cycles (useful for testing).
    """
    interval = poll_interval if poll_interval is not None else config.poll_interval
    iteration = 0

    logger.info("Starting pipewatch poll loop (interval=%ds).", interval)
    while True:
        try:
            report = run_once(config)
            print(report)
        except Exception as exc:  # pragma: no cover
            logger.error("Unexpected error during poll cycle: %s", exc)

        iteration += 1
        if max_iterations is not None and iteration >= max_iterations:
            logger.info("Reached max_iterations=%d, stopping.", max_iterations)
            break

        logger.debug("Sleeping %ds before next poll.", interval)
        time.sleep(interval)
