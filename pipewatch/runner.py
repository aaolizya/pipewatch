"""Runner: orchestrates a single poll cycle or a continuous loop."""

from __future__ import annotations

import logging
import time
from typing import Optional

from pipewatch.alerting import build_source_map, evaluate_all, format_report
from pipewatch.collector import collect_all
from pipewatch.config import PipewatchConfig
from pipewatch.watchdog import Watchdog

logger = logging.getLogger(__name__)


def run_once(
    config: PipewatchConfig,
    watchdog: Optional[Watchdog] = None,
) -> str:
    """Collect metrics, evaluate alerts, and return a formatted report.

    If a *watchdog* is provided, each source that successfully returns
    metrics is recorded; stale sources are appended to the report.
    """
    metrics = collect_all(config.sources)
    source_map = build_source_map(config.sources, metrics)
    results = evaluate_all(source_map)
    report_lines = [format_report(results)]

    if watchdog is not None:
        import datetime

        now = datetime.datetime.utcnow()
        # Register all configured sources so unresponsive ones are tracked.
        for src in config.sources:
            watchdog.register(src.name)

        # Record sources that returned at least one metric.
        seen = {m.source for m in metrics}
        for name in seen:
            watchdog.record(name, ts=now)

        stale = watchdog.check(now=now)
        if stale:
            report_lines.append("\n--- Stale Sources ---")
            for s in stale:
                report_lines.append(str(s))
                logger.warning("Stale source detected: %s", s.source_name)

    return "\n".join(report_lines)


def run_loop(
    config: PipewatchConfig,
    watchdog: Optional[Watchdog] = None,
) -> None:
    """Poll sources in a loop, sleeping *poll_interval* seconds between runs."""
    interval = config.poll_interval
    logger.info("Starting poll loop (interval=%ss)", interval)
    while True:
        try:
            report = run_once(config, watchdog=watchdog)
            print(report)
        except Exception:  # noqa: BLE001
            logger.exception("Unhandled error during poll cycle")
        time.sleep(interval)
