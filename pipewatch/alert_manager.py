"""Ties together alerting, history, and notification dispatch."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.alerting import evaluate_all, format_report
from pipewatch.config import PipewatchConfig
from pipewatch.history import MetricHistory
from pipewatch.metrics import AlertResult, Metric
from pipewatch.notifier import Notifier, build_notifiers_from_config, dispatch
from pipewatch.summary import PipelineSummary

logger = logging.getLogger(__name__)


@dataclass
class AlertManager:
    """Orchestrates evaluation, history recording, and notification."""

    config: PipewatchConfig
    history: MetricHistory
    notifiers: List[Notifier] = field(default_factory=list)
    _previous_critical: set = field(default_factory=set, init=False, repr=False)

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, config: PipewatchConfig, history: Optional[MetricHistory] = None) -> "AlertManager":
        if history is None:
            history = MetricHistory()
        notify_cfg = getattr(config, "notifications", {}) or {}
        notifiers = build_notifiers_from_config(notify_cfg)
        return cls(config=config, history=history, notifiers=notifiers)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process(self, metrics: List[Metric]) -> PipelineSummary:
        """Evaluate *metrics*, record history, fire notifications, return summary."""
        for metric in metrics:
            self.history.record(metric)

        results: List[AlertResult] = evaluate_all(metrics, self.config.sources)
        summary = PipelineSummary.from_results(results)

        newly_critical = self._find_newly_critical(results)
        if newly_critical:
            self._notify(newly_critical, results)

        self._previous_critical = {r.metric.name for r in results if r.is_alert()}
        return summary

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _find_newly_critical(self, results: List[AlertResult]) -> List[AlertResult]:
        return [
            r for r in results
            if r.is_alert() and r.metric.name not in self._previous_critical
        ]

    def _notify(self, triggered: List[AlertResult], all_results: List[AlertResult]) -> None:
        subject = f"[pipewatch] {len(triggered)} new alert(s) detected"
        body = format_report(all_results)
        logger.info("Dispatching alert notification: %s", subject)
        dispatch(self.notifiers, subject, body)
