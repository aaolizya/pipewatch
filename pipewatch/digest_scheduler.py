"""Schedule and dispatch periodic digest reports."""
from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field
from typing import List, Optional

from pipewatch.digest import DigestReport, build_digest
from pipewatch.history import MetricHistory
from pipewatch.config import SourceConfig
from pipewatch.notifier import Notifier

logger = logging.getLogger(__name__)


@dataclass
class DigestScheduler:
    """Fires digest reports at a fixed cadence."""
    interval_seconds: int
    window_seconds: int
    notifiers: List[Notifier] = field(default_factory=list)
    _last_sent: Optional[datetime.datetime] = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")

    def is_due(self, now: Optional[datetime.datetime] = None) -> bool:
        if now is None:
            now = datetime.datetime.utcnow()
        if self._last_sent is None:
            return True
        return (now - self._last_sent).total_seconds() >= self.interval_seconds

    def maybe_send(
        self,
        sources: List[SourceConfig],
        history: MetricHistory,
        now: Optional[datetime.datetime] = None,
    ) -> Optional[DigestReport]:
        """Build and dispatch a digest if the interval has elapsed."""
        if now is None:
            now = datetime.datetime.utcnow()
        if not self.is_due(now):
            return None
        report = build_digest(sources, history, self.window_seconds, now=now)
        self._dispatch(report)
        self._last_sent = now
        return report

    def _dispatch(self, report: DigestReport) -> None:
        message = str(report)
        for notifier in self.notifiers:
            try:
                notifier.send(subject="Pipewatch Digest Report", body=message)
            except Exception as exc:  # pragma: no cover
                logger.warning("Digest notifier %s failed: %s", notifier, exc)
