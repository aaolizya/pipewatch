"""Alert silencing (maintenance window) support for pipewatch."""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional


@dataclass
class SilenceRule:
    """A rule that suppresses alerts for matching sources/metrics during a window."""

    source_pattern: str
    metric_pattern: str = "*"
    start: Optional[datetime] = None
    end: Optional[datetime] = None
    reason: str = ""

    def is_active(self, now: Optional[datetime] = None) -> bool:
        """Return True if this rule is currently active."""
        if now is None:
            now = datetime.now(tz=timezone.utc)
        if self.start is not None and now < self.start:
            return False
        if self.end is not None and now > self.end:
            return False
        return True

    def matches(self, source: str, metric: str) -> bool:
        """Return True if the rule pattern matches the given source and metric."""
        return (
            fnmatch.fnmatch(source, self.source_pattern)
            and fnmatch.fnmatch(metric, self.metric_pattern)
        )


@dataclass
class AlertSilencer:
    """Manages a collection of silence rules."""

    rules: List[SilenceRule] = field(default_factory=list)

    def add_rule(self, rule: SilenceRule) -> None:
        """Register a new silence rule."""
        self.rules.append(rule)

    def is_silenced(
        self,
        source: str,
        metric: str,
        now: Optional[datetime] = None,
    ) -> bool:
        """Return True if any active rule matches the source/metric pair."""
        return any(
            rule.is_active(now) and rule.matches(source, metric)
            for rule in self.rules
        )

    def active_rules(self, now: Optional[datetime] = None) -> List[SilenceRule]:
        """Return all currently active rules."""
        return [r for r in self.rules if r.is_active(now)]

    def purge_expired(self, now: Optional[datetime] = None) -> int:
        """Remove rules whose end time has passed. Returns count removed."""
        if now is None:
            now = datetime.now(tz=timezone.utc)
        before = len(self.rules)
        self.rules = [
            r for r in self.rules if r.end is None or r.end > now
        ]
        return before - len(self.rules)
