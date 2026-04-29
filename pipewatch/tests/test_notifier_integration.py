"""Integration-style tests: build_notifiers_from_config → dispatch."""

from __future__ import annotations

import logging

from pipewatch.notifier import build_notifiers_from_config, dispatch


def test_dispatch_log_only_config(caplog):
    """With no extra config only LogNotifier is active; dispatch should log."""
    notifiers = build_notifiers_from_config({})
    with caplog.at_level(logging.WARNING, logger="pipewatch.notifier"):
        dispatch(notifiers, "Pipeline degraded", "latency_ms is critical")
    assert "Pipeline degraded" in caplog.text
    assert "latency_ms is critical" in caplog.text


def test_dispatch_multiple_notifiers_all_receive():
    from unittest.mock import MagicMock

    n1, n2, n3 = MagicMock(), MagicMock(), MagicMock()
    dispatch([n1, n2, n3], "subject", "body")
    for n in (n1, n2, n3):
        n.send.assert_called_once_with("subject", "body")


def test_dispatch_partial_failure_does_not_abort():
    from unittest.mock import MagicMock
    from pipewatch.notifier import NotificationError

    bad = MagicMock()
    bad.send.side_effect = NotificationError("timeout")
    good = MagicMock()

    dispatch([bad, good], "s", "b")
    good.send.assert_called_once_with("s", "b")


def test_build_notifiers_email_fields():
    cfg = {
        "email": {
            "smtp_host": "smtp.example.com",
            "smtp_port": 587,
            "from": "alerts@example.com",
            "to": ["team@example.com"],
            "use_tls": True,
        }
    }
    from pipewatch.notifier import EmailNotifier, LogNotifier

    notifiers = build_notifiers_from_config(cfg)
    assert len(notifiers) == 2
    log_n, email_n = notifiers
    assert isinstance(log_n, LogNotifier)
    assert isinstance(email_n, EmailNotifier)
    assert email_n.smtp_port == 587
    assert email_n.use_tls is True
    assert email_n.from_addr == "alerts@example.com"
