"""Tests for pipewatch.notifier."""

from __future__ import annotations

import logging
import smtplib
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.notifier import (
    EmailNotifier,
    LogNotifier,
    NotificationError,
    build_notifiers_from_config,
    dispatch,
)


# ---------------------------------------------------------------------------
# LogNotifier
# ---------------------------------------------------------------------------

def test_log_notifier_logs_message(caplog):
    notifier = LogNotifier(level=logging.WARNING)
    with caplog.at_level(logging.WARNING, logger="pipewatch.notifier"):
        notifier.send("test subject", "test body")
    assert "test subject" in caplog.text
    assert "test body" in caplog.text


def test_log_notifier_default_level():
    notifier = LogNotifier()
    assert notifier.level == logging.WARNING


# ---------------------------------------------------------------------------
# EmailNotifier
# ---------------------------------------------------------------------------

@patch("pipewatch.notifier.smtplib.SMTP")
def test_email_notifier_sends(mock_smtp_cls):
    mock_smtp = MagicMock()
    mock_smtp_cls.return_value.__enter__ = lambda s: mock_smtp
    mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)

    notifier = EmailNotifier(
        smtp_host="localhost",
        smtp_port=1025,
        to_addrs=["ops@example.com"],
    )
    notifier.send("Alert!", "Something is wrong.")

    mock_smtp.send_message.assert_called_once()


@patch("pipewatch.notifier.smtplib.SMTP")
def test_email_notifier_no_recipients_skips(mock_smtp_cls):
    notifier = EmailNotifier(smtp_host="localhost", to_addrs=[])
    notifier.send("Alert!", "body")
    mock_smtp_cls.assert_not_called()


@patch("pipewatch.notifier.smtplib.SMTP", side_effect=smtplib.SMTPException("conn refused"))
def test_email_notifier_raises_notification_error(mock_smtp_cls):
    notifier = EmailNotifier(smtp_host="localhost", to_addrs=["x@y.com"])
    with pytest.raises(NotificationError, match="SMTP error"):
        notifier.send("Alert!", "body")


# ---------------------------------------------------------------------------
# build_notifiers_from_config
# ---------------------------------------------------------------------------

def test_build_notifiers_empty_config():
    notifiers = build_notifiers_from_config({})
    assert len(notifiers) == 1
    assert isinstance(notifiers[0], LogNotifier)


def test_build_notifiers_with_email():
    cfg = {"email": {"smtp_host": "mail.example.com", "to": ["a@b.com"]}}
    notifiers = build_notifiers_from_config(cfg)
    assert len(notifiers) == 2
    assert isinstance(notifiers[1], EmailNotifier)
    assert notifiers[1].smtp_host == "mail.example.com"


# ---------------------------------------------------------------------------
# dispatch
# ---------------------------------------------------------------------------

def test_dispatch_calls_all_notifiers():
    n1, n2 = MagicMock(), MagicMock()
    dispatch([n1, n2], "subj", "body")
    n1.send.assert_called_once_with("subj", "body")
    n2.send.assert_called_once_with("subj", "body")


def test_dispatch_continues_on_error(caplog):
    failing = MagicMock()
    failing.send.side_effect = NotificationError("boom")
    ok = MagicMock()
    with caplog.at_level(logging.ERROR, logger="pipewatch.notifier"):
        dispatch([failing, ok], "subj", "body")
    ok.send.assert_called_once()
    assert "boom" in caplog.text
