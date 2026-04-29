"""Notification backends for pipewatch alerts."""

from __future__ import annotations

import logging
import smtplib
from dataclasses import dataclass, field
from email.message import EmailMessage
from typing import List, Optional, Protocol

logger = logging.getLogger(__name__)


class NotificationError(Exception):
    """Raised when a notification cannot be delivered."""


class Notifier(Protocol):
    """Interface that all notification backends must satisfy."""

    def send(self, subject: str, body: str) -> None:  # pragma: no cover
        ...


@dataclass
class LogNotifier:
    """Writes alerts to the Python logger (always available)."""

    level: int = logging.WARNING

    def send(self, subject: str, body: str) -> None:
        logger.log(self.level, "[pipewatch alert] %s\n%s", subject, body)


@dataclass
class EmailNotifier:
    """Sends alert e-mails via an SMTP relay."""

    smtp_host: str
    smtp_port: int = 25
    from_addr: str = "pipewatch@localhost"
    to_addrs: List[str] = field(default_factory=list)
    username: Optional[str] = None
    password: Optional[str] = None
    use_tls: bool = False

    def send(self, subject: str, body: str) -> None:
        if not self.to_addrs:
            logger.debug("EmailNotifier: no recipients configured, skipping.")
            return

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.from_addr
        msg["To"] = ", ".join(self.to_addrs)
        msg.set_content(body)

        try:
            cls = smtplib.SMTP_SSL if self.use_tls else smtplib.SMTP
            with cls(self.smtp_host, self.smtp_port) as smtp:
                if self.username and self.password:
                    smtp.login(self.username, self.password)
                smtp.send_message(msg)
            logger.debug("EmailNotifier: sent '%s' to %s", subject, self.to_addrs)
        except smtplib.SMTPException as exc:
            raise NotificationError(f"SMTP error: {exc}") from exc


def build_notifiers_from_config(cfg: dict) -> List[Notifier]:
    """Instantiate notifier objects from a raw config dict."""
    notifiers: List[Notifier] = [LogNotifier()]
    email_cfg = cfg.get("email")
    if email_cfg:
        notifiers.append(
            EmailNotifier(
                smtp_host=email_cfg["smtp_host"],
                smtp_port=int(email_cfg.get("smtp_port", 25)),
                from_addr=email_cfg.get("from", "pipewatch@localhost"),
                to_addrs=email_cfg.get("to", []),
                username=email_cfg.get("username"),
                password=email_cfg.get("password"),
                use_tls=bool(email_cfg.get("use_tls", False)),
            )
        )
    return notifiers


def dispatch(notifiers: List[Notifier], subject: str, body: str) -> None:
    """Send *subject* / *body* through every configured notifier."""
    for notifier in notifiers:
        try:
            notifier.send(subject, body)
        except NotificationError as exc:
            logger.error("Notifier %s failed: %s", type(notifier).__name__, exc)
