from __future__ import annotations

"""Notification delivery via Telegram and Email."""

import smtplib
import socket
from email.mime.text import MIMEText
from typing import Dict

import requests
from sqlalchemy.orm import Session

from .app_logging import get_logger
from .models import AppSetting

logger = get_logger(__name__)


def _probe_smtp_endpoint(host: str, port: int, timeout: float = 10.0) -> tuple[bool, str]:
    """Check that the SMTP endpoint resolves and accepts TCP connections."""
    try:
        addr_info = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        return False, f"dns_resolution_failed: {exc}"

    last_error = "unknown_connect_error"
    for family, socktype, proto, _, sockaddr in addr_info:
        try:
            with socket.socket(family, socktype, proto) as sock:
                sock.settimeout(timeout)
                sock.connect(sockaddr)
            return True, "ok"
        except OSError as exc:
            last_error = f"{exc.__class__.__name__}: {exc}"

    return False, last_error


def _trim_text(value: str, limit: int) -> str:
    """Trim text to a safe size for external delivery channels."""
    text = (value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 14)] + "\n...truncated"


def get_settings(db: Session) -> Dict[str, str]:
    """Load global settings into a regular dictionary."""
    items = db.query(AppSetting).all()
    return {item.key: item.value for item in items}


def send_telegram(db: Session, message: str, last_log_line: str = "") -> bool:
    """Send a Telegram message via notifier URL or BOT:TOKEN."""
    settings = get_settings(db)
    raw_telegram_url = settings.get("telegram_notifier_url", "").strip()
    chat_id = settings.get("telegram_chat_id", "").strip()
    thread_id = settings.get("telegram_thread_id", "").strip()

    if not raw_telegram_url or not chat_id:
        logger.warning("Telegram notify skipped: missing notifier_url or chat_id")
        return False

    text = _trim_text(message, 3000)
    if last_log_line:
        trimmed_last_line = _trim_text(last_log_line, 700)
        text += f"\n\nПоследняя строка лога:\n`{trimmed_last_line}`"
    text = _trim_text(text, 3800)

    payload: dict[str, object] = {
        "chat_id": int(chat_id) if chat_id.lstrip("-").isdigit() else chat_id,
        "text": text,
    }
    if thread_id and thread_id.isdigit():
        payload["message_thread_id"] = int(thread_id)

    notifier_url = raw_telegram_url
    if raw_telegram_url.startswith("http://") or raw_telegram_url.startswith("https://"):
        notifier_url = raw_telegram_url
    elif ":" in raw_telegram_url:
        notifier_url = f"https://api.telegram.org/bot{raw_telegram_url}/sendMessage"
    else:
        logger.warning("Telegram notify skipped: unsupported telegram_notifier_url format")
        return False

    try:
        resp = requests.post(notifier_url, json=payload, timeout=8)
        if resp.status_code >= 400:
            logger.warning("Telegram notify failed: status=%s body=%s", resp.status_code, (resp.text or "")[:500])
            return False
        return True
    except requests.RequestException as exc:
        logger.warning("Telegram notify request exception: %s", exc)
        return False


def send_email(db: Session, subject: str, body: str) -> bool:
    """Send an email notification using the stored SMTP settings."""
    settings = get_settings(db)

    smtp_host = settings.get("smtp_host", "").strip()
    smtp_port = settings.get("smtp_port", "").strip()
    smtp_user = settings.get("smtp_user", "").strip()
    smtp_password = settings.get("smtp_password", "").strip()
    smtp_from = settings.get("smtp_from", "").strip()
    smtp_to = settings.get("smtp_to", "").strip()
    smtp_security = settings.get("smtp_security", "").strip().lower()
    if not smtp_security:
        smtp_tls = settings.get("smtp_starttls", "true").lower() == "true"
        smtp_security = "starttls" if smtp_tls else "none"

    if not smtp_host or not smtp_port or not smtp_from or not smtp_to:
        logger.warning("Email notify skipped: SMTP settings incomplete")
        return False

    try:
        smtp_port_int = int(smtp_port)
    except ValueError:
        logger.warning("Email notify skipped: invalid smtp_port=%r", smtp_port)
        return False

    probe_ok, probe_details = _probe_smtp_endpoint(smtp_host, smtp_port_int)
    if not probe_ok:
        logger.warning(
            "Email notify skipped: SMTP endpoint unreachable host=%s port=%s security=%s details=%s",
            smtp_host,
            smtp_port_int,
            smtp_security or "none",
            probe_details,
        )
        return False

    msg = MIMEText(_trim_text(body, 20000), _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = smtp_to

    try:
        if smtp_security == "ssl_tls":
            server_ctx = smtplib.SMTP_SSL(smtp_host, smtp_port_int, timeout=10)
        else:
            server_ctx = smtplib.SMTP(smtp_host, smtp_port_int, timeout=10)

        with server_ctx as server:
            if smtp_security == "starttls":
                server.starttls()
            if smtp_user:
                server.login(smtp_user, smtp_password)
            server.sendmail(smtp_from, [x.strip() for x in smtp_to.split(",") if x.strip()], msg.as_string())
        return True
    except Exception as exc:
        logger.warning(
            "Email notify exception host=%s port=%s security=%s: %s",
            smtp_host,
            smtp_port_int,
            smtp_security or "none",
            exc,
        )
        return False
