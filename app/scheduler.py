from __future__ import annotations

"""APScheduler integration for automatic cron-based backups."""

import re

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from .app_logging import get_logger
from .backup_engine import enqueue_backup
from .config import TIMEZONE
from .db import SessionLocal
from .models import BackupTarget

scheduler = BackgroundScheduler(timezone=TIMEZONE)
logger = get_logger(__name__)

_STD_CRON_DOW_NAMES = {
    "0": "sun",
    "1": "mon",
    "2": "tue",
    "3": "wed",
    "4": "thu",
    "5": "fri",
    "6": "sat",
    "7": "sun",
}


def _normalize_standard_cron_dow_token(token: str) -> str:
    token = (token or "").strip().lower()
    if not token:
        return token
    if "/" in token:
        base, step = token.split("/", 1)
        normalized_base = _normalize_standard_cron_dow_token(base) if base != "*" else "*"
        return f"{normalized_base}/{step}"
    if "-" in token:
        start, end = token.split("-", 1)
        start_norm = _STD_CRON_DOW_NAMES.get(start, start)
        end_norm = _STD_CRON_DOW_NAMES.get(end, end)
        return f"{start_norm}-{end_norm}"
    return _STD_CRON_DOW_NAMES.get(token, token)


def normalize_cron_expr(expr: str) -> str:
    """Normalize a standard 5-field cron expression for APScheduler."""
    raw = (expr or "").strip()
    parts = raw.split()
    if len(parts) != 5:
        raise ValueError("Cron expression must contain exactly 5 fields")
    minute, hour, day, month, dow = parts
    normalized_dow = ",".join(_normalize_standard_cron_dow_token(token) for token in dow.split(","))
    return " ".join([minute, hour, day, month, normalized_dow])


def build_cron_trigger(expr: str) -> CronTrigger:
    """Build a CronTrigger with support for standard DOW notation."""
    return CronTrigger.from_crontab(normalize_cron_expr(expr), timezone=TIMEZONE)


def sync_scheduler_jobs() -> None:
    """Rebuild scheduler jobs from active targets and their cron expressions."""
    scheduler.remove_all_jobs()

    db = SessionLocal()
    try:
        targets = db.query(BackupTarget).filter(BackupTarget.enabled == True).all()  # noqa: E712
        for target in targets:
            cron = (target.cron_expr or "").strip()
            if not cron:
                continue
            try:
                trigger = build_cron_trigger(cron)
            except ValueError as exc:
                logger.warning("Scheduler: invalid cron for target id=%s name=%r cron=%r: %s", target.id, target.name, cron, exc)
                continue
            scheduler.add_job(
                enqueue_backup,
                trigger=trigger,
                args=[target.id, "scheduled"],
                id=f"backup_target_{target.id}",
                replace_existing=True,
            )
    finally:
        db.close()
