from __future__ import annotations

"""Timezone helpers for run timestamps and logs."""

from datetime import datetime
from zoneinfo import ZoneInfo

from .config import TIMEZONE


def _tz() -> ZoneInfo:
    """Return the configured timezone, or UTC on failure."""
    aliases = {"MSK": "Europe/Moscow", "UTC+3": "Europe/Moscow"}
    tz_name = aliases.get(TIMEZONE, TIMEZONE)
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo("UTC")


def now_local_naive() -> datetime:
    """Return the current local time as a naive datetime for DB storage."""
    # The database stores naive datetimes, but in the configured local timezone.
    return datetime.now(_tz()).replace(tzinfo=None)


def from_timestamp_local_naive(ts: float) -> datetime:
    """Convert a POSIX timestamp to a local naive datetime."""
    return datetime.fromtimestamp(ts, _tz()).replace(tzinfo=None)
