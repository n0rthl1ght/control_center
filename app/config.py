from __future__ import annotations

"""Application configuration.

Values are read from environment variables and include sensible local
defaults for development runs.
"""

import os
from pathlib import Path

# Project root and local runtime data directory (SQLite by default).
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR = DATA_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# SQLAlchemy URL for the application metadata/log database.
DATABASE_URL = os.getenv("CONTROL_CENTER_DB_URL", f"sqlite:///{DATA_DIR / 'control_center.db'}")
# Default path used for backup storage.
DEFAULT_BACKUP_ROOT = os.getenv("DEFAULT_BACKUP_ROOT", "/mnt/backup/backup_db/postgres_dump")
# Default retention window used to clean up old archives.
DEFAULT_RETENTION_DAYS = int(os.getenv("DEFAULT_RETENTION_DAYS", "14"))
# Default number of concurrently running jobs.
DEFAULT_MAX_CONCURRENT_RUNS = int(os.getenv("DEFAULT_MAX_CONCURRENT_RUNS", "3"))
# UI/runtime timezone (naive local time is stored in the database).
TIMEZONE = os.getenv("BACKUP_TZ") or os.getenv("TZ") or "Europe/Moscow"
