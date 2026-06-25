from __future__ import annotations

"""File logging for internal application events."""

import logging
from logging.handlers import RotatingFileHandler

from .config import LOGS_DIR

LOGGER_NAME = "control_center"
APP_LOG_PATH = LOGS_DIR / "app.log"


def setup_logging() -> logging.Logger:
    """Initialize the application file logger once."""
    logger = logging.getLogger(LOGGER_NAME)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(APP_LOG_PATH, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def get_logger(name: str | None = None) -> logging.Logger:
    """Return an application logger with a hierarchical name."""
    setup_logging()
    if not name:
        return logging.getLogger(LOGGER_NAME)
    return logging.getLogger(f"{LOGGER_NAME}.{name}")
