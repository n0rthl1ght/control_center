from __future__ import annotations

"""Pydantic response schemas for API endpoints."""

from pydantic import BaseModel


class LogItem(BaseModel):
    """A single run log line from `/api/runs/{id}/logs`."""

    id: int
    level: str
    message: str
    created_at: str


class RunStatus(BaseModel):
    """Run status/progress payload from `/api/runs/{id}/status`."""

    id: int
    status: str
    progress: int
    step: str
    error_message: str
