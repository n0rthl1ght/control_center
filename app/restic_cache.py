from __future__ import annotations

"""Background cache for Restic snapshots used by the /restic page."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .restic_service import restic_snapshots

_RESTIC_REFRESH_POOL = ThreadPoolExecutor(max_workers=2)
_RESTIC_SNAPSHOTS_CACHE: dict[int, dict[str, object]] = {}
_RESTIC_CACHE_LOCK = threading.Lock()


def _entry(target_id: int) -> dict[str, object]:
    return _RESTIC_SNAPSHOTS_CACHE.setdefault(
        target_id,
        {
            "rows": [],
            "error": "",
            "updated_monotonic": 0.0,
            "loading": False,
        },
    )


def refresh_restic_snapshots_background(target_id: int, repository: str, password: str) -> None:
    """Start a background refresh of the snapshot cache for a target."""
    with _RESTIC_CACHE_LOCK:
        entry = _entry(target_id)
        if bool(entry.get("loading")):
            return
        entry["loading"] = True

    def _job() -> None:
        try:
            ok, rows, err = restic_snapshots(repository, password)
            with _RESTIC_CACHE_LOCK:
                entry = _entry(target_id)
                entry["rows"] = rows if ok else []
                entry["error"] = "" if ok else (err or "Ошибка получения снапшотов Restic")
                entry["updated_monotonic"] = time.monotonic()
                entry["loading"] = False
        except Exception as exc:  # noqa: BLE001
            with _RESTIC_CACHE_LOCK:
                entry = _entry(target_id)
                entry["error"] = str(exc)
                entry["updated_monotonic"] = time.monotonic()
                entry["loading"] = False

    _RESTIC_REFRESH_POOL.submit(_job)


def get_cached_snapshots(
    target_id: int,
    repository: str,
    password: str,
    ttl_sec: int = 120,
) -> tuple[list[dict], str, bool]:
    """Return cached snapshots and trigger a background refresh if needed."""
    with _RESTIC_CACHE_LOCK:
        entry = _entry(target_id)
        rows = list(entry.get("rows", []))  # type: ignore[arg-type]
        error = str(entry.get("error", "") or "")
        loading = bool(entry.get("loading", False))
        updated = float(entry.get("updated_monotonic", 0.0) or 0.0)

    age_sec = float(time.monotonic() - updated)
    should_refresh = False
    if not rows and not error and not loading:
        should_refresh = True
    elif age_sec > max(1, ttl_sec) and not loading:
        should_refresh = True
    if should_refresh:
        refresh_restic_snapshots_background(target_id, repository, password)
        loading = True

    return rows, error, loading


def shutdown_restic_cache_pool() -> None:
    """Shut down the background snapshot cache refresh pool."""
    _RESTIC_REFRESH_POOL.shutdown(wait=False, cancel_futures=True)
