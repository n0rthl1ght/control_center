from __future__ import annotations

"""Helpers for running the restic CLI and parsing its output."""

import json
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable


def _run_restic(repository: str, password: str, args: list[str]) -> tuple[int, str]:
    """Run a restic command with `-r <repo>` and password in the environment."""
    env = os.environ.copy()
    env["RESTIC_PASSWORD"] = password
    env.setdefault("RCLONE_CONFIG", "/config/rclone/rclone.conf")
    env.setdefault("RESTIC_CACHE_DIR", "/tmp/restic-cache")
    proc = subprocess.run(
        ["restic", "-r", repository, *args],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return proc.returncode, (proc.stdout or "").strip()


def _normalize_restic_tag(value: str) -> str:
    """Normalize a Restic tag into a safe format."""
    tag = (value or "").strip()
    if not tag:
        return ""
    tag = re.sub(r"\s+", "_", tag)
    tag = re.sub(r"[^A-Za-z0-9._:-]", "-", tag)
    return tag[:128].strip("-_.:")


def _append_tag_args(args: list[str], tags: Iterable[str]) -> list[str]:
    """Append `--tag` arguments for valid Restic tags."""
    result = list(args)
    seen: set[str] = set()
    for raw in tags:
        tag = _normalize_restic_tag(str(raw))
        if not tag or tag in seen:
            continue
        seen.add(tag)
        result.extend(["--tag", tag])
    return result


def _extract_snapshot_id(output: str) -> str:
    """Extract a snapshot ID from restic JSON output or text fallback."""
    # `restic --json` emits JSON lines; the final output usually includes the snapshot ID.
    for line in (output or "").splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        sid = str(payload.get("snapshot_id", "")).strip()
        if sid:
            return sid

    # Fallback parsing for non-JSON output.
    match = re.search(r"\b([0-9a-f]{8,64})\b", output or "", flags=re.IGNORECASE)
    return match.group(1) if match else ""


def _snapshot_sort_key(item: dict) -> tuple[float, str]:
    """Return a snapshot sort key: time first, then ID."""
    raw_time = str(item.get("time", "") or "").strip()
    if raw_time:
        normalized = raw_time.replace("Z", "+00:00")
        try:
            return (datetime.fromisoformat(normalized).timestamp(), str(item.get("id", "")))
        except ValueError:
            pass
    return (0.0, str(item.get("id", "")))


def send_archive_to_restic(
    archive_path: Path,
    repository: str,
    password: str,
    target_name: str,
    run_id: int,
    extra_tags: list[str] | None = None,
) -> tuple[bool, str, str]:
    """Send a backup path (archive or directory) to Restic and return status/message/snapshot ID."""
    if not repository.strip() or not password:
        return False, "Restic repository/password не заполнены", ""
    if not archive_path.exists():
        return False, f"Путь бэкапа не найден: {archive_path}", ""

    default_tags = [target_name]
    if extra_tags:
        default_tags.extend(extra_tags)
    backup_args = _append_tag_args(["backup", str(archive_path), "--json"], default_tags)

    code, out = _run_restic(
        repository.strip(),
        password,
        backup_args,
    )
    snapshot_id = _extract_snapshot_id(out)
    if code != 0:
        return False, out or f"restic backup failed (rc={code})", snapshot_id
    return True, out, snapshot_id


def restic_forget_prune(
    repository: str,
    password: str,
    keep_within_days: int,
    tag: str | None = None,
) -> tuple[bool, str]:
    """Delete old snapshots using keep-last, optionally filtered by tag."""
    if not repository.strip() or not password:
        return False, "Restic repository/password не заполнены"
    keep_last = max(1, int(keep_within_days))
    args = ["forget", "--group-by=", f"--keep-last={keep_last}"]
    normalized_tag = _normalize_restic_tag(tag or "")
    if normalized_tag:
        args.extend(["--tag", normalized_tag])

    def _is_lock_error(output: str) -> bool:
        text = (output or "").lower()
        return (
            "repo already locked" in text
            or "repository is already locked" in text
            or "unable to create lock" in text
        )

    attempts = 3
    wait_sec = 10
    last_out = ""
    for attempt in range(1, attempts + 1):
        code, out = _run_restic(
            repository.strip(),
            password,
            args,
        )
        last_out = out or ""
        if code == 0:
            return True, out
        if not _is_lock_error(out) or attempt == attempts:
            return False, out
        time.sleep(wait_sec)
    return False, last_out


def restic_snapshots(repository: str, password: str) -> tuple[bool, list[dict], str]:
    """Return the snapshot list from JSON or text output of `restic snapshots`."""
    if not repository.strip() or not password:
        return False, [], "Restic repository/password не заполнены"
    code, out = _run_restic(repository.strip(), password, ["snapshots", "--json"])
    if code != 0:
        return False, [], out or f"restic snapshots failed (rc={code})"

    rows: list[dict] = []

    # Restic may return warning lines and JSON within the same output.
    def _try_parse_json_payload(raw: str) -> bool:
        payload_text = (raw or "").strip()
        if not payload_text:
            return False

        def _consume_payload(payload: object) -> bool:
            appended = False
            if isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict):
                        _append_snapshot(item)
                        appended = True
            elif isinstance(payload, dict):
                nested = payload.get("snapshots") if isinstance(payload, dict) else None
                if isinstance(nested, list):
                    for item in nested:
                        if isinstance(item, dict):
                            _append_snapshot(item)
                            appended = True
                # Sometimes the payload is a single snapshot object.
                if isinstance(payload, dict) and (payload.get("id") or payload.get("tree")):
                    _append_snapshot(payload)
                    appended = True
            return appended

        # Try parsing the payload as-is.
        try:
            payload = json.loads(payload_text)
            if _consume_payload(payload):
                return True
        except json.JSONDecodeError:
            pass

        # Try trimming everything before the first JSON token.
        first_obj = payload_text.find("{")
        first_arr = payload_text.find("[")
        starts = [x for x in (first_obj, first_arr) if x != -1]
        if not starts:
            return False
        start = min(starts)
        trimmed = payload_text[start:]
        try:
            payload = json.loads(trimmed)
            return _consume_payload(payload)
        except json.JSONDecodeError:
            return False

    def _append_snapshot(item: dict) -> None:
        sid = str(item.get("id", "") or "").strip()
        if not sid:
            return
        rows.append(
            {
                "id": sid,
                "short_id": sid[:8],
                "time": str(item.get("time", "-") or "-"),
                "paths": [str(p) for p in (item.get("paths") or []) if str(p).strip()],
                "tags": [str(t) for t in (item.get("tags") or []) if str(t).strip()],
            }
        )

    # Option 1: full JSON payload (including warning + JSON mixed output).
    if _try_parse_json_payload(out or ""):
        rows.sort(key=_snapshot_sort_key, reverse=True)
        return True, rows, ""

    # Option 2: line-by-line JSON stream plus possible service lines.
    for line in (out or "").splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("{"):
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                _append_snapshot(item)
            continue
        if line.startswith("["):
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, list):
                for item in payload:
                    if isinstance(item, dict):
                        _append_snapshot(item)
    if rows:
        rows.sort(key=_snapshot_sort_key, reverse=True)
        return True, rows, ""

    # Fallback: text output if JSON is unavailable or malformed.
    for line in (out or "").splitlines():
        text = line.strip()
        if not text:
            continue
        if text.lower().startswith("id ") or set(text) == {"-"}:
            continue
        if text.lower().startswith("snapshots") and "processed" in text.lower():
            continue
        parts = text.split()
        if not parts:
            continue
        snap_id = parts[0]
        if not re.fullmatch(r"[0-9a-fA-F]{6,64}", snap_id):
            continue
        snap_time = " ".join(parts[1:3]) if len(parts) >= 3 else "-"
        tail = " ".join(parts[3:]).strip()
        rows.append(
            {
                "id": snap_id,
                "short_id": snap_id[:8],
                "time": snap_time,
                "paths": [tail] if tail else [],
                "tags": [],
            }
        )
    if rows:
        rows.sort(key=_snapshot_sort_key, reverse=True)
        return True, rows, ""

    # If Restic returned something but parsing failed, expose a hint in the UI.
    snippet = (out or "").strip()
    if len(snippet) > 500:
        snippet = snippet[:500] + "..."
    if snippet:
        return False, [], f"Не удалось распарсить ответ restic snapshots: {snippet}"
    return True, [], ""
