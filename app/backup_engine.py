from __future__ import annotations
"""Background backup executor.

This module contains:
- command execution wrappers with cancellation/timeout support
- DB-specific dump flows (PostgreSQL, MySQL, MongoDB)
- archive verification and retention cleanup
- status/log updates and notification delivery
"""
import os
import json
import re
import shlex
import shutil
import signal
import subprocess
import tarfile
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable
from sqlalchemy.orm import Session
from .app_logging import get_logger
from .config import DEFAULT_MAX_CONCURRENT_RUNS, LOGS_DIR
from .db import SessionLocal
from .models import BackupRun, BackupTarget, RunLog
from .notifications import send_email, send_telegram
from .restic_cache import refresh_restic_snapshots_background
from .restic_service import restic_forget_prune, send_archive_to_restic
from .time_utils import from_timestamp_local_naive, now_local_naive

# The global pool is used for background runs from the API/scheduler.
# Its size can be changed on the fly: tasks that already started stay in the
# old executor, while new ones are routed to the reinitialized pool.
EXECUTOR_LOCK = Lock()
EXECUTOR = ThreadPoolExecutor(max_workers=DEFAULT_MAX_CONCURRENT_RUNS)

# The known_hosts file is shared by all SSH/MySQL tasks. This lets us keep
# host key checking enabled without relying on a user's home directory.
SSH_KNOWN_HOSTS_FILE = "/tmp/cc_ssh_known_hosts"

# Threshold for force-cleaning stale dump processes. Protects against
# hung pg_dump/mysqldump/mongodump/rsync processes after app crashes.
STALE_PROCESS_KILL_SEC = max(600, int(os.getenv("STALE_PROCESS_KILL_SEC", "21600")))
logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Background worker-pool management.
# These helper functions are used by the API/scheduler to enqueue tasks and
# inspect pool state without touching executor private fields from elsewhere.
# ---------------------------------------------------------------------------
def normalize_max_concurrent_runs(value: int | str | None) -> int:
    """Normalize the concurrent-task limit into a safe range."""
    try:
        normalized = int(str(value or DEFAULT_MAX_CONCURRENT_RUNS).strip())
    except (TypeError, ValueError):
        normalized = DEFAULT_MAX_CONCURRENT_RUNS
    return max(1, min(normalized, 32))


def get_executor_max_workers() -> int:
    """Return the current concurrent-task limit."""
    with EXECUTOR_LOCK:
        try:
            return int(getattr(EXECUTOR, "_max_workers", DEFAULT_MAX_CONCURRENT_RUNS))
        except Exception:
            return DEFAULT_MAX_CONCURRENT_RUNS


def configure_executor(max_workers: int | str | None) -> int:
    """Reinitialize the executor for new tasks with a new limit."""
    global EXECUTOR
    normalized = normalize_max_concurrent_runs(max_workers)
    old_executor = None
    old_workers = None
    with EXECUTOR_LOCK:
        try:
            old_workers = int(getattr(EXECUTOR, "_max_workers", DEFAULT_MAX_CONCURRENT_RUNS))
        except Exception:
            old_workers = DEFAULT_MAX_CONCURRENT_RUNS
        if old_workers == normalized:
            return normalized
        old_executor = EXECUTOR
        EXECUTOR = ThreadPoolExecutor(max_workers=normalized)
    logger.info(
        "Executor reconfigured: max_workers=%s (previous=%s). Running tasks continue in old pool; new tasks use new pool.",
        normalized,
        old_workers,
    )
    if old_executor is not None:
        old_executor.shutdown(wait=False, cancel_futures=False)
    return normalized
def _kill_stale_backup_processes() -> list[dict[str, int | str | bool]]:
    """Terminate stale backup processes older than STALE_PROCESS_KILL_SEC."""
    cmd = ["ps", "-eo", "pid=,etimes=,cmd="]
    try:
        raw = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
    except Exception:
        return []
    patterns = ("pg_dump", "mysqldump", "mongodump", " rsync ", " scp ", " ssh ", " restic ", " rclone ")
    current_pid = os.getpid()
    killed: list[dict[str, int | str | bool]] = []
    for line in raw.splitlines():
        row = line.strip()
        if not row:
            continue
        parts = row.split(None, 2)
        if len(parts) < 3:
            continue
        try:
            pid = int(parts[0])
            elapsed = int(parts[1])
        except ValueError:
            continue
        if pid <= 1 or pid == current_pid or elapsed < STALE_PROCESS_KILL_SEC:
            continue
        cmdline = f" {parts[2].lower()} "
        if not any(p in cmdline for p in patterns):
            continue
        killed_now = False
        sigkill_used = False
        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(1.0)
            try:
                os.kill(pid, 0)
                os.kill(pid, signal.SIGKILL)
                sigkill_used = True
            except ProcessLookupError:
                pass
            killed_now = True
        except ProcessLookupError:
            killed_now = True
        except Exception:
            killed_now = False
        if killed_now:
            killed.append(
                {
                    "pid": pid,
                    "elapsed_sec": elapsed,
                    "sigkill": sigkill_used,
                    "command": parts[2][:400],
                }
            )
    return killed
class BackupCanceledError(Exception):
    """Raised when a run cancellation is requested by the user."""
    pass


# ---------------------------------------------------------------------------
# Logging and run-state primitives.
# These small utilities:
# - normalize user input
# - write lines to the DB and file log
# - update progress/status/step on BackupRun
# ---------------------------------------------------------------------------
def get_executor_diagnostics() -> dict[str, int | bool]:
    """Return basic thread-pool diagnostics."""
    pending = -1
    thread_count = -1
    is_shutdown = False
    max_workers = -1
    with EXECUTOR_LOCK:
        executor = EXECUTOR
    try:
        work_queue = getattr(executor, "_work_queue", None)
        if work_queue and hasattr(work_queue, "qsize"):
            pending = int(work_queue.qsize())
    except Exception:
        pending = -1
    try:
        threads = getattr(executor, "_threads", None)
        if threads is not None:
            thread_count = len(threads)
    except Exception:
        thread_count = -1
    try:
        is_shutdown = bool(getattr(executor, "_shutdown", False))
    except Exception:
        is_shutdown = False
    try:
        max_workers = int(getattr(executor, "_max_workers", -1))
    except Exception:
        max_workers = -1
    return {
        "max_workers": max_workers,
        "active_threads": thread_count,
        "pending_tasks": pending,
        "executor_shutdown": is_shutdown,
    }
def parse_csv_items(raw: str) -> list[str]:
    """Split CSV/multiline input into non-empty trimmed values."""
    return [x.strip() for x in raw.replace("\n", ",").split(",") if x.strip()]
def _append_log_file(run_id: int, level: str, message: str) -> None:
    """Append a run log line to the file log under `data/logs`."""
    log_path = LOGS_DIR / f"run_{run_id}.log"
    timestamp = now_local_naive().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {level.upper()} {message}\n"
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(line)
def _last_log_line(text: str) -> str:
    """Return the last non-empty line from an error or log text."""
    lines = [line.strip() for line in (text or "").splitlines() if line.strip()]
    return lines[-1] if lines else (text or "").strip()

def append_log(db: Session, run: BackupRun, message: str, level: str = "info") -> None:
    """Persist a single log line for the run."""
    db.add(RunLog(run_id=run.id, level=level, message=message))
    db.commit()
    try:
        _append_log_file(run.id, level, message)
    except Exception:
        # A file-log failure must not break backup execution.
        pass
def set_run_state(db: Session, run: BackupRun, status: str | None = None, progress: int | None = None, step: str | None = None, error: str | None = None) -> None:
    """Update mutable run-state fields."""
    if status is not None:
        run.status = status
    if progress is not None:
        run.progress = progress
    if step is not None:
        run.step = step
    if error is not None:
        run.error_message = error
    db.commit()
def _run_command(
    cmd: list[str],
    env: dict[str, str],
    cwd: Path | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> tuple[int, str]:
    """Run a command and return combined stdout/stderr with cancellation support."""
    # Base wrapper for "regular" commands. Used where a single error text is
    # enough and stdout/stderr do not need to be handled separately.
    proc = subprocess.Popen(
        cmd,
        env=env,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    while proc.poll() is None:
        if should_cancel and should_cancel():
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
            return 130, "Canceled by user"
        time.sleep(0.2)
    out = proc.stdout.read() if proc.stdout else ""
    err = proc.stderr.read() if proc.stderr else ""
    text = (out or "") + (err or "")
    return proc.returncode, text.strip()


def _run_command_text(
    cmd: list[str],
    env: dict[str, str],
    cwd: Path | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> tuple[int, str, str]:
    """Run a command and return stdout/stderr separately."""
    # Used for flows where stdout is the "useful" result (for example,
    # a list of collections/tables) while stderr should be logged separately.
    proc = subprocess.Popen(
        cmd,
        env=env,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    while proc.poll() is None:
        if should_cancel and should_cancel():
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
            return 130, "", "Canceled by user"
        time.sleep(0.2)
    out = proc.stdout.read() if proc.stdout else ""
    err = proc.stderr.read() if proc.stderr else ""
    return proc.returncode, (out or ""), (err or "")


def _run_command_to_file(
    cmd: list[str],
    out_path: Path,
    env: dict[str, str],
    should_cancel: Callable[[], bool] | None = None,
) -> tuple[int, str]:
    """Run a command with stdout redirected to a file and return stderr."""
    # Typical pg_dump/mysqldump flow: write large data directly to a file and
    # keep only stderr in memory so the process does not bloat.
    with out_path.open("wb") as out_fd:
        proc = subprocess.Popen(cmd, env=env, stdout=out_fd, stderr=subprocess.PIPE, text=True)
        while proc.poll() is None:
            if should_cancel and should_cancel():
                proc.terminate()
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                return 130, "Canceled by user"
            time.sleep(0.2)
        err = proc.stderr.read() if proc.stderr else ""
    return proc.returncode, (err or "").strip()


def _run_command_with_watchdog(
    cmd: list[str],
    env: dict[str, str],
    watch_path: Path | None = None,
    idle_timeout_sec: int = 0,
    hard_timeout_sec: int = 0,
    cwd: Path | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> tuple[int, str]:
    """Run a long command with idle and overall timeouts."""
    # This variant is used for long copy/archive operations where we need to
    # track whether the process is "alive" not only by PID but by file growth.
    with tempfile.NamedTemporaryFile(prefix="backup_cmd_", suffix=".log", delete=False) as tmp:
        log_path = Path(tmp.name)
    timed_out_msg = ""
    try:
        with log_path.open("wb") as log_fd:
            proc = subprocess.Popen(
                cmd,
                env=env,
                cwd=str(cwd) if cwd else None,
                stdout=log_fd,
                stderr=subprocess.STDOUT,
            )
            start = time.monotonic()
            last_progress = start
            last_size = watch_path.stat().st_size if watch_path and watch_path.exists() else 0
            while proc.poll() is None:
                if should_cancel and should_cancel():
                    proc.terminate()
                    try:
                        proc.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    return 130, "Canceled by user"
                now = time.monotonic()
                if hard_timeout_sec > 0 and (now - start) > hard_timeout_sec:
                    timed_out_msg = f"Превышен общий таймаут команды ({hard_timeout_sec} сек)"
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    break
                if watch_path and idle_timeout_sec > 0:
                    size = watch_path.stat().st_size if watch_path.exists() else 0
                    if size > last_size:
                        last_size = size
                        last_progress = now
                    elif (now - last_progress) > idle_timeout_sec:
                        timed_out_msg = f"Нет прогресса копирования более {idle_timeout_sec} сек"
                        proc.terminate()
                        try:
                            proc.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                        break
                time.sleep(1.0)
        output = log_path.read_text(encoding="utf-8", errors="replace").strip()
        if timed_out_msg:
            combined = f"{timed_out_msg}\n{output}".strip()
            return 124, combined
        return proc.returncode, output
    finally:
        log_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Artifact verification and finalization.
# These functions are DB-agnostic: they validate files/directories, create the
# final archive, and clean local data after a successful Restic upload.
# ---------------------------------------------------------------------------
def _verify_file(path: Path) -> tuple[bool, str]:
    """Basic file validation: it exists and is not empty."""
    if not path.exists():
        return False, f"{path} does not exist"
    if path.stat().st_size <= 0:
        return False, f"{path} is empty"
    return True, "ok"
def _verify_dir_with_files(path: Path) -> tuple[bool, str]:
    """Check that a directory exists and contains at least one file."""
    if not path.exists() or not path.is_dir():
        return False, f"{path} does not exist or is not a directory"
    for _ in path.rglob("*"):
        return True, "ok"
    return False, f"{path} has no files"
def _verify_archive(archive: Path) -> tuple[bool, str]:
    """Validate a tar.gz archive: it opens and contains entries."""
    try:
        with tarfile.open(archive, "r:gz") as tf:
            names = tf.getnames()
            if not names:
                return False, "archive has no members"
    except Exception as exc:  # noqa: BLE001
        return False, f"archive verify failed: {exc}"
    return True, "ok"
def _safe_extract_tar_gz(archive: Path, dest_dir: Path) -> None:
    """Safely extract a tar.gz archive inside `dest_dir`.

    Rules:
    - symlink/hardlink entries are forbidden
    - absolute paths are forbidden
    - every final member path must remain inside `dest_dir`
    """
    dest_resolved = dest_dir.resolve()
    with tarfile.open(archive, "r:gz") as tf:
        members = tf.getmembers()
        for member in members:
            if member.issym() or member.islnk():
                raise RuntimeError(f"Небезопасный архив: link entry запрещен ({member.name})")
            if not (member.isfile() or member.isdir()):
                raise RuntimeError(f"Небезопасный архив: unsupported entry type ({member.name})")
            member_name = member.name or ""
            if member_name.startswith("/"):
                raise RuntimeError(f"Небезопасный архив: абсолютный путь запрещен ({member_name})")
            target_path = (dest_resolved / member_name).resolve()
            if target_path != dest_resolved and dest_resolved not in target_path.parents:
                raise RuntimeError(f"Небезопасный архив: выход за пределы каталога ({member_name})")
        tf.extractall(path=dest_resolved, members=members)
def _cleanup_old_archives(
    target_root: Path,
    retention_days: int,
    exclude_paths: set[Path] | None = None,
) -> list[Path]:
    """Delete old `.tgz` archives by calendar days in local time.

    Important: `retention_days=1` means "keep only today's archives",
    not "keep the last 24 hours".
    """
    removed: list[Path] = []
    keep_days = max(1, int(retention_days or 1))
    today = now_local_naive().date()
    cutoff_date = today - timedelta(days=keep_days - 1)
    excluded = {p.resolve() for p in (exclude_paths or set())}

    for item in target_root.glob("*.tgz"):
        try:
            if item.resolve() in excluded:
                continue
        except FileNotFoundError:
            continue

        archive_date = None
        name = item.name
        if len(name) >= 8:
            try:
                archive_date = datetime.strptime(name[:8], "%d-%m-%y").date()
            except ValueError:
                archive_date = None
        if archive_date is None:
            archive_date = from_timestamp_local_naive(item.stat().st_mtime).date()

        if archive_date < cutoff_date:
            item.unlink(missing_ok=True)
            removed.append(item)

    return removed
def _materialize_backup_output(work_dir: Path, archive_path: Path, archive_enabled: bool) -> tuple[Path, str]:
    """Materialize and verify the final backup output.

    Returns:
    - path to the final artifact
    - artifact type: `archive` or `directory`
    """
    # Decide in one place what counts as the final run output: a tar.gz archive
    # or a live dump directory. This simplifies downstream code because
    # Restic/notifications receive a normalized output either way.
    if archive_enabled:
        with tarfile.open(archive_path, "w:gz") as tf:
            tf.add(work_dir, arcname=".")
        ok, reason = _verify_archive(archive_path)
        if not ok:
            raise RuntimeError(f"Проверка архива не пройдена: {reason}")
        return archive_path, "archive"
    ok, reason = _verify_dir_with_files(work_dir)
    if not ok:
        raise RuntimeError(f"Проверка каталога дампа не пройдена: {reason}")
    return work_dir, "directory"
def _cleanup_output_after_restic(output_path: Path, output_type: str) -> tuple[bool, str]:
    """Delete local data after a successful Restic upload."""
    try:
        # By current logic we do not delete archives: the archive itself is the
        # final local artifact and may still be needed for manual download.
        if output_type == "archive":
            return True, f"Локальный архив сохранен после Restic: {output_path}"
        if output_type == "directory":
            shutil.rmtree(output_path, ignore_errors=False)
            return True, f"Локальный каталог удален после Restic: {output_path}"
        return False, f"Неизвестный тип output для удаления: {output_type}"
    except Exception as exc:  # noqa: BLE001
        return False, f"Не удалось удалить локальные данные после Restic: {exc}"
def _mongo_auth_args(target: BackupTarget) -> list[str]:
    """Build auth arguments for mongodump.

    If username/password are not set, authentication is fully disabled.
    """
    args: list[str] = []
    user = (target.db_user or "").strip()
    password = (target.db_password or "").strip()
    if not user and not password:
        return args
    if user:
        args.extend(["--username", user])
    if password:
        args.extend(["--password", password])
    if target.mongo_auth_db.strip():
        args.extend(["--authenticationDatabase", target.mongo_auth_db.strip()])
    return args


def _mongosh_auth_args(target: BackupTarget) -> list[str]:
    """Build auth arguments for mongosh."""
    args: list[str] = []
    user = (target.db_user or "").strip()
    password = (target.db_password or "").strip()
    if not user and not password:
        return args
    if user:
        args.extend(["--username", user])
    if password:
        args.extend(["--password", password])
    if target.mongo_auth_db.strip():
        args.extend(["--authenticationDatabase", target.mongo_auth_db.strip()])
    return args
def _mongo_parse_collection_groups(
    collection_names: list[str],
    prefixes: list[str],
    latest_count: int,
    blacklist_values: list[str],
    group_name_mode: str = "simple",
    collection_parts: list[str] | None = None,
) -> tuple[list[str], list[str]]:
    """Select collections from the latest groups by numeric suffix."""
    # This function is used by latest_collection_groups. It takes a flat list
    # of collection names and returns the final dump list plus warnings about
    # gaps in the detected groups.
    normalized_prefixes = [x.strip() for x in prefixes if x.strip()]
    if not normalized_prefixes:
        raise RuntimeError("Для Mongo latest_collection_groups требуется хотя бы один префикс группы")
    normalized_mode = (group_name_mode or "simple").strip().lower()
    normalized_parts = [x.strip() for x in (collection_parts or []) if x.strip()]
    if normalized_mode not in {"simple", "multipart"}:
        raise RuntimeError(f"Неподдерживаемый Mongo group_name_mode: {normalized_mode}")
    if normalized_mode == "multipart" and not normalized_parts:
        raise RuntimeError("Для Mongo multipart-режима требуется список частей группы")
    blacklist_names = {x.strip() for x in blacklist_values if x.strip() and not x.strip().isdigit()}
    blacklist_suffixes = {int(x.strip()) for x in blacklist_values if x.strip().isdigit()}
    grouped: dict[int, dict[str, dict[str, str] | str]] = {}
    for raw_name in collection_names:
        name = (raw_name or "").strip()
        if not name or name in blacklist_names:
            continue
        for prefix in normalized_prefixes:
            if normalized_mode == "simple":
                match = re.fullmatch(rf"{re.escape(prefix)}_(\d+)", name)
                if not match:
                    continue
                suffix = int(match.group(1))
                if suffix in blacklist_suffixes or f"{prefix}_{suffix}" in blacklist_names:
                    break
                grouped.setdefault(suffix, {})[prefix] = name
                break
            for part in normalized_parts:
                match = re.fullmatch(rf"{re.escape(prefix)}_(\d+)\.{re.escape(part)}", name)
                if not match:
                    continue
                suffix = int(match.group(1))
                if (
                    suffix in blacklist_suffixes
                    or f"{prefix}_{suffix}" in blacklist_names
                    or f"{prefix}_{suffix}.{part}" in blacklist_names
                ):
                    break
                grouped.setdefault(suffix, {}).setdefault(prefix, {})[part] = name
                break
            else:
                continue
            break
    if not grouped:
        raise RuntimeError("После фильтрации не найдено ни одной MongoDB-коллекции для latest_collection_groups")
    selected_suffixes = sorted(grouped.keys())[-max(1, latest_count):]
    selected_collections: list[str] = []
    warnings: list[str] = []
    for suffix in selected_suffixes:
        present = grouped.get(suffix, {})
        if normalized_mode == "simple":
            missing = [prefix for prefix in normalized_prefixes if prefix not in present]
            if missing:
                warnings.append(
                    f"Для группы suffix={suffix} отсутствуют коллекции: {', '.join(f'{prefix}_{suffix}' for prefix in missing)}"
                )
            for prefix in normalized_prefixes:
                collection_name = present.get(prefix)
                if isinstance(collection_name, str) and collection_name:
                    selected_collections.append(collection_name)
        else:
            for prefix in normalized_prefixes:
                present_parts = present.get(prefix)
                if not isinstance(present_parts, dict):
                    warnings.append(
                        f"Для группы suffix={suffix} отсутствуют коллекции: "
                        f"{', '.join(f'{prefix}_{suffix}.{part}' for part in normalized_parts)}"
                    )
                    continue
                missing_parts = [part for part in normalized_parts if part not in present_parts]
                if missing_parts:
                    warnings.append(
                        f"Для группы suffix={suffix} отсутствуют коллекции: "
                        f"{', '.join(f'{prefix}_{suffix}.{part}' for part in missing_parts)}"
                    )
                for part in normalized_parts:
                    collection_name = present_parts.get(part)
                    if collection_name:
                        selected_collections.append(collection_name)
    if not selected_collections:
        raise RuntimeError("После отбора последних MongoDB-групп не осталось коллекций для дампа")
    return selected_collections, warnings


# ---------------------------------------------------------------------------
# MySQL/SSH/rsync helper functions.
# They encapsulate CLI argument building, secret masking, and SSH/rsync
# transport logic so the main MySQL branch reads as a scenario.
# ---------------------------------------------------------------------------
def _mysql_ignore_table_arg(db_name: str, table_ref: str) -> str:
    """Build a mysql `--ignore-table` argument with an explicit DB prefix."""
    table_ref = table_ref.strip()
    if "." in table_ref:
        return f"--ignore-table={table_ref}"
    return f"--ignore-table={db_name}.{table_ref}"
def _table_name_only(table_ref: str) -> str:
    """Return the table name from `db.table` or `table`."""
    return table_ref.strip().split(".", 1)[-1]
def _mysql_full_table_name(default_db: str, table_ref: str) -> str:
    """Ensure a DB prefix is present on a MySQL table reference."""
    table_ref = table_ref.strip()
    if "." in table_ref:
        return table_ref
    return f"{default_db}.{table_ref}"
def _is_set_gtid_unsupported(stderr_text: str) -> bool:
    """Detect MariaDB/MySQL builds without `set-gtid-purged`."""
    text = (stderr_text or "").lower()
    return "unknown variable 'set-gtid-purged" in text
def _mysql_password_args(password: str) -> list[str]:
    """Build mysql CLI password arguments when a password is present."""
    if not password:
        return []
    # An explicit argument is more reliable than MYSQL_PWD in mixed
    # MySQL/MariaDB environments.
    return [f"--password={password}"]
def _mysql_conn_args(target: BackupTarget) -> list[str]:
    """Build mysql CLI connection arguments with local-socket support."""
    args: list[str] = []
    host = (target.db_host or "").strip()
    user = (target.db_user or "").strip()
    password = target.db_password or ""
    # For local backups without credentials, implicit socket auth is preferred.
    # This matters in environments where the root/mysql user authenticates via
    # the unix_socket plugin and an explicit `--host 127.0.0.1` breaks the flow.
    use_implicit_local = not user and not password and host in {"", "localhost", "127.0.0.1"}
    if not use_implicit_local:
        if host:
            args.extend(["--host", host])
        if target.db_port:
            args.extend(["--port", str(target.db_port)])
    if user:
        args.extend(["--user", user])
    args.extend(_mysql_password_args(password))
    return args
def _shell_join(args: list[str]) -> str:
    """Build a shell-quoted string for safe remote invocation/logging."""
    return " ".join(shlex.quote(x) for x in args)
def _remote_ssh_host(target: BackupTarget) -> str:
    return (getattr(target, "remote_ssh_host", "") or getattr(target, "mysql_ssh_host", "") or "").strip()
def _remote_ssh_port(target: BackupTarget) -> int:
    value = getattr(target, "remote_ssh_port", None)
    if value in (None, "", 0):
        value = getattr(target, "mysql_ssh_port", 22)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 22
def _remote_ssh_user(target: BackupTarget) -> str:
    return (getattr(target, "remote_ssh_user", "") or getattr(target, "mysql_ssh_user", "") or "").strip()
def _remote_ssh_target(target: BackupTarget) -> str:
    """Return the SSH target in `user@host` format."""
    return f"{_remote_ssh_user(target)}@{_remote_ssh_host(target)}"
def _remote_ssh_auth_type(target: BackupTarget) -> str:
    """Normalize the SSH authentication type for a backup task."""
    auth_type = (getattr(target, "remote_ssh_auth_type", "") or getattr(target, "mysql_ssh_auth_type", "") or "").strip().lower()
    if auth_type in {"password", "private_key"}:
        return auth_type
    if (
        (getattr(target, "remote_ssh_private_key", "") or "").strip()
        or (target.mysql_ssh_key_path or "").strip()
        or (getattr(target, "mysql_ssh_private_key", "") or "").strip()
    ):
        return "private_key"
    return "password"
def _remote_ssh_password(target: BackupTarget) -> str:
    return (getattr(target, "remote_ssh_password", "") or getattr(target, "mysql_ssh_password", "") or "")
def _remote_tmp_dir(target: BackupTarget, default: str = "/tmp/backup_control_center") -> str:
    value = (getattr(target, "remote_ssh_remote_tmp_dir", "") or getattr(target, "mysql_ssh_remote_tmp_dir", "") or "").strip()
    return value or default
def _ssh_identity_path(target: BackupTarget, identity_file: str | None = None) -> str:
    """Return the SSH key path for the current run."""
    if identity_file:
        return identity_file.strip()
    return (target.mysql_ssh_key_path or "").strip()
def _build_inline_ssh_private_key_file(target: BackupTarget) -> Path | None:
    """Materialize an inline SSH private key into a temp file with chmod 600."""
    auth_type = _remote_ssh_auth_type(target)
    if auth_type != "private_key":
        return None
    raw_key = (getattr(target, "remote_ssh_private_key", "") or getattr(target, "mysql_ssh_private_key", "") or "").strip()
    if not raw_key:
        return None
    with tempfile.NamedTemporaryFile(prefix="cc_remote_ssh_", suffix=".key", delete=False, mode="w", encoding="utf-8") as fh:
        key_path = Path(fh.name)
        fh.write(raw_key)
        fh.write("\n")
    os.chmod(key_path, 0o600)
    return key_path
def _docker_remote_prefix(target: BackupTarget, *args: str) -> list[str]:
    """Add `sudo -n` to a remote Docker command when enabled for the task."""
    if bool(getattr(target, "docker_use_sudo", False)):
        return ["sudo", "-n", *args]
    return list(args)


def _docker_compose_candidates(target: BackupTarget) -> list[str]:
    custom_file = (getattr(target, "docker_compose_file", "") or "").strip()
    if custom_file:
        return [custom_file]
    return ["docker-compose.yml", "docker-compose.yaml", "compose.yml", "compose.yaml"]
def _ssh_base_args(target: BackupTarget, identity_file: str | None = None) -> list[str]:
    """Base SSH arguments for connection liveness and authentication."""
    # Keepalive options reduce the chance of "silent" hangs during long
    # mysqldump/rsync operations over unstable networks or NAT.
    use_password = _remote_ssh_auth_type(target) == "password" and bool(_remote_ssh_password(target).strip())
    args = [
        "ssh",
        "-p",
        str(_remote_ssh_port(target)),
        "-o",
        "BatchMode=no" if use_password else "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        f"UserKnownHostsFile={SSH_KNOWN_HOSTS_FILE}",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=6",
        "-o",
        "TCPKeepAlive=yes",
        "-o",
        "ConnectTimeout=20",
    ]
    identity_path = _ssh_identity_path(target, identity_file)
    if identity_path:
        args.extend(["-i", identity_path])
    args.append(_remote_ssh_target(target))
    return args
def _scp_base_args(target: BackupTarget, identity_file: str | None = None) -> list[str]:
    """Base SCP arguments with an optional identity file."""
    args = [
        "scp",
        "-P",
        str(_remote_ssh_port(target)),
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        f"UserKnownHostsFile={SSH_KNOWN_HOSTS_FILE}",
    ]
    identity_path = _ssh_identity_path(target, identity_file)
    if identity_path:
        args.extend(["-i", identity_path])
    return args
def _with_ssh_password(cmd: list[str], target: BackupTarget, env: dict[str, str]) -> list[str]:
    """Wrap a command with sshpass when password authentication is used."""
    if _remote_ssh_auth_type(target) != "password":
        return cmd
    password = _remote_ssh_password(target)
    if not password:
        return cmd
    if shutil.which("sshpass") is None:
        raise RuntimeError("Для SSH password требуется установленный sshpass на сервере control center")
    env["SSHPASS"] = password
    return ["sshpass", "-e", *cmd]
def _trim_output(text: str, max_lines: int = 12, max_chars: int = 2500) -> str:
    """Trim noisy command output for compact log storage."""
    raw = (text or "").strip()
    if not raw:
        return ""
    lines = raw.splitlines()
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
        raw = "\n".join(lines)
        raw = f"...\n{raw}"
    if len(raw) > max_chars:
        raw = f"...{raw[-max_chars:]}"
    return raw
def _mask_cmd_for_log(cmd: list[str]) -> str:
    """Mask secrets in a command before writing it to run logs."""
    masked: list[str] = []
    hide_next = False
    for token in cmd:
        if hide_next:
            masked.append("******")
            hide_next = False
            continue
        lower = token.lower()
        if lower in {"-p", "--password"}:
            masked.append(token)
            hide_next = True
            continue
        if "--password=" in token:
            masked.append(re.sub(r"--password=\S+", "--password=******", token))
            continue
        if "cc_remote_ssh_" in token and token.endswith(".key"):
            masked.append(token.rsplit("/", 1)[0] + "/******.key" if "/" in token else "******.key")
            continue
        masked.append(token)
    return _shell_join(masked)
def _rsync_ssh_transport(target: BackupTarget, identity_file: str | None = None) -> str:
    """Build the SSH transport string used in `rsync -e`."""
    use_password = _remote_ssh_auth_type(target) == "password" and bool(_remote_ssh_password(target).strip())
    ssh_cmd = [
        "ssh",
        "-p",
        str(_remote_ssh_port(target)),
        "-o",
        "BatchMode=no" if use_password else "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        f"UserKnownHostsFile={SSH_KNOWN_HOSTS_FILE}",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=6",
        "-o",
        "TCPKeepAlive=yes",
        "-o",
        "ConnectTimeout=20",
    ]
    identity_path = _ssh_identity_path(target, identity_file)
    if identity_path:
        ssh_cmd.extend(["-i", identity_path])
    return _shell_join(ssh_cmd)
def _rsync_copy_from_remote(
    target: BackupTarget,
    remote_path: str,
    local_path: Path,
    env: dict[str, str],
    identity_file: str | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> tuple[int, str]:
    """Copy a remote file via rsync with retries and timeout control."""
    # rsync is the primary transport for downloading remote bundles: it supports
    # partial/resume and handles network interruptions better than plain scp.
    cmd = [
        "rsync",
        "-az",
        "--partial",
        "--append-verify",
        "--timeout=60",
        "--info=progress2",
        "-e",
        _rsync_ssh_transport(target, identity_file=identity_file),
    ]
    if target.transfer_limit_kbps > 0:
        cmd.append(f"--bwlimit={target.transfer_limit_kbps}")
    cmd.extend([f"{_remote_ssh_target(target)}:{remote_path}", str(local_path)])
    wrapped_cmd = _with_ssh_password(cmd, target, env)
    attempts = 4
    idle_timeout_sec = 300
    hard_timeout_sec = 7200
    logs: list[str] = []
    for attempt in range(1, attempts + 1):
        # Each retry writes its own block to the aggregated log so the run log
        # shows exactly which attempt hit a timeout/disconnect.
        code, out = _run_command_with_watchdog(
            wrapped_cmd,
            env,
            watch_path=local_path,
            idle_timeout_sec=idle_timeout_sec,
            hard_timeout_sec=hard_timeout_sec,
            should_cancel=should_cancel,
        )
        logs.append(f"[attempt {attempt}/{attempts}] rc={code}\n{out}".strip())
        if code == 0:
            break
        if code == 130:
            break
        # Temporary SSH/transport issues: retry with backoff while preserving the partial file.
        if attempt < attempts:
            time.sleep(min(30, 4 * attempt))
    return code, "\n\n".join(logs)


# ---------------------------------------------------------------------------
# Public entry points.
# enqueue_backup creates a run record and hands control to the thread pool.
# execute_backup_run is the full end-to-end flow:
# preflight -> dump -> verify/archive -> restic -> notifications -> cleanup.
# ---------------------------------------------------------------------------
def enqueue_backup(target_id: int, launch_type: str = "manual") -> int:
    """Create a queued run and submit it to a background worker."""
    db = SessionLocal()
    try:
        target = db.query(BackupTarget).filter(BackupTarget.id == target_id).first()
        if not target:
            raise ValueError(f"Target {target_id} not found")
        # The queue here is logical, not external: create the BackupRun record,
        # commit it, and only then pass run_id into the thread pool.
        run = BackupRun(
            target_id=target_id,
            status="queued",
            progress=0,
            step="queued",
            launch_type=launch_type,
            cancel_requested=False,
            backup_date=now_local_naive().strftime("%d-%m-%y"),
            restic_status="",
            restic_snapshot_id="",
            restic_message="",
        )
        db.add(run)
        db.commit()
        db.refresh(run)
        run_id = run.id
    finally:
        db.close()
    logger.info("Enqueue run_id=%s target_id=%s launch_type=%s", run_id, target_id, launch_type)
    with EXECUTOR_LOCK:
        executor = EXECUTOR
    executor.submit(execute_backup_run, run_id)
    logger.info("Submitted run_id=%s to executor", run_id)
    return run_id
def execute_backup_run(run_id: int) -> None:
    """Main worker function that executes a run from start to finish."""
    db = SessionLocal()
    ssh_private_key_file: Path | None = None
    docker_final_archive: Path | None = None
    # Temporary remote directories are tracked here so we can still attempt
    # cleanup in finally even after abnormal termination.
    remote_cleanup_queue: list[tuple[BackupTarget, str]] = []
    try:
        run = db.query(BackupRun).filter(BackupRun.id == run_id).first()
        if not run:
            return
        target = db.query(BackupTarget).filter(BackupTarget.id == run.target_id).first()
        if not target:
            return

        # --- Per-run inner helper closures -----------------------------------
        # They intentionally live inside execute_backup_run because they use the
        # current `db`, `run`, `target`, and `env`, and are not needed elsewhere.
        def is_cancel_requested() -> bool:
            db.refresh(run)
            return bool(run.cancel_requested)
        def ensure_not_canceled(step: str = "") -> None:
            if is_cancel_requested():
                msg = "Бэкап отменен пользователем"
                if step:
                    msg += f" ({step})"
                append_log(db, run, msg, level="warning")
                raise BackupCanceledError(msg)
        def ensure_command_ok(code: int, out: str, err_prefix: str) -> None:
            if code == 130:
                raise BackupCanceledError("Бэкап отменен пользователем")
            if code != 0:
                raise RuntimeError(f"{err_prefix}: {out}")
        def log_command_output(prefix: str, out: str, level: str = "info") -> None:
            trimmed = _trim_output(out)
            if trimmed:
                append_log(db, run, f"{prefix}\n{trimmed}", level=level)
        def mark_progress(progress: int, step: str, message: str | None = None) -> None:
            set_run_state(db, run, progress=progress, step=step)
            if message:
                append_log(db, run, message)
        def add_remote_cleanup(cleanup_target: BackupTarget, remote_path: str) -> None:
            path = (remote_path or "").strip()
            if not path:
                return
            for existing_target, existing_path in remote_cleanup_queue:
                if existing_target.id == cleanup_target.id and existing_path == path:
                    return
            remote_cleanup_queue.append((cleanup_target, path))
        def remove_remote_cleanup(cleanup_target: BackupTarget, remote_path: str) -> None:
            path = (remote_path or "").strip()
            if not path:
                return
            remote_cleanup_queue[:] = [
                (existing_target, existing_path)
                for existing_target, existing_path in remote_cleanup_queue
                if not (existing_target.id == cleanup_target.id and existing_path == path)
            ]
        logger.info("Run start run_id=%s target=%s db_type=%s host=%s:%s archive=%s restic=%s", run.id, target.name, target.db_type, target.db_host, target.db_port, target.archive_enabled, target.restic_enabled)
        run.started_at = now_local_naive()
        run.status = "running"
        run.progress = 3
        run.step = "initializing"
        db.commit()
        try:
            # Preflight tries to clean stale processes before a new dump starts
            # so old zombies do not keep sockets open, fill disk space, or
            # interfere with competing rsync/ssh flows.
            stale = _kill_stale_backup_processes()
            if stale:
                append_log(
                    db,
                    run,
                    f"Preflight: остановлено зависших backup-процессов: {len(stale)} (порог {STALE_PROCESS_KILL_SEC} сек)",
                    level="warning",
                )
                logger.warning("Run preflight killed stale processes run_id=%s count=%s threshold_sec=%s", run.id, len(stale), STALE_PROCESS_KILL_SEC)
                for item in stale[:20]:
                    append_log(
                        db,
                        run,
                        f"[stale-kill] pid={item['pid']} elapsed={item['elapsed_sec']}s sigkill={item['sigkill']} cmd={item['command']}",
                        level="warning",
                    )
            else:
                append_log(
                    db,
                    run,
                    f"Preflight: зависшие backup-процессы не найдены (порог {STALE_PROCESS_KILL_SEC} сек)",
                )
        except Exception as exc:  # noqa: BLE001
            append_log(db, run, f"Preflight stale-kill warning: {exc}", level="warning")
        ensure_not_canceled("init")
        target_root = Path(target.backup_root).expanduser() / target.name
        backup_date = now_local_naive().strftime("%d-%m-%y")
        work_dir = target_root / f"{backup_date}_run{run.id}"
        archive_path = target_root / f"{backup_date}_run{run.id}.tgz"
        work_dir.mkdir(parents=True, exist_ok=True)
        run.backup_dir = str(work_dir)
        run.archive_file = str(archive_path) if target.archive_enabled else ""
        db.commit()

        # At this stage the run infrastructure is already prepared:
        # the working directory exists, the final path is computed, and status
        # is set. The DB-specific part begins below.
        mark_progress(7, "prepared", f"Рабочая директория подготовлена: {work_dir}")
        db_type = (target.db_type or "postgresql").strip().lower()
        append_log(db, run, f"Старт бэкапа '{target.name}'")
        append_log(db, run, f"Тип источника: {db_type}. Режим: {target.dump_mode}. Каталог: {work_dir}")
        env = os.environ.copy()
        if db_type == "postgresql":
            env["PGPASSWORD"] = target.db_password
        if db_type == "mysql":
            # Keep MYSQL_PWD for compatibility, but also pass explicit --password below.
            env["MYSQL_PWD"] = target.db_password
        def run_cmd_logged(
            label: str,
            cmd: list[str],
            should_cancel: Callable[[], bool] | None = None,
        ) -> tuple[int, str]:
            # All shell calls go through a single logging layer:
            # 1) the command is written to logs in masked form
            # 2) duration is recorded
            # 3) the caller decides whether rc should be treated as an error
            append_log(db, run, f"[cmd] {label}: {_mask_cmd_for_log(cmd)}")
            started = time.monotonic()
            code, out = _run_command(cmd, env, should_cancel=should_cancel)
            append_log(db, run, f"[cmd] {label} finished rc={code} in {time.monotonic() - started:.1f}s")
            return code, out
        def run_cmd_text_logged(
            label: str,
            cmd: list[str],
            should_cancel: Callable[[], bool] | None = None,
        ) -> tuple[int, str, str]:
            # Variant for commands where stdout/stderr are consumed differently.
            append_log(db, run, f"[cmd] {label}: {_mask_cmd_for_log(cmd)}")
            started = time.monotonic()
            code, out, err = _run_command_text(cmd, env, should_cancel=should_cancel)
            append_log(db, run, f"[cmd] {label} finished rc={code} in {time.monotonic() - started:.1f}s")
            return code, out, err
        def run_cmd_to_file_logged(
            label: str,
            cmd: list[str],
            out_path: Path,
            should_cancel: Callable[[], bool] | None = None,
        ) -> tuple[int, str]:
            # Separate wrapper for "dump to file": log both the command and the
            # output path so it is clear which artifact was created.
            append_log(db, run, f"[cmd] {label}: {_mask_cmd_for_log(cmd)} -> {out_path}")
            started = time.monotonic()
            code, out = _run_command_to_file(cmd, out_path, env, should_cancel=should_cancel)
            append_log(db, run, f"[cmd] {label} finished rc={code} in {time.monotonic() - started:.1f}s")
            return code, out
        def run_rsync_logged(
            label: str,
            remote_path: str,
            local_path: Path,
            identity_file: str | None = None,
            should_cancel: Callable[[], bool] | None = None,
        ) -> tuple[int, str]:
            # rsync is wrapped separately because the actual command is built
            # deeper down from SSH arguments and transfer limits.
            append_log(db, run, f"[cmd] {label}: rsync {remote_path} -> {local_path}")
            started = time.monotonic()
            code, out = _rsync_copy_from_remote(
                target,
                remote_path,
                local_path,
                env,
                identity_file=identity_file,
                should_cancel=should_cancel,
            )
            append_log(db, run, f"[cmd] {label} finished rc={code} in {time.monotonic() - started:.1f}s")
            return code, out
        db_list = parse_csv_items(target.databases_csv)
        split_tables = parse_csv_items(target.split_tables_csv)
        restic_extra_tags: list[str] = []
        if db_type == "docker":
            project_name = (target.docker_project_name or "").strip()
            if not project_name:
                project_name = Path((target.docker_project_host_dir or "").strip().rstrip("/")).name.strip()
            if not project_name:
                raise RuntimeError("Для Docker backup не удалось определить PROJECT_NAME")
            db_list = [project_name]
            restic_extra_tags = [project_name, "docker"]
        if db_type == "mongodb" and not db_list:
            append_log(db, run, "MongoDB: список баз пуст, будет выполнен дамп всех баз")
            db_list = ["__all__"]
        if not db_list:
            raise RuntimeError("Не задан список баз для бэкапа")
        total_dbs = len(db_list)

        # For PostgreSQL and Mongo the loop really iterates over each database.
        # For MySQL below, one shared batch handles the full `main_db_list`,
        # so the branch exits the loop after the first pass via `break`.
        for index, db_name in enumerate(db_list, start=1):
            display_db_name = "all_databases" if db_type == "mongodb" and db_name == "__all__" else db_name
            ensure_not_canceled(f"before {display_db_name}")
            host_dir_name = _remote_ssh_host(target) if db_type == "docker" else target.db_host
            host_dir = work_dir / host_dir_name
            host_dir.mkdir(parents=True, exist_ok=True)
            append_log(db, run, f"Начало резервного копирования базы {display_db_name} на {host_dir_name}")
            base_progress = 10 + int(((index - 1) / max(1, total_dbs)) * 60)
            mark_progress(base_progress, f"dump:{display_db_name}", f"Этап дампа {index}/{total_dbs}: {display_db_name}")
            if db_type == "postgresql":
                if target.dump_mode == "full":
                    # Simplest scenario: one SQL file per database.
                    out_file = host_dir / f"{db_name}.sql"
                    cmd = [
                        "pg_dump",
                        "-h",
                        target.db_host,
                        "-p",
                        str(target.db_port),
                        "-U",
                        target.db_user,
                        "-d",
                        db_name,
                        "-f",
                        str(out_file),
                    ]
                    code, out = run_cmd_logged(f"pg_dump full {db_name}", cmd, should_cancel=is_cancel_requested)
                    ensure_command_ok(code, out, f"pg_dump error for {db_name}")
                    ok, reason = _verify_file(out_file)
                    if not ok:
                        raise RuntimeError(f"Проверка дампа {out_file.name} не пройдена: {reason}")
                    append_log(db, run, f"Успешно: {out_file}")
                elif target.dump_mode == "split_excluded_tables":
                    # Two-file mode:
                    # - main dump without selected tables
                    # - separate schema-only dump for those tables so their
                    #   structure can be restored independently
                    if not split_tables:
                        raise RuntimeError("Для режима split_excluded_tables требуется список таблиц")
                    data_file = host_dir / f"{db_name}_data.dump"
                    schema_file = host_dir / f"{db_name}_schema.dump"
                    dump_cmd = [
                        "pg_dump",
                        "-h",
                        target.db_host,
                        "-p",
                        str(target.db_port),
                        "-U",
                        target.db_user,
                        "-d",
                        db_name,
                        "-Fc",
                    ]
                    for t in split_tables:
                        dump_cmd.append(f"--exclude-table={t}")
                    code, out = run_cmd_to_file_logged(
                        f"pg_dump split data {db_name}",
                        dump_cmd,
                        data_file,
                        should_cancel=is_cancel_requested,
                    )
                    ensure_command_ok(code, out, f"pg_dump (data) error for {db_name}")
                    schema_cmd = [
                        "pg_dump",
                        "-h",
                        target.db_host,
                        "-p",
                        str(target.db_port),
                        "-U",
                        target.db_user,
                        "-d",
                        db_name,
                        "--schema-only",
                        "-Fc",
                    ]
                    for t in split_tables:
                        schema_cmd.append(f"--table={t}")
                    code, out = run_cmd_to_file_logged(
                        f"pg_dump split schema {db_name}",
                        schema_cmd,
                        schema_file,
                        should_cancel=is_cancel_requested,
                    )
                    ensure_command_ok(code, out, f"pg_dump (schema) error for {db_name}")
                    ok, reason = _verify_file(data_file)
                    if not ok:
                        raise RuntimeError(f"Проверка дампа данных не пройдена: {reason}")
                    ok, reason = _verify_file(schema_file)
                    if not ok:
                        raise RuntimeError(f"Проверка дампа схемы не пройдена: {reason}")
                    append_log(db, run, f"Успешно: {data_file.name} и {schema_file.name}")
                elif target.dump_mode == "custom_excludes":
                    # Flexible PostgreSQL mode mirroring pg_dump CLI behavior:
                    # exclusions by tables, table data, and schemas.
                    out_file = host_dir / f"{db_name}.sql"
                    exclude_tables = parse_csv_items(target.pg_exclude_tables_csv)
                    exclude_table_data = parse_csv_items(target.pg_exclude_table_data_csv)
                    exclude_schemas = parse_csv_items(target.pg_exclude_schemas_csv)
                    cmd = [
                        "pg_dump",
                        "-h",
                        target.db_host,
                        "-p",
                        str(target.db_port),
                        "-U",
                        target.db_user,
                        "-d",
                        db_name,
                        "-f",
                        str(out_file),
                    ]
                    for item in exclude_tables:
                        cmd.append(f"--exclude-table={item}")
                    for item in exclude_table_data:
                        cmd.append(f"--exclude-table-data={item}")
                    for item in exclude_schemas:
                        cmd.append(f"--exclude-schema={item}")
                    code, out = run_cmd_logged(f"pg_dump custom excludes {db_name}", cmd, should_cancel=is_cancel_requested)
                    ensure_command_ok(code, out, f"pg_dump custom_excludes error for {db_name}")
                    ok, reason = _verify_file(out_file)
                    if not ok:
                        raise RuntimeError(f"Проверка дампа {out_file.name} не пройдена: {reason}")
                    append_log(
                        db,
                        run,
                        f"Успешно: {out_file} (exclude-table={len(exclude_tables)}, exclude-table-data={len(exclude_table_data)}, exclude-schema={len(exclude_schemas)})",
                    )
                else:
                    raise RuntimeError(f"Неизвестный режим дампа для PostgreSQL: {target.dump_mode}")
            elif db_type == "mysql":
                # MySQL flow:
                # 1) one shared mysqldump for the main DBs with ignore-table if provided
                # 2) a separate schema-only dump for tables from mysql_structure_tables_csv if provided
                # The two operations are intentionally split so "heavy" tables
                # can be excluded from the main dump while their structure is
                # still preserved for later restoration.
                main_db_list = parse_csv_items(target.databases_csv)
                if not main_db_list:
                    raise RuntimeError("Для MySQL не задан список основных БД")
                ignore_tables = parse_csv_items(target.mysql_ignore_tables_csv)
                structure_tables = parse_csv_items(target.mysql_structure_tables_csv)
                main_file = host_dir / f"main.{now_local_naive().strftime('%Y%m%d_%H%M')}.sql"
                schema_file = host_dir / f"empty_tables.{now_local_naive().strftime('%Y%m%d_%H%M')}.sql"
                use_ssh = bool(target.mysql_use_ssh and _remote_ssh_host(target) and _remote_ssh_user(target))
                if use_ssh and ssh_private_key_file is None:
                    ssh_private_key_file = _build_inline_ssh_private_key_file(target)
                remote_tmp_base = _remote_tmp_dir(target, "/tmp/mysql_backup_agent").rstrip("/")
                remote_run_dir = f"{remote_tmp_base}/{target.name}_run{run.id}"
                remote_main = f"{remote_run_dir}/{main_file.name}"
                remote_schema = f"{remote_run_dir}/{schema_file.name}"
                remote_bundle = f"{remote_run_dir}/mysql_bundle_{run.id}.tar.gz"
                local_bundle = host_dir / f"mysql_bundle_{run.id}.tar.gz"
                main_cmd = [
                    "mysqldump",
                    *_mysql_conn_args(target),
                    "--opt",
                    "--set-gtid-purged=OFF",
                    "--databases",
                    "--triggers",
                    "--routines",
                    "--events",
                    *main_db_list,
                ]
                for t in ignore_tables:
                    # If the user specified just `table`, bind it to the first
                    # DB in the list. The `db.table` format is also supported.
                    default_db = main_db_list[0]
                    main_cmd.append(f"--ignore-table={_mysql_full_table_name(default_db, t)}")
                if use_ssh:
                    # SSH mode means the dump is created near the MySQL server,
                    # then archived, and only after that downloaded to CC.
                    # This reduces network traffic and avoids streaming large
                    # SQL data through a single SSH session.
                    add_remote_cleanup(target, remote_run_dir)
                    append_log(db, run, f"MySQL дамп через SSH: {_remote_ssh_target(target)}")
                    identity_file = str(ssh_private_key_file) if ssh_private_key_file else None
                    ssh_base = _with_ssh_password(_ssh_base_args(target, identity_file=identity_file), target, env)
                    scp_base = _with_ssh_password(_scp_base_args(target, identity_file=identity_file), target, env)
                    if target.transfer_limit_kbps > 0:
                        append_log(db, run, f"Лимит скорости копирования: {target.transfer_limit_kbps} KB/s")
                    mark_progress(18, "mysql:ssh_prepare", f"[remote] Подготовка директории {remote_run_dir}")
                    mkdir_remote = ssh_base + [f"mkdir -p {shlex.quote(remote_run_dir)}"]
                    code, out = run_cmd_logged("mysql remote mkdir", mkdir_remote, should_cancel=is_cancel_requested)
                    log_command_output("[remote] mkdir output", out, level="warning" if code != 0 else "info")
                    ensure_command_ok(code, out, "Не удалось подготовить remote tmp dir")
                    mark_progress(28, "mysql:remote_main_dump", "[remote] Запуск mysqldump основных БД")
                    remote_main_cmd = ssh_base + [f"{_shell_join(main_cmd)} > {shlex.quote(remote_main)}"]
                    code, out = run_cmd_logged("mysql remote main", remote_main_cmd, should_cancel=is_cancel_requested)
                    if code != 0 and _is_set_gtid_unsupported(out):
                        append_log(db, run, "remote mysqldump не поддерживает --set-gtid-purged=OFF, повтор без этого флага", level="warning")
                        main_cmd_retry = [x for x in main_cmd if not x.startswith("--set-gtid-purged=")]
                        remote_main_cmd = ssh_base + [f"{_shell_join(main_cmd_retry)} > {shlex.quote(remote_main)}"]
                        code, out = run_cmd_logged("mysql remote main retry", remote_main_cmd, should_cancel=is_cancel_requested)
                    log_command_output("[remote] mysqldump main output", out, level="warning" if code != 0 else "info")
                    ensure_command_ok(code, out, "mysqldump основных БД завершился с ошибкой")
                    if structure_tables:
                        # Create the second file only when schema-only is
                        # actually requested. It contains DDL only (`--no-data`).
                        dump_schema_db = main_db_list[0]
                        object_list = [_table_name_only(x) for x in structure_tables]
                        mark_progress(48, "mysql:remote_schema_dump", f"[remote] Дамп schema-only таблиц ({len(object_list)} шт.)")
                        schema_cmd = [
                            "mysqldump",
                            *_mysql_conn_args(target),
                            "--set-gtid-purged=OFF",
                            "--no-data",
                            dump_schema_db,
                            *object_list,
                        ]
                        remote_schema_cmd = ssh_base + [f"{_shell_join(schema_cmd)} > {shlex.quote(remote_schema)}"]
                        code, out = run_cmd_logged("mysql remote schema", remote_schema_cmd, should_cancel=is_cancel_requested)
                        if code != 0 and _is_set_gtid_unsupported(out):
                            append_log(db, run, "remote mysqldump schema не поддерживает --set-gtid-purged=OFF, повтор без этого флага", level="warning")
                            schema_cmd_retry = [x for x in schema_cmd if not x.startswith("--set-gtid-purged=")]
                            remote_schema_cmd = ssh_base + [f"{_shell_join(schema_cmd_retry)} > {shlex.quote(remote_schema)}"]
                            code, out = run_cmd_logged("mysql remote schema retry", remote_schema_cmd, should_cancel=is_cancel_requested)
                        log_command_output("[remote] mysqldump schema output", out, level="warning" if code != 0 else "info")
                        ensure_command_ok(code, out, "Дамп schema-only таблиц завершился с ошибкой")
                    mark_progress(58, "mysql:remote_bundle", "[remote] Архивация mysql-дампа на удаленном хосте")
                    remote_bundle_files = [shlex.quote(main_file.name)]
                    if structure_tables:
                        remote_bundle_files.append(shlex.quote(schema_file.name))
                    bundle_cmd = ssh_base + [
                        f"tar -czf {shlex.quote(remote_bundle)} -C {shlex.quote(remote_run_dir)} "
                        f"{' '.join(remote_bundle_files)}"
                    ]
                    code, out = run_cmd_logged("mysql remote bundle", bundle_cmd, should_cancel=is_cancel_requested)
                    log_command_output("[remote] tar bundle output", out, level="warning" if code != 0 else "info")
                    ensure_command_ok(code, out, "Не удалось собрать remote tar.gz")
                    verify_bundle_cmd = ssh_base + [f"tar -tzf {shlex.quote(remote_bundle)} >/dev/null"]
                    code, out = run_cmd_logged("mysql remote bundle verify", verify_bundle_cmd, should_cancel=is_cancel_requested)
                    log_command_output("[remote] tar verify output", out, level="warning" if code != 0 else "info")
                    ensure_command_ok(code, out, "Проверка remote tar.gz не пройдена")
                    mark_progress(66, "mysql:transfer_bundle", "[transfer] Копирование mysql_bundle.tar.gz на control center")
                    code, out = run_rsync_logged(
                        "mysql transfer bundle",
                        remote_bundle,
                        local_bundle,
                        identity_file=identity_file,
                        should_cancel=is_cancel_requested,
                    )
                    log_command_output("[transfer] rsync bundle output", out, level="warning" if code != 0 else "info")
                    if code != 0:
                        append_log(db, run, "[transfer] rsync bundle не удался, fallback на scp", level="warning")
                        remote_prefix = _remote_ssh_target(target)
                        code, out = run_cmd_logged(
                            "mysql transfer bundle scp fallback",
                            scp_base + [f"{remote_prefix}:{remote_bundle}", str(local_bundle)],
                            should_cancel=is_cancel_requested,
                        )
                        log_command_output("[transfer] scp bundle output", out, level="warning" if code != 0 else "info")
                    ensure_command_ok(code, out, "Не удалось скачать mysql bundle")
                    mark_progress(72, "mysql:extract_bundle", "[local] Распаковка mysql_bundle.tar.gz")
                    ok, reason = _verify_archive(local_bundle)
                    if not ok:
                        raise RuntimeError(f"Проверка локального mysql bundle не пройдена: {reason}")
                    _safe_extract_tar_gz(local_bundle, host_dir)
                    local_bundle.unlink(missing_ok=True)
                    mark_progress(75, "mysql:ssh_cleanup", f"[remote] Очистка временной директории {remote_run_dir}")
                    cleanup_remote = ssh_base + [f"rm -rf {shlex.quote(remote_run_dir)}"]
                    code, out = run_cmd_logged("mysql remote cleanup", cleanup_remote)
                    log_command_output("[remote] cleanup output", out, level="warning" if code != 0 else "info")
                    if code == 0:
                        remove_remote_cleanup(target, remote_run_dir)
                else:
                    # Local mode: mysqldump runs directly on control_center,
                    # without an intermediate remote directory or tar/scp/rsync.
                    mark_progress(22, "mysql:local_main_dump", "Запуск mysqldump основных БД (local mode)")
                    code, out = run_cmd_to_file_logged(
                        "mysql local main",
                        main_cmd,
                        main_file,
                        should_cancel=is_cancel_requested,
                    )
                    if code != 0 and _is_set_gtid_unsupported(out):
                        append_log(db, run, "mysqldump не поддерживает --set-gtid-purged=OFF, повтор без этого флага", level="warning")
                        main_cmd_retry = [x for x in main_cmd if not x.startswith("--set-gtid-purged=")]
                        code, out = run_cmd_to_file_logged(
                            "mysql local main retry",
                            main_cmd_retry,
                            main_file,
                            should_cancel=is_cancel_requested,
                        )
                    log_command_output("mysqldump main output", out, level="warning" if code != 0 else "info")
                    ensure_command_ok(code, out, "mysqldump основных БД завершился с ошибкой")
                    if structure_tables:
                        dump_schema_db = main_db_list[0]
                        object_list = [_table_name_only(x) for x in structure_tables]
                        mark_progress(52, "mysql:local_schema_dump", f"Дамп schema-only таблиц ({len(object_list)} шт.)")
                        schema_cmd = [
                            "mysqldump",
                            *_mysql_conn_args(target),
                            "--set-gtid-purged=OFF",
                            "--no-data",
                            dump_schema_db,
                            *object_list,
                        ]
                        code, out = run_cmd_to_file_logged(
                            "mysql local schema",
                            schema_cmd,
                            schema_file,
                            should_cancel=is_cancel_requested,
                        )
                        if code != 0 and _is_set_gtid_unsupported(out):
                            append_log(db, run, "mysqldump schema не поддерживает --set-gtid-purged=OFF, повтор без этого флага", level="warning")
                            schema_cmd_retry = [x for x in schema_cmd if not x.startswith("--set-gtid-purged=")]
                            code, out = run_cmd_to_file_logged(
                                "mysql local schema retry",
                                schema_cmd_retry,
                                schema_file,
                                should_cancel=is_cancel_requested,
                            )
                        log_command_output("mysqldump schema output", out, level="warning" if code != 0 else "info")
                        ensure_command_ok(code, out, "Дамп schema-only таблиц завершился с ошибкой")
                mark_progress(78, "mysql:verify", "Проверка локально сохраненных MySQL-дампов")
                ok, reason = _verify_file(main_file)
                if not ok:
                    raise RuntimeError(f"Проверка основного дампа MySQL не пройдена: {reason}")
                if structure_tables:
                    # Only verify the second file when it was expected to exist.
                    ok, reason = _verify_file(schema_file)
                    if not ok:
                        raise RuntimeError(f"Проверка schema-only дампа MySQL не пройдена: {reason}")
                    append_log(db, run, f"Успешно: {main_file.name} и {schema_file.name}")
                else:
                    append_log(db, run, f"Успешно: {main_file.name}")
                # The MySQL scenario runs as one batch per run; no DB loop is needed.
                mark_progress(82, "dump:mysql_full", "MySQL этап завершен")
                break
            elif db_type == "mongodb":
                mongo_base = [
                    "mongodump",
                    "--host",
                    target.db_host,
                    "--port",
                    str(target.db_port),
                    *_mongo_auth_args(target),
                ]
                if db_name != "__all__":
                    mongo_base.extend(["--db", db_name])
                if target.dump_mode == "full":
                    # Mongo full mode can run in two variants:
                    # - dump all databases if the DB list is empty
                    # - dump one specific database
                    if db_name == "__all__":
                        if split_tables:
                            append_log(
                                db,
                                run,
                                "MongoDB: коллекции заданы, но список баз пуст. "
                                "В режиме all-databases коллекции игнорируются.",
                                level="warning",
                            )
                        cmd = mongo_base + ["--out", str(host_dir)]
                        code, out = run_cmd_logged("mongodump all databases", cmd, should_cancel=is_cancel_requested)
                        ensure_command_ok(code, out, "mongodump (all databases) error")
                        ok, reason = _verify_dir_with_files(host_dir)
                        if not ok:
                            raise RuntimeError(f"Проверка дампа MongoDB (all databases) не пройдена: {reason}")
                        append_log(db, run, f"Успешно: all MongoDB databases в {host_dir}")
                    else:
                        if split_tables:
                            append_log(db, run, f"MongoDB: выборочный дамп коллекций ({len(split_tables)}): {', '.join(split_tables)}")
                            for coll in split_tables:
                                coll_cmd = mongo_base + ["--collection", coll, "--out", str(host_dir)]
                                code, out = run_cmd_logged(
                                    f"mongodump collection {db_name}.{coll}",
                                    coll_cmd,
                                    should_cancel=is_cancel_requested,
                                )
                                ensure_command_ok(code, out, f"mongodump (collection {coll}) error for {db_name}")
                        else:
                            cmd = mongo_base + ["--out", str(host_dir)]
                            code, out = run_cmd_logged(f"mongodump full {db_name}", cmd, should_cancel=is_cancel_requested)
                            ensure_command_ok(code, out, f"mongodump error for {db_name}")
                        ok, reason = _verify_dir_with_files(host_dir / db_name)
                        if not ok:
                            raise RuntimeError(f"Проверка дампа MongoDB не пройдена: {reason}")
                        append_log(db, run, f"Успешно: {host_dir / db_name}")
                elif target.dump_mode == "split_excluded_tables":
                    # Mongo split follows the PostgreSQL split idea:
                    # save "everything except selected collections" separately
                    # and store the selected collections in a dedicated dump.
                    if not split_tables:
                        raise RuntimeError("Для режима split_excluded_tables требуется список коллекций")
                    data_dir = host_dir / f"{db_name}_without_selected"
                    selected_dir = host_dir / f"{db_name}_selected"
                    data_cmd = mongo_base + ["--out", str(data_dir)]
                    for coll in split_tables:
                        data_cmd.extend(["--excludeCollection", coll])
                    code, out = run_cmd_logged(
                        f"mongodump split exclude {db_name}",
                        data_cmd,
                        should_cancel=is_cancel_requested,
                    )
                    ensure_command_ok(code, out, f"mongodump (exclude collections) error for {db_name}")
                    for coll in split_tables:
                        coll_cmd = mongo_base + ["--collection", coll, "--out", str(selected_dir)]
                        code, out = run_cmd_logged(
                            f"mongodump split collection {db_name}.{coll}",
                            coll_cmd,
                            should_cancel=is_cancel_requested,
                        )
                        ensure_command_ok(code, out, f"mongodump (collection {coll}) error for {db_name}")
                    ok, reason = _verify_dir_with_files(data_dir / db_name)
                    if not ok:
                        raise RuntimeError(f"Проверка Mongo dump (without selected) не пройдена: {reason}")
                    ok, reason = _verify_dir_with_files(selected_dir / db_name)
                    if not ok:
                        raise RuntimeError(f"Проверка Mongo dump (selected collections) не пройдена: {reason}")
                    append_log(db, run, f"Успешно: {data_dir} и {selected_dir}")
                elif target.dump_mode == "latest_collection_groups":
                    # This mode selects not a fixed collection list but the
                    # "latest groups" according to a naming convention.
                    # First fetch all names via mongosh, then filter them, and
                    # only after that run targeted mongodump per collection.
                    if db_name == "__all__":
                        raise RuntimeError("Для режима latest_collection_groups необходимо указать конкретную MongoDB-базу")
                    prefixes = parse_csv_items(target.mongo_group_prefixes_csv)
                    collection_parts = parse_csv_items(target.mongo_collection_parts_csv)
                    blacklist_values = parse_csv_items(target.mongo_collection_blacklist_csv)
                    latest_count = max(1, int(target.mongo_latest_group_count or 1))
                    group_name_mode = (target.mongo_group_name_mode or "simple").strip().lower()
                    if not prefixes:
                        raise RuntimeError("Для режима latest_collection_groups не заданы префиксы групп MongoDB")
                    mark_progress(base_progress + 8, f"mongo:groups:{db_name}", f"Получение списка коллекций MongoDB для {db_name} через mongosh")
                    mongosh_script = (
                        f"const dbh = db.getSiblingDB({json.dumps(db_name)});"
                        "print(JSON.stringify(dbh.getCollectionNames()));"
                    )
                    list_cmd = [
                        "mongosh",
                        "--quiet",
                        "--host",
                        target.db_host,
                        "--port",
                        str(target.db_port),
                        *_mongosh_auth_args(target),
                        "--eval",
                        mongosh_script,
                    ]
                    code, stdout, stderr = run_cmd_text_logged(
                        f"mongosh list collections {db_name}",
                        list_cmd,
                        should_cancel=is_cancel_requested,
                    )
                    log_command_output("mongosh stderr", stderr, level="warning" if code != 0 else "info")
                    ensure_command_ok(code, stderr, f"mongosh list collections error for {db_name}")
                    stdout_text = (stdout or "").strip()
                    if not stdout_text:
                        raise RuntimeError(f"mongosh не вернул список коллекций для {db_name}")
                    try:
                        collection_names = json.loads(stdout_text.splitlines()[-1])
                    except json.JSONDecodeError as exc:
                        raise RuntimeError(f"Не удалось разобрать список коллекций mongosh для {db_name}: {exc}") from exc
                    if not isinstance(collection_names, list):
                        raise RuntimeError(f"mongosh вернул неожиданный формат списка коллекций для {db_name}")
                    selected_collections, selection_warnings = _mongo_parse_collection_groups(
                        [str(x) for x in collection_names],
                        prefixes,
                        latest_count,
                        blacklist_values,
                        group_name_mode=group_name_mode,
                        collection_parts=collection_parts,
                    )
                    append_log(
                        db,
                        run,
                        f"MongoDB: выбраны коллекции последних групп ({len(selected_collections)}): {', '.join(selected_collections)}",
                    )
                    for warning in selection_warnings:
                        append_log(db, run, warning, level="warning")
                    for collection_name in selected_collections:
                        coll_cmd = mongo_base + ["--collection", collection_name, "--out", str(host_dir)]
                        code, out = run_cmd_logged(
                            f"mongodump latest group collection {db_name}.{collection_name}",
                            coll_cmd,
                            should_cancel=is_cancel_requested,
                        )
                        ensure_command_ok(code, out, f"mongodump (collection {collection_name}) error for {db_name}")
                    ok, reason = _verify_dir_with_files(host_dir / db_name)
                    if not ok:
                        raise RuntimeError(f"Проверка Mongo latest groups dump не пройдена: {reason}")
                    append_log(db, run, f"Успешно: latest MongoDB groups в {host_dir / db_name}")
                else:
                    raise RuntimeError(f"Неизвестный режим дампа для MongoDB: {target.dump_mode}")
            elif db_type == "docker":
                project_name = db_name
                project_host_dir = (target.docker_project_host_dir or "").strip()
                if not project_host_dir:
                    raise RuntimeError("Для Docker backup не задан PROJECT_HOST_DIR")
                if not _remote_ssh_host(target) or not _remote_ssh_user(target):
                    raise RuntimeError("Для Docker backup не заданы SSH host/user")
                if ssh_private_key_file is None:
                    ssh_private_key_file = _build_inline_ssh_private_key_file(target)
                identity_file = str(ssh_private_key_file) if ssh_private_key_file else None
                ssh_base = _with_ssh_password(_ssh_base_args(target, identity_file=identity_file), target, env)
                scp_base = _with_ssh_password(_scp_base_args(target, identity_file=identity_file), target, env)
                remote_tmp_base = _remote_tmp_dir(target, "/tmp/backup_control_center").rstrip("/")
                remote_run_dir = f"{remote_tmp_base}/{target.name}_run{run.id}"
                remote_archive = f"{remote_run_dir}/{project_name}.tar.gz"
                local_archive = archive_path
                add_remote_cleanup(target, remote_run_dir)
                append_log(db, run, f"Docker project backup через SSH: {_remote_ssh_target(target)}")
                project_dir_quoted = shlex.quote(project_host_dir)
                configured_compose_file = (getattr(target, "docker_compose_file", "") or "").strip()
                detected_compose_file = configured_compose_file
                compose_stopped = False
                docker_backup_error: Exception | None = None
                docker_restart_error: str = ""
                try:
                    mark_progress(18, "docker:ssh_prepare", f"[remote] Подготовка директории {remote_run_dir}")
                    mkdir_remote = ssh_base + [f"mkdir -p {shlex.quote(remote_run_dir)}"]
                    code, out = run_cmd_logged("docker remote mkdir", mkdir_remote, should_cancel=is_cancel_requested)
                    log_command_output("[remote] mkdir output", out, level="warning" if code != 0 else "info")
                    ensure_command_ok(code, out, "Не удалось подготовить remote tmp dir для Docker backup")
                    compose_candidates = _docker_compose_candidates(target)
                    quoted_candidates = " ".join(shlex.quote(name) for name in compose_candidates)
                    detect_body = (
                        f"cd {project_dir_quoted} || exit 1\n"
                        f"candidates=({quoted_candidates})\n"
                        "for name in \"${candidates[@]}\"; do\n"
                        "  if [ -f \"$name\" ]; then\n"
                        "    printf '%s\\n' \"$name\"\n"
                        "    exit 0\n"
                        "  fi\n"
                        "done\n"
                        "if [ -n \"$(find . -maxdepth 1 -type f \\( -name '*.yml' -o -name '*.yaml' \\) -printf '.' -quit)\" ]; then\n"
                        "  count=$(find . -maxdepth 1 -type f \\( -name '*.yml' -o -name '*.yaml' \\) | wc -l)\n"
                        "  if [ \"$count\" -eq 1 ]; then\n"
                        "    find . -maxdepth 1 -type f \\( -name '*.yml' -o -name '*.yaml' \\) -printf '%f\\n' -quit\n"
                        "    exit 0\n"
                        "  fi\n"
                        "  echo '__BCC_MULTIPLE_YAMLS__'\n"
                        "  find . -maxdepth 1 -type f \\( -name '*.yml' -o -name '*.yaml' \\) -printf '%f\\n' | sort\n"
                        "  exit 12\n"
                        "fi\n"
                        "exit 11\n"
                    )
                    validate_cmd = ssh_base + [
                        "bash -lc "
                        + shlex.quote(
                            _shell_join(
                                _docker_remote_prefix(
                                    target,
                                    "bash",
                                    "-lc",
                                    f"test -d {project_dir_quoted} && bash -lc {shlex.quote(detect_body)}",
                                )
                            )
                        )
                    ]
                    code, out = run_cmd_logged("docker remote validate", validate_cmd, should_cancel=is_cancel_requested)
                    log_command_output("[remote] validate output", out, level="warning" if code != 0 else "info")
                    detected_output = [line.strip() for line in (out or "").splitlines() if line.strip()]
                    if code == 0 and detected_output:
                        detected_compose_file = detected_output[-1]
                        append_log(db, run, f"Docker compose file: {detected_compose_file}")
                    elif code == 12:
                        raise RuntimeError(
                            "В PROJECT_HOST_DIR найдено несколько .yml/.yaml файлов. "
                            "Укажите Compose file name явно в настройках задачи."
                        )
                    else:
                        ensure_command_ok(
                            code,
                            out,
                            "PROJECT_HOST_DIR не найден, compose-файл отсутствует, либо SSH-пользователь не имеет прав на чтение каталога",
                        )
                    compose_args = ["docker", "compose"]
                    if detected_compose_file:
                        compose_args.extend(["-f", detected_compose_file])
                    if bool(getattr(target, "docker_stop_before_backup", False)):
                        mark_progress(26, "docker:compose_down", f"[remote] Остановка compose-проекта {project_name}")
                        compose_down_cmd = ssh_base + [
                            "bash -lc "
                            + shlex.quote(
                                _shell_join(
                                    _docker_remote_prefix(
                                        target,
                                        "bash",
                                        "-lc",
                                        f"cd {project_dir_quoted} && {_shell_join([*compose_args, 'down'])}",
                                    )
                                )
                            )
                        ]
                        code, out = run_cmd_logged(
                            "docker compose down",
                            compose_down_cmd,
                            should_cancel=is_cancel_requested,
                        )
                        log_command_output("[remote] compose down output", out, level="warning" if code != 0 else "info")
                        ensure_command_ok(code, out, "Не удалось остановить compose-проект перед бэкапом")
                        compose_stopped = True
                    mark_progress(34, "docker:remote_archive", f"[remote] Архивация compose-проекта {project_name}")
                    project_parent = str(Path(project_host_dir).parent)
                    exclude_patterns = parse_csv_items(target.docker_excludes_csv)
                    tar_cmd = _docker_remote_prefix(
                        target,
                        "tar",
                        "-czf",
                        remote_archive,
                    )
                    for pattern in exclude_patterns:
                        tar_cmd.append(f"--exclude={project_name}/{pattern}")
                    tar_cmd.extend(["-C", project_parent, project_name])
                    remote_tar_cmd = ssh_base + [_shell_join(tar_cmd)]
                    code, out = run_cmd_logged("docker remote archive", remote_tar_cmd, should_cancel=is_cancel_requested)
                    log_command_output("[remote] tar archive output", out, level="warning" if code != 0 else "info")
                    ensure_command_ok(code, out, "Не удалось собрать архив compose-проекта")
                    verify_bundle_cmd = ssh_base + [
                        _shell_join(_docker_remote_prefix(target, "tar", "-tzf", remote_archive))
                        + " >/dev/null"
                    ]
                    code, out = run_cmd_logged("docker remote archive verify", verify_bundle_cmd, should_cancel=is_cancel_requested)
                    log_command_output("[remote] tar verify output", out, level="warning" if code != 0 else "info")
                    ensure_command_ok(code, out, "Проверка удаленного Docker-архива не пройдена")
                    mark_progress(58, "docker:transfer_archive", "[transfer] Копирование Docker-архива на control center")
                    code, out = run_rsync_logged(
                        "docker transfer archive",
                        remote_archive,
                        local_archive,
                        identity_file=identity_file,
                        should_cancel=is_cancel_requested,
                    )
                    log_command_output("[transfer] rsync archive output", out, level="warning" if code != 0 else "info")
                    if code != 0:
                        append_log(db, run, "[transfer] rsync archive не удался, fallback на scp", level="warning")
                        remote_prefix = _remote_ssh_target(target)
                        code, out = run_cmd_logged(
                            "docker transfer archive scp fallback",
                            scp_base + [f"{remote_prefix}:{remote_archive}", str(local_archive)],
                            should_cancel=is_cancel_requested,
                        )
                        log_command_output("[transfer] scp archive output", out, level="warning" if code != 0 else "info")
                    ensure_command_ok(code, out, "Не удалось скачать Docker-архив")
                    ok, reason = _verify_archive(local_archive)
                    if not ok:
                        raise RuntimeError(f"Проверка локального Docker-архива не пройдена: {reason}")
                    docker_final_archive = local_archive
                    run.archive_file = str(local_archive)
                    db.commit()
                    append_log(db, run, f"Успешно: {local_archive}")
                    mark_progress(74, "docker:ssh_cleanup", f"[remote] Очистка временной директории {remote_run_dir}")
                    cleanup_remote = ssh_base + [_shell_join(_docker_remote_prefix(target, "rm", "-rf", remote_run_dir))]
                    code, out = run_cmd_logged("docker remote cleanup", cleanup_remote)
                    log_command_output("[remote] cleanup output", out, level="warning" if code != 0 else "info")
                    if code == 0:
                        remove_remote_cleanup(target, remote_run_dir)
                except Exception as exc:
                    docker_backup_error = exc
                finally:
                    if compose_stopped:
                        mark_progress(78, "docker:compose_up", f"[remote] Запуск compose-проекта {project_name}")
                        compose_up_cmd = ssh_base + [
                            "bash -lc "
                            + shlex.quote(
                                _shell_join(
                                    _docker_remote_prefix(
                                        target,
                                        "bash",
                                        "-lc",
                                        f"cd {project_dir_quoted} && {_shell_join([*compose_args, 'up', '-d'])}",
                                    )
                                )
                            )
                        ]
                        code, out = run_cmd_logged("docker compose up -d", compose_up_cmd)
                        log_command_output("[remote] compose up output", out, level="warning" if code != 0 else "info")
                        if code != 0:
                            docker_restart_error = f"Не удалось поднять compose-проект после бэкапа: {out or 'unknown error'}"
                    if docker_backup_error is not None:
                        if docker_restart_error:
                            raise RuntimeError(f"{docker_backup_error}; {docker_restart_error}") from docker_backup_error
                        raise docker_backup_error
                    if docker_restart_error:
                        raise RuntimeError(docker_restart_error)
                mark_progress(82, "dump:docker_full", "Docker project этап завершен")
                break
            else:
                raise RuntimeError(f"Неподдерживаемый тип БД: {db_type}")
            complete_progress = 10 + int((index / max(1, total_dbs)) * 68)
            mark_progress(min(82, complete_progress), f"dump:{display_db_name}")
        ensure_not_canceled("before finalize")
        # After the DB-specific branch finishes, the result is materialized
        # either as an archive or as a directory. The remaining logic is the
        # same for both; only verification and retention differ.
        if db_type == "docker":
            if docker_final_archive is None:
                raise RuntimeError("Docker backup не сформировал локальный архив")
            final_output_path = docker_final_archive
            final_output_type = "archive"
            restic_output_path = final_output_path
            restic_output_type = final_output_type
            mark_progress(86, "archive_verify", "Проверка Docker-архива завершена")
            run.archive_file = str(final_output_path)
            db.commit()
            append_log(db, run, f"Архив проверен: {final_output_path}")
            shutil.rmtree(work_dir, ignore_errors=True)
            append_log(db, run, f"Рабочая директория удалена: {work_dir}")
            mark_progress(95, "retention", "Применение retention-политики")
            keep_days = max(1, int(target.retention_days or 1))
            cutoff_date = now_local_naive().date() - timedelta(days=keep_days - 1)
            append_log(
                db,
                run,
                f"Retention архивов: keep_days={keep_days}, cutoff_date={cutoff_date.isoformat()} (локальные календарные дни)",
            )
            removed = _cleanup_old_archives(
                target_root,
                target.retention_days,
                exclude_paths={final_output_path},
            )
            if removed:
                append_log(db, run, "Удалены старые архивы: " + ", ".join(str(p.name) for p in removed))
            else:
                append_log(db, run, "Retention: старых архивов для удаления не найдено")
        elif target.archive_enabled:
            mark_progress(86, "archive", "Архивация результатов")
            final_output_path, final_output_type = _materialize_backup_output(
                work_dir=work_dir,
                archive_path=archive_path,
                archive_enabled=target.archive_enabled,
            )
            restic_output_path = final_output_path
            restic_output_type = final_output_type
            # In archive mode remove the working directory immediately after
            # tar.gz verification so duplicate data is not kept on disk.
            mark_progress(90, "archive_verify", "Проверка архива")
            run.archive_file = str(final_output_path)
            db.commit()
            append_log(db, run, f"Архив проверен: {final_output_path}")
            shutil.rmtree(work_dir, ignore_errors=True)
            append_log(db, run, f"Рабочая директория удалена: {work_dir}")
            mark_progress(95, "retention", "Применение retention-политики")
            keep_days = max(1, int(target.retention_days or 1))
            cutoff_date = now_local_naive().date() - timedelta(days=keep_days - 1)
            append_log(
                db,
                run,
                f"Retention архивов: keep_days={keep_days}, cutoff_date={cutoff_date.isoformat()} (локальные календарные дни)",
            )
            removed = _cleanup_old_archives(
                target_root,
                target.retention_days,
                exclude_paths={archive_path},
            )
            if removed:
                append_log(db, run, "Удалены старые архивы: " + ", ".join(str(p.name) for p in removed))
            else:
                append_log(db, run, "Retention: старых архивов для удаления не найдено")
        else:
            mark_progress(86, "output_finalize", "Архивирование отключено: проверка каталога результатов")
            # If archiving is disabled, the final run output is the dump
            # directory itself, and tar.gz retention does not apply here.
            final_output_path, final_output_type = _materialize_backup_output(
                work_dir=work_dir,
                archive_path=archive_path,
                archive_enabled=target.archive_enabled,
            )
            restic_output_path = final_output_path
            restic_output_type = final_output_type
            mark_progress(90, "output_verify", "Проверка каталога результатов завершена")
            run.archive_file = ""
            db.commit()
            append_log(db, run, f"Каталог дампа проверен: {final_output_path}")
            mark_progress(95, "retention", "Архивирование отключено: retention архивов пропущен")
            append_log(db, run, "Retention для архивов пропущен: архивирование отключено")
        if target.restic_enabled and target.restic_repository.strip():
            # Restic uploads the final run artifact as-is:
            # either an archive or the whole directory. After success, local
            # artifact cleanup and a separate Restic retention policy may run.
            output_kind = "архива" if restic_output_type == "archive" else "каталога"
            mark_progress(98, "restic", f"Отправка {output_kind} в Restic")
            ok, msg, snapshot_id = send_archive_to_restic(
                archive_path=restic_output_path,
                repository=target.restic_repository,
                password=target.restic_password,
                target_name=target.name,
                run_id=run.id,
                extra_tags=restic_extra_tags,
            )
            run.restic_status = "success" if ok else "failed"
            run.restic_snapshot_id = snapshot_id
            run.restic_message = (msg or "")[:4000]
            run.restic_sent_at = now_local_naive()
            db.commit()
            if ok:
                append_log(db, run, f"Restic: {output_kind} отправлен успешно. snapshot={snapshot_id or '-'}")
                cleanup_ok, cleanup_msg = _cleanup_output_after_restic(restic_output_path, restic_output_type)
                append_log(db, run, cleanup_msg, level="info" if cleanup_ok else "warning")
                if cleanup_ok and restic_output_type == "directory":
                    run.backup_dir = ""
                    db.commit()
                keep_days = max(1, int(target.restic_keep_within_days or 1))
                forget_ok, forget_out = restic_forget_prune(
                    target.restic_repository,
                    target.restic_password,
                    keep_days,
                    tag=target.name,
                )
                if forget_ok:
                    append_log(
                        db,
                        run,
                        f"Restic retention применен: keep-last={keep_days}, tag={target.name}",
                    )
                else:
                    append_log(
                        db,
                        run,
                        f"Restic retention ошибка (keep-last={keep_days}, tag={target.name}): {forget_out}",
                        level="warning",
                    )
                refresh_restic_snapshots_background(
                    target_id=target.id,
                    repository=target.restic_repository,
                    password=target.restic_password,
                )
            else:
                append_log(db, run, f"Restic: ошибка отправки. {msg}", level="warning")
        else:
            run.restic_status = "skipped"
            run.restic_message = "Restic не настроен для цели"
            run.restic_sent_at = now_local_naive()
            db.commit()
        run.status = "success"
        run.progress = 100
        run.step = "finished"
        run.finished_at = now_local_naive()
        db.commit()
        append_log(db, run, "Бэкап завершен успешно")
        logger.info("Run success run_id=%s target=%s output=%s output_type=%s", run.id, target.name, final_output_path, final_output_type)
        output_label = "Архив" if final_output_type == "archive" else "Каталог"
        summary = (
            "#BACKUP_CENTER\n"
            "✅ Успешно\n"
            f"Имя: {target.name}\n"
            f"Тип источника: {db_type}\n"
            f"Хост: {target.db_host}:{target.db_port}\n"
            f"Режим: {target.dump_mode}\n"
            f"{output_label}: {final_output_path}"
        )
        if target.telegram_enabled:
            tg_ok = send_telegram(db, summary)
            if not tg_ok:
                append_log(db, run, "Telegram уведомление не отправлено", level="warning")
                logger.warning("Telegram notification failed run_id=%s target=%s event=success", run.id, target.name)
        if target.email_enabled:
            email_ok = send_email(db, f"Backup success: {target.name}", summary)
            if not email_ok:
                append_log(db, run, "Email уведомление не отправлено", level="warning")
                logger.warning("Email notification failed run_id=%s target=%s event=success", run.id, target.name)
    except BackupCanceledError as exc:
        # Cancellation is treated as its own normal outcome, not an "error".
        # That is why we use the canceled status and separate notification text.
        db.rollback()
        run = db.query(BackupRun).filter(BackupRun.id == run_id).first()
        target = db.query(BackupTarget).filter(BackupTarget.id == run.target_id).first() if run else None
        if run:
            run.status = "canceled"
            run.step = "canceled"
            run.error_message = str(exc)
            run.finished_at = now_local_naive()
            db.commit()
            cancel_line = _last_log_line(str(exc))
            append_log(db, run, f"Отменено: {exc}", level="warning")
            logger.warning("Run canceled run_id=%s target=%s reason=%s", run.id, target.name if target else "-", cancel_line)
        if run and target and target.telegram_enabled:
            db_type = (target.db_type or "postgresql").strip().lower()
            msg = (
                "#BACKUP_CENTER\n"
                "⏹ Отменено\n"
                f"Имя: {target.name}\n"
                f"Тип источника: {db_type}\n"
                f"Хост: {target.db_host}:{target.db_port}\n"
                f"Режим: {target.dump_mode}\n"
                f"Причина: {cancel_line}"
            )
            tg_ok = send_telegram(db, msg, cancel_line)
            if not tg_ok:
                append_log(db, run, "Telegram уведомление об отмене не отправлено", level="warning")
                logger.warning("Telegram notification failed run_id=%s target=%s event=canceled", run.id, target.name)
        if run and target and target.email_enabled:
            db_type = (target.db_type or "postgresql").strip().lower()
            email_msg = (
                "#BACKUP_CENTER\n"
                "⏹ Отменено\n"
                f"Имя: {target.name}\n"
                f"Тип источника: {db_type}\n"
                f"Хост: {target.db_host}:{target.db_port}\n"
                f"Режим: {target.dump_mode}\n"
                f"Причина: {cancel_line}"
            )
            email_ok = send_email(db, f"Backup canceled: {target.name}", email_msg)
            if not email_ok:
                append_log(db, run, "Email уведомление об отмене не отправлено", level="warning")
                logger.warning("Email notification failed run_id=%s target=%s event=canceled", run.id, target.name)
    except Exception as exc:  # noqa: BLE001
        # Any uncaught error moves the run into failed. It is important to
        # reload run/target from the DB because the previous transaction may
        # have been rolled back and current ORM objects may be inconsistent.
        db.rollback()
        run = db.query(BackupRun).filter(BackupRun.id == run_id).first()
        target = db.query(BackupTarget).filter(BackupTarget.id == run.target_id).first() if run else None
        if run:
            run.status = "failed"
            run.progress = max(1, int(run.progress or 0))
            run.step = "failed"
            run.error_message = str(exc)
            run.finished_at = now_local_naive()
            db.commit()
            append_log(db, run, f"Ошибка: {exc}", level="error")
            logger.exception("Run failed run_id=%s target=%s", run.id, target.name if target else "-")
        if run and target:
            last_line = _last_log_line(str(exc))
            db_type = (target.db_type or "postgresql").strip().lower()
            msg = (
                "#BACKUP_CENTER\n"
                "❌ Ошибка\n"
                f"Имя: {target.name}\n"
                f"Тип источника: {db_type}\n"
                f"Хост: {target.db_host}:{target.db_port}\n"
                f"Режим: {target.dump_mode}\n"
                f"Ошибка: {last_line}"
            )
            if target.telegram_enabled:
                tg_ok = send_telegram(db, msg, last_line)
                if not tg_ok:
                    append_log(db, run, "Telegram уведомление об ошибке не отправлено", level="warning")
                    logger.warning("Telegram notification failed run_id=%s target=%s event=failed", run.id, target.name)
            if target.email_enabled:
                email_ok = send_email(db, f"Backup failed: {target.name}", msg)
                if not email_ok:
                    append_log(db, run, "Email уведомление об ошибке не отправлено", level="warning")
                    logger.warning("Email notification failed run_id=%s target=%s event=failed", run.id, target.name)
    finally:
        # Final remote cleanup always runs, even if the main cleanup already
        # succeeded. This is a safety net for crashes in the middle of the SSH
        # flow so temporary directories do not accumulate on target hosts.
        if remote_cleanup_queue:
            run = db.query(BackupRun).filter(BackupRun.id == run_id).first()
            for cleanup_target, remote_path in remote_cleanup_queue:
                cleanup_env = os.environ.copy()
                try:
                    identity_file = str(ssh_private_key_file) if ssh_private_key_file else None
                    cleanup_cmd = _with_ssh_password(_ssh_base_args(cleanup_target, identity_file=identity_file), cleanup_target, cleanup_env) + [
                        f"rm -rf {shlex.quote(remote_path)}"
                    ]
                    code, out = _run_command(cleanup_cmd, cleanup_env)
                    if run:
                        if code == 0:
                            append_log(db, run, f"[remote] cleanup (finally) выполнен: {remote_path}")
                            logger.info("Run cleanup success run_id=%s remote_path=%s", run.id, remote_path)
                        else:
                            append_log(
                                db,
                                run,
                                f"[remote] cleanup (finally) ошибка rc={code}: {out or '-'}",
                                level="warning",
                            )
                            logger.warning("Run cleanup failed run_id=%s remote_path=%s rc=%s out=%s", run.id, remote_path, code, out or "-")
                except Exception as cleanup_exc:  # noqa: BLE001
                    if run:
                        append_log(db, run, f"[remote] cleanup (finally) exception: {cleanup_exc}", level="warning")
                        logger.warning("Run cleanup exception run_id=%s remote_path=%s error=%s", run.id, remote_path, cleanup_exc)
        if ssh_private_key_file is not None:
            ssh_private_key_file.unlink(missing_ok=True)
        db.close()

