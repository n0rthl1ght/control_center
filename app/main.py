from __future__ import annotations

"""FastAPI entry point for Backup Control Center.

Responsible for:
- rendering pages (dashboard, forms, logs)
- CRUD for backup targets and run management
- polling APIs for run status/logs
- notification settings and Restic operations
"""

from pathlib import Path
import shutil
import subprocess
from urllib.parse import quote_plus

from fastapi import Depends, FastAPI, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import func, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .app_logging import get_logger, setup_logging
from .backup_engine import (
    configure_executor,
    enqueue_backup,
    get_executor_diagnostics,
    normalize_max_concurrent_runs,
    parse_csv_items,
)
from .config import DEFAULT_BACKUP_ROOT, DEFAULT_MAX_CONCURRENT_RUNS, DEFAULT_RETENTION_DAYS
from .db import Base, SessionLocal, engine, get_db
from .migrations import run_migrations
from .models import AppSetting, BackupRun, BackupTarget, RunLog
from .notifications import send_email, send_telegram
from .restic_cache import get_cached_snapshots, refresh_restic_snapshots_background, shutdown_restic_cache_pool
from .restic_service import restic_forget_prune, send_archive_to_restic
from .scheduler import build_cron_trigger, scheduler, sync_scheduler_jobs
from .time_utils import now_local_naive

app = FastAPI(title="Backup Control Center", version="0.1.0")

logger = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


DEFAULT_SETTINGS = {
    "telegram_notifier_url": "http://notifier.svc.stage.i-sphere.local/api/v1/messages/telegram",
    "telegram_chat_id": "",
    "telegram_thread_id": "",
    "smtp_host": "",
    "smtp_port": "587",
    "smtp_user": "",
    "smtp_password": "",
    "smtp_from": "",
    "smtp_to": "",
    "smtp_security": "starttls",
    "restic_keep_within_days": "30",
    "max_concurrent_runs": str(DEFAULT_MAX_CONCURRENT_RUNS),
    "local_retention_days": str(DEFAULT_RETENTION_DAYS),
}


def _bool_from_form(value: str | None) -> bool:
    """Convert an HTML checkbox value to bool."""
    return value in {"on", "true", "1", "yes"}


def _normalize_db_type(value: str) -> str:
    db_type = value.strip().lower()
    if db_type not in {"postgresql", "mysql", "mongodb", "docker"}:
        raise HTTPException(status_code=400, detail="Unsupported db_type")
    return db_type


def _normalize_dump_mode(db_type: str, dump_mode: str) -> str:
    mode = (dump_mode or "full").strip().lower()
    if db_type in {"mysql", "docker"}:
        return "full"
    if db_type == "mongodb":
        if mode not in {"full", "split_excluded_tables", "latest_collection_groups"}:
            return "full"
        return mode
    if mode not in {"full", "split_excluded_tables", "custom_excludes"}:
        return "full"
    return mode


def _normalize_mysql_ssh_auth_type(value: str | None) -> str:
    auth_type = (value or "password").strip().lower()
    if auth_type not in {"password", "private_key"}:
        return "password"
    return auth_type


def _normalize_remote_ssh_auth_type(value: str | None) -> str:
    auth_type = (value or "password").strip().lower()
    if auth_type not in {"password", "private_key"}:
        return "password"
    return auth_type


def _derive_docker_project_name(project_host_dir: str) -> str:
    path = (project_host_dir or "").strip().rstrip("/")
    if not path:
        return ""
    return Path(path).name.strip()


def _normalize_docker_compose_file(value: str | None) -> str:
    compose_file = (value or "").strip()
    if not compose_file:
        return ""
    if "/" in compose_file or "\\" in compose_file:
        raise ValueError("Compose file name должен быть именем файла, а не путем")
    if not compose_file.lower().endswith((".yml", ".yaml")):
        raise ValueError("Compose file name должен оканчиваться на .yml или .yaml")
    return compose_file


def _get_or_create_settings(db: Session) -> dict[str, str]:
    for key, val in DEFAULT_SETTINGS.items():
        if not db.query(AppSetting).filter(AppSetting.key == key).first():
            db.add(AppSetting(key=key, value=val))
    db.commit()

    items = db.query(AppSetting).all()
    return {item.key: item.value for item in items}


def _save_settings(db: Session, values: dict[str, str]) -> None:
    for key, val in values.items():
        row = db.query(AppSetting).filter(AppSetting.key == key).first()
        if not row:
            row = AppSetting(key=key, value=val)
            db.add(row)
        else:
            row.value = val
    db.commit()


def _target_name_map(db: Session) -> dict[int, str]:
    rows = db.query(BackupTarget.id, BackupTarget.name).all()
    return {row[0]: row[1] for row in rows}


def _build_restic_snapshots_payload(
    db: Session,
    target_id: int | None = None,
    snapshot_filter: str = "",
) -> dict:
    """Build Restic snapshot data for the selected repository."""
    snapshot_filter = (snapshot_filter or "").strip()
    snapshot_filter_lc = snapshot_filter.lower()

    targets = (
        db.query(BackupTarget)
        .filter(BackupTarget.restic_enabled == True, BackupTarget.restic_repository != "")  # noqa: E712
        .order_by(BackupTarget.name.asc())
        .all()
    )

    repositories_map: dict[str, list[BackupTarget]] = {}
    for t in targets:
        repo = (t.restic_repository or "").strip()
        if not repo:
            continue
        repositories_map.setdefault(repo, []).append(t)

    repository_options: list[dict] = []
    for repo, repo_targets in repositories_map.items():
        representative = next((x for x in repo_targets if (x.restic_password or "").strip()), repo_targets[0])
        label = repo if len(repo_targets) == 1 else f"{repo} ({len(repo_targets)} задач)"
        repository_options.append(
            {
                "id": representative.id,
                "repository": repo,
                "label": label,
                "target_names": [x.name for x in repo_targets],
            }
        )

    repository_options.sort(key=lambda x: str(x.get("label", "")).lower())

    selected_target = None
    if target_id:
        selected_target = next((t for t in targets if t.id == target_id), None)
    if selected_target is None and repository_options:
        selected_target = next((t for t in targets if t.id == repository_options[0]["id"]), None)

    snapshots: list[dict] = []
    snapshots_error = ""
    snapshots_info = ""

    if selected_target and selected_target.restic_repository and selected_target.restic_password:
        rows, snapshots_error, loading = get_cached_snapshots(
            target_id=selected_target.id,
            repository=selected_target.restic_repository,
            password=selected_target.restic_password,
            ttl_sec=120,
        )
        snapshots = sorted(
            rows,
            key=lambda row: (str(row.get("time", "") or ""), str(row.get("id", "") or "")),
            reverse=True,
        )
        if loading:
            snapshots_info = "Снапшоты обновляются в фоне."
    elif selected_target and selected_target.restic_repository and not selected_target.restic_password:
        snapshots_error = "Для выбранного репозитория не задан пароль Restic в задаче-представителе."

    if snapshot_filter_lc and snapshots:
        filtered_rows = []
        for row in snapshots:
            text_blob = " ".join(
                [
                    str(row.get("id", "")),
                    str(row.get("short_id", "")),
                    str(row.get("time", "")),
                    " ".join(row.get("paths", []) or []),
                    " ".join(row.get("tags", []) or []),
                ]
            ).lower()
            if snapshot_filter_lc in text_blob:
                filtered_rows.append(row)
        snapshots = filtered_rows

    return {
        "targets": repository_options,
        "selected_target": selected_target,
        "selected_target_id": selected_target.id if selected_target else None,
        "selected_repository": (selected_target.restic_repository if selected_target else ""),
        "snapshots": snapshots,
        "snapshots_error": snapshots_error,
        "snapshots_info": snapshots_info,
    }


def _build_restic_queue_payload(
    db: Session,
    queue_filter: str = "",
    limit: int = 200,
) -> dict:
    """Build data for the Restic upload queue."""
    queue_filter = (queue_filter or "").strip()
    safe_limit = max(1, min(int(limit), 500))
    completed_runs_query = (
        db.query(BackupRun)
        .join(BackupTarget, BackupTarget.id == BackupRun.target_id)
        .filter(
            BackupRun.status.in_(["success", "failed", "canceled"]),
            BackupTarget.restic_enabled == True,  # noqa: E712
            BackupTarget.restic_repository != "",
        )
    )
    if queue_filter:
        completed_runs_query = completed_runs_query.filter(
            func.lower(BackupTarget.name).contains(queue_filter.lower())
        )
    completed_runs = completed_runs_query.order_by(BackupRun.id.desc()).limit(safe_limit).all()
    target_names = _target_name_map(db)
    return {
        "runs": [
            {
                "id": r.id,
                "target_id": r.target_id,
                "target_name": target_names.get(r.target_id, f"ID {r.target_id}"),
                "status": r.status,
                "restic_status": r.restic_status or "",
                "restic_snapshot_id": r.restic_snapshot_id or "",
                "repository": (db.query(BackupTarget.restic_repository).filter(BackupTarget.id == r.target_id).scalar() or "").strip(),
                "archive_or_dir": (r.archive_file or r.backup_dir or "").strip(),
                "has_payload": bool((r.archive_file or "").strip() or (r.backup_dir or "").strip()),
            }
            for r in completed_runs
        ]
    }


def _next_duplicate_name(db: Session, base_name: str) -> str:
    idx = 1
    while True:
        name = f"{base_name} (copy {idx})"
        exists = db.query(BackupTarget).filter(BackupTarget.name == name).first()
        if not exists:
            return name
        idx += 1


def _list_backup_processes(limit: int = 20) -> list[dict]:
    """Return a list of external processes related to backups."""
    cmd = ["ps", "-eo", "pid=,etimes=,stat=,cmd="]
    try:
        raw = subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT)
    except Exception:
        return []

    keywords = ("pg_dump", "mysqldump", "mongodump", "ssh ", "scp ", "rsync ", "restic ", "rclone ")
    rows: list[dict] = []
    for line in raw.splitlines():
        row = line.strip()
        if not row:
            continue
        lower = row.lower()
        if not any(k in lower for k in keywords):
            continue
        parts = row.split(None, 3)
        if len(parts) < 4:
            continue
        pid, etimes, stat, command = parts
        try:
            rows.append({
                "pid": int(pid),
                "elapsed_sec": int(etimes),
                "state": stat,
                "command": command,
            })
        except ValueError:
            continue

    rows.sort(key=lambda x: x["elapsed_sec"], reverse=True)
    return rows[: max(1, min(limit, 100))]


def _cancel_stale_runs_on_startup(db: Session) -> int:
    """Cancel unfinished runs during service startup."""
    stale_runs = (
        db.query(BackupRun)
        .filter(BackupRun.status.in_(["queued", "running"]))
        .all()
    )
    if not stale_runs:
        return 0

    now = now_local_naive()
    reason = "Отменено при старте сервиса: найден незавершенный запуск из прошлой сессии"
    for run in stale_runs:
        run.status = "canceled"
        run.step = "canceled"
        run.error_message = reason
        run.finished_at = now
        run.cancel_requested = True
        db.add(
            RunLog(
                run_id=run.id,
                level="warning",
                message=reason,
            )
        )
    db.commit()
    return len(stale_runs)


def _send_run_to_restic(db: Session, run: BackupRun, target: BackupTarget) -> tuple[bool, str]:
    """Upload a run artifact to Restic and store the result on the run record."""
    if (target.db_type or "").strip().lower() == "mongodb":
        output_candidate = (run.backup_dir or "").strip() or (run.archive_file or "").strip()
    else:
        output_candidate = (run.archive_file or "").strip() or (run.backup_dir or "").strip()
    output_path = Path(output_candidate)
    extra_tags: list[str] = []
    if (target.db_type or "").strip().lower() == "docker":
        project_name = (target.docker_project_name or "").strip() or _derive_docker_project_name(target.docker_project_host_dir or "")
        if project_name:
            extra_tags.extend([project_name, "docker"])
    ok, message, snapshot_id = send_archive_to_restic(
        archive_path=output_path,
        repository=target.restic_repository,
        password=target.restic_password,
        target_name=target.name,
        run_id=run.id,
        extra_tags=extra_tags,
    )
    run.restic_status = "success" if ok else "failed"
    run.restic_message = message[:4000]
    run.restic_snapshot_id = snapshot_id
    run.restic_sent_at = now_local_naive()
    if ok:
        try:
            if output_path.is_dir():
                shutil.rmtree(output_path, ignore_errors=False)
                run.backup_dir = ""
        except Exception as exc:  # noqa: BLE001
            warn = f" Локальные данные после Restic не удалены: {exc}"
            run.restic_message = (run.restic_message + warn)[:4000]
        keep_days = max(1, int(target.restic_keep_within_days or 1))
        forget_ok, forget_out = restic_forget_prune(
            target.restic_repository,
            target.restic_password,
            keep_days,
            tag=target.name,
        )
        if not forget_ok:
            tail = (forget_out or "").strip()
            if len(tail) > 800:
                tail = tail[-800:]
            warn = (
                f" Upload OK, но автоудаление (keep-last={keep_days}) завершилось с ошибкой."
                + (f" {tail}" if tail else "")
            )
            run.restic_message = (run.restic_message + warn)[:4000]
    db.commit()
    if ok:
        refresh_restic_snapshots_background(
            target_id=target.id,
            repository=target.restic_repository,
            password=target.restic_password,
        )
    return ok, message



@app.on_event("startup")
def on_startup() -> None:
    """Initialize DB schema, scheduler, and stale-run cleanup."""
    setup_logging()
    logger.info("Startup: initialize application")
    Base.metadata.create_all(bind=engine)
    run_migrations()

    db = SessionLocal()
    try:
        settings = _get_or_create_settings(db)
        configured_workers = configure_executor(settings.get("max_concurrent_runs", str(DEFAULT_MAX_CONCURRENT_RUNS)))
        logger.info("Startup: executor max_workers=%s", configured_workers)
        canceled_count = _cancel_stale_runs_on_startup(db)
        if canceled_count:
            logger.warning("Startup: canceled stale runs count=%s", canceled_count)
    finally:
        db.close()

    if not scheduler.running:
        scheduler.start()
        logger.info("Startup: scheduler started")
    sync_scheduler_jobs()
    logger.info("Startup: scheduler jobs synchronized")


@app.on_event("shutdown")
def on_shutdown() -> None:
    """Gracefully stop the background scheduler."""
    logger.info("Shutdown: stopping application")
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Shutdown: scheduler stopped")
    shutdown_restic_cache_pool()
    logger.info("Shutdown: restic cache pool stopped")


@app.get("/")
def dashboard(request: Request, db: Session = Depends(get_db)):
    """Render the dashboard with targets and recent runs."""
    targets = db.query(BackupTarget).order_by(BackupTarget.id.desc()).all()
    runs = db.query(BackupRun).order_by(BackupRun.id.desc()).limit(20).all()
    target_names = _target_name_map(db)
    flash = request.query_params.get("flash", "")
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "targets": targets,
            "runs": runs,
            "target_names": target_names,
            "flash": flash,
        },
    )


@app.get("/api/health/workers")
def api_health_workers(db: Session = Depends(get_db)):
    """Inspect background backup workers without restarting the service."""
    executor = get_executor_diagnostics()
    queued_runs = (
        db.query(BackupRun)
        .filter(BackupRun.status == "queued")
        .order_by(BackupRun.id.asc())
        .limit(50)
        .all()
    )
    running_runs = (
        db.query(BackupRun)
        .filter(BackupRun.status == "running")
        .order_by(BackupRun.started_at.asc())
        .limit(50)
        .all()
    )
    target_names = _target_name_map(db)
    now = now_local_naive()

    running_payload = []
    stale_running_ids = []
    for run in running_runs:
        started = run.started_at
        age_sec = None
        if started is not None:
            age_sec = int((now - started).total_seconds())
            if age_sec > 4 * 3600:
                stale_running_ids.append(run.id)
        running_payload.append(
            {
                "run_id": run.id,
                "target_id": run.target_id,
                "target_name": target_names.get(run.target_id, f"ID {run.target_id}"),
                "progress": int(run.progress or 0),
                "step": run.step or "",
                "started_at": started.isoformat() if started else None,
                "age_sec": age_sec,
                "cancel_requested": bool(run.cancel_requested),
            }
        )

    queued_payload = [
        {
            "run_id": run.id,
            "target_id": run.target_id,
            "target_name": target_names.get(run.target_id, f"ID {run.target_id}"),
            "created_backup_date": run.backup_date,
            "cancel_requested": bool(run.cancel_requested),
        }
        for run in queued_runs
    ]

    return {
        "now": now.isoformat(),
        "scheduler_running": bool(scheduler.running),
        "executor": executor,
        "counts": {
            "queued": len(queued_payload),
            "running": len(running_payload),
            "stale_running_over_4h": len(stale_running_ids),
        },
        "stale_running_ids": stale_running_ids,
        "running_runs": running_payload,
        "queued_runs": queued_payload,
        "processes": _list_backup_processes(limit=30),
    }


@app.get("/api/dashboard/runs")
def api_dashboard_runs(limit: int = 20, db: Session = Depends(get_db)):
    """API for auto-refreshing the recent-runs dashboard block."""
    safe_limit = max(1, min(int(limit), 100))
    runs = db.query(BackupRun).order_by(BackupRun.id.desc()).limit(safe_limit).all()
    target_names = _target_name_map(db)
    return {
        "runs": [
            {
                "id": r.id,
                "target_id": r.target_id,
                "target_name": target_names.get(r.target_id, f"ID {r.target_id}"),
                "status": r.status,
                "progress": r.progress,
                "step": r.step,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                "display_time": (r.finished_at or r.started_at).isoformat() if (r.finished_at or r.started_at) else None,
            }
            for r in runs
        ]
    }


@app.get("/targets/new")
def target_new_page(request: Request, db: Session = Depends(get_db)):
    """Render an empty target creation form."""
    flash = request.query_params.get("flash", "")
    settings = _get_or_create_settings(db)
    restic_default_keep_within_days = settings.get("restic_keep_within_days", "30")
    local_retention_days = settings.get("local_retention_days", str(DEFAULT_RETENTION_DAYS))
    return templates.TemplateResponse(
        "target_form.html",
        {
            "request": request,
            "target": None,
            "default_backup_root": DEFAULT_BACKUP_ROOT,
            "default_retention": max(1, int(local_retention_days or DEFAULT_RETENTION_DAYS)),
            "restic_default_keep_within_days": restic_default_keep_within_days,
            "flash": flash,
        },
    )


@app.get("/targets/{target_id}/edit")
def target_edit_page(target_id: int, request: Request, db: Session = Depends(get_db)):
    """Render the target edit form with current values."""
    target = db.query(BackupTarget).filter(BackupTarget.id == target_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Target not found")
    flash = request.query_params.get("flash", "")
    settings = _get_or_create_settings(db)
    restic_default_keep_within_days = settings.get("restic_keep_within_days", "30")
    local_retention_days = settings.get("local_retention_days", str(DEFAULT_RETENTION_DAYS))
    return templates.TemplateResponse(
        "target_form.html",
        {
            "request": request,
            "target": target,
            "default_backup_root": DEFAULT_BACKUP_ROOT,
            "default_retention": max(1, int(local_retention_days or DEFAULT_RETENTION_DAYS)),
            "restic_default_keep_within_days": restic_default_keep_within_days,
            "flash": flash,
        },
    )


@app.post("/targets")
def create_target(
    name: str = Form(...),
    enabled: str | None = Form(None),
    db_type: str = Form("postgresql"),
    db_host: str = Form(...),
    db_port: int = Form(5432),
    db_user: str = Form(""),
    db_password: str = Form(""),
    mongo_auth_db: str = Form("admin"),
    mongo_group_name_mode: str = Form("simple"),
    mongo_group_prefixes_csv: str = Form(""),
    mongo_collection_parts_csv: str = Form(""),
    mongo_latest_group_count: int = Form(3),
    mongo_collection_blacklist_csv: str = Form(""),
    mysql_ignore_tables_csv: str = Form(""),
    mysql_structure_tables_csv: str = Form(""),
    mysql_use_ssh: str | None = Form(None),
    remote_ssh_host: str = Form(""),
    remote_ssh_port: int = Form(22),
    remote_ssh_user: str = Form(""),
    remote_ssh_auth_type: str = Form("password"),
    remote_ssh_private_key: str = Form(""),
    remote_ssh_password: str = Form(""),
    remote_ssh_remote_tmp_dir: str = Form("/tmp/backup_control_center"),
    docker_project_host_dir: str = Form(""),
    docker_compose_file: str = Form(""),
    docker_excludes_csv: str = Form(""),
    docker_stop_before_backup: str | None = Form(None),
    docker_use_sudo: str | None = Form(None),
    databases_csv: str = Form(""),
    split_tables_csv: str = Form(""),
    pg_exclude_tables_csv: str = Form(""),
    pg_exclude_table_data_csv: str = Form(""),
    pg_exclude_schemas_csv: str = Form(""),
    dump_mode: str = Form("full"),
    backup_root: str = Form(DEFAULT_BACKUP_ROOT),
    retention_days: int = Form(DEFAULT_RETENTION_DAYS),
    archive_enabled: str | None = Form(None),
    transfer_limit_kbps: int = Form(0),
    restic_enabled: str | None = Form(None),
    restic_repository: str = Form(""),
    restic_password: str = Form(""),
    restic_keep_within_days: int = Form(30),
    cron_expr: str = Form(""),
    telegram_enabled: str | None = Form(None),
    email_enabled: str | None = Form(None),
    db: Session = Depends(get_db),
):
    """Create a new backup target."""
    normalized_db_type = _normalize_db_type(db_type)
    normalized_dump_mode = _normalize_dump_mode(normalized_db_type, dump_mode)
    normalized_remote_ssh_auth_type = _normalize_remote_ssh_auth_type(remote_ssh_auth_type)
    normalized_databases = ",".join(parse_csv_items(databases_csv))
    normalized_name = name.strip()
    mysql_use_ssh_enabled = _bool_from_form(mysql_use_ssh)
    normalized_remote_ssh_host = remote_ssh_host.strip()
    normalized_remote_ssh_user = remote_ssh_user.strip()
    normalized_docker_project_host_dir = docker_project_host_dir.strip()
    normalized_docker_project_name = _derive_docker_project_name(normalized_docker_project_host_dir)
    try:
        normalized_docker_compose_file = _normalize_docker_compose_file(docker_compose_file)
    except ValueError as exc:
        msg = quote_plus(str(exc))
        return RedirectResponse(url=f"/targets/new?flash={msg}", status_code=303)
    if normalized_db_type in {"postgresql", "mysql"} and not normalized_databases:
        msg = quote_plus("Для PostgreSQL/MySQL необходимо указать список баз")
        return RedirectResponse(url=f"/targets/new?flash={msg}", status_code=303)
    if normalized_db_type == "mongodb" and normalized_dump_mode == "latest_collection_groups" and not normalized_databases:
        msg = quote_plus("Для MongoDB latest_collection_groups необходимо указать хотя бы одну базу")
        return RedirectResponse(url=f"/targets/new?flash={msg}", status_code=303)
    if normalized_db_type == "mysql" and mysql_use_ssh_enabled:
        if not normalized_remote_ssh_host or not normalized_remote_ssh_user:
            msg = quote_plus("Для MySQL SSH-режима необходимо заполнить SSH host и SSH user")
            return RedirectResponse(url=f"/targets/new?flash={msg}", status_code=303)
        if normalized_remote_ssh_auth_type == "password" and not remote_ssh_password:
            msg = quote_plus("Для MySQL SSH password-аутентификации необходимо заполнить SSH password")
            return RedirectResponse(url=f"/targets/new?flash={msg}", status_code=303)
        if normalized_remote_ssh_auth_type == "private_key" and not remote_ssh_private_key.strip():
            msg = quote_plus("Для MySQL SSH key-аутентификации необходимо вставить приватный ключ")
            return RedirectResponse(url=f"/targets/new?flash={msg}", status_code=303)
    if normalized_db_type == "docker":
        if not normalized_docker_project_host_dir:
            msg = quote_plus("Для Docker backup необходимо указать PROJECT_HOST_DIR")
            return RedirectResponse(url=f"/targets/new?flash={msg}", status_code=303)
        if not normalized_docker_project_name:
            msg = quote_plus("Не удалось определить PROJECT_NAME из PROJECT_HOST_DIR")
            return RedirectResponse(url=f"/targets/new?flash={msg}", status_code=303)
        if not normalized_remote_ssh_host or not normalized_remote_ssh_user:
            msg = quote_plus("Для Docker backup необходимо заполнить SSH host и SSH user")
            return RedirectResponse(url=f"/targets/new?flash={msg}", status_code=303)
        if normalized_remote_ssh_auth_type == "password" and not remote_ssh_password:
            msg = quote_plus("Для Docker SSH password-аутентификации необходимо заполнить SSH password")
            return RedirectResponse(url=f"/targets/new?flash={msg}", status_code=303)
        if normalized_remote_ssh_auth_type == "private_key" and not remote_ssh_private_key.strip():
            msg = quote_plus("Для Docker SSH key-аутентификации необходимо вставить приватный ключ")
            return RedirectResponse(url=f"/targets/new?flash={msg}", status_code=303)
    normalized_cron_expr = cron_expr.strip()
    if normalized_cron_expr:
        try:
            build_cron_trigger(normalized_cron_expr)
        except ValueError as exc:
            msg = quote_plus(f"Некорректный Cron: {exc}")
            return RedirectResponse(url=f"/targets/new?flash={msg}", status_code=303)
    existing = db.query(BackupTarget).filter(BackupTarget.name == normalized_name).first()
    if existing:
        msg = quote_plus("Задача с таким именем уже существует")
        return RedirectResponse(url=f"/targets/new?flash={msg}", status_code=303)

    target = BackupTarget(
        name=normalized_name,
        enabled=_bool_from_form(enabled),
        db_type=normalized_db_type,
        db_host=(normalized_remote_ssh_host if normalized_db_type == "docker" else db_host.strip()),
        db_port=(remote_ssh_port if normalized_db_type == "docker" else db_port),
        db_user=("" if normalized_db_type == "docker" else db_user.strip()),
        db_password=("" if normalized_db_type == "docker" else db_password),
        mongo_auth_db=mongo_auth_db.strip() or "admin",
        mongo_group_name_mode=(mongo_group_name_mode or "simple").strip().lower(),
        mongo_group_prefixes_csv=",".join(parse_csv_items(mongo_group_prefixes_csv)),
        mongo_collection_parts_csv=",".join(parse_csv_items(mongo_collection_parts_csv)),
        mongo_latest_group_count=max(1, int(mongo_latest_group_count)),
        mongo_collection_blacklist_csv=",".join(parse_csv_items(mongo_collection_blacklist_csv)),
        mysql_ignore_tables_csv=",".join(parse_csv_items(mysql_ignore_tables_csv)),
        mysql_structure_tables_csv=",".join(parse_csv_items(mysql_structure_tables_csv)),
        mysql_use_ssh=mysql_use_ssh_enabled,
        mysql_ssh_host=normalized_remote_ssh_host if normalized_db_type == "mysql" else "",
        mysql_ssh_port=remote_ssh_port if normalized_db_type == "mysql" else 22,
        mysql_ssh_user=normalized_remote_ssh_user if normalized_db_type == "mysql" else "",
        mysql_ssh_auth_type=normalized_remote_ssh_auth_type,
        mysql_ssh_key_path="",
        mysql_ssh_private_key=remote_ssh_private_key.strip() if normalized_db_type == "mysql" else "",
        mysql_ssh_password=remote_ssh_password if normalized_db_type == "mysql" and normalized_remote_ssh_auth_type == "password" else "",
        mysql_ssh_remote_tmp_dir=(remote_ssh_remote_tmp_dir.strip() or "/tmp/backup_control_center") if normalized_db_type == "mysql" else "/tmp/mysql_backup_agent",
        remote_ssh_host=normalized_remote_ssh_host,
        remote_ssh_port=remote_ssh_port,
        remote_ssh_user=normalized_remote_ssh_user,
        remote_ssh_auth_type=normalized_remote_ssh_auth_type,
        remote_ssh_private_key=remote_ssh_private_key.strip(),
        remote_ssh_password=remote_ssh_password if normalized_remote_ssh_auth_type == "password" else "",
        remote_ssh_remote_tmp_dir=remote_ssh_remote_tmp_dir.strip() or "/tmp/backup_control_center",
        databases_csv=normalized_databases,
        split_tables_csv=",".join(parse_csv_items(split_tables_csv)),
        pg_exclude_tables_csv=",".join(parse_csv_items(pg_exclude_tables_csv)),
        pg_exclude_table_data_csv=",".join(parse_csv_items(pg_exclude_table_data_csv)),
        pg_exclude_schemas_csv=",".join(parse_csv_items(pg_exclude_schemas_csv)),
        dump_mode=normalized_dump_mode,
        backup_root=backup_root.strip() or DEFAULT_BACKUP_ROOT,
        retention_days=max(1, int(retention_days)),
        archive_enabled=_bool_from_form(archive_enabled),
        transfer_limit_kbps=max(0, transfer_limit_kbps),
        docker_project_name=normalized_docker_project_name,
        docker_project_host_dir=normalized_docker_project_host_dir,
        docker_compose_file=normalized_docker_compose_file,
        docker_excludes_csv=",".join(parse_csv_items(docker_excludes_csv)),
        docker_stop_before_backup=_bool_from_form(docker_stop_before_backup),
        docker_use_sudo=_bool_from_form(docker_use_sudo),
        restic_enabled=_bool_from_form(restic_enabled),
        restic_repository=restic_repository.strip(),
        restic_password=restic_password,
        restic_keep_within_days=max(1, int(restic_keep_within_days)),
        cron_expr=normalized_cron_expr,
        telegram_enabled=_bool_from_form(telegram_enabled),
        email_enabled=_bool_from_form(email_enabled),
    )
    db.add(target)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        msg = quote_plus("Не удалось сохранить: имя задачи должно быть уникальным")
        return RedirectResponse(url=f"/targets/new?flash={msg}", status_code=303)
    sync_scheduler_jobs()
    return RedirectResponse(url="/", status_code=303)


@app.post("/targets/{target_id}")
def update_target(
    target_id: int,
    name: str = Form(...),
    enabled: str | None = Form(None),
    db_type: str = Form("postgresql"),
    db_host: str = Form(...),
    db_port: int = Form(5432),
    db_user: str = Form(""),
    db_password: str = Form(""),
    mongo_auth_db: str = Form("admin"),
    mongo_group_name_mode: str = Form("simple"),
    mongo_group_prefixes_csv: str = Form(""),
    mongo_collection_parts_csv: str = Form(""),
    mongo_latest_group_count: int = Form(3),
    mongo_collection_blacklist_csv: str = Form(""),
    mysql_ignore_tables_csv: str = Form(""),
    mysql_structure_tables_csv: str = Form(""),
    mysql_use_ssh: str | None = Form(None),
    remote_ssh_host: str = Form(""),
    remote_ssh_port: int = Form(22),
    remote_ssh_user: str = Form(""),
    remote_ssh_auth_type: str = Form("password"),
    remote_ssh_private_key: str = Form(""),
    remote_ssh_password: str = Form(""),
    remote_ssh_remote_tmp_dir: str = Form("/tmp/backup_control_center"),
    docker_project_host_dir: str = Form(""),
    docker_compose_file: str = Form(""),
    docker_excludes_csv: str = Form(""),
    docker_stop_before_backup: str | None = Form(None),
    docker_use_sudo: str | None = Form(None),
    databases_csv: str = Form(""),
    split_tables_csv: str = Form(""),
    pg_exclude_tables_csv: str = Form(""),
    pg_exclude_table_data_csv: str = Form(""),
    pg_exclude_schemas_csv: str = Form(""),
    dump_mode: str = Form("full"),
    backup_root: str = Form(DEFAULT_BACKUP_ROOT),
    retention_days: int = Form(DEFAULT_RETENTION_DAYS),
    archive_enabled: str | None = Form(None),
    transfer_limit_kbps: int = Form(0),
    restic_enabled: str | None = Form(None),
    restic_repository: str = Form(""),
    restic_password: str = Form(""),
    restic_keep_within_days: int = Form(30),
    cron_expr: str = Form(""),
    telegram_enabled: str | None = Form(None),
    email_enabled: str | None = Form(None),
    db: Session = Depends(get_db),
):
    """Update an existing backup target."""
    normalized_db_type = _normalize_db_type(db_type)
    normalized_dump_mode = _normalize_dump_mode(normalized_db_type, dump_mode)
    normalized_remote_ssh_auth_type = _normalize_remote_ssh_auth_type(remote_ssh_auth_type)
    normalized_databases = ",".join(parse_csv_items(databases_csv))
    mysql_use_ssh_enabled = _bool_from_form(mysql_use_ssh)
    normalized_remote_ssh_host = remote_ssh_host.strip()
    normalized_remote_ssh_user = remote_ssh_user.strip()
    normalized_docker_project_host_dir = docker_project_host_dir.strip()
    try:
        normalized_docker_compose_file = _normalize_docker_compose_file(docker_compose_file)
    except ValueError as exc:
        msg = quote_plus(str(exc))
        return RedirectResponse(url=f"/targets/{target_id}/edit?flash={msg}", status_code=303)
    target = db.query(BackupTarget).filter(BackupTarget.id == target_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Target not found")

    normalized_name = name.strip()
    existing = (
        db.query(BackupTarget)
        .filter(BackupTarget.name == normalized_name, BackupTarget.id != target_id)
        .first()
    )
    if existing:
        msg = quote_plus("Задача с таким именем уже существует")
        return RedirectResponse(url=f"/targets/{target_id}/edit?flash={msg}", status_code=303)
    if normalized_db_type in {"postgresql", "mysql"} and not normalized_databases:
        msg = quote_plus("Для PostgreSQL/MySQL необходимо указать список баз")
        return RedirectResponse(url=f"/targets/{target_id}/edit?flash={msg}", status_code=303)
    if normalized_db_type == "mongodb" and normalized_dump_mode == "latest_collection_groups" and not normalized_databases:
        msg = quote_plus("Для MongoDB latest_collection_groups необходимо указать хотя бы одну базу")
        return RedirectResponse(url=f"/targets/{target_id}/edit?flash={msg}", status_code=303)
    normalized_docker_project_name = _derive_docker_project_name(normalized_docker_project_host_dir)
    if normalized_db_type == "mysql" and mysql_use_ssh_enabled:
        if not normalized_remote_ssh_host or not normalized_remote_ssh_user:
            msg = quote_plus("Для MySQL SSH-режима необходимо заполнить SSH host и SSH user")
            return RedirectResponse(url=f"/targets/{target_id}/edit?flash={msg}", status_code=303)
        if normalized_remote_ssh_auth_type == "password" and not remote_ssh_password:
            msg = quote_plus("Для MySQL SSH password-аутентификации необходимо заполнить SSH password")
            return RedirectResponse(url=f"/targets/{target_id}/edit?flash={msg}", status_code=303)
        if (
            normalized_remote_ssh_auth_type == "private_key"
            and not remote_ssh_private_key.strip()
            and not (target.remote_ssh_private_key or "").strip()
            and not (target.mysql_ssh_key_path or "").strip()
        ):
            msg = quote_plus("Для MySQL SSH key-аутентификации необходимо вставить приватный ключ")
            return RedirectResponse(url=f"/targets/{target_id}/edit?flash={msg}", status_code=303)
    if normalized_db_type == "docker":
        if not normalized_docker_project_host_dir:
            msg = quote_plus("Для Docker backup необходимо указать PROJECT_HOST_DIR")
            return RedirectResponse(url=f"/targets/{target_id}/edit?flash={msg}", status_code=303)
        if not normalized_docker_project_name:
            msg = quote_plus("Не удалось определить PROJECT_NAME из PROJECT_HOST_DIR")
            return RedirectResponse(url=f"/targets/{target_id}/edit?flash={msg}", status_code=303)
        if not normalized_remote_ssh_host or not normalized_remote_ssh_user:
            msg = quote_plus("Для Docker backup необходимо заполнить SSH host и SSH user")
            return RedirectResponse(url=f"/targets/{target_id}/edit?flash={msg}", status_code=303)
        if normalized_remote_ssh_auth_type == "password" and not remote_ssh_password:
            msg = quote_plus("Для Docker SSH password-аутентификации необходимо заполнить SSH password")
            return RedirectResponse(url=f"/targets/{target_id}/edit?flash={msg}", status_code=303)
        if (
            normalized_remote_ssh_auth_type == "private_key"
            and not remote_ssh_private_key.strip()
            and not (target.remote_ssh_private_key or "").strip()
        ):
            msg = quote_plus("Для Docker SSH key-аутентификации необходимо вставить приватный ключ")
            return RedirectResponse(url=f"/targets/{target_id}/edit?flash={msg}", status_code=303)
    normalized_cron_expr = cron_expr.strip()
    if normalized_cron_expr:
        try:
            build_cron_trigger(normalized_cron_expr)
        except ValueError as exc:
            msg = quote_plus(f"Некорректный Cron: {exc}")
            return RedirectResponse(url=f"/targets/{target_id}/edit?flash={msg}", status_code=303)

    target.name = normalized_name
    target.enabled = _bool_from_form(enabled)
    target.db_type = normalized_db_type
    target.db_host = normalized_remote_ssh_host if normalized_db_type == "docker" else db_host.strip()
    target.db_port = remote_ssh_port if normalized_db_type == "docker" else db_port
    target.db_user = "" if normalized_db_type == "docker" else db_user.strip()
    target.db_password = "" if normalized_db_type == "docker" else db_password
    target.mongo_auth_db = mongo_auth_db.strip() or "admin"
    target.mongo_group_name_mode = (mongo_group_name_mode or "simple").strip().lower()
    target.mongo_group_prefixes_csv = ",".join(parse_csv_items(mongo_group_prefixes_csv))
    target.mongo_collection_parts_csv = ",".join(parse_csv_items(mongo_collection_parts_csv))
    target.mongo_latest_group_count = max(1, int(mongo_latest_group_count))
    target.mongo_collection_blacklist_csv = ",".join(parse_csv_items(mongo_collection_blacklist_csv))
    target.mysql_ignore_tables_csv = ",".join(parse_csv_items(mysql_ignore_tables_csv))
    target.mysql_structure_tables_csv = ",".join(parse_csv_items(mysql_structure_tables_csv))
    target.mysql_use_ssh = mysql_use_ssh_enabled
    target.mysql_ssh_host = normalized_remote_ssh_host if normalized_db_type == "mysql" else ""
    target.mysql_ssh_port = remote_ssh_port if normalized_db_type == "mysql" else 22
    target.mysql_ssh_user = normalized_remote_ssh_user if normalized_db_type == "mysql" else ""
    target.mysql_ssh_auth_type = normalized_remote_ssh_auth_type
    if normalized_remote_ssh_auth_type == "private_key":
        target.mysql_ssh_private_key = remote_ssh_private_key.strip() if normalized_db_type == "mysql" else ""
        target.mysql_ssh_password = ""
        if remote_ssh_private_key.strip():
            target.mysql_ssh_key_path = ""
    else:
        target.mysql_ssh_password = remote_ssh_password if normalized_db_type == "mysql" else ""
        target.mysql_ssh_private_key = ""
        if normalized_db_type == "mysql":
            target.mysql_ssh_key_path = ""
    target.mysql_ssh_remote_tmp_dir = (remote_ssh_remote_tmp_dir.strip() or "/tmp/backup_control_center") if normalized_db_type == "mysql" else "/tmp/mysql_backup_agent"
    target.remote_ssh_host = normalized_remote_ssh_host
    target.remote_ssh_port = remote_ssh_port
    target.remote_ssh_user = normalized_remote_ssh_user
    target.remote_ssh_auth_type = normalized_remote_ssh_auth_type
    if normalized_remote_ssh_auth_type == "private_key":
        if remote_ssh_private_key.strip():
            target.remote_ssh_private_key = remote_ssh_private_key.strip()
        target.remote_ssh_password = ""
    else:
        target.remote_ssh_password = remote_ssh_password
        target.remote_ssh_private_key = ""
    target.remote_ssh_remote_tmp_dir = remote_ssh_remote_tmp_dir.strip() or "/tmp/backup_control_center"
    target.databases_csv = normalized_databases
    target.split_tables_csv = ",".join(parse_csv_items(split_tables_csv))
    target.pg_exclude_tables_csv = ",".join(parse_csv_items(pg_exclude_tables_csv))
    target.pg_exclude_table_data_csv = ",".join(parse_csv_items(pg_exclude_table_data_csv))
    target.pg_exclude_schemas_csv = ",".join(parse_csv_items(pg_exclude_schemas_csv))
    target.dump_mode = normalized_dump_mode
    target.backup_root = backup_root.strip() or DEFAULT_BACKUP_ROOT
    target.retention_days = max(1, int(retention_days))
    target.archive_enabled = _bool_from_form(archive_enabled)
    target.transfer_limit_kbps = max(0, transfer_limit_kbps)
    target.docker_project_name = normalized_docker_project_name
    target.docker_project_host_dir = normalized_docker_project_host_dir
    target.docker_compose_file = normalized_docker_compose_file
    target.docker_excludes_csv = ",".join(parse_csv_items(docker_excludes_csv))
    target.docker_stop_before_backup = _bool_from_form(docker_stop_before_backup)
    target.docker_use_sudo = _bool_from_form(docker_use_sudo)
    target.restic_enabled = _bool_from_form(restic_enabled)
    target.restic_repository = restic_repository.strip()
    target.restic_password = restic_password
    target.restic_keep_within_days = max(1, int(restic_keep_within_days))
    target.cron_expr = normalized_cron_expr
    target.telegram_enabled = _bool_from_form(telegram_enabled)
    target.email_enabled = _bool_from_form(email_enabled)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        msg = quote_plus("Не удалось сохранить: имя задачи должно быть уникальным")
        return RedirectResponse(url=f"/targets/{target_id}/edit?flash={msg}", status_code=303)
    sync_scheduler_jobs()
    return RedirectResponse(url="/", status_code=303)


@app.post("/targets/{target_id}/delete")
def delete_target(target_id: int, db: Session = Depends(get_db)):
    """Delete a target and its related data."""
    target = db.query(BackupTarget).filter(BackupTarget.id == target_id).first()
    if target:
        db.delete(target)
        db.commit()
    sync_scheduler_jobs()
    return RedirectResponse(url="/", status_code=303)


@app.post("/targets/{target_id}/run")
def run_target(target_id: int, db: Session = Depends(get_db)):
    """Queue a manual run and redirect to the run page."""
    target = db.query(BackupTarget).filter(BackupTarget.id == target_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Target not found")

    run_id = enqueue_backup(target.id, "manual")
    return RedirectResponse(url=f"/runs/{run_id}", status_code=303)


@app.post("/targets/{target_id}/duplicate")
def duplicate_target(target_id: int, db: Session = Depends(get_db)):
    """Create an editable copy of an existing target."""
    source = db.query(BackupTarget).filter(BackupTarget.id == target_id).first()
    if not source:
        raise HTTPException(status_code=404, detail="Target not found")

    new_target = BackupTarget(
        name=_next_duplicate_name(db, source.name),
        enabled=source.enabled,
        db_type=source.db_type,
        db_host=source.db_host,
        db_port=source.db_port,
        db_user=source.db_user,
        db_password=source.db_password,
        mongo_auth_db=source.mongo_auth_db,
        mongo_group_name_mode=source.mongo_group_name_mode,
        mongo_group_prefixes_csv=source.mongo_group_prefixes_csv,
        mongo_collection_parts_csv=source.mongo_collection_parts_csv,
        mongo_latest_group_count=source.mongo_latest_group_count,
        mongo_collection_blacklist_csv=source.mongo_collection_blacklist_csv,
        mysql_ignore_tables_csv=source.mysql_ignore_tables_csv,
        mysql_structure_tables_csv=source.mysql_structure_tables_csv,
        mysql_use_ssh=source.mysql_use_ssh,
        mysql_ssh_host=source.mysql_ssh_host,
        mysql_ssh_port=source.mysql_ssh_port,
        mysql_ssh_user=source.mysql_ssh_user,
        mysql_ssh_auth_type=source.mysql_ssh_auth_type,
        mysql_ssh_key_path=source.mysql_ssh_key_path,
        mysql_ssh_private_key=source.mysql_ssh_private_key,
        mysql_ssh_password=source.mysql_ssh_password,
        mysql_ssh_remote_tmp_dir=source.mysql_ssh_remote_tmp_dir,
        remote_ssh_host=source.remote_ssh_host,
        remote_ssh_port=source.remote_ssh_port,
        remote_ssh_user=source.remote_ssh_user,
        remote_ssh_auth_type=source.remote_ssh_auth_type,
        remote_ssh_private_key=source.remote_ssh_private_key,
        remote_ssh_password=source.remote_ssh_password,
        remote_ssh_remote_tmp_dir=source.remote_ssh_remote_tmp_dir,
        databases_csv=source.databases_csv,
        split_tables_csv=source.split_tables_csv,
        pg_exclude_tables_csv=source.pg_exclude_tables_csv,
        pg_exclude_table_data_csv=source.pg_exclude_table_data_csv,
        pg_exclude_schemas_csv=source.pg_exclude_schemas_csv,
        dump_mode=_normalize_dump_mode(source.db_type, source.dump_mode),
        backup_root=source.backup_root,
        retention_days=source.retention_days,
        archive_enabled=source.archive_enabled,
        transfer_limit_kbps=source.transfer_limit_kbps,
        docker_project_name=source.docker_project_name,
        docker_project_host_dir=source.docker_project_host_dir,
        docker_compose_file=source.docker_compose_file,
        docker_excludes_csv=source.docker_excludes_csv,
        docker_stop_before_backup=source.docker_stop_before_backup,
        docker_use_sudo=source.docker_use_sudo,
        restic_enabled=source.restic_enabled,
        restic_repository=source.restic_repository,
        restic_password=source.restic_password,
        restic_keep_within_days=source.restic_keep_within_days,
        cron_expr=source.cron_expr,
        telegram_enabled=source.telegram_enabled,
        email_enabled=source.email_enabled,
    )
    db.add(new_target)
    db.commit()
    db.refresh(new_target)
    sync_scheduler_jobs()
    return RedirectResponse(url=f"/targets/{new_target.id}/edit", status_code=303)


@app.post("/targets/bulk/status")
def bulk_targets_status(
    action: str = Form(...),
    target_ids: list[int] = Form([]),
    db: Session = Depends(get_db),
):
    """Bulk-enable or bulk-disable selected targets."""
    ids = [int(x) for x in target_ids if str(x).strip()]
    if not ids:
        return RedirectResponse(url=f"/?flash={quote_plus('Не выбрано ни одной задачи')}", status_code=303)

    if action == "activate":
        enabled_value = True
        flash = "Выбранные задачи активированы"
    elif action == "deactivate":
        enabled_value = False
        flash = "Выбранные задачи деактивированы"
    else:
        raise HTTPException(status_code=400, detail="Unsupported bulk action")

    rows = db.query(BackupTarget).filter(BackupTarget.id.in_(ids)).all()
    for row in rows:
        row.enabled = enabled_value
    db.commit()
    sync_scheduler_jobs()
    return RedirectResponse(url=f"/?flash={quote_plus(flash)}", status_code=303)


@app.post("/runs/{run_id}/cancel")
def cancel_run(run_id: int, db: Session = Depends(get_db)):
    """Request cancellation for a queued or running manual/scheduled run."""
    run = db.query(BackupRun).filter(BackupRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if run.status not in {"queued", "running"}:
        return RedirectResponse(url=f"/runs/{run_id}", status_code=303)

    run.cancel_requested = True
    db.commit()
    return RedirectResponse(url=f"/runs/{run_id}", status_code=303)


@app.get("/runs/{run_id}")
def run_page(run_id: int, request: Request, db: Session = Depends(get_db)):
    """Render the run detail page with live updates."""
    run = db.query(BackupRun).filter(BackupRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    target = db.query(BackupTarget).filter(BackupTarget.id == run.target_id).first()
    logs = db.query(RunLog).filter(RunLog.run_id == run_id).order_by(RunLog.id.asc()).limit(300).all()
    return templates.TemplateResponse("run_detail.html", {"request": request, "run": run, "target": target, "logs": logs})


@app.get("/logs")
def logs_page(request: Request, db: Session = Depends(get_db)):
    """Render run history and the live logs page."""
    runs = db.query(BackupRun).order_by(BackupRun.id.desc()).limit(100).all()
    target_names = _target_name_map(db)
    return templates.TemplateResponse("logs.html", {"request": request, "runs": runs, "target_names": target_names})


@app.get("/api/logs/history")
def api_logs_history(limit: int = 100, db: Session = Depends(get_db)):
    """API for auto-refreshing the run history table."""
    safe_limit = max(1, min(int(limit), 300))
    runs = db.query(BackupRun).order_by(BackupRun.id.desc()).limit(safe_limit).all()
    target_names = _target_name_map(db)
    return {
        "runs": [
            {
                "id": r.id,
                "target_id": r.target_id,
                "target_name": target_names.get(r.target_id, f"ID {r.target_id}"),
                "status": r.status,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                "display_time": (r.finished_at or r.started_at).isoformat() if (r.finished_at or r.started_at) else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                "error_message": r.error_message or "",
            }
            for r in runs
        ]
    }


@app.get("/settings")
def settings_page(request: Request, db: Session = Depends(get_db)):
    """Render the global settings page."""
    settings = _get_or_create_settings(db)
    flash = request.query_params.get("flash", "")
    return templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "settings": settings,
            "flash": flash,
            "default_max_concurrent_runs": DEFAULT_MAX_CONCURRENT_RUNS,
        },
    )


@app.post("/settings/telegram")
def settings_update_telegram(
    telegram_notifier_url: str = Form(""),
    telegram_chat_id: str = Form(""),
    telegram_thread_id: str = Form(""),
    db: Session = Depends(get_db),
):
    """Save Telegram notification settings."""
    values = {
        "telegram_notifier_url": telegram_notifier_url,
        "telegram_chat_id": telegram_chat_id,
        "telegram_thread_id": telegram_thread_id,
    }
    _save_settings(db, values)
    return RedirectResponse(url="/settings", status_code=303)


@app.post("/settings/email")
def settings_update_email(
    smtp_host: str = Form(""),
    smtp_port: str = Form("587"),
    smtp_user: str = Form(""),
    smtp_password: str = Form(""),
    smtp_from: str = Form(""),
    smtp_to: str = Form(""),
    smtp_security: str = Form("starttls"),
    db: Session = Depends(get_db),
):
    """Save Email/SMTP settings."""
    normalized_security = smtp_security.strip().lower()
    if normalized_security not in {"none", "starttls", "ssl_tls"}:
        normalized_security = "starttls"

    values = {
        "smtp_host": smtp_host,
        "smtp_port": smtp_port,
        "smtp_user": smtp_user,
        "smtp_password": smtp_password,
        "smtp_from": smtp_from,
        "smtp_to": smtp_to,
        "smtp_security": normalized_security,
    }
    _save_settings(db, values)
    return RedirectResponse(url="/settings", status_code=303)


@app.post("/settings/telegram/test")
def settings_test_telegram(db: Session = Depends(get_db)):
    """Send a test Telegram message."""
    ok = send_telegram(db, "Тестовое сообщение из Backup Control Center")
    flash = "Telegram тест: отправлено" if ok else "Telegram тест: не отправлено (проверьте настройки)"
    return RedirectResponse(url=f"/settings?flash={quote_plus(flash)}", status_code=303)


@app.post("/settings/email/test")
def settings_test_email(db: Session = Depends(get_db)):
    """Send a test email message."""
    ok = send_email(
        db,
        "Backup Control Center test",
        "Это тестовое email-сообщение из Backup Control Center.",
    )
    flash = "Email тест: отправлено" if ok else "Email тест: не отправлено (проверьте настройки)"
    return RedirectResponse(url=f"/settings?flash={quote_plus(flash)}", status_code=303)


@app.post("/settings/local-retention")
def settings_update_local_retention(
    local_retention_days: int = Form(DEFAULT_RETENTION_DAYS),
    apply_to_all_targets: str | None = Form(None),
    db: Session = Depends(get_db),
):
    """Save global local-archive retention and optionally apply it to all targets."""
    keep_days = max(1, int(local_retention_days or DEFAULT_RETENTION_DAYS))
    _save_settings(db, {"local_retention_days": str(keep_days)})

    updated = 0
    if _bool_from_form(apply_to_all_targets):
        targets = db.query(BackupTarget).all()
        for target in targets:
            target.retention_days = keep_days
            updated += 1
        db.commit()

    if updated:
        flash = f"Локальный retention обновлен: {keep_days} дн. Применено к задачам: {updated}."
    else:
        flash = f"Локальный retention по умолчанию обновлен: {keep_days} дн. Будет применяться для новых задач."
    return RedirectResponse(url=f"/settings?flash={quote_plus(flash)}", status_code=303)


@app.post("/settings/executor")
def settings_update_executor(
    max_concurrent_runs: str = Form(str(DEFAULT_MAX_CONCURRENT_RUNS)),
    db: Session = Depends(get_db),
):
    """Save the concurrent-run limit and apply it to new runs."""
    normalized = normalize_max_concurrent_runs(max_concurrent_runs)
    _save_settings(db, {"max_concurrent_runs": str(normalized)})
    applied = configure_executor(normalized)
    flash = (
        "Лимит параллельного выполнения обновлен: "
        f"{applied}. Новые задачи пойдут с новым лимитом, текущие запуски не прерываются."
    )
    return RedirectResponse(url=f"/settings?flash={quote_plus(flash)}", status_code=303)


@app.get("/restic")
def restic_page(
    request: Request,
    target_id: int | None = None,
    queue_filter: str = "",
    snapshot_filter: str = "",
    db: Session = Depends(get_db),
):
    """Render the Restic management page."""
    flash = request.query_params.get("flash", "")
    settings = _get_or_create_settings(db)
    queue_filter = queue_filter.strip()
    snapshot_filter = snapshot_filter.strip()
    snapshots_payload = _build_restic_snapshots_payload(
        db=db,
        target_id=target_id,
        snapshot_filter=snapshot_filter,
    )
    restic_targets = snapshots_payload["targets"]
    selected_target = snapshots_payload["selected_target"]
    snapshots = snapshots_payload["snapshots"]
    snapshots_error = snapshots_payload["snapshots_error"]
    snapshots_info = snapshots_payload["snapshots_info"]

    queue_payload = _build_restic_queue_payload(db=db, queue_filter=queue_filter, limit=200)
    return templates.TemplateResponse(
        "restic.html",
        {
            "request": request,
            "flash": flash,
            "settings": settings,
            "runs": queue_payload["runs"],
            "targets": restic_targets,
            "selected_target": selected_target,
            "snapshots": snapshots,
            "snapshots_error": snapshots_error,
            "snapshots_info": snapshots_info,
            "queue_filter": queue_filter,
            "snapshot_filter": snapshot_filter,
        },
    )


@app.get("/api/restic/snapshots")
def api_restic_snapshots(
    target_id: int | None = None,
    snapshot_filter: str = "",
    db: Session = Depends(get_db),
):
    """API for auto-refreshing the Restic snapshots table."""
    return _build_restic_snapshots_payload(
        db=db,
        target_id=target_id,
        snapshot_filter=snapshot_filter,
    )


@app.get("/api/restic/queue")
def api_restic_queue(
    queue_filter: str = "",
    limit: int = 200,
    db: Session = Depends(get_db),
):
    """API for auto-refreshing the Restic upload queue table."""
    return _build_restic_queue_payload(
        db=db,
        queue_filter=queue_filter,
        limit=limit,
    )


@app.post("/restic/runs/{run_id}/send")
def restic_send_run(run_id: int, db: Session = Depends(get_db)):
    """Immediately upload the selected finished run artifact to Restic."""
    run = db.query(BackupRun).filter(BackupRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    target = db.query(BackupTarget).filter(BackupTarget.id == run.target_id).first()
    if not target:
        raise HTTPException(status_code=404, detail="Target not found")
    if not target.restic_repository.strip() or not target.restic_password:
        flash = "Restic не настроен: заполните Repository и Password в задаче"
        return RedirectResponse(url=f"/restic?flash={quote_plus(flash)}", status_code=303)
    if not (run.archive_file or "").strip() and not (run.backup_dir or "").strip():
        flash = "У запуска нет данных для отправки в Restic (ни archive_file, ни backup_dir)"
        return RedirectResponse(url=f"/restic?flash={quote_plus(flash)}", status_code=303)

    ok, msg = _send_run_to_restic(db, run, target)
    flash = "Отправка в Restic выполнена" if ok else f"Restic ошибка: {msg[:200]}"
    return RedirectResponse(url=f"/restic?flash={quote_plus(flash)}", status_code=303)


@app.post("/restic/retention")
def restic_retention(
    restic_keep_within_days: int = Form(30),
    apply_now: str | None = Form(None),
    db: Session = Depends(get_db),
):
    """Set retention days for Restic-enabled targets and optionally apply immediately."""
    keep_days = max(1, int(restic_keep_within_days))
    _save_settings(db, {"restic_keep_within_days": str(keep_days)})

    if not _bool_from_form(apply_now):
        flash = "Значение сохранено. Поля задач не изменены (включите галку для массового применения)."
        return RedirectResponse(url=f"/restic?flash={quote_plus(flash)}", status_code=303)

    targets = (
        db.query(BackupTarget)
        .filter(BackupTarget.restic_enabled == True)  # noqa: E712
        .all()
    )
    for target in targets:
        target.restic_keep_within_days = keep_days
    db.commit()

    results: list[str] = []
    for target in targets:
        if not target.restic_repository.strip() or not target.restic_password:
            results.append(f"{target.name}:SKIP")
            continue
        ok, out = restic_forget_prune(
            target.restic_repository,
            target.restic_password,
            keep_days,
            tag=target.name,
        )
        status = "OK" if ok else "ERR"
        results.append(f"{target.name}:{status}")

    flash = "Retention применен: " + ", ".join(results) if results else "Нет задач с настроенным Restic"
    return RedirectResponse(url=f"/restic?flash={quote_plus(flash)}", status_code=303)


@app.get("/api/runs/{run_id}/status")
def api_run_status(run_id: int, db: Session = Depends(get_db)):
    """API endpoint for polling run status."""
    run = db.query(BackupRun).filter(BackupRun.id == run_id).first()
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return {
        "id": run.id,
        "status": run.status,
        "progress": run.progress,
        "step": run.step,
        "error_message": run.error_message,
        "archive_file": run.archive_file,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "launch_type": run.launch_type,
        "cancel_requested": run.cancel_requested,
    }


@app.get("/api/runs/{run_id}/logs")
def api_run_logs(run_id: int, after_id: int = 0, db: Session = Depends(get_db)):
    """API endpoint for incremental log polling."""
    rows = (
        db.query(RunLog)
        .filter(RunLog.run_id == run_id, RunLog.id > after_id)
        .order_by(RunLog.id.asc())
        .limit(500)
        .all()
    )

    return {
        "logs": [
            {
                "id": x.id,
                "level": x.level,
                "message": x.message,
                "created_at": x.created_at.isoformat(),
            }
            for x in rows
        ]
    }
