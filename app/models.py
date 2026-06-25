from __future__ import annotations

"""ORM models for backup targets, runs, logs, and application settings."""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base
from .time_utils import now_local_naive


class BackupTarget(Base):
    """Backup job configuration: what to back up, where, and how to notify."""

    __tablename__ = "backup_targets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    db_type: Mapped[str] = mapped_column(Text, default="postgresql", nullable=False)

    db_host: Mapped[str] = mapped_column(Text, nullable=False)
    db_port: Mapped[int] = mapped_column(Integer, default=5432, nullable=False)
    db_user: Mapped[str] = mapped_column(Text, nullable=False)
    db_password: Mapped[str] = mapped_column(Text, nullable=False)
    mongo_auth_db: Mapped[str] = mapped_column(Text, default="admin", nullable=False)
    mongo_group_name_mode: Mapped[str] = mapped_column(Text, default="simple", nullable=False)
    mongo_group_prefixes_csv: Mapped[str] = mapped_column(Text, default="", nullable=False)
    mongo_collection_parts_csv: Mapped[str] = mapped_column(Text, default="", nullable=False)
    mongo_latest_group_count: Mapped[int] = mapped_column(Integer, default=3, nullable=False)
    mongo_collection_blacklist_csv: Mapped[str] = mapped_column(Text, default="", nullable=False)
    mysql_ignore_tables_csv: Mapped[str] = mapped_column(Text, default="", nullable=False)
    mysql_views_schema_db: Mapped[str] = mapped_column(Text, default="", nullable=False)
    mysql_structure_tables_csv: Mapped[str] = mapped_column(Text, default="", nullable=False)
    mysql_use_ssh: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    mysql_ssh_host: Mapped[str] = mapped_column(Text, default="", nullable=False)
    mysql_ssh_port: Mapped[int] = mapped_column(Integer, default=22, nullable=False)
    mysql_ssh_user: Mapped[str] = mapped_column(Text, default="", nullable=False)
    mysql_ssh_auth_type: Mapped[str] = mapped_column(Text, default="password", nullable=False)
    mysql_ssh_key_path: Mapped[str] = mapped_column(Text, default="", nullable=False)
    mysql_ssh_private_key: Mapped[str] = mapped_column(Text, default="", nullable=False)
    mysql_ssh_password: Mapped[str] = mapped_column(Text, default="", nullable=False)
    mysql_ssh_remote_tmp_dir: Mapped[str] = mapped_column(Text, default="/tmp/mysql_backup_agent", nullable=False)
    remote_ssh_host: Mapped[str] = mapped_column(Text, default="", nullable=False)
    remote_ssh_port: Mapped[int] = mapped_column(Integer, default=22, nullable=False)
    remote_ssh_user: Mapped[str] = mapped_column(Text, default="", nullable=False)
    remote_ssh_auth_type: Mapped[str] = mapped_column(Text, default="password", nullable=False)
    remote_ssh_private_key: Mapped[str] = mapped_column(Text, default="", nullable=False)
    remote_ssh_password: Mapped[str] = mapped_column(Text, default="", nullable=False)
    remote_ssh_remote_tmp_dir: Mapped[str] = mapped_column(Text, default="/tmp/backup_control_center", nullable=False)

    databases_csv: Mapped[str] = mapped_column(Text, default="", nullable=False)
    split_tables_csv: Mapped[str] = mapped_column(Text, default="", nullable=False)
    pg_exclude_tables_csv: Mapped[str] = mapped_column(Text, default="", nullable=False)
    pg_exclude_table_data_csv: Mapped[str] = mapped_column(Text, default="", nullable=False)
    pg_exclude_schemas_csv: Mapped[str] = mapped_column(Text, default="", nullable=False)
    dump_mode: Mapped[str] = mapped_column(Text, default="full", nullable=False)

    backup_root: Mapped[str] = mapped_column(Text, nullable=False)
    retention_days: Mapped[int] = mapped_column(Integer, default=14, nullable=False)
    archive_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    transfer_limit_kbps: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    cron_expr: Mapped[str] = mapped_column(Text, default="", nullable=False)
    docker_project_name: Mapped[str] = mapped_column(Text, default="", nullable=False)
    docker_project_host_dir: Mapped[str] = mapped_column(Text, default="", nullable=False)
    docker_compose_file: Mapped[str] = mapped_column(Text, default="", nullable=False)
    docker_excludes_csv: Mapped[str] = mapped_column(Text, default="", nullable=False)
    docker_stop_before_backup: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    docker_use_sudo: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    restic_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    restic_repository: Mapped[str] = mapped_column(Text, default="", nullable=False)
    restic_password: Mapped[str] = mapped_column(Text, default="", nullable=False)
    restic_keep_within_days: Mapped[int] = mapped_column(Integer, default=30, nullable=False)

    telegram_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    email_enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=now_local_naive, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=now_local_naive, onupdate=now_local_naive, nullable=False)

    runs = relationship("BackupRun", back_populates="target", cascade="all,delete")


class BackupRun(Base):
    """One concrete execution of a `BackupTarget`."""

    __tablename__ = "backup_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    target_id: Mapped[int] = mapped_column(ForeignKey("backup_targets.id"), nullable=False, index=True)

    status: Mapped[str] = mapped_column(Text, default="queued", nullable=False)
    progress: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    step: Mapped[str] = mapped_column(Text, default="queued", nullable=False)
    launch_type: Mapped[str] = mapped_column(Text, default="manual", nullable=False)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    backup_date: Mapped[str] = mapped_column(Text, nullable=False)
    backup_dir: Mapped[str] = mapped_column(Text, default="", nullable=False)
    archive_file: Mapped[str] = mapped_column(Text, default="", nullable=False)

    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    error_message: Mapped[str] = mapped_column(Text, default="", nullable=False)
    restic_status: Mapped[str] = mapped_column(Text, default="", nullable=False)
    restic_snapshot_id: Mapped[str] = mapped_column(Text, default="", nullable=False)
    restic_message: Mapped[str] = mapped_column(Text, default="", nullable=False)
    restic_sent_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    target = relationship("BackupTarget", back_populates="runs")
    logs = relationship("RunLog", back_populates="run", cascade="all,delete")


class RunLog(Base):
    """Structured log lines associated with a single run."""

    __tablename__ = "run_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("backup_runs.id"), nullable=False, index=True)
    level: Mapped[str] = mapped_column(Text, default="info", nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=now_local_naive, nullable=False)

    run = relationship("BackupRun", back_populates="logs")


class AppSetting(Base):
    """Global key/value settings (SMTP, Telegram, Restic, etc.)."""

    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(Text, primary_key=True)
    value: Mapped[str] = mapped_column(Text, default="", nullable=False)
