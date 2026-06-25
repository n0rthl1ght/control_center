from __future__ import annotations

"""Lightweight SQLite migrations.

The project uses additive migrations only (`ALTER TABLE ADD COLUMN`) so
existing local databases remain compatible after updates.
"""

from sqlalchemy import text

from .db import engine


_SQLITE_COLUMN_ADDS = [
    "ALTER TABLE backup_targets ADD COLUMN db_type TEXT NOT NULL DEFAULT 'postgresql'",
    "ALTER TABLE backup_targets ADD COLUMN mongo_auth_db TEXT NOT NULL DEFAULT 'admin'",
    "ALTER TABLE backup_targets ADD COLUMN mongo_group_name_mode TEXT NOT NULL DEFAULT 'simple'",
    "ALTER TABLE backup_targets ADD COLUMN mongo_group_prefixes_csv TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN mongo_collection_parts_csv TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN mongo_latest_group_count INTEGER NOT NULL DEFAULT 3",
    "ALTER TABLE backup_targets ADD COLUMN mongo_collection_blacklist_csv TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN mysql_ignore_tables_csv TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN mysql_views_schema_db TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN mysql_structure_tables_csv TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN mysql_use_ssh BOOLEAN NOT NULL DEFAULT 0",
    "ALTER TABLE backup_targets ADD COLUMN mysql_ssh_host TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN mysql_ssh_port INTEGER NOT NULL DEFAULT 22",
    "ALTER TABLE backup_targets ADD COLUMN mysql_ssh_user TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN mysql_ssh_auth_type TEXT NOT NULL DEFAULT 'password'",
    "ALTER TABLE backup_targets ADD COLUMN mysql_ssh_key_path TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN mysql_ssh_private_key TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN mysql_ssh_password TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN mysql_ssh_remote_tmp_dir TEXT NOT NULL DEFAULT '/tmp/mysql_backup_agent'",
    "ALTER TABLE backup_targets ADD COLUMN remote_ssh_host TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN remote_ssh_port INTEGER NOT NULL DEFAULT 22",
    "ALTER TABLE backup_targets ADD COLUMN remote_ssh_user TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN remote_ssh_auth_type TEXT NOT NULL DEFAULT 'password'",
    "ALTER TABLE backup_targets ADD COLUMN remote_ssh_private_key TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN remote_ssh_password TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN remote_ssh_remote_tmp_dir TEXT NOT NULL DEFAULT '/tmp/backup_control_center'",
    "ALTER TABLE backup_targets ADD COLUMN transfer_limit_kbps INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE backup_targets ADD COLUMN restic_enabled BOOLEAN NOT NULL DEFAULT 0",
    "ALTER TABLE backup_targets ADD COLUMN restic_repository TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN restic_password TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN restic_keep_within_days INTEGER NOT NULL DEFAULT 30",
    "ALTER TABLE backup_targets ADD COLUMN archive_enabled BOOLEAN NOT NULL DEFAULT 1",
    "ALTER TABLE backup_targets ADD COLUMN pg_exclude_tables_csv TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN pg_exclude_table_data_csv TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN pg_exclude_schemas_csv TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN docker_project_name TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN docker_project_host_dir TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN docker_compose_file TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN docker_excludes_csv TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_targets ADD COLUMN docker_stop_before_backup BOOLEAN NOT NULL DEFAULT 0",
    "ALTER TABLE backup_targets ADD COLUMN docker_use_sudo BOOLEAN NOT NULL DEFAULT 0",
]

_SQLITE_RUN_COLUMN_ADDS = [
    "ALTER TABLE backup_runs ADD COLUMN launch_type TEXT NOT NULL DEFAULT 'manual'",
    "ALTER TABLE backup_runs ADD COLUMN cancel_requested BOOLEAN NOT NULL DEFAULT 0",
    "ALTER TABLE backup_runs ADD COLUMN restic_status TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_runs ADD COLUMN restic_snapshot_id TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_runs ADD COLUMN restic_message TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE backup_runs ADD COLUMN restic_sent_at DATETIME",
]


def _sqlite_columns(table_name: str) -> set[str]:
    """Return the current set of columns for a SQLite table."""
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
    return {str(r[1]) for r in rows}


def run_migrations() -> None:
    """Apply missing additive migrations for SQLite."""
    if not str(engine.url).startswith("sqlite"):
        return

    cols = _sqlite_columns("backup_targets")
    run_cols = _sqlite_columns("backup_runs")
    with engine.begin() as conn:
        if "db_type" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[0]))
        if "mongo_auth_db" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[1]))
        if "mongo_group_name_mode" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[2]))
        if "mongo_group_prefixes_csv" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[3]))
        if "mongo_collection_parts_csv" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[4]))
        if "mongo_latest_group_count" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[5]))
        if "mongo_collection_blacklist_csv" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[6]))
        if "mysql_ignore_tables_csv" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[7]))
        if "mysql_views_schema_db" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[8]))
        if "mysql_structure_tables_csv" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[9]))
        if "mysql_use_ssh" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[10]))
        if "mysql_ssh_host" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[11]))
        if "mysql_ssh_port" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[12]))
        if "mysql_ssh_user" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[13]))
        if "mysql_ssh_auth_type" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[14]))
        if "mysql_ssh_key_path" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[15]))
        if "mysql_ssh_private_key" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[16]))
        if "mysql_ssh_password" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[17]))
        if "mysql_ssh_remote_tmp_dir" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[18]))
        if "remote_ssh_host" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[19]))
        if "remote_ssh_port" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[20]))
        if "remote_ssh_user" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[21]))
        if "remote_ssh_auth_type" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[22]))
        if "remote_ssh_private_key" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[23]))
        if "remote_ssh_password" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[24]))
        if "remote_ssh_remote_tmp_dir" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[25]))
        if "transfer_limit_kbps" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[26]))
        if "restic_enabled" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[27]))
        if "restic_repository" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[28]))
        if "restic_password" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[29]))
        if "restic_keep_within_days" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[30]))
        if "archive_enabled" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[31]))
        if "pg_exclude_tables_csv" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[32]))
        if "pg_exclude_table_data_csv" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[33]))
        if "pg_exclude_schemas_csv" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[34]))
        if "docker_project_name" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[35]))
        if "docker_project_host_dir" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[36]))
        if "docker_compose_file" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[37]))
        if "docker_excludes_csv" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[38]))
        if "docker_stop_before_backup" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[39]))
        if "docker_use_sudo" not in cols:
            conn.execute(text(_SQLITE_COLUMN_ADDS[40]))
        if "launch_type" not in run_cols:
            conn.execute(text(_SQLITE_RUN_COLUMN_ADDS[0]))
        if "cancel_requested" not in run_cols:
            conn.execute(text(_SQLITE_RUN_COLUMN_ADDS[1]))
        if "restic_status" not in run_cols:
            conn.execute(text(_SQLITE_RUN_COLUMN_ADDS[2]))
        if "restic_snapshot_id" not in run_cols:
            conn.execute(text(_SQLITE_RUN_COLUMN_ADDS[3]))
        if "restic_message" not in run_cols:
            conn.execute(text(_SQLITE_RUN_COLUMN_ADDS[4]))
        if "restic_sent_at" not in run_cols:
            conn.execute(text(_SQLITE_RUN_COLUMN_ADDS[5]))
