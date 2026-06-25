#!/usr/bin/env bash
set -euo pipefail

mkdir -p /app/data /backups

# Default values can be overridden via docker-compose environment variables.
export CONTROL_CENTER_DB_URL="${CONTROL_CENTER_DB_URL:-sqlite:////app/data/control_center.db}"
export DEFAULT_BACKUP_ROOT="${DEFAULT_BACKUP_ROOT:-/backups}"
export BACKUP_TZ="${BACKUP_TZ:-Europe/Moscow}"
export TZ="${TZ:-$BACKUP_TZ}"

exec "$@"
