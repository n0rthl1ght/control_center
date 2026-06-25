# Backup Control Center

Русский | [English](#english)

Backup Control Center is a web application for managing backup jobs for PostgreSQL, MySQL, MongoDB, and Docker/Compose projects. It provides a UI for creating backup targets, running them manually or by schedule, monitoring progress in real time, sending notifications, applying retention policies, and pushing archives to Restic repositories.

## Русский

### Описание

`Backup Control Center` нужен для централизованного управления бэкапами через web UI. Приложение хранит конфигурацию задач в SQLite, запускает бэкапы в фоне, пишет детальные логи, показывает статус выполнения в реальном времени и позволяет отправлять готовые архивы в Restic.

Поддерживаемые типы источников:

- `PostgreSQL`
- `MySQL`
- `MongoDB`
- `Docker`

### Основной функционал

- Создание, редактирование, дублирование, удаление и массовое включение/отключение backup-задач.
- Ручной запуск backup-задачи из UI.
- Плановые запуски по cron через APScheduler.
- Отмена активного запуска.
- Realtime-страница выполнения с логами и прогрессом.
- История запусков и логов.
- Отправка результата в Restic.
- Просмотр очереди отправки в Restic и списка снапшотов.
- Telegram-уведомления.
- Email-уведомления через SMTP.
- Локальный retention архивов.
- Отдельный retention снапшотов Restic.
- Работа как в локальном Python-режиме, так и в Docker.

### Что умеет по типам источников

#### PostgreSQL

Режимы:

- `full`
- `split_excluded_tables`
- `custom_excludes`

Возможности:

- полный `pg_dump` по списку баз;
- split-сценарий: дамп данных без выбранных таблиц и отдельный schema-only dump этих таблиц;
- custom exclude-сценарий:
  - `exclude-table`
  - `exclude-table-data`
  - `exclude-schema`

#### MySQL

Режимы:

- `full`

Возможности:

- общий `mysqldump` по списку баз;
- `ignore-table`;
- отдельный schema-only dump для выбранных таблиц;
- выполнение локально или по SSH;
- SSH-аутентификация по паролю или через вставку приватного ключа.

#### MongoDB

Режимы:

- `full`
- `split_excluded_tables`
- `latest_collection_groups`

Возможности:

- backup всех баз или выбранных баз;
- split-режим для коллекций;
- выбор последних групп коллекций по числовому суффиксу;
- поддержка шаблонов:
  - `simple: prefix_suffix`
  - `multipart: prefix_suffix.part`

#### Docker

Под `Docker` здесь понимается backup директории проекта, в которой расположен compose-файл.

Возможности:

- backup каталога проекта по `PROJECT_HOST_DIR`;
- явное указание `Compose file name`;
- автоопределение compose-файла:
  - `docker-compose.yml`
  - `docker-compose.yaml`
  - `compose.yml`
  - `compose.yaml`
  - или единственный `.yml/.yaml` в корне каталога;
- опциональный `docker compose down` перед бэкапом;
- автоматический `docker compose up -d` после бэкапа;
- опциональный запуск remote-команд через `sudo -n`;
- архив создается на удаленном хосте и затем скачивается на сервер с Backup Control Center;
- повторной локальной упаковки нет: используется скачанный `tar.gz`.

### Архитектура

Ключевые компоненты:

- `app/main.py` — FastAPI-приложение, HTML-страницы, API, CRUD, настройки, Restic UI.
- `app/backup_engine.py` — выполнение backup-задач, orchestrator, SSH/rsync/scp, verification, retention, Restic.
- `app/models.py` — модели SQLite/SQLAlchemy.
- `app/scheduler.py` — cron-планировщик на APScheduler.
- `app/restic_service.py` — интеграция с Restic.
- `app/restic_cache.py` — кэш снапшотов Restic.
- `app/notifications.py` — Telegram и Email.

### Требования

#### Для локального запуска

- Python `3.13+`
- `pip`
- SQLite

Для реального выполнения backup-команд на хосте должны быть доступны:

- `pg_dump`
- `mysqldump`
- `mongodump`
- `mongosh`
- `gzip`
- `tar`
- `ssh`
- `scp`
- `rsync`
- `sshpass` — если используется SSH-аутентификация по паролю
- `restic` — если используется отправка в Restic
- `rclone` — если Restic-репозиторий использует backend `rclone:...`

#### Для Docker-режима

Нужен Docker Engine и Docker Compose plugin.

Контейнерный образ уже включает:

- `pg_dump`
- `mysqldump`
- `mongodump`
- `mongosh`
- `restic`
- `rclone`
- `rsync`
- `ssh`
- `scp`
- `sshpass`
- `gzip`
- `tar`

### Установка и запуск

#### Вариант 1. Локальный запуск

```bash
cd control_center
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8090 --reload
```

Открыть:

- `http://127.0.0.1:8090`

#### Вариант 2. Docker Compose

Подготовка:

```bash
cd control_center
cp .env.example .env
mkdir -p ./runtime/data ./runtime/backups ./runtime/rclone
```

Запуск основного сервиса:

```bash
docker compose up -d --build control-center
```

Запуск варианта с SQLite во внутреннем Docker volume:

```bash
docker compose --profile dockerdb up -d --build control-center-dockerdb
```

### Переменные окружения

Основные параметры описаны в `.env.example`.

Ключевые:

- `BACKUP_TZ` — часовой пояс приложения и scheduler.
- `DEFAULT_RETENTION_DAYS` — retention локальных архивов по умолчанию.
- `DEFAULT_BACKUP_ROOT` — базовый путь хранения новых задач по умолчанию.
- `CONTROL_CENTER_DB_URL` — SQLite URL для основного контейнера.
- `CONTROL_CENTER_DB_URL_DOCKERDB` — SQLite URL для варианта `dockerdb`.
- `HOST_APP_DATA_DIR` — каталог хоста для SQLite и runtime-данных.
- `HOST_BACKUPS_DIR` — каталог хоста, куда сохраняются backup-архивы.
- `CONTAINER_BACKUPS_DIR` — путь внутри контейнера для новых backup-задач.
- `CONTAINER_LEGACY_BACKUPS_DIR` — legacy mount для старых путей задач.
- `RCLONE_CONFIG` — путь к `rclone.conf` в контейнере.
- `RCLONE_CONFIG_HOST_DIR` — каталог на хосте, который монтируется как `/config/rclone`.

### Первый запуск

После старта приложение:

- создает SQLite-базу;
- применяет добавочные миграции;
- поднимает thread pool для фоновых задач;
- запускает scheduler;
- пересобирает cron-задачи из БД;
- отменяет stale-запуски, если приложение было остановлено во время выполнения.

### Как пользоваться

#### 1. Создать задачу

Откройте:

- `Targets -> New backup`

Общие поля:

- `Имя`
- `Тип источника`
- `Режим дампа` — если применимо
- `Активация`

#### 2. Настроить источник

##### PostgreSQL

Минимально:

- `DB Host`
- `DB Port`
- `DB User`
- `DB Password`
- `Список баз`

Дополнительно для специальных режимов:

- `Таблицы для split-режима`
- `exclude-table`
- `exclude-table-data`
- `exclude-schema`

##### MySQL

Минимально:

- `DB Host`
- `DB Port`
- `DB User`
- `DB Password`
- `Основные БД MySQL`

Опционально:

- `Ignore-table`
- `Таблицы для schema-only`

Для SSH-режима:

- `Выполнять дамп по SSH`
- `SSH host`
- `SSH port`
- `SSH user`
- `SSH auth type`
- `SSH password` или `SSH private key`
- `Remote tmp dir`

##### MongoDB

Минимально:

- `DB Host`
- `DB Port`
- `DB User` — если требуется
- `DB Password` — если требуется
- `Список баз MongoDB` — опционально в `full`, обязательно в специальных режимах

Для `latest_collection_groups`:

- `Префиксы групп MongoDB`
- `Шаблон имени группы`
- `Количество последних групп`
- `Части multipart-группы` — если выбран multipart-режим
- `Blacklist коллекций/суффиксов` — опционально

##### Docker

Минимально:

- `PROJECT_HOST_DIR`
- `SSH host`
- `SSH port`
- `SSH user`
- `SSH auth type`
- `SSH password` или `SSH private key`
- `Remote tmp dir`

Опционально:

- `Compose file name`
- `Exclude patterns`
- `Останавливать Docker Compose перед бэкапом`
- `Использовать sudo -n`

Поведение:

- если `Compose file name` пустой, приложение ищет стандартные compose-файлы;
- если стандартных файлов нет, но найден ровно один `.yml/.yaml`, он выбирается автоматически;
- если найдено несколько `.yml/.yaml`, нужно указать `Compose file name` явно.

#### 3. Настроить хранение и расписание

Поля:

- `Путь хранения`
- `Архивировать`
- `Retention (days)`
- `Лимит копирования (KB/s)`
- `Cron`

#### 4. Настроить Restic

Поля:

- `Отправлять в Restic`
- `Restic Repository`
- `Restic Password`
- `Автоудаление снапшотов Restic (дней)`

Теги Restic:

- всегда добавляется тег имени задачи;
- для Docker дополнительно добавляются:
  - `PROJECT_NAME`
  - `docker`

#### 5. Настроить уведомления

На уровне задачи:

- `Телеграм`
- `Email`

Глобальные настройки находятся в разделе:

- `Settings`

Доступно:

- Telegram notifier URL или `BOT:TOKEN`
- `chat_id`
- `thread_id`
- SMTP host/port/user/password
- `from`
- `to`
- тип безопасности:
  - `none`
  - `starttls`
  - `ssl_tls`

### Планировщик и cron

Используется 5-польный cron-формат:

```text
minute hour day_of_month month day_of_week
```

Примеры:

- `0 3 * * *` — каждый день в 03:00
- `0 3 * * sun` — каждое воскресенье в 03:00
- `10 2 * * 7` — тоже воскресенье в 02:10

Поддержка дней недели:

- `0` и `7` = воскресенье
- `1` = понедельник
- `...`
- `6` = суббота
- также поддерживаются `mon`, `tue`, `wed`, `thu`, `fri`, `sat`, `sun`

Некорректный cron теперь не игнорируется молча: форма сохранения возвращает ошибку.

### Работа со страницей Restic

Раздел `/restic` умеет:

- показывать очередь завершенных запусков для отправки в Restic;
- запускать ручную отправку конкретного run;
- менять глобальный retention снапшотов;
- показывать снапшоты выбранного репозитория;
- фильтровать снапшоты по ID, tag, path и времени.

Снапшоты:

- кэшируются в фоне;
- сортируются от новых к старым;
- больше не режутся искусственным лимитом на уровне UI/кэша.

### Логика хранения

#### Локальные архивы

- результат backup сохраняется в локальный каталог задачи;
- старые архивы удаляются по retention;
- активный текущий файл не удаляется из-под работающего запуска.

#### Restic

- после успешной отправки архив может остаться локально;
- для некоторых сценариев директория исходного дампа может быть удалена после Restic, если в Restic отправлялся каталог, а не архив;
- retention Restic применяется отдельно от локального retention.

### Особенности Docker backup

- архив сначала создается на удаленном хосте;
- нужен свободный объем в `Remote tmp dir` под полный `tar.gz`;
- `docker compose down` полезен для консистентного backup, но не решает проблему прав на файлы сам по себе;
- если SSH-пользователь не может читать bind-mounted или root-owned файлы, включайте `Использовать sudo -n`;
- для `sudo -n` на удаленном хосте должна быть настроена соответствующая запись в `sudoers`.

### Безопасность и ограничения

- встроенной аутентификации/авторизации в приложении нет;
- секреты задач и настроек хранятся в SQLite;
- проект рассчитан на использование во внутренней инфраструктуре;
- перед публичным размещением или выходом наружу рекомендуется добавить reverse proxy, аутентификацию и защиту секретов.

### Структура проекта

```text
control_center/
  app/
    main.py
    backup_engine.py
    models.py
    scheduler.py
    restic_service.py
    restic_cache.py
    notifications.py
    migrations.py
    templates/
    static/
  docker/
  Dockerfile
  docker-compose.yml
  requirements.txt
  .env.example
```

### Типичные сценарии

#### PostgreSQL

- ежедневный backup нескольких баз;
- split backup больших таблиц;
- исключение технических или временных схем.

#### MySQL

- backup нескольких баз одной задачей;
- вынос выполнения на удаленный сервер через SSH;
- schema-only dump отдельных таблиц.

#### MongoDB

- full dump конкретной базы или всех баз;
- backup только последних коллекционных групп по шаблону имен;
- исключение шумных или устаревших суффиксов через blacklist.

#### Docker

- backup compose-проекта из `/opt/...`;
- backup с нестандартным compose-файлом;
- backup с остановкой/подъемом сервисов;
- backup через `sudo -n`, если проектные данные доступны только root.

### Troubleshooting

#### Задача не запускается по cron

Проверьте:

- задача включена;
- cron валиден;
- scheduler стартовал;
- часовой пояс `BACKUP_TZ` установлен корректно.

#### Docker backup пишет, что compose-файл не найден

Проверьте:

- `PROJECT_HOST_DIR`;
- права SSH-пользователя на чтение каталога;
- `Compose file name`;
- не лежит ли compose-файл глубже первого уровня каталога.

#### Docker backup падает по `Permission denied`

Проверьте:

- доступ SSH-пользователя к файлам проекта и volumes;
- опцию `Использовать sudo -n`;
- настройку `sudoers`.

#### Docker backup падает по `No space left on device`

Проверьте:

- `Remote tmp dir`;
- свободное место на удаленном хосте;
- размер итогового архива.

#### Restic не показывает снапшоты

Проверьте:

- `Restic Repository`;
- `Restic Password`;
- доступность backend;
- корректность `rclone.conf`, если используется `rclone:...`.

---

## English

### Overview

`Backup Control Center` is a web application for managing backup jobs from a single UI. It stores job configuration in SQLite, executes backups in background workers, keeps detailed logs, shows real-time status, sends notifications, and can push resulting artifacts to Restic repositories.

Supported source types:

- `PostgreSQL`
- `MySQL`
- `MongoDB`
- `Docker`

### Main features

- Create, edit, duplicate, delete, enable, and disable backup jobs.
- Run jobs manually from the UI.
- Schedule jobs with cron via APScheduler.
- Cancel active runs.
- Real-time run page with progress and logs.
- Run history and log history.
- Push archives to Restic.
- View Restic send queue and repository snapshots.
- Telegram notifications.
- Email notifications via SMTP.
- Local archive retention.
- Separate Restic snapshot retention.
- Works both as a local Python app and as a Dockerized service.

### Source types and backup modes

#### PostgreSQL

Modes:

- `full`
- `split_excluded_tables`
- `custom_excludes`

Capabilities:

- full `pg_dump` for selected databases;
- split mode: data dump without selected tables plus separate schema-only dump;
- custom exclusion mode:
  - `exclude-table`
  - `exclude-table-data`
  - `exclude-schema`

#### MySQL

Modes:

- `full`

Capabilities:

- combined `mysqldump` for multiple databases;
- `ignore-table`;
- separate schema-only dump for selected tables;
- local execution or SSH execution;
- SSH authentication via password or inline private key.

#### MongoDB

Modes:

- `full`
- `split_excluded_tables`
- `latest_collection_groups`

Capabilities:

- dump all databases or selected databases;
- split collection mode;
- backup of the latest collection groups by numeric suffix;
- supported naming patterns:
  - `simple: prefix_suffix`
  - `multipart: prefix_suffix.part`

#### Docker

For `Docker`, the app backs up a project directory containing a compose file.

Capabilities:

- project directory backup using `PROJECT_HOST_DIR`;
- explicit `Compose file name`;
- auto-detection of compose file:
  - `docker-compose.yml`
  - `docker-compose.yaml`
  - `compose.yml`
  - `compose.yaml`
  - or a single `.yml/.yaml` file in the project root;
- optional `docker compose down` before backup;
- automatic `docker compose up -d` after backup;
- optional execution of remote commands via `sudo -n`;
- archive is created on the remote host and then downloaded to Backup Control Center;
- no second local repackaging: the downloaded `tar.gz` is the final artifact.

### Architecture

Main components:

- `app/main.py` — FastAPI application, HTML pages, API, CRUD, settings, Restic UI.
- `app/backup_engine.py` — backup execution engine, orchestration, SSH/rsync/scp, verification, retention, Restic.
- `app/models.py` — SQLite/SQLAlchemy models.
- `app/scheduler.py` — cron scheduler based on APScheduler.
- `app/restic_service.py` — Restic integration.
- `app/restic_cache.py` — Restic snapshot cache.
- `app/notifications.py` — Telegram and Email notifications.

### Requirements

#### Local run

- Python `3.13+`
- `pip`
- SQLite

To actually execute backups, the host should provide:

- `pg_dump`
- `mysqldump`
- `mongodump`
- `mongosh`
- `gzip`
- `tar`
- `ssh`
- `scp`
- `rsync`
- `sshpass` — if password-based SSH is used
- `restic` — if Restic upload is enabled
- `rclone` — if Restic uses an `rclone:...` backend

#### Docker mode

Requires Docker Engine and Docker Compose plugin.

The application image already contains:

- `pg_dump`
- `mysqldump`
- `mongodump`
- `mongosh`
- `restic`
- `rclone`
- `rsync`
- `ssh`
- `scp`
- `sshpass`
- `gzip`
- `tar`

### Installation and startup

#### Option 1. Local run

```bash
cd control_center
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8090 --reload
```

Open:

- `http://127.0.0.1:8090`

#### Option 2. Docker Compose

Preparation:

```bash
cd control_center
cp .env.example .env
mkdir -p ./runtime/data ./runtime/backups ./runtime/rclone
```

Run the main service:

```bash
docker compose up -d --build control-center
```

Run the variant with SQLite in an internal Docker volume:

```bash
docker compose --profile dockerdb up -d --build control-center-dockerdb
```

### Environment variables

The main settings are documented in `.env.example`.

Key variables:

- `BACKUP_TZ` — application and scheduler timezone.
- `DEFAULT_RETENTION_DAYS` — default local retention.
- `DEFAULT_BACKUP_ROOT` — default storage root for new jobs.
- `CONTROL_CENTER_DB_URL` — SQLite URL for the main container.
- `CONTROL_CENTER_DB_URL_DOCKERDB` — SQLite URL for the `dockerdb` profile.
- `HOST_APP_DATA_DIR` — host directory for SQLite and runtime data.
- `HOST_BACKUPS_DIR` — host directory where backup archives are stored.
- `CONTAINER_BACKUPS_DIR` — in-container path for new backup jobs.
- `CONTAINER_LEGACY_BACKUPS_DIR` — legacy mount for old stored paths.
- `RCLONE_CONFIG` — path to `rclone.conf` inside the container.
- `RCLONE_CONFIG_HOST_DIR` — host directory mounted as `/config/rclone`.

### First use

On startup the application:

- creates the SQLite database;
- applies additive migrations;
- configures the background worker pool;
- starts the scheduler;
- rebuilds cron jobs from the database;
- cancels stale runs if the service was stopped during execution.

### How to use

#### 1. Create a job

Open:

- `Targets -> New backup`

General fields:

- `Name`
- `Source type`
- `Dump mode` — when applicable
- `Enabled`

#### 2. Configure the source

##### PostgreSQL

Minimum:

- `DB Host`
- `DB Port`
- `DB User`
- `DB Password`
- `Database list`

Additional fields for advanced modes:

- `Split tables`
- `exclude-table`
- `exclude-table-data`
- `exclude-schema`

##### MySQL

Minimum:

- `DB Host`
- `DB Port`
- `DB User`
- `DB Password`
- `Main MySQL databases`

Optional:

- `Ignore-table`
- `Schema-only tables`

For SSH mode:

- `Run dump over SSH`
- `SSH host`
- `SSH port`
- `SSH user`
- `SSH auth type`
- `SSH password` or `SSH private key`
- `Remote tmp dir`

##### MongoDB

Minimum:

- `DB Host`
- `DB Port`
- `DB User` — if needed
- `DB Password` — if needed
- `MongoDB database list` — optional for `full`, required for advanced modes

For `latest_collection_groups`:

- `MongoDB group prefixes`
- `Group name mode`
- `Latest group count`
- `Multipart group parts` — for multipart mode
- `Collection/suffix blacklist` — optional

##### Docker

Minimum:

- `PROJECT_HOST_DIR`
- `SSH host`
- `SSH port`
- `SSH user`
- `SSH auth type`
- `SSH password` or `SSH private key`
- `Remote tmp dir`

Optional:

- `Compose file name`
- `Exclude patterns`
- `Stop Docker Compose before backup`
- `Use sudo -n`

Behavior:

- if `Compose file name` is empty, the app searches for standard compose file names;
- if no standard files exist but exactly one `.yml/.yaml` file exists in the project root, it is auto-selected;
- if multiple `.yml/.yaml` files are found, you must specify `Compose file name` explicitly.

#### 3. Configure storage and schedule

Fields:

- `Storage path`
- `Archive`
- `Retention (days)`
- `Transfer limit (KB/s)`
- `Cron`

#### 4. Configure Restic

Fields:

- `Send to Restic`
- `Restic Repository`
- `Restic Password`
- `Restic snapshot retention (days)`

Restic tags:

- target name is always added;
- for Docker backups the app also adds:
  - `PROJECT_NAME`
  - `docker`

#### 5. Configure notifications

Per target:

- `Telegram`
- `Email`

Global settings are available in:

- `Settings`

Supported:

- Telegram notifier URL or `BOT:TOKEN`
- `chat_id`
- `thread_id`
- SMTP host/port/user/password
- `from`
- `to`
- security mode:
  - `none`
  - `starttls`
  - `ssl_tls`

### Scheduler and cron

The app uses 5-field cron expressions:

```text
minute hour day_of_month month day_of_week
```

Examples:

- `0 3 * * *` — every day at 03:00
- `0 3 * * sun` — every Sunday at 03:00
- `10 2 * * 7` — also Sunday at 02:10

Day-of-week support:

- `0` and `7` = Sunday
- `1` = Monday
- `...`
- `6` = Saturday
- names are also supported: `mon`, `tue`, `wed`, `thu`, `fri`, `sat`, `sun`

Invalid cron expressions are no longer silently ignored: the form now returns a validation error.

### Restic page

The `/restic` section can:

- show completed runs that can be sent to Restic;
- trigger manual Restic send for a specific run;
- update global Restic retention;
- show snapshots for the selected repository;
- filter snapshots by ID, tag, path, and time.

Snapshots:

- are cached in the background;
- are sorted from newest to oldest;
- are no longer truncated by hard-coded UI/cache limits.

### Storage model

#### Local archives

- backup results are stored in the job-specific local directory;
- old archives are deleted according to retention;
- the current active file is not deleted during the running job.

#### Restic

- after a successful send, an archive may remain local;
- for some workflows the original dump directory may be removed after Restic if a directory, not an archive, was sent;
- Restic retention is applied separately from local retention.

### Docker-specific notes

- the archive is created on the remote host first;
- `Remote tmp dir` must have enough free space for the full `tar.gz`;
- `docker compose down` can improve consistency, but does not solve file permission issues on its own;
- if the SSH user cannot read bind-mounted or root-owned files, enable `Use sudo -n`;
- for `sudo -n`, the remote host must have a matching `sudoers` rule.

### Security and limitations

- the application has no built-in authentication/authorization;
- job secrets and settings are stored in SQLite;
- the project is intended for internal infrastructure usage;
- for public exposure, use a reverse proxy, authentication, and proper secret protection.

### Project layout

```text
control_center/
  app/
    main.py
    backup_engine.py
    models.py
    scheduler.py
    restic_service.py
    restic_cache.py
    notifications.py
    migrations.py
    templates/
    static/
  docker/
  Dockerfile
  docker-compose.yml
  requirements.txt
  .env.example
```

### Common use cases

#### PostgreSQL

- daily backups for multiple databases;
- split backups for large tables;
- excluding technical or temporary schemas.

#### MySQL

- backing up several databases with one job;
- running the dump on a remote host over SSH;
- schema-only dumps for selected tables.

#### MongoDB

- full dump of a single database or all databases;
- backup only the latest collection groups by naming pattern;
- exclude noisy or outdated suffixes with a blacklist.

#### Docker

- backup a compose project in `/opt/...`;
- backup a project with a non-standard compose filename;
- backup with stop/start of services;
- backup using `sudo -n` when project data is readable only by root.

### Troubleshooting

#### A job does not run by cron

Check:

- the job is enabled;
- the cron expression is valid;
- the scheduler has started;
- `BACKUP_TZ` is correct.

#### Docker backup says compose file was not found

Check:

- `PROJECT_HOST_DIR`;
- SSH user permissions on the directory;
- `Compose file name`;
- whether the compose file is deeper than the first directory level.

#### Docker backup fails with `Permission denied`

Check:

- SSH user access to project files and volumes;
- the `Use sudo -n` option;
- `sudoers` configuration.

#### Docker backup fails with `No space left on device`

Check:

- `Remote tmp dir`;
- free space on the remote host;
- resulting archive size.

#### Restic snapshots are missing

Check:

- `Restic Repository`;
- `Restic Password`;
- backend connectivity;
- correctness of `rclone.conf` if `rclone:...` is used.
