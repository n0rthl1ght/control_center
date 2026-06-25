FROM python:3.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

ARG TARGETARCH
ARG RCLONE_VERSION=1.69.1
ARG RESTIC_VERSION=0.18.0
ARG MONGO_TOOLS_VERSION=100.10.0

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl gnupg; \
    install -d /usr/share/keyrings; \
    curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
      | gpg --dearmor -o /usr/share/keyrings/postgresql.gpg; \
    . /etc/os-release; \
    echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt ${VERSION_CODENAME}-pgdg main" \
      > /etc/apt/sources.list.d/pgdg.list; \
    curl -fsSL https://pgp.mongodb.com/server-8.0.asc \
      | gpg --dearmor -o /usr/share/keyrings/mongodb-server-8.0.gpg; \
    echo "deb [signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg] https://repo.mongodb.org/apt/debian ${VERSION_CODENAME}/mongodb-org/8.0 main" \
      > /etc/apt/sources.list.d/mongodb-org-8.0.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      bash \
      bzip2 \
      default-mysql-client \
      gzip \
      jq \
      mongodb-mongosh \
      openssh-client \
      postgresql-client-16 \
      rsync \
      sshpass \
      tar \
      tini \
      tzdata \
      unzip; \
    apt-get purge -y --auto-remove gnupg; \
    rm -rf /var/lib/apt/lists/*

# Install pinned rclone/restic versions (not from apt) to avoid
# drifting upgrades and cross-build incompatibilities.
RUN set -eux; \
    arch="${TARGETARCH:-amd64}"; \
    case "$arch" in \
      amd64) rclone_arch="amd64"; restic_arch="amd64" ;; \
      arm64) rclone_arch="arm64"; restic_arch="arm64" ;; \
      *) echo "Unsupported TARGETARCH: $arch" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://downloads.rclone.org/v${RCLONE_VERSION}/rclone-v${RCLONE_VERSION}-linux-${rclone_arch}.zip" -o /tmp/rclone.zip; \
    unzip -q /tmp/rclone.zip -d /tmp; \
    rclone_dir="$(find /tmp -maxdepth 1 -type d -name "rclone-v${RCLONE_VERSION}-linux-*${rclone_arch}" | head -n1)"; \
    cp "${rclone_dir}/rclone" /usr/local/bin/rclone; \
    chmod +x /usr/local/bin/rclone; \
    curl -fsSL "https://github.com/restic/restic/releases/download/v${RESTIC_VERSION}/restic_${RESTIC_VERSION}_linux_${restic_arch}.bz2" -o /tmp/restic.bz2; \
    bzip2 -d /tmp/restic.bz2; \
    cp /tmp/restic /usr/local/bin/restic; \
    chmod +x /usr/local/bin/restic; \
    rm -rf /tmp/rclone.zip /tmp/restic "${rclone_dir}"

# Install mongodump as a pinned version from MongoDB Database Tools.
RUN set -eux; \
    arch="${TARGETARCH:-amd64}"; \
    case "$arch" in \
      amd64) mongo_arch="x86_64" ;; \
      arm64) mongo_arch="aarch64" ;; \
      *) echo "Unsupported TARGETARCH: $arch" >&2; exit 1 ;; \
    esac; \
    url="https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian12-${mongo_arch}-${MONGO_TOOLS_VERSION}.tgz"; \
    curl -fsSL "$url" -o /tmp/mongodb-tools.tgz; \
    tar -xzf /tmp/mongodb-tools.tgz -C /tmp; \
    tools_dir="$(find /tmp -maxdepth 1 -type d -name 'mongodb-database-tools-*' | head -n1)"; \
    cp "$tools_dir/bin/mongodump" /usr/local/bin/mongodump; \
    chmod +x /usr/local/bin/mongodump; \
    rm -rf /tmp/mongodb-tools.tgz "$tools_dir"

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

COPY app /app/app
COPY docker/entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh && mkdir -p /app/data /backups /root/.config/rclone /root/.cache/rclone

EXPOSE 8090

ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8090"]
