#!/usr/bin/env bash
# build-prebaked-images.sh — bake the heavy first-boot init step into
# MySQL / Postgres / PostGIS images so testcontainers cold-starts in CI
# avoid the 30-60s (under disk-I/O contention: 2-3 min) init writes.
#
# History: tasks #60, #63, #64, and #69 walked the budget upward
# (single-shot → 3-retry → 4-min timeout → WithWaitStrategyAndDeadline)
# without eliminating the root cause — concurrent MySQL containers booting
# on the same /var/lib/docker hammer the disk while
# `mysqld --initialize-insecure` writes ~50-100MB of system tables
# (and PG initdb writes ~40MB). Pre-baking the init step into the image
# makes the second-boot path the only path; containers reach
# "ready to accept connections" in ~5s instead of ~30-60s.
#
# What it does for each engine:
#   1. Pull the upstream base image (mysql:8.0 / postgres:16 / postgis/postgis:16-3.4).
#   2. Generate a Dockerfile that FROMs the base and RUNs the
#      init step inline. The init also creates the test user with the
#      password the integration suite uses (rootpw for MySQL, test/test
#      for PG) plus the seed databases that
#      testcontainers.WithDatabase(...) would normally create at first
#      boot (these env vars are ignored on a pre-initialized datadir, so
#      we have to bake them).
#   3. `docker build` produces a tagged image; ENTRYPOINT / CMD /
#      EXPOSE / USER are preserved from the base.
#   4. Push to ghcr.io (skipped when SKIP_PUSH=1, useful for local dev).
#
# Why docker-build instead of docker-run + docker-commit:
#   docker-commit needs `docker run -v $hostfile:/tmp/x.sh` to inject a
#   multi-statement script, which trips Git Bash / MSYS path-translation
#   on Windows. docker-build's RUN happens entirely inside the build
#   container — no host-path translation, portable across Linux CI and
#   local Windows.
#
# Byte-equivalence with the upstream image:
#   The pre-baked image is the SAME base image plus the result of the
#   init step the tests would otherwise run on first boot. No extra
#   layers, no extensions, no config files copied in. This is what makes
#   the image safe to swap in without touching test fidelity — the only
#   observable difference is "the system tables / WAL / postgis catalog
#   are already on disk", which is exactly what every test relies on
#   becoming true within the wait-for-ready window anyway.
#
# Idempotency:
#   `docker manifest inspect` is consulted before each push. If the
#   target tag already exists with the same base image digest as the
#   upstream pull just produced, the bake is skipped. This makes the
#   weekly cron a no-op when upstream hasn't republished its tag — and
#   safe to re-run manually after a partial failure.
#
# Auth:
#   When pushing, $GHCR_TOKEN (or $GITHUB_TOKEN with packages:write
#   scope) must be exported. Set GHCR_USER to the GitHub username
#   that owns the token (defaults to `orware`).
#
# Usage:
#   ./scripts/build-prebaked-images.sh             # build + push all 3
#   SKIP_PUSH=1 ./scripts/build-prebaked-images.sh # build only, no push
#   ENGINES=mysql ./scripts/build-prebaked-images.sh
#   ENGINES="postgres postgis" ./scripts/build-prebaked-images.sh
#
# CI usage: invoked by .github/workflows/build-prebaked-images.yml on a
# weekly cron + workflow_dispatch.

set -euo pipefail

# --- Config -----------------------------------------------------------

GHCR_NAMESPACE="${GHCR_NAMESPACE:-ghcr.io/orware}"
GHCR_USER="${GHCR_USER:-orware}"

# Engines to bake. Override via $ENGINES (space-separated subset).
ENGINES_DEFAULT="mysql postgres postgis"
ENGINES="${ENGINES:-$ENGINES_DEFAULT}"

# Base + target image identifiers. Keep these aligned with the imports
# in internal/engines/{mysql,postgres}/shared_container_integration_test.go
# and .github/workflows/ci.yml's docker-pull steps.
MYSQL_BASE_IMAGE="mysql:8.0"
MYSQL_TARGET_IMAGE="${GHCR_NAMESPACE}/sluice-mysql:8.0-prebaked"

POSTGRES_BASE_IMAGE="postgres:16"
POSTGRES_TARGET_IMAGE="${GHCR_NAMESPACE}/sluice-postgres:16-prebaked"

POSTGIS_BASE_IMAGE="postgis/postgis:16-3.4"
POSTGIS_TARGET_IMAGE="${GHCR_NAMESPACE}/sluice-postgis:16-3.4-prebaked"

# --- Helpers ----------------------------------------------------------

log() {
    printf '[build-prebaked] %s\n' "$*" >&2
}

# ghcr_login authenticates the local docker daemon against ghcr.io
# using $GHCR_TOKEN (preferred) or $GITHUB_TOKEN. Skipped when
# SKIP_PUSH=1 because no push means no auth required for the read-side
# tag-existence check below (which uses an anonymous manifest pull
# attempt — GHCR returns a 401 for "exists but not authorized", a 404
# for "doesn't exist"; either is treated as "needs bake").
ghcr_login() {
    if [[ "${SKIP_PUSH:-0}" == "1" ]]; then
        log "SKIP_PUSH=1: skipping ghcr login"
        return 0
    fi
    local token="${GHCR_TOKEN:-${GITHUB_TOKEN:-}}"
    if [[ -z "$token" ]]; then
        log "ERROR: need GHCR_TOKEN or GITHUB_TOKEN env var to push to ghcr.io"
        return 1
    fi
    log "logging into ghcr.io as ${GHCR_USER}"
    printf '%s' "$token" | docker login ghcr.io -u "$GHCR_USER" --password-stdin
}

# image_exists_remote returns 0 if the target tag already exists on
# ghcr.io, 1 otherwise. Used for idempotency — if the tag exists and
# its base digest matches the current upstream, the bake is a no-op.
image_exists_remote() {
    local image="$1"
    docker manifest inspect "$image" >/dev/null 2>&1
}

# base_digest_local returns the sha256 manifest digest of the locally
# pulled base image. The pre-baked image's bake-time base-digest is
# stored as a docker label so subsequent runs can compare it without
# rebuilding.
base_digest_local() {
    local image="$1"
    docker image inspect --format '{{index .RepoDigests 0}}' "$image" 2>/dev/null || true
}

# baked_label_remote returns the value of the sluice.basedigest label
# on the remote pre-baked image, or empty if not set / image missing.
baked_label_remote() {
    local image="$1"
    docker manifest inspect "$image" >/dev/null 2>&1 || {
        printf ''
        return
    }
    # `docker pull` is the simplest portable way to read a label from a
    # remote — manifest inspect only exposes the raw v2 manifest, not
    # the config-layer labels. We accept the bandwidth cost of a pull
    # here (only on the "tag exists, recheck digest" path) because
    # ghcr.io is layer-deduplicated against the upstream base, so the
    # pull is mostly metadata.
    docker pull "$image" >/dev/null 2>&1 || {
        printf ''
        return
    }
    docker image inspect --format '{{ index .Config.Labels "sluice.basedigest" }}' "$image" 2>/dev/null || printf ''
}

# bake_via_dockerfile builds a tagged image from a heredoc Dockerfile.
# Caller passes the target tag and the Dockerfile body on stdin.
bake_via_dockerfile() {
    local target="$1"
    local builddir
    builddir="$(mktemp -d)"
    cat >"$builddir/Dockerfile"
    docker build -t "$target" "$builddir"
    rm -rf "$builddir"
}

# bake_mysql produces the pre-baked MySQL image:
#   - mysqld --initialize-insecure populates /var/lib/mysql.
#   - A temporary mysqld instance is started on a unix socket to set
#     the root password to `rootpw` and create the seed databases.
#   - docker build preserves ENTRYPOINT (docker-entrypoint.sh), CMD
#     (mysqld), and EXPOSE (3306, 33060) from the base, so
#     testcontainers boot semantics are identical to upstream mysql:8.0.
#
# The --log-bin / --binlog-format / --binlog-row-image flags are NOT
# passed to the bake-time mysqld because those are runtime mysqld
# behaviours — the resulting binlog file lives in the datadir but is
# discarded on second boot when testcontainers passes its own Cmd args.
# Passing them here would only produce noise; they're applied at every
# test boot from the TestMain Cmd args.
#
# Seed databases (matching mysqltc.WithDatabase calls across the
# integration suite — these env vars are ignored on a pre-initialized
# datadir, so we have to bake them):
#   - sluice_shared_seed: shared-TestMain seed db (per-test reset drops
#     and recreates the test's own db, so this just needs to exist for
#     the boot wait to authenticate).
#   - source_db: dominant per-test db across internal/pipeline + a few
#     internal/engines/mysql per-test boots.
#   - warehouse: shapea_spike + shard_consolidation_router_pg.
# Tests that need other db names (dynamic dbName in
# shard_consolidation_phase2e_streamer_pg) CREATE DATABASE explicitly
# after boot — they don't depend on the seed list.
bake_mysql() {
    log "baking ${MYSQL_TARGET_IMAGE}"

    docker pull "$MYSQL_BASE_IMAGE"
    local base_digest
    base_digest="$(base_digest_local "$MYSQL_BASE_IMAGE")"
    log "base digest: ${base_digest}"

    if image_exists_remote "$MYSQL_TARGET_IMAGE"; then
        local existing
        existing="$(baked_label_remote "$MYSQL_TARGET_IMAGE")"
        if [[ -n "$existing" && "$existing" == "$base_digest" ]]; then
            log "${MYSQL_TARGET_IMAGE} already up-to-date against base digest; skipping"
            return 0
        fi
        log "remote tag exists but base digest differs (was=${existing}); rebaking"
    fi

    log "running mysqld --initialize-insecure + root-password fixup"
    bake_via_dockerfile "$MYSQL_TARGET_IMAGE" <<DOCKERFILE
FROM ${MYSQL_BASE_IMAGE}
LABEL sluice.basedigest="${base_digest}"
LABEL sluice.baseimage="${MYSQL_BASE_IMAGE}"
USER root
RUN set -e; \\
    mysqld --initialize-insecure --user=mysql --datadir=/var/lib/mysql; \\
    mysqld --user=mysql --datadir=/var/lib/mysql --skip-networking --socket=/tmp/mysql-bake.sock & \\
    mysqld_pid=\$!; \\
    i=0; \\
    while [ \$i -lt 30 ]; do \\
        if mysqladmin --socket=/tmp/mysql-bake.sock ping >/dev/null 2>&1; then break; fi; \\
        sleep 1; \\
        i=\$((i + 1)); \\
    done; \\
    mysql --socket=/tmp/mysql-bake.sock -uroot -e "ALTER USER 'root'@'localhost' IDENTIFIED BY 'rootpw'; CREATE USER 'root'@'%' IDENTIFIED BY 'rootpw'; GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION; FLUSH PRIVILEGES; CREATE DATABASE sluice_shared_seed CHARACTER SET utf8mb4; CREATE DATABASE source_db CHARACTER SET utf8mb4; CREATE DATABASE warehouse CHARACTER SET utf8mb4;"; \\
    mysqladmin --socket=/tmp/mysql-bake.sock -uroot -prootpw shutdown; \\
    wait \$mysqld_pid || true; \\
    rm -f /tmp/mysql-bake.sock
DOCKERFILE

    if [[ "${SKIP_PUSH:-0}" != "1" ]]; then
        log "pushing ${MYSQL_TARGET_IMAGE}"
        docker push "$MYSQL_TARGET_IMAGE"
    fi
}

# bake_postgres produces the pre-baked Postgres image:
#   - initdb populates /var/lib/postgresql/data with the cluster.
#   - A temporary postgres on a unix socket creates the `test`
#     superuser and the seed databases.
#   - docker build preserves ENTRYPOINT, CMD, EXPOSE, USER from the
#     base so testcontainers boot semantics are identical.
#
# Runtime GUCs (wal_level=logical, max_wal_senders=4,
# max_replication_slots=4) are NOT baked into postgresql.conf — they're
# set by testcontainers' Cmd args at every boot, which would override
# any baked value anyway. We deliberately keep postgresql.conf
# untouched so the runtime args win.
#
# Seed databases mirror the MySQL list (matching pgtc.WithDatabase
# calls across the integration suite).
bake_postgres() {
    log "baking ${POSTGRES_TARGET_IMAGE}"

    docker pull "$POSTGRES_BASE_IMAGE"
    local base_digest
    base_digest="$(base_digest_local "$POSTGRES_BASE_IMAGE")"
    log "base digest: ${base_digest}"

    if image_exists_remote "$POSTGRES_TARGET_IMAGE"; then
        local existing
        existing="$(baked_label_remote "$POSTGRES_TARGET_IMAGE")"
        if [[ -n "$existing" && "$existing" == "$base_digest" ]]; then
            log "${POSTGRES_TARGET_IMAGE} already up-to-date against base digest; skipping"
            return 0
        fi
        log "remote tag exists but base digest differs (was=${existing}); rebaking"
    fi

    log "running initdb + test-user fixup"
    # psql with multiple statements via `-c` wraps them in an implicit
    # transaction; CREATE DATABASE can't run inside a transaction
    # block. Issue each top-level statement as its own `-c` so psql
    # opens / closes a new transaction per statement.
    bake_via_dockerfile "$POSTGRES_TARGET_IMAGE" <<DOCKERFILE
FROM ${POSTGRES_BASE_IMAGE}
LABEL sluice.basedigest="${base_digest}"
LABEL sluice.baseimage="${POSTGRES_BASE_IMAGE}"
USER postgres
RUN set -e; \\
    echo postgres > /tmp/pgpw; \\
    initdb --username=postgres --pwfile=/tmp/pgpw --auth-local=trust --auth-host=trust --encoding=UTF8 -D /var/lib/postgresql/data; \\
    rm -f /tmp/pgpw; \\
    echo "host all all 0.0.0.0/0 trust" >> /var/lib/postgresql/data/pg_hba.conf; \\
    echo "host all all ::/0 trust"      >> /var/lib/postgresql/data/pg_hba.conf; \\
    mkdir -p /tmp/pgsock; \\
    pg_ctl -D /var/lib/postgresql/data -o "-c listen_addresses='' -c unix_socket_directories='/tmp/pgsock'" -l /tmp/pg.log -w start; \\
    psql -h /tmp/pgsock -U postgres -d postgres \\
        -c "CREATE ROLE test WITH SUPERUSER LOGIN PASSWORD 'test'" \\
        -c "CREATE DATABASE sluice_shared_seed OWNER test" \\
        -c "CREATE DATABASE source_db OWNER test" \\
        -c "CREATE DATABASE warehouse OWNER test"; \\
    pg_ctl -D /var/lib/postgresql/data -w stop
DOCKERFILE

    if [[ "${SKIP_PUSH:-0}" != "1" ]]; then
        log "pushing ${POSTGRES_TARGET_IMAGE}"
        docker push "$POSTGRES_TARGET_IMAGE"
    fi
}

# bake_postgis mirrors bake_postgres but uses the postgis-flavored
# base image. PostGIS adds the postgis/postgis_topology extension
# catalog rows via CREATE EXTENSION on first connect; pre-baking
# initdb means we still pay the CREATE EXTENSION cost lazily, but the
# much heavier initdb step is reused.
bake_postgis() {
    log "baking ${POSTGIS_TARGET_IMAGE}"

    docker pull "$POSTGIS_BASE_IMAGE"
    local base_digest
    base_digest="$(base_digest_local "$POSTGIS_BASE_IMAGE")"
    log "base digest: ${base_digest}"

    if image_exists_remote "$POSTGIS_TARGET_IMAGE"; then
        local existing
        existing="$(baked_label_remote "$POSTGIS_TARGET_IMAGE")"
        if [[ -n "$existing" && "$existing" == "$base_digest" ]]; then
            log "${POSTGIS_TARGET_IMAGE} already up-to-date against base digest; skipping"
            return 0
        fi
        log "remote tag exists but base digest differs (was=${existing}); rebaking"
    fi

    log "running initdb + test-user fixup (postgis flavor)"
    bake_via_dockerfile "$POSTGIS_TARGET_IMAGE" <<DOCKERFILE
FROM ${POSTGIS_BASE_IMAGE}
LABEL sluice.basedigest="${base_digest}"
LABEL sluice.baseimage="${POSTGIS_BASE_IMAGE}"
USER postgres
RUN set -e; \\
    echo postgres > /tmp/pgpw; \\
    initdb --username=postgres --pwfile=/tmp/pgpw --auth-local=trust --auth-host=trust --encoding=UTF8 -D /var/lib/postgresql/data; \\
    rm -f /tmp/pgpw; \\
    echo "host all all 0.0.0.0/0 trust" >> /var/lib/postgresql/data/pg_hba.conf; \\
    echo "host all all ::/0 trust"      >> /var/lib/postgresql/data/pg_hba.conf; \\
    mkdir -p /tmp/pgsock; \\
    pg_ctl -D /var/lib/postgresql/data -o "-c listen_addresses='' -c unix_socket_directories='/tmp/pgsock'" -l /tmp/pg.log -w start; \\
    psql -h /tmp/pgsock -U postgres -d postgres \\
        -c "CREATE ROLE test WITH SUPERUSER LOGIN PASSWORD 'test'" \\
        -c "CREATE DATABASE sluice_shared_seed OWNER test" \\
        -c "CREATE DATABASE source_db OWNER test" \\
        -c "CREATE DATABASE warehouse OWNER test"; \\
    pg_ctl -D /var/lib/postgresql/data -w stop
DOCKERFILE

    if [[ "${SKIP_PUSH:-0}" != "1" ]]; then
        log "pushing ${POSTGIS_TARGET_IMAGE}"
        docker push "$POSTGIS_TARGET_IMAGE"
    fi
}

# --- Main -------------------------------------------------------------

main() {
    log "engines=${ENGINES} push=$([[ "${SKIP_PUSH:-0}" == "1" ]] && echo no || echo yes)"

    ghcr_login

    for engine in $ENGINES; do
        case "$engine" in
            mysql)    bake_mysql ;;
            postgres) bake_postgres ;;
            postgis)  bake_postgis ;;
            *)
                log "ERROR: unknown engine '${engine}' (want one of: mysql postgres postgis)"
                exit 1
                ;;
        esac
    done

    log "done"
}

main "$@"
