#!/usr/bin/env bash
# bench-mysql-up.sh — bring up the MySQL bench cluster (src seeded + dst empty)
# REUSING the already-seeded source so re-runs skip the seed. The corpus
# persists in the `benchmysqlsrc` Docker named volume (container removal/restart
# don't touch it; only the TARGET is reset between runs).
#
# Two tuned mysql:8.0 containers on `benchnet`:
#   bench-mysql-src  port 3326  (seeded, persistent volume benchmysqlsrc)
#   bench-mysql-dst  port 3327  (empty, fresh volume each up)
# Ports 3326/3327 avoid the local rig's 3316/3317.
#
# Both MUST have local_infile=ON — sluice's LOAD DATA LOCAL INFILE fast loader
# needs it on the TARGET; the source loopback seed path uses it too. Tuned
# buffer pool / redo so the index-build timing is realistic, not log-starved.
#
# First-time setup (no volume yet): this script boots, seeds the tally, then
# fans the per-table gen_table() calls across parallel mysql connections, then
# verifies. After that, re-running is a ~10s container reattach (seed skipped).
#
# LOCAL REUSE ONLY. The volume isn't portable; regenerate from gen_mysql.sql
# elsewhere.
set -uo pipefail
export PATH="$PATH:/c/Program Files/Rancher Desktop/resources/resources/win32/bin"
export MSYS_NO_PATHCONV=1
export TESTCONTAINERS_RYUK_DISABLED=true

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NET=benchnet
IMG=mysql:8.0
SRC=bench-mysql-src
DST=bench-mysql-dst
ROOTPW=bench
DB=benchdb

# Corpus size: 30 tables x 1.5M rows. ~5-8 GB with the 4 secondary indexes.
NTABLES="${NTABLES:-30}"
NROWS="${NROWS:-1500000}"
SEED_PAR="${SEED_PAR:-6}"   # parallel seed connections

# Tuned MySQL config. local_infile=ON is mandatory (LOAD DATA fast loader).
# 3G buffer pool, big redo so index builds aren't redo-log-starved.
TUNE=(
  --local-infile=ON
  --max-connections=300
  --innodb-buffer-pool-size=3G
  --innodb-redo-log-capacity=4G
  --innodb-flush-log-at-trx-commit=2
  --innodb-flush-method=O_DIRECT
  --skip-log-bin
)

retry() { local what="$1"; shift; local n; for n in 1 2 3 4 5 6; do "$@" && return 0; echo "  (retry $what: attempt $n failed, backing off ${n}s)" >&2; sleep "$n"; done; echo "  ($what failed after 6 attempts)" >&2; return 1; }
mysql_root() { docker exec -i "$1" mysql --local-infile=1 -uroot -p"$ROOTPW" "${@:2}"; }

docker network create "$NET" 2>/dev/null || true
docker rm -f "$SRC" "$DST" 2>/dev/null || true     # containers only — src volume kept
docker volume create benchmysqlsrc >/dev/null        # no-op if it already holds the seed
docker volume rm benchmysqldst 2>/dev/null || true   # target is always fresh
docker volume create benchmysqldst >/dev/null

# MYSQL_* env is honoured only on a fresh datadir, so bench-mysql-src reattaching
# to the seeded volume just boots existing data; bench-mysql-dst initialises empty.
retry "run-src" docker run -d --name "$SRC" --network "$NET" -p 3326:3306 \
  -e MYSQL_ROOT_PASSWORD="$ROOTPW" -e MYSQL_DATABASE="$DB" \
  -v benchmysqlsrc:/var/lib/mysql "$IMG" "${TUNE[@]}" >/dev/null
retry "run-dst" docker run -d --name "$DST" --network "$NET" -p 3327:3306 \
  -e MYSQL_ROOT_PASSWORD="$ROOTPW" -e MYSQL_DATABASE="$DB" \
  -v benchmysqldst:/var/lib/mysql "$IMG" "${TUNE[@]}" >/dev/null

echo "waiting for MySQL to accept connections..."
for i in $(seq 1 60); do
  if mysql_root "$SRC" -e "SELECT 1" >/dev/null 2>&1 && \
     mysql_root "$DST" -e "SELECT 1" >/dev/null 2>&1; then break; fi
  sleep 2
done
mysql_root "$SRC" -e "SELECT 1" >/dev/null 2>&1 || { echo "FATAL: src never came up"; exit 1; }
mysql_root "$DST" -e "SELECT 1" >/dev/null 2>&1 || { echo "FATAL: dst never came up"; exit 1; }

# Already seeded? (bench_1 present with the expected rowcount) -> skip the seed.
existing="$(mysql_root "$SRC" -N -e "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$DB' AND table_name LIKE 'bench\\_%'" 2>/dev/null || echo 0)"
if [ "${existing:-0}" -ge "$NTABLES" ]; then
  echo "src already seeded ($existing bench tables) — skipping seed"
else
  echo "seeding tally + $NTABLES tables x $NROWS rows (parallel=$SEED_PAR) ..."
  # 1) tally + gen_table procedure
  retry "load-gen" sh -c "docker exec -i $SRC mysql --local-infile=1 -uroot -p$ROOTPW $DB < '$HERE/gen_mysql.sql'"
  # 2) fan gen_table() across parallel connections
  seed_one() {
    local n="$1"
    docker exec -i "$SRC" mysql -uroot -p"$ROOTPW" "$DB" \
      -e "CALL gen_table('bench_${n}', ${NROWS})" 2>&1 | grep -v "Using a password" || true
  }
  export -f seed_one; export SRC ROOTPW DB NROWS
  pids=()
  for n in $(seq 1 "$NTABLES"); do
    seed_one "$n" &
    pids+=($!)
    # throttle to SEED_PAR concurrent
    while [ "$(jobs -rp | wc -l)" -ge "$SEED_PAR" ]; do sleep 0.5; done
  done
  wait
  # drop the tally + helper proc so a full copy doesn't carry them to the target
  mysql_root "$SRC" "$DB" -e "DROP TABLE IF EXISTS _tally; DROP PROCEDURE IF EXISTS gen_table" 2>/dev/null || true
  echo "seed complete"
fi

# Realized size + table count.
echo "up: $SRC (seeded, port 3326) + $DST (empty, port 3327) on $NET"
mysql_root "$SRC" -N -e "
  SELECT CONCAT('source corpus: ',
    ROUND(SUM(data_length+index_length)/1073741824, 2), ' GiB total, ',
    ROUND(SUM(data_length)/1073741824, 2), ' GiB data, ',
    ROUND(SUM(index_length)/1073741824, 2), ' GiB index, ',
    COUNT(*), ' tables')
  FROM information_schema.tables
  WHERE table_schema='$DB' AND table_name LIKE 'bench\\_%'" 2>/dev/null
