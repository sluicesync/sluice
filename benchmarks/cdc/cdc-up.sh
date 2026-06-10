#!/usr/bin/env bash
# cdc-up.sh — bring up the CDC-bench cluster and seed the source.
#
# Differences from benchmarks/pgcopydb/bench-up.sh (the migrate harness):
#   * SOURCE runs wal_level=logical + a generous max_replication_slots /
#     max_wal_senders — required for sluice's PG CDC replication slot.
#   * the corpus is the LOWER-SCALE cdc_NN tables (gen_cdc.sql), good-sized
#     but minutes-to-copy, so the run spends its time on continuous-sync
#     correctness under concurrent writes rather than raw throughput.
#   * the seed persists in the `benchcdcsrc` volume; re-runs reuse it (the
#     TARGET is always reset fresh by cdc-bench.sh).
#
# Usage: cdc-up.sh [n_tables] [rows_per_table]   (defaults: 12 tables x 2,000,000 rows ~ 5 GB heap)
# LOCAL reuse only — regenerate from gen_cdc.sql elsewhere.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NT="${1:-12}"; ROWS="${2:-2000000}"

TUNE="-c max_connections=300 -c shared_buffers=2GB -c max_wal_size=16GB -c maintenance_work_mem=1GB -c checkpoint_timeout=30min -c checkpoint_completion_target=0.9 -c wal_compression=on -c synchronous_commit=off"
# Source-only: logical decoding for the CDC slot.
SRC_TUNE="$TUNE -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10"

docker network create benchnet 2>/dev/null || true
docker rm -f bench-cdc-src bench-cdc-dst 2>/dev/null || true   # containers only; src volume kept
docker volume create benchcdcsrc >/dev/null                    # no-op if it already holds the seed
docker volume rm benchcdcdst 2>/dev/null || true               # target always fresh
docker volume create benchcdcdst >/dev/null

# Host ports 5453/5454 (debugging only — the bench containers talk over the
# benchnet Docker network). Chosen to avoid the local rig's 5442/5443.
docker run -d --name bench-cdc-src --network benchnet -p 5453:5432 --shm-size=2g \
  -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=benchdb \
  -v benchcdcsrc:/var/lib/postgresql/data postgres:16 $SRC_TUNE >/dev/null
docker run -d --name bench-cdc-dst --network benchnet -p 5454:5432 --shm-size=2g \
  -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=benchdb \
  -v benchcdcdst:/var/lib/postgresql/data postgres:16 $TUNE >/dev/null

for i in $(seq 1 30); do
  if docker exec bench-cdc-src pg_isready -U postgres >/dev/null 2>&1 && \
     docker exec bench-cdc-dst pg_isready -U postgres >/dev/null 2>&1; then break; fi
  sleep 2
done

# Confirm logical decoding is on (a mis-tuned source would fail the slot later
# with a confusing error; fail loudly here instead).
wl="$(docker exec bench-cdc-src psql -U postgres -d benchdb -tAc 'SHOW wal_level')"
[ "$wl" = "logical" ] || { echo "FATAL: source wal_level=$wl (need logical)"; exit 1; }

# Seed only if the corpus isn't already present (volume reuse).
have="$(docker exec bench-cdc-src psql -U postgres -d benchdb -tAc \
  "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='r' AND n.nspname='public' AND c.relname LIKE 'cdc\\_%'")"
if [ "${have:-0}" -lt "$NT" ]; then
  echo "seeding ${NT} tables x ${ROWS} rows ..."
  docker exec -i bench-cdc-src psql -U postgres -d benchdb -q < "$HERE/gen_cdc.sql"
  # Parallelise the per-table generate_series across a few connections.
  for ((t=0; t<NT; t++)); do
    tbl="$(printf 'cdc_%02d' "$t")"
    docker exec -i bench-cdc-src psql -U postgres -d benchdb -qtAc "SELECT gen_cdc_table('${tbl}', ${ROWS})" &
    if (( (t+1) % 4 == 0 )); then wait; fi
  done
  wait
else
  echo "reusing existing seed (${have} cdc_ tables in benchcdcsrc volume)"
fi

echo "up: bench-cdc-src (wal_level=logical, seeded, port 5453) + bench-cdc-dst (empty, port 5454)"
docker exec bench-cdc-src psql -U postgres -d benchdb -tAc \
  "SELECT 'source corpus: '||pg_size_pretty(sum(pg_total_relation_size(c.oid)))||' total, '||count(*)||' cdc tables' FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='r' AND n.nspname='public' AND c.relname LIKE 'cdc\\_%'"
