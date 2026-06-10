#!/usr/bin/env bash
# Bring the bench cluster up REUSING the already-seeded source so re-runs skip
# the ~30-min seed. The ~110 GB corpus persists in the `benchsrc` Docker named
# volume (container removal/restart don't touch it; only the TARGET is reset
# between runs). This re-attaches a postgres:16 container to that volume and
# brings up a fresh empty target, ready for bench.sh.
#
# LOCAL REUSE ONLY. A 110 GB volume isn't portable to GitHub Actions / remote;
# regenerate there from seed.sql + gen_fn.sql.
#
# First-time setup (no volume yet): boot the containers, run gen_fn.sql + the
# parallel seed loop, then verify.sql. After that, this script is all you need.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# max_connections=300 headroom for high-parallelism tuned runs (the connection
# budget only governs the TARGET; the SOURCE needs slots for table x within
# readers too — 8x8 over-ran the stock 100). Defaults (32-way) fit either way.
TUNE="-c max_connections=300 -c shared_buffers=2GB -c max_wal_size=16GB -c maintenance_work_mem=1GB -c checkpoint_timeout=30min -c checkpoint_completion_target=0.9 -c max_worker_processes=8 -c max_parallel_maintenance_workers=4 -c wal_compression=on -c synchronous_commit=off"

docker network create benchnet 2>/dev/null || true
docker rm -f bench-src bench-dst 2>/dev/null || true   # containers only — volumes are kept
docker volume create benchsrc >/dev/null               # no-op if it already holds the seed
docker volume rm benchdst 2>/dev/null || true          # target is always fresh
docker volume create benchdst >/dev/null

# POSTGRES_* env is honoured only on a fresh datadir, so bench-src reattaching to
# the seeded volume just boots the existing data; bench-dst initialises empty.
# --shm-size=8g: Docker's default 64 MB /dev/shm starves PG's parallel
# workers (parallel COPY / VACUUM / index builds use shared memory); without
# this pgcopydb's parallel steps die with "could not resize shared memory
# segment: No space left on device". Both tools need it for a valid/fair run.
docker run -d --name bench-src --network benchnet -p 5433:5432 --shm-size=8g \
  -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=benchdb \
  -v benchsrc:/var/lib/postgresql/data postgres:16 $TUNE >/dev/null
docker run -d --name bench-dst --network benchnet -p 5434:5432 --shm-size=8g \
  -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=benchdb \
  -v benchdst:/var/lib/postgresql/data postgres:16 $TUNE >/dev/null

for i in $(seq 1 30); do
  if docker exec bench-src pg_isready -U postgres >/dev/null 2>&1 && \
     docker exec bench-dst pg_isready -U postgres >/dev/null 2>&1; then break; fi
  sleep 2
done

# The corpus is pure data tables — bench_checksum is NOT persisted (bench.sh
# defines it transiently at checksum time so the full-clone tools never copy it).
echo "up: bench-src (seeded, port 5433) + bench-dst (empty, port 5434) on benchnet"
docker exec bench-src psql -U postgres -d benchdb -tAc \
  "SELECT 'source corpus: '||pg_size_pretty(sum(pg_total_relation_size(c.oid)))||' total, '||count(*)||' tables' FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='r' AND n.nspname='public'"
