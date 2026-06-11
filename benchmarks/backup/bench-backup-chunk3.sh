#!/usr/bin/env bash
# Backup-comparison chunk 3: the j1 decomposition leg + the incremental-vs-redump
# measurement (THE pg_dump-has-no-incremental story).
#
# Prereqs (already true when this runs):
#   * bench-src (5433, wal_level=logical) still holds the seeded 133 GB corpus
#   * the sluice full backup (snapshot-anchored) is in volume bkupstore,
#     manifest EndPosition = {"slot":"sluice_slot","lsn":"27/221582E8"}
#   * pg_restore -j8 leg has finished (disk quiet)
#   * sluice-bench:v09934 image present
#
# Sequence (order matters):
#   1. pg_dump -Fd -j1 zstd:3  — single-worker comparator, BEFORE the corpus
#      mutates, decomposing the full-backup gap into parallelism vs per-row cost.
#   2. Create the standing chain slot sluice_slot (pgoutput). No writes have
#      occurred since the full's consistent_point, so the chain has no gap even
#      though the slot is created after the full (methodology note in the doc).
#   3. Write burst on bench-src: 3.0M inserts + 0.5M updates + 0.1M deletes
#      = 3.6M row events (~1 GB heap delta).
#   4. sluice backup incremental --max-changes=3600000 (stops at backlog end).
#      Comparator is the already-measured 232 s / 16 GB full re-dump.
set -u
export MSYS_NO_PATHCONV=1
export PATH="$PATH:/c/Program Files/Rancher Desktop/resources/resources/win32/bin"

NET=benchnet
SRC_DSN='postgres://postgres:bench@bench-src:5432/benchdb?sslmode=disable'
PSQL_SRC=(docker exec bench-src psql -U postgres -d benchdb)

echo "=== [1/4] pg_dump -Fd -j1 zstd (single-worker comparator) ==="
docker run --rm -v pgdumpstore:/store postgres:16 sh -c 'rm -rf /store/dump-j1'
t0=$(date +%s)
docker run --rm --network "$NET" -v pgdumpstore:/store -e PGPASSWORD=bench postgres:16 \
  pg_dump -h bench-src -U postgres -d benchdb -Fd -j 1 --compress=zstd:3 -f /store/dump-j1
rc=$?
t1=$(date +%s)
echo "pgdump-j1: exit=$rc wall=$((t1-t0))s"
docker run --rm -v pgdumpstore:/store postgres:16 du -sh /store/dump-j1

echo "=== [2/4] create standing chain slot sluice_slot ==="
"${PSQL_SRC[@]}" -c "SELECT pg_create_logical_replication_slot('sluice_slot','pgoutput')"
"${PSQL_SRC[@]}" -c "SELECT slot_name, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots"

echo "=== [3/4] write burst: 3.0M ins + 0.5M upd + 0.1M del = 3.6M row events ==="
t0=$(date +%s)
for t in medium_1 medium_2 medium_3 medium_4 medium_5; do
  "${PSQL_SRC[@]}" -c "INSERT INTO ${t} (id, user_id, amount, event_type, payload, created_at, is_active, filler)
    SELECT g, (g % 5000000) + 1, round((random()*100000)::numeric,2),
           (ARRAY['click','view','purchase','signup','logout','error','refund','search'])[1 + (g % 8)],
           jsonb_build_object('k', md5(g::text), 'n', g % 1000, 'tags', to_jsonb(ARRAY[g % 7, g % 13])),
           timestamptz '2020-01-01' + ((g % 126230400) || ' seconds')::interval,
           (g % 3) = 0, repeat('x', 80)
    FROM generate_series(3500001, 4100000) g"
done
"${PSQL_SRC[@]}" -c "UPDATE huge_1 SET amount = amount + 1, is_active = NOT is_active WHERE id BETWEEN 1 AND 500000"
"${PSQL_SRC[@]}" -c "DELETE FROM medium_40 WHERE id BETWEEN 1 AND 100000"
t1=$(date +%s)
echo "burst: wall=$((t1-t0))s (3,600,000 row events)"

echo "=== [4/4] sluice backup incremental (the no-redump path) ==="
t0=$(date +%s)
docker run --rm --network "$NET" -v bkupstore:/store sluice-bench:v09934 \
  backup incremental \
  --source-driver=postgres --source="$SRC_DSN" \
  --output-dir=/store --max-changes=3600000 --window=60m \
  >/tmp/incr.log 2>&1
rc=$?
t1=$(date +%s)
tail -15 /tmp/incr.log
echo "sluice-incremental: exit=$rc wall=$((t1-t0))s"
docker run --rm -v bkupstore:/store postgres:16 sh -c 'du -sh /store; du -sh /store/chunks 2>/dev/null; ls /store/manifests 2>/dev/null || ls /store'
