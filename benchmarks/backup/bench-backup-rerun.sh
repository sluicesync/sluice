#!/usr/bin/env bash
# Backup-comparison AFTER-measurement: re-run the P1 legs on the same
# corpus with ADR-0084 (cross-table parallel backup reads + parallel
# restore apply) on main. Corpus = bench-src as mutated by the chunk-3
# bursts (~7% bigger than the original 133 GB — pg_dump/pg_restore are
# re-run fresh so every comparison is corpus-matched).
set -u
export MSYS_NO_PATHCONV=1
export PATH="$PATH:/c/Program Files/Rancher Desktop/resources/resources/win32/bin"

NET=benchnet
SRC='postgres://postgres:bench@bench-src:5432/benchdb?sslmode=disable'
DST='postgres://postgres:bench@bench-dst:5432/benchdb?sslmode=disable'
IMG=sluice-bench:adr0084

echo "=== [0] build sluice from main + bench image ==="
cd /c/code/sluice
git log -1 --oneline
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o benchmarks/pgcopydb/img/sluice ./cmd/sluice || exit 1
docker build -q -t "$IMG" benchmarks/pgcopydb/img/ || exit 1

echo "=== [1] corpus size (post-burst) ==="
docker exec bench-src psql -U postgres -d benchdb -tAc "SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))), pg_size_pretty(sum(pg_relation_size(c.oid))), sum(c.reltuples)::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='r'"

echo "=== [2] volumes: drop old, create fresh ==="
docker volume rm bkupstore pgdumpstore >/dev/null 2>&1
docker volume create bkupstore3 >/dev/null
docker volume create pgdumpstore2 >/dev/null
docker run --rm --user root -v bkupstore3:/store postgres:16 chown 65532:65532 /store

echo "=== [3] pg_dump -Fd -j8 zstd (corpus-matched comparator) ==="
t0=$(date +%s)
docker run --rm --network "$NET" -v pgdumpstore2:/store -e PGPASSWORD=bench postgres:16 \
  pg_dump -h bench-src -U postgres -d benchdb -Fd -j 8 --compress=zstd:3 -f /store/dump
rc=$?; t1=$(date +%s); echo "pgdump-j8: exit=$rc wall=$((t1-t0))s"
docker run --rm -v pgdumpstore2:/store postgres:16 du -sh /store/dump

echo "=== [4] sluice backup full (defaults = ADR-0084 parallel) ==="
t0=$(date +%s)
docker run --rm --network "$NET" -v bkupstore3:/store "$IMG" \
  backup full --source-driver=postgres --source="$SRC" --output-dir=/store \
  >/tmp/bk-after.log 2>&1
rc=$?; t1=$(date +%s); echo "sluice-backup-parallel: exit=$rc wall=$((t1-t0))s"
grep -E "parallel reads engaged|parallel reads not engaged|backup complete" /tmp/bk-after.log
docker run --rm -v bkupstore3:/store postgres:16 du -sh /store

echo "=== [5] pg_restore -j8 (corpus-matched comparator) ==="
docker exec bench-dst psql -U postgres -d benchdb -q -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
t0=$(date +%s)
docker run --rm --network "$NET" -v pgdumpstore2:/store -e PGPASSWORD=bench postgres:16 \
  pg_restore -h bench-dst -U postgres -d benchdb -j 8 --no-owner --no-acl /store/dump
rc=$?; t1=$(date +%s); echo "pgrestore-j8: exit=$rc wall=$((t1-t0))s"
docker exec bench-dst psql -U postgres -d benchdb -tAc "SELECT count(*) FROM pg_tables WHERE schemaname='public'"

echo "=== [6] sluice restore (defaults = ADR-0084 parallel) ==="
docker exec bench-dst psql -U postgres -d benchdb -q -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
t0=$(date +%s)
docker run --rm --network "$NET" -v bkupstore3:/store "$IMG" \
  restore --target-driver=postgres --target="$DST" --from-dir=/store \
  >/tmp/rst-after.log 2>&1
rc=$?; t1=$(date +%s); echo "sluice-restore-parallel: exit=$rc wall=$((t1-t0))s"
grep -E "parallel apply engaged|parallel apply not engaged|restore complete|restored" /tmp/rst-after.log | tail -5
echo "--- verify counts src vs dst:"
docker exec bench-src psql -U postgres -d benchdb -tAc "SELECT count(*), sum(n_live_tup) FROM pg_stat_user_tables"
docker exec bench-dst psql -U postgres -d benchdb -tAc "SELECT count(*), sum(n_live_tup) FROM pg_stat_user_tables"
echo "DONE"
