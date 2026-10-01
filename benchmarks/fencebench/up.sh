#!/usr/bin/env bash
# up.sh — bring up the fencebench containers: a MySQL and a Postgres source,
# a MySQL and a Postgres target, and a `tc` sidecar in each target's network
# namespace (cell.sh adds netem latency there for LAT > 0 cells).
#
#   fb-mysrc :13306   MySQL 8.0, GTID, row binlog      root / pw
#   fb-mydst :13307   MySQL 8.0                        root / pw
#   fb-pgsrc :15432   Postgres 16, wal_level=logical   postgres / pw
#   fb-pgdst :15433   Postgres 16                      postgres / pw
#   fb-tc-mydst, fb-tc-pgdst   alpine + iproute2, NET_ADMIN, sharing the target's netns
#
# Every cell drops and recreates its `bench` database, so the containers hold
# no state between cells. Tear down with down.sh.
set -euo pipefail
MYSQL_IMG="${MYSQL_IMG:-mysql:8.0}"
PG_IMG="${PG_IMG:-postgres:16}"

docker rm -f fb-mysrc fb-mydst fb-pgsrc fb-pgdst fb-tc-mydst fb-tc-pgdst >/dev/null 2>&1 || true

docker run -d --name fb-mysrc -p 13306:3306 -e MYSQL_ROOT_PASSWORD=pw "$MYSQL_IMG" \
  --server-id=1 --log-bin=binlog --binlog-format=ROW --binlog-row-image=FULL \
  --gtid-mode=ON --enforce-gtid-consistency=ON --max-connections=500 >/dev/null
docker run -d --name fb-mydst -p 13307:3306 -e MYSQL_ROOT_PASSWORD=pw "$MYSQL_IMG" \
  --server-id=2 --max-connections=500 >/dev/null
docker run -d --name fb-pgsrc -p 15432:5432 -e POSTGRES_PASSWORD=pw "$PG_IMG" \
  -c wal_level=logical -c max_replication_slots=20 -c max_wal_senders=20 -c max_connections=300 >/dev/null
docker run -d --name fb-pgdst -p 15433:5432 -e POSTGRES_PASSWORD=pw "$PG_IMG" \
  -c max_connections=300 >/dev/null

for i in $(seq 1 90); do
  ok=1
  docker exec fb-mysrc mysqladmin -uroot -ppw ping >/dev/null 2>&1 || ok=0
  docker exec fb-mydst mysqladmin -uroot -ppw ping >/dev/null 2>&1 || ok=0
  docker exec fb-pgsrc pg_isready -U postgres >/dev/null 2>&1 || ok=0
  docker exec fb-pgdst pg_isready -U postgres >/dev/null 2>&1 || ok=0
  [ $ok = 1 ] && break
  sleep 2
done
[ $ok = 1 ] || { echo "FATAL: the fencebench databases did not come up"; exit 1; }

# The sidecars share each target's network namespace, so a qdisc on their
# eth0 delays the target's traffic in both directions.
for t in mydst pgdst; do
  docker run -d --name "fb-tc-$t" --network "container:fb-$t" --cap-add NET_ADMIN alpine:3.20 \
    sh -c 'apk add -q --no-cache iproute2 && sleep infinity' >/dev/null
done

wl="$(docker exec fb-pgsrc psql -U postgres -tAc 'SHOW wal_level')"
[ "$wl" = "logical" ] || { echo "FATAL: fb-pgsrc wal_level=$wl (need logical)"; exit 1; }
echo "up: fb-mysrc :13306, fb-mydst :13307, fb-pgsrc :15432, fb-pgdst :15433 (+ fb-tc-mydst, fb-tc-pgdst)"
