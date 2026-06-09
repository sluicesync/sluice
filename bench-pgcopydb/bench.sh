#!/usr/bin/env bash
# bench.sh — run ONE PG->PG copy (sluice or pgcopydb) against the bench corpus,
# time it, and verify zero-loss. Tier A: both tools run as containers on the
# `benchnet` Docker network against two tuned postgres:16 containers
# (bench-src / bench-dst), so neither pays a host->container tax.
#
# Hardened against the Rancher/WSL2 "timed out dialing Hyper-V socket" blip
# (intermittent ~10% on this host): every docker CLI call is retry-wrapped, and
# the copy itself runs DETACHED + polled (a control-plane blip during the run
# can't abort the already-running container's data plane).
#
# Usage:
#   bench.sh sluice   <label> [extra sluice migrate flags...]
#   bench.sh pgcopydb <label> [extra pgcopydb clone flags...]
set -uo pipefail
export MSYS_NO_PATHCONV=1  # stop Git-Bash translating container-side paths (e.g. pgcopydb args) to C:\...

NET=benchnet
SRC_URI="postgres://postgres:bench@bench-src:5432/benchdb?sslmode=disable"
DST_URI="postgres://postgres:bench@bench-dst:5432/benchdb?sslmode=disable"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGDIR="$HERE/results"; VERIFY="$HERE/verify.sql"
mkdir -p "$LOGDIR"

# checksum <container>: define bench_checksum TRANSIENTLY (create -> run -> drop)
# so it is never a persistent object the full-clone tools (pgcopydb) would copy
# into the target and collide on. Emits the md5 line.
checksum() {
  retry "checksum-$1" sh -c "{ cat '$VERIFY'; printf 'SELECT bench_checksum();\nDROP FUNCTION IF EXISTS bench_checksum();\n'; } | docker exec -i $1 psql -U postgres -d benchdb -tA 2>/dev/null | grep 'over tables'"
}

tool="${1:?tool: sluice|pgcopydb}"; shift
label="${1:?label}"; shift
extra=("$@")
stamp="$(echo "${tool}-${label}" | tr -c 'a-zA-Z0-9._-' '_')"
log="$LOGDIR/${stamp}.log"

# retry <label> <cmd...> — survive the intermittent Hyper-V-socket blip.
retry() {
  local what="$1"; shift
  local n
  for n in 1 2 3 4 5 6; do
    if "$@"; then return 0; fi
    echo "  (retry $what: attempt $n failed, backing off ${n}s)" >&2
    sleep "$n"
  done
  echo "  ($what failed after 6 attempts)" >&2
  return 1
}
psql_src() { retry "src-query" docker exec bench-src psql -U postgres -d benchdb -tAc "$1"; }
psql_dst() { retry "dst-query" docker exec bench-dst psql -U postgres -d benchdb -tAc "$1"; }

echo "### $tool / $label  (flags: ${extra[*]:-none})"

retry "target-reset" docker exec bench-dst psql -U postgres -d benchdb -q -c \
  "DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO postgres;" >/dev/null \
  || { echo "FATAL: could not reset target"; exit 1; }
echo "target reset"

heap_bytes="$(psql_src "SELECT sum(pg_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='r' AND n.nspname='public'")"
heap_mb=$(awk -v b="${heap_bytes:-0}" 'BEGIN{printf "%.0f", b/1048576}')
[ "${heap_mb:-0}" -lt 1000 ] && { echo "FATAL: source heap looks empty ($heap_mb MB)"; exit 1; }

cname="benchrun-${stamp}"
docker rm -f "$cname" >/dev/null 2>&1 || true
start=$(date +%s)
case "$tool" in
  sluice)
    retry "launch" docker run -d --name "$cname" --network "$NET" sluice-bench:main migrate \
      --source-driver=postgres --source="$SRC_URI" \
      --target-driver=postgres --target="$DST_URI" "${extra[@]}" >/dev/null ;;
  pgcopydb)
    retry "launch" docker run -d --name "$cname" --network "$NET" \
      -e PGCOPYDB_SOURCE_PGURI="$SRC_URI" -e PGCOPYDB_TARGET_PGURI="$DST_URI" \
      ghcr.io/dimitri/pgcopydb:latest \
      pgcopydb clone --no-owner --no-acl "${extra[@]}" >/dev/null ;;
  *) echo "unknown tool $tool"; exit 2 ;;
esac

# Poll for exit (a CLI blip just retries the poll; the container keeps running).
rc=""
while :; do
  st="$(retry "poll" docker inspect -f '{{.State.Status}}' "$cname" 2>/dev/null)"
  if [ "$st" = "exited" ]; then
    rc="$(docker inspect -f '{{.State.ExitCode}}' "$cname" 2>/dev/null)"
    break
  fi
  sleep 3
done
end=$(date +%s)
wall=$((end - start))
retry "logs" sh -c "docker logs '$cname' > '$log' 2>&1"
docker rm -f "$cname" >/dev/null 2>&1 || true

# sluice phase split from its structured-log timestamps (bulk vs index).
bulk_s=""; idx_s=""
if [ "$tool" = "sluice" ]; then
  te() { date -d "$1" +%s.%N 2>/dev/null || echo ""; }
  gt() { grep -oE "time=[^ ]+.*phase=$1" "$log" | head -1 | grep -oE 'time=[^ ]+' | cut -d= -f2; }
  t0=$(te "$(gt tables)"); t1=$(te "$(gt bulk_copy)"); t2=$(te "$(gt indexes)")
  [ -n "$t0" ] && [ -n "$t1" ] && bulk_s=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')
  [ -n "$t1" ] && [ -n "$t2" ] && idx_s=$(awk -v a="$t1" -v b="$t2" 'BEGIN{printf "%.1f", b-a}')
fi

cs_src="$(checksum bench-src)"
cs_dst="$(checksum bench-dst)"
zl="MISMATCH"; [ "$cs_src" = "$cs_dst" ] && zl="ZERO-LOSS-OK"

rate_basis="${bulk_s:-$wall}"
mbps=$(awk -v mb="$heap_mb" -v s="$rate_basis" 'BEGIN{ if(s>0) printf "%.1f", mb/s; else print "n/a" }')

echo "----"
printf 'tool=%s label=%s rc=%s\n' "$tool" "$label" "$rc"
printf 'heap=%s MB | wall=%ss | bulk=%ss | index=%ss\n' "$heap_mb" "$wall" "${bulk_s:-?}" "${idx_s:-?}"
printf 'bulk-copy throughput: %s MB/s (heap over %ss)\n' "$mbps" "$rate_basis"
printf 'zero-loss: %s\n  src=%s\n  dst=%s\n' "$zl" "$cs_src" "$cs_dst"
echo "log: $log"
