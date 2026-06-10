#!/usr/bin/env bash
# bench-mysql.sh — run ONE MySQL->MySQL `sluice migrate` against the bench corpus,
# time it, and verify zero-loss + all-indexes-present. Same-engine isolates the
# ADR-0080 index-build-overlap signal (no cross-engine translation noise). Both
# the sluice container and the two tuned mysql:8.0 containers sit on the
# `benchnet` Docker network, so nothing pays a host->container tax.
#
# Hardened against the Rancher/WSL2 "timed out dialing Hyper-V socket" blip:
# every docker CLI call is retry-wrapped, and the migrate runs DETACHED + polled
# (a control-plane blip during the run can't abort the already-running
# container's data plane). Wall time comes from docker inspect StartedAt/
# FinishedAt, which is immune to control-plane blips.
#
# Usage:
#   SLUICE_IMG=sluice-bench-mysql:v0.99.30 bench-mysql.sh <label> [extra migrate flags...]
#   SLUICE_IMG=sluice-bench-mysql:v0.99.29 bench-mysql.sh <label> [extra migrate flags...]
set -uo pipefail
export PATH="$PATH:/c/Program Files/Rancher Desktop/resources/resources/win32/bin"
export MSYS_NO_PATHCONV=1
export TESTCONTAINERS_RYUK_DISABLED=true

NET=benchnet
SRC=bench-mysql-src
DST=bench-mysql-dst
ROOTPW=bench
DB=benchdb
IMG="${SLUICE_IMG:?set SLUICE_IMG=sluice-bench-mysql:v0.99.30|v0.99.29}"

# sluice DSNs (resolve over benchnet to the container hostnames). The MySQL
# driver DSN form sluice expects: user:pass@tcp(host:port)/db
SRC_DSN="root:${ROOTPW}@tcp(${SRC}:3306)/${DB}"
DST_DSN="root:${ROOTPW}@tcp(${DST}:3306)/${DB}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGDIR="$HERE/results"; VERIFY="$HERE/verify_mysql.sql"
mkdir -p "$LOGDIR"

retry() { local what="$1"; shift; local n; for n in 1 2 3 4 5 6; do "$@" && return 0; echo "  (retry $what: attempt $n failed, backing off ${n}s)" >&2; sleep "$n"; done; echo "  ($what failed after 6 attempts)" >&2; return 1; }
m_src() { retry "src-q" sh -c "docker exec -i $SRC mysql -N -uroot -p$ROOTPW $DB -e \"$1\" 2>/dev/null"; }
m_dst() { retry "dst-q" sh -c "docker exec -i $DST mysql -N -uroot -p$ROOTPW $DB -e \"$1\" 2>/dev/null"; }

# checksum <container>: define bench_checksum TRANSIENTLY (create -> call -> drop)
# so it is never a persistent object a full copy would carry into the target.
checksum() {
  retry "checksum-$1" sh -c "
    docker exec -i $1 mysql --local-infile=1 -uroot -p$ROOTPW $DB < '$VERIFY' 2>/dev/null
    docker exec -i $1 mysql -N -uroot -p$ROOTPW $DB -e \"CALL bench_checksum(@r); SELECT @r;\" 2>/dev/null
    docker exec -i $1 mysql -uroot -p$ROOTPW $DB -e 'DROP PROCEDURE IF EXISTS bench_checksum' 2>/dev/null
  " | grep 'over tables'
}

label="${1:?label}"; shift
extra=("$@")
ver="$(docker run --rm "$IMG" --version 2>/dev/null | grep -oE 'v[0-9.]+' | head -1)"
stamp="$(echo "${ver}-${label}" | tr -c 'a-zA-Z0-9._-' '_')"
log="$LOGDIR/${stamp}.log"

echo "### sluice $ver / $label  (img=$IMG, flags: ${extra[*]:-none})"

# Reset target: drop the whole DB and recreate empty.
retry "target-reset" sh -c "docker exec -i $DST mysql -uroot -p$ROOTPW -e 'DROP DATABASE IF EXISTS $DB; CREATE DATABASE $DB' 2>/dev/null" \
  || { echo "FATAL: could not reset target"; exit 1; }
echo "target reset"

# Source size sanity (GiB data+index).
src_bytes="$(m_src "SELECT COALESCE(SUM(data_length+index_length),0) FROM information_schema.tables WHERE table_schema='$DB' AND table_name LIKE 'bench\\\\_%'")"
src_mb=$(awk -v b="${src_bytes:-0}" 'BEGIN{printf "%.0f", b/1048576}')
[ "${src_mb:-0}" -lt 1000 ] && { echo "FATAL: source looks empty ($src_mb MB)"; exit 1; }
src_tables="$(m_src "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$DB' AND table_name LIKE 'bench\\\\_%'")"

cname="benchrun-mysql-${stamp}"
docker rm -f "$cname" >/dev/null 2>&1 || true
retry "launch" docker run -d --name "$cname" --network "$NET" "$IMG" migrate \
  --source-driver=mysql --source="$SRC_DSN" \
  --target-driver=mysql --target="$DST_DSN" "${extra[@]}" >/dev/null

# Poll for exit (a CLI blip just retries the poll; the container keeps running).
rc=""
while :; do
  st="$(retry "poll" sh -c "docker inspect -f '{{.State.Status}}' $cname 2>/dev/null")"
  if [ "$st" = "exited" ]; then
    rc="$(docker inspect -f '{{.State.ExitCode}}' "$cname" 2>/dev/null)"
    break
  fi
  sleep 3
done

# Wall time from docker inspect timestamps (immune to control-plane blips).
started="$(docker inspect -f '{{.State.StartedAt}}' "$cname" 2>/dev/null)"
finished="$(docker inspect -f '{{.State.FinishedAt}}' "$cname" 2>/dev/null)"
to_epoch() { date -d "$1" +%s.%N 2>/dev/null || echo ""; }
ws="$(to_epoch "$started")"; we="$(to_epoch "$finished")"
wall="n/a"; [ -n "$ws" ] && [ -n "$we" ] && wall=$(awk -v a="$ws" -v b="$we" 'BEGIN{printf "%.1f", b-a}')

retry "logs" sh -c "docker logs '$cname' > '$log' 2>&1"
docker rm -f "$cname" >/dev/null 2>&1 || true

# Phase split from sluice's structured-log "phase complete" timestamps.
#   t0 = tables complete, t1 = bulk_copy complete, t2 = indexes complete.
# v0.99.29 (serial): idx = t2-t1 is the genuine separate post-copy index tail.
# v0.99.30 (overlap): bulk_copy & indexes "phase complete" are logged back-to-
#   back, so idx ~ 0 and bulk = t1-t0 already INCLUDES the overlapped builds.
#   The honest cross-binary comparison is therefore TOTAL WALL.
te() { date -d "$1" +%s.%N 2>/dev/null || echo ""; }
gt() { grep -oE "time=[^ ]+ level=[^ ]+ msg=\"migration: phase complete\" phase=$1" "$log" | head -1 | grep -oE 'time=[^ ]+' | cut -d= -f2; }
t0=$(te "$(gt tables)"); t1=$(te "$(gt bulk_copy)"); t2=$(te "$(gt indexes)")
bulk_s=""; idx_s=""
[ -n "$t0" ] && [ -n "$t1" ] && bulk_s=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.1f", b-a}')
[ -n "$t1" ] && [ -n "$t2" ] && idx_s=$(awk -v a="$t1" -v b="$t2" 'BEGIN{printf "%.1f", b-a}')

# Zero-loss content checksum. An EMPTY checksum means the verify
# harness itself failed (mysql exec error, dropped container, bad
# VERIFY path) — report that distinctly so it is never misread as a
# data-loss verdict in either direction (empty==empty must not pass,
# and harness failure must not masquerade as MISMATCH).
cs_src="$(checksum $SRC)"
cs_dst="$(checksum $DST)"
if [ -z "$cs_src" ] || [ -z "$cs_dst" ]; then
  zl="CHECKSUM-EMPTY (verify harness failed: src='$cs_src' dst='$cs_dst' — rerun; NOT a data-loss verdict)"
elif [ "$cs_src" = "$cs_dst" ]; then
  zl="ZERO-LOSS-OK"
else
  zl="MISMATCH"
fi

# Index-presence: every bench table on the target must carry its 4 secondary
# indexes (idx_user_id, idx_created_at, idx_event_type, idx_active_amt). Count
# distinct non-PRIMARY index names per table and assert == 4 for all.
idx_ok="INDEX-CHECK-FAIL"
bad="$(m_dst "
  SELECT COUNT(*) FROM (
    SELECT table_name, COUNT(DISTINCT index_name) AS n
    FROM information_schema.statistics
    WHERE table_schema='$DB' AND table_name LIKE 'bench\\\\_%' AND index_name <> 'PRIMARY'
    GROUP BY table_name HAVING n <> 4
  ) x")"
tcnt="$(m_dst "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='$DB' AND table_name LIKE 'bench\\\\_%'")"
[ "${bad:-1}" = "0" ] && [ "${tcnt:-0}" = "${src_tables:-X}" ] && idx_ok="ALL-INDEXES-OK (${tcnt} tables x 4)"

rate_basis="${bulk_s:-$wall}"
mbps=$(awk -v mb="$src_mb" -v s="$rate_basis" 'BEGIN{ if(s>0) printf "%.1f", mb/s; else print "n/a" }')

echo "----"
printf 'sluice=%s label=%s rc=%s\n' "$ver" "$label" "$rc"
printf 'src=%s MB / %s tables | wall=%ss | bulk_copy=%ss | index=%ss\n' "$src_mb" "$src_tables" "$wall" "${bulk_s:-?}" "${idx_s:-?}"
printf 'bulk-copy throughput: %s MB/s (data+index over %ss)\n' "$mbps" "$rate_basis"
printf 'zero-loss: %s\n  src=%s\n  dst=%s\n' "$zl" "$cs_src" "$cs_dst"
printf 'indexes:   %s\n' "$idx_ok"
echo "log: $log"
