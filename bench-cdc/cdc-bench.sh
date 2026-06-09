#!/usr/bin/env bash
# cdc-bench.sh — validate the CONTINUOUS-SYNC path end-to-end under concurrent
# writes, at a good-sized (lower-than-110GB) scale.
#
# Flow:
#   1. reset target.
#   2. launch `sluice sync start` PG->PG (detached; cold-copies the seed via the
#      ADR-0079 fast cold-start, then follows CDC).
#   3. start the concurrent writer (writer.sh) IMMEDIATELY — so INSERT/UPDATE/
#      DELETE land DURING the cold-copy (exercising the snapshot/CDC boundary)
#      and continue through CDC steady-state.
#   4. stop the writer; let CDC drain.
#   5. compare cdc_checksum() on source vs target — must converge to EQUAL
#      (zero-loss: every write delivered exactly once). Report timings.
#
# Robustness mirrors bench-pgcopydb/bench.sh: every docker call retried (the
# Rancher Hyper-V socket flaps), the sync runs detached so a control-plane blip
# can't abort the data plane, and timing comes from container/log facts.
#
# Usage: cdc-bench.sh [writer_secs] [drain_timeout_secs]   (defaults 90 / 300)
# Env: SLUICE_IMG (default sluice-bench:main — BUILD IT FROM CURRENT main FIRST).
set -uo pipefail
export MSYS_NO_PATHCONV=1

NET=benchnet
SRC_URI="postgres://postgres:bench@bench-cdc-src:5432/benchdb?sslmode=disable"
DST_URI="postgres://postgres:bench@bench-cdc-dst:5432/benchdb?sslmode=disable"
IMG="${SLUICE_IMG:-sluice-bench:main}"
STREAM="cdcbench"
WRITER_SECS="${1:-90}"; DRAIN_TIMEOUT="${2:-300}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGDIR="$HERE/results"; mkdir -p "$LOGDIR"
VERIFY="$HERE/verify_cdc.sql"
NT="$(docker exec bench-cdc-src psql -U postgres -d benchdb -tAc "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='r' AND n.nspname='public' AND c.relname LIKE 'cdc\\_%'" | tr -d '[:space:]')"
SEED="$(docker exec bench-cdc-src psql -U postgres -d benchdb -tAc "SELECT coalesce(max(id),0) FROM cdc_00" | tr -d '[:space:]')"

retry() { local what="$1"; shift; local n; for n in 1 2 3 4 5 6; do "$@" && return 0; echo "  (retry $what #$n)" >&2; sleep "$n"; done; echo "  ($what failed x6)" >&2; return 1; }
# cksum <container>: define cdc_checksum transiently, run it, drop it. The
# function is never persisted so CDC never tries to replicate it.
cksum() { { cat "$VERIFY"; printf 'SELECT cdc_checksum();\nDROP FUNCTION IF EXISTS cdc_checksum();\n'; } | docker exec -i "$1" psql -U postgres -d benchdb -tA 2>/dev/null | grep 'over tables'; }

echo "### cdc-bench  img=$IMG  tables=$NT  seed_max_id=$SEED  writer=${WRITER_SECS}s"

retry "target-reset" docker exec bench-cdc-dst psql -U postgres -d benchdb -q -c \
  "DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO postgres;" >/dev/null \
  || { echo "FATAL: target reset"; exit 1; }

# Source slot hygiene: a prior run's cold-start left `sluice_slot` on the
# SOURCE (slots are server-side — removing the sync container does NOT drop
# them), and sluice correctly REFUSES to start over an existing slot
# (ADR-0022). Terminate any walsender still holding it, then drop it, so each
# run cold-starts fresh. (Resetting only the target schema is not enough.)
retry "slot-cleanup" docker exec bench-cdc-src psql -U postgres -d benchdb -q -c \
  "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name='sluice_slot' AND active_pid IS NOT NULL;" >/dev/null 2>&1 || true
sleep 1
retry "slot-drop" docker exec bench-cdc-src psql -U postgres -d benchdb -q -c \
  "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='sluice_slot';" >/dev/null 2>&1 || true

cname="cdcrun-${STREAM}"
docker rm -f "$cname" >/dev/null 2>&1 || true
t_start=$(date +%s)
retry "launch-sync" docker run -d --name "$cname" --network "$NET" "$IMG" sync start \
  --source-driver=postgres --source="$SRC_URI" \
  --target-driver=postgres --target="$DST_URI" --stream-id="$STREAM" >/dev/null \
  || { echo "FATAL: launch sync"; exit 1; }
echo "sync launched (detached)"

# Start the writer IMMEDIATELY so writes land during the cold-copy.
bash "$HERE/writer.sh" bench-cdc-src "$NT" "$SEED" "$WRITER_SECS" > "$LOGDIR/writer.log" 2>&1 &
writer_pid=$!
echo "writer started (pid $writer_pid) — mutating source during cold-copy + CDC"

# Wait for cold-start to finish (the streamer logs this once per stream).
# Exit-detection is hardened against the Rancher Hyper-V-socket blip: only a
# SUCCESSFUL `docker inspect` reporting exited/dead is treated as a real exit;
# a failed/empty inspect (control-plane blip) just retries next iteration. A
# bare `docker ps | grep` here false-positived "exited" on a healthy,
# still-copying container.
cold_done=""
for _ in $(seq 1 600); do
  if docker logs "$cname" 2>&1 | grep -q "entering CDC mode"; then cold_done=1; break; fi
  st="$(docker inspect -f '{{.State.Status}}' "$cname" 2>/dev/null)"
  if [ "$st" = "exited" ] || [ "$st" = "dead" ]; then
    echo "FATAL: sync container exited during cold-start (status=$st)"; docker logs "$cname" 2>&1 | tail -30; exit 1
  fi
  sleep 2
done
t_cold=$(date +%s)
[ -n "$cold_done" ] && echo "cold-start complete in $((t_cold - t_start))s (writes were in flight)" || echo "WARN: cold-start marker not seen"

wait "$writer_pid" 2>/dev/null; echo "$(cat "$LOGDIR/writer.log" | tail -1)"
t_writer_done=$(date +%s)

# Drain: poll until src checksum == dst checksum, stable for 2 reads.
echo "draining CDC (timeout ${DRAIN_TIMEOUT}s) ..."
matches=0; zl="TIMEOUT"; drain_start=$(date +%s)
while [ $(( $(date +%s) - drain_start )) -lt "$DRAIN_TIMEOUT" ]; do
  cs_src="$(cksum bench-cdc-src)"; cs_dst="$(cksum bench-cdc-dst)"
  if [ -n "$cs_src" ] && [ "$cs_src" = "$cs_dst" ]; then
    matches=$((matches+1)); [ "$matches" -ge 2 ] && { zl="ZERO-LOSS-OK"; break; }
  else
    matches=0
  fi
  sleep 5
done
t_drained=$(date +%s)

retry "logs" sh -c "docker logs '$cname' > '$LOGDIR/sync.log' 2>&1"
docker rm -f "$cname" >/dev/null 2>&1 || true

echo "----"
printf 'cold_start=%ss | writer=%ss | cdc_drain_after_writer=%ss\n' \
  "$((t_cold - t_start))" "$((t_writer_done - t_cold))" "$((t_drained - t_writer_done))"
printf 'fast-cold-start engaged: %s\n' "$(grep -c 'fast parallel copy engaged' "$LOGDIR/sync.log")"
printf 'continuous-sync zero-loss: %s\n  src=%s\n  dst=%s\n' "$zl" "$cs_src" "$cs_dst"
echo "logs: $LOGDIR/sync.log  $LOGDIR/writer.log"
[ "$zl" = "ZERO-LOSS-OK" ] || exit 1
