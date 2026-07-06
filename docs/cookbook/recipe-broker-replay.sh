#!/usr/bin/env bash
# Executable pin for docs/cookbook/recipe-broker-replay.md
# (roadmap item 53: cookbook-as-executable-tests).
#
# Runs the recipe's producer/consumer backup-chain replication flow
# verbatim-in-intent against the released binary ($SLUICE) and asserts the
# documented outcomes:
#   - Step 1 "producer takes the full backup": `backup full` lands a chain root
#     (manifest + lineage) in the store
#   - Step 3 "consumer bulk-copies the full": `sluice restore` brings the target
#     to the full's state
#   - Step 4 "consumer starts the broker": `sync from-backup run --at-chain-id`
#     tails the chain and applies each incremental — the target converges
#     BYTE-EXACT with the source (md5 of the full row set matches)
#   - "Verification post-soak": a LIVE increment produced after the broker is
#     already running is picked up on the next poll and applied (byte-exact again)
#
# NOTE on Step 2 (same substitution as recipe-backup-encrypted.sh): the recipe
# documents `backup stream run` — a long-running rolling-incremental daemon. On
# an idle rig that blocks in WalSenderWaitForWAL draining the wall-clock window,
# so this pin uses the bounded one-shot `backup incremental --window=20s` to
# extend the chain deterministically. Same encrypted/plain chain-extend path the
# broker consumes; production operators run `backup stream run` under a
# supervisor as the recipe says. The broker itself (`sync from-backup run`) has
# NO `--window`; it is launched detached and polled with a bounded wait_until,
# never an unbounded foreground wait.
#
# EXCLUDED sub-shapes (documented in the recipe's "What this recipe doesn't
# cover"; out of the local-rig automated path): S3/GCS/Azure decoupled stores,
# encrypted chains (covered by recipe-backup-encrypted.sh), cross-engine broker,
# and multi-segment rotation following (needs a long producer soak).
#
# PG src -> PG dst; throwaway DBs (cookbook_brk_src / cookbook_brk_dst) + a temp
# chain dir, all created + dropped here. The producer's chain slot is dropped in
# cleanup so the standing rig is left with zero replication slots.
set -uo pipefail
cd "$(dirname "$0")"; . ./_cookbook-lib.sh
RECIPE_PAGE="docs/cookbook/recipe-broker-replay.md"
require_sluice

SRCDB="cookbook_brk_src"; DSTDB="cookbook_brk_dst"
SRC_DSN="$(pg_src_dsn "$SRCDB")"; DST_DSN="$(pg_dst_dsn "$DSTDB")"
BKDIR="${TMPDIR:-/tmp}/cookbook_brk_$$"; STREAM="cookbook_broker"
BROKER_LOG="${TMPDIR:-/tmp}/cookbook_brk_broker_$$.log"; BROKER_PID=""

src_md5(){ pg_src_sql "$SRCDB" "SELECT md5(string_agg(id||'|'||sku||'|'||qty, E'\n' ORDER BY id)) FROM item"; }
dst_md5(){ pg_dst_sql "$DSTDB" "SELECT md5(string_agg(id||'|'||sku||'|'||qty, E'\n' ORDER BY id)) FROM item"; }

cleanup(){
  [ -n "$BROKER_PID" ] && kill_pid "$BROKER_PID"
  # drop the producer's chain slot so the standing rig stays slot-clean
  pg_src_sql "$SRCDB" "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots;" >/dev/null 2>&1 || true
  pg_src_admin "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots;" >/dev/null 2>&1 || true
  pg_src_admin "DROP DATABASE IF EXISTS $SRCDB;" >/dev/null 2>&1 || true
  pg_dst_admin "DROP DATABASE IF EXISTS $DSTDB;" >/dev/null 2>&1 || true
  rm -rf "$BKDIR" 2>/dev/null || true
  rm -f "$BROKER_LOG" "$BROKER_LOG.out" "$BROKER_LOG.pid" 2>/dev/null || true
}
cleanup  # start clean
mkdir -p "$BKDIR"

echo "== recipe-broker-replay: backup chain -> sync from-backup broker replay =="

# ---- setup: a PG source with rows to replicate ------------------------------
pg_src_admin "CREATE DATABASE $SRCDB;" >/dev/null
pg_src_sql "$SRCDB" "CREATE TABLE item (id bigint PRIMARY KEY, sku text NOT NULL, qty int);" >/dev/null
pg_src_sql "$SRCDB" "INSERT INTO item SELECT g, 'sku-'||g, g*2 FROM generate_series(1,100) g;" >/dev/null
pg_dst_admin "CREATE DATABASE $DSTDB;" >/dev/null
SRC_CNT=$(pg_src_sql "$SRCDB" "SELECT count(*) FROM item")
echo "  seeded: item=$SRC_CNT"

# ---- Step 1: producer takes the full backup (chain root) --------------------
STEP="Step 1: producer takes the full backup"
"$SLUICE" backup full \
  --source-driver postgres --source "$SRC_DSN" \
  --output-dir "$BKDIR" --chain-slot
assert_rc0 "$?" "$STEP" "backup full --chain-slot exits 0"
assert_file_glob "$BKDIR" "lineage.json" "$STEP" "chain lineage written"
assert_file_glob "$BKDIR" "manifest.json" "$STEP" "full manifest written"
# the full's backup id — the broker's cold-start --at-chain-id (recipe Step 4).
FULLID=$(grep -oE '"segment_id": *"[a-f0-9]+"' "$BKDIR/lineage.json" | head -1 | grep -oE '[a-f0-9]{8,}')
[ -n "$FULLID" ] || _recipe_fail "$STEP" "could not read the full manifest's backup id from lineage.json"
pass_note "chain root backup id = $FULLID"

# ---- Step 3: consumer bulk-copies the full (restore) ------------------------
STEP="Step 3: consumer bulk-copies the full (restore)"
"$SLUICE" restore \
  --from-dir "$BKDIR" \
  --target-driver postgres --target "$DST_DSN"
assert_rc0 "$?" "$STEP" "sluice restore exits 0"
DST_CNT=$(pg_dst_sql "$DSTDB" "SELECT count(*) FROM item")
assert_eq "$DST_CNT" "$SRC_CNT" "$STEP" "target at the full's state after restore"

# ---- Step 4: consumer starts the broker (detached; --at-chain-id cold-start) -
STEP="Step 4: consumer starts the broker"
BROKER_PID=$(sluice_detached "$BROKER_LOG" --log-level=info sync from-backup run \
  --backup-dir "$BKDIR" \
  --target-driver postgres --target "$DST_DSN" \
  --stream-id "$STREAM" --poll-interval 5s --at-chain-id "$FULLID")
[ -n "$BROKER_PID" ] || _recipe_fail "$STEP" "could not launch the broker"
# broker should announce its cold-start assertion + start ticking. Presence
# poll (not an equality wait_until — the tick line recurs, so a count != 1).
_bi=0; while [ "$_bi" -lt 30 ]; do grep -qiE 'broker: started|broker tick' "$BROKER_LOG" && break; sleep 1; _bi=$((_bi+1)); done
if ! grep -qiE 'broker: started|broker tick' "$BROKER_LOG"; then
  echo "  --- broker log tail ---" >&2; tail -20 "$BROKER_LOG" 2>/dev/null >&2
  _recipe_fail "$STEP" "broker did not start / tick"
fi
pass_note "broker started (pid=$BROKER_PID), tailing the chain from $FULLID"

# ---- Step 2 (bounded producer; see NOTE): extend the chain, broker converges -
STEP="Broker converges byte-exact (applies incremental #1)"
pg_src_sql "$SRCDB" "UPDATE item SET qty=qty+1000 WHERE id<=10;" >/dev/null
pg_src_sql "$SRCDB" "INSERT INTO item SELECT g,'sku-'||g,g FROM generate_series(101,130) g;" >/dev/null
"$SLUICE" backup incremental \
  --source-driver postgres --source "$SRC_DSN" \
  --output-dir "$BKDIR" --window=20s
assert_rc0 "$?" "$STEP" "backup incremental (bounded --window) extends the chain"
WANT1=$(pg_src_sql "$SRCDB" "SELECT count(*) FROM item")
if ! wait_until "pg_dst_sql $DSTDB 'SELECT count(*) FROM item'" "$WANT1" 60; then
  echo "  --- broker log tail ---" >&2; tail -20 "$BROKER_LOG" 2>/dev/null >&2
  _recipe_fail "$STEP" "broker did not apply incremental #1 (target row count != $WANT1)"
fi
S1=$(src_md5); D1=$(dst_md5)
assert_eq "$D1" "$S1" "$STEP" "target byte-exact with source after broker replay (md5)"

# ---- Verification post-soak: a LIVE increment is picked up ------------------
STEP="Broker applies a live increment"
pg_src_sql "$SRCDB" "UPDATE item SET sku=sku||'-v2' WHERE id BETWEEN 20 AND 40;" >/dev/null
pg_src_sql "$SRCDB" "INSERT INTO item SELECT g,'live-'||g,g FROM generate_series(131,150) g;" >/dev/null
"$SLUICE" backup incremental \
  --source-driver postgres --source "$SRC_DSN" \
  --output-dir "$BKDIR" --window=20s
assert_rc0 "$?" "$STEP" "second backup incremental extends the chain"
WANT2=$(pg_src_sql "$SRCDB" "SELECT count(*) FROM item")
if ! wait_until "pg_dst_sql $DSTDB 'SELECT count(*) FROM item'" "$WANT2" 60; then
  echo "  --- broker log tail ---" >&2; tail -20 "$BROKER_LOG" 2>/dev/null >&2
  _recipe_fail "$STEP" "broker did not apply the live increment (target row count != $WANT2)"
fi
S2=$(src_md5); D2=$(dst_md5)
assert_eq "$D2" "$S2" "$STEP" "target byte-exact after the live increment (md5)"

# ---- stop the broker cleanly ------------------------------------------------
kill_pid "$BROKER_PID"; BROKER_PID=""

echo ""
echo "RECIPE PASS: recipe-broker-replay.md"
cleanup
