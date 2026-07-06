#!/usr/bin/env bash
# Executable pin for docs/cookbook/recipe-bidirectional-cutover.md
# (roadmap item 53: cookbook-as-executable-tests).
#
# Runs the recipe's low-downtime-cutover flow verbatim-in-intent against the
# released binary ($SLUICE) and asserts the documented outcomes:
#   - Step 1 "start the sync stream": `sluice sync start` is self-contained —
#     it cold-starts (captures the CDC anchor, bulk-snapshots every table while
#     the source stays writable), reaching row parity on the target with NO
#     separate `migrate` step. Then a live source INSERT propagates via CDC.
#   - Step 3 "prime the target's sequences": `sluice cutover --sequence-margin`
#     advances the target sequence past the source's current MAX(id) + margin,
#     so post-cutover target inserts can't collide (the load-bearing step).
#   - Step 5 "drain and stop": `sluice sync stop --wait` drains + exits cleanly.
#
# ---- WHY THIS RECIPE WAS REWRITTEN (the doc bug this pin now guards) ----------
# An earlier revision of recipe-bidirectional-cutover.md documented a WRONG
# two-step zero-downtime model: Step 1 `sluice migrate --migration-id X` (bulk
# copy), Step 2 `sluice sync start --stream-id X --resume` claiming it "resumes
# from the bulk-copy's end-position rather than re-snapshotting." Both halves
# were false against the released binary:
#   (1) `sync start` has NO `--resume` flag — kong rejects it (exit 80).
#   (2) `migrate` writes `sluice_migrate_state`, NOT a `sluice_cdc_state` row,
#       so a subsequent `sync start` on the now-populated target refuses to
#       cold-start (SLUICE-E-COLDSTART-TARGET-NOT-EMPTY). There is no
#       migrate->resume warm handoff.
# The ACTUAL gapless snapshot->CDC handoff (docs/snapshot-cdc-handoff.md) is
# `sync start`'s OWN integrated cold-start: it stamps sluice_cdc_state in phase 1
# BEFORE the bulk reader, snapshots from that anchor while the source stays
# writable, then resumes CDC from the exact anchor — a SINGLE command, no
# migrate, no --resume. The recipe now documents that; this sidecar pins it.
#
# CDC-wait discipline (idle-rig WalSenderWaitForWAL hang): `sync start` is a
# long-running daemon with no `--window`; this pin launches it detached and
# polls with a bounded wait_until, exactly like quickstart.sh — never an
# unbounded foreground CDC wait.
#
# MySQL src -> PG dst; throwaway DBs (cookbook_cut_src / cookbook_cut_dst).
set -uo pipefail
cd "$(dirname "$0")"; . ./_cookbook-lib.sh
RECIPE_PAGE="docs/cookbook/recipe-bidirectional-cutover.md"
require_sluice

SRCDB="cookbook_cut_src"; DSTDB="cookbook_cut_dst"
SRC_DSN="$(my_src_dsn "$SRCDB")"; DST_DSN="$(pg_dst_dsn "$DSTDB")"
STREAM="cookbook_cutover"; SYNC_LOG="${TMPDIR:-/tmp}/cookbook_cut_sync_$$.log"; SYNC_PID=""

cleanup(){
  [ -n "$SYNC_PID" ] && kill_pid "$SYNC_PID"
  # MySQL source has no replication slot; PG target isn't a CDC source here.
  my_src_admin "DROP DATABASE IF EXISTS $SRCDB;" >/dev/null 2>&1 || true
  pg_dst_admin "DROP DATABASE IF EXISTS $DSTDB;" >/dev/null 2>&1 || true
  rm -f "$SYNC_LOG" "$SYNC_LOG.out" "$SYNC_LOG.pid" 2>/dev/null || true
}
cleanup  # start clean

echo "== recipe-bidirectional-cutover: sync start -> CDC -> cutover -> drain =="

# ---- setup: MySQL source with an AUTO_INCREMENT identity column --------------
my_src_admin "CREATE DATABASE $SRCDB;" >/dev/null
my_src_sql "$SRCDB" "CREATE TABLE account (
  id INT AUTO_INCREMENT PRIMARY KEY, owner VARCHAR(120) NOT NULL, balance DECIMAL(12,2)
);"
my_src_sql "$SRCDB" "INSERT INTO account (owner,balance)
  SELECT CONCAT('owner',n), n*10.0
  FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<120) SELECT n FROM s) t;"
# Empty target: sync start's cold-start requires an empty target (that is the
# whole point — there is NO migrate step first).
pg_dst_admin "CREATE DATABASE $DSTDB;" >/dev/null
SRC_CNT=$(my_src_sql "$SRCDB" "SELECT count(*) FROM account")
echo "  seeded: account=$SRC_CNT"

# ---- Step 1: start the sync stream (self-contained cold-start -> CDC) --------
#      No migrate, no --resume. sync start owns the bulk snapshot AND the CDC
#      handoff in one process; the source stays writable throughout.
STEP="Step 1: start the sync stream (cold-start handoff)"
SYNC_PID=$(sluice_detached "$SYNC_LOG" --log-level=info sync start \
  --source-driver mysql --source "$SRC_DSN" \
  --target-driver postgres --target "$DST_DSN" \
  --stream-id "$STREAM")
[ -n "$SYNC_PID" ] || _recipe_fail "$STEP" "could not launch sync start"
echo "  sync started (pid=$SYNC_PID); waiting for cold-start bulk copy..."
if ! wait_until "pg_dst_sql $DSTDB 'SELECT count(*) FROM account'" "$SRC_CNT" 90; then
  echo "  --- sync log tail ---" >&2; tail -20 "$SYNC_LOG" 2>/dev/null >&2
  _recipe_fail "$STEP" "cold-start bulk copy did not reach $SRC_CNT rows on target"
fi
pass_note "cold-start handoff complete ($SRC_CNT rows), now in CDC mode"

# CDC propagation: insert on the (still-writable) source, expect it on target.
STEP="Step 1/2: CDC propagation"
my_src_sql "$SRCDB" "INSERT INTO account (owner,balance) VALUES ('cutover-probe', 999.99)"
if ! wait_until "pg_dst_sql $DSTDB \"SELECT count(*) FROM account WHERE owner='cutover-probe'\"" "1" 60; then
  echo "  --- sync log tail ---" >&2; tail -20 "$SYNC_LOG" 2>/dev/null >&2
  _recipe_fail "$STEP" "CDC did not propagate the source INSERT to the target"
fi
pass_note "CDC propagated the live source INSERT to the target"
DST_CNT=$(pg_dst_sql "$DSTDB" "SELECT count(*) FROM account")
assert_eq "$DST_CNT" "$((SRC_CNT + 1))" "$STEP" "row parity after cold-start + CDC propagation"

# ---- Step 3: prime the target's sequences (the load-bearing step) -----------
STEP="Step 3: prime the target's sequences (cutover)"
SRC_MAX=$(my_src_sql "$SRCDB" "SELECT MAX(id) FROM account")
CUT=$("$SLUICE" cutover \
  --target-driver postgres --target "$DST_DSN" \
  --source-driver mysql --source "$SRC_DSN" \
  --sequence-margin=1000 2>&1)
assert_rc0 "$?" "$STEP" "sluice cutover exits 0"
assert_contains "$CUT" "primed" "$STEP" "cutover reports a primed sequence"
# documented outcome: the target's next id is now > source MAX(id) (+margin),
# so a post-cutover insert can't collide with an in-flight CDC row.
NEXTVAL=$(pg_dst_sql "$DSTDB" "SELECT nextval(pg_get_serial_sequence('public.account','id'))")
if [ -z "$NEXTVAL" ] || [ "$NEXTVAL" -le "$SRC_MAX" ]; then
  _recipe_fail "$STEP" "sequence not primed past source MAX(id): nextval=$NEXTVAL, source_max=$SRC_MAX"
fi
pass_note "sequence primed past source high-water mark (nextval=$NEXTVAL > source MAX=$SRC_MAX, margin 1000)"

# ---- Step 5: drain and stop the CDC stream ----------------------------------
STEP="Step 5: drain and stop (sync stop --wait)"
"$SLUICE" sync stop \
  --target-driver postgres --target "$DST_DSN" \
  --stream-id "$STREAM" --wait --timeout=30s
assert_rc0 "$?" "$STEP" "sync stop --wait drains and exits cleanly"
kill_pid "$SYNC_PID"; SYNC_PID=""

echo ""
echo "RECIPE PASS: recipe-bidirectional-cutover.md"
cleanup
