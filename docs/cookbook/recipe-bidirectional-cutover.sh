#!/usr/bin/env bash
# Executable pin for docs/cookbook/recipe-bidirectional-cutover.md
# (roadmap item 53: cookbook-as-executable-tests).
#
# Runs the recipe's low-downtime-cutover flow verbatim-in-intent against the
# released binary ($SLUICE) and asserts the documented outcomes:
#   - Step 1 "bulk copy": `sluice migrate --migration-id` lands every row
#     (row parity after migrate)
#   - Step 2 "start the CDC stream": a live sync propagates a source INSERT to
#     the target (CDC propagation)
#   - Step 4 "prime the target's sequences": `sluice cutover --sequence-margin`
#     advances the target sequence past the source's current MAX(id) + margin,
#     so post-cutover target inserts can't collide (the load-bearing step)
#   - Step 6 "drain and stop": `sluice sync stop --wait` drains + exits cleanly
#
# ---- TWO REAL DOC BUGS this pin surfaces (flagged, NOT worked around) --------
# recipe-bidirectional-cutover.md "Step 2: start the CDC stream" documents:
#       sluice sync start ... --stream-id myapp-prod --resume
#   with the claim: "sluice resumes from the bulk-copy's end-position rather
#    than re-snapshotting. `--resume` is the explicit acknowledgement ...".
# Against the released binary BOTH halves of that are wrong:
#   (BUG 1) `sync start` has NO `--resume` flag — it errors "unknown flag
#           --resume". (The flag exists on `migrate`, not on `sync start`.)
#   (BUG 2) A standalone `sluice migrate` writes `sluice_migrate_state`, NOT a
#           `sluice_cdc_state` row — so a subsequent `sync start` (matching
#           stream-id) canNOT warm-resume from the migrate; it refuses loudly
#           with SLUICE-E-COLDSTART-TARGET-NOT-EMPTY ("target table already
#           contains data — no cdc-state row exists for this stream, so
#           warm-resume isn't possible either"). The ACTUAL gapless snapshot->CDC
#           handoff (docs/snapshot-cdc-handoff.md) is `sync start`'s OWN
#           integrated cold-start (it stamps sluice_cdc_state in phase 1 BEFORE
#           the bulk reader), a single command — not a two-command
#           migrate-then-resume sequence.
# This sidecar PINS both real refusals as loud-failure assertions, then drives
# the ACTUAL working CDC path (`sync start` integrated cold-start) for the
# propagation + cutover + drain assertions. Suggested docs fix: rewrite Step 2
# to a single `sync start` (no --resume; the cold-start IS the handoff), or make
# Step 1 the `sync start` cold-start and drop the separate migrate.
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

echo "== recipe-bidirectional-cutover: migrate -> CDC -> cutover -> drain =="

# ---- setup: MySQL source with an AUTO_INCREMENT identity column --------------
my_src_admin "CREATE DATABASE $SRCDB;" >/dev/null
my_src_sql "$SRCDB" "CREATE TABLE account (
  id INT AUTO_INCREMENT PRIMARY KEY, owner VARCHAR(120) NOT NULL, balance DECIMAL(12,2)
);"
my_src_sql "$SRCDB" "INSERT INTO account (owner,balance)
  SELECT CONCAT('owner',n), n*10.0
  FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<120) SELECT n FROM s) t;"
pg_dst_admin "CREATE DATABASE $DSTDB;" >/dev/null
SRC_CNT=$(my_src_sql "$SRCDB" "SELECT count(*) FROM account")
echo "  seeded: account=$SRC_CNT"

# ---- Step 1: bulk copy (migrate) --> row parity -----------------------------
STEP="Step 1: bulk copy (migrate)"
"$SLUICE" migrate \
  --source-driver mysql --source "$SRC_DSN" \
  --target-driver postgres --target "$DST_DSN" \
  --migration-id "$STREAM"
assert_rc0 "$?" "$STEP" "sluice migrate --migration-id exits 0"
DST_CNT=$(pg_dst_sql "$DSTDB" "SELECT count(*) FROM account")
assert_eq "$DST_CNT" "$SRC_CNT" "$STEP" "row parity after migrate"

# ---- Step 2 doc-bug pins (see header): the documented resume path refuses ----
STEP="Step 2 DOC BUG 1: sync start --resume is an unknown flag"
"$SLUICE" sync start \
  --source-driver mysql --source "$SRC_DSN" \
  --target-driver postgres --target "$DST_DSN" \
  --stream-id "$STREAM" --resume >/dev/null 2>&1
assert_rc_nonzero "$?" "$STEP" "sync start rejects the documented --resume flag"

STEP="Step 2 DOC BUG 2: migrate leaves no cdc-state; sync start can't warm-resume"
# no --resume this time; it still refuses because the migrated target is
# populated and has no sluice_cdc_state row to resume from.
"$SLUICE" sync start \
  --source-driver mysql --source "$SRC_DSN" \
  --target-driver postgres --target "$DST_DSN" \
  --stream-id "$STREAM" >/dev/null 2>&1
assert_rc_nonzero "$?" "$STEP" "sync start refuses to resume from a plain migrate (cold-start-not-empty)"

# ---- Step 2 (real path): the actual gapless handoff is sync start's own -----
#      integrated cold-start. Reset the target and let sync start do it.
STEP="Step 2: start the CDC stream (real cold-start handoff)"
pg_dst_sql "$DSTDB" "DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO $PG_USER;" >/dev/null
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

# CDC propagation: insert on source, expect it on target within the CDC lag.
STEP="Step 2/3: CDC propagation"
my_src_sql "$SRCDB" "INSERT INTO account (owner,balance) VALUES ('cutover-probe', 999.99)"
if ! wait_until "pg_dst_sql $DSTDB \"SELECT count(*) FROM account WHERE owner='cutover-probe'\"" "1" 60; then
  echo "  --- sync log tail ---" >&2; tail -20 "$SYNC_LOG" 2>/dev/null >&2
  _recipe_fail "$STEP" "CDC did not propagate the source INSERT to the target"
fi
pass_note "CDC propagated the live source INSERT to the target"

# ---- Step 4: prime the target's sequences (the load-bearing step) -----------
STEP="Step 4: prime the target's sequences (cutover)"
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

# ---- Step 6: drain and stop the CDC stream ----------------------------------
STEP="Step 6: drain and stop (sync stop --wait)"
"$SLUICE" sync stop \
  --target-driver postgres --target "$DST_DSN" \
  --stream-id "$STREAM" --wait --timeout=30s
assert_rc0 "$?" "$STEP" "sync stop --wait drains and exits cleanly"
kill_pid "$SYNC_PID"; SYNC_PID=""

echo ""
echo "RECIPE PASS: recipe-bidirectional-cutover.md"
cleanup
