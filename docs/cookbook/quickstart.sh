#!/usr/bin/env bash
# Executable pin for docs/examples/quickstart.md (the getting-started flow;
# roadmap item 53: cookbook-as-executable-tests).
#
# The published quickstart drives a sakila-loaded MySQL container via its own
# docker-compose. This pin runs the SAME flow verbatim-in-intent against the
# standing rig with a self-contained seed (no 30 MB sakila download), asserting
# the documented outcomes of each step:
#   - §4b "Dry-run first": `migrate --dry-run` prints "DRY RUN" and writes nothing
#   - §4c "Apply the migration": `sluice migrate` lands every row
#   - §4d "Spot-check the result": the identity sequence advanced past the max
#   - §5  "Continuous sync": `sync start` cold-starts, then a source INSERT
#         propagates to the target
#   - §5a "Stop and resume": a restarted `sync start` warm-resumes (no re-snapshot)
#
# Uses throwaway DBs (cookbook_qs_src / cookbook_qs_dst) it creates + drops.
set -uo pipefail
cd "$(dirname "$0")"; . ./_cookbook-lib.sh
RECIPE_PAGE="docs/examples/quickstart.md"
require_sluice

SRCDB="cookbook_qs_src"; DSTDB="cookbook_qs_dst"
SRC_DSN="$(my_src_dsn "$SRCDB")"; DST_DSN="$(pg_dst_dsn "$DSTDB")"
STREAM="cookbook_quickstart"; SYNC_LOG="${TMPDIR:-/tmp}/cookbook_qs_sync_$$.log"; SYNC_PID=""

cleanup(){
  [ -n "$SYNC_PID" ] && kill_pid "$SYNC_PID"
  my_src_admin "DROP DATABASE IF EXISTS $SRCDB;" >/dev/null 2>&1 || true
  pg_dst_admin "DROP DATABASE IF EXISTS $DSTDB;" >/dev/null 2>&1 || true
  rm -f "$SYNC_LOG" "$SYNC_LOG.out" "$SYNC_LOG.pid" "${SYNC_LOG%.log}_resume.log" "${SYNC_LOG%.log}_resume.log.out" "${SYNC_LOG%.log}_resume.log.pid" 2>/dev/null || true
}
cleanup  # start clean

echo "== quickstart: MySQL -> Postgres migrate + continuous sync =="

# ---- setup: a small MySQL source (stands in for sakila) ---------------------
my_src_admin "CREATE DATABASE $SRCDB;" >/dev/null
my_src_sql "$SRCDB" "CREATE TABLE actor (
  id INT AUTO_INCREMENT PRIMARY KEY,
  first_name VARCHAR(100), last_name VARCHAR(100),
  KEY idx_last (last_name)
);"
my_src_sql "$SRCDB" "INSERT INTO actor (first_name,last_name)
  SELECT CONCAT('First',n), CONCAT('Last',n)
  FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<300) SELECT n FROM s) t;"
pg_dst_admin "DROP DATABASE IF EXISTS $DSTDB;" >/dev/null 2>&1 || true
pg_dst_admin "CREATE DATABASE $DSTDB;" >/dev/null
SRC_CNT=$(my_src_sql "$SRCDB" "SELECT count(*) FROM actor")
echo "  seeded: actor=$SRC_CNT"

# ---- §4b "Dry-run first" -----------------------------------------------------
STEP="§4b Dry-run first"
DRY=$("$SLUICE" migrate \
  --source-driver mysql --source "$SRC_DSN" \
  --target-driver postgres --target "$DST_DSN" \
  --dry-run 2>&1)
assert_rc0 "$?" "$STEP" "migrate --dry-run runs"
# NOTE: quickstart.md §4b's sample shows a "DRY RUN — would migrate ..." banner,
# but the released binary emits structured slog ("dry run: migration plan ...
# tables=N", "dry run: table name=actor ..."). The documented *behavior* (reads
# schema, names each table, writes nothing) is what we pin here; the banner text
# in the doc is stale sample output (flagged for a docs fix, not silently matched).
assert_contains "$DRY" "dry run" "$STEP" "dry-run prints a migration plan"
assert_contains "$DRY" "actor" "$STEP" "dry-run names the source table"
NTAB=$(pg_dst_sql "$DSTDB" "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'")
assert_eq "$NTAB" "0" "$STEP" "dry-run wrote nothing to target"

# ---- §4c "Apply the migration" ----------------------------------------------
STEP="§4c Apply the migration"
"$SLUICE" migrate \
  --source-driver mysql --source "$SRC_DSN" \
  --target-driver postgres --target "$DST_DSN"
assert_rc0 "$?" "$STEP" "sluice migrate exits 0"
DST_CNT=$(pg_dst_sql "$DSTDB" "SELECT count(*) FROM actor")
assert_eq "$DST_CNT" "$SRC_CNT" "$STEP" "actor row parity"

# ---- §4d "Spot-check the result" (sequence advanced past copied max) ---------
STEP="§4d Spot-check the result"
NEXTVAL=$(pg_dst_sql "$DSTDB" "SELECT nextval(pg_get_serial_sequence('public.actor','id'))")
if [ -z "$NEXTVAL" ] || [ "$NEXTVAL" -le "$SRC_CNT" ]; then
  _recipe_fail "$STEP" "sequence not advanced past copied max: nextval=$NEXTVAL, copied=$SRC_CNT"
fi
pass_note "sequence advanced (nextval=$NEXTVAL > copied max=$SRC_CNT)"

# ---- §5 "Continuous sync": reset target, cold-start, propagate an insert -----
STEP="§5 Continuous sync"
# recipe resets the target so the snapshot+CDC handoff is demonstrated cleanly
pg_dst_sql "$DSTDB" "DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO $PG_USER;" >/dev/null
SYNC_PID=$(sluice_detached "$SYNC_LOG" --log-level=info sync start \
  --source-driver mysql --source "$SRC_DSN" \
  --target-driver postgres --target "$DST_DSN" \
  --stream-id "$STREAM")
[ -n "$SYNC_PID" ] || _recipe_fail "$STEP" "could not launch sync start"
echo "  sync started (pid=$SYNC_PID); waiting for cold-start bulk copy..."
if ! wait_until "pg_dst_sql $DSTDB 'SELECT count(*) FROM actor'" "$SRC_CNT" 90; then
  echo "  --- sync log tail ---" >&2; tail -20 "$SYNC_LOG" 2>/dev/null >&2
  _recipe_fail "$STEP" "cold-start bulk copy did not reach $SRC_CNT rows on target"
fi
pass_note "cold-start bulk copy complete ($SRC_CNT rows), CDC mode"

# insert on source, expect it on target within the CDC lag
my_src_sql "$SRCDB" "INSERT INTO actor (first_name,last_name) VALUES ('Test','Actor')"
WANT=$((SRC_CNT + 1))
if ! wait_until "pg_dst_sql $DSTDB \"SELECT count(*) FROM actor WHERE last_name='Actor'\"" "1" 60; then
  echo "  --- sync log tail ---" >&2; tail -20 "$SYNC_LOG" 2>/dev/null >&2
  _recipe_fail "$STEP" "CDC did not propagate the source INSERT to the target"
fi
pass_note "CDC propagated the live source INSERT to the target"

# ---- §5a "Stop and resume" (warm resume, no re-snapshot) --------------------
STEP="§5a Stop and resume"
kill_pid "$SYNC_PID"; SYNC_PID=""; sleep 3
# Use a FRESH log file for the restart: the just-killed process can briefly
# retain the Windows file handle to the old log, so redirecting the restart's
# stderr to the same path can silently drop it (the grep below would then never
# see the resume line). A distinct file is race-free.
SYNC_LOG2="${SYNC_LOG%.log}_resume.log"; rm -f "$SYNC_LOG2" "$SYNC_LOG2.out" "$SYNC_LOG2.pid" 2>/dev/null
SYNC_PID=$(sluice_detached "$SYNC_LOG2" --log-level=info sync start \
  --source-driver mysql --source "$SRC_DSN" \
  --target-driver postgres --target "$DST_DSN" \
  --stream-id "$STREAM")
[ -n "$SYNC_PID" ] || _recipe_fail "$STEP" "could not relaunch sync start"
# documented outcome: warm resume from persisted position (not another cold start)
if ! wait_until "grep -ciE 'warm resume|resume from persisted|resuming' '$SYNC_LOG2'" "1" 45 \
   && ! grep -qiE 'warm resume|resume from persisted|resuming|persisted position' "$SYNC_LOG2"; then
  echo "  --- resume log tail ---" >&2; tail -25 "$SYNC_LOG2" 2>/dev/null >&2
  _recipe_fail "$STEP" "restart did not log a warm resume (looks like a re-snapshot)"
fi
# and it must NOT re-cold-start
if grep -qiE 'cold start|snapshot captured' "$SYNC_LOG2"; then
  echo "  --- resume log tail ---" >&2; tail -25 "$SYNC_LOG2" 2>/dev/null >&2
  _recipe_fail "$STEP" "restart cold-started again instead of warm-resuming"
fi
pass_note "restart warm-resumed from persisted position (no re-snapshot)"

echo ""
echo "RECIPE PASS: quickstart.md"
cleanup
