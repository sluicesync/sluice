#!/usr/bin/env bash
# Executable pin for docs/cookbook/recipe-migrate-once.md
# (roadmap item 53: cookbook-as-executable-tests).
#
# Runs the recipe's documented commands verbatim-in-intent against the
# released binary ($SLUICE) and asserts the documented outcomes:
#   - `schema preview` emits target DDL (recipe "Preview before you run")
#   - `sluice migrate` runs the 5 phases and lands every row (recipe "The command")
#   - identity_sync advances the target sequence past the copied max, so
#     future inserts don't collide (recipe "The command", phase 3)
#   - `sluice verify --depth=count` passes (recipe "What's NOT in this recipe")
#
# Cloud/failure-only steps NOT in the automated path:
#   - `sluice migrate --resume` (needs a forced mid-copy failure; operator path)
#
# Uses throwaway DBs (cookbook_mig_src / cookbook_mig_dst) it creates + drops.
set -uo pipefail
cd "$(dirname "$0")"; . ./_cookbook-lib.sh
RECIPE_PAGE="docs/cookbook/recipe-migrate-once.md"
require_sluice

SRCDB="cookbook_mig_src"; DSTDB="cookbook_mig_dst"
SRC_DSN="$(my_src_dsn "$SRCDB")"; DST_DSN="$(pg_dst_dsn "$DSTDB")"

cleanup(){
  my_src_admin "DROP DATABASE IF EXISTS $SRCDB;" >/dev/null 2>&1 || true
  pg_dst_admin "DROP DATABASE IF EXISTS $DSTDB;" >/dev/null 2>&1 || true
}
cleanup  # start clean

echo "== recipe-migrate-once: MySQL -> Postgres one-shot =="

# ---- setup: MySQL source with AUTO_INCREMENT PK, secondary index, + an FK
#      child table so all five documented phases (tables/bulk_copy/identity_sync/
#      indexes/constraints) actually get exercised. -----------------------------
my_src_admin "CREATE DATABASE $SRCDB;" >/dev/null
my_src_sql "$SRCDB" "CREATE TABLE customer (
  id INT AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(200) NOT NULL,
  name VARCHAR(200),
  balance DECIMAL(12,2),
  UNIQUE KEY uq_email (email),
  KEY idx_name (name)
);"
my_src_sql "$SRCDB" "CREATE TABLE orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT NOT NULL,
  total DECIMAL(12,2),
  KEY idx_cust (customer_id),
  CONSTRAINT fk_cust FOREIGN KEY (customer_id) REFERENCES customer(id)
);"
my_src_sql "$SRCDB" "INSERT INTO customer (email,name,balance)
  SELECT CONCAT('user',n,'@example.com'), CONCAT('Name ',n), n*1.25
  FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<200) SELECT n FROM s) t;"
my_src_sql "$SRCDB" "INSERT INTO orders (customer_id,total)
  SELECT (n%200)+1, n*2.5
  FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM s WHERE n<500) SELECT n FROM s) t;"
pg_dst_admin "DROP DATABASE IF EXISTS $DSTDB;" >/dev/null 2>&1 || true
pg_dst_admin "CREATE DATABASE $DSTDB;" >/dev/null
SRC_CUST=$(my_src_sql "$SRCDB" "SELECT COUNT(*) FROM customer")
SRC_ORD=$(my_src_sql "$SRCDB" "SELECT COUNT(*) FROM orders")
echo "  seeded: customer=$SRC_CUST orders=$SRC_ORD"

# ---- recipe step "Preview before you run" -----------------------------------
STEP="Preview before you run"
PREVIEW=$("$SLUICE" schema preview \
  --source-driver mysql --source "$SRC_DSN" \
  --target-driver postgres --target "$DST_DSN" 2>&1)
assert_rc0 "$?" "$STEP" "schema preview runs"
assert_contains "$PREVIEW" "CREATE TABLE" "$STEP" "preview emits target DDL"

# ---- recipe step "The command" (the whole happy-path migrate) ---------------
STEP="The command"
"$SLUICE" migrate \
  --source-driver mysql --source "$SRC_DSN" \
  --target-driver postgres --target "$DST_DSN"
assert_rc0 "$?" "$STEP" "sluice migrate exits 0"

# documented outcome: every row landed (phase bulk_copy)
DST_CUST=$(pg_dst_sql "$DSTDB" "SELECT count(*) FROM customer")
DST_ORD=$(pg_dst_sql "$DSTDB" "SELECT count(*) FROM orders")
assert_eq "$DST_CUST" "$SRC_CUST" "$STEP" "customer row parity"
assert_eq "$DST_ORD" "$SRC_ORD" "$STEP" "orders row parity"

# documented outcome: phase 3 identity_sync advanced the sequence past MAX(id)
# so a fresh insert doesn't collide with a bulk-copied PK.
NEXTVAL=$(pg_dst_sql "$DSTDB" "SELECT nextval(pg_get_serial_sequence('public.customer','id'))")
if [ -z "$NEXTVAL" ] || [ "$NEXTVAL" -le "$SRC_CUST" ]; then
  _recipe_fail "The command (phase 3 identity_sync)" \
    "sequence not advanced past copied max: nextval=$NEXTVAL, copied=$SRC_CUST"
fi
pass_note "identity_sync advanced sequence (nextval=$NEXTVAL > copied max=$SRC_CUST)"

# ---- recipe step "What's NOT in this recipe -> Verifying every row landed" ---
STEP="What's NOT in this recipe (verify)"
"$SLUICE" verify \
  --source-driver mysql --source "$SRC_DSN" \
  --target-driver postgres --target "$DST_DSN" \
  --depth=count
assert_rc0 "$?" "$STEP" "sluice verify --depth=count passes"

echo ""
echo "RECIPE PASS: recipe-migrate-once.md"
cleanup
