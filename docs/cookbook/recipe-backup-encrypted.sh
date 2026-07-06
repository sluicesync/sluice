#!/usr/bin/env bash
# Executable pin for docs/cookbook/recipe-backup-encrypted.md
# (roadmap item 53: cookbook-as-executable-tests).
#
# Runs the recipe's PASSPHRASE-mode flow verbatim-in-intent against the
# released binary ($SLUICE) and asserts the documented outcomes:
#   - `backup full --chain-slot --encrypt --encryption-passphrase` writes a
#     chain root manifest (recipe "Step 1: full backup")
#   - `backup incremental` chains an encrypted increment off the full
#     (recipe "Step 2: incremental stream" — see NOTE below on stream vs incremental)
#   - `backup verify --encrypt` passes with the right passphrase AND loudly
#     refuses a WRONG passphrase (recipe "Step 3: verify periodically" +
#     "Common pitfalls: Lost the passphrase")
#   - `sluice restore --encrypt` reproduces the source byte-for-byte
#     (recipe "Step 4: restore")
#
# EXCLUDED from the automated path (roadmap item 53 gotcha #1 — cloud deps):
#   - AWS KMS mode (`--kms-key-arn`): needs a live KMS endpoint; operator-only.
#     The passphrase path exercises the identical envelope machinery locally.
#
# NOTE on Step 2: the recipe documents `backup stream run` (a long-running
# rolling-incremental daemon). On an idle rig that call blocks in
# WalSenderWaitForWAL draining the wall-clock window, so the automated pin
# uses the bounded one-shot `backup incremental --window=20s` — same encrypted
# chain-extend path, deterministically terminating. Production operators run
# `backup stream run` under systemd/k8s as the recipe says.
#
# Uses a throwaway PG source DB (cookbook_bkup_src), a restore target DB
# (cookbook_bkup_rst), and a temp backup dir, all created + dropped here.
set -uo pipefail
cd "$(dirname "$0")"; . ./_cookbook-lib.sh
RECIPE_PAGE="docs/cookbook/recipe-backup-encrypted.md"
require_sluice

SRCDB="cookbook_bkup_src"; RSTDB="cookbook_bkup_rst"
SRC_DSN="$(pg_src_dsn "$SRCDB")"; RST_DSN="$(pg_dst_dsn "$RSTDB")"
BKDIR="${TMPDIR:-/tmp}/cookbook_bkup_$$"; PASS="cookbook-correct-horse-battery"; WRONGPASS="wrong-passphrase-xyz"

cleanup(){
  pg_src_sql "$SRCDB" "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots;" >/dev/null 2>&1 || true
  pg_src_admin "DROP DATABASE IF EXISTS $SRCDB;" >/dev/null 2>&1 || true
  pg_dst_admin "DROP DATABASE IF EXISTS $RSTDB;" >/dev/null 2>&1 || true
  rm -rf "$BKDIR" 2>/dev/null || true
}
cleanup  # start clean
mkdir -p "$BKDIR"

echo "== recipe-backup-encrypted: passphrase-mode full+incremental chain =="

# ---- setup: a live PG source with some rows to back up ----------------------
pg_src_admin "CREATE DATABASE $SRCDB;" >/dev/null
pg_src_sql "$SRCDB" "CREATE TABLE ledger (
  id bigint PRIMARY KEY, acct text NOT NULL, amount numeric(14,2), note text
);" >/dev/null
pg_src_sql "$SRCDB" "INSERT INTO ledger
  SELECT g, 'acct-'||(g%50), (g*3.33)::numeric(14,2), 'note '||g
  FROM generate_series(1,1000) g;" >/dev/null
SRC_CNT=$(pg_src_sql "$SRCDB" "SELECT count(*) FROM ledger")
echo "  seeded: ledger=$SRC_CNT"

# ---- recipe "Step 1: full backup" (passphrase mode, + --chain-slot for PG) ---
STEP="Step 1: full backup"
"$SLUICE" backup full \
  --source-driver postgres --source "$SRC_DSN" \
  --output-dir "$BKDIR" --chain-slot \
  --encrypt --encryption-passphrase "$PASS"
assert_rc0 "$?" "$STEP" "backup full --encrypt exits 0"
# documented artifact: the chain root manifest lands under the output dir.
assert_file_glob "$BKDIR" "*.json" "$STEP" "chain manifest written"

# ---- recipe "Step 2: incremental stream" (bounded incremental; see NOTE) -----
STEP="Step 2: incremental stream"
# produce some source changes for the incremental to capture
pg_src_sql "$SRCDB" "INSERT INTO ledger SELECT g,'acct-'||(g%50),(g*3.33)::numeric(14,2),'note '||g FROM generate_series(1001,1200) g;" >/dev/null
pg_src_sql "$SRCDB" "UPDATE ledger SET amount=amount+100 WHERE id BETWEEN 1 AND 100;" >/dev/null
"$SLUICE" backup incremental \
  --source-driver postgres --source "$SRC_DSN" \
  --output-dir "$BKDIR" --window=20s \
  --encrypt --encryption-passphrase "$PASS"
assert_rc0 "$?" "$STEP" "backup incremental (bounded --window) exits 0"
SRC_CNT2=$(pg_src_sql "$SRCDB" "SELECT count(*) FROM ledger")

# ---- recipe "Step 3: verify periodically" (right passphrase -> pass) ---------
STEP="Step 3: verify periodically"
"$SLUICE" backup verify \
  --from-dir "$BKDIR" \
  --encrypt --encryption-passphrase "$PASS"
assert_rc0 "$?" "$STEP" "backup verify --encrypt (correct passphrase) passes"

# documented outcome (Step 3 + Common pitfalls): a WRONG passphrase must fail
# loudly, not silently pass. This is the redaction-keyset-class pin: a
# regressed envelope check that silently accepted any passphrase would slip
# past an exit-0-only test.
STEP="Common pitfalls: wrong passphrase must refuse loudly"
"$SLUICE" backup verify \
  --from-dir "$BKDIR" \
  --encrypt --encryption-passphrase "$WRONGPASS" >/dev/null 2>&1
assert_rc_nonzero "$?" "$STEP" "backup verify with WRONG passphrase refuses"

# ---- recipe "Step 4: restore" (into a fresh target; byte-exact) --------------
STEP="Step 4: restore"
pg_dst_admin "CREATE DATABASE $RSTDB;" >/dev/null
"$SLUICE" restore \
  --from-dir "$BKDIR" \
  --target-driver postgres --target "$RST_DSN" \
  --encrypt --encryption-passphrase "$PASS"
assert_rc0 "$?" "$STEP" "sluice restore --encrypt exits 0"

RST_CNT=$(pg_dst_sql "$RSTDB" "SELECT count(*) FROM ledger")
assert_eq "$RST_CNT" "$SRC_CNT2" "$STEP" "restored row count matches source (post-incremental)"
SRC_MD5=$(pg_src_sql "$SRCDB" "SELECT md5(string_agg(id||'|'||acct||'|'||amount||'|'||coalesce(note,''), E'\n' ORDER BY id)) FROM ledger")
RST_MD5=$(pg_dst_sql "$RSTDB" "SELECT md5(string_agg(id||'|'||acct||'|'||amount||'|'||coalesce(note,''), E'\n' ORDER BY id)) FROM ledger")
assert_eq "$RST_MD5" "$SRC_MD5" "$STEP" "restored data byte-exact (md5)"

echo ""
echo "RECIPE PASS: recipe-backup-encrypted.md"
cleanup
