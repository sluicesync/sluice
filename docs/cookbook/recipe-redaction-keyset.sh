#!/usr/bin/env bash
# Executable pin for docs/cookbook/recipe-redaction-keyset.md
# (roadmap item 53: cookbook-as-executable-tests).
#
# Runs the recipe's file-backed-keyset flow verbatim-in-intent against the
# released binary ($SLUICE) and asserts the documented outcomes:
#   - a keyed redaction (`hash:hmac-sha256:<keyname>`) actually redacts the PII
#     column on the target: the value differs from the source AND equals an
#     INDEPENDENTLY computed HMAC-SHA256 (recipe "Step 2: declare redactions" +
#     "How sluice preserves cross-stream determinism")
#   - the surrogate is deterministic across runs — a second migrate produces the
#     SAME surrogate (recipe headline: "deterministically across runs")
#   - a keyed strategy declared WITHOUT `--keyset-source` refuses loudly at
#     preflight (recipe "Common pitfalls: No --keyset-source, but
#     hash:hmac-sha256 declared" — "refuses loudly ... there's no fallback")
#
# THE SYNTAX THIS PINS (the bug this recipe just fixed): the redaction rule is
# colon-separated — `--redact TABLE.COL=STRATEGY[:arg[:arg]]`, e.g.
# `hash:hmac-sha256:<keyname>` — NOT the old comma/`key=` form. The keyset is
# named via the trailing `:<keyname>` segment and sourced with
# `--keyset-source file:<path>`.
#
# ---- REAL DOC BUG this pin surfaces (flagged, NOT silently worked around) ----
# recipe-redaction-keyset.md "Step 1 / Option A: file-backed" documents the
# keyset YAML as:
#       keys:
#         email_v1: "base64-of-32-random-bytes-here..."
# The released binary REJECTS that shape at preflight:
#   "--keyset-source ...: redact: keyset has no keys (the 'keyset.keys' list is
#    empty); declare at least one named key with a generation"
# The shape the binary actually accepts is the nested one from the authoritative
# docs/redaction.md "Keyset shape":
#       keyset:
#         default: <keyname>
#         keys:
#           - name: <keyname>
#             active: 1
#             generations:
#               - generation: 1
#                 bytes: "<base64 32-byte secret>"
# This pin writes the REAL (accepted) shape so it stays green, and this header +
# the runner report are the flag for a docs fix to recipe-redaction-keyset.md
# Step 1 (align its Option A/B/C examples with docs/redaction.md's keyset shape).
#
# Local PG src -> PG dst; throwaway DBs (cookbook_red_src / cookbook_red_dst)
# it creates + drops, plus a temp keyset file.
set -uo pipefail
cd "$(dirname "$0")"; . ./_cookbook-lib.sh
RECIPE_PAGE="docs/cookbook/recipe-redaction-keyset.md"
require_sluice

SRCDB="cookbook_red_src"; DSTDB="cookbook_red_dst"
SRC_DSN="$(pg_src_dsn "$SRCDB")"; DST_DSN="$(pg_dst_dsn "$DSTDB")"
KSDIR="${TMPDIR:-/tmp}/cookbook_red_$$"; KEYNAME="email_v1"

cleanup(){
  pg_src_admin "DROP DATABASE IF EXISTS $SRCDB;" >/dev/null 2>&1 || true
  pg_dst_admin "DROP DATABASE IF EXISTS $DSTDB;" >/dev/null 2>&1 || true
  pg_dst_admin "DROP DATABASE IF EXISTS ${DSTDB}_nk;" >/dev/null 2>&1 || true
  rm -rf "$KSDIR" 2>/dev/null || true
}
cleanup  # start clean
mkdir -p "$KSDIR"

echo "== recipe-redaction-keyset: file-backed keyset, deterministic HMAC redaction =="

# ---- setup: a PG source with a PII column (email) ---------------------------
pg_src_admin "CREATE DATABASE $SRCDB;" >/dev/null
pg_src_sql "$SRCDB" "CREATE TABLE users (
  id int PRIMARY KEY, email text NOT NULL, name text
);" >/dev/null
pg_src_sql "$SRCDB" "INSERT INTO users VALUES
  (1,'alice@example.com','Alice'),
  (2,'bob@example.com','Bob'),
  (3,'carol@example.com','Carol');" >/dev/null
pg_dst_admin "CREATE DATABASE $DSTDB;" >/dev/null
SRC_CNT=$(pg_src_sql "$SRCDB" "SELECT count(*) FROM users")
echo "  seeded: users=$SRC_CNT"

# ---- Step 1: provision a file-backed keyset (REAL nested shape; see header) --
# A 32-byte random secret, base64-encoded, is the HMAC key named "$KEYNAME".
STEP="Step 1: provision a keyset"
SECRET=$(head -c 32 /dev/urandom | base64 | tr -d '\n')
KSFILE="$KSDIR/keyset.yaml"
cat > "$KSFILE" <<EOF
keyset:
  default: $KEYNAME
  keys:
    - name: $KEYNAME
      active: 1
      generations:
        - generation: 1
          bytes: "$SECRET"
EOF
[ -s "$KSFILE" ] || _recipe_fail "$STEP" "keyset file not written"
pass_note "keyset provisioned (file-backed, 1 named key '$KEYNAME')"

# independent ground truth: HMAC-SHA256(key=secret_bytes, msg=email) as hex.
# This is what a correct deterministic hash MUST produce; sluice computing
# anything else is a redaction bug.
HEXKEY=$(printf '%s' "$SECRET" | base64 -d | xxd -p | tr -d '\n')
gt_hmac(){ printf '%s' "$1" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$HEXKEY" | awk '{print $NF}'; }
GT_ALICE=$(gt_hmac "alice@example.com")

# ---- Step 2 + Step 3: declare the redaction + run migrate -------------------
STEP="Step 2/3: declare redaction + run"
"$SLUICE" migrate \
  --source-driver postgres --source "$SRC_DSN" \
  --target-driver postgres --target "$DST_DSN" \
  --keyset-source "file:$KSFILE" \
  --redact "public.users.email=hash:hmac-sha256:$KEYNAME"
assert_rc0 "$?" "$STEP" "migrate with keyed redaction exits 0"

# every row still landed (redaction doesn't change row counts)
DST_CNT=$(pg_dst_sql "$DSTDB" "SELECT count(*) FROM users")
assert_eq "$DST_CNT" "$SRC_CNT" "$STEP" "row parity (redaction preserves counts)"

# documented outcome: the PII column is redacted on the target ...
DST_EMAIL1=$(pg_dst_sql "$DSTDB" "SELECT email FROM users WHERE id=1")
SRC_EMAIL1="alice@example.com"
if [ "$DST_EMAIL1" = "$SRC_EMAIL1" ]; then
  _recipe_fail "$STEP" "target email NOT redacted — still the plaintext source value '$SRC_EMAIL1' (silent-loss class)"
fi
pass_note "target PII differs from source (redacted, not plaintext)"

# ... and it equals the independently-computed deterministic HMAC surrogate.
assert_eq "$DST_EMAIL1" "$GT_ALICE" "$STEP" "redacted value == independent HMAC-SHA256(key,email)"

# name column was NOT declared -> must pass through untouched
DST_NAME1=$(pg_dst_sql "$DSTDB" "SELECT name FROM users WHERE id=1")
assert_eq "$DST_NAME1" "Alice" "$STEP" "non-redacted column passes through unchanged"

# ---- recipe headline: deterministic ACROSS RUNS -----------------------------
STEP="Determinism across runs"
# re-run the same migrate into a fresh target; the surrogate must be identical.
pg_dst_sql "$DSTDB" "DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO $PG_USER;" >/dev/null
"$SLUICE" migrate \
  --source-driver postgres --source "$SRC_DSN" \
  --target-driver postgres --target "$DST_DSN" \
  --keyset-source "file:$KSFILE" \
  --redact "public.users.email=hash:hmac-sha256:$KEYNAME" >/dev/null 2>&1
assert_rc0 "$?" "$STEP" "second migrate exits 0"
DST_EMAIL1B=$(pg_dst_sql "$DSTDB" "SELECT email FROM users WHERE id=1")
assert_eq "$DST_EMAIL1B" "$DST_EMAIL1" "$STEP" "same input -> same surrogate across runs"

# ---- Common pitfalls: keyed strategy WITHOUT --keyset-source refuses loudly --
STEP="Common pitfalls: no --keyset-source must refuse"
pg_dst_admin "DROP DATABASE IF EXISTS ${DSTDB}_nk;" >/dev/null 2>&1 || true
pg_dst_admin "CREATE DATABASE ${DSTDB}_nk;" >/dev/null
NK_DSN="$(pg_dst_dsn "${DSTDB}_nk")"
"$SLUICE" migrate \
  --source-driver postgres --source "$SRC_DSN" \
  --target-driver postgres --target "$NK_DSN" \
  --redact "public.users.email=hash:hmac-sha256:$KEYNAME" >/dev/null 2>&1
assert_rc_nonzero "$?" "$STEP" "keyed redaction without keyset refuses at preflight"
pg_dst_admin "DROP DATABASE IF EXISTS ${DSTDB}_nk;" >/dev/null 2>&1 || true

echo ""
echo "RECIPE PASS: recipe-redaction-keyset.md"
cleanup
