#!/bin/sh
# neki-probe.sh — measure what a Neki (sharded PlanetScale Postgres) router
# actually does, for the questions docs/dev/neki-readiness.md says decide
# whether sluice can support it and in what shape.
#
# READ-ONLY BY DEFAULT. The write and DDL probes are gated behind
# --allow-writes, because the first run against a cluster we do not yet
# understand should not be the thing that creates state on it.
#
# Every probe prints its raw answer alongside a verdict. That is deliberate:
# a verdict is this script's hypothesis about the answer, and the raw output
# is the evidence — when they disagree, the raw output wins and the script is
# what needs fixing.
#
# Usage:
#   NEKI_DSN='postgres://...' sh scripts/neki-probe.sh [--allow-writes]
#
# The DSN is never printed and never written to a file. It is passed into the
# client container through the environment.
set -eu

ALLOW_WRITES=0
for arg in "$@"; do
	case "$arg" in
	--allow-writes) ALLOW_WRITES=1 ;;
	-h | --help)
		sed -n '2,22p' "$0"
		exit 0
		;;
	*)
		echo "neki-probe: unknown argument: $arg" >&2
		exit 2
		;;
	esac
done

if [ -z "${NEKI_DSN:-}" ]; then
	echo "neki-probe: NEKI_DSN is not set." >&2
	echo "  Supply a Neki role connection string, e.g.:" >&2
	echo "    set -a; . /c/code/NEKI_SLUICESYNC.env; set +a" >&2
	echo "  On this machine the PlanetScale service token can SEE neki-test at org level" >&2
	echo "  but has no branch-level grant, so it cannot mint one." >&2
	exit 2
fi

# A dockerised client, because this machine has no local psql. Pinned so the
# client version is part of the recorded evidence rather than whatever the
# host happened to have.
PG_IMAGE="${PG_IMAGE:-postgres:16}"
DOCKER="${DOCKER:-docker}"
command -v "$DOCKER" >/dev/null 2>&1 || DOCKER="/c/Program Files/Rancher Desktop/resources/resources/win32/bin/docker.exe"

PASS=0
FAIL=0
UNKNOWN=0

# psql_q runs one SQL statement and echoes its output (or the error text).
# Never echoes the DSN.
psql_q() {
	"$DOCKER" run --rm -e PGCONNECT_TIMEOUT=15 -e NEKI_DSN="$NEKI_DSN" "$PG_IMAGE" \
		psql "$NEKI_DSN" -X -A -t -v ON_ERROR_STOP=1 -c "$1" 2>&1 || true
}

# psql_repl opens a REPLICATION connection and runs one replication command.
# This is probe R-1's whole point: the connection either is accepted or it is
# not, and the answer decides whether sluice has any CDC path here at all.
#
# The replication parameter is appended to the DSN rather than passed as a
# second -d. `psql "$DSN" -d "replication=database"` looks like it adds an
# option and does not — the last -d WINS and becomes the entire conninfo, so
# the DSN is discarded and psql dials a local unix socket. The first run of
# this script did that and reported "no replication protocol through the
# router" while never having contacted the router.
repl_dsn() {
	case "$NEKI_DSN" in
	*\?*) printf '%s&replication=database' "$NEKI_DSN" ;;
	*) printf '%s?replication=database' "$NEKI_DSN" ;;
	esac
}

psql_repl() {
	"$DOCKER" run --rm -e PGCONNECT_TIMEOUT=15 "$PG_IMAGE" \
		psql "$(repl_dsn)" -X -A -t -v ON_ERROR_STOP=1 -c "$1" 2>&1 || true
}

# unreachable distinguishes "we never reached the server" from "the server
# answered no". Without this the two are the same string to a `case` pattern,
# and every probe reports its own interesting verdict off one dead connection.
#
# This is not hypothetical caution: the first run of this script did exactly
# that. A missing TLS root cert failed every connection, and the script
# cheerfully reported "COPY (SELECT …) TO is rejected" and "no replication
# protocol through the router" — two findings that would have gone into the
# readiness doc as measurements. A verdict whose evidence is the same failure
# as every other verdict is not evidence.
unreachable() {
	# A server that answered FATAL / ERROR SPOKE to us. That is a measurement,
	# and the most interesting ones arrive exactly that way — R-1's answer is a
	# FATAL carried inside a "connection to server … failed:" wrapper, because
	# a refused startup packet IS a failed connection at the libpq layer.
	#
	# Checking this FIRST is the whole point. The first cut of this function
	# matched the wrapper and classified R-1 as UNREACHABLE, suppressing the
	# single most valuable finding in the run — a classifier eating the answer
	# it was written to protect.
	case "$1" in
	*"FATAL:"* | *"ERROR:"* | *"not implemented"*) return 1 ;;
	esac
	case "$1" in
	*"psql: error: connection to server"* | \
		*"could not connect"* | \
		*"No such file or directory"* | \
		*"root certificate file"* | \
		*"Unable to find image"* | \
		*"timeout expired"*)
		return 0
		;;
	esac
	return 1
}

report() {
	# report <id> <verdict> <question> <raw>
	id="$1"
	verdict="$2"
	question="$3"
	raw="$4"
	if unreachable "$raw"; then
		verdict="UNREACHABLE — the connection failed, so this probe measured NOTHING. Not a finding."
	fi
	printf '\n=== %s — %s\n' "$id" "$question"
	printf '    raw: %s\n' "$(printf '%s' "$raw" | head -c 600 | tr '\n' ' ')"
	printf '    VERDICT: %s\n' "$verdict"
	case "$verdict" in
	PASS*) PASS=$((PASS + 1)) ;;
	FAIL*) FAIL=$((FAIL + 1)) ;;
	*) UNKNOWN=$((UNKNOWN + 1)) ;;
	esac
}

echo "neki-probe: read-only=$([ "$ALLOW_WRITES" -eq 1 ] && echo no || echo YES)  client=$PG_IMAGE"

# ---------------------------------------------------------------------------
# Baseline — is this thing reachable and what does it say it is?
# ---------------------------------------------------------------------------
raw=$(psql_q "SELECT version()")
case "$raw" in
*PostgreSQL*)
	report "B-0" "PASS — reachable" "does the router answer at all, and as what?" "$raw"
	;;
*)
	report "B-0" "FAIL — cannot reach the router" "does the router answer at all?" "$raw"
	echo ""
	echo "neki-probe: ABORTING. Every probe below would report a verdict derived from this"
	echo "            same failure, which is how a dead connection turns into a page of"
	echo "            confident findings. Fix the connection first."
	echo "            Common cause on this machine: the minted DSN carries sslmode=verify-full"
	echo "            and the client container has no root cert. PlanetScale Postgres wants"
	echo "            sslmode=require."
	exit 1
	;;
esac

raw=$(psql_q "SHOW server_version")
report "B-1" "INFO" "server_version through the router" "$raw"

raw=$(psql_q "SELECT current_setting('__neki.fanout', true)")
case "$raw" in
"") report "B-2" "UNKNOWN — setting absent; is this actually a Neki router?" "__neki.fanout default" "$raw" ;;
*single*) report "B-2" "FAIL — default 'single' rejects every scatter read sluice makes (readiness A-6)" "__neki.fanout default" "$raw" ;;
*) report "B-2" "PASS — scatter-capable default" "__neki.fanout default" "$raw" ;;
esac

raw=$(psql_q "SELECT current_setting('__neki.tx_mode', true)")
report "B-3" "INFO — readiness A-2 keys on this" "__neki.tx_mode default" "$raw"

# ---------------------------------------------------------------------------
# R-1 .. R-5 — the replication surface. R-1 is the load-bearing one.
# ---------------------------------------------------------------------------
raw=$(psql_repl "IDENTIFY_SYSTEM")
case "$raw" in
*systemid* | *timeline*)
	report "R-1" "PASS — the router accepts a cluster-wide REPLICATION connection; sluice's PG CDC lane may attach as-is" \
		"does the router speak the replication sub-protocol?" "$raw"
	report "R-2" "INFO — record this systemid; sluice v0.149.0 pins it into the snapshot position" \
		"what identity does IDENTIFY_SYSTEM return through a router?" "$raw"
	;;
*__neki.shard* | *"must target a specific shard"*)
	report "R-1" "PARTIAL — the router SPEAKS replication but requires a per-SHARD target (options=-c __neki.shard=<uid>). CDC is therefore per-shard: N connections, N slots, N positions. sluice's single-position model does not fit as-is" \
		"does the router speak the replication sub-protocol?" "$raw"
	;;
*)
	report "R-1" "FAIL — no replication protocol through the router: sluice has NO CDC path against a Neki DSN as it stands (readiness R-6 is then the next question)" \
		"does the router speak the replication sub-protocol?" "$raw"
	;;
esac

raw=$(psql_q "SELECT slot_name, plugin, slot_type, database, active FROM pg_replication_slots")
case "$raw" in
*ERROR* | *error* | *does\ not\ exist*)
	report "R-4" "FAIL — pg_replication_slots not readable; v0.149.0's resume compares against confirmed_flush_lsn and cannot" \
		"is pg_replication_slots readable through the router?" "$raw" ;;
*)
	report "R-4" "PASS — readable (empty is a fine answer here)" \
		"is pg_replication_slots readable through the router?" "$raw" ;;
esac

raw=$(psql_q "SELECT name, setting FROM pg_settings WHERE name IN ('wal_level','max_replication_slots','max_wal_senders')")
report "R-4b" "INFO — wal_level must be 'logical' for any slot to exist" "replication-related settings" "$raw"

# ---------------------------------------------------------------------------
# R-8 — can sluice tell a sharded table from an unsharded one? Every Tier-A
# refusal keys on this; without it sluice must refuse database-wide.
# ---------------------------------------------------------------------------
raw=$(psql_q "SELECT n.nspname FROM pg_namespace n WHERE n.nspname LIKE '%neki%' OR n.nspname LIKE '%shard%'")
report "R-8a" "INFO" "is there a Neki-owned catalog namespace?" "$raw"

raw=$(psql_q "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast') AND c.relkind='r' ORDER BY 1 LIMIT 30")
report "R-8b" "INFO — the reader's view of user tables; compare against what the dashboard says is sharded" "what tables does the catalog expose?" "$raw"

# ---------------------------------------------------------------------------
# A-3 / A-7 — the SQL and COPY restrictions sluice's own emitters would hit.
# ---------------------------------------------------------------------------
raw=$(psql_q "COPY (SELECT 1) TO STDOUT")
case "$raw" in
*ERROR* | *error* | *reject*)
	report "A-3" "FAIL (expected) — COPY (SELECT …) TO is rejected; sluice's fast PG read path and its chunked reads are built on exactly this" \
		"is COPY (SELECT …) TO available?" "$raw" ;;
*)
	report "A-3" "PASS — available; note the docs say otherwise, so re-read them before relying on it" \
		"is COPY (SELECT …) TO available?" "$raw" ;;
esac

raw=$(psql_q "SELECT 1 EXCEPT SELECT 2")
case "$raw" in
*ERROR* | *error* | *reject*)
	report "A-7" "FAIL (expected on application tables) — check whether any verify/diff path renders EXCEPT" \
		"is EXCEPT available?" "$raw" ;;
*)
	report "A-7" "PASS — available at least for constant selects" "is EXCEPT available?" "$raw" ;;
esac

raw=$(psql_q "SELECT pg_is_in_recovery()")
report "A-8" "INFO — sluice's standby refusal keys on this; a REPLICA-routed connection must not read as a primary" \
	"what does pg_is_in_recovery() say through the router?" "$raw"

# ---------------------------------------------------------------------------
# Write / DDL probes — gated.
# ---------------------------------------------------------------------------
if [ "$ALLOW_WRITES" -eq 1 ]; then
	raw=$(psql_q "CREATE TABLE IF NOT EXISTS sluice_probe_unsharded (id bigint PRIMARY KEY, t text)")
	report "W-2" "INFO — does a CREATE TABLE with no shard key succeed, and silently?" \
		"shard-key-less CREATE TABLE (the H-2 door's analogue)" "$raw"

	raw=$(psql_q "INSERT INTO sluice_probe_unsharded (id, t) VALUES (1,'a'),(2,'b') ON CONFLICT DO NOTHING; SELECT count(*) FROM sluice_probe_unsharded")
	report "W-1a" "INFO" "does a plain multi-row INSERT work?" "$raw"

	echo ""
	echo "neki-probe: write probes left table sluice_probe_unsharded in place."
	echo "            Drop it when done:  DROP TABLE sluice_probe_unsharded;"
else
	echo ""
	echo "neki-probe: write/DDL probes SKIPPED (pass --allow-writes to run them)."
fi

# ---------------------------------------------------------------------------
printf '\n---\nneki-probe: %d pass, %d fail, %d informational/unknown\n' "$PASS" "$FAIL" "$UNKNOWN"
echo "Record the raw answers into docs/dev/neki-readiness.md — a verdict without its evidence is the thing that file exists to prevent."
