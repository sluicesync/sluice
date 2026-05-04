package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

// createLogicalReplicationSlot creates a logical replication slot
// using the pgoutput plugin, opting into the FAILOVER flag on
// PG 17+ servers and falling back to the FAILOVER-less path on PG
// ≤ 16. The exportSnapshot flag controls whether EXPORT_SNAPSHOT is
// included in the protocol-level command — pass true from the
// snapshot+CDC handoff path (cdc_snapshot.go) and false from the
// cold-start path (cdc_reader.go) where the snapshot isn't needed.
//
// Returns (consistentPoint, snapshotName, err). snapshotName is
// always empty when exportSnapshot is false, and may be empty even
// when true if the server didn't return one. consistentPoint is the
// LSN string the server reports as the slot's
// consistent_point — the caller parses it via pglogrepl.ParseLSN
// when a typed LSN is needed.
//
// # Why this exists
//
// PlanetScale Postgres (and any Patroni-fronted PG ≥ 17 deployment)
// requires logical replication slots to be created with the
// FAILOVER flag *and* listed in the cluster's permanent-slots
// configuration to survive switchover or failover. Without
// FAILOVER, slots are primary-local and silently lost on the next
// failover, with recovery requiring drop + recreate + re-snapshot.
//
// PG 17 added a FAILOVER option to the CREATE_REPLICATION_SLOT
// replication-protocol command. The pglogrepl library sluice
// depends on does not yet expose this option in its
// CreateReplicationSlotOptions struct (only Temporary,
// SnapshotAction, Mode), so we bypass pglogrepl's constructor and
// send the raw protocol command via pgconn.PgConn.Exec. The
// MultiResultReader.ReadAll path mirrors what
// pglogrepl.ParseCreateReplicationSlot does internally: a single
// result set with a single row of (slot_name, consistent_point,
// snapshot_name, output_plugin).
//
// On PG ≤ 16 the FAILOVER option doesn't exist; we emit a one-time
// stderr warning naming the slot and pointing the operator at
// docs/postgres-source-prep.md. Patroni (and PG 17's
// sync_replication_slots) is the only mechanism on those versions,
// and detecting "is this Patroni" cleanly is non-trivial — so the
// pragmatic call is to warn unconditionally on PG ≤ 16 and let the
// operator suppress noise by putting the slot in their permanent-
// slots config (which is what they should be doing anyway).
//
// # Protocol-level command shape
//
// On PG 17+, the command looks like:
//
//	CREATE_REPLICATION_SLOT "slotname" LOGICAL pgoutput (FAILOVER true)
//	CREATE_REPLICATION_SLOT "slotname" LOGICAL pgoutput (SNAPSHOT 'export', FAILOVER true)
//
// Options inside the parens-list are comma-separated. The snapshot
// option is the named form `SNAPSHOT 'export'` (or 'use' / 'nothing')
// — the bare `EXPORT_SNAPSHOT` keyword is the *old* PG ≤ 16 syntax
// and is rejected by PG 17+'s option-list parser with
// `ERROR: unrecognized option: export_snapshot`. PlanetScale Postgres
// surfaced this in v0.2.0 testing; the named form is the only one
// that survives.
//
// The boolean argument to FAILOVER is required (the docs allow
// defaults but always passing `true` is unambiguous). Slot
// identifiers go through quoteIdent to protect against names that
// would otherwise require escaping (the default "sluice_slot"
// doesn't, but a custom name might).
//
// On PG ≤ 16, we delegate to pglogrepl.CreateReplicationSlot,
// which sends the legacy bare-keyword form (EXPORT_SNAPSHOT) outside
// of any option-list — a separate parser path on the server, so
// nothing here interferes.
func createLogicalReplicationSlot(
	ctx context.Context,
	db *sql.DB,
	replConn *pgconn.PgConn,
	slotName string,
	exportSnapshot bool,
) (consistentPoint, snapshotName string, err error) {
	version, err := serverVersionNum(ctx, db)
	if err != nil {
		return "", "", err
	}

	if version >= pgVersionFailoverSupport {
		return createSlotWithFailover(ctx, replConn, slotName, exportSnapshot)
	}

	warnNoFailoverSupport(slotName, version)
	return createSlotViaPglogrepl(ctx, replConn, slotName, exportSnapshot)
}

// createSlotWithFailover sends the raw CREATE_REPLICATION_SLOT
// protocol command including FAILOVER true. Only safe on PG 17+;
// older servers reject FAILOVER as an unknown option.
func createSlotWithFailover(
	ctx context.Context,
	conn *pgconn.PgConn,
	slotName string,
	exportSnapshot bool,
) (consistentPoint, snapshotName string, err error) {
	// Build the parens-list option string. Order doesn't matter to
	// the server, but we put SNAPSHOT first to match the order in
	// the PG 17 protocol-replication docs.
	opts := []string{}
	if exportSnapshot {
		// PG 17+ uses the named-option form: SNAPSHOT 'export'. The
		// bare EXPORT_SNAPSHOT keyword (pre-PG-17 syntax) is rejected
		// inside an option-list with "ERROR: unrecognized option:
		// export_snapshot" — observed against PlanetScale Postgres
		// during v0.2.0 testing.
		opts = append(opts, "SNAPSHOT 'export'")
	}
	opts = append(opts, "FAILOVER true")

	// quoteIdent doubles embedded double-quotes; the default slot
	// name "sluice_slot" is unaffected, but a custom slot name with
	// quotes would otherwise corrupt the command.
	cmd := fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL pgoutput (%s)",
		quoteIdent(slotName), strings.Join(opts, ", "))

	mrr := conn.Exec(ctx, cmd)
	results, err := mrr.ReadAll()
	if err != nil {
		return "", "", fmt.Errorf("postgres: create replication slot %q (FAILOVER): %w", slotName, err)
	}
	if len(results) != 1 {
		return "", "", fmt.Errorf("postgres: create replication slot %q: expected 1 result set, got %d", slotName, len(results))
	}
	rows := results[0].Rows
	if len(rows) != 1 {
		return "", "", fmt.Errorf("postgres: create replication slot %q: expected 1 result row, got %d", slotName, len(rows))
	}
	row := rows[0]
	if len(row) != 4 {
		return "", "", fmt.Errorf("postgres: create replication slot %q: expected 4 result columns, got %d", slotName, len(row))
	}

	// row[0] = slot_name, row[1] = consistent_point, row[2] =
	// snapshot_name, row[3] = output_plugin. Mirrors
	// pglogrepl.ParseCreateReplicationSlot.
	return string(row[1]), string(row[2]), nil
}

// createSlotViaPglogrepl is the PG ≤ 16 fallback. Identical to the
// pre-existing call sites; preserves the EXPORT_SNAPSHOT vs
// default behaviour through SnapshotAction.
func createSlotViaPglogrepl(
	ctx context.Context,
	conn *pgconn.PgConn,
	slotName string,
	exportSnapshot bool,
) (consistentPoint, snapshotName string, err error) {
	opts := pglogrepl.CreateReplicationSlotOptions{
		Mode: pglogrepl.LogicalReplication,
	}
	if exportSnapshot {
		opts.SnapshotAction = "EXPORT_SNAPSHOT"
	}
	result, err := pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput", opts)
	if err != nil {
		return "", "", fmt.Errorf("postgres: create replication slot %q: %w", slotName, err)
	}
	return result.ConsistentPoint, result.SnapshotName, nil
}

// warnedSlots tracks slot names we've already warned about so the
// stderr message fires once per process per slot, not once per
// retry. sync.Map is the right shape here: warn-on-first-write
// with no read contention after the first call.
var warnedSlots sync.Map

// warnNoFailoverSupport emits a one-time stderr warning when sluice
// creates a slot on a PG ≤ 16 server. The message names the slot,
// the server version, and points at the operator-facing prep doc
// for the manual workaround (Patroni "slots:" / PlanetScale
// "Logical slot name" / PG 17 sync_replication_slots).
//
// Suppressed on the second-and-later call for the same slot name
// so retries / reconnect storms don't spam stderr.
func warnNoFailoverSupport(slotName string, version int) {
	if _, loaded := warnedSlots.LoadOrStore(slotName, struct{}{}); loaded {
		return
	}
	fmt.Fprintf(os.Stderr,
		"postgres: cdc: warning: creating slot %q on server_version_num=%d "+
			"(PG <17) — FAILOVER flag is not supported on this server version, "+
			"so the slot will be lost on switchover/failover events. To preserve "+
			"the slot, add it to your cluster's permanent-slots config (Patroni "+
			"\"slots:\" / PlanetScale \"Logical slot name\" UI). See "+
			"docs/postgres-source-prep.md.\n",
		slotName, version)
}

// resetSlotWarningsForTest clears the warned-slots set. Used only
// from unit tests in this package; not part of the engine's public
// surface.
func resetSlotWarningsForTest() {
	warnedSlots.Range(func(key, _ any) bool {
		warnedSlots.Delete(key)
		return true
	})
}
