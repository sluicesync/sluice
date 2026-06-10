//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-version pin for the ADR-0082 legacy-blob → per-table-rows
// migrate-state upgrade: a sluice_migrate_state row in the EXACT
// shape a v0.99.x binary writes (legacy table DDL, whole-map JSON
// blob, no state_format column) must resume correctly under the new
// store — upgraded once, transactionally, into
// sluice_migrate_table_progress rows.
//
// Fixture provenance: legacyBlobFixture was byte-captured from the
// current ir.TableProgress marshaller at the pre-ADR-0082 tree
// (json.Marshal of the map — exactly what both engines'
// encodeTableProgress did), which is byte-identical to v0.99.33's:
// internal/ir/migration_state.go and both engines' migration_state.go
// were last touched before that tag (verified via
// `git log v0.99.33..HEAD -- <paths>` → empty). The one hand-added
// entry is "legacy3":"in_progress" — the v0.3.0 bare-string form that
// only v0.3.0 binaries wrote but every decoder since must accept.
//
// Pin-the-class (the Bug 74 discipline): the blob carries EVERY
// persisted TableProgress family × shape — bare-string complete,
// object-form complete+indexes_built (ADR-0077), cursor-bearing
// in_progress with a 1-column AND a multi-column PK, bare-string
// v0.3.0 in_progress, the no-PK sentinel, and the v0.5.0 chunked form
// with a complete AND an in-progress chunk — so a per-family decode
// or re-key regression in the upgrade can't hide behind a green
// representative.

package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/migratestate"
)

const legacyBlobFixture = `{"archive":{"state":"complete","indexes_built":true},` +
	`"events_log":"no_pk_truncate_and_redo",` +
	`"legacy3":"in_progress",` +
	`"orders":{"state":"in_progress","last_pk":[12345],"rows_copied":12345},` +
	`"products":{"state":"in_progress","last_pk":["a",7],"rows_copied":8000},` +
	`"shipments":{"state":"in_progress","chunks":[` +
	`{"chunk_index":0,"upper_pk":[100],"last_pk":[100],"rows_copied":100,"state":"complete"},` +
	`{"chunk_index":1,"lower_pk":[100],"upper_pk":[200],"last_pk":[142],"rows_copied":42,"state":"in_progress"}]},` +
	`"users":"complete"}`

// legacyMigrateStateDDL is the v0.99.x header-table shape, verbatim
// (no state_format column) — what a real upgraded deployment's target
// carries before the new binary's first EnsureControlTable.
const legacyMigrateStateDDL = `
	CREATE TABLE sluice_migrate_state (
		migration_id    VARCHAR(255) NOT NULL,
		phase           VARCHAR(32)  NOT NULL,
		table_progress  TEXT         NULL,
		started_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
		last_error      TEXT         NULL,
		PRIMARY KEY (migration_id)
	)`

// assertUpgradedProgressFamilies checks every family in the fixture
// decoded into the expected TableProgress shape. Shared by the PG and
// MySQL upgrade pins in spirit; duplicated per engine package because
// the packages don't share test helpers.
func assertUpgradedProgressFamilies(t *testing.T, got map[string]ir.TableProgress) {
	t.Helper()
	if len(got) != 7 {
		t.Fatalf("TableProgress len = %d; want 7 (%v)", len(got), got)
	}
	if e := got["users"]; e.State != ir.TableProgressComplete || e.IndexesBuilt {
		t.Errorf("users = %+v; want bare complete", e)
	}
	if e := got["archive"]; e.State != ir.TableProgressComplete || !e.IndexesBuilt {
		t.Errorf("archive = %+v; want complete + indexes_built", e)
	}
	if e := got["orders"]; e.State != ir.TableProgressInProgress || len(e.LastPK) != 1 || e.RowsCopied != 12345 {
		t.Errorf("orders = %+v; want in_progress cursor [12345]", e)
	}
	if e := got["products"]; e.State != ir.TableProgressInProgress || len(e.LastPK) != 2 || e.RowsCopied != 8000 {
		t.Errorf("products = %+v; want in_progress multi-col cursor", e)
	}
	if e := got["events_log"]; e.State != ir.TableProgressNoPKTruncateAndRedo {
		t.Errorf("events_log = %+v; want no_pk_truncate_and_redo", e)
	}
	if e := got["legacy3"]; e.State != ir.TableProgressInProgress || e.LastPK != nil {
		t.Errorf("legacy3 = %+v; want v0.3.0 cursor-less in_progress", e)
	}
	sh := got["shipments"]
	if sh.State != ir.TableProgressInProgress || len(sh.Chunks) != 2 {
		t.Fatalf("shipments = %+v; want in_progress with 2 chunks", sh)
	}
	if c := sh.Chunks[0]; c.State != ir.TableProgressComplete || c.RowsCopied != 100 || len(c.UpperPK) != 1 {
		t.Errorf("shipments chunk 0 = %+v; want complete/100 with upper bound", c)
	}
	if c := sh.Chunks[1]; c.State != ir.TableProgressInProgress || c.RowsCopied != 42 ||
		len(c.LowerPK) != 1 || len(c.UpperPK) != 1 || len(c.LastPK) != 1 {
		t.Errorf("shipments chunk 1 = %+v; want in_progress/42 with bounds + cursor", c)
	}
}

// TestMigrationStateStore_LegacyBlobUpgrade is the end-to-end
// cross-version pin on real Postgres.
func TestMigrationStateStore_LegacyBlobUpgrade(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	// 1. Seed the v0.99.x world: legacy table shape + legacy blob row.
	if _, err := db.ExecContext(ctx, legacyMigrateStateDDL); err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"INSERT INTO sluice_migrate_state (migration_id, phase, table_progress, last_error) VALUES ($1, $2, $3, $4)",
		"legacy-mig", "bulk_copy", legacyBlobFixture, "phase failed: connection reset"); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}

	// 2. New binary arrives: Ensure adds state_format + the progress
	// table.
	eng := Engine{}
	store, err := eng.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenMigrationStateStore: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	// 3. Plant an orphan progress row for the same migration_id (a
	// previous life's leftovers — e.g. an old binary's ClearMigration
	// deleted only the header). The upgrade's delete-first must clear
	// it so it can't shadow the blob's truth.
	if _, err := db.ExecContext(ctx,
		"INSERT INTO sluice_migrate_table_progress (migration_id, table_name, progress) VALUES ($1, $2, $3)",
		"legacy-mig", "ghost_table", `"complete"`); err != nil {
		t.Fatalf("insert orphan progress row: %v", err)
	}

	// 4. First Read performs the one-time upgrade and returns the
	// blob's progress.
	got, ok, err := store.Read(ctx, "legacy-mig")
	if err != nil {
		t.Fatalf("Read (upgrade): %v", err)
	}
	if !ok {
		t.Fatal("Read ok=false on legacy row")
	}
	if got.Phase != ir.MigrationPhaseBulkCopy {
		t.Errorf("Phase = %q; want bulk_copy", got.Phase)
	}
	if got.LastError != "phase failed: connection reset" {
		t.Errorf("LastError = %q; want preserved", got.LastError)
	}
	if got.StartedAt.IsZero() {
		t.Error("StartedAt is zero; want preserved server timestamp")
	}
	assertUpgradedProgressFamilies(t, got.TableProgress)

	// 5. On-disk shape after the upgrade: format 2, sentinel blob,
	// exactly the fixture's 7 progress rows (orphan gone).
	var (
		format int
		blob   string
	)
	if err := db.QueryRowContext(ctx,
		"SELECT state_format, table_progress FROM sluice_migrate_state WHERE migration_id = $1",
		"legacy-mig").Scan(&format, &blob); err != nil {
		t.Fatalf("inspect header: %v", err)
	}
	if format != migratestate.FormatPerTableRows {
		t.Errorf("state_format = %d; want %d", format, migratestate.FormatPerTableRows)
	}
	if blob != migratestate.UpgradedBlobSentinel {
		t.Errorf("table_progress = %q; want the upgrade sentinel", blob)
	}
	var rows int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sluice_migrate_table_progress WHERE migration_id = $1",
		"legacy-mig").Scan(&rows); err != nil {
		t.Fatalf("count progress rows: %v", err)
	}
	if rows != 7 {
		t.Errorf("progress rows = %d; want 7 (orphan must be cleared)", rows)
	}

	// 6. Old-binary loudness: the sentinel must FAIL the ≤v0.99.x
	// decoder (json.Unmarshal into the map — exactly what the old
	// Read did) so a downgraded binary refuses loudly instead of
	// silently reading "no progress" and re-copying every table.
	legacyDecode := map[string]ir.TableProgress{}
	if err := json.Unmarshal([]byte(blob), &legacyDecode); err == nil {
		t.Error("sentinel decoded cleanly under the legacy decoder; want loud failure")
	}

	// 7. Idempotence: a second Read sees format 2 and changes nothing.
	again, ok, err := store.Read(ctx, "legacy-mig")
	if err != nil || !ok {
		t.Fatalf("second Read = ok=%v err=%v", ok, err)
	}
	assertUpgradedProgressFamilies(t, again.TableProgress)

	// 8. The hot path lands on the upgraded layout: a per-table write
	// updates one row, peers untouched.
	if err := store.WriteTableProgress(ctx, "legacy-mig", "orders",
		ir.TableProgress{State: ir.TableProgressInProgress, LastPK: []any{int64(20000)}, RowsCopied: 20000}); err != nil {
		t.Fatalf("WriteTableProgress: %v", err)
	}
	after, _, err := store.Read(ctx, "legacy-mig")
	if err != nil {
		t.Fatalf("Read after per-table write: %v", err)
	}
	if after.TableProgress["orders"].RowsCopied != 20000 {
		t.Errorf("orders after per-table write = %+v; want rows_copied 20000", after.TableProgress["orders"])
	}
	if after.TableProgress["users"].State != ir.TableProgressComplete || len(after.TableProgress) != 7 {
		t.Errorf("peer entries disturbed by per-table write: %v", after.TableProgress)
	}

	// 9. ClearMigration removes the header AND the progress rows.
	if err := store.ClearMigration(ctx, "legacy-mig"); err != nil {
		t.Fatalf("ClearMigration: %v", err)
	}
	if _, ok, err := store.Read(ctx, "legacy-mig"); err != nil || ok {
		t.Errorf("Read after clear = ok=%v err=%v; want ok=false err=nil", ok, err)
	}
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sluice_migrate_table_progress WHERE migration_id = $1",
		"legacy-mig").Scan(&rows); err != nil {
		t.Fatalf("count progress rows after clear: %v", err)
	}
	if rows != 0 {
		t.Errorf("progress rows after clear = %d; want 0", rows)
	}
}
