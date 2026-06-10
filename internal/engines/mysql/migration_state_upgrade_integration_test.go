//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-version pin for the ADR-0082 legacy-blob → per-table-rows
// migrate-state upgrade on MySQL — the mirror of the postgres-side
// pin (see migration_state_upgrade_integration_test.go there for the
// fixture provenance and the pin-the-class family-matrix rationale;
// the fixture and assertions are kept byte/shape-identical so the two
// engines' upgrade paths are pinned against the SAME v0.99.x blob).

package mysql

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
// (no state_format column).
const legacyMigrateStateDDL = "CREATE TABLE `sluice_migrate_state` (" +
	"migration_id    VARCHAR(255) NOT NULL," +
	"phase           VARCHAR(32)  NOT NULL," +
	"table_progress  TEXT         NULL," +
	"started_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP," +
	"updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
	"last_error      TEXT         NULL," +
	"PRIMARY KEY (migration_id)" +
	") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

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

// TestMigrationStateStoreMySQL_LegacyBlobUpgrade is the end-to-end
// cross-version pin on real MySQL.
func TestMigrationStateStoreMySQL_LegacyBlobUpgrade(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
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
		"INSERT INTO `sluice_migrate_state` (migration_id, phase, table_progress, last_error) VALUES (?, ?, ?, ?)",
		"legacy-mig", "bulk_copy", legacyBlobFixture, "phase failed: connection reset"); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}

	// 2. New binary arrives: Ensure adds state_format (detect-then-
	// ALTER) + creates the progress table.
	eng := Engine{}
	store, err := eng.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenMigrationStateStore: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	// Idempotency: the detect-then-ALTER path is a no-op second time.
	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("second EnsureControlTable: %v", err)
	}

	// 3. Orphan progress row — the upgrade's delete-first must clear it.
	if _, err := db.ExecContext(ctx,
		"INSERT INTO `sluice_migrate_table_progress` (migration_id, table_name, progress) VALUES (?, ?, ?)",
		"legacy-mig", "ghost_table", `"complete"`); err != nil {
		t.Fatalf("insert orphan progress row: %v", err)
	}

	// 4. First Read performs the one-time upgrade.
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
	assertUpgradedProgressFamilies(t, got.TableProgress)

	// 5. On-disk shape: format 2, sentinel blob, 7 rows, orphan gone.
	var (
		format int
		blob   string
	)
	if err := db.QueryRowContext(ctx,
		"SELECT state_format, table_progress FROM `sluice_migrate_state` WHERE migration_id = ?",
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
		"SELECT COUNT(*) FROM `sluice_migrate_table_progress` WHERE migration_id = ?",
		"legacy-mig").Scan(&rows); err != nil {
		t.Fatalf("count progress rows: %v", err)
	}
	if rows != 7 {
		t.Errorf("progress rows = %d; want 7 (orphan must be cleared)", rows)
	}

	// 6. Old-binary loudness: the sentinel must fail the ≤v0.99.x
	// decoder.
	legacyDecode := map[string]ir.TableProgress{}
	if err := json.Unmarshal([]byte(blob), &legacyDecode); err == nil {
		t.Error("sentinel decoded cleanly under the legacy decoder; want loud failure")
	}

	// 7. Idempotence.
	again, ok, err := store.Read(ctx, "legacy-mig")
	if err != nil || !ok {
		t.Fatalf("second Read = ok=%v err=%v", ok, err)
	}
	assertUpgradedProgressFamilies(t, again.TableProgress)

	// 8. Per-table hot path on the upgraded layout.
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

	// 9. ClearMigration removes header + progress rows.
	if err := store.ClearMigration(ctx, "legacy-mig"); err != nil {
		t.Fatalf("ClearMigration: %v", err)
	}
	if _, ok, err := store.Read(ctx, "legacy-mig"); err != nil || ok {
		t.Errorf("Read after clear = ok=%v err=%v; want ok=false err=nil", ok, err)
	}
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM `sluice_migrate_table_progress` WHERE migration_id = ?",
		"legacy-mig").Scan(&rows); err != nil {
		t.Fatalf("count progress rows after clear: %v", err)
	}
	if rows != 0 {
		t.Errorf("progress rows after clear = %d; want 0", rows)
	}
}
