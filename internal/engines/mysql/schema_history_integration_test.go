//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the ADR-0049 sluice_cdc_schema_history
// control table (Chunk A). Boots a MySQL container and asserts:
//
//   - ensureSchemaHistoryTable is additive: a target that already has
//     sluice_cdc_state data keeps it intact, and a second ensure call
//     is a no-op.
//   - writeSchemaVersion → resolveSchemaVersion round-trips an
//     ir.Table through the backup tagged-union codec, selecting the
//     correct version per the GTID partial order.
//   - A position below the retention floor surfaces a loud
//     ir.ErrPositionInvalid (→ ADR-0022 cold-start).

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

func TestEnsureSchemaHistoryTable_AdditiveToCDCState(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	// Pre-create sluice_cdc_state with a row, as a live stream would.
	applyMySQLApplier(t, dsn, "CREATE TABLE `sluice_cdc_state` ("+
		"  stream_id       VARCHAR(255) NOT NULL,"+
		"  source_position TEXT         NOT NULL,"+
		"  updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"+
		"  PRIMARY KEY (stream_id)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"+
		"INSERT INTO `sluice_cdc_state` (stream_id, source_position) VALUES ('live-stream', 'tok-1');")

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// The schema-history table now exists...
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'sluice_cdc_schema_history'`).Scan(&n); err != nil {
		t.Fatalf("table lookup: %v", err)
	}
	if n != 1 {
		t.Fatalf("sluice_cdc_schema_history missing after EnsureControlTable; count=%d", n)
	}

	// ...and the pre-existing cdc-state row is untouched.
	var tok string
	if err := db.QueryRowContext(ctx,
		"SELECT source_position FROM `sluice_cdc_state` WHERE stream_id = ?", "live-stream").Scan(&tok); err != nil {
		t.Fatalf("cdc-state select: %v", err)
	}
	if tok != "tok-1" {
		t.Errorf("cdc-state row mutated: token = %q; want tok-1", tok)
	}

	// Second ensure is a no-op (idempotent).
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("second EnsureControlTable: %v", err)
	}
}

func TestSchemaHistory_WriteResolveRoundTrip(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := ensureSchemaHistoryTable(ctx, db); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	const u = "11111111-1111-1111-1111-111111111111"
	anchorOld := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-10"}`}
	anchorNew := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-20"}`}

	tblOld := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
	}}
	tblNew := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: true},
	}}

	if err := writeSchemaVersion(ctx, db, "stream-1", "", "users", anchorOld, tblOld); err != nil {
		t.Fatalf("writeSchemaVersion old: %v", err)
	}
	if err := writeSchemaVersion(ctx, db, "stream-1", "", "users", anchorNew, tblNew); err != nil {
		t.Fatalf("writeSchemaVersion new: %v", err)
	}
	// Idempotent re-write of the same anchor.
	if err := writeSchemaVersion(ctx, db, "stream-1", "", "users", anchorNew, tblNew); err != nil {
		t.Fatalf("writeSchemaVersion new (idempotent): %v", err)
	}

	// Event at GTID 1-15 → between old and new → resolves to the OLD
	// (pre-ALTER) schema, the position-anchored-correctness property.
	pBetween := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-15"}`}
	got, err := resolveSchemaVersion(ctx, db, eng, "stream-1", "", "users", pBetween)
	if err != nil {
		t.Fatalf("resolve between: %v", err)
	}
	if len(got.Columns) != 1 {
		t.Errorf("between-resolve should be the 1-column pre-ALTER schema; got %d cols", len(got.Columns))
	}

	// Event at GTID 1-25 → at-or-after new → resolves to the NEW schema.
	pAfter := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-25"}`}
	got, err = resolveSchemaVersion(ctx, db, eng, "stream-1", "", "users", pAfter)
	if err != nil {
		t.Fatalf("resolve after: %v", err)
	}
	if len(got.Columns) != 2 {
		t.Errorf("after-resolve should be the 2-column post-ALTER schema; got %d cols", len(got.Columns))
	}

	// Event at GTID 1-5 → before the oldest retained anchor → loud
	// ErrPositionInvalid (DP-2 floor → ADR-0022 cold-start).
	pBelow := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-5"}`}
	if _, err := resolveSchemaVersion(ctx, db, eng, "stream-1", "", "users", pBelow); !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("below-floor resolve must wrap ir.ErrPositionInvalid; got %v", err)
	}
}

// TestSchemaHistory_VersionAndPosition_SameTxAtomicity is the
// locked-decision-#4a integration pin: the schema-version write and
// the ADR-0007 position write MUST be the same target transaction, so
// a failure in the version write rolls back the position write (a
// cross-tx crash that persists a position whose schema version isn't
// durable causes a spurious ADR-0022 cold-start). We inject a
// version-write failure (the schema-history table is absent) inside a
// tx that has already written the position, then assert the position
// row never landed after the rollback.
//
// **ADR-0049 #4a invariant (Chunk E regression-pin):** the
// schema-history write rides the SAME target tx as the ADR-0007
// position write. This test IS the direct extension of the ADR-0007
// position-and-data atomicity contract into the schema-history
// realm — pre-Chunk-B, a position write that committed without a
// version write was structurally impossible (no version write
// existed); post-Chunk-B, the same property must hold by sharing
// the tx, NOT by serial writes. Any change that introduces a
// separate tx for the version write (e.g. write-then-commit-tx,
// then start-tx for position) silently breaks this invariant and
// regresses the spurious-cold-start class.
func TestSchemaHistory_VersionAndPosition_SameTxAtomicity(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := ensureControlTable(ctx, db); err != nil {
		t.Fatalf("ensureControlTable: %v", err)
	}
	// Deliberately do NOT create sluice_cdc_schema_history so the
	// version write inside the tx fails (table doesn't exist).

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	// Position write succeeds first (mirrors applyOne ordering: data
	// + version on the tx, then writePositionTx).
	if err := writePositionTx(ctx, tx, "atomic-stream", "tok-after-ddl", "", "", ""); err != nil {
		_ = tx.Rollback()
		t.Fatalf("writePositionTx: %v", err)
	}
	// Version write on the SAME tx fails (no schema-history table).
	anchor := ir.Position{Engine: engineNameMySQL, Token: "tok-after-ddl"}
	verr := writeSchemaVersion(ctx, tx, "atomic-stream", "", "users", anchor,
		&ir.Table{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}})
	if verr == nil {
		_ = tx.Rollback()
		t.Fatal("expected version write to fail (schema-history table absent), got nil")
	}
	// #4b: the failure is fatal/loud — the caller rolls back the
	// WHOLE tx, so the position write must NOT be durable.
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	_, ok, err := readPosition(ctx, db, "atomic-stream")
	if err != nil {
		t.Fatalf("readPosition: %v", err)
	}
	if ok {
		t.Fatal("position row IS present after a version-write failure + rollback — " +
			"version and position are NOT in the same tx (#4a violated; spurious-cold-start class)")
	}
}
