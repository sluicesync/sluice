//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the ADR-0054 Phase 2c ShapeDeltaApplier +
// ShardConsolidationProber engine impls (Postgres). The PG-side
// idempotency relies on IF [NOT] EXISTS clauses; these tests pin
// that contract end-to-end via a real PG container.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

func TestShapeDeltaApplier_AddDropColumn_IdempotentRoundtrip(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."shape_users" (id INT PRIMARY KEY)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	eng := Engine{}
	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() { _ = sw.(*SchemaWriter).Close() }()

	table := &ir.Table{
		Schema: "public",
		Name:   "shape_users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
		},
	}
	addCol := &ir.Column{Name: "added_at", Type: ir.Timestamp{}, Nullable: true}

	// Apply twice — idempotent.
	pgsw := sw.(*SchemaWriter)
	if err := pgsw.AlterAddColumn(ctx, table, []*ir.Column{addCol}); err != nil {
		t.Fatalf("AlterAddColumn (1st): %v", err)
	}
	if err := pgsw.AlterAddColumn(ctx, table, []*ir.Column{addCol}); err != nil {
		t.Fatalf("AlterAddColumn (2nd, idempotent): %v", err)
	}

	// Drop twice — idempotent.
	if err := pgsw.AlterDropColumn(ctx, table, []*ir.Column{addCol}); err != nil {
		t.Fatalf("AlterDropColumn (1st): %v", err)
	}
	if err := pgsw.AlterDropColumn(ctx, table, []*ir.Column{addCol}); err != nil {
		t.Fatalf("AlterDropColumn (2nd, idempotent): %v", err)
	}
}

func TestShapeDeltaApplier_CreateDropIndex_IdempotentRoundtrip(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."shape_items" (id INT PRIMARY KEY, sku TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	eng := Engine{}
	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() { _ = sw.(*SchemaWriter).Close() }()

	table := &ir.Table{
		Schema: "public",
		Name:   "shape_items",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "sku", Type: ir.Text{}},
		},
	}
	idx := &ir.Index{
		Name:    "ix_sku",
		Columns: []ir.IndexColumn{{Column: "sku"}},
	}

	pgsw := sw.(*SchemaWriter)
	if err := pgsw.CreateShapeIndex(ctx, table, []*ir.Index{idx}); err != nil {
		t.Fatalf("CreateShapeIndex (1st): %v", err)
	}
	if err := pgsw.CreateShapeIndex(ctx, table, []*ir.Index{idx}); err != nil {
		t.Fatalf("CreateShapeIndex (2nd, idempotent): %v", err)
	}

	if err := pgsw.DropShapeIndex(ctx, table, []*ir.Index{idx}); err != nil {
		t.Fatalf("DropShapeIndex (1st): %v", err)
	}
	if err := pgsw.DropShapeIndex(ctx, table, []*ir.Index{idx}); err != nil {
		t.Fatalf("DropShapeIndex (2nd, idempotent): %v", err)
	}
}

func TestShardConsolidationProber_AddColumnAppliedNotApplied(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."shape_probe" (id INT PRIMARY KEY, added_at TIMESTAMP NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	eng := Engine{}
	a, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := a.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	applier := a.(*ChangeApplier)
	table := &ir.Table{Schema: "public", Name: "shape_probe"}

	// Column already exists → Applied.
	outcome, err := applier.ProbeAddColumn(ctx, table, []*ir.Column{{Name: "added_at"}})
	if err != nil {
		t.Fatalf("ProbeAddColumn (added_at exists): %v", err)
	}
	if outcome != ir.ProbeOutcomeApplied {
		t.Errorf("outcome = %v, want Applied", outcome)
	}

	// Column doesn't exist → NotApplied.
	outcome, err = applier.ProbeAddColumn(ctx, table, []*ir.Column{{Name: "no_such_col"}})
	if err != nil {
		t.Fatalf("ProbeAddColumn (no_such_col): %v", err)
	}
	if outcome != ir.ProbeOutcomeNotApplied {
		t.Errorf("outcome = %v, want NotApplied", outcome)
	}

	// Mixed → Inconsistent.
	outcome, err = applier.ProbeAddColumn(ctx, table, []*ir.Column{
		{Name: "added_at"},
		{Name: "missing_col"},
	})
	if err != nil {
		t.Fatalf("ProbeAddColumn (mixed): %v", err)
	}
	if outcome != ir.ProbeOutcomeInconsistent {
		t.Errorf("outcome = %v, want Inconsistent", outcome)
	}
}

func TestShardConsolidationProber_AlterColumnNullability(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."shape_null" (id INT PRIMARY KEY, n_col INT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	eng := Engine{}
	a, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := a.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	applier := a.(*ChangeApplier)
	table := &ir.Table{Schema: "public", Name: "shape_null"}

	// Want nullable=true, current is NULL → Applied.
	outcome, err := applier.ProbeAlterColumnNullability(ctx, table, &ir.Column{Name: "n_col", Nullable: true})
	if err != nil {
		t.Fatalf("ProbeAlterColumnNullability (match): %v", err)
	}
	if outcome != ir.ProbeOutcomeApplied {
		t.Errorf("outcome = %v, want Applied", outcome)
	}

	// Want NOT NULL, current is NULL → NotApplied.
	outcome, err = applier.ProbeAlterColumnNullability(ctx, table, &ir.Column{Name: "n_col", Nullable: false})
	if err != nil {
		t.Fatalf("ProbeAlterColumnNullability (mismatch): %v", err)
	}
	if outcome != ir.ProbeOutcomeNotApplied {
		t.Errorf("outcome = %v, want NotApplied", outcome)
	}

	// Absent column → Inconsistent.
	outcome, err = applier.ProbeAlterColumnNullability(ctx, table, &ir.Column{Name: "missing", Nullable: true})
	if err != nil {
		t.Fatalf("ProbeAlterColumnNullability (absent): %v", err)
	}
	if outcome != ir.ProbeOutcomeInconsistent {
		t.Errorf("outcome = %v, want Inconsistent", outcome)
	}
}

// TestShardConsolidationProber_AlterColumnType_V2 pins the v0.76.0
// task #20 IR-type-matching semantics (ADR-0054 closure):
//
//   - Column present with matching IR type → Applied.
//   - Column present with mismatched IR type → Inconsistent + error.
//   - Column absent → Inconsistent.
//
// Drives the full silent-divergence scenario at the bottom of the test:
// an ALTER COLUMN INT → BIGINT lands, the probe with want=BIGINT
// returns Applied; the column is then manually dropped + re-added with
// the wrong type (TEXT), and the probe with want=BIGINT returns
// Inconsistent with an error naming both types.
func TestShardConsolidationProber_AlterColumnType_V2(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."shape_type_v2" (id INT PRIMARY KEY, amount INT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	eng := Engine{}
	a, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := a.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	applier := a.(*ChangeApplier)
	table := &ir.Table{Schema: "public", Name: "shape_type_v2"}

	// Pre-ALTER: column is INT, want=BIGINT → Inconsistent (the v2
	// type-match catches the mismatch).
	outcome, err := applier.ProbeAlterColumnType(ctx, table, &ir.Column{Name: "amount", Type: ir.Integer{Width: 64}})
	if err == nil {
		t.Fatal("ProbeAlterColumnType: expected error for mismatched type (INT vs BIGINT)")
	}
	if outcome != ir.ProbeOutcomeInconsistent {
		t.Errorf("outcome = %v, want Inconsistent (pre-ALTER mismatch)", outcome)
	}
	// Error message includes both observed (Int32) and want (Int64).
	if !containsAll(err.Error(), "Int64", "Int32") {
		t.Errorf("error message %q should name expected and observed types", err.Error())
	}

	// Land the ALTER: INT → BIGINT.
	if _, err := db.ExecContext(ctx, `ALTER TABLE "public"."shape_type_v2" ALTER COLUMN amount TYPE BIGINT`); err != nil {
		t.Fatalf("ALTER INT→BIGINT: %v", err)
	}

	// Post-ALTER: want=BIGINT now matches → Applied.
	outcome, err = applier.ProbeAlterColumnType(ctx, table, &ir.Column{Name: "amount", Type: ir.Integer{Width: 64}})
	if err != nil {
		t.Fatalf("ProbeAlterColumnType (post-ALTER): %v", err)
	}
	if outcome != ir.ProbeOutcomeApplied {
		t.Errorf("outcome = %v, want Applied (post-ALTER, types match)", outcome)
	}

	// Column absent → Inconsistent.
	outcome, err = applier.ProbeAlterColumnType(ctx, table, &ir.Column{Name: "missing", Type: ir.Integer{Width: 64}})
	if err != nil {
		t.Fatalf("ProbeAlterColumnType (absent): %v", err)
	}
	if outcome != ir.ProbeOutcomeInconsistent {
		t.Errorf("outcome = %v, want Inconsistent (absent)", outcome)
	}

	// Silent-divergence shape: manually drop + re-add with the WRONG
	// type. This is the post-DDL state a crashed lease holder + a
	// bug-induced wrong-type recovery could leave behind; pre-v2 the
	// probe returned Applied (existence-only); v2 surfaces it as
	// Inconsistent loudly.
	if _, err := db.ExecContext(ctx, `ALTER TABLE "public"."shape_type_v2" DROP COLUMN amount`); err != nil {
		t.Fatalf("DROP COLUMN amount: %v", err)
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE "public"."shape_type_v2" ADD COLUMN amount TEXT`); err != nil {
		t.Fatalf("ADD COLUMN amount TEXT: %v", err)
	}
	outcome, err = applier.ProbeAlterColumnType(ctx, table, &ir.Column{Name: "amount", Type: ir.Integer{Width: 64}})
	if err == nil {
		t.Fatal("ProbeAlterColumnType v2: expected error for drop+re-add with wrong type")
	}
	if outcome != ir.ProbeOutcomeInconsistent {
		t.Errorf("outcome = %v, want Inconsistent (drop+re-add wrong type — v2 silent-divergence catch)", outcome)
	}
	// The error message names both types so the operator can recover.
	if !containsAll(err.Error(), "Text", "Int64") {
		t.Errorf("error message %q should name observed Text + expected Int64", err.Error())
	}
}

// containsAll is a tiny multi-substring helper for the test's
// error-message assertions. Returns true when s contains every entry
// in subs. Named -All to disambiguate from the existing single-needle
// `contains` helper in cdc_reader_integration_test.go.
func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		found := false
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// TestShardConsolidationProber_AlterColumnType_V2_NumericUnconstrained
// pins the PG NUMERIC unconstrained vs constrained distinction the v2
// probe must handle correctly (per the ProbeAlterColumnType comment).
func TestShardConsolidationProber_AlterColumnType_V2_NumericUnconstrained(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	// Bare NUMERIC = unconstrained.
	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."shape_num_v2" (id INT PRIMARY KEY, value NUMERIC)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	eng := Engine{}
	a, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := a.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	applier := a.(*ChangeApplier)
	table := &ir.Table{Schema: "public", Name: "shape_num_v2"}

	// Observed unconstrained, want unconstrained → Applied.
	outcome, err := applier.ProbeAlterColumnType(ctx, table, &ir.Column{Name: "value", Type: ir.Decimal{Unconstrained: true}})
	if err != nil {
		t.Fatalf("ProbeAlterColumnType (unconstrained == unconstrained): %v", err)
	}
	if outcome != ir.ProbeOutcomeApplied {
		t.Errorf("outcome = %v, want Applied", outcome)
	}

	// Observed unconstrained, want NUMERIC(10,2) → Inconsistent.
	outcome, err = applier.ProbeAlterColumnType(ctx, table, &ir.Column{Name: "value", Type: ir.Decimal{Precision: 10, Scale: 2}})
	if err == nil {
		t.Fatal("expected error for unconstrained vs NUMERIC(10,2)")
	}
	if outcome != ir.ProbeOutcomeInconsistent {
		t.Errorf("outcome = %v, want Inconsistent", outcome)
	}
}

// TestShardConsolidationProber_CreateDropIndex exercises both Probe
// methods round-trip. CreateShapeIndex lands the index; ProbeCreate-
// Index returns Applied; DropShapeIndex removes it; ProbeDropIndex
// returns Applied. Partial state (mixed present/absent) returns
// Inconsistent.
func TestShardConsolidationProber_CreateDropIndex(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."shape_idx" (id INT PRIMARY KEY, sku TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	eng := Engine{}
	a, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := a.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	applier := a.(*ChangeApplier)
	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() { _ = sw.(*SchemaWriter).Close() }()

	table := &ir.Table{Schema: "public", Name: "shape_idx", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "sku", Type: ir.Text{}},
	}}
	idx := &ir.Index{
		Name:    "ix_sku",
		Columns: []ir.IndexColumn{{Column: "sku"}},
	}

	// Pre-create: NotApplied.
	outcome, err := applier.ProbeCreateIndex(ctx, table, []*ir.Index{idx})
	if err != nil {
		t.Fatalf("ProbeCreateIndex (pre): %v", err)
	}
	if outcome != ir.ProbeOutcomeNotApplied {
		t.Errorf("outcome = %v, want NotApplied (pre-create)", outcome)
	}

	// Apply via the ShapeDeltaApplier.
	pgsw := sw.(*SchemaWriter)
	if err := pgsw.CreateShapeIndex(ctx, table, []*ir.Index{idx}); err != nil {
		t.Fatalf("CreateShapeIndex: %v", err)
	}

	// Post-create: Applied.
	outcome, err = applier.ProbeCreateIndex(ctx, table, []*ir.Index{idx})
	if err != nil {
		t.Fatalf("ProbeCreateIndex (post): %v", err)
	}
	if outcome != ir.ProbeOutcomeApplied {
		t.Errorf("outcome = %v, want Applied (post-create)", outcome)
	}

	// ProbeDropIndex: NotApplied (index still exists).
	outcome, err = applier.ProbeDropIndex(ctx, table, []*ir.Index{idx})
	if err != nil {
		t.Fatalf("ProbeDropIndex (pre-drop): %v", err)
	}
	if outcome != ir.ProbeOutcomeNotApplied {
		t.Errorf("outcome = %v, want NotApplied (pre-drop, index present)", outcome)
	}

	// Drop the index.
	if err := pgsw.DropShapeIndex(ctx, table, []*ir.Index{idx}); err != nil {
		t.Fatalf("DropShapeIndex: %v", err)
	}

	// ProbeDropIndex: Applied (index gone).
	outcome, err = applier.ProbeDropIndex(ctx, table, []*ir.Index{idx})
	if err != nil {
		t.Fatalf("ProbeDropIndex (post-drop): %v", err)
	}
	if outcome != ir.ProbeOutcomeApplied {
		t.Errorf("outcome = %v, want Applied (post-drop, index gone)", outcome)
	}
}

// TestShapeDeltaApplier_RenameColumn_IdempotentRoundtrip pins the
// v0.78.0 task #22 RENAME COLUMN engine path on PG: AlterRenameColumn
// is idempotent on the post-state (a second call after the rename
// landed is a no-op), and the renamed column preserves row data.
func TestShapeDeltaApplier_RenameColumn_IdempotentRoundtrip(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."shape_rename" (
		id INT PRIMARY KEY,
		old_name TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO "public"."shape_rename" (id, old_name) VALUES (1, 'alpha'), (2, 'beta')`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	eng := Engine{}
	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() { _ = sw.(*SchemaWriter).Close() }()

	table := &ir.Table{
		Schema: "public",
		Name:   "shape_rename",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "old_name", Type: ir.Text{}, Nullable: false},
		},
	}
	pgsw := sw.(*SchemaWriter)

	// First rename — fires the actual ALTER.
	if err := pgsw.AlterRenameColumn(ctx, table, "old_name", "new_name"); err != nil {
		t.Fatalf("AlterRenameColumn (1st): %v", err)
	}
	// Idempotent: second call on the post-state is a no-op.
	if err := pgsw.AlterRenameColumn(ctx, table, "old_name", "new_name"); err != nil {
		t.Fatalf("AlterRenameColumn (2nd, idempotent): %v", err)
	}

	// Verify the catalog reflects the rename + row data preserved.
	var hasNew, hasOld int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'shape_rename' AND column_name = 'new_name'`).Scan(&hasNew); err != nil {
		t.Fatalf("scan new_name: %v", err)
	}
	if hasNew != 1 {
		t.Errorf("new_name column missing post-rename")
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'shape_rename' AND column_name = 'old_name'`).Scan(&hasOld); err != nil {
		t.Fatalf("scan old_name: %v", err)
	}
	if hasOld != 0 {
		t.Errorf("old_name column should be absent post-rename")
	}
	// Row data preserved.
	var got string
	if err := db.QueryRowContext(ctx, `SELECT new_name FROM "public"."shape_rename" WHERE id = 1`).Scan(&got); err != nil {
		t.Fatalf("read renamed column: %v", err)
	}
	if got != "alpha" {
		t.Errorf("new_name @ id=1 = %q, want alpha", got)
	}
}

// TestShapeDeltaApplier_RenameColumn_BothPresentRefusesLoudly:
// when both old and new column names exist on the target — a
// partial-recovery state the operator must resolve — the applier
// refuses loudly rather than guessing.
func TestShapeDeltaApplier_RenameColumn_BothPresentRefusesLoudly(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."shape_rename_both" (
		id INT PRIMARY KEY,
		old_name TEXT,
		new_name TEXT
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	eng := Engine{}
	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() { _ = sw.(*SchemaWriter).Close() }()

	table := &ir.Table{Schema: "public", Name: "shape_rename_both"}
	if err := sw.(*SchemaWriter).AlterRenameColumn(ctx, table, "old_name", "new_name"); err == nil {
		t.Fatal("expected refusal when both old and new columns exist")
	}
}

// TestShardConsolidationProber_RenameColumn pins the v0.78.0 task #22
// RENAME COLUMN probe semantics on PG:
//
//   - Pre-rename (oldName present, newName absent) → NotApplied.
//   - Post-rename (newName present, oldName absent, type matches) →
//     Applied.
//   - newName present but with WRONG TYPE → Inconsistent + error.
//   - Both absent → Inconsistent.
func TestShardConsolidationProber_RenameColumn(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."shape_probe_rename" (
		id INT PRIMARY KEY,
		legacy_name TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	eng := Engine{}
	a, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := a.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	applier := a.(*ChangeApplier)
	table := &ir.Table{Schema: "public", Name: "shape_probe_rename"}
	// PG TEXT maps to ir.Text{Size: TextLong} (unbounded), per the
	// Text comment in internal/ir/types.go.
	wantText := &ir.Column{Name: "current_name", Type: ir.Text{Size: ir.TextLong}}

	// Pre-rename: legacy_name present, current_name absent → NotApplied.
	outcome, err := applier.ProbeRenameColumn(ctx, table, "legacy_name", "current_name", wantText)
	if err != nil {
		t.Fatalf("ProbeRenameColumn (pre): %v", err)
	}
	if outcome != ir.ProbeOutcomeNotApplied {
		t.Errorf("outcome = %v, want NotApplied (pre-rename)", outcome)
	}

	// Land the rename.
	if _, err := db.ExecContext(ctx, `ALTER TABLE "public"."shape_probe_rename" RENAME COLUMN legacy_name TO current_name`); err != nil {
		t.Fatalf("apply rename: %v", err)
	}

	// Post-rename: current_name present, legacy_name absent, type
	// matches → Applied.
	outcome, err = applier.ProbeRenameColumn(ctx, table, "legacy_name", "current_name", wantText)
	if err != nil {
		t.Fatalf("ProbeRenameColumn (post): %v", err)
	}
	if outcome != ir.ProbeOutcomeApplied {
		t.Errorf("outcome = %v, want Applied (post-rename)", outcome)
	}

	// WRONG TYPE: want=Integer when observed is Text → Inconsistent +
	// error naming the mismatch. Mirrors the v0.76.0 ProbeAlterColumnType
	// v2 silent-divergence catch on the rename path.
	wantWrongType := &ir.Column{Name: "current_name", Type: ir.Integer{Width: 64}}
	outcome, err = applier.ProbeRenameColumn(ctx, table, "legacy_name", "current_name", wantWrongType)
	if err == nil {
		t.Fatal("expected error on type mismatch")
	}
	if outcome != ir.ProbeOutcomeInconsistent {
		t.Errorf("outcome = %v, want Inconsistent (post-rename wrong type)", outcome)
	}

	// Both absent → Inconsistent.
	outcome, err = applier.ProbeRenameColumn(ctx, table, "no_such_old", "no_such_new", wantText)
	if err == nil {
		t.Fatal("expected error when neither column exists")
	}
	if outcome != ir.ProbeOutcomeInconsistent {
		t.Errorf("outcome = %v, want Inconsistent (both absent)", outcome)
	}
}
