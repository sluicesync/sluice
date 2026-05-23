//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the ADR-0054 Phase 2c ShapeDeltaApplier +
// ShardConsolidationProber engine impls (MySQL). MySQL's
// idempotency uses detect-then-ALTER (no IF [NOT] EXISTS on older
// 8.0.x); these tests pin the round-trip via a real MySQL container.

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

func TestShapeDeltaApplier_MySQL_AddDropColumn_IdempotentRoundtrip(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE `shape_users` (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	eng := Engine{}
	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() { _ = sw.(*SchemaWriter).Close() }()

	table := &ir.Table{
		Name: "shape_users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
		},
	}
	addCol := &ir.Column{Name: "added_at", Type: ir.Timestamp{}, Nullable: true}

	mysw := sw.(*SchemaWriter)
	if err := mysw.AlterAddColumn(ctx, table, []*ir.Column{addCol}); err != nil {
		t.Fatalf("AlterAddColumn (1st): %v", err)
	}
	if err := mysw.AlterAddColumn(ctx, table, []*ir.Column{addCol}); err != nil {
		t.Fatalf("AlterAddColumn (2nd, idempotent): %v", err)
	}
	if err := mysw.AlterDropColumn(ctx, table, []*ir.Column{addCol}); err != nil {
		t.Fatalf("AlterDropColumn (1st): %v", err)
	}
	if err := mysw.AlterDropColumn(ctx, table, []*ir.Column{addCol}); err != nil {
		t.Fatalf("AlterDropColumn (2nd, idempotent): %v", err)
	}
}

func TestShardConsolidationProber_MySQL_AddColumnAppliedNotApplied(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE `shape_probe` (id INT PRIMARY KEY, added_at TIMESTAMP NULL)"); err != nil {
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
	table := &ir.Table{Name: "shape_probe"}

	outcome, err := applier.ProbeAddColumn(ctx, table, []*ir.Column{{Name: "added_at"}})
	if err != nil {
		t.Fatalf("ProbeAddColumn (added_at): %v", err)
	}
	if outcome != ir.ProbeOutcomeApplied {
		t.Errorf("outcome = %v, want Applied", outcome)
	}

	outcome, err = applier.ProbeAddColumn(ctx, table, []*ir.Column{{Name: "missing_col"}})
	if err != nil {
		t.Fatalf("ProbeAddColumn (missing): %v", err)
	}
	if outcome != ir.ProbeOutcomeNotApplied {
		t.Errorf("outcome = %v, want NotApplied", outcome)
	}

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

// TestShardConsolidationProber_MySQL_AlterColumnNullability pins the
// nullability probe against real MySQL information_schema.
func TestShardConsolidationProber_MySQL_AlterColumnNullability(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE `shape_null` (id INT PRIMARY KEY, n_col INT NULL)"); err != nil {
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
	table := &ir.Table{Name: "shape_null"}

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

// TestShardConsolidationProber_MySQL_AlterColumnType_V2 pins the
// v0.76.0 task #20 IR-type-matching semantics on MySQL (ADR-0054
// closure). v1 was existence-only; v2 catches the silent-divergence
// shape where a column is dropped + re-added with the wrong type.
func TestShardConsolidationProber_MySQL_AlterColumnType_V2(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE `shape_type_v2` (id INT PRIMARY KEY, amount INT)"); err != nil {
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
	table := &ir.Table{Name: "shape_type_v2"}

	// Pre-ALTER: INT column, want BIGINT → Inconsistent.
	outcome, err := applier.ProbeAlterColumnType(ctx, table, &ir.Column{Name: "amount", Type: ir.Integer{Width: 64}})
	if err == nil {
		t.Fatal("ProbeAlterColumnType v2: expected error for INT vs BIGINT")
	}
	if outcome != ir.ProbeOutcomeInconsistent {
		t.Errorf("outcome = %v, want Inconsistent", outcome)
	}

	// Land the ALTER: INT → BIGINT.
	if _, err := db.ExecContext(ctx, "ALTER TABLE `shape_type_v2` MODIFY COLUMN amount BIGINT"); err != nil {
		t.Fatalf("ALTER INT→BIGINT: %v", err)
	}

	// Post-ALTER: want=BIGINT matches → Applied.
	outcome, err = applier.ProbeAlterColumnType(ctx, table, &ir.Column{Name: "amount", Type: ir.Integer{Width: 64}})
	if err != nil {
		t.Fatalf("ProbeAlterColumnType (post-ALTER): %v", err)
	}
	if outcome != ir.ProbeOutcomeApplied {
		t.Errorf("outcome = %v, want Applied", outcome)
	}

	// Drop + re-add with wrong type — v2 silent-divergence catch.
	if _, err := db.ExecContext(ctx, "ALTER TABLE `shape_type_v2` DROP COLUMN amount"); err != nil {
		t.Fatalf("DROP COLUMN: %v", err)
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE `shape_type_v2` ADD COLUMN amount TEXT"); err != nil {
		t.Fatalf("ADD COLUMN amount TEXT: %v", err)
	}
	outcome, err = applier.ProbeAlterColumnType(ctx, table, &ir.Column{Name: "amount", Type: ir.Integer{Width: 64}})
	if err == nil {
		t.Fatal("ProbeAlterColumnType v2: expected error after drop+re-add with TEXT")
	}
	if outcome != ir.ProbeOutcomeInconsistent {
		t.Errorf("outcome = %v, want Inconsistent (drop+re-add wrong type)", outcome)
	}
}

// TestShardConsolidationProber_MySQL_CreateDropIndex round-trips
// CreateShapeIndex + DropShapeIndex against information_schema.
func TestShardConsolidationProber_MySQL_CreateDropIndex(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE `shape_idx` (id INT PRIMARY KEY, sku VARCHAR(64) NOT NULL)"); err != nil {
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

	table := &ir.Table{Name: "shape_idx", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "sku", Type: ir.Varchar{Length: 64}},
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

	mysw := sw.(*SchemaWriter)
	if err := mysw.CreateShapeIndex(ctx, table, []*ir.Index{idx}); err != nil {
		t.Fatalf("CreateShapeIndex: %v", err)
	}

	outcome, err = applier.ProbeCreateIndex(ctx, table, []*ir.Index{idx})
	if err != nil {
		t.Fatalf("ProbeCreateIndex (post): %v", err)
	}
	if outcome != ir.ProbeOutcomeApplied {
		t.Errorf("outcome = %v, want Applied (post-create)", outcome)
	}

	if err := mysw.DropShapeIndex(ctx, table, []*ir.Index{idx}); err != nil {
		t.Fatalf("DropShapeIndex: %v", err)
	}

	outcome, err = applier.ProbeDropIndex(ctx, table, []*ir.Index{idx})
	if err != nil {
		t.Fatalf("ProbeDropIndex (post-drop): %v", err)
	}
	if outcome != ir.ProbeOutcomeApplied {
		t.Errorf("outcome = %v, want Applied (post-drop)", outcome)
	}
}

// TestShapeDeltaApplier_MySQL_RenameColumn_IdempotentRoundtrip pins
// the v0.78.0 task #22 RENAME COLUMN engine path on MySQL:
// AlterRenameColumn is idempotent on the post-state and the renamed
// column preserves row data.
func TestShapeDeltaApplier_MySQL_RenameColumn_IdempotentRoundtrip(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE `shape_rename` (id INT PRIMARY KEY, old_name VARCHAR(64) NOT NULL)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO `shape_rename` (id, old_name) VALUES (1, 'alpha'), (2, 'beta')"); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	eng := Engine{}
	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() { _ = sw.(*SchemaWriter).Close() }()

	table := &ir.Table{Name: "shape_rename"}
	mysw := sw.(*SchemaWriter)

	// First rename — fires the actual ALTER.
	if err := mysw.AlterRenameColumn(ctx, table, "old_name", "new_name"); err != nil {
		t.Fatalf("AlterRenameColumn (1st): %v", err)
	}
	// Idempotent: second call on the post-state is a no-op.
	if err := mysw.AlterRenameColumn(ctx, table, "old_name", "new_name"); err != nil {
		t.Fatalf("AlterRenameColumn (2nd, idempotent): %v", err)
	}

	// Verify the catalog reflects the rename + row data preserved.
	var hasNew, hasOld int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'shape_rename' AND column_name = 'new_name'`).Scan(&hasNew); err != nil {
		t.Fatalf("scan new_name: %v", err)
	}
	if hasNew != 1 {
		t.Errorf("new_name column missing post-rename")
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = 'shape_rename' AND column_name = 'old_name'`).Scan(&hasOld); err != nil {
		t.Fatalf("scan old_name: %v", err)
	}
	if hasOld != 0 {
		t.Errorf("old_name column should be absent post-rename")
	}
	var got string
	if err := db.QueryRowContext(ctx, "SELECT new_name FROM `shape_rename` WHERE id = 1").Scan(&got); err != nil {
		t.Fatalf("read renamed column: %v", err)
	}
	if got != "alpha" {
		t.Errorf("new_name @ id=1 = %q, want alpha", got)
	}
}

// TestShapeDeltaApplier_MySQL_RenameColumn_BothPresentRefusesLoudly:
// when both old and new column names exist on the target, the
// applier refuses loudly.
func TestShapeDeltaApplier_MySQL_RenameColumn_BothPresentRefusesLoudly(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE `shape_rename_both` (id INT PRIMARY KEY, old_name VARCHAR(64), new_name VARCHAR(64))"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	eng := Engine{}
	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() { _ = sw.(*SchemaWriter).Close() }()

	table := &ir.Table{Name: "shape_rename_both"}
	if err := sw.(*SchemaWriter).AlterRenameColumn(ctx, table, "old_name", "new_name"); err == nil {
		t.Fatal("expected refusal when both old and new columns exist")
	}
}

// TestShardConsolidationProber_MySQL_RenameColumn pins the v0.78.0
// task #22 RENAME COLUMN probe on MySQL.
func TestShardConsolidationProber_MySQL_RenameColumn(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	// INT columns avoid the MySQL Varchar Charset/Collation
	// projection edge that the v0.76.0 MySQL ProbeAlterColumnType v2
	// test sidesteps the same way (see Varchar charset normalizer
	// note in shard_consolidation_probe.go).
	if _, err := db.ExecContext(ctx, "CREATE TABLE `shape_probe_rename` (id INT PRIMARY KEY, legacy_count INT NOT NULL)"); err != nil {
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
	table := &ir.Table{Name: "shape_probe_rename"}
	wantInt := &ir.Column{Name: "current_count", Type: ir.Integer{Width: 32}}

	// Pre-rename: legacy_count present, current_count absent → NotApplied.
	outcome, err := applier.ProbeRenameColumn(ctx, table, "legacy_count", "current_count", wantInt)
	if err != nil {
		t.Fatalf("ProbeRenameColumn (pre): %v", err)
	}
	if outcome != ir.ProbeOutcomeNotApplied {
		t.Errorf("outcome = %v, want NotApplied (pre-rename)", outcome)
	}

	// Land the rename.
	if _, err := db.ExecContext(ctx, "ALTER TABLE `shape_probe_rename` RENAME COLUMN legacy_count TO current_count"); err != nil {
		t.Fatalf("apply rename: %v", err)
	}

	// Post-rename: types match → Applied.
	outcome, err = applier.ProbeRenameColumn(ctx, table, "legacy_count", "current_count", wantInt)
	if err != nil {
		t.Fatalf("ProbeRenameColumn (post): %v", err)
	}
	if outcome != ir.ProbeOutcomeApplied {
		t.Errorf("outcome = %v, want Applied (post-rename)", outcome)
	}

	// Wrong type → Inconsistent + error.
	wantWrongType := &ir.Column{Name: "current_count", Type: ir.Integer{Width: 64}}
	outcome, err = applier.ProbeRenameColumn(ctx, table, "legacy_count", "current_count", wantWrongType)
	if err == nil {
		t.Fatal("expected error on type mismatch")
	}
	if outcome != ir.ProbeOutcomeInconsistent {
		t.Errorf("outcome = %v, want Inconsistent (post-rename wrong type)", outcome)
	}

	// Both absent → Inconsistent.
	outcome, err = applier.ProbeRenameColumn(ctx, table, "no_such_old", "no_such_new", wantInt)
	if err == nil {
		t.Fatal("expected error when neither column exists")
	}
	if outcome != ir.ProbeOutcomeInconsistent {
		t.Errorf("outcome = %v, want Inconsistent (both absent)", outcome)
	}
}
