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
