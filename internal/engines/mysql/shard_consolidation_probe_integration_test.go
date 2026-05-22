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
