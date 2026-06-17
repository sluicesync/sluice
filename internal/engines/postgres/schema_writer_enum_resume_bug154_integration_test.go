//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 154 integration pins: the PG cold-start enum CREATE TYPE must be
// idempotent so a resumed/restarted cold-start (interrupted after the
// CREATE but before the migration committed) re-runs cleanly instead of
// crash-looping on SQLSTATE 42710 "type already exists" — and a
// --reset-target-data must drop the synthesized enum TYPES too (the
// parity gap), including a PG-source type carried by name (Bug 19c), so
// a reset leaves no orphan to re-trigger 42710 on the next fresh run.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSchemaWriter_ColdStartEnum_IsIdempotent_Bug154 simulates a resume:
// CreateTablesWithoutConstraints runs once (creating the enum type), then
// runs AGAIN against the same target — the exact shape of a cold-start
// interrupted mid-flight and restarted. Pre-fix the second run failed the
// bare CREATE TYPE with 42710; post-fix the DO-block guard makes it a
// no-op and the call completes.
func TestSchemaWriter_ColdStartEnum_IsIdempotent_Bug154(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	swIface, err := (Engine{}).OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() {
		if c, ok := swIface.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	sw := swIface.(*SchemaWriter)

	mkSchema := func() *ir.Schema {
		return &ir.Schema{Tables: []*ir.Table{{
			Name: "orders",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				// MySQL-source shape (no TypeName → synthesized name).
				{Name: "status", Type: ir.Enum{Values: []string{"open", "shipped", "closed"}}},
				// PG-source shape (carries TypeName → verbatim, Bug 19c).
				{Name: "tier", Type: ir.Enum{Values: []string{"free", "pro"}, TypeName: "account_tier"}},
			},
			PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		}}}
	}

	// First cold-start pass.
	if err := sw.CreateTablesWithoutConstraints(ctx, mkSchema()); err != nil {
		t.Fatalf("first CreateTablesWithoutConstraints: %v", err)
	}

	// Simulate the interrupted-then-resumed restart: the same phase runs
	// again. CREATE TABLE is already IF NOT EXISTS; the enum CREATE TYPE
	// is the part that used to 42710. Must succeed (the bug repro).
	if err := sw.CreateTablesWithoutConstraints(ctx, mkSchema()); err != nil {
		t.Fatalf("Bug 154: resumed CreateTablesWithoutConstraints failed (expected idempotent): %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, typ := range []string{"orders_status_enum", "account_tier"} {
		if !typeExists(t, ctx, db, typ) {
			t.Errorf("enum type %q missing after resumed cold-start", typ)
		}
	}
}

// TestRowWriter_DropSchemaTypes_DropsBothNamedAndSynthesized_Bug154 pins
// that --reset-target-data drops BOTH the synthesized <table>_<col>_enum
// type AND a PG-source named type — the old code dropped only the
// synthesized name, leaving the named type orphaned to re-trigger 42710
// on the next fresh cold-start.
func TestRowWriter_DropSchemaTypes_DropsBothNamedAndSynthesized_Bug154(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "status", Type: ir.Enum{Values: []string{"open", "closed"}}},
			{Name: "tier", Type: ir.Enum{Values: []string{"free", "pro"}, TypeName: "account_tier"}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}}}

	swIface, err := (Engine{}).OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() {
		if c, ok := swIface.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := swIface.(*SchemaWriter).CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open verify db: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, typ := range []string{"orders_status_enum", "account_tier"} {
		if !typeExists(t, ctx, db, typ) {
			t.Fatalf("enum type %q missing before reset", typ)
		}
	}

	rwIface, err := (Engine{}).OpenRowWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowWriter: %v", err)
	}
	defer func() {
		if c, ok := rwIface.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	dropper, ok := rwIface.(ir.SchemaTypeDropper)
	if !ok {
		t.Fatalf("PG RowWriter does not implement ir.SchemaTypeDropper")
	}

	// Drop the table first (column references the type), then the types —
	// the same order the reset path uses.
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS "public"."orders" CASCADE`); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if err := dropper.DropSchemaTypes(ctx, schema); err != nil {
		t.Fatalf("DropSchemaTypes: %v", err)
	}

	for _, typ := range []string{"orders_status_enum", "account_tier"} {
		if typeExists(t, ctx, db, typ) {
			t.Errorf("Bug 154: enum type %q left orphaned after reset; want it dropped", typ)
		}
	}

	// And the reset must be idempotent: a fresh cold-start now succeeds
	// (no 42710 from a leftover type).
	if err := swIface.(*SchemaWriter).CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("Bug 154: fresh cold-start after reset failed: %v", err)
	}
}
