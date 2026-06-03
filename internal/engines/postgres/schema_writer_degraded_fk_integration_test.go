// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// degradedFKSeed is the IR shape both phases of the test share:
// parent (PK id) and child (PK id, FK parent_id → parent.id). The
// writer's CreateConstraints attaches the FK after the row data is in
// place, which is exactly when SQLSTATE 23503 fires for an orphan
// child row.
func degradedFKSeedSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{
			Name: "parent",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
			},
			PrimaryKey: &ir.Index{
				Unique:  true,
				Columns: []ir.IndexColumn{{Column: "id"}},
			},
		},
		{
			Name: "child",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "parent_id", Type: ir.Integer{Width: 64}},
			},
			PrimaryKey: &ir.Index{
				Unique:  true,
				Columns: []ir.IndexColumn{{Column: "id"}},
			},
			ForeignKeys: []*ir.ForeignKey{
				{
					Name:              "child_parent_fkey",
					Columns:           []string{"parent_id"},
					ReferencedTable:   "parent",
					ReferencedColumns: []string{"id"},
				},
			},
		},
	}}
}

// TestSchemaWriter_CreateConstraints_DegradedFK_OffByDefault confirms
// the loud-failure baseline: with --allow-degraded-fks NOT set, an FK
// that would validate against an orphan row in the child must surface
// the PG 23503 error unchanged. This is the on-tenet default.
func TestSchemaWriter_CreateConstraints_DegradedFK_OffByDefault(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schema := degradedFKSeedSchema()
	sw, err := (Engine{}).OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() {
		if c, ok := sw.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}
	// Insert an orphan: child.parent_id = 99 with no matching parent.
	if err := execOrFail(ctx, dsn, `INSERT INTO child (id, parent_id) VALUES (1, 99);`); err != nil {
		t.Fatalf("insert orphan: %v", err)
	}

	err = sw.CreateConstraints(ctx, schema)
	if err == nil {
		t.Fatal("CreateConstraints unexpectedly succeeded; want the 23503 hard fail (loud-failure default)")
	}
	if !strings.Contains(err.Error(), "23503") && !strings.Contains(err.Error(), "violates foreign key") {
		t.Errorf("CreateConstraints err = %v; want it to mention 23503 / foreign key violation", err)
	}
}

// TestSchemaWriter_CreateConstraints_DegradedFK_NotValidRetry pins
// the pgcopydb-PR-#27-equivalent retry path. When the operator opts
// into degraded FKs (via the ir.DegradedFKAllower optional interface),
// CreateConstraints catches the 23503 from the validating ADD
// CONSTRAINT, retries the same DDL with NOT VALID appended, and
// records the constraint in the report list. The constraint exists on
// the target (`pg_constraint.convalidated = false`) so subsequent
// writes are still enforced; the operator validates later with
// `ALTER TABLE ... VALIDATE CONSTRAINT <name>` after fixing the
// orphans.
func TestSchemaWriter_CreateConstraints_DegradedFK_NotValidRetry(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schema := degradedFKSeedSchema()
	sw, err := (Engine{}).OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() {
		if c, ok := sw.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// Opt in via the optional interface — same path the pipeline
	// orchestrator uses when threading --allow-degraded-fks.
	allower, ok := sw.(ir.DegradedFKAllower)
	if !ok {
		t.Fatal("postgres SchemaWriter does not implement ir.DegradedFKAllower (regression of the contract this PR introduces)")
	}
	allower.EnableDegradedFKs()

	if err := sw.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}
	if err := execOrFail(ctx, dsn, `INSERT INTO child (id, parent_id) VALUES (1, 99);`); err != nil {
		t.Fatalf("insert orphan: %v", err)
	}

	if err := sw.CreateConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateConstraints (with EnableDegradedFKs): %v\n"+
			"want the 23503 to be caught + retried as NOT VALID, not surfaced as an error", err)
	}

	// Report shape: exactly one degraded FK, on the child table,
	// with the actionable Hint pointing at VALIDATE CONSTRAINT.
	reporter, ok := sw.(ir.DegradedFKReporter)
	if !ok {
		t.Fatal("postgres SchemaWriter does not implement ir.DegradedFKReporter (regression of the contract this PR introduces)")
	}
	got := reporter.DegradedFKs()
	if len(got) != 1 {
		t.Fatalf("DegradedFKs len = %d; want 1; got=%+v", len(got), got)
	}
	d := got[0]
	if d.Table != "child" {
		t.Errorf("DegradedFK.Table = %q; want %q", d.Table, "child")
	}
	if d.ConstraintName != "child_parent_fkey" {
		t.Errorf("DegradedFK.ConstraintName = %q; want %q", d.ConstraintName, "child_parent_fkey")
	}
	if d.ReferencedTable != "parent" {
		t.Errorf("DegradedFK.ReferencedTable = %q; want %q", d.ReferencedTable, "parent")
	}
	if !strings.Contains(d.Hint, "VALIDATE CONSTRAINT") {
		t.Errorf("DegradedFK.Hint should mention VALIDATE CONSTRAINT; got %q", d.Hint)
	}
	if !strings.Contains(d.Reason, "23503") && !strings.Contains(d.Reason, "violates foreign key") {
		t.Errorf("DegradedFK.Reason should carry the original 23503 / FK-violation text; got %q", d.Reason)
	}

	// Target shape: the FK exists on pg_constraint and is marked
	// NOT VALIDATED (`convalidated = false`). New writes that would
	// orphan a row are still rejected by PG, so the constraint is
	// real — only the historical-row scan is deferred.
	var convalidated bool
	if err := scalarBool(
		ctx, dsn,
		`SELECT convalidated FROM pg_constraint WHERE conname = 'child_parent_fkey'`,
		&convalidated,
	); err != nil {
		t.Fatalf("query pg_constraint.convalidated: %v", err)
	}
	if convalidated {
		t.Error("pg_constraint.convalidated = true; want false (the FK should be NOT VALID after the retry)")
	}
}

// execOrFail runs a one-shot SQL on the given DSN. Test-only helper —
// the package's other integration helpers use a similar pattern; this
// is local to keep the dependency surface tight.
func execOrFail(ctx context.Context, dsn, stmt string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	_, err = db.ExecContext(ctx, stmt)
	return err
}

// scalarBool reads one boolean from a one-row query. Same rationale
// as execOrFail.
func scalarBool(ctx context.Context, dsn, query string, dst *bool) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return db.QueryRowContext(ctx, query).Scan(dst)
}
