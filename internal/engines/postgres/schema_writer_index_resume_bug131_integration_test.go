//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCreateIndexes_IdempotentOnResume_Bug131 pins that a second
// CreateIndexes pass over a schema whose secondary indexes already exist
// does NOT fail. The migrate phase=indexes path must be idempotent so a
// resume that re-enters it — e.g. after a --include-table/--exclude-table
// scope change forces an earlier phase to re-run, or a partially-completed
// index phase — doesn't abort with "relation already exists". The PG path
// promotes CREATE INDEX to the IF NOT EXISTS form.
func TestCreateIndexes_IdempotentOnResume_Bug131(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "widgets",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				{Name: "sku", Type: ir.Varchar{Length: 64}},
				{Name: "name", Type: ir.Varchar{Length: 255}},
			},
			PrimaryKey: &ir.Index{
				Name:    "widgets_pkey",
				Unique:  true,
				Columns: []ir.IndexColumn{{Column: "id"}},
			},
			Indexes: []*ir.Index{
				{
					Name:    "widgets_sku_unique",
					Unique:  true,
					Kind:    ir.IndexKindBTree,
					Columns: []ir.IndexColumn{{Column: "sku"}},
				},
				{
					Name:    "widgets_name_idx",
					Kind:    ir.IndexKindBTree,
					Columns: []ir.IndexColumn{{Column: "name"}},
				},
			},
		},
	}}

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(swHandle)

	if err := swHandle.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}
	if err := swHandle.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("CreateIndexes (first pass): %v", err)
	}
	// The Bug 131 reproducer: a second pass over the already-built indexes
	// used to fail. It must now be a no-op.
	if err := swHandle.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("CreateIndexes (second pass, resume) must be idempotent, got: %v", err)
	}

	// Each secondary index exists exactly once (idempotent, not duplicated).
	sw := swHandle.(*SchemaWriter)
	for _, want := range []string{"widgets_sku_unique", "widgets_name_idx"} {
		var count int
		if err := sw.db.QueryRowContext(
			ctx,
			"SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND indexname = $1",
			want,
		).Scan(&count); err != nil {
			t.Fatalf("query pg_indexes for %q: %v", want, err)
		}
		if count != 1 {
			t.Errorf("index %q: got count %d, want 1", want, count)
		}
	}
}

// TestCreateConstraints_IdempotentOnResume_Bug131 pins the same-class FK
// follow-up: a second CreateConstraints pass over an already-added foreign
// key must be a no-op, not a "constraint already exists" error — the
// constraints phase must be idempotent on resume for the same reasons the
// index phase must (Bug 131).
func TestCreateConstraints_IdempotentOnResume_Bug131(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name:       "parent",
			Columns:    []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}}},
			PrimaryKey: &ir.Index{Name: "parent_pkey", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		},
		{
			Name: "child",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				{Name: "parent_id", Type: ir.Integer{Width: 64}},
			},
			PrimaryKey: &ir.Index{Name: "child_pkey", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
			ForeignKeys: []*ir.ForeignKey{{
				Name:              "fk_child_parent",
				Columns:           []string{"parent_id"},
				ReferencedTable:   "parent",
				ReferencedColumns: []string{"id"},
			}},
		},
	}}

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(swHandle)

	if err := swHandle.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}
	if err := swHandle.CreateConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateConstraints (first pass): %v", err)
	}
	// The same-class reproducer: a second pass over the already-added FK
	// used to fail with "constraint already exists". It must now be a no-op.
	if err := swHandle.CreateConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateConstraints (second pass, resume) must be idempotent, got: %v", err)
	}

	sw := swHandle.(*SchemaWriter)
	var count int
	if err := sw.db.QueryRowContext(
		ctx,
		`SELECT count(*) FROM pg_constraint c
			JOIN pg_class t     ON t.oid = c.conrelid
			JOIN pg_namespace n ON n.oid = t.relnamespace
			WHERE c.contype = 'f' AND c.conname = 'fk_child_parent'
			  AND t.relname = 'child' AND n.nspname = 'public'`,
	).Scan(&count); err != nil {
		t.Fatalf("query pg_constraint: %v", err)
	}
	if count != 1 {
		t.Errorf("FK fk_child_parent: got count %d, want 1", count)
	}
}
