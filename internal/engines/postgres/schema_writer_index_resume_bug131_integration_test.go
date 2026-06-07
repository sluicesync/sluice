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
