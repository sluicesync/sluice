//go:build integration

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCreateIndexes_IdempotentOnResume_Bug131 pins that a second
// CreateIndexes pass over a schema whose secondary indexes already exist
// does NOT fail with MySQL 1061 ("Duplicate key name"). The migrate
// phase=indexes path must be idempotent so a resume that re-enters it —
// e.g. after a --include-table/--exclude-table scope change forces an
// earlier phase to re-run, or a partially-completed index phase — doesn't
// abort. MySQL has no CREATE INDEX IF NOT EXISTS, so the writer probes
// information_schema.statistics and skips indexes that already exist.
func TestCreateIndexes_IdempotentOnResume_Bug131(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "bug131_idx_resume")
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
				Name:    "PRIMARY",
				Unique:  true,
				Columns: []ir.IndexColumn{{Column: "id"}},
			},
			Indexes: []*ir.Index{
				{
					Name:    "widgets_sku_unique",
					Unique:  true,
					Columns: []ir.IndexColumn{{Column: "sku"}},
				},
				{
					Name:    "widgets_name_idx",
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
	// used to fail with Error 1061. It must now be a no-op.
	if err := swHandle.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("CreateIndexes (second pass, resume) must be idempotent, got: %v", err)
	}

	// Each secondary index exists exactly once (idempotent, not duplicated).
	sw := swHandle.(*SchemaWriter)
	for _, want := range []string{"widgets_sku_unique", "widgets_name_idx"} {
		var count int
		if err := sw.db.QueryRowContext(
			ctx,
			`SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics
				WHERE table_schema = ? AND table_name = ? AND index_name = ?`,
			sw.schema, "widgets", want,
		).Scan(&count); err != nil {
			t.Fatalf("query information_schema.statistics for %q: %v", want, err)
		}
		if count != 1 {
			t.Errorf("index %q: got count %d, want 1", want, count)
		}
	}
}
