//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// indexNamesOnTable returns the distinct secondary-index names present on a
// table in information_schema.statistics (PRIMARY included if present).
func indexNamesOnTable(ctx context.Context, t *testing.T, sw *SchemaWriter, table string) map[string]bool {
	t.Helper()
	rows, err := sw.db.QueryContext(
		ctx,
		`SELECT DISTINCT index_name FROM information_schema.statistics
			WHERE table_schema = ? AND table_name = ?`,
		sw.schema, table,
	)
	if err != nil {
		t.Fatalf("query information_schema.statistics: %v", err)
	}
	defer func() { _ = rows.Close() }()
	got := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan index_name: %v", err)
		}
		got[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate index_name rows: %v", err)
	}
	return got
}

// TestCreateIndexes_AllKindsLandViaCombinedPath_ADR0080 pins the combined-ALTER
// follow-up's no-silent-loss invariant across the WHOLE index-kind family
// (CLAUDE.md Bug 74 family-matrix discipline): a table carrying regular +
// UNIQUE (the combinable group that collapses into ONE ALTER) plus FULLTEXT
// and SPATIAL (each its own statement — Error 1795 / no-LOCK=NONE) must end up
// with EVERY index present. A wrong grouping would either error loudly (1795 /
// algorithm downgrade) or — the danger this guards — silently drop an index.
func TestCreateIndexes_AllKindsLandViaCombinedPath_ADR0080(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "adr0080_combined_allkinds")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "places",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				{Name: "sku", Type: ir.Varchar{Length: 64}},
				{Name: "name", Type: ir.Varchar{Length: 255}},
				{Name: "body", Type: ir.Text{}},
				// SPATIAL requires a NOT NULL geometry column (Nullable false).
				{Name: "pt", Type: ir.Geometry{Subtype: ir.GeometryPoint}},
			},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
			Indexes: []*ir.Index{
				{Name: "places_name_idx", Kind: ir.IndexKindBTree, Columns: []ir.IndexColumn{{Column: "name"}}},
				{Name: "places_sku_unique", Unique: true, Kind: ir.IndexKindBTree, Columns: []ir.IndexColumn{{Column: "sku"}}},
				{Name: "places_body_ft", Kind: ir.IndexKindFullText, Columns: []ir.IndexColumn{{Column: "body"}}},
				{Name: "places_pt_sp", Kind: ir.IndexKindSpatial, Columns: []ir.IndexColumn{{Column: "pt"}}},
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
		t.Fatalf("CreateIndexes (combined path): %v", err)
	}

	sw := swHandle.(*SchemaWriter)
	got := indexNamesOnTable(ctx, t, sw, "places")
	for _, want := range []string{
		"places_name_idx", "places_sku_unique", "places_body_ft", "places_pt_sp",
	} {
		if !got[want] {
			t.Errorf("index %q missing after combined-path CreateIndexes; present: %v", want, got)
		}
	}

	// Idempotent resume over the full (already-built) set is a no-op.
	if err := swHandle.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("CreateIndexes (resume, full set already present) must be idempotent: %v", err)
	}
}

// TestCreateIndexes_PartialResumeCombinedPath_ADR0080 pins the per-index probe
// inside the combined builder: when SOME of a table's indexes already exist,
// the second pass must build ONLY the missing ones — never re-issue an ADD for
// an existing index (MySQL 1061). This exercises the path where the combined
// ALTER's clause list is the filtered survivor set, not the whole index set.
func TestCreateIndexes_PartialResumeCombinedPath_ADR0080(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "adr0080_combined_partial")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	schema := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "gadgets",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				{Name: "a", Type: ir.Integer{Width: 64}},
				{Name: "b", Type: ir.Integer{Width: 64}},
				{Name: "c", Type: ir.Integer{Width: 64}},
			},
			PrimaryKey: &ir.Index{Name: "PRIMARY", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
			Indexes: []*ir.Index{
				{Name: "gadgets_a_idx", Kind: ir.IndexKindBTree, Columns: []ir.IndexColumn{{Column: "a"}}},
				{Name: "gadgets_b_idx", Kind: ir.IndexKindBTree, Columns: []ir.IndexColumn{{Column: "b"}}},
				{Name: "gadgets_c_idx", Kind: ir.IndexKindBTree, Columns: []ir.IndexColumn{{Column: "c"}}},
			},
		},
	}}

	swHandle, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer closeIf(swHandle)
	sw := swHandle.(*SchemaWriter)

	if err := swHandle.CreateTablesWithoutConstraints(ctx, schema); err != nil {
		t.Fatalf("CreateTablesWithoutConstraints: %v", err)
	}
	if err := swHandle.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("CreateIndexes (first pass): %v", err)
	}

	// Drop one index out from under the writer to simulate a partially
	// completed prior run; the resume must rebuild exactly the missing one and
	// skip the two that survive — no 1061 on the survivors.
	if _, err := sw.db.ExecContext(ctx, "ALTER TABLE `gadgets` DROP INDEX `gadgets_b_idx`"); err != nil {
		t.Fatalf("drop index for partial-resume setup: %v", err)
	}
	if got := indexNamesOnTable(ctx, t, sw, "gadgets"); got["gadgets_b_idx"] {
		t.Fatalf("setup: gadgets_b_idx should be dropped, present: %v", got)
	}

	if err := swHandle.CreateIndexes(ctx, schema); err != nil {
		t.Fatalf("CreateIndexes (partial resume) must skip survivors and rebuild the dropped one: %v", err)
	}

	got := indexNamesOnTable(ctx, t, sw, "gadgets")
	for _, want := range []string{"gadgets_a_idx", "gadgets_b_idx", "gadgets_c_idx"} {
		if !got[want] {
			t.Errorf("index %q missing after partial resume; present: %v", want, got)
		}
	}
}
