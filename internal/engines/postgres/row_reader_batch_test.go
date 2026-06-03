// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestBuildBatchedSelect_SinglePK_FirstBatch confirms the no-cursor
// shape: SELECT cols FROM tbl ORDER BY pk LIMIT N.
func TestBuildBatchedSelect_SinglePK_FirstBatch(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{Name: "pk_users", Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	got := buildBatchedSelect("public", table, 5000, false)
	want := `SELECT "id", "email" FROM "public"."users" ORDER BY "id" LIMIT 5000`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestBuildBatchedSelect_SinglePK_WithCursor confirms the row-
// comparison form for a single-column PK still uses tuple notation
// (a row of one is degenerate but legal SQL).
func TestBuildBatchedSelect_SinglePK_WithCursor(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{Name: "pk_users", Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	got := buildBatchedSelect("public", table, 5000, true)
	want := `SELECT "id", "email" FROM "public"."users" WHERE ("id") > ($1) ORDER BY "id" LIMIT 5000`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestBuildBatchedSelect_CompositePK confirms the row-comparison
// predicate descends into composite PKs correctly. The tuple in WHERE
// matches the ORDER BY tuple and the placeholder count.
func TestBuildBatchedSelect_CompositePK(t *testing.T) {
	table := &ir.Table{
		Name: "products",
		Columns: []*ir.Column{
			{Name: "tenant", Type: ir.Varchar{Length: 32}},
			{Name: "sku", Type: ir.Integer{Width: 64}},
			{Name: "name", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{
			Name: "pk_products",
			Columns: []ir.IndexColumn{
				{Column: "tenant"},
				{Column: "sku"},
			},
		},
	}
	got := buildBatchedSelect("public", table, 1000, true)
	want := `SELECT "tenant", "sku", "name" FROM "public"."products" WHERE ("tenant", "sku") > ($1, $2) ORDER BY "tenant", "sku" LIMIT 1000`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestReadRowsBatch_RejectsNoPK confirms the defensive error path:
// the orchestrator routes no-PK tables away from the cursor path,
// but if a caller calls ReadRowsBatch directly on a no-PK table the
// error must be clear, not a malformed SQL statement.
func TestReadRowsBatch_RejectsNoPK(t *testing.T) {
	r := &RowReader{schema: "public"}
	table := &ir.Table{
		Name:    "events",
		Columns: []*ir.Column{{Name: "data", Type: ir.Text{}}},
	}
	_, err := r.ReadRowsBatch(t.Context(), table, nil, 100)
	if err == nil {
		t.Fatal("ReadRowsBatch on no-PK table succeeded; want error")
	}
	if !strings.Contains(err.Error(), "no primary key") {
		t.Errorf("err = %v; want 'no primary key' wording", err)
	}
}

// TestReadRowsBatch_RejectsBadCursorLength confirms that an after
// slice with the wrong number of values surfaces a clear error
// rather than landing as a bad SQL placeholder count.
func TestReadRowsBatch_RejectsBadCursorLength(t *testing.T) {
	r := &RowReader{schema: "public"}
	table := &ir.Table{
		Name:    "products",
		Columns: []*ir.Column{{Name: "tenant", Type: ir.Varchar{Length: 32}}, {Name: "sku", Type: ir.Integer{Width: 64}}},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{
			{Column: "tenant"}, {Column: "sku"},
		}},
	}
	// Pass one cursor value to a 2-PK table.
	_, err := r.ReadRowsBatch(t.Context(), table, []any{"a"}, 100)
	if err == nil {
		t.Fatal("ReadRowsBatch with wrong cursor length succeeded; want error")
	}
}
