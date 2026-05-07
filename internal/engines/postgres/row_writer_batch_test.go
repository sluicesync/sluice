// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestBuildBatchUpsert_SinglePK confirms the upsert form for a
// single-PK table: the conflict target is the bare PK column, the
// SET list covers every non-PK column.
func TestBuildBatchUpsert_SinglePK(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
			{Name: "name", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
	}
	pk := primaryKeyColumns(table)
	got := buildBatchUpsert("public", table, 2, pk)
	want := `INSERT INTO "public"."users" ("id", "email", "name") VALUES ($1, $2, $3), ($4, $5, $6) ON CONFLICT ("id") DO UPDATE SET "email" = EXCLUDED."email", "name" = EXCLUDED."name"`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestBuildBatchUpsert_CompositePK confirms the conflict target is
// the multi-column PK tuple; SET still covers non-PK columns only.
func TestBuildBatchUpsert_CompositePK(t *testing.T) {
	table := &ir.Table{
		Name: "products",
		Columns: []*ir.Column{
			{Name: "tenant", Type: ir.Varchar{Length: 32}},
			{Name: "sku", Type: ir.Integer{Width: 64}},
			{Name: "name", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{
			{Column: "tenant"}, {Column: "sku"},
		}},
	}
	pk := primaryKeyColumns(table)
	got := buildBatchUpsert("public", table, 1, pk)
	want := `INSERT INTO "public"."products" ("tenant", "sku", "name") VALUES ($1, $2, $3) ON CONFLICT ("tenant", "sku") DO UPDATE SET "name" = EXCLUDED."name"`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestBuildBatchUpsert_AllPKColumns confirms the DO NOTHING fallback
// when every column is part of the PK (nothing to update on conflict).
func TestBuildBatchUpsert_AllPKColumns(t *testing.T) {
	table := &ir.Table{
		Name: "tags",
		Columns: []*ir.Column{
			{Name: "tenant", Type: ir.Varchar{Length: 32}},
			{Name: "tag", Type: ir.Varchar{Length: 64}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{
			{Column: "tenant"}, {Column: "tag"},
		}},
	}
	pk := primaryKeyColumns(table)
	got := buildBatchUpsert("public", table, 1, pk)
	if !strings.Contains(got, "DO NOTHING") {
		t.Errorf("got %q; want 'DO NOTHING' fallback", got)
	}
}

// TestBuildBatchUpsert_NoPK confirms the plain-INSERT fallback when
// pkCols is empty. The orchestrator routes no-PK tables to truncate-
// and-redo, but the helper keeps the fallback for direct callers.
func TestBuildBatchUpsert_NoPK(t *testing.T) {
	table := &ir.Table{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "ts", Type: ir.Timestamp{}},
			{Name: "data", Type: ir.Text{}},
		},
	}
	got := buildBatchUpsert("public", table, 1, nil)
	if strings.Contains(got, "ON CONFLICT") {
		t.Errorf("expected plain INSERT for no-PK table; got %q", got)
	}
}

// TestPrimaryKeyColumns confirms the extraction shape — declaration
// order is preserved, nil PK returns nil.
func TestPrimaryKeyColumns(t *testing.T) {
	table := &ir.Table{
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{
			{Column: "a"}, {Column: "b"}, {Column: "c"},
		}},
	}
	got := primaryKeyColumns(table)
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Errorf("primaryKeyColumns: got %v; want [a b c]", got)
	}

	if primaryKeyColumns(&ir.Table{}) != nil {
		t.Error("primaryKeyColumns: expected nil for table without PK")
	}
}
