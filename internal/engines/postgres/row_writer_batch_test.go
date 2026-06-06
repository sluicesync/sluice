// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
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

// TestBuildBatchUpsert_NoKeyColsFallsBackToPlainInsert confirms the
// plain-INSERT fallback when keyCols is empty (defensive path; the
// idempotent writer refuses keyless tables before reaching here,
// Bug 125).
func TestBuildBatchUpsert_NoKeyColsFallsBackToPlainInsert(t *testing.T) {
	table := &ir.Table{
		Name: "events",
		Columns: []*ir.Column{
			{Name: "ts", Type: ir.Timestamp{}},
			{Name: "data", Type: ir.Text{}},
		},
	}
	got := buildBatchUpsert("public", table, 1, nil)
	if strings.Contains(got, "ON CONFLICT") {
		t.Errorf("expected plain INSERT for empty keyCols; got %q", got)
	}
}

// TestBuildBatchUpsert_NonNullUniqueKey is the Bug 125 PG pin: a
// PK-less table keyed on a non-null UNIQUE index produces an
// ON CONFLICT (key) DO UPDATE that excludes the key columns from the
// SET list, so VStream COPY catchup re-emissions upsert against the
// inline-promoted UNIQUE constraint instead of colliding.
func TestBuildBatchUpsert_NonNullUniqueKey(t *testing.T) {
	table := &ir.Table{
		Name: "connections",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "payload", Type: ir.Text{}},
		},
		Indexes: []*ir.Index{
			{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		},
	}
	keyCols, ok := effectiveUpsertKeyColumns(table)
	if !ok {
		t.Fatal("effectiveUpsertKeyColumns: ok=false; want a non-null unique key")
	}
	got := buildBatchUpsert("public", table, 1, keyCols)
	want := `INSERT INTO "public"."connections" ("id", "payload") VALUES ($1, $2) ON CONFLICT ("id") DO UPDATE SET "payload" = EXCLUDED."payload"`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestBuildBatchUpsert_NonNullUniqueAllKeyCols confirms the DO NOTHING
// fallback when every non-generated column is part of the chosen
// non-null UNIQUE key (nothing to update on conflict).
func TestBuildBatchUpsert_NonNullUniqueAllKeyCols(t *testing.T) {
	table := &ir.Table{
		Name: "pairs",
		Columns: []*ir.Column{
			{Name: "a", Type: ir.Integer{Width: 32}, Nullable: false},
			{Name: "b", Type: ir.Integer{Width: 32}, Nullable: false},
		},
		Indexes: []*ir.Index{
			{Name: "uq_ab", Unique: true, Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}},
		},
	}
	keyCols, ok := effectiveUpsertKeyColumns(table)
	if !ok {
		t.Fatal("effectiveUpsertKeyColumns: ok=false; want the composite non-null unique key")
	}
	got := buildBatchUpsert("public", table, 1, keyCols)
	want := `INSERT INTO "public"."pairs" ("a", "b") VALUES ($1, $2) ON CONFLICT ("a", "b") DO NOTHING`
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestEffectiveUpsertKeyColumns is the (a)-(g) class matrix (Bug 125,
// PG side). Pins the selection rule against every shape so the
// representative-vs-class trap (CLAUDE.md "pin the class") can't hide a
// gap: PK precedence, single/composite non-null unique, nullable-unique
// rejection, keyless, expression-only rejection, and the deterministic
// tie-break.
func TestEffectiveUpsertKeyColumns(t *testing.T) {
	cases := []struct {
		name    string
		table   *ir.Table
		wantOK  bool
		wantKey []string
	}{
		{
			// (a) PK present -> PK cols (wins over a unique index).
			name: "pk wins over unique",
			table: &ir.Table{
				Columns:    []*ir.Column{{Name: "id", Nullable: false}, {Name: "u", Nullable: false}},
				PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
				Indexes:    []*ir.Index{{Name: "uq_u", Unique: true, Columns: []ir.IndexColumn{{Column: "u"}}}},
			},
			wantOK:  true,
			wantKey: []string{"id"},
		},
		{
			// (b) no-PK single-col NOT-NULL unique -> that col.
			name: "non-null unique when no pk",
			table: &ir.Table{
				Columns: []*ir.Column{{Name: "id", Nullable: false}},
				Indexes: []*ir.Index{{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}}},
			},
			wantOK:  true,
			wantKey: []string{"id"},
		},
		{
			// (c) no-PK composite NOT-NULL unique -> those cols, in order.
			name: "composite non-null unique",
			table: &ir.Table{
				Columns: []*ir.Column{{Name: "a", Nullable: false}, {Name: "b", Nullable: false}},
				Indexes: []*ir.Index{{Name: "uq_ab", Unique: true, Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}}},
			},
			wantOK:  true,
			wantKey: []string{"a", "b"},
		},
		{
			// (d) no-PK only a NULLABLE unique -> ok=false (PG NULLS
			// DISTINCT lets duplicate NULLs slip past — same hazard as
			// no key).
			name: "nullable unique does NOT qualify",
			table: &ir.Table{
				Columns: []*ir.Column{{Name: "id", Nullable: true}},
				Indexes: []*ir.Index{{Name: "uq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}}},
			},
			wantOK: false,
		},
		{
			// (e) truly keyless -> ok=false.
			name: "truly keyless",
			table: &ir.Table{
				Columns: []*ir.Column{{Name: "a", Nullable: false}, {Name: "b", Nullable: true}},
			},
			wantOK: false,
		},
		{
			// (f) expression-only unique -> ok=false (can't be a stable
			// ON CONFLICT arbiter).
			name: "expression-only unique does NOT qualify",
			table: &ir.Table{
				Columns: []*ir.Column{{Name: "email", Nullable: false}},
				Indexes: []*ir.Index{{Name: "uq_lower", Unique: true, Columns: []ir.IndexColumn{{Expression: "lower(email)"}}}},
			},
			wantOK: false,
		},
		{
			// (g) tie-break: two non-null unique indexes -> fewest cols,
			// then lexicographically smallest name.
			name: "fewest columns then name wins",
			table: &ir.Table{
				Columns: []*ir.Column{{Name: "a", Nullable: false}, {Name: "b", Nullable: false}, {Name: "c", Nullable: false}},
				Indexes: []*ir.Index{
					{Name: "uq_ab", Unique: true, Columns: []ir.IndexColumn{{Column: "a"}, {Column: "b"}}},
					{Name: "uq_z", Unique: true, Columns: []ir.IndexColumn{{Column: "c"}}},
					{Name: "uq_a", Unique: true, Columns: []ir.IndexColumn{{Column: "a"}}},
				},
			},
			wantOK:  true,
			wantKey: []string{"a"}, // uq_a and uq_z both single-col; uq_a < uq_z.
		},
		{
			// non-unique index does NOT qualify (only UNIQUE indexes are
			// eligible).
			name: "non-unique index does NOT qualify",
			table: &ir.Table{
				Columns: []*ir.Column{{Name: "id", Nullable: false}},
				Indexes: []*ir.Index{{Name: "idx_id", Unique: false, Columns: []ir.IndexColumn{{Column: "id"}}}},
			},
			wantOK: false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, ok := effectiveUpsertKeyColumns(c.table)
			if ok != c.wantOK {
				t.Fatalf("ok = %v; want %v (key=%v)", ok, c.wantOK, got)
			}
			if !ok {
				return
			}
			if len(got) != len(c.wantKey) {
				t.Fatalf("key = %v; want %v", got, c.wantKey)
			}
			for i := range c.wantKey {
				if got[i] != c.wantKey[i] {
					t.Errorf("key[%d] = %q; want %q", i, got[i], c.wantKey[i])
				}
			}
		})
	}
}

// TestWriteRowsIdempotent_RefusesKeylessTable pins the loud refusal: a
// table with no PK and no non-null UNIQUE index cannot be copied
// idempotently (nothing for ON CONFLICT to infer against), so the
// writer refuses rather than silently duplicating catchup re-emissions
// (Bug 125). Mirrors the MySQL-side pin.
func TestWriteRowsIdempotent_RefusesKeylessTable(t *testing.T) {
	w := &RowWriter{schema: "public"}
	table := &ir.Table{
		Name: "log_lines",
		Columns: []*ir.Column{
			{Name: "ts", Type: ir.Timestamp{}, Nullable: false},
			{Name: "msg", Type: ir.Text{}, Nullable: true},
		},
	}
	rows := make(chan ir.Row)
	close(rows)
	err := w.WriteRowsIdempotent(context.Background(), table, rows)
	if err == nil {
		t.Fatal("WriteRowsIdempotent on keyless table: err=nil; want loud refusal")
	}
	if !strings.Contains(err.Error(), "log_lines") || !strings.Contains(err.Error(), "Bug 125") {
		t.Errorf("error %q; want it to name the table and Bug 125", err.Error())
	}
}

// TestHandlesNoPKIdempotentCopy pins the capability declaration that
// makes the orchestrator's Bug-125 cross-engine gate route no-PK
// tables to this writer (rather than refuse them).
func TestHandlesNoPKIdempotentCopy(t *testing.T) {
	var w RowWriter
	if !w.HandlesNoPKIdempotentCopy() {
		t.Fatal("HandlesNoPKIdempotentCopy = false; want true (Bug 125 cross-engine symmetry)")
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
