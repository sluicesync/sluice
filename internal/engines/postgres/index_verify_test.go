// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"sort"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestExpectedIndexes_MirrorsWhatTheBuildActuallyCreates pins the verified
// SET against the built set. The whole value of the [ir.IndexVerifier] net
// is that the two agree: an over-broad set false-flags a healthy migration
// (and an operator who hits that once stops trusting the net), while an
// under-broad one re-opens the silent no-op it exists to catch.
//
// Every carve-out below is a case where the index phase deliberately does
// NOT emit a CREATE INDEX, so probing for it would be a false refusal.
func TestExpectedIndexes_MirrorsWhatTheBuildActuallyCreates(t *testing.T) {
	w := &SchemaWriter{schema: "public"}

	tbl := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
			{Name: "a", Type: ir.Integer{Width: 64}},
			{Name: "b", Type: ir.Varchar{Length: 64}},
		},
		PrimaryKey: &ir.Index{Name: "t_pkey", Unique: true, Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			// Built → verified, table-prefixed by pgIndexName.
			mkNamedIdx("a_idx", false),
			// Built → verified, UNIQUE tracked so the verifier can catch a
			// same-named non-unique index sitting where this one belongs.
			{Name: "t_b_unique", Unique: true, Columns: []ir.IndexColumn{{Column: "b"}}},
			// Re-created as ALTER TABLE ADD CONSTRAINT by the constraints
			// phase, never by the index phase.
			{Name: "t_b_ck", Unique: true, ConstraintBacked: true, Columns: []ir.IndexColumn{{Column: "b"}}},
			// PG auto-names an unnamed index; nothing to probe for.
			{Name: "", Columns: []ir.IndexColumn{{Column: "a"}}},
			// ADR-0133: emitCreateIndex WARN-SKIPS a non-portable SQLite
			// expression index entirely, so it is never created.
			{
				Name:    "t_sqlite_expr",
				Columns: []ir.IndexColumn{{Expression: "some_unportable_fn(a)", ExpressionDialect: sqliteSourceDialect}},
			},
		},
	}

	got := w.expectedIndexes(&ir.Schema{Tables: []*ir.Table{tbl}})
	idents := make([]string, 0, len(got))
	for _, e := range got {
		idents = append(idents, e.ident)
	}
	sort.Strings(idents)

	want := []string{"t_a_idx", "t_b_unique"}
	if len(idents) != len(want) {
		t.Fatalf("expected set = %v; want %v", idents, want)
	}
	for i := range want {
		if idents[i] != want[i] {
			t.Fatalf("expected set = %v; want %v", idents, want)
		}
	}

	for _, e := range got {
		switch e.ident {
		case "t_a_idx":
			if e.unique {
				t.Errorf("%s: unique=true; the source index is not unique", e.ident)
			}
			if e.source != "t.a_idx" {
				t.Errorf("%s: source = %q; want the operator-facing table.index spelling", e.ident, e.source)
			}
		case "t_b_unique":
			if !e.unique {
				t.Error("t_b_unique: unique=false; a UNIQUE source index must be verified AS unique, " +
					"or a no-op'd CREATE UNIQUE INDEX IF NOT EXISTS onto a non-unique index goes unnoticed")
			}
		}
	}
}

// TestExpectedIndexes_SkipsTheInlinePromotedUniqueKey pins the Bug 125
// carve-out: for a PK-less table, emitTableDef promotes a non-null UNIQUE
// key inline as a CONSTRAINT and the index phase skips it, so the verifier
// must skip it too — the index it names exists, but it is a constraint's
// backing index, not one the index phase built.
func TestExpectedIndexes_SkipsTheInlinePromotedUniqueKey(t *testing.T) {
	w := &SchemaWriter{schema: "public"}
	tbl := &ir.Table{
		Name: "keyless",
		Columns: []*ir.Column{
			{Name: "sku", Type: ir.Varchar{Length: 64}},
			{Name: "name", Type: ir.Varchar{Length: 64}},
		},
		Indexes: []*ir.Index{
			{Name: "keyless_sku_uq", Unique: true, Columns: []ir.IndexColumn{{Column: "sku"}}},
			mkNamedIdx("name_idx", false),
		},
	}
	inline := inlineUniqueKeyForCopy(tbl)
	if inline == nil {
		t.Fatal("setup: this table should qualify for the Bug 125 inline unique-key promotion")
	}

	for _, e := range w.expectedIndexes(&ir.Schema{Tables: []*ir.Table{tbl}}) {
		if e.ident == pgIndexName(tbl.Name, inline.Name) {
			t.Fatalf("the inline-promoted unique key %q must NOT be in the verified set — "+
				"the index phase never builds it, so probing for it false-flags a healthy migration", e.ident)
		}
	}
}
