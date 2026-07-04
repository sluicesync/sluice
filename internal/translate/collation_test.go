// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCollationDialect pins the charset-paired collation rule: a
// collation with a charset is MySQL-dialect (MySQL's
// information_schema always pairs them), a collation without one is
// PG-dialect (the PG reader never records a charset), and no collation
// means no dialect.
func TestCollationDialect(t *testing.T) {
	cases := []struct {
		name, charset, collation, want string
	}{
		{"no collation", "", "", ""},
		{"charset only (MySQL default-collation gap never occurs, but classify as none)", "utf8mb4", "", ""},
		{"pg libc C", "", "C", "postgres"},
		{"pg locale", "", "en_US.utf8", "postgres"},
		{"pg icu", "", "en-x-icu", "postgres"},
		{"mysql paired", "utf8mb4", "utf8mb4_0900_ai_ci", "mysql"},
		{"mysql legacy paired", "latin1", "latin1_swedish_ci", "mysql"},
	}
	for _, tc := range cases {
		if got := CollationDialect(tc.charset, tc.collation); got != tc.want {
			t.Errorf("%s: CollationDialect(%q, %q) = %q; want %q",
				tc.name, tc.charset, tc.collation, got, tc.want)
		}
	}
}

// TestColumnCollation covers every IR string family that carries a
// collation (Char / Varchar / Text — the full set; pin the class, not
// the representative) plus non-carriers.
func TestColumnCollation(t *testing.T) {
	cases := []struct {
		name          string
		typ           ir.Type
		wantCharset   string
		wantCollation string
	}{
		{"Char", ir.Char{Length: 3, Charset: "utf8mb4", Collation: "utf8mb4_bin"}, "utf8mb4", "utf8mb4_bin"},
		{"Varchar", ir.Varchar{Length: 10, Collation: "C"}, "", "C"},
		{"Text", ir.Text{Size: ir.TextLong, Collation: "en_US"}, "", "en_US"},
		{"Integer carries none", ir.Integer{Width: 32}, "", ""},
		{"Domain recurses into base (effective collation resolved by the PG reader)", ir.Domain{Name: "email_address", BaseType: ir.Text{Collation: "C"}}, "", "C"},
		{"Domain with nil base", ir.Domain{Name: "broken"}, "", ""},
		{"Array element collation not modelled (reader never populates it)", ir.Array{Element: ir.Text{Collation: "C"}}, "", ""},
	}
	for _, tc := range cases {
		cs, coll := ColumnCollation(tc.typ)
		if cs != tc.wantCharset || coll != tc.wantCollation {
			t.Errorf("%s: ColumnCollation = (%q, %q); want (%q, %q)",
				tc.name, cs, coll, tc.wantCharset, tc.wantCollation)
		}
	}
}

// TestDroppedCollationColumns pins the per-table drop scan each writer
// WARNs from: same-dialect collations survive, foreign ones list, and
// an unknown target dialect (sqlite) treats every collation as foreign.
func TestDroppedCollationColumns(t *testing.T) {
	tbl := &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "a", Type: ir.Varchar{Length: 5, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"}},
			{Name: "b", Type: ir.Text{Collation: "C"}},
			{Name: "c", Type: ir.Integer{Width: 64}},
			{Name: "d", Type: ir.Char{Length: 2}},
		},
	}
	if got, want := DroppedCollationColumns(tbl, "postgres"), []string{"a (utf8mb4_0900_ai_ci)"}; !reflect.DeepEqual(got, want) {
		t.Errorf("postgres target: dropped = %v; want %v", got, want)
	}
	if got, want := DroppedCollationColumns(tbl, "mysql"), []string{"b (C)"}; !reflect.DeepEqual(got, want) {
		t.Errorf("mysql target: dropped = %v; want %v", got, want)
	}
	if got, want := DroppedCollationColumns(tbl, "sqlite"), []string{"a (utf8mb4_0900_ai_ci)", "b (C)"}; !reflect.DeepEqual(got, want) {
		t.Errorf("sqlite target: dropped = %v; want %v", got, want)
	}
	if got := DroppedCollationColumns(nil, "postgres"); got != nil {
		t.Errorf("nil table: dropped = %v; want nil", got)
	}
}
