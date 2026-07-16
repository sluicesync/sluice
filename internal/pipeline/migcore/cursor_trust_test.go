// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// cursorTrustTable builds a composite-PK table covering the families
// the suspicion fingerprints dispatch on: integer, binary, text, and
// a domain-wrapped integer.
func cursorTrustTable() *ir.Table {
	return &ir.Table{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "bin", Type: ir.Binary{Length: 16}},
			{Name: "label", Type: ir.Varchar{Length: 32}},
			{Name: "dom", Type: ir.Domain{Name: "posint", BaseType: ir.Integer{Width: 64}}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{
			{Column: "id"}, {Column: "bin"}, {Column: "label"}, {Column: "dom"},
		}},
	}
}

func TestSuspectLegacyCursor(t *testing.T) {
	table := cursorTrustTable()
	cases := []struct {
		name    string
		cursor  []any
		suspect string // substring of the reason; "" = trusted
	}{
		{"clean int64", []any{int64(9007199254740995)}, ""},
		{"clean bytes", []any{int64(1), []byte{0x9F, 0x80}}, ""},
		{"clean string over binary (backfill stringified byte-exact)", []any{int64(1), "abc"}, ""},
		{"clean composite", []any{int64(1), []byte{0x01}, "x", int64(2)}, ""},
		{"prefix-width chunk bound", []any{int64(7)}, ""},
		{"U+FFFD string", []any{int64(1), "��A�\x10"}, "U+FFFD"},
		{"float over integer PK", []any{float64(1.75e18)}, "float-typed"},
		{"float over domain-wrapped integer PK", []any{int64(1), []byte{0x01}, "x", float64(9.007199254740996e15)}, "float-typed"},
		{"float over text PK is fine", []any{int64(1), []byte{0x01}, float64(2)}, ""},
		{"nil cursor", nil, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := SuspectLegacyCursor(table, tc.cursor)
			if tc.suspect == "" && got != "" {
				t.Errorf("SuspectLegacyCursor = %q; want trusted", got)
			}
			if tc.suspect != "" && !strings.Contains(got, tc.suspect) {
				t.Errorf("SuspectLegacyCursor = %q; want reason containing %q", got, tc.suspect)
			}
		})
	}
}

// TestSuspectLegacyMigrateCursor pins the migrate-only extra
// fingerprint: a bare string over a binary-family PK column is the
// pre-envelope migrate path's base64 garbage (plain json.Marshal of
// []byte), content-indistinguishable from real bytes — while the same
// shape stays TRUSTED for backfill, whose executors stringified the
// raw bytes before the store (byte-exact when valid UTF-8).
func TestSuspectLegacyMigrateCursor(t *testing.T) {
	table := cursorTrustTable()
	cursor := []any{int64(1), "n4BB/hA="} // base64-looking string over the bin column

	if got := SuspectLegacyMigrateCursor(table, cursor); !strings.Contains(got, "binary-family") {
		t.Errorf("migrate: got %q; want bare-string-over-binary suspicion", got)
	}
	if got := SuspectLegacyCursor(table, cursor); got != "" {
		t.Errorf("backfill: got %q; want trusted (executors stringified byte-exact)", got)
	}
	// []byte over the binary column (a post-envelope row) is trusted in
	// both modes.
	if got := SuspectLegacyMigrateCursor(table, []any{int64(1), []byte{0x9F, 0x80}}); got != "" {
		t.Errorf("migrate []byte: got %q; want trusted", got)
	}
	// Shared fingerprints still apply in migrate mode.
	if got := SuspectLegacyMigrateCursor(table, []any{float64(1.75e18)}); !strings.Contains(got, "float-typed") {
		t.Errorf("migrate float: got %q; want float-typed suspicion", got)
	}
}
