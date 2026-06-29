// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestScanSQLiteAffinityNotices_FamilyMatrix pins EVERY IR type family the
// SQLite writer normalizes onto a different affinity (the Bug-74 "pin the
// class" discipline — the scan dispatches on the type family, so every
// family that normalizes must be exercised, not one representative), AND a
// representative set of identity-mapped types that must produce NO note.
func TestScanSQLiteAffinityNotices_FamilyMatrix(t *testing.T) {
	normalized := []struct {
		name     string
		typ      ir.Type
		affinity string
	}{
		{"decimal_bounded", ir.Decimal{Precision: 10, Scale: 2}, "TEXT"},
		{"decimal_unconstrained", ir.Decimal{Unconstrained: true}, "TEXT"},
		{"json", ir.JSON{}, "TEXT"},
		{"uuid", ir.UUID{}, "TEXT"},
		{"enum", ir.Enum{Values: []string{"a", "b"}}, "TEXT"},
		{"set", ir.Set{Values: []string{"x", "y"}}, "TEXT"},
		{"char", ir.Char{Length: 8}, "TEXT"},
		{"varchar", ir.Varchar{Length: 255}, "TEXT"},
		{"integer_signed", ir.Integer{Width: 32}, "INTEGER"},
		{"integer_unsigned", ir.Integer{Width: 64, Unsigned: true}, "INTEGER"},
	}
	for _, tc := range normalized {
		t.Run("normalized/"+tc.name, func(t *testing.T) {
			schema := &ir.Schema{Tables: []*ir.Table{{
				Name:    "t",
				Columns: []*ir.Column{{Name: "c", Type: tc.typ}},
			}}}
			got := ScanSQLiteAffinityNotices(schema, "sqlite")
			if len(got) != 1 {
				t.Fatalf("%s: got %d notices, want 1: %+v", tc.name, len(got), got)
			}
			if got[0].TargetType != tc.affinity {
				t.Errorf("%s: affinity = %q, want %q", tc.name, got[0].TargetType, tc.affinity)
			}
			if got[0].SourceType != tc.typ.String() {
				t.Errorf("%s: source = %q, want %q", tc.name, got[0].SourceType, tc.typ.String())
			}
			if got[0].Note == "" {
				t.Errorf("%s: note is empty", tc.name)
			}
		})
	}

	// Identity-mapped types: the SQLite writer emits an affinity matching
	// the nominal type, so there is nothing to surface — these must NOT
	// produce a note (a false positive would be operator noise).
	identity := []struct {
		name string
		typ  ir.Type
	}{
		{"boolean", ir.Boolean{}},
		{"float", ir.Float{Precision: ir.FloatDouble}},
		{"text", ir.Text{Size: ir.TextLong}},
		{"blob", ir.Blob{}},
		{"binary", ir.Binary{Length: 16}},
		{"date", ir.Date{}},
		{"time", ir.Time{}},
		{"timestamp", ir.Timestamp{}},
	}
	for _, tc := range identity {
		t.Run("identity/"+tc.name, func(t *testing.T) {
			schema := &ir.Schema{Tables: []*ir.Table{{
				Name:    "t",
				Columns: []*ir.Column{{Name: "c", Type: tc.typ}},
			}}}
			if got := ScanSQLiteAffinityNotices(schema, "sqlite"); len(got) != 0 {
				t.Errorf("%s: got %d notices, want 0: %+v", tc.name, len(got), got)
			}
		})
	}
}

// TestScanSQLiteAffinityNotices_DecimalHeadline pins the Bug 162 value-
// fidelity wording — the decimal → TEXT note must explain that TEXT
// preserves the exact value and that NUMERIC would be lossy.
func TestScanSQLiteAffinityNotices_DecimalHeadline(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "money",
		Columns: []*ir.Column{{Name: "amount", Type: ir.Decimal{Precision: 10, Scale: 2}}},
	}}}
	got := ScanSQLiteAffinityNotices(schema, "sqlite")
	if len(got) != 1 {
		t.Fatalf("got %d notices, want 1", len(got))
	}
	for _, want := range []string{"TEXT", "exact decimal", "NUMERIC", "REAL"} {
		if !strings.Contains(got[0].Note, want) {
			t.Errorf("decimal note missing %q: %q", want, got[0].Note)
		}
	}
}

// TestScanSQLiteAffinityNotices_NonSQLiteTarget verifies the scan is
// target-gated: any non-SQLite target produces no notes.
func TestScanSQLiteAffinityNotices_NonSQLiteTarget(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name:    "t",
		Columns: []*ir.Column{{Name: "c", Type: ir.Decimal{Precision: 10, Scale: 2}}},
	}}}
	for _, target := range []string{"mysql", "postgres", "planetscale", ""} {
		if got := ScanSQLiteAffinityNotices(schema, target); len(got) != 0 {
			t.Errorf("target %q: got %d notices, want 0", target, len(got))
		}
	}
	if got := ScanSQLiteAffinityNotices(nil, "sqlite"); got != nil {
		t.Errorf("nil schema: got %+v, want nil", got)
	}
}

// TestScanSQLiteAffinityNotices_SortStable verifies deterministic ordering
// by (table, column).
func TestScanSQLiteAffinityNotices_SortStable(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{
		{Name: "b", Columns: []*ir.Column{{Name: "z", Type: ir.UUID{}}}},
		{Name: "a", Columns: []*ir.Column{
			{Name: "y", Type: ir.UUID{}},
			{Name: "x", Type: ir.UUID{}},
		}},
	}}
	got := ScanSQLiteAffinityNotices(schema, "SQLite") // also pins case-insensitive match
	want := [][2]string{{"a", "x"}, {"a", "y"}, {"b", "z"}}
	if len(got) != len(want) {
		t.Fatalf("got %d notices, want %d", len(got), len(want))
	}
	for i, w := range want {
		if got[i].Table != w[0] || got[i].Column != w[1] {
			t.Errorf("notice[%d] = %s.%s, want %s.%s", i, got[i].Table, got[i].Column, w[0], w[1])
		}
	}
}
