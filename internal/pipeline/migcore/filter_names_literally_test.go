// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import "testing"

// TestNamesLiterally pins the rules the INCLUDE-TABLE-SCOPE-DRIFT door keys on
// (Bug 321): only an entry with no glob metacharacter names a table, the
// comparison is case-sensitive, a backslash makes an entry a glob, and a
// schema-qualified entry names no bare table — each matching what
// [TableFilter.Allows] does with the same entry.
func TestNamesLiterally(t *testing.T) {
	cases := []struct {
		entry, table  string
		names, allows bool
	}{
		{"kd", "kd", true, true},
		{"Kd", "kd", false, false},
		{"k*", "kd", false, true},
		{"k?", "kd", false, true},
		{"[k]d", "kd", false, true},
		{`k\d`, "kd", false, true},
		{"app.kd", "kd", false, false},
	}
	for _, tc := range cases {
		f, err := NewTableFilter([]string{tc.entry}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := f.NamesLiterally(tc.table); got != tc.names {
			t.Errorf("entry %q, table %q: NamesLiterally = %v, want %v", tc.entry, tc.table, got, tc.names)
		}
		if got := f.Allows(tc.table); got != tc.allows {
			t.Errorf("entry %q, table %q: Allows = %v, want %v", tc.entry, tc.table, got, tc.allows)
		}
	}
	if (TableFilter{}).NamesLiterally("kd") {
		t.Error("an unfiltered stream names no table literally")
	}
}
