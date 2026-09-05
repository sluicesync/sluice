// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package migcore

import (
	"testing"
)

// TestUnmatchedPatterns_Bug272 pins the detection of a filter pattern that
// matches nothing.
//
// WHY THIS MATTERS MORE THAN IT LOOKS. On the exclude path a pattern matching
// nothing FAILS OPEN: the operator believes a table is excluded, it is not,
// and its rows are copied at exit 0 with no warning of any kind. The v0.141.4
// regression cycle found it with `--exclude-table=public.pii`, which copied
// the PII table and every row it held.
//
// The dominant cause is schema qualification, and it is close to a trap:
// these patterns match the BARE table name, while sluice's own diagnostics
// print names qualified (`- public.nopk_t: no-primary-key`), so the form an
// operator copies out of sluice's output is exactly the form that silently
// matches nothing.
func TestUnmatchedPatterns_Bug272(t *testing.T) {
	tables := []string{"users", "orders", "pii"}

	t.Run("a schema-qualified exclude matches nothing", func(t *testing.T) {
		f, err := NewTableFilter(nil, []string{"public.pii"})
		if err != nil {
			t.Fatal(err)
		}
		// The load-bearing half: the filter ALLOWS the table the operator
		// tried to exclude. This is the fail-open, stated as an assertion so
		// it cannot quietly stop being true.
		if !f.Allows("pii") {
			t.Fatal("premise gone: a schema-qualified pattern now matches the bare name, so the " +
				"warning this test grades would be unreachable — re-derive the fix")
		}
		got := f.UnmatchedPatterns(tables)
		if len(got) != 1 || got[0] != "public.pii" {
			t.Errorf("UnmatchedPatterns = %v; want [public.pii] — the operator gets no signal that "+
				"their exclusion did nothing, and the rows are copied", got)
		}
		if !LooksSchemaQualified(got[0]) {
			t.Error("the qualified shape was not recognised, so the remedy would be the generic " +
				"spelling hint rather than the one that actually explains it")
		}
	})

	t.Run("a typo matches nothing", func(t *testing.T) {
		f, _ := NewTableFilter(nil, []string{"userz"})
		got := f.UnmatchedPatterns(tables)
		if len(got) != 1 || got[0] != "userz" {
			t.Errorf("UnmatchedPatterns = %v; want [userz]", got)
		}
		if LooksSchemaQualified(got[0]) {
			t.Error("a bare typo was reported as schema-qualified; the remedy would misdirect")
		}
	})

	t.Run("a dead glob matches nothing", func(t *testing.T) {
		f, _ := NewTableFilter(nil, []string{"tmp_*"})
		if got := f.UnmatchedPatterns(tables); len(got) != 1 {
			t.Errorf("UnmatchedPatterns = %v; want the dead glob reported", got)
		}
	})

	t.Run("patterns that DO match are silent", func(t *testing.T) {
		f, _ := NewTableFilter(nil, []string{"pii", "ord*"})
		if got := f.UnmatchedPatterns(tables); len(got) != 0 {
			t.Errorf("UnmatchedPatterns = %v; want none — warning on a working filter would train "+
				"operators to ignore the message", got)
		}
	})

	t.Run("include mode is covered too", func(t *testing.T) {
		f, _ := NewTableFilter([]string{"users", "public.orders"}, nil)
		got := f.UnmatchedPatterns(tables)
		if len(got) != 1 || got[0] != "public.orders" {
			t.Errorf("UnmatchedPatterns = %v; want [public.orders] — include-mode silence means "+
				"fewer tables than the operator asked for", got)
		}
	})

	t.Run("bareName renders the remedy", func(t *testing.T) {
		for pat, want := range map[string]string{
			"public.pii": "pii",
			"pii":        "pii",
			"a.b.c":      "c",
			"trailing.":  "trailing.",
		} {
			if got := bareName(pat); got != want {
				t.Errorf("bareName(%q) = %q; want %q", pat, got, want)
			}
		}
	})
}
