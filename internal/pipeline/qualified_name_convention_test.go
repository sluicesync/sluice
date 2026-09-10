// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"os"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// THE DISPATCH FILTER AND THE READERS' SCOPE PREDICATES MUST DERIVE THE
// SAME TABLE NAME FROM A QUALIFIED ONE.
//
// # Why this can go wrong, and did
//
// [ir.qualified] joins a schema and a table with a dot, and it has no
// unambiguous inverse: a TABLE NAME may itself contain a dot, so `db.a.b`
// could be database `db` with table `a.b`, or schema `db.a` with table
// `b`. The string alone cannot say. Which reading is right matters far
// less than every consumer picking the SAME one.
//
// They did not. Audit 2026-09-09 VF0909C-3: this package's dispatch
// filter scanned from the END while the MySQL reader's scope predicate
// split at the FIRST dot. For a table named `a.b` the reader asked the
// filter about `a.b` and the dispatch asked about `b`, so a
// stream-killing refusal the reader skipped as out-of-scope could have
// its schema boundary forwarded by a dispatch that considered the table
// in scope — the reader declining to refuse a value-changing re-cast
// that the target then received. Low likelihood, because dotted table
// names are rare, and a genuine silent-divergence path.
//
// # What this gate reaches
//
// The convention itself, and the fact that the dispatch filter routes
// through it. It cannot reach an engine package from here — Go's import
// graph puts the readers below the pipeline — so the MySQL side is held
// by its own sibling assertion in that package. What both sides share is
// [ir.UnqualifiedTableName], and a second implementation of that scan
// appearing anywhere is the thing to catch; the source check below is
// what catches it in this package.
func TestUnqualifiedTableName_IsTheOneConvention(t *testing.T) {
	for _, tc := range []struct {
		name, qualified, want string
	}{
		{"plain schema and table", "db.orders", "orders"},
		{"no schema at all", "orders", "orders"},
		{"a table name containing a dot", "db.a.b", "b"},
		{"two dots in the table name", "db.a.b.c", "c"},
		{"empty", "", ""},
		{"trailing dot yields an empty table", "db.", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := ir.UnqualifiedTableName(tc.qualified); got != tc.want {
				t.Errorf("ir.UnqualifiedTableName(%q) = %q; want %q", tc.qualified, got, tc.want)
			}
		})
	}
}

// TestDispatchFilterUsesTheSharedConvention pins that the dispatch side
// actually calls the shared helper rather than carrying its own copy of
// the scan.
//
// A table test alone cannot catch a re-introduced inline loop: it would
// grade the helper, which stays correct, while the caller quietly stops
// using it. That is exactly how the two sides drifted apart the first
// time — each read correctly on its own.
func TestDispatchFilterUsesTheSharedConvention(t *testing.T) {
	raw, err := os.ReadFile("streamer_filter_flip.go")
	if err != nil {
		t.Fatalf("read streamer_filter_flip.go: %v", err)
	}
	src := string(raw)
	if !strings.Contains(src, "ir.UnqualifiedTableName(") {
		t.Error("the dispatch filter no longer routes through ir.UnqualifiedTableName.\n\n" +
			"Both this filter and the CDC readers' scope predicates must derive the same table name from a " +
			"qualified one, or a refusal one side skips as out-of-scope has its boundary forwarded by the " +
			"other (audit 2026-09-09 VF0909C-3). Call the shared helper rather than re-implementing the scan.")
	}
	// An inline scan is the shape that replaced it last time. `name[i] == '.'`
	// inside a descending loop is that shape's fingerprint.
	if strings.Contains(src, "name[i] == '.'") {
		t.Error("streamer_filter_flip.go carries an inline dot-scan again; use ir.UnqualifiedTableName so " +
			"the readers' predicates and this filter cannot answer differently")
	}
}
