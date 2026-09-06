// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"sort"
	"testing"
)

// addTableShapePreflights are the SOURCE-SHAPE preflights every row-emitting
// entry point owes, as distinct from the once-per-run infrastructure ones.
//
// The split is the point. `schema add-table` legitimately does not run
// preflightSourceReplication or preflightReplicationHeadroom — it creates no
// slot and consumes no new one — so demanding the whole cold-start reference
// set here would be a false requirement, and a gate that demands the wrong
// thing gets weakened rather than obeyed. What it DOES do is copy rows for a
// table, and these three are the checks that decide whether those rows can be
// read faithfully at all.
var addTableShapePreflights = []string{
	"preflightRLS",
	"preflightPartitionedTables",
	"preflightInheritanceTables",
}

// TestAddTableRosterReachesTheShapePreflights is the gate for audit
// 2026-09-06 W4 H3.
//
// THE DEFECT. `schema add-table` is a row-emitting entry point and ran none of
// the three source-shape preflights. The RLS one is the sharp end, because the
// paths connect: preflightRLS's own documented remedy is to `--exclude-table`
// the offending table, and `schema add-table` is precisely how an operator
// brings that table back later. So sluice's advice on one door routed the
// operator through a door with no check — and an RLS-enabled table read by a
// role without BYPASSRLS returns only the rows the policy admits. A silent
// PARTIAL copy at exit 0, arrived at by following the instructions.
//
// WHY A SEPARATE GATE from TestColdStartPreflightRoster_MultiDatabaseReaches-
// EverySibling: that roster's universe is "the two sibling cold-start entry
// points", so add-table was outside it by construction — the gate could not
// have caught this however green it was. Widening a roster's UNIVERSE is a
// different act from adding a case to it, and the audit named this one
// specifically ("widen the consumer set to (*AddTable).Run and (*Backup).Run").
//
// WHAT IT REACHES, stated so the name cannot be read as broader: the three
// symbols above, called from EITHER `(*AddTable).Run` or the shape-preflight
// helper it delegates to, discovered by the same AST walk the cold-start
// roster uses. It does NOT prove they run in the right
// ORDER, that they run against the scoped schema rather than the whole one,
// or that the handle is still open when they do — those are pinned by the
// behaviour of the preflights themselves.
//
// STILL OUTSIDE ANY ROSTER: `(*Backup).Run`, the third entry point the audit
// named. It is not wired yet and is therefore not asserted here — a gate that
// listed it with an exemption would be a standing excuse, and one that
// demanded it would fail the build. Tracked in the audit backlog; when it is
// wired it belongs in this test, not in a new one.
func TestAddTableRosterReachesTheShapePreflights(t *testing.T) {
	calls := discoverPreflightCallsByFunc(t)

	// BOTH the entry point and the helper it delegates to. The helper exists
	// only because Run outgrew the funlen limit — and when it was extracted,
	// THIS GATE WENT RED naming all three preflights, which is the whole
	// reason it is written as a consumer SET rather than one function name.
	// The cold-start roster carries the same scar: a fix that landed in a
	// helper outside its listed set left it green while still exempt.
	consumers := []string{"(*AddTable).Run", "(*AddTable).preflightAddTableShape"}
	reached := map[string]bool{}
	total := 0
	for _, c := range consumers {
		for sym := range calls[c] {
			reached[sym] = true
			total++
		}
	}
	if total == 0 {
		t.Fatalf("the AST walk found no preflight calls in %v at all — either a function was renamed "+
			"or the matcher broke. Either way this gate is grading nothing; fix the walk, not the floor.", consumers)
	}
	consumer := consumers[0]

	// Anti-vacuity: add-table runs several preflights beyond these three
	// (binary type-override, replica identity, unlogged, redaction). A walk
	// that suddenly sees only a couple has stopped matching.
	if len(reached) < 4 {
		got := make([]string, 0, len(reached))
		for sym := range reached {
			got = append(got, sym)
		}
		sort.Strings(got)
		t.Fatalf("anti-vacuity: %s reaches only %d preflight symbol(s) (%v); it runs more than that — the matcher has drifted",
			consumer, len(reached), got)
	}

	for _, want := range addTableShapePreflights {
		if !reached[want] {
			t.Errorf("%s does not call %s.\n\n"+
				"add-table copies rows for the table it adds, so it owes the same source-shape checks the "+
				"cold-start entry points run. The RLS one especially: its own remedy tells the operator to "+
				"--exclude-table the offending table, and this command is how they bring it back — unchecked, "+
				"an RLS-enabled table read without BYPASSRLS yields only the rows the policy admits, silently, "+
				"at exit 0.", consumer, want)
		}
	}
}
