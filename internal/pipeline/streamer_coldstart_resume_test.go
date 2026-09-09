// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// Unit pins for the A0909-STOP-1 gate and ladder. The end-to-end
// behaviour is pinned on real PostgreSQL
// (streamer_coldstart_resume_pg_integration_test.go); these cover the
// decision table exhaustively, which a container test cannot afford to.

// TestGradeRecordedColdStartHeader_EveryPhase walks the WHOLE phase
// enum rather than a representative pair, because the question the
// predicate answers ("could this run have got here with its copy
// finished?") is decided per phase and a missed value fails open — it
// would authorise skipping a copy.
func TestGradeRecordedColdStartHeader_EveryPhase(t *testing.T) {
	const anchor = `{"slot":"sluice_slot","lsn":"0/1946620"}`
	// Every ir.MigrationPhase there is, plus the empty value a
	// half-written row could carry.
	cases := []struct {
		phase    ir.MigrationPhase
		resumamt bool
	}{
		{ir.MigrationPhasePending, false},
		{ir.MigrationPhaseTables, false},
		// bulk_copy is ADMITTED, and it is the case the feature exists
		// for: measured on real PostgreSQL, an operator stop during the
		// overlapped index build leaves the header at bulk_copy, because
		// the failure mark's own write is cancelled along with everything
		// else. The phase is a floor; everyTableCopied is the proof.
		{ir.MigrationPhaseBulkCopy, true},
		{ir.MigrationPhaseIdentitySync, true},
		{ir.MigrationPhaseIndexes, true},
		{ir.MigrationPhaseConstraints, true},
		{ir.MigrationPhaseViews, true},
		{ir.MigrationPhaseComplete, true},
		// `failed` is the literal the design doc reserves and markFailed
		// does not write; if one ever appears it says nothing about the
		// copy, so it must not resume.
		{ir.MigrationPhaseFailed, false},
		{ir.MigrationPhase(""), false},
	}
	for _, tc := range cases {
		t.Run(string(tc.phase), func(t *testing.T) {
			state := ir.MigrationState{Phase: tc.phase, SnapshotAnchor: anchor}
			why := gradeRecordedColdStartHeader(state, true)
			if got := why == ""; got != tc.resumamt {
				t.Fatalf("phase %q: resumable=%v (%q); want %v", tc.phase, got, why, tc.resumamt)
			}
		})
	}
}

func TestGradeRecordedColdStartHeader_EvidenceRequired(t *testing.T) {
	const anchor = `{"slot":"sluice_slot","lsn":"0/1946620"}`
	t.Run("no recorded run", func(t *testing.T) {
		if why := gradeRecordedColdStartHeader(ir.MigrationState{}, false); why == "" {
			t.Fatal("a stream with NO recorded cold start graded resumable")
		}
	})
	t.Run("an older binary's row carries no anchor", func(t *testing.T) {
		state := ir.MigrationState{Phase: ir.MigrationPhaseComplete}
		why := gradeRecordedColdStartHeader(state, true)
		if why == "" {
			t.Fatal("a row with an EMPTY snapshot anchor graded resumable; empty means the run recorded no " +
				"anchor (an older sluice, or a failed anchor write), never 'the empty position'")
		}
		if !strings.Contains(why, "anchor") {
			t.Errorf("the decline reason does not mention the anchor: %q", why)
		}
	})
	t.Run("complete with an anchor", func(t *testing.T) {
		state := ir.MigrationState{Phase: ir.MigrationPhaseComplete, SnapshotAnchor: anchor}
		if why := gradeRecordedColdStartHeader(state, true); why != "" {
			t.Fatalf("a finished recorded run with an anchor graded unresumable: %q", why)
		}
	})
}

// TestEveryTableCopied is the load-bearing half of the gate: the
// per-table evidence, which is what actually proves the copy finished.
// See the file comment on streamer_coldstart_resume.go for why the
// phase cannot carry that proof on the PostgreSQL path.
func TestEveryTableCopied(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{Name: "users"}, {Name: "orders"}}}
	complete := ir.TableProgress{State: ir.TableProgressComplete}

	t.Run("every table complete", func(t *testing.T) {
		state := ir.MigrationState{TableProgress: map[string]ir.TableProgress{"users": complete, "orders": complete}}
		if missing, ok := everyTableCopied(schema, state); !ok {
			t.Fatalf("ok=false (first missing %q) with every table recorded complete", missing)
		}
	})
	t.Run("one table still in progress", func(t *testing.T) {
		state := ir.MigrationState{TableProgress: map[string]ir.TableProgress{
			"users":  complete,
			"orders": {State: ir.TableProgressInProgress},
		}}
		missing, ok := everyTableCopied(schema, state)
		if ok {
			t.Fatal("a PARTIAL copy graded as finished — a resume here skips the rest of it, silently")
		}
		if missing != "orders" {
			t.Errorf("first missing = %q; want orders", missing)
		}
	})
	t.Run("a table with no progress row at all", func(t *testing.T) {
		state := ir.MigrationState{TableProgress: map[string]ir.TableProgress{"users": complete}}
		if missing, ok := everyTableCopied(schema, state); ok || missing != "orders" {
			t.Fatalf("missing=%q ok=%v; a table absent from the progress map was never copied", missing, ok)
		}
	})
	t.Run("a table added to the filter since the recorded run", func(t *testing.T) {
		// The scope is the CURRENT run's, so a newly in-scope table
		// correctly reads as not copied.
		wider := &ir.Schema{Tables: []*ir.Table{{Name: "users"}, {Name: "orders"}, {Name: "audit"}}}
		state := ir.MigrationState{TableProgress: map[string]ir.TableProgress{"users": complete, "orders": complete}}
		if missing, ok := everyTableCopied(wider, state); ok || missing != "audit" {
			t.Fatalf("missing=%q ok=%v; a table the recorded run never had in scope must not read as copied", missing, ok)
		}
	})
	t.Run("no tables at all", func(t *testing.T) {
		// Zero tables must not read as "every table copied": that is the
		// vacuous-true reading, and it would let an empty or unreadable
		// schema authorise skipping a copy.
		if _, ok := everyTableCopied(&ir.Schema{}, ir.MigrationState{}); ok {
			t.Fatal("an EMPTY schema graded as fully copied")
		}
		if _, ok := everyTableCopied(nil, ir.MigrationState{}); ok {
			t.Fatal("a nil schema graded as fully copied")
		}
	})
	t.Run("a no-PK truncate-and-redo table is not complete", func(t *testing.T) {
		state := ir.MigrationState{TableProgress: map[string]ir.TableProgress{
			"users":  complete,
			"orders": {State: ir.TableProgressNoPKTruncateAndRedo},
		}}
		if _, ok := everyTableCopied(schema, state); ok {
			t.Fatal("a table marked truncate-and-redo graded as copied")
		}
	})
}

// TestRunColdStartResumePhases_LadderPerRecordedPhase pins which DDL
// phases run for each recorded phase. The ladder is NOT the enum's
// order (the PG fast path overlaps the index build with the copy and
// runs identity-sync after both), so it is spelled out per phase and
// asserted against the writer's own call log.
func TestRunColdStartResumePhases_LadderPerRecordedPhase(t *testing.T) {
	// The schema carries a VIEW deliberately: migcore.RunViewsPhase is a
	// no-op on a view-less schema, so a view-less fixture would record
	// "views ran" as an empty log and the ladder's last rung would be
	// unobservable — a gate that cannot tell the phase ran from the phase
	// being skipped.
	schema := &ir.Schema{
		Tables: []*ir.Table{{Name: "users"}},
		Views:  []*ir.View{{Name: "active_users", Definition: "SELECT 1"}},
	}
	cases := []struct {
		from ir.MigrationPhase
		want []string
	}{
		// The index build was in flight: indexes may be partial and
		// identity-sync has not run. bulk_copy is the SAME case — it is
		// what an operator stop in that window actually records — so both
		// spellings must run the same rungs.
		{ir.MigrationPhaseBulkCopy, []string{"CreateIndexes", "SyncIdentitySequences", "CreateConstraints", "CreateViews"}},
		{ir.MigrationPhaseIndexes, []string{"CreateIndexes", "SyncIdentitySequences", "CreateConstraints", "CreateViews"}},
		// The index phase completed (it precedes identity-sync in every
		// branch), so only re-run identity-sync onward.
		{ir.MigrationPhaseIdentitySync, []string{"SyncIdentitySequences", "CreateConstraints", "CreateViews"}},
		{ir.MigrationPhaseConstraints, []string{"CreateConstraints", "CreateViews"}},
		{ir.MigrationPhaseViews, []string{"CreateViews"}},
		// Everything finished; only the CDC anchor was missing.
		{ir.MigrationPhaseComplete, nil},
	}
	for _, tc := range cases {
		t.Run(string(tc.from), func(t *testing.T) {
			var log []string
			sw := &recordingSchemaWriter{phaseLog: &log}
			state := ir.MigrationState{MigrationID: "sync-s1", Phase: tc.from}
			if err := runColdStartResumePhases(context.Background(), resumeContext{}, &state, schema, sw); err != nil {
				t.Fatalf("runColdStartResumePhases(from=%s): %v", tc.from, err)
			}
			if !reflect.DeepEqual(log, tc.want) {
				t.Fatalf("from %s ran %v; want %v", tc.from, log, tc.want)
			}
		})
	}
}

// TestRunColdStartResumePhases_NeverCreatesTables is the guard that
// keeps this path a RESUME. Creating tables here would mean the resume
// had decided the target was not already shaped — and since it also
// skips the copy, that combination is an empty target reported as
// synced.
func TestRunColdStartResumePhases_NeverCreatesTables(t *testing.T) {
	// The schema carries a VIEW deliberately: migcore.RunViewsPhase is a
	// no-op on a view-less schema, so a view-less fixture would record
	// "views ran" as an empty log and the ladder's last rung would be
	// unobservable — a gate that cannot tell the phase ran from the phase
	// being skipped.
	schema := &ir.Schema{
		Tables: []*ir.Table{{Name: "users"}},
		Views:  []*ir.View{{Name: "active_users", Definition: "SELECT 1"}},
	}
	for _, from := range []ir.MigrationPhase{
		ir.MigrationPhaseBulkCopy,
		ir.MigrationPhaseIndexes,
		ir.MigrationPhaseIdentitySync,
		ir.MigrationPhaseConstraints,
		ir.MigrationPhaseViews,
		ir.MigrationPhaseComplete,
	} {
		var log []string
		var created []string
		sw := &recordingSchemaWriter{phaseLog: &log, createdTables: &created}
		state := ir.MigrationState{MigrationID: "sync-s1", Phase: from}
		if err := runColdStartResumePhases(context.Background(), resumeContext{}, &state, schema, sw); err != nil {
			t.Fatalf("from %s: %v", from, err)
		}
		if len(created) != 0 {
			t.Fatalf("from %s CREATED tables %v; the resume must never create — it is finishing a copy that "+
				"already landed", from, created)
		}
	}
}

// TestResumeStoppedColdStart_UnsupportedSourceIsNotHandled pins the
// scope declaration itself: a source with no
// ir.SnapshotAnchorVerifier must fall straight through to today's
// behaviour, touching nothing — not even the target's progress store.
//
// stubEngine panics on any unexpected call, so "touched nothing" is
// enforced rather than asserted.
func TestResumeStoppedColdStart_UnsupportedSourceIsNotHandled(t *testing.T) {
	s := &Streamer{Source: stubEngine{}, Target: stubEngine{}}
	ctx := context.Background()
	changes, stop, handled, err := s.resumeStoppedColdStart(ctx, ctx, nil, nil, "s1")
	if handled || err != nil || changes != nil || stop != nil {
		t.Fatalf("a source with no anchor verifier was handled (handled=%v err=%v); every non-PostgreSQL "+
			"source must keep today's behaviour exactly", handled, err)
	}
}
