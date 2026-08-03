// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The fresh-copy-reason dispatch gate (audit 2026-08-01 S5).
//
// S5 was a single `forceFresh bool` standing for two different intents. Both
// need the Bug-9 populated-target refusal suppressed, and only one of them
// should clear the target first — collapsing them into one bit is what let an
// idempotent re-copy merge onto rows the source had deleted, silently and
// permanently.
//
// These pin the decision table directly, because it is the whole fix. The
// dispatch that consumes it is exercised by the streamer's own cold-start
// integration tests; what must not drift is which reason means what.

package pipeline

import "testing"

func TestFreshCopyReason_DecisionTable(t *testing.T) {
	cases := []struct {
		name         string
		reason       freshCopyReason
		forcesFresh  bool
		clearsTarget bool
		why          string
	}{
		{
			name:         "none",
			reason:       freshCopyNone,
			forcesFresh:  false,
			clearsTarget: false,
			why: "an ordinary cold-start keeps the Bug-9 populated-target refusal; this is the zero " +
				"value, and it must stay the safe one",
		},
		{
			name:         "operator restart",
			reason:       freshCopyOperatorRestart,
			forcesFresh:  true,
			clearsTarget: true,
			why: "--restart-from-scratch is taken at its word on EITHER reader: the target is cleared " +
				"so the re-copy reproduces the source rather than merging onto rows the source may have " +
				"deleted (S5)",
		},
		{
			name:         "auto resnapshot",
			reason:       freshCopyAutoResnapshot,
			forcesFresh:  true,
			clearsTarget: false,
			why: "the ADR-0093 fall-through fires without operator involvement on a LIVE stream, so it " +
				"must not open an empty-target window; it warns about the deletes it cannot reconcile " +
				"instead",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.reason.forcesFresh(); got != tc.forcesFresh {
				t.Errorf("forcesFresh() = %v, want %v — %s", got, tc.forcesFresh, tc.why)
			}
			if got := tc.reason.clearsTarget(); got != tc.clearsTarget {
				t.Errorf("clearsTarget() = %v, want %v — %s", got, tc.clearsTarget, tc.why)
			}
		})
	}
}

// TestFreshCopyReason_ZeroValueIsTheSafeOne is the CLAUDE.md zero-value rule
// applied to this type: whatever else changes, the zero value must be the
// ordinary cold-start that keeps every guard on. A reordering of the constants
// that made the zero value mean "forced re-copy" would silently disable the
// Bug-9 populated-target refusal for every caller that forgot to set it.
func TestFreshCopyReason_ZeroValueIsTheSafeOne(t *testing.T) {
	var zero freshCopyReason
	if zero != freshCopyNone {
		t.Fatalf("the zero freshCopyReason is %v, not freshCopyNone", zero)
	}
	if zero.forcesFresh() || zero.clearsTarget() {
		t.Fatal("the zero value must neither force a fresh copy nor clear the target — otherwise a " +
			"caller that forgets to set a reason silently loses the Bug-9 populated-target refusal")
	}
}

// TestRestartReason_MapsTheOperatorFlag and TestAutoResnapshotReason_* keep the
// two mappers honest. autoResnapshotReason is the ADR-0075 Phase 2b contract:
// a source NOT eligible for the automatic fall-through must get freshCopyNone
// so the gate refuses loudly rather than re-snapshotting a PG logical-slot
// source behind the operator's back.
func TestRestartReason_MapsTheOperatorFlag(t *testing.T) {
	if got := restartReason(true); got != freshCopyOperatorRestart {
		t.Errorf("restartReason(true) = %v, want freshCopyOperatorRestart", got)
	}
	if got := restartReason(false); got != freshCopyNone {
		t.Errorf("restartReason(false) = %v, want freshCopyNone", got)
	}
}
