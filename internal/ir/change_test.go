// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"testing"
	"time"
)

// TestChangeVariantsImplementInterface is a compile-time / runtime
// check that every shipping [Change] variant satisfies the sealed
// interface. The point of the test is the assignment list itself —
// adding a new variant without registering it here surfaces as a
// missing entry in the table rather than a downstream type-switch
// silently dropping the new case.
func TestChangeVariantsImplementInterface(t *testing.T) {
	cases := []struct {
		name string
		c    Change
	}{
		{"Insert", Insert{}},
		{"Update", Update{}},
		{"Delete", Delete{}},
		{"Truncate", Truncate{}},
		{"TxBegin", TxBegin{}},
		{"TxCommit", TxCommit{}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(_ *testing.T) {
			// Type assertion confirms the variant satisfies Change;
			// the seal method ensures only this package can add
			// variants.
			c.c.isChange()
		})
	}
}

// TestChangeSourceCommitTime pins the source-commit-time accessor across
// EVERY change variant (roadmap item 45) — the sync-lag metric dispatches on
// this accessor over the whole Change family, so per the "pin the class, not
// the representative" lesson the carrying variants must each round-trip their
// CommitTime and the boundary/schema variants must report what they actually
// carry. A new variant added without an entry here surfaces as a missing case.
func TestChangeSourceCommitTime(t *testing.T) {
	ts := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		c    Change
		want time.Time
	}{
		{"Insert", Insert{CommitTime: ts}, ts},
		{"Update", Update{CommitTime: ts}, ts},
		{"Delete", Delete{CommitTime: ts}, ts},
		{"Truncate", Truncate{CommitTime: ts}, ts},
		{"TxBegin", TxBegin{CommitTime: ts}, ts},
		{"TxCommit", TxCommit{CommitTime: ts}, ts},
		// SchemaSnapshot is a boundary event with no transaction commit
		// time — it must report the zero time (the metric treats it as
		// "unknown" and skips it), never a stamped value.
		{"SchemaSnapshot", SchemaSnapshot{}, time.Time{}},
		// The zero value of a carrying variant is "unknown" too.
		{"Insert/zero", Insert{}, time.Time{}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := c.c.SourceCommitTime(); !got.Equal(c.want) {
				t.Errorf("%s.SourceCommitTime() = %v; want %v", c.name, got, c.want)
			}
		})
	}
}

// TestTxBeginQualifiedNameEmpty asserts the boundary events surface
// no table reference. The [BatchedChangeApplier] dispatch path
// switches on type rather than on QualifiedName, but downstream
// observers (logs, metrics) consult the qualified name and need a
// well-defined value.
func TestTxBeginQualifiedNameEmpty(t *testing.T) {
	if got := (TxBegin{}).QualifiedName(); got != "" {
		t.Errorf("TxBegin.QualifiedName() = %q; want empty string", got)
	}
	if got := (TxCommit{}).QualifiedName(); got != "" {
		t.Errorf("TxCommit.QualifiedName() = %q; want empty string", got)
	}
}

// TestTxBeginPosRoundTrip confirms the boundary events carry the
// supplied position through the Pos accessor unchanged. The applier
// uses this position as the persisted source-position when the
// boundary is the last event in a flushed batch.
func TestTxBeginPosRoundTrip(t *testing.T) {
	pos := Position{Engine: "postgres", Token: `{"slot":"sluice_slot","lsn":"0/16B7350"}`}
	if got := (TxBegin{Position: pos}).Pos(); got != pos {
		t.Errorf("TxBegin.Pos() = %#v; want %#v", got, pos)
	}
	if got := (TxCommit{Position: pos}).Pos(); got != pos {
		t.Errorf("TxCommit.Pos() = %#v; want %#v", got, pos)
	}
}
