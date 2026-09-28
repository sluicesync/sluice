// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package triggercdc

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeApplyID_Shape pins the identity's spelling and its refusal to
// name a change it cannot stamp: an empty stamp is no identity (never
// skipped), not `<engine>:<id>:` — a name a reset id sequence could re-issue.
func TestChangeApplyID_Shape(t *testing.T) {
	if got, want := ChangeApplyID("postgres-trigger", 42, "731"), (ir.ApplyID{TxID: "postgres-trigger:42:731", Seq: 1}); got != want {
		t.Errorf("ChangeApplyID = %v; want %v", got, want)
	}
	if got := ChangeApplyID("sqlite-trigger", 42, ""); !got.IsZero() {
		t.Errorf("an empty stamp yielded identity %v; want none", got)
	}
}
