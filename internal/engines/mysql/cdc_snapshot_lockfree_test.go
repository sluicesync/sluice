// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"testing"
)

// Audit B-4 pins for the lock-free capture window's DECISION. The ORDER of
// the two statements — the half that actually moves the residual from loss
// to duplication — cannot be observed from here (it is statement sequence
// inside openBinlogSnapshotStreamShared against a live server), so it is
// pinned separately by TestLockFreeSnapshotCapturesPositionBeforeSnapshot in
// the integration suite, which reads the SERVER'S OWN general log rather
// than anything sluice reports about itself.

// TestLockFreeCaptureWindow_ReturnsTheEarlierTip is the crux: whichever way
// the window went, the handoff position is the PRE-snapshot cut. Returning
// `post` is the ordering that loses a commit; there is no input for which
// this function may return it. The GTID-mode cells (SLM-4) pin that the
// cut comes back WHOLE — a resolver that copied the pre tip but the post
// executed set would hand a GTID-mode source the losing ordering while
// every file/pos cell stayed green.
func TestLockFreeCaptureWindow_ReturnsTheEarlierTip(t *testing.T) {
	cases := []struct {
		name      string
		pre, post snapshotCut
	}{
		{
			name: "quiet window (tip did not move)",
			pre:  snapshotCut{File: "binlog.000007", Pos: 4096},
			post: snapshotCut{File: "binlog.000007", Pos: 4096},
		},
		{
			name: "commits inside the window (position advanced)",
			pre:  snapshotCut{File: "binlog.000007", Pos: 4096},
			post: snapshotCut{File: "binlog.000007", Pos: 9001},
		},
		{
			name: "commits inside the window (binlog rotated)",
			pre:  snapshotCut{File: "binlog.000007", Pos: 4096},
			post: snapshotCut{File: "binlog.000008", Pos: 157},
		},
		{
			name: "gtid mode, quiet window",
			pre:  snapshotCut{File: "binlog.000007", Pos: 4096, GTIDSet: "uuid:1-40"},
			post: snapshotCut{File: "binlog.000007", Pos: 4096, GTIDSet: "uuid:1-40"},
		},
		{
			name: "gtid mode, commits inside the window (set advanced with the tip)",
			pre:  snapshotCut{File: "binlog.000007", Pos: 4096, GTIDSet: "uuid:1-40"},
			post: snapshotCut{File: "binlog.000007", Pos: 9001, GTIDSet: "uuid:1-42"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveLockFreeCapture(context.Background(), tc.pre, tc.post)
			if got != tc.pre {
				t.Errorf("handoff cut = %+v; want the PRE cut %+v. "+
					"Handing back the later cut is the ordering where a commit inside the window lands in "+
					"neither the snapshot nor the CDC tail — the B-4 silent-loss case",
					got, tc.pre)
			}
		})
	}
}

// TestSnapshotFreezeMode names both paths so a run's log says which one it
// took rather than leaving the operator to infer it from a missing warning.
func TestSnapshotFreezeMode(t *testing.T) {
	if got := snapshotFreezeMode(true); got != "flush-tables-with-read-lock" {
		t.Errorf("locked mode = %q", got)
	}
	if got := snapshotFreezeMode(false); got == snapshotFreezeMode(true) {
		t.Error("the two capture paths must be distinguishable in the log")
	}
}
