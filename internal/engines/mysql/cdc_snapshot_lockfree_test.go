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
// the window went, the handoff position is the PRE-snapshot tip. Returning
// `post` is the ordering that loses a commit; there is no input for which
// this function may return it.
func TestLockFreeCaptureWindow_ReturnsTheEarlierTip(t *testing.T) {
	cases := []struct {
		name      string
		pre, post binlogTip
	}{
		{
			name: "quiet window (tip did not move)",
			pre:  binlogTip{File: "binlog.000007", Pos: 4096},
			post: binlogTip{File: "binlog.000007", Pos: 4096},
		},
		{
			name: "commits inside the window (position advanced)",
			pre:  binlogTip{File: "binlog.000007", Pos: 4096},
			post: binlogTip{File: "binlog.000007", Pos: 9001},
		},
		{
			name: "commits inside the window (binlog rotated)",
			pre:  binlogTip{File: "binlog.000007", Pos: 4096},
			post: binlogTip{File: "binlog.000008", Pos: 157},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file, pos := resolveLockFreeCapturePosition(context.Background(), tc.pre, tc.post)
			if file != tc.pre.File || pos != tc.pre.Pos {
				t.Errorf("handoff position = %s:%d; want the PRE tip %s:%d. "+
					"Handing back the later tip is the ordering where a commit inside the window lands in "+
					"neither the snapshot nor the CDC tail — the B-4 silent-loss case",
					file, pos, tc.pre.File, tc.pre.Pos)
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
