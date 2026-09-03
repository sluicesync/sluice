// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # The lock-free snapshot capture window (audit B-4)
//
// A cold start has to name one logical instant twice: as a row VIEW (the
// consistent-snapshot transaction the bulk copy reads under) and as a
// binlog POSITION (where the CDC tail picks up). `FLUSH TABLES WITH READ
// LOCK` makes the two coincide by blocking commits across both reads.
//
// When FTWRL is unavailable the two reads bracket a window in which
// commits can happen, and sluice has to choose which side of the window
// to hand the CDC reader. That choice is the entire content of this
// file, and it used to be made implicitly by statement order.
//
// The window cannot be closed here. `LOCK INSTANCE FOR BACKUP` is
// RDS-permitted but blocks DDL, not DML, so it does not stop the commits
// that create the window; `performance_schema.log_status` is a better
// way to READ a position (it is atomic with the storage-engine LSN and
// needs only BACKUP_ADMIN) but it is still a separate statement from
// `START TRANSACTION WITH CONSISTENT SNAPSHOT`, so it leaves the same
// ordering algebra untouched. Neither is implemented, and neither would
// let this file claim more than it does. What IS available cheaply is a
// probe that says whether the window caught anything at all — which is
// the difference between a permanent warning nobody can act on and a
// per-run fact.

package mysql

import (
	"context"
	"log/slog"
)

// binlogTip is one (file, position) reading of the source's binlog tip.
// Comparable by design: the whole point is `pre == post`.
type binlogTip struct {
	File string
	Pos  uint32
}

// snapshotFreezeMode names, for the log, which capture path a run
// actually took. Operators reading a support thread should not have to
// infer this from the absence of a warning.
func snapshotFreezeMode(locked bool) string {
	if locked {
		return "flush-tables-with-read-lock"
	}
	return "none (lock-free fallback)"
}

// resolveLockFreeCapture picks the CDC handoff cut for a lock-free
// capture and reports what the capture window actually caught. It
// returns the PRE-snapshot cut — the duplicate side — always, in
// whichever mode it was read: on a GTID-mode source the handoff is the
// pre-snapshot executed set, and the probe below still compares the two
// binlog TIPS, which is the half that moves for every commit
// (cdc_snapshot_position.go says why the tip is read before the set).
//
// # Why pre, unconditionally
//
// See the ordering table in openBinlogSnapshotStreamShared. Handing back
// `post` is the ordering where a commit inside the window is in neither
// the snapshot nor the tail; handing back `pre` is the ordering where it
// is in both. Re-delivery is recoverable and loss is not, so the
// direction is not a judgement call.
//
// # The probe, and what it is evidence of
//
// `pre` is read before the snapshot transaction opens and `post` inside
// it, so the window is a SUBSET of the interval between the two reads.
// If the binlog tip did not move across that interval, nothing committed
// inside it — so nothing committed inside the window either, and the
// position is not merely safe but EXACT: no duplicates, no loss.
//
// The premise that makes the probe mean anything is that every committed
// change advances the binlog tip. It is not assumed silently: this
// capture path runs behind [preflightBinlogFormat] (refuses a
// STATEMENT/MIXED source) and [preflightBinlogRowImage], and
// [snapshotMasterStatus] returning a position at all requires binary
// logging to be on. A source that satisfies all three cannot commit a
// row change without moving the tip.
// TestLockFreeCaptureWindow_ReturnsTheEarlierTip pins the resolver's
// branches; TestLockFreeSnapshotCapturesPositionBeforeSnapshot
// (integration) pins the ORDER of the two statements against a real
// server, reading it out of `mysql.general_log`.
//
// The probe over-reports and never under-reports: the tip also moves for
// commits to tables outside the sync's scope, so a moved tip means "the
// server was writing", not necessarily "your rows were duplicated".
// That asymmetry is the right one for a warning.
func resolveLockFreeCapture(ctx context.Context, pre, post snapshotCut) snapshotCut {
	if pre.tip() == post.tip() {
		slog.InfoContext(
			ctx,
			"mysql: snapshot: lock-free capture window was quiet — the binlog tip did not move between the "+
				"position capture and the consistent snapshot, so no commit landed inside the window and the "+
				"CDC handoff position is exact (no duplicate delivery, no gap)",
			"binlog_file", pre.File,
			"binlog_pos", pre.Pos,
		)
		return pre
	}
	slog.WarnContext(
		ctx,
		"mysql: snapshot: the source committed during the lock-free capture window (no FLUSH TABLES WITH READ "+
			"LOCK was available to freeze it). The CDC tail starts at the EARLIER position, so anything that "+
			"committed inside the window is delivered again rather than skipped — the idempotent apply absorbs "+
			"a re-delivered change on a table with a PRIMARY KEY or a NOT NULL UNIQUE index. A table with "+
			"NEITHER has no key for the apply to collide on, so a re-delivered INSERT on such a table can "+
			"produce a duplicate row (the ADR-0089 at-least-once accounting): quiesce writers for the capture, "+
			"or give those tables a key",
		"binlog_file_before", pre.File,
		"binlog_pos_before", pre.Pos,
		"binlog_file_after", post.File,
		"binlog_pos_after", post.Pos,
	)
	return pre
}
