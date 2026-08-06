// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # The unlocked backup capture window (audit B-4b)
//
// [Engine.openBackupSnapshotSerial] has to name one logical instant twice,
// exactly as the cold-start capture in cdc_snapshot_lockfree.go does: as a
// row VIEW (the consistent-snapshot transaction the table sweep reads
// under) and as the chain ANCHOR (the manifest's EndPosition, which is
// where the next `backup incremental` resumes the binlog).
//
// Unlike the coordinated open next door it holds NO lock. It is the floor
// every FTWRL failure falls back to, and on AWS RDS the platform blocks
// FTWRL for the master user regardless of grants — so on RDS this is not a
// fallback, it is the path. The two reads therefore bracket a real window,
// and the order is not a style question:
//
//	snapshot → anchor  a commit inside the window is ABOVE the read view
//	                   (the sweep never sees it) and BELOW the anchor (the
//	                   incremental starts after it). It is in NEITHER the
//	                   full nor any incremental chained off it. Silent loss.
//	anchor → snapshot  the same commit is BELOW the read view (the sweep
//	                   reads it) and ABOVE the anchor (the incremental
//	                   replays it). It is in BOTH. Duplicate delivery.
//
// So the anchor is captured FIRST. Stated as the property the chain needs:
// the full's data must be a SUPERSET of everything at or below the recorded
// EndPosition. Capturing the anchor first makes that true by construction;
// capturing it second made it false for the width of a round-trip.
//
// # What the chain invariants require, checked before the order was flipped
//
// The order was left unflipped when B-4 landed because the anchor is
// load-bearing for incremental lineage and ADR-0088's replay-window
// invariants, and that had not been validated. It has been now; each
// consumer of the anchor is listed with what it needs:
//
//   - Lineage (`incremental.Run` step 1): `startPos = parent.EndPosition`,
//     and the window is (anchor, …]. An EARLIER anchor only ever WIDENS the
//     window. Widening replays; narrowing skips. Safe direction.
//   - `assertDataWindowEndPositionInvariant`: returns early on
//     `len(ChangeChunks) == 0`. A full has none, so a full's anchor never
//     reaches it. Not affected.
//   - Resume-anchor adoption (`backup_resume_anchor`): a resumed run ADOPTS
//     the first attempt's recorded anchor rather than its own. Unchanged —
//     this changes which instant an anchor names, not which anchor is kept.
//   - BackupID / manifest path: derived FROM the anchor, with no ordering
//     predicate over it. Not affected.
//   - Chain ack (`releaseChainAckTo`): server-side consumer state, i.e.
//     Postgres slots. MySQL has none; retention is
//     binlog_expire_logs_seconds. An earlier anchor needs marginally more
//     retention — one round-trip's worth, against a window operators
//     configure in hours.
//   - Restore replay (`chain_restore`): incremental change chunks go through
//     [ir.ChangeApplier.ApplyBatch], which is idempotent per ADR-0010. A
//     re-delivered change over a row the full already restored is an upsert
//     onto itself. The exception is the keyless table, below.
//
// Nothing here required the losing order, and nothing here required a
// change of its own.
//
// # The residual, and who pays it
//
// This does not CLOSE the window — it moves the residual from loss to
// duplication. A table with a PRIMARY KEY or a NOT NULL UNIQUE index has a
// key for the idempotent apply to collide on and pays nothing. A table with
// NEITHER has no such key, so a re-delivered INSERT can produce a duplicate
// row on restore: the ADR-0089 at-least-once accounting, identical to the
// residual B-4 documented for the cold-start lane.
//
// # The probe
//
// The window is a SUBSET of the interval between the two binlog-tip reads
// below (the near tip is read before the anchor, the far tip after the read
// view freezes). If the tip did not move across that interval, nothing
// committed inside it — so nothing committed inside the window either, and
// the anchor is not merely safe but EXACT.
//
// The premise that gives the probe meaning is that every committed change
// advances the binlog tip. That premise is CHECKED rather than assumed, and
// the check is the probe itself: [snapshotMasterStatus] returns a position
// only when binary logging is on, so a tip that reads at all is a tip that
// moves. When either read fails the probe reports UNKNOWN and never claims
// exactness — which is also the honest answer for the one shape where the
// premise would otherwise fail silently, a `gtid_mode=ON` + `log_bin=OFF`
// source whose local commits get no GTID.
//
// The probe over-reports and never under-reports: the tip also moves for
// commits to tables outside the backup's scope, so a moved tip means "the
// server was writing", not necessarily "your rows were duplicated". That is
// the right asymmetry for a warning.

package mysql

import (
	"context"
	"database/sql"
	"log/slog"
)

// readBinlogTip reads one edge of the capture window, best-effort. A nil
// return means the tip is unreadable on this source; the caller degrades
// the probe to UNKNOWN rather than failing the backup, because the probe is
// evidence about the window, not the thing that makes the window safe. The
// ORDER is what makes it safe, and the order does not depend on this.
func readBinlogTip(ctx context.Context, conn *sql.Conn) *binlogTip {
	file, pos, err := snapshotMasterStatus(ctx, conn)
	if err != nil {
		return nil
	}
	return &binlogTip{File: file, Pos: pos}
}

// reportBackupCaptureWindow logs what the unlocked capture window actually
// caught, given the two tips that bracket it. It decides nothing — the
// anchor was already captured on the safe side — so it returns nothing;
// its whole job is to turn a permanent caveat nobody can act on into a
// per-run fact.
func reportBackupCaptureWindow(ctx context.Context, pre, post *binlogTip) {
	switch {
	case pre == nil || post == nil:
		slog.WarnContext(
			ctx,
			"mysql: backup snapshot: could not read the binlog tip around the unlocked capture window, so "+
				"whether a commit landed inside it is UNKNOWN. The chain anchor was still taken BEFORE the "+
				"snapshot opened, so such a commit is re-delivered by the next incremental rather than lost; "+
				"on a table with neither a PRIMARY KEY nor a NOT NULL UNIQUE index that re-delivery can "+
				"duplicate a row (ADR-0089 at-least-once)",
			"tip_before_readable", pre != nil,
			"tip_after_readable", post != nil,
		)
	case *pre == *post:
		slog.InfoContext(
			ctx,
			"mysql: backup snapshot: unlocked capture window was quiet — the binlog tip did not move between "+
				"the chain-anchor capture and the consistent snapshot, so no commit landed inside the window "+
				"and the anchor is exact (no duplicate replay, no gap)",
			"binlog_file", pre.File,
			"binlog_pos", pre.Pos,
		)
	default:
		slog.WarnContext(
			ctx,
			"mysql: backup snapshot: the source committed during the unlocked capture window (no FLUSH TABLES "+
				"WITH READ LOCK was available to freeze it). The chain anchor is the EARLIER position, so "+
				"anything that committed inside the window is in the full AND replayed by the next "+
				"incremental rather than missing from both — the idempotent apply absorbs the replay on a "+
				"table with a PRIMARY KEY or a NOT NULL UNIQUE index. A table with NEITHER has no key for "+
				"the apply to collide on, so a replayed INSERT on such a table can produce a duplicate row "+
				"(the ADR-0089 at-least-once accounting): quiesce writers for the capture, grant RELOAD so "+
				"the coordinated FTWRL open engages, or give those tables a key",
			"binlog_file_before", pre.File,
			"binlog_pos_before", pre.Pos,
			"binlog_file_after", post.File,
			"binlog_pos_after", post.Pos,
		)
	}
}
