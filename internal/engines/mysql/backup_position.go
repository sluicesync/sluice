// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"sluicesync.dev/sluice/internal/ir"
)

// binlogEnabledQuery reads whether the source is writing a binary log at
// all. A failed read FAILS the capture (see [binlogEnabled]), so "the
// server answers this variable" is a PREMISE this preflight rests on —
// and a premise about somebody else's server is exactly the thing this
// project keeps writing down instead of measuring. The roster, per
// flavor, measured rather than assumed:
//
//   - mysql, mariadb — ground-truthed 2026-08-07 on MySQL 8.0.46 and
//     MariaDB 11.4, both booted --skip-log-bin: `@@GLOBAL.log_bin` reads
//     0 rather than erroring or going missing. Both capturer doors are
//     reachable on these two. Pinned by TestCaptureBackupPosition_NoBinlog.
//   - planetscale, vitess — the connection terminates at VTGATE, not at
//     mysqld, and vtgate serves its own PARTIAL system-variable surface,
//     so neither measurement above says anything about these two. Measured
//     2026-08-07 on the real multi-process Vitess 24.0.1 cluster
//     (testdata/vitesscluster: etcd + vtctld + primary/replica vttablet +
//     vtgate): `SELECT @@GLOBAL.log_bin` returns 1. vtgate does not answer
//     it locally — it ROUTES the query to a tablet, which is visible when
//     no tablet is serving yet ("target: test.0.primary: no healthy tablet
//     available for … tablet_type:PRIMARY"). So the value is that tablet's
//     mysqld's, and a Vitess tablet runs mysqld WITH binary logging —
//     VReplication and VStream both ride it — which is why 1 here is
//     structural rather than a lucky default. Pinned by
//     TestVStream_BackupPositionProbesAnswerThroughVtgate (per-PR, vtcombo)
//     and TestVitessCluster_BackupPositionProbesAnswerThroughVtgate (the
//     weekly version matrix, vtgate v21..v24 — the axis on which a claim
//     about someone else's proxy is likeliest to rot).
//
// Because that read is ROUTED, it fails when no tablet is serving. That
// is not a NEW failure class on the only path a VStream flavor can reach
// this preflight from: [SchemaReader.CaptureBackupPosition]'s own GTID
// arm reads `SHOW VARIABLES LIKE 'gtid_mode'` and `@@global.gtid_executed`
// through the same routing, so a tablet-less vtgate already failed the
// capture before the preflight existed. Softening an unreadable variable
// into "assume ON" was therefore rejected: it would buy nothing on the
// flavors where the read is routed, and on the flavors where it is not it
// would hand the GTID arm back a source it cannot describe — the exact
// silent-loss shape (a well-formed, resumable-looking, EMPTY GTID cursor)
// the preflight exists to close.
//
// Reachability on the two VStream flavors is narrow, and narrow is not
// zero: `backup full` on a healthy planetscale/vitess source anchors on
// [Engine.openBackupSnapshotVStream] and never calls either capturer. Only
// the DEGRADED fallback reaches this — the VStream snapshot open failed
// and --chain-slot was not requested, so the orchestrator falls through to
// captureEndPosition — i.e. precisely when vtgate is already unhappy.
const binlogEnabledQuery = "SELECT @@GLOBAL.log_bin"

// binlogEnabled reports whether the source has binary logging on — the
// premise both backup-position capturers rest on, and the one that used
// to be asserted rather than read (see [SchemaReader.CaptureBackupPosition]).
// An unreadable variable is an ERROR, not a "probably on"; the flavor
// roster behind that choice is on [binlogEnabledQuery].
func binlogEnabled(ctx context.Context, q rowQuerier) (bool, error) {
	var on bool
	if err := q.QueryRowContext(ctx, binlogEnabledQuery).Scan(&on); err != nil {
		return false, fmt.Errorf("read @@GLOBAL.log_bin: %w", err)
	}
	return on, nil
}

// warnNoBinlogCursor says, once per capture, that this backup records NO
// resume cursor and why.
//
// The alternative — failing the backup — was rejected deliberately: a
// standalone `backup full` of a binlog-less source (a reporting replica,
// a restored dev copy) is a legitimate thing to want, and refusing it
// would break a use case to protect a chain the operator may never
// build. What is NOT legitimate is recording a cursor that looks real
// and covers nothing, which is what this used to do. The empty
// [ir.Position] is the truthful value and it already has meaning
// downstream: `backup incremental` WARNs that it must start "from now",
// and `sluice sync start --position-from-manifest` refuses outright.
func warnNoBinlogCursor(ctx context.Context) {
	slog.WarnContext(
		ctx, "backup: the source has binary logging DISABLED (@@GLOBAL.log_bin = 0), so this backup records NO end position — it is a standalone full, not a chain root",
		slog.String("consequence", "`backup incremental` against it can only start from the CDC position at the time it runs, leaving the window between this backup and that moment uncovered; `sync start --position-from-manifest` will refuse it"),
		slog.String("remedy", "to chain from this backup, enable binary logging on the source (log_bin=ON, binlog_format=ROW, binlog_row_image=FULL) and take the full again"),
	)
}

// CaptureBackupPosition implements [irbackup.PositionCapturer]. Returns
// the source's current binlog cursor — `@@global.gtid_executed` when
// GTID mode is on, or `(file, position)` otherwise — encoded the same
// way the CDC reader emits via [encodeBinlogPos]. Phase 3.3: the full
// backup orchestrator records this on [irbackup.Manifest.EndPosition] so a
// subsequent incremental can resume CDC from the cursor at end-of-
// backup.
//
// MySQL has no slot concept; the slotName argument is accepted for
// interface uniformity and ignored.
//
// CORRECTION (2026-08-07): this doc used to close with "Empty input is
// fine; the binlog stream is always available to a privileged
// connection." That premise was never checked and it is FALSE. A source
// with `log_bin=OFF` has no binlog stream at all, and the two arms below
// disagreed about it: the file/pos arm already refused loudly (`SHOW
// MASTER STATUS` returns zero rows), while the GTID arm read an EMPTY
// `@@global.gtid_executed` / `@@gtid_binlog_pos` and encoded it into a
// perfectly well-formed-looking position token — a backup that verifies
// clean and cannot be resumed from. Both spellings of that shape are
// reachable and were ground-truthed: MySQL 8.0.46 ACCEPTS
// `--skip-log-bin --gtid-mode=ON --enforce-gtid-consistency=ON` and
// reports `gtid_mode=ON` with `log_bin=0` and an EMPTY
// `gtid_executed`; and MariaDB needs
// no such contrivance, since [gtidModeOnFor] reports GTID mode ON for
// that flavor UNCONDITIONALLY, so every binlog-less MariaDB took the
// GTID arm. `backup_snapshot_lockfree.go` had already named the
// `gtid_mode=ON` + `log_bin=OFF` shape in prose; this is the check.
//
// So the binlog cursor is now PREFLIGHTED, before either arm, and a
// binlog-less source records the empty [ir.Position] plus a WARN rather
// than a plausible fake (see [warnNoBinlogCursor] for why that and not a
// refusal). Pinned by TestCaptureBackupPosition_NoBinlog across both
// binlog families. The conn-scoped sibling [captureBackupPosition] carries
// the same preflight — it had the identical defect.
//
// This is the ONLY capturer door a VStream flavor can reach (the sibling's
// two callers both sit behind `!usesVStream()`), and it reaches it only on
// the degraded fallback. That the preflight's `@@GLOBAL.log_bin` read
// survives a vtgate connection at all is a measured premise, not an
// assumption — the roster is on [binlogEnabledQuery].
//
// In GTID mode the position is set-membership-comparable: the
// incremental's CDC reader streams every transaction whose GTID is not
// in the recorded set, so the cursor naturally bridges the
// "captured-during-backup" and "post-backup" event windows. In
// file/pos mode the cursor is a byte offset into the named binlog
// file; the incremental's CDC reader resumes from that offset.
func (r *SchemaReader) CaptureBackupPosition(ctx context.Context, _ string) (ir.Position, error) {
	if r.db == nil {
		return ir.Position{}, errors.New("mysql: CaptureBackupPosition: reader not opened")
	}
	on, err := binlogEnabled(ctx, r.db)
	if err != nil {
		return ir.Position{}, fmt.Errorf("mysql: CaptureBackupPosition: %w", err)
	}
	if !on {
		warnNoBinlogCursor(ctx)
		return ir.Position{}, nil
	}
	useGTID, err := gtidModeOnFor(ctx, r.db, r.flavor)
	if err != nil {
		return ir.Position{}, fmt.Errorf("mysql: CaptureBackupPosition: detect gtid mode: %w", err)
	}
	if useGTID {
		set, err := coldStartGTIDSetFor(ctx, r.db, r.flavor)
		if err != nil {
			return ir.Position{}, fmt.Errorf("mysql: CaptureBackupPosition: %w", err)
		}
		pos, err := encodeBinlogPos(binlogPos{Mode: positionModeGTID, GTIDSet: set})
		if err != nil {
			return ir.Position{}, fmt.Errorf("mysql: CaptureBackupPosition: %w", err)
		}
		return pos, nil
	}
	file, p, err := masterStatus(ctx, r.db)
	if err != nil {
		return ir.Position{}, fmt.Errorf("mysql: CaptureBackupPosition: master status: %w", err)
	}
	pos, err := encodeBinlogPos(binlogPos{Mode: positionModeFilePos, File: file, Pos: p})
	if err != nil {
		return ir.Position{}, fmt.Errorf("mysql: CaptureBackupPosition: %w", err)
	}
	return pos, nil
}
