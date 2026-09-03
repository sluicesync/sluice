// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
)

// The sync snapshot openers' position capture (audit 2026-09-01 SLM-4).
//
// Both `sync` cold-start openers — the serial one in cdc_snapshot.go and
// the N-way one in cdc_snapshot_concurrent.go — used to stamp a file/pos
// handoff position unconditionally, while the two backup capture doors
// and the from-now open detected `gtid_mode` through [gtidModeOnFor] and
// stamped a GTID set when it was ON. Observed on a real gtid_mode=ON
// mysql:8.0: `sync start` persisted `{"mode":"file_pos",…,"server_uuid":…}`.
// The consequence is the exact case GTID mode exists for: after a
// failover to a promoted replica the resume is a file/pos position bound
// to the OLD primary's @@server_uuid, the identity guard refuses it, and
// the refusal's fall-through drops the target tables and re-copies — a
// loud, destructive outcome on a working configuration, and one the
// operator doc's GTID-resume promise said would not happen.
//
// This file is the one place the sync openers decide the arm, so the
// decision cannot diverge between them again, and it decides it with the
// SAME resolver every other capture door on the engine uses. The roster
// of capture doors that stamp a position, and which arm each takes:
//
//	openBinlogSnapshotStreamShared      sync (serial)      snapshotHandoffPosition
//	openBinlogSnapshotStreamConcurrent  sync (N-way)       snapshotHandoffPosition
//	SchemaReader.CaptureBackupPosition  backup (pool)      gtidModeOnFor, inline
//	captureBackupPosition               backup (conn)      gtidModeOnFor, inline
//	CDCReader.resolveStartPosition      from-now open      gtidModeOnFor, inline
//	CDCReader.positionFor               every streamed event — copies the
//	                                    start position's mode forward
//	vstreamSnapshotStream.positionFor   planetscale/vitess — VGTID, no
//	vstreamCDCReader.positionFor        binlog arm to choose (exempt)
//
// MariaDB reaches the GTID arm on every door unconditionally (it has no
// gtid_mode; see [gtidModeOnFor]), so a MariaDB `sync` cold start now
// anchors in GTID mode like its backups do — a position persisted by
// v0.138.0 or earlier is file/pos plus the lineage anchor, which
// [verifyMariaDBLineage] keeps accepting.

// snapshotCut is what a sync snapshot opener reads at its consistent cut:
// the binlog tip, and — when the source is in GTID mode — the executed set
// at that same tip. Under FLUSH TABLES WITH READ LOCK the two are read at
// one frozen instant; on the lock-free fallback they bracket the
// snapshot and the EARLIER cut is the handoff (see
// [resolveLockFreeCapture]).
type snapshotCut struct {
	File string
	Pos  uint32
	// GTIDSet is @@global.gtid_executed (@@gtid_binlog_pos on MariaDB) read
	// at the same cut; empty when the source is not in GTID mode.
	GTIDSet string
}

// tip is the file/pos half, for the lock-free window probe and the
// MariaDB lineage anchor, both of which are about the binlog byte.
func (c snapshotCut) tip() binlogTip { return binlogTip{File: c.File, Pos: c.Pos} }

// snapshotPositionMode reports which arm a sync opener stamps. It is
// [gtidModeOnFor] — the resolver the backup doors and the from-now open
// already use — with the openers' error wording; a source whose mode
// cannot be read refuses the open loudly, as [SchemaReader.CaptureBackupPosition]
// does, rather than silently taking the file/pos arm.
func snapshotPositionMode(ctx context.Context, q rowQuerier, flavor Flavor) (useGTID bool, err error) {
	useGTID, err = gtidModeOnFor(ctx, q, flavor)
	if err != nil {
		return false, fmt.Errorf("mysql: snapshot: detect gtid mode: %w", err)
	}
	return useGTID, nil
}

// captureSnapshotCut reads the binlog tip on conn and, when useGTID, the
// executed set. The tip is read FIRST: on the lock-free path the window
// probe compares two tips, and a commit that lands between the tip read
// and the set read then moves the post-snapshot tip and draws the
// re-delivery WARN even though the set already covers it — the probe
// overstates a possible duplicate rather than ever understating a gap.
func captureSnapshotCut(ctx context.Context, conn *sql.Conn, flavor Flavor, useGTID bool) (snapshotCut, error) {
	file, pos, err := snapshotMasterStatus(ctx, conn)
	if err != nil {
		return snapshotCut{}, err
	}
	cut := snapshotCut{File: file, Pos: pos}
	if useGTID {
		set, err := coldStartGTIDSetFor(ctx, conn, flavor)
		if err != nil {
			return snapshotCut{}, err
		}
		cut.GTIDSet = set
	}
	return cut, nil
}

// snapshotHandoffPosition builds the position the CDC tail resumes from
// out of the cut an opener captured: a GTID-set position when the source
// is in GTID mode — the shape the backup doors stamp, which the handoff,
// [CDCReader.verifyPositionResumableInner]'s GTID arm and
// [verifyGTIDLineageContinuity] already accept — and a file/pos position
// bound to the instance's @@server_uuid otherwise. MariaDB takes the
// lineage anchor on the GTID arm (mariadb_lineage.go); q must be the
// capturing connection, since BINLOG_GTID_POS is asked about the byte
// the cut was read at.
func (e Engine) snapshotHandoffPosition(ctx context.Context, q rowQuerier, cut snapshotCut, useGTID bool, serverUUID string) binlogPos {
	if useGTID {
		p := binlogPos{Mode: positionModeGTID, GTIDSet: cut.GTIDSet}
		if e.Flavor == FlavorMariaDB {
			p = captureMariaDBLineageAnchor(ctx, q, p, cut.File, cut.Pos)
		}
		return p
	}
	return binlogPos{
		Mode:       positionModeFilePos,
		File:       cut.File,
		Pos:        cut.Pos,
		ServerUUID: serverUUID,
	}
}

// logSnapshotHandoff is the openers' shared "captured" line, naming the
// arm so a run's log says which resume mode the cold start is on
// without the operator decoding the token.
func logSnapshotHandoff(ctx context.Context, msg, freeze string, cut snapshotCut, useGTID bool) {
	mode := positionModeFilePos
	if useGTID {
		mode = positionModeGTID
	}
	slog.InfoContext(
		ctx, msg,
		"freeze", freeze,
		"position_mode", string(mode),
		"binlog_file", cut.File,
		"binlog_pos", cut.Pos,
		"gtid_set", cut.GTIDSet,
	)
}
