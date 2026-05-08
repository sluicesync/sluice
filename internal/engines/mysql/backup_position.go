// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// CaptureBackupPosition implements [ir.BackupPositionCapturer]. Returns
// the source's current binlog cursor — `@@global.gtid_executed` when
// GTID mode is on, or `(file, position)` otherwise — encoded the same
// way the CDC reader emits via [encodeBinlogPos]. Phase 3.3: the full
// backup orchestrator records this on [ir.Manifest.EndPosition] so a
// subsequent incremental can resume CDC from the cursor at end-of-
// backup.
//
// MySQL has no slot concept; the slotName argument is accepted for
// interface uniformity and ignored. Empty input is fine; the binlog
// stream is always available to a privileged connection.
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
	useGTID, err := gtidModeOn(ctx, r.db)
	if err != nil {
		return ir.Position{}, fmt.Errorf("mysql: CaptureBackupPosition: detect gtid mode: %w", err)
	}
	if useGTID {
		set, err := executedGTIDSet(ctx, r.db)
		if err != nil {
			return ir.Position{}, fmt.Errorf("mysql: CaptureBackupPosition: read @@gtid_executed: %w", err)
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
