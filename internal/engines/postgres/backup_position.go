// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// CaptureBackupPosition implements [ir.BackupPositionCapturer]. Returns
// the source's current WAL position paired with the supplied slot name,
// encoded into the same JSON envelope shape the CDC reader emits via
// [encodePGPos]. Phase 3.3 of the logical-backup feature: the full
// backup orchestrator records this on [ir.Manifest.EndPosition] so a
// subsequent incremental can resume CDC from the LSN at end-of-backup.
//
// The recorded position semantically marks the source's cursor at the
// moment of capture (just before the full's manifest flips to
// `complete`). A Phase 3 incremental chained off this manifest opens
// CDC at this LSN; if the slot named slotName exists with a
// `restart_lsn` covering this LSN, CDC resumes via slot reuse — if not,
// `sluice sync start --position-from-manifest` runs the pre-flight
// checks defined in the design doc (slot existence + `wal_keep_size`
// sufficiency + Patroni detection).
//
// Empty slotName falls back to the engine's [defaultSlot] so the
// captured shape matches what the streamer would later look for. The
// slot need not exist at capture time — the CDC handoff path guards
// the slot lifecycle.
func (r *SchemaReader) CaptureBackupPosition(ctx context.Context, slotName string) (ir.Position, error) {
	if r.db == nil {
		return ir.Position{}, errors.New("postgres: CaptureBackupPosition: reader not opened")
	}
	if slotName == "" {
		slotName = defaultSlot
	}
	var lsn string
	if err := r.db.QueryRowContext(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&lsn); err != nil {
		return ir.Position{}, fmt.Errorf("postgres: CaptureBackupPosition: %w", err)
	}
	pos, err := encodePGPos(pgPos{Slot: slotName, LSN: lsn})
	if err != nil {
		return ir.Position{}, fmt.Errorf("postgres: CaptureBackupPosition: %w", err)
	}
	return pos, nil
}
