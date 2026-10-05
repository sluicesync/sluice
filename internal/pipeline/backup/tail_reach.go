// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// EndPositionUnreached is THE tail-reach rule for an incremental: true when
// its manifest records an advanced EndPosition that the last position-bearing
// change its chunks record does not equal. lastRecorded is that change's
// position (the zero Position when the chunks record none).
//
// Two shapes are exempt, both legitimate writer output with no change AT
// EndPosition: an empty EndPosition (a one-shot window that recorded nothing;
// readers resolve it against StartPosition), and EndPosition == StartPosition
// (an empty rollover written anyway, a DDL-only rollover whose one chunk
// carries only a schema snapshot, a window that never advanced).
//
// One predicate, three callers, so they cannot disagree:
//   - chain restore's in-apply tail backstop ([ChainRestore.streamIncrementalChanges]);
//   - the broker's in-apply backstop and its zero-chunk guard
//     (pipeline.SyncFromBackup) — which, before this function, lacked the
//     StartPosition exemption and so refused a DDL-only stream rollover that
//     restore accepted;
//   - the severed-transaction door's shape C ([endPastRecordedError]), which
//     runs it BEFORE anything is applied.
//
// Graded across all of them, and `backup verify`, by
// TestSeveredTransactionDoor_ShapeC_AgreementTable.
func EndPositionUnreached(m *irbackup.Manifest, lastRecorded ir.Position) bool {
	end := m.EndPosition
	if (end.Engine == "" && end.Token == "") || end == m.StartPosition {
		return false
	}
	return lastRecorded != end
}
