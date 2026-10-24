// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "context"

// The added table's replay window (Bugs 333 and 336).
//
// A drained `sluice schema add-table` copies the new table while the stream
// is stopped, and the restarted stream resumes from its pre-add position. So
// the stream replays changes to the table the copy already holds, onto a
// target table that is AHEAD of them: the copy holds the source as of the
// position it completed at. Which changes are replayed depends on the lane:
// on the binlog every change since the stream stopped, on a VStream the same
// plus the changes vttablet replayed inside the copy, on Postgres the changes
// committed between the add's ALTER PUBLICATION and its snapshot.
//
// A replayed change is only safe on such a target if applying it converges
// whatever the target holds, and the ordinary apply does not: it is
// compare-and-set (VStream: an UPDATE or DELETE addressed by the full
// before-image misses, silently, when the row has moved on) or key-addressed
// (binlog and Postgres: a replayed key change X→Y moves whatever row sits at X
// now). Measured, at exit 0: a VStream key change followed by an INSERT
// reusing the old key lost the moved row (Bug 333); on binlog and Postgres,
// `UPDATE id=-X WHERE id=X; UPDATE id=X WHERE id=W; DELETE id=-X` moved the
// copy's row X away and deleted it (Bug 336); and a key change the copy had
// already seen wedged the stream on a duplicate key on every lane.
//
// The fix is a WINDOW: the add records the position its copy is complete at,
// and the restarted stream applies the table's changes up to that position
// idempotently — every INSERT and UPDATE as an upsert of its complete
// after-image, a key change as a delete of the old key plus an upsert of the
// new row, every DELETE by key. Per key, the last change of the replay then
// decides the row, whatever the copy held, which is the source's value at the
// copy's position. Past the position the target equals the source and the
// ordinary apply resumes.

// AddedTableReplayWindow is one added table's window: the source position its
// copy is complete at, the key its idempotent replay addresses rows by, and
// the columns a complete after-image carries.
//
// A zero CopyPosition records that the table has NO window: its add predates
// the record (sluice v0.163.10 and earlier), or its copy came from a cold
// start, whose stream starts at the copy's own position. The entry exists so
// a later start does not report the table as unrecorded again.
type AddedTableReplayWindow struct {
	Table        string
	CopyPosition Position
	// Key is the column set the replay deletes by and detects a key change
	// on: the table's primary key, else the deterministic non-null unique
	// key the copy's idempotent writer upserts on. Empty only when
	// CopyPosition is zero.
	Key []string
	// Columns are the table's non-generated columns when it was added: an
	// after-image missing one of them is partial (pgoutput's unchanged-TOAST
	// omission), and an upsert cannot replay it. Empty only when
	// CopyPosition is zero.
	Columns []string
}

// HasWindow reports whether w bounds a replay (a recorded copy position).
func (w AddedTableReplayWindow) HasWindow() bool { return w.CopyPosition.Token != "" }

// AddedTableReplayWindowStore is the optional [ChangeApplier] surface that
// keeps a stream's added-table replay windows on its row of the per-target
// `sluice_cdc_state` table (the added_table_copy_positions column). The
// column is added by the writes below on their first use, never at startup,
// and by nothing else: a control table this binary cannot ALTER keeps every
// other write working, and only an add-table refuses (at its Prepare, before
// it changes anything).
//
// The value round-trips through a store, so it is a codec: every position
// token, table and column name comes back byte for byte, or the write
// refuses. Implemented by the Postgres and MySQL-family appliers.
type AddedTableReplayWindowStore interface {
	// PrepareAddedTableReplayWindows makes sure a window can be recorded —
	// the column exists with the type the codec needs — and writes nothing.
	// `schema add-table` calls it before it changes anything, so a target
	// that cannot hold the window refuses there rather than after the copy.
	PrepareAddedTableReplayWindows(ctx context.Context, streamID string) error

	// RecordAddedTableReplayWindows writes windows, replacing any entry for
	// the same tables and keeping the rest. The row must exist.
	RecordAddedTableReplayWindows(ctx context.Context, streamID string, windows []AddedTableReplayWindow) error

	// ResetAddedTableReplayWindows replaces every entry with windows (a cold
	// start: its copy re-seeds the target, so no earlier window can still
	// bound anything). A value that does not decode is overwritten.
	ResetAddedTableReplayWindows(ctx context.Context, streamID string, windows []AddedTableReplayWindow) error

	// ReadAddedTableReplayWindows returns streamID's entries: none when there
	// is no row, no control table, or no column. A value that does not decode
	// is an [UndecodableTableSetError].
	ReadAddedTableReplayWindows(ctx context.Context, streamID string) ([]AddedTableReplayWindow, error)
}

// AddedTableReplayWindowSource is the optional [Engine] capability declaring
// that a drained add-table on this source needs a replay window — the
// stopped stream's change stream carries changes to the added table that its
// copy already holds — and how to bound it.
//
// The window's rule is: a row change at position p is inside the window
// unless [PositionOrderer.PositionAtOrAfter](p, copyPosition) holds. So a
// declaring engine owes, and a test for each:
//
//   - a copy position no change the copy holds is at or after (each
//     engine's AddedTableCopyPosition says how its row positions make that
//     true); a position later than strictly needed is safe, since the
//     idempotent form is right whatever the target holds, so where the
//     boundary is uncertain the engine errs late;
//   - row changes whose after-image is complete, or that the window can tell
//     are not (an after-image missing one of the table's non-generated
//     columns). The window replays a partial after-image of a key-preserving
//     UPDATE as a key-addressed UPDATE of the columns it carries, and refuses
//     one of a key-changing UPDATE (ADDED-TABLE-REPLAY-IMAGE-INCOMPLETE).
//
// Declared by the MySQL family (binlog and VStream) and Postgres.
type AddedTableReplayWindowSource interface {
	PositionOrderer

	// AddedTableCopyPosition is the position the add records as its copy's
	// bound, in the form the stream's own row positions are compared in:
	// snapshot is the add's snapshot stream position (after its copy), and
	// resumeFrom the stream's persisted position, which the restart resumes
	// from (Postgres names its slot there; the add's snapshot rides a
	// temporary one).
	AddedTableCopyPosition(snapshot, resumeFrom Position) (Position, error)
}
