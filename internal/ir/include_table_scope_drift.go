// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"errors"
)

// ErrIncludeTableScopeDrift classifies the warm-resume refusal of a stream
// whose current table scope holds a table the stream never brought in
// (INCLUDE-TABLE-SCOPE-DRIFT, Bug 321). A warm resume copies nothing: it
// continues the change stream from the persisted position, so a table that
// joins the scope by a widened `--include-table` (or a narrowed
// `--exclude-table`) is never copied, and on a Postgres pgoutput source it is
// not even streamed — the publication the cold start scoped does not list it.
// The stream used to come back green while the table stayed empty or absent
// on the target, at exit 0. Bringing a table in is `sluice schema
// add-table`'s job; the refusal names that and the re-copy as its remedies.
//
// It covers two arms:
//
//   - a table the stream's publication does not list (pgoutput source,
//     single-namespace stream): the server never decodes its changes;
//   - a table whose `sluice schema add-table` started and did not complete
//     (every source): the add records the table as pending before it changes
//     anything and clears the record only once the table is fully copied and
//     handed to the stream ([PendingAddedTablesStore]), so a failed or
//     interrupted add cannot leave a table the next restart streams into
//     with its original rows missing.
//
// Both read state a restart reads back unchanged — the source's publication
// catalog and the control row — so it is in the fleet supervisor's
// do-not-restart roster.
var ErrIncludeTableScopeDrift = errors.New("INCLUDE-TABLE-SCOPE-DRIFT")

// PendingAddedTablesStore is the optional [ChangeApplier] surface that
// records, on the stream's row of the per-target `sluice_cdc_state` table,
// the tables whose `sluice schema add-table` has started and not completed.
//
// `sluice schema add-table` records its table before the first change it
// makes (the target CREATE, the publication add) and clears it only after the
// copy and the hand-off to the stream have succeeded. A failure in between —
// an error, a Ctrl-C, a killed process — leaves the record, and `sync start`
// refuses a warm resume whose scope includes the table
// ([ErrIncludeTableScopeDrift]) instead of streaming changes into a table
// whose copy never landed. A cold start that copies the table clears it.
//
// Implemented by the Postgres and MySQL-family appliers, the two that keep a
// control table. The column is added on the first Record, not at every
// start, so a role that cannot ALTER a pre-created control table runs every
// stream it ran before; a control table without the column holds no record.
type PendingAddedTablesStore interface {
	// RecordPendingAddedTable adds table to streamID's pending set. The row
	// must exist (add-table refuses a stream without one first); a missing row
	// is an error, never a silent no-op, because an add that could not record
	// itself must not go on to change anything. Idempotent.
	RecordPendingAddedTable(ctx context.Context, streamID, table string) error

	// ClearPendingAddedTables removes tables from streamID's pending set.
	// Idempotent and tolerant of a missing row, table or column.
	ClearPendingAddedTables(ctx context.Context, streamID string, tables []string) error

	// ReadPendingAddedTables returns streamID's pending set: empty when there
	// is no row, no control table, or no column (Record adds the column before
	// it writes, so a table without it has never held a record). A stored
	// value that does not decode is an error: an unreadable record refuses,
	// it does not read as "nothing pending".
	ReadPendingAddedTables(ctx context.Context, streamID string) ([]string, error)
}
