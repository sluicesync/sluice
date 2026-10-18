// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"errors"
)

// ErrIncludeTableScopeDrift classifies the warm-resume refusal of a stream
// whose current scope holds a table the stream never brought in
// (INCLUDE-TABLE-SCOPE-DRIFT, Bug 321). A warm resume copies nothing: it
// continues the change stream from the persisted position, so a table that
// joins the scope on a restart is never copied — on a Postgres pgoutput source
// not even streamed, on a binlog, VStream or trigger source streamed with its
// earlier rows missing. The stream used to come back green at exit 0.
// Bringing a table in is `sluice schema add-table`'s job; the refusal names
// that and the re-copy as its remedies.
//
// It refuses:
//
//   - a table an --include-table entry names literally that the stream's
//     [EstablishedTablesStore] record does not hold (or, for a stream with no
//     record, that its provably unshared publication does not deliver);
//   - any in-scope table whose `sluice schema add-table` started and did not
//     complete ([PendingAddedTablesStore]).
//
// Each reads state a restart reads back unchanged — the control row, the
// source catalog — so it is in the fleet supervisor's do-not-restart roster.
var ErrIncludeTableScopeDrift = errors.New("INCLUDE-TABLE-SCOPE-DRIFT")

// EstablishedTablesStore is the optional [ChangeApplier] surface that records,
// on the stream's row of `sluice_cdc_state`, the tables the stream
// ESTABLISHED: copied by its cold start (every lane, each namespace of a
// multi-database stream) or by a completed `sluice schema add-table`, and
// applied continuously since. A warm resume whose scope drops a table
// narrows the record, because that table's changes are discarded from then
// on and coming back to it needs a fresh copy.
//
// It is the INCLUDE-TABLE-SCOPE-DRIFT door's primary evidence on every
// source: a table in the current scope and not in the record was never
// copied by this stream, whatever the source would deliver for it.
//
// An entry is the bare table name on a single-namespace stream, and
// `<namespace>\x00<table>` on a multi-database one. Implemented by the
// Postgres and MySQL-family appliers; the column is added on first write.
type EstablishedTablesStore interface {
	// ReadEstablishedTables returns the record, and recorded=false when there
	// is none (no row, no column, or NULL: a stream cold-started before this
	// record existed). An undecodable value is an error.
	ReadEstablishedTables(ctx context.Context, streamID string) (tables []string, recorded bool, err error)

	// RecordEstablishedTables replaces the record with exactly tables (the
	// empty set included). The row must exist.
	RecordEstablishedTables(ctx context.Context, streamID string, tables []string) error

	// AddEstablishedTable adds table to the record; a no-op while there is no
	// record (one table is not a record of what the stream established).
	AddEstablishedTable(ctx context.Context, streamID, table string) error
}

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
