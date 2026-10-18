// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"errors"
	"time"
)

// ErrIncludeTableScopeDrift classifies the warm-resume refusal of a stream
// whose current scope holds a table whose changes it would apply onto a target
// copy it never made, or never kept up (INCLUDE-TABLE-SCOPE-DRIFT, Bug 321). A
// warm resume copies nothing: it continues the change stream from the
// persisted position, so a table that joins the scope on a restart is never
// copied — on a Postgres pgoutput source not even streamed, on a binlog,
// VStream or trigger source streamed with its earlier rows missing. The
// stream used to come back green at exit 0. Bringing a table in is `sluice
// schema add-table`'s job; the refusal names that and the re-copy as its
// remedies.
//
// Each conviction reads state a restart reads back unchanged — the control
// row, the source and target catalogs — so it is in the fleet supervisor's
// do-not-restart roster. The one restartable case has its own marker,
// [ErrIncludeTableAddInProgress].
var ErrIncludeTableScopeDrift = errors.New("INCLUDE-TABLE-SCOPE-DRIFT")

// ErrIncludeTableAddInProgress classifies the warm-resume refusal raised
// when the door's only conviction is a `sluice schema add-table` that is
// still running — its pending entry's heartbeat is fresh
// ([PendingAddedTable.Fresh]). The add will clear it, so a restart after it
// finishes resumes: unlike [ErrIncludeTableScopeDrift] this is NOT in the
// fleet supervisor's do-not-restart roster, which backs off and retries.
var ErrIncludeTableAddInProgress = errors.New("INCLUDE-TABLE-ADD-IN-PROGRESS")

// EstablishedRecord is a stream's record of the tables it established, as
// read back. An entry is the bare table name on a single-namespace stream and
// `<namespace>\x00<table>` on a multi-database one.
type EstablishedRecord struct {
	// Recorded is false when the stream has no record: no row, no column,
	// NULL — a stream cold-started before the record existed, or a cold start
	// that reset it and could not write the new one.
	Recorded bool
	// Established are the tables copied by the stream's cold start or by a
	// completed `sluice schema add-table`, and applied continuously since.
	Established []string
	// Dropped are tables that left the stream's scope since they were
	// established: their changes were discarded from then on, so the target
	// copy is stale and a scope that re-admits them refuses until they are
	// copied again.
	Dropped []string
}

// EstablishedTablesStore is the optional [ChangeApplier] surface that keeps a
// stream's [EstablishedRecord] on its row of `sluice_cdc_state`
// (established_tables, dropped_tables). It is the INCLUDE-TABLE-SCOPE-DRIFT
// door's primary evidence on every source. Implemented by the Postgres and
// MySQL-family appliers; a column is added on its first write, never at
// startup.
type EstablishedTablesStore interface {
	// ReadEstablishedRecord returns the record. An undecodable value is an
	// error.
	ReadEstablishedRecord(ctx context.Context, streamID string) (EstablishedRecord, error)

	// ResetEstablishedRecord sets both columns to NULL — "not recorded". A
	// cold start calls it before it copies anything, so a failure anywhere
	// before the new record is written leaves the stream unrecorded, never
	// holding a record that describes an older copy. Tolerant of a missing
	// row or column; issues no DDL.
	ResetEstablishedRecord(ctx context.Context, streamID string) error

	// RecordEstablishedTables replaces the established set (the empty set
	// included). The row must exist.
	RecordEstablishedTables(ctx context.Context, streamID string, tables []string) error

	// RecordDroppedTables replaces the dropped set. The row must exist.
	RecordDroppedTables(ctx context.Context, streamID string, tables []string) error

	// AddEstablishedTable records a completed add-table: table joins the
	// established set and leaves the dropped set. The established half is a
	// no-op while there is no record (one table is not a record of what the
	// stream established) and on a multi-database stream (add-table names a
	// bare table; the record keys namespace-qualified entries).
	AddEstablishedTable(ctx context.Context, streamID, table string) error
}

// Pending add-table liveness. A running add-table refreshes its entry's
// heartbeat every PendingAddHeartbeatInterval; an entry whose heartbeat is
// older than PendingAddStaleAfter belongs to an add that died. Ten missed
// beats: long enough that a slow control-table write or a modest clock skew
// between the add-table host and the reader cannot make a live add look dead,
// short enough that a killed add is reported within minutes.
const (
	PendingAddHeartbeatInterval = 30 * time.Second
	PendingAddStaleAfter        = 5 * time.Minute
)

// PendingAddedTable is one pending add-table entry.
type PendingAddedTable struct {
	Table       string
	StartedAt   time.Time
	HeartbeatAt time.Time
}

// Fresh reports whether the add that wrote the entry was alive within
// [PendingAddStaleAfter] of now.
func (p PendingAddedTable) Fresh(now time.Time) bool {
	return now.Sub(p.HeartbeatAt) < PendingAddStaleAfter
}

// PendingAddedTablesStore is the optional [ChangeApplier] surface that
// records, on the stream's row of the per-target `sluice_cdc_state` table,
// the tables whose `sluice schema add-table` has started and not completed.
//
// `sluice schema add-table` records its table before the first change it
// makes (the target CREATE, the publication add), refreshes the entry's
// heartbeat while it copies, and clears it only after the copy and the
// hand-off to the stream have succeeded. A failure in between — an error, a
// Ctrl-C, a killed process — leaves the entry, and `sync start` refuses a
// warm resume whose scope includes the table instead of streaming changes
// into a table whose copy never landed. A cold start that copies the table,
// and `sluice schema add-table --abandon`, clear it.
//
// Implemented by the Postgres and MySQL-family appliers. The column is added
// on the first Record, not at every start; a control table without it holds
// no record.
type PendingAddedTablesStore interface {
	// RecordPendingAddedTable writes table's entry, started and alive at now.
	// The row must exist; a missing row is an error, because an add that
	// could not record itself must not go on to change anything.
	RecordPendingAddedTable(ctx context.Context, streamID, table string, now time.Time) error

	// HeartbeatPendingAddedTable refreshes table's heartbeat to now. Tolerant
	// of an entry that is gone (an --abandon raced the add).
	HeartbeatPendingAddedTable(ctx context.Context, streamID, table string, now time.Time) error

	// ClearPendingAddedTables removes tables' entries. Idempotent and
	// tolerant of a missing row, table or column.
	ClearPendingAddedTables(ctx context.Context, streamID string, tables []string) error

	// ReadPendingAddedTables returns streamID's entries: none when there is no
	// row, no control table, or no column. A stored value that does not decode
	// is an error: an unreadable record refuses, it does not read as "nothing
	// pending".
	ReadPendingAddedTables(ctx context.Context, streamID string) ([]PendingAddedTable, error)
}

// PublicationMembership is what a publication source reports about a stream's
// publication for the INCLUDE-TABLE-SCOPE-DRIFT door.
type PublicationMembership struct {
	// Publication names it; empty means it does not exist.
	Publication string
	// Missing are the asked-about tables it does not deliver
	// (pg_publication_tables, the server's own expansion).
	Missing []string
	// AllTables: the publication is FOR ALL TABLES.
	AllTables bool
	// Shared: another sluice replication slot exists on the source, so the
	// publication may be the union of several streams' scopes.
	Shared bool
}
