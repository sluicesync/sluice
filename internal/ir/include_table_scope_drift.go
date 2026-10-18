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
// when every table the door refuses has a `sluice schema add-table` still
// running — its pending entry's heartbeat is fresh by the control-table
// server's clock ([PendingAddedTable.Fresh]). A running add overrides the
// convictions of its own table that its completion cures (never copied,
// literally named or delivered; re-admitted after a drop); it does not
// override an established table gone from the publication, nor any other
// table's conviction, which keep [ErrIncludeTableScopeDrift]. The add's
// completion records the table, so a restart after it resumes: unlike
// [ErrIncludeTableScopeDrift] this is NOT in the fleet supervisor's
// do-not-restart roster, which backs off and retries.
var ErrIncludeTableAddInProgress = errors.New("INCLUDE-TABLE-ADD-IN-PROGRESS")

// ErrPendingAddSuperseded classifies an add-table that found its own pending
// entry gone or replaced: `sluice schema add-table --abandon` removed it, or a
// re-run add took it over. The add stops without recording the table as
// established — its copy is not the one the stream will be told about.
var ErrPendingAddSuperseded = errors.New("PENDING-ADD-SUPERSEDED")

// EstablishedRecord is a stream's record of the tables it established, as
// read back. An entry is the bare table name on a single-namespace stream and
// `<namespace>\x00<table>` on a multi-database one.
type EstablishedRecord struct {
	// Recorded is false when the stream has no record: no row, no column,
	// NULL — a stream cold-started before the record existed. A stream that
	// has a record never goes back to this state.
	Recorded bool
	// ColdStartInProgress: a cold start passed its point of no return (its
	// first destructive or copying step) and has not written its record yet.
	// Established is empty then; the warm-resume door refuses until a cold
	// start finishes.
	ColdStartInProgress bool
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

	// BeginColdStartRecord marks a cold start past its point of no return
	// ([EstablishedRecord.ColdStartInProgress]), leaving the dropped set as
	// it is. A missing row is nothing to mark (a fresh stream has no record
	// to protect).
	BeginColdStartRecord(ctx context.Context, streamID string) error

	// RecordEstablishedTables replaces the established set (the empty set
	// included), ending a cold start in progress. The row must exist.
	RecordEstablishedTables(ctx context.Context, streamID string, tables []string) error

	// RecordDroppedTables replaces the dropped set. The row must exist.
	RecordDroppedTables(ctx context.Context, streamID string, tables []string) error
}

// Pending add-table liveness. A running add-table refreshes its entry's
// heartbeat every PendingAddHeartbeatInterval; an entry whose heartbeat is
// older than PendingAddStaleAfter belongs to an add that died. Every stamp and
// every comparison uses the control-table server's clock, so no two hosts'
// clocks are ever compared; ten missed beats leaves room for a slow write. A
// heartbeat more than PendingAddFutureTolerance ahead of the server's now can
// only come from a clock that went backwards, and counts as stale.
const (
	PendingAddHeartbeatInterval = 30 * time.Second
	PendingAddStaleAfter        = 5 * time.Minute
	PendingAddFutureTolerance   = time.Minute
)

// PendingAddedTable is one pending add-table entry. Token identifies the add
// that wrote it; every later write by that add compares-and-sets on it.
type PendingAddedTable struct {
	Table       string
	Token       string
	StartedAt   time.Time
	HeartbeatAt time.Time
}

// Fresh reports whether the add that wrote the entry was alive within
// [PendingAddStaleAfter] of now, the control-table server's time.
func (p PendingAddedTable) Fresh(now time.Time) bool {
	age := now.Sub(p.HeartbeatAt)
	return age < PendingAddStaleAfter && age >= -PendingAddFutureTolerance
}

// FromTheFuture reports a heartbeat further ahead of now than
// [PendingAddFutureTolerance] — stale, and worth a WARN.
func (p PendingAddedTable) FromTheFuture(now time.Time) bool {
	return p.HeartbeatAt.Sub(now) > PendingAddFutureTolerance
}

// PendingAddedTablesStore is the optional [ChangeApplier] surface that
// records, on the stream's row of the per-target `sluice_cdc_state` table,
// the tables whose `sluice schema add-table` has started and not completed.
//
// `sluice schema add-table` records its table under a fresh token before the
// first change it makes (the target CREATE, the publication add), refreshes
// the entry's heartbeat while it copies, and completes it — removing the
// entry and recording the table as established, in one transaction — only
// after the copy and the hand-off to the stream have succeeded. Every write
// after the first compares-and-sets on the token, so an add whose entry was
// abandoned or taken over stops instead of recording a copy nobody is
// tracking ([ErrPendingAddSuperseded]). A failure leaves the entry, and
// `sync start` refuses a warm resume whose scope includes the table instead
// of streaming changes into a table whose copy never landed. A cold start
// that copies the table, and `sluice schema add-table --abandon`, clear it.
//
// Timestamps are the server's (NOW()), and reads return the server's now.
// Implemented by the Postgres and MySQL-family appliers. The columns are
// added on the first Record, not at every start.
type PendingAddedTablesStore interface {
	// RecordPendingAddedTable writes table's entry under token, started and
	// alive at the server's now, replacing any earlier entry for the table.
	// The row must exist.
	RecordPendingAddedTable(ctx context.Context, streamID, table, token string) error

	// TouchPendingAddedTable refreshes the heartbeat of table's entry to the
	// server's now (alive) or to the epoch (the add failed and stopped). It
	// returns [ErrPendingAddSuperseded] when the entry is gone or carries
	// another token.
	TouchPendingAddedTable(ctx context.Context, streamID, table, token string, alive bool) error

	// CompletePendingAddedTable removes table's entry and records the table
	// as established (and no longer dropped), in one transaction, when — and
	// only when — the entry still carries token; otherwise
	// [ErrPendingAddSuperseded] and nothing is written. The established half
	// is skipped on a stream with no record, during a cold start in progress,
	// and on a multi-database stream (add-table names a bare table).
	CompletePendingAddedTable(ctx context.Context, streamID, table, token string) error

	// ClearPendingAddedTables removes tables' entries whatever their tokens.
	// Idempotent and tolerant of a missing row, table or column.
	ClearPendingAddedTables(ctx context.Context, streamID string, tables []string) error

	// ReadPendingAddedTables returns streamID's entries and the server's now:
	// none when there is no row, no control table, or no column. A stored
	// value that does not decode is an error.
	ReadPendingAddedTables(ctx context.Context, streamID string) ([]PendingAddedTable, time.Time, error)
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
	// Shared: another sluice replication slot exists on the publication's
	// database (a slot on another database cannot read it), so the
	// publication may be the union of several streams' scopes.
	Shared bool
}
