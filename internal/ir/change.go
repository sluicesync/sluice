// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "errors"

// Position is an opaque, durable bookmark within a source's change
// stream. Engines populate Token in their own format (binlog file plus
// offset, Postgres LSN, etc.); the IR treats it as opaque. Engine names
// the engine that produced the position so a position store can guard
// against cross-engine confusion.
type Position struct {
	Engine string
	Token  string
}

// ErrPositionInvalid signals that a persisted [Position] no longer
// references resumable state on the source — the PG slot was dropped,
// the MySQL binlog was purged, the VStream GTIDs are too old. The
// pipeline orchestrator detects this via [errors.Is] from a CDC
// reader's start path and falls through to cold-start (re-snapshot +
// fresh slot/position) rather than surfacing an unrecoverable error
// the operator has no flag to bypass.
//
// Engines wrap their specific diagnostic with this sentinel via %w:
//
//	return fmt.Errorf("postgres: replication slot %q no longer exists: %w",
//	    name, ir.ErrPositionInvalid)
//
// See ADR-0022 for the full rationale and the trigger-narrowness
// contract (only applies when the referenced state is gone, not when
// it's merely degraded — `wal_status='lost'` stays strict).
var ErrPositionInvalid = errors.New("ir: persisted position is no longer valid; cold-start is the only recovery path")

// Row is a single tuple of values keyed by column name. Values use
// Go-native types corresponding to the column's IR type; the engine
// reader is responsible for converting from driver-native types into
// IR-native ones.
type Row map[string]any

// Change is the sealed interface for events in a continuous-sync change
// stream. Implementations are [Insert], [Update], [Delete], [Truncate],
// [TxBegin], and [TxCommit]. New variants must be added in this package.
type Change interface {
	// isChange seals the interface.
	isChange()
	// Pos returns the durable position from which this change can be
	// resumed. The value is opaque to consumers of the IR.
	Pos() Position
	// QualifiedName returns "schema.table" for the affected table, or
	// just "table" when Schema is empty. Transaction-boundary events
	// ([TxBegin], [TxCommit]) carry no table reference and return "".
	QualifiedName() string
}

// qualified is a small helper that mirrors Table's identification logic.
func qualified(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// Insert is a row-insertion change event.
type Insert struct {
	Position Position
	Schema   string
	Table    string
	Row      Row
}

func (Insert) isChange()               {}
func (e Insert) Pos() Position         { return e.Position }
func (e Insert) QualifiedName() string { return qualified(e.Schema, e.Table) }

// Update is a row-modification change event. Before captures the prior
// state (when available from the source); After captures the new state.
// Engines that cannot supply a Before image should leave it nil; the
// applier's behaviour in that case is engine-pair specific.
type Update struct {
	Position Position
	Schema   string
	Table    string
	Before   Row // optional; nil when the source does not supply it
	After    Row
}

func (Update) isChange()               {}
func (e Update) Pos() Position         { return e.Position }
func (e Update) QualifiedName() string { return qualified(e.Schema, e.Table) }

// Delete is a row-removal change event. Before holds the row that was
// removed, when the source supplies it.
type Delete struct {
	Position Position
	Schema   string
	Table    string
	Before   Row // optional; nil when the source does not supply it
}

func (Delete) isChange()               {}
func (e Delete) Pos() Position         { return e.Position }
func (e Delete) QualifiedName() string { return qualified(e.Schema, e.Table) }

// Truncate is a whole-table truncation event. Some sources emit this as
// a DDL-flavoured event; the IR treats it as a data-stream change so
// appliers can react without parsing DDL.
type Truncate struct {
	Position Position
	Schema   string
	Table    string
}

func (Truncate) isChange()               {}
func (e Truncate) Pos() Position         { return e.Position }
func (e Truncate) QualifiedName() string { return qualified(e.Schema, e.Table) }

// TxBegin marks the start of a source-side transaction. Engines that
// observe transaction boundaries in their replication protocol
// (Postgres' BeginMessage, MySQL's BEGIN QueryEvent) emit one of
// these immediately before the row events that belong to the
// transaction; [TxCommit] marks the end.
//
// The IR carries these so a [BatchedChangeApplier] can preserve
// transactional cohesion: a 5000-row source transaction commits as
// one 5000-row target transaction instead of being split across
// multiple batches by the row-count cap. Engines that don't surface
// boundaries (legacy CDCReaders, future engines that lack the
// concept) simply omit the events; the applier's row-count and idle
// flush paths still work as before, so backwards compatibility is
// automatic.
//
// Position carries the boundary's source position. For Postgres,
// pgoutput's `BeginMessage.FinalLSN` (the commit LSN of the
// transaction this Begin opens — known up front because pgoutput
// emits Begin only after the source transaction has committed). For
// MySQL, the position of the BEGIN QueryEvent in the binlog stream.
//
// Per-change [ChangeApplier.Apply] implementations treat TxBegin /
// TxCommit as no-ops: each row event already commits its own target
// transaction, so the boundary signal carries no extra information.
// See ADR-0027.
type TxBegin struct {
	Position Position
}

func (TxBegin) isChange()               {}
func (e TxBegin) Pos() Position         { return e.Position }
func (e TxBegin) QualifiedName() string { return "" }

// TxCommit marks the end of a source-side transaction. See [TxBegin]
// for the design rationale. A [BatchedChangeApplier] that observes
// TxCommit flushes the in-flight target transaction at this boundary
// (subject to the empty-source-tx skip — a TxBegin → TxCommit pair
// with no row events between them does not produce an empty target
// commit).
//
// Position carries the source-side commit position. For Postgres,
// pgoutput's `CommitMessage.CommitLSN`. For MySQL, the position of
// the XIDEvent (InnoDB transaction commit marker).
type TxCommit struct {
	Position Position
}

func (TxCommit) isChange()               {}
func (e TxCommit) Pos() Position         { return e.Position }
func (e TxCommit) QualifiedName() string { return "" }
