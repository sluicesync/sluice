// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"context"
	"errors"
)

// CutoverSequenceMarginDefault is the default safety margin added to
// every primed sequence/AUTO_INCREMENT during `sluice cutover`. The
// margin is added on top of the source's observed sequence value to
// give operator headroom against any in-flight source-side INSERT
// activity that may have advanced the source between the read and the
// apply (or between the read and the operator flipping traffic to the
// target). 1000 is generous enough for any reasonable transactional
// workload's per-second INSERT rate; operators driving very high
// per-second insert volumes can override via
// `--cutover-sequence-margin=N`.
const CutoverSequenceMarginDefault int64 = 1000

// SequenceState is one source-side sequence reading captured by a
// [SequenceStateReader] at cutover time. The pipeline ferries a slice
// of these from the source engine to the target engine's
// [SequencePrimer]; engines never share a connection.
//
// Wire shape:
//
//   - Table is the unqualified table name (matches [Table.Name]).
//   - Column is the unqualified column name carrying the identity /
//     AUTO_INCREMENT on the source.
//   - Value is the source's *last issued* value — i.e. on Postgres, the
//     sequence's `last_value` (when `is_called=true`); on MySQL, the
//     `INFORMATION_SCHEMA.TABLES.AUTO_INCREMENT` reading minus 1 (MySQL
//     reports the *next* value, sluice canonicalises to *last issued*
//     so engines align). Zero means "sequence has never produced a
//     value" — the target's bump can be a no-op or a setval to the
//     starting position.
//
// The orchestrator builds the slice once per cutover invocation; the
// target engine's [SequencePrimer.PrimeSequences] consumes it.
type SequenceState struct {
	Table  string
	Column string
	Value  int64
}

// SequencePrimeAction is the per-table outcome of a cutover sequence
// priming pass, surfaced in [SequencePrimeReport]. Operators consume
// this on stdout / JSON to verify each table's sequence landed in the
// expected state.
type SequencePrimeAction struct {
	// Table is the target table the action describes.
	Table string

	// Column is the identity / AUTO_INCREMENT column on the target.
	Column string

	// SourceValue is the source's last-issued sequence value (after
	// the engine's canonicalisation; see [SequenceState.Value]).
	SourceValue int64

	// TargetBefore is the target's sequence value the primer observed
	// before the bump. -1 means "target value was not directly
	// readable" (e.g. the source side reported no sequence for the
	// table; the primer skipped without observing the target).
	TargetBefore int64

	// TargetAfter is the value the primer set the target's sequence
	// to. For Postgres this is the `setval` argument (with `is_called
	// = true`, so the next nextval returns this+1). For MySQL it is
	// the `AUTO_INCREMENT = N` argument (which on MySQL is the
	// *next-to-issue* value; the engine adjusts internally so the
	// reported TargetAfter is consistently the last-issued shape).
	TargetAfter int64

	// Outcome is a short operator-facing tag describing what
	// happened. One of:
	//
	//   - "primed"      — sequence bumped forward by source+margin.
	//   - "skipped"     — table has no sequence on either side (composite
	//                      PK, UUID PK, or operator-managed identifier).
	//   - "noop"        — source value is zero or target was already at
	//                      or above the would-be apply point; no SQL ran.
	//   - "refused"     — target is ahead of source by more than the
	//                      idempotency tolerance; refused loudly (see
	//                      Reason for the operator hint).
	Outcome string

	// Reason carries an operator-actionable note for the "refused" /
	// "skipped" outcomes. Empty for "primed" / "noop".
	Reason string
}

// SequencePrimeReport is the full result of a cutover sequence priming
// pass. The CLI renders this to stdout (text or JSON) so operators have
// a single-glance summary of every table's sequence landing.
//
// Refused is non-empty when at least one table's target sequence is
// already ahead of source by more than the engine's idempotency
// tolerance — i.e. the operator already ran post-cutover INSERTs
// against the target and a forward bump would now risk a collision.
// The cutover command exits non-zero when Refused is non-empty so a
// scripted runbook can branch on the failure.
type SequencePrimeReport struct {
	Actions []SequencePrimeAction
}

// HasRefusals reports whether the report contains any "refused"
// outcomes. Operators / the CLI gate exit-code 0 on this returning
// false.
func (r *SequencePrimeReport) HasRefusals() bool {
	for i := range r.Actions {
		if r.Actions[i].Outcome == "refused" {
			return true
		}
	}
	return false
}

// SequenceStateReader is the optional source-side engine surface that
// reads each identity / AUTO_INCREMENT column's current value from a
// live source database. Implemented on the [SchemaReader] of engines
// that have a sequence concept (Postgres, MySQL today; future engines
// opt in by satisfying the interface).
//
// The orchestrator calls [ReadSequenceState] on the source engine's
// SchemaReader at cutover time, then ferries the returned slice to the
// target engine's [SequencePrimer.PrimeSequences].
//
// **Why operators care (F10).** During CDC catch-up, the source
// continues advancing sequences as new rows are inserted. At cutover
// the target's sequence value lags the source's by however many rows
// were inserted during the catch-up window. A new INSERT on the
// target post-cutover gets a sequence value that collides with an
// existing row's id (one inserted on source during catch-up,
// replicated via CDC, but whose sequence wasn't bumped on target).
// F10 closes the gap by re-reading the source's sequence states at
// cutover and bumping the target ahead by at least
// (source_value + margin). See ADR-0062.
//
// **Refuse loudly on missing columns.** If schema declares an
// identity column the source can't resolve (e.g. the column was
// dropped on the source between snapshot and cutover), the
// implementation refuses with a clear error rather than silently
// dropping the entry. The cutover orchestrator surfaces the refusal
// per the loud-failure tenet.
type SequenceStateReader interface {
	// ReadSequenceState walks every identity / AUTO_INCREMENT column
	// in schema (the source-side IR) and returns the source's
	// last-issued value for each. Tables / columns without a sequence
	// concept (composite PK, UUID PK) are omitted from the result
	// rather than represented with a zero — the target-side primer
	// uses the omission as the "skip" signal.
	//
	// Implementations consult their own catalog (Postgres:
	// pg_sequences via pg_get_serial_sequence; MySQL:
	// INFORMATION_SCHEMA.TABLES.AUTO_INCREMENT) on a fresh
	// transaction so the read is a point-in-time observation.
	// Sequences are not locked — F10 v1 accepts that some forward
	// drift may happen between this read and the target apply; the
	// `--cutover-sequence-margin` knob carries the tolerance.
	ReadSequenceState(ctx context.Context, schema *Schema) ([]SequenceState, error)
}

// SequencePrimer is the optional target-side engine surface that
// applies source-observed sequence states to the target with a safety
// margin. Implemented on the [SchemaWriter] of engines that have a
// sequence concept; engines without one (or service variants that
// manage sequences out-of-band) omit the method and the cutover
// command surfaces a clear "engine X does not support cutover
// sequence priming" error.
//
// **Idempotent.** Running cutover twice does NOT regress sequence
// values. Each PrimeSequences invocation re-reads the source state,
// computes (source + margin), and only emits setval / ALTER TABLE
// AUTO_INCREMENT when the would-be apply point is strictly greater
// than the target's current value. Postgres `setval` itself is not
// monotonic — a `setval(seq, lower)` happily moves the sequence
// backwards — so the engine implementation MUST guard the call with
// a target-side read.
//
// **Refuse loudly when target is ahead of source.** If the target's
// current sequence value is already greater than source+margin by
// more than the engine's idempotency tolerance (typically the same
// `margin` value — i.e. operator can run cutover twice without
// triggering the refusal), the implementation surfaces a "refused"
// outcome in the [SequencePrimeAction] rather than emitting any DDL.
// This catches the "operator already INSERTed post-cutover" scenario
// where a forward bump would risk an id collision.
type SequencePrimer interface {
	// PrimeSequences walks the source-observed states and applies
	// them to the target's sequences with the supplied margin.
	// Engines that have no sequence concept return an empty report
	// rather than an error (the orchestrator treats "no work to do"
	// as success).
	//
	// margin <= 0 is normalised to [CutoverSequenceMarginDefault] so
	// the IR contract guarantees a non-zero safety buffer regardless
	// of operator input shape; the CLI clamps to >= 0 before this
	// path is reached, so this is defensive.
	PrimeSequences(ctx context.Context, schema *Schema, sourceStates []SequenceState, margin int64) (*SequencePrimeReport, error)
}

// ErrCutoverSequenceTargetAhead is the sentinel error engines surface
// when at least one target sequence is ahead of source by more than
// the idempotency tolerance. The CLI translates this to a non-zero
// exit code with an operator-actionable message. The full per-table
// detail lives in the [SequencePrimeReport] returned alongside.
var ErrCutoverSequenceTargetAhead = errors.New("cutover: target sequence is ahead of source — operator likely INSERTed post-cutover; manual re-snapshot recommended")
