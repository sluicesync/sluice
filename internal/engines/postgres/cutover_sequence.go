// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// ReadSequenceState implements [ir.SequenceStateReader] for Postgres
// — severity-A finding F10 of the 2026-05-22 Reddit-research run, see
// ADR-0062.
//
// For each identity / AUTO_INCREMENT-tagged column in schema, the
// reader resolves the owning sequence via `pg_get_serial_sequence`
// and reads its current `last_value` / `is_called` from
// `pg_sequences`. Tables / columns without an owning sequence
// (composite PK, UUID PK, manually-managed identifiers) are omitted
// from the returned slice — the target-side primer treats omission
// as the "skip" signal.
//
// The reading is a *snapshot*: each sequence is read on a fresh
// statement against the SchemaReader's pool. F10 v1 does NOT lock
// the source sequences during the read — operators driving cutover
// against a still-receiving-writes source carry the safety margin
// (`--cutover-sequence-margin=N`) to bridge any drift between this
// read and the target apply.
//
// **Canonicalisation.** Postgres' `pg_sequences.last_value` is the
// most-recently-issued value when `is_called=true`; on a freshly
// created sequence (no `nextval()` ever called), `last_value` is the
// start value (typically 1) but `is_called=false` and the next
// `nextval` returns that start value rather than start+1. The reader
// canonicalises to "last issued":
//
//   - `is_called=true`  → SequenceState.Value = last_value
//   - `is_called=false` → SequenceState.Value = 0 (never issued)
//
// This matches MySQL's reading shape (where AUTO_INCREMENT reports
// the *next* value but the orchestrator subtracts 1 for the same
// last-issued canonicalisation).
func (r *SchemaReader) ReadSequenceState(ctx context.Context, schema *ir.Schema) ([]ir.SequenceState, error) {
	if r.db == nil {
		return nil, errors.New("postgres: ReadSequenceState: reader not opened")
	}
	if schema == nil {
		return nil, errors.New("postgres: ReadSequenceState: schema is nil")
	}
	effSchema := r.schema
	if effSchema == "" {
		effSchema = "public"
	}

	out := make([]ir.SequenceState, 0)
	for _, table := range schema.Tables {
		if table == nil {
			continue
		}
		for _, col := range table.Columns {
			intT, isInt := col.Type.(ir.Integer)
			if !isInt || !intT.AutoIncrement {
				continue
			}
			state, ok, err := r.readOneSequenceState(ctx, effSchema, table.Name, col.Name)
			if err != nil {
				return nil, fmt.Errorf("postgres: read sequence state for %q.%q.%q: %w",
					effSchema, table.Name, col.Name, err)
			}
			if !ok {
				// No owning sequence — column is identity-tagged but
				// pg_get_serial_sequence returned NULL. Skip silently;
				// the target side will treat omission as "no work".
				continue
			}
			out = append(out, state)
		}
	}

	// Standalone sequences (item 51, delta review finding #3): under
	// continuous sync the source's standalone sequences keep advancing
	// after the migrate-time position carry, so cutover re-reads their
	// LIVE position here (raw last_value + is_called — the identity
	// 0-sentinel canonicalisation is ambiguous for descending
	// sequences) and the target primer applies the same margin
	// discipline the identity walk gets. Keyed by sequence name;
	// Table/Column stay empty.
	for _, seq := range schema.Sequences {
		if seq == nil || seq.Name == "" {
			continue
		}
		lastValue, isCalled, err := readSequencePositionOn(ctx, r.db, effSchema, seq.Name)
		if err != nil {
			return nil, fmt.Errorf("postgres: read standalone sequence state for %q.%q: %w",
				effSchema, seq.Name, err)
		}
		out = append(out, ir.SequenceState{
			Sequence: seq.Name,
			Value:    lastValue,
			IsCalled: isCalled,
		})
	}
	return out, nil
}

// readOneSequenceState resolves the owning sequence and reads its
// last-issued value for a single (schema, table, column). Returns
// (state, true, nil) when the column has an owning sequence and a
// successful read; (zero, false, nil) when the column has no owning
// sequence; and (zero, false, err) on a catalog query failure.
func (r *SchemaReader) readOneSequenceState(ctx context.Context, schema, table, column string) (ir.SequenceState, bool, error) {
	// Resolve the owning sequence via pg_get_serial_sequence. The
	// function returns NULL when the column is not driven by a
	// sequence (composite PK, UUID PK, manually-managed identifier).
	// We quote both schema and table so case-preserved names round-
	// trip cleanly through the catalog (Bug 87 / task #42 regression
	// pin — same shape SyncIdentitySequences uses).
	const seqQuery = `SELECT pg_get_serial_sequence($1, $2)`
	tableArg := quoteIdent(schema) + "." + quoteIdent(table)
	var seqName sql.NullString
	if err := r.db.QueryRowContext(ctx, seqQuery, tableArg, column).Scan(&seqName); err != nil {
		return ir.SequenceState{}, false, fmt.Errorf("pg_get_serial_sequence: %w", err)
	}
	if !seqName.Valid || seqName.String == "" {
		return ir.SequenceState{}, false, nil
	}

	// pg_get_serial_sequence returns a `schema.name` text that
	// pg_sequences exposes split. Parse the qualified form to feed
	// pg_sequences' two-column WHERE.
	seqSchema, seqLocal, err := splitQualifiedSequence(seqName.String)
	if err != nil {
		return ir.SequenceState{}, false, fmt.Errorf("split sequence name %q: %w", seqName.String, err)
	}

	// pg_sequences exposes last_value as NULL when the sequence has
	// never been called (no nextval()) and as the most-recently-issued
	// value once it has. There is no `is_called` column on the view —
	// the NULL-vs-not distinction is the canonical signal. (Compare:
	// the underlying sequence object has an is_called row when queried
	// directly via `SELECT * FROM <seq>`, but pg_sequences does not
	// surface it.)
	const lastQuery = `
		SELECT last_value
		FROM   pg_sequences
		WHERE  schemaname   = $1
		  AND  sequencename = $2`
	var lastValue sql.NullInt64
	switch err := r.db.QueryRowContext(ctx, lastQuery, seqSchema, seqLocal).Scan(&lastValue); {
	case errors.Is(err, sql.ErrNoRows):
		// pg_get_serial_sequence returned a name pg_sequences
		// doesn't know about — surfaces on PG <10 (no pg_sequences
		// view), corrupt catalog, or a permissions class where the
		// connecting role can see pg_class but not pg_sequences. Be
		// loud: F10's contract is to advance the target ahead of
		// source; an undiscoverable source is a refusal class, not
		// a silent skip.
		return ir.SequenceState{}, false, fmt.Errorf("sequence %q.%q not visible in pg_sequences — verify connecting role has SELECT on pg_sequences (PG 10+)", seqSchema, seqLocal)
	case err != nil:
		return ir.SequenceState{}, false, fmt.Errorf("read pg_sequences row: %w", err)
	}

	// Canonicalise to "last issued":
	//   last_value IS NOT NULL → sequence has produced at least one value.
	//   last_value IS NULL     → sequence has never produced a value; treat
	//                            as 0 so the target side's no-op branch is
	//                            reached (or the margin bumps the target
	//                            starting at the engine default).
	var value int64
	if lastValue.Valid {
		value = lastValue.Int64
	}
	return ir.SequenceState{
		Table:  table,
		Column: column,
		Value:  value,
	}, true, nil
}

// splitQualifiedSequence parses a `schema.name` qualified sequence
// reference (as returned by `pg_get_serial_sequence`) into its two
// components. Both components may be double-quoted; this peels
// matching outer quotes off each side. The pg_sequences view stores
// schemaname / sequencename UNQUOTED (the parsed identifier), so the
// peeled form is what we feed the WHERE clause.
//
// Returns an error when the input has no dot separator — that would
// mean pg_get_serial_sequence returned a bare name without a
// namespace prefix, which the catalog doesn't do for any standard
// IDENTITY column.
func splitQualifiedSequence(qualified string) (schema, name string, err error) {
	// Walk the runes and respect quoted segments so a schema name
	// containing a literal dot inside double quotes doesn't trip the
	// naive strings.SplitN("…", ".", 2).
	inQuotes := false
	splitAt := -1
	for i, r := range qualified {
		switch r {
		case '"':
			inQuotes = !inQuotes
		case '.':
			if !inQuotes {
				splitAt = i
			}
		}
		if splitAt >= 0 {
			break
		}
	}
	if splitAt < 0 {
		return "", "", fmt.Errorf("sequence name %q is not qualified (no schema separator)", qualified)
	}
	schema = unquoteIdent(qualified[:splitAt])
	name = unquoteIdent(qualified[splitAt+1:])
	return schema, name, nil
}

// unquoteIdent strips one pair of matching outer double quotes from
// an identifier; doubled interior `""` becomes `"`. Mirrors PG's
// identifier parser for the subset the catalog actually produces.
// Non-quoted input is returned verbatim — PG folds it to lowercase
// during parsing, so the catalog rows already carry the folded form.
func unquoteIdent(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		inner := s[1 : len(s)-1]
		// Doubled interior quotes — PG's escape for a literal quote.
		// We don't expect this in practice for sluice-managed
		// sequences, but be correct.
		out := make([]byte, 0, len(inner))
		for i := 0; i < len(inner); i++ {
			if inner[i] == '"' && i+1 < len(inner) && inner[i+1] == '"' {
				out = append(out, '"')
				i++
				continue
			}
			out = append(out, inner[i])
		}
		return string(out)
	}
	return s
}

// PrimeSequences implements [ir.SequencePrimer] for Postgres. Applies
// the source-observed sequence states to the target with the
// configured safety margin — severity-A finding F10 of the 2026-05-22
// Reddit-research run, see ADR-0062.
//
// Lifecycle, per (table, column) entry in sourceStates:
//
//  1. Resolve the target's owning sequence via pg_get_serial_sequence.
//  2. Read the target's current last-issued value.
//  3. Compute applyValue = source + margin.
//  4. Decision tree:
//     - target ≥ applyValue + margin → "refused" (operator post-cutover INSERT class)
//     - target ≥ applyValue          → "noop" (idempotent re-run, no work)
//     - otherwise                    → "primed" via setval(seq, applyValue, true)
//
// Tables in schema's identity columns that have no entry in
// sourceStates (the source-side reader returned no row for them) are
// emitted as "skipped" with a clear reason.
//
// **Idempotency.** Postgres' setval moves the sequence to the supplied
// value regardless of direction — re-running cutover MUST NOT regress
// the target. The decision tree's "noop" branch guarantees this: if
// the target is already at or beyond applyValue (which on the second
// run is guaranteed because the first run set it to source+margin),
// no setval runs.
//
// **Refusal tolerance.** The target is "ahead of source" by more than
// `margin` when target > applyValue + margin. The tolerance equals
// the operator-supplied margin: an idempotent re-run (which left the
// target at source+margin) does not trip the refusal. Operator INSERTs
// post-cutover that advance the target by more than `margin` rows
// since the last priming pass do.
func (w *SchemaWriter) PrimeSequences(ctx context.Context, schema *ir.Schema, sourceStates []ir.SequenceState, margin int64) (*ir.SequencePrimeReport, error) {
	if schema == nil {
		return nil, errors.New("postgres: PrimeSequences: schema is nil")
	}
	if margin <= 0 {
		margin = ir.CutoverSequenceMarginDefault
	}

	// Index source states by Table/Column for O(1) lookup as we walk
	// the IR's identity columns.
	sourceByKey := make(map[string]ir.SequenceState, len(sourceStates))
	for _, s := range sourceStates {
		sourceByKey[s.Table+"\x00"+s.Column] = s
	}

	report := &ir.SequencePrimeReport{}
	for _, table := range orderedTables(schema) {
		if table == nil {
			continue
		}
		for _, col := range table.Columns {
			intT, isInt := col.Type.(ir.Integer)
			if !isInt || !intT.AutoIncrement {
				continue
			}
			src, hasSource := sourceByKey[table.Name+"\x00"+col.Name]
			action, err := w.primeOneSequence(ctx, table, col.Name, src, hasSource, margin)
			if err != nil {
				return report, fmt.Errorf("postgres: prime sequence for %q.%q.%q: %w",
					w.schema, table.Name, col.Name, err)
			}
			report.Actions = append(report.Actions, action)
		}
	}

	// Standalone sequences (item 51, delta review finding #3): the
	// identity walk above never sees them (their columns carry a
	// verbatim nextval default, not AutoIncrement — and app-driven
	// sequences have no column at all), so they get their own
	// margin-disciplined, direction-aware prime keyed by sequence
	// name. This also covers the re-optioned serial the item-51
	// reclassification moved OFF the AutoIncrement path.
	stateBySeq := make(map[string]ir.SequenceState, len(sourceStates))
	for _, s := range sourceStates {
		if s.Sequence != "" {
			stateBySeq[s.Sequence] = s
		}
	}
	for _, seq := range schema.Sequences {
		if seq == nil || seq.Name == "" {
			continue
		}
		src, hasSource := stateBySeq[seq.Name]
		action, err := w.primeOneStandaloneSequence(ctx, seq, src, hasSource, margin)
		if err != nil {
			return report, fmt.Errorf("postgres: prime standalone sequence %q.%q: %w",
				w.schema, seq.Name, err)
		}
		report.Actions = append(report.Actions, action)
	}

	if report.HasRefusals() {
		return report, ir.ErrCutoverSequenceTargetAhead
	}
	return report, nil
}

// primeOneStandaloneSequence is the [Schema.Sequences] counterpart of
// primeOneSequence — the same source+margin decision tree, made
// direction-aware (descending sequences advance downward) and applied
// to the sequence's own position rather than a column's owning
// sequence. Positions compare on the "last issued" scale: a
// never-called sequence sits one increment BEFORE its start value, so
// a fresh prime lands at start-inc+margin and the very next nextval
// clears the margin headroom — the same advance-ahead-of-source
// contract the identity walk applies (margin is in VALUE units, so an
// INCREMENT-BY-N sequence's margin spans margin/N calls).
//
// CYCLE sequences are skipped with a named reason: "ahead/behind" is
// ill-defined once a sequence wraps, and a margin push near the bound
// would either wrap (silently reordering) or error. The skip is loud
// in the report; operators verify cycling sequences manually.
func (w *SchemaWriter) primeOneStandaloneSequence(
	ctx context.Context,
	seq *ir.Sequence,
	src ir.SequenceState,
	hasSource bool,
	margin int64,
) (ir.SequencePrimeAction, error) {
	action := ir.SequencePrimeAction{
		Sequence:     seq.Name,
		TargetBefore: -1,
	}
	if !hasSource {
		action.Outcome = "skipped"
		action.Reason = "source reported no state for this standalone sequence — dropped on the source since the schema read?"
		return action, nil
	}
	if seq.Cycle {
		action.Outcome = "skipped"
		action.Reason = "CYCLE sequence — ahead/behind is ill-defined once a sequence wraps; verify its position manually after cutover"
		return action, nil
	}
	dir := int64(1)
	if seq.Increment < 0 {
		dir = -1
	}
	// Canonicalise the source position to "last issued": a
	// never-called sequence has issued nothing, i.e. it sits one
	// increment before its start value.
	srcLast := src.Value
	if !src.IsCalled {
		srcLast = src.Value - seq.Increment
	}
	action.SourceValue = srcLast

	// Read the target's live position directly (is_called included —
	// pg_sequences hides it). A missing target sequence is a refusal
	// class, not a skip: the migrate leg creates it, so absence means
	// the target predates the item-51 carry or was hand-pruned.
	targetLV, targetCalled, err := readSequencePositionOn(ctx, w.db, w.schema, seq.Name)
	if err != nil {
		return action, fmt.Errorf("read target position (does the target carry the sequence? it is created by migrate/restore since v0.99.175): %w", err)
	}
	targetLast := targetLV
	if !targetCalled {
		targetLast = targetLV - seq.Increment
	}
	action.TargetBefore = targetLast

	applyValue := srcLast + dir*margin
	// Clamp into the sequence's declared bounds — setval outside
	// [MinValue, MaxValue] errors. At the clamped bound the target
	// behaves exactly as the source would at exhaustion (nextval
	// errors loudly), which is the faithful shape.
	if dir > 0 && applyValue > seq.MaxValue {
		applyValue = seq.MaxValue
	}
	if dir < 0 && applyValue < seq.MinValue {
		applyValue = seq.MinValue
	}

	ahead := func(a, b int64) bool {
		if dir < 0 {
			return a < b
		}
		return a > b
	}

	// Refusal: target is beyond source+margin by more than the
	// idempotency tolerance — post-cutover writes already advanced it.
	if ahead(targetLast, applyValue+dir*margin) {
		action.Outcome = "refused"
		action.Reason = fmt.Sprintf("target position %d is ahead of source+margin (%d%+d=%d) by more than the idempotency tolerance; manual re-snapshot recommended",
			targetLast, srcLast, dir*margin, applyValue)
		action.TargetAfter = targetLast
		return action, nil
	}
	// No-op: target already at or beyond the apply point (idempotent
	// re-run lands here).
	if !ahead(applyValue, targetLast) {
		action.Outcome = "noop"
		action.TargetAfter = targetLast
		return action, nil
	}
	if err := w.setvalSequence(ctx, w.db, seq, applyValue, true); err != nil {
		return action, err
	}
	action.Outcome = "primed"
	action.TargetAfter = applyValue
	return action, nil
}

// primeOneSequence executes the per-column decision tree.
func (w *SchemaWriter) primeOneSequence(
	ctx context.Context,
	table *ir.Table,
	column string,
	src ir.SequenceState,
	hasSource bool,
	margin int64,
) (ir.SequencePrimeAction, error) {
	action := ir.SequencePrimeAction{
		Table:        table.Name,
		Column:       column,
		TargetBefore: -1,
	}
	if !hasSource {
		action.Outcome = "skipped"
		action.Reason = "source has no owning sequence for this column — composite PK / UUID PK / manually-managed identifier"
		return action, nil
	}
	action.SourceValue = src.Value

	// Resolve the target's owning sequence.
	const seqQuery = `SELECT pg_get_serial_sequence($1, $2)`
	tableArg := quoteIdent(w.schema) + "." + quoteIdent(table.Name)
	var seqName sql.NullString
	if err := w.db.QueryRowContext(ctx, seqQuery, tableArg, column).Scan(&seqName); err != nil {
		return action, fmt.Errorf("pg_get_serial_sequence: %w", err)
	}
	if !seqName.Valid || seqName.String == "" {
		action.Outcome = "skipped"
		action.Reason = "target has no owning sequence — IR declares identity but pg_get_serial_sequence returned NULL"
		return action, nil
	}

	// Read the target's current last-issued value via pg_sequences.
	seqSchema, seqLocal, err := splitQualifiedSequence(seqName.String)
	if err != nil {
		return action, fmt.Errorf("split target sequence name %q: %w", seqName.String, err)
	}
	// pg_sequences exposes last_value as NULL when the sequence has
	// never been called; non-NULL once nextval() has issued at least
	// one value. There is no is_called column on the view.
	const lastQuery = `
		SELECT last_value
		FROM   pg_sequences
		WHERE  schemaname   = $1
		  AND  sequencename = $2`
	var lastValue sql.NullInt64
	switch err := w.db.QueryRowContext(ctx, lastQuery, seqSchema, seqLocal).Scan(&lastValue); {
	case errors.Is(err, sql.ErrNoRows):
		return action, fmt.Errorf("target sequence %q.%q not visible in pg_sequences", seqSchema, seqLocal)
	case err != nil:
		return action, fmt.Errorf("read target pg_sequences row: %w", err)
	}
	var targetBefore int64
	if lastValue.Valid {
		targetBefore = lastValue.Int64
	}
	action.TargetBefore = targetBefore

	applyValue := src.Value + margin
	if applyValue < 1 {
		// Defensive: PG setval refuses values below the sequence's
		// minvalue (default 1). When source is 0 (never called) and
		// margin is somehow zero, snap to 1 so we still leave the
		// sequence in a usable shape.
		applyValue = 1
	}

	// Refusal: target is ahead of source+margin by more than margin.
	// (target > applyValue + margin) means the operator likely ran
	// post-cutover INSERTs that advanced the target past where the
	// would-be priming pass would land it.
	if targetBefore > applyValue+margin {
		action.Outcome = "refused"
		action.Reason = fmt.Sprintf("target value %d is ahead of source+margin (%d+%d=%d) by more than the idempotency tolerance; manual re-snapshot recommended",
			targetBefore, src.Value, margin, applyValue)
		action.TargetAfter = targetBefore
		return action, nil
	}

	// No-op: target is already at or above the would-be apply point.
	// Idempotent re-run lands here.
	if targetBefore >= applyValue {
		action.Outcome = "noop"
		action.TargetAfter = targetBefore
		return action, nil
	}

	// Prime: setval to applyValue with is_called=true so next nextval
	// returns applyValue+1.
	const setvalQuery = `SELECT setval($1, $2, true)`
	if _, err := w.db.ExecContext(ctx, setvalQuery, seqName.String, applyValue); err != nil {
		return action, fmt.Errorf("setval(%q, %d, true): %w", seqName.String, applyValue, err)
	}
	action.Outcome = "primed"
	action.TargetAfter = applyValue
	return action, nil
}
