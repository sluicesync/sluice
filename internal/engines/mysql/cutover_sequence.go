// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"sluicesync.dev/sluice/internal/ir"
)

// ReadSequenceState implements [ir.SequenceStateReader] for MySQL —
// severity-A finding F10 of the 2026-05-22 Reddit-research run, see
// ADR-0062.
//
// MySQL has no first-class sequence object — AUTO_INCREMENT is a
// per-table counter stored in
// `INFORMATION_SCHEMA.TABLES.AUTO_INCREMENT`. The reader walks every
// identity-tagged column in schema (one per table, as MySQL allows
// only one AUTO_INCREMENT column per table) and returns the source's
// observed counter value.
//
// **Canonicalisation.** MySQL's AUTO_INCREMENT reading reports the
// *next-to-issue* value (the one the next INSERT will pick up). The
// reader subtracts 1 so SequenceState.Value is the *last-issued*
// shape — aligning with Postgres' last_value semantics. When the
// counter has not yet issued any value (`AUTO_INCREMENT=1` on a
// fresh table), the canonicalised value is 0.
//
// **Why not SHOW TABLE STATUS.** Both surfaces report the same
// counter; INFORMATION_SCHEMA is preferred because it's part of
// SQL-standard schema introspection (consistent with how
// sluice's existing schema reader queries metadata) and avoids the
// session-variable-driven `information_schema_stats_expiry` caching
// quirk on MySQL 8.0+ — the reader's call site bypasses the cache by
// using a fresh session connection.
func (r *SchemaReader) ReadSequenceState(ctx context.Context, schema *ir.Schema) ([]ir.SequenceState, error) {
	if r.db == nil {
		return nil, errors.New("mysql: ReadSequenceState: reader not opened")
	}
	if schema == nil {
		return nil, errors.New("mysql: ReadSequenceState: schema is nil")
	}

	// information_schema_stats_expiry caches I_S.TABLES rows for up
	// to a default 86400s window; setting expiry=0 forces fresh
	// catalog reads for the duration of this transaction. Best-effort
	// — a permissions class that refuses SET surfaces back as a
	// non-fatal warning; the reader proceeds with whatever cached
	// reading the catalog has.
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("mysql: ReadSequenceState: open conn: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		// Pre-8.0 MySQL doesn't have this variable; ignore the
		// failure. The catalog read still works, just with whatever
		// caching the server has.
		_ = err
	}

	out := make([]ir.SequenceState, 0)
	for _, table := range schema.Tables {
		if table == nil {
			continue
		}
		// Find the table's AUTO_INCREMENT column. MySQL allows at
		// most one per table.
		autoCol := ""
		for _, col := range table.Columns {
			intT, isInt := col.Type.(ir.Integer)
			if isInt && intT.AutoIncrement {
				autoCol = col.Name
				break
			}
		}
		if autoCol == "" {
			continue
		}

		nextVal, ok, err := readMySQLAutoIncrement(ctx, conn, r.schema, table.Name)
		if err != nil {
			return nil, fmt.Errorf("mysql: read AUTO_INCREMENT for %q.%q: %w",
				r.schema, table.Name, err)
		}
		if !ok {
			// Table missing from I_S.TABLES — surfaces if the table
			// was dropped on the source between snapshot and
			// cutover. Be loud: the orchestrator's caller surfaces
			// the failure rather than silently skipping.
			return nil, fmt.Errorf("mysql: table %q.%q not present in INFORMATION_SCHEMA.TABLES — verify the source schema matches the cutover request",
				r.schema, table.Name)
		}

		// Canonicalise to last-issued: subtract 1 because MySQL
		// reports the *next* AUTO_INCREMENT.
		var lastIssued int64
		if nextVal > 1 {
			lastIssued = nextVal - 1
		}

		out = append(out, ir.SequenceState{
			Table:  table.Name,
			Column: autoCol,
			Value:  lastIssued,
		})
	}
	return out, nil
}

// readMySQLAutoIncrement returns the source's AUTO_INCREMENT counter
// for (schema, table) and a present-bit. NULL in the catalog (which
// MySQL returns for tables without an AUTO_INCREMENT column or freshly
// truncated tables) maps to (1, true) — the next-to-issue starting
// value, which canonicalises to last-issued 0.
func readMySQLAutoIncrement(ctx context.Context, conn *sql.Conn, schema, table string) (nextVal int64, present bool, err error) {
	const q = `
		SELECT AUTO_INCREMENT
		FROM   INFORMATION_SCHEMA.TABLES
		WHERE  TABLE_SCHEMA = ?
		  AND  TABLE_NAME   = ?`
	var v sql.NullInt64
	switch scanErr := conn.QueryRowContext(ctx, q, schema, table).Scan(&v); {
	case errors.Is(scanErr, sql.ErrNoRows):
		return 0, false, nil
	case scanErr != nil:
		return 0, false, scanErr
	}
	if !v.Valid {
		// Table exists but has no AUTO_INCREMENT (or counter is
		// freshly reset). Treat as "next value = 1".
		return 1, true, nil
	}
	return v.Int64, true, nil
}

// PrimeSequences implements [ir.SequencePrimer] for MySQL. Applies
// source-observed AUTO_INCREMENT values to the target with the
// configured safety margin — severity-A finding F10 of the 2026-05-22
// Reddit-research run, see ADR-0062.
//
// For each (table, column) in sourceStates, the primer:
//
//  1. Reads the target's current AUTO_INCREMENT via
//     INFORMATION_SCHEMA.TABLES.
//  2. Computes applyNext = source.Value + margin + 1
//     (MySQL's `ALTER TABLE ... AUTO_INCREMENT = N` sets the
//     *next-to-issue* value; +1 brings the source's last-issued shape
//     into the next-to-issue shape).
//  3. Decides:
//     - target ≥ applyNext + margin → "refused" (post-cutover INSERT class)
//     - target ≥ applyNext          → "noop" (idempotent re-run)
//     - otherwise                   → "primed" via ALTER TABLE
//
// **Idempotency.** MySQL's `ALTER TABLE ... AUTO_INCREMENT = N` is
// monotonic forward only — supplying N ≤ current AUTO_INCREMENT is
// silently clamped to current. So even without the noop guard,
// MySQL won't regress; the explicit guard is for predictable report
// output (operators see "noop" not a wasted ALTER).
//
// **Refusal tolerance.** The same shape as the PG primer: target is
// ahead of source by more than margin → refuse. The operator-supplied
// margin doubles as the idempotency tolerance.
func (w *SchemaWriter) PrimeSequences(ctx context.Context, schema *ir.Schema, sourceStates []ir.SequenceState, margin int64) (*ir.SequencePrimeReport, error) {
	if schema == nil {
		return nil, errors.New("mysql: PrimeSequences: schema is nil")
	}
	// item-51: standalone PG sequences have no MySQL counterpart to
	// prime. The migrate/restore refusal in
	// pipeline.checkCrossEngineSupportable normally stops such a pair
	// long before cutover; this backstop keeps the cutover surface
	// loud too rather than silently ignoring the sequence-keyed
	// source states.
	if len(schema.Sequences) > 0 {
		return nil, fmt.Errorf(
			"mysql: PrimeSequences: schema carries standalone sequence %q (PG-only — "+
				"MySQL has no sequence objects to prime); migrate to a PG target or drop "+
				"the sequence on the source",
			schema.Sequences[0].Name,
		)
	}
	if margin <= 0 {
		margin = ir.CutoverSequenceMarginDefault
	}

	sourceByKey := make(map[string]ir.SequenceState, len(sourceStates))
	for _, s := range sourceStates {
		sourceByKey[s.Table+"\x00"+s.Column] = s
	}

	report := &ir.SequencePrimeReport{}
	for _, table := range orderedTables(schema) {
		if table == nil {
			continue
		}
		autoCol := ""
		for _, col := range table.Columns {
			intT, isInt := col.Type.(ir.Integer)
			if isInt && intT.AutoIncrement {
				autoCol = col.Name
				break
			}
		}
		if autoCol == "" {
			continue
		}
		src, hasSource := sourceByKey[table.Name+"\x00"+autoCol]
		action, err := w.primeOneSequence(ctx, table.Name, autoCol, src, hasSource, margin)
		if err != nil {
			return report, fmt.Errorf("mysql: prime sequence for %q.%q.%q: %w",
				w.schema, table.Name, autoCol, err)
		}
		report.Actions = append(report.Actions, action)
	}

	if report.HasRefusals() {
		return report, ir.ErrCutoverSequenceTargetAhead
	}
	return report, nil
}

// primeOneSequence executes the per-table decision tree.
func (w *SchemaWriter) primeOneSequence(
	ctx context.Context,
	table, column string,
	src ir.SequenceState,
	hasSource bool,
	margin int64,
) (ir.SequencePrimeAction, error) {
	action := ir.SequencePrimeAction{
		Table:        table,
		Column:       column,
		TargetBefore: -1,
	}
	if !hasSource {
		action.Outcome = "skipped"
		action.Reason = "source has no AUTO_INCREMENT for this table — composite PK / surrogate-managed identifier"
		return action, nil
	}
	action.SourceValue = src.Value

	// Read the target's current AUTO_INCREMENT (next-to-issue).
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return action, fmt.Errorf("open conn: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SET SESSION information_schema_stats_expiry = 0"); err != nil {
		// Pre-8.0 MySQL doesn't have this variable; ignore the
		// failure. The catalog read still works, just with whatever
		// caching the server has. (Same rationale as ReadSequenceState.)
		_ = err
	}

	targetNext, ok, err := readMySQLAutoIncrement(ctx, conn, w.schema, table)
	if err != nil {
		return action, fmt.Errorf("read target AUTO_INCREMENT: %w", err)
	}
	if !ok {
		return action, fmt.Errorf("target table %q.%q not present in INFORMATION_SCHEMA.TABLES", w.schema, table)
	}
	// Express the target in last-issued shape for the report.
	var targetBefore int64
	if targetNext > 1 {
		targetBefore = targetNext - 1
	}
	action.TargetBefore = targetBefore

	// applyNext is the value we would pass to ALTER TABLE ... AUTO_INCREMENT = N.
	// MySQL semantics: that's the *next* value the engine will issue,
	// so we add 1 on top of source+margin to keep the post-apply
	// state consistent with "leave a buffer of size margin above
	// source's last-issued."
	applyNext := src.Value + margin + 1
	if applyNext < 1 {
		applyNext = 1
	}

	// Refusal: target.next > applyNext + margin.
	if targetNext > applyNext+margin {
		action.Outcome = "refused"
		action.Reason = fmt.Sprintf("target AUTO_INCREMENT %d is ahead of source+margin (next=%d) by more than the idempotency tolerance; manual re-snapshot recommended",
			targetNext, applyNext)
		action.TargetAfter = targetBefore
		return action, nil
	}

	// No-op: target is already at or above the would-be apply point.
	if targetNext >= applyNext {
		action.Outcome = "noop"
		action.TargetAfter = targetBefore
		return action, nil
	}

	// Prime via ALTER TABLE. MySQL allows AUTO_INCREMENT only on the
	// table-owning database; w.schema is the connection's default
	// database from the SchemaWriter's DSN. The applyNext value is
	// server-validated derived from operator-supplied margin +
	// catalog-read source.Value — no operator string reaches this
	// path so there's no injection surface, and the integer form
	// keeps the statement single-allocation.
	q := "ALTER TABLE " + quoteIdent(table) + " AUTO_INCREMENT = " + strconv.FormatInt(applyNext, 10)
	if _, err := conn.ExecContext(ctx, q); err != nil {
		return action, fmt.Errorf("ALTER TABLE AUTO_INCREMENT: %w", err)
	}
	action.Outcome = "primed"
	// Report in last-issued shape: applyNext - 1.
	action.TargetAfter = applyNext - 1
	return action, nil
}
