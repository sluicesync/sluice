// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// The key-collation guard on the idempotent copy writer
// (KEY-COLLATION-ROWWRITER), the Postgres half: the applier's in-statement
// backstop (key_collation_guard.go, whose file comment has the rule and the
// measured mechanism) put on the copy's `INSERT … ON CONFLICT`. The MySQL
// writer carries the twin (engines/mysql row_writer_key_collation.go).
//
// The idempotent writer serves every copy that may meet a row already on
// the target — a resumed or chunk-resumed copy, `schema add-table`, a
// VStream COPY onto Postgres, a restore. Where the target's conflict key is
// under a NONDETERMINISTIC collation the source's does not share, ON
// CONFLICT matched a second source row ('A' against 'a') and DO UPDATE
// overwrote the first, or — on a table whose every column is the key — DO
// NOTHING dropped it. Measured 2026-10-12 on Postgres 16 for a source key on
// "C", the database default and ucs_basic: two source rows, one target row,
// exit 0. Two such rows inside ONE statement already failed loudly
// (cardinality_violation), as does a merge through a unique key that is not
// the arbiter (unique_violation).
//
// The rule is the applier's, with the copy's own table as the source's
// shape. One catalog read per table per writer (the target's column
// collations), cached; nothing per row — the check is the DO UPDATE's own
// WHERE, evaluated only on a conflict, and an unarmed table renders the
// statement byte-identical to before.

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// copyKeyCollationGuardEntry caches a table's guard against the source shape
// it was resolved for.
type copyKeyCollationGuardEntry struct {
	source *ir.Table
	plan   *pgKeyCollationPlan
}

// copyKeyCollationPlanFor returns the guard for an idempotent copy of table
// (the source's shape) conflicting on keyCols, or nil when nothing is armed.
// A key the guard cannot be written for refuses here, before any row of the
// table is written (KEY-COLLATION-GUARD-UNSUPPORTED).
func (w *RowWriter) copyKeyCollationPlanFor(ctx context.Context, table *ir.Table, keyCols []string) (*pgKeyCollationPlan, error) {
	qn := schemaTableKey(w.schema, table.Name)
	if v, ok := w.keyCollationGuards.Load(qn); ok {
		if e := v.(copyKeyCollationGuardEntry); e.source == table {
			return e.plan, nil
		}
	}
	target, err := targetKeyCollations(ctx, w.db, w.schema, table.Name)
	if err != nil {
		return nil, fmt.Errorf("postgres: copy into %s: read the key collations for the key-collation guard: %w", qn, err)
	}
	plan, unexpressible := planPGKeyCollationGuard(table, keyCols, target)
	if len(unexpressible) > 0 {
		return nil, keyCollationGuardUnexpressible(qn, unexpressible)
	}
	if plan != nil {
		if err := checkPGCollationsExist(ctx, w.db, qn, plan); err != nil {
			return nil, err
		}
	}
	w.keyCollationGuards.Store(qn, copyKeyCollationGuardEntry{source: table, plan: plan})
	return plan, nil
}

// copyKeyCollationMergeRefusal attributes a guarded copy statement's failure
// ([isKeyCollationMergeRaise]); anything else is returned unchanged.
func copyKeyCollationMergeRefusal(err error, plan *pgKeyCollationPlan, schema, table string) error {
	if plan == nil || !isKeyCollationMergeRaise(err) {
		return err
	}
	return &ir.KeyCollationMergeError{
		Schema: schema, Table: table, Cause: err,
		Remedy: fmt.Sprintf("These are two rows of the source the copy is reading, and the target table cannot hold "+
			"both. Give the target's key column(s) a deterministic collation, or the source's (for example `ALTER "+
			"TABLE %s.%s ALTER COLUMN <column> TYPE … COLLATE …`), and run the copy again, or exclude the table",
			quoteIdent(schema), quoteIdent(table)),
	}
}
