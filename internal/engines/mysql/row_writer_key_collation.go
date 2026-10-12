// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// The key-collation guard on the idempotent copy writer
// (KEY-COLLATION-ROWWRITER): the applier's in-statement backstop
// (key_collation_guard.go, whose file comment has the full rule and the
// measured mechanism) put on the copy's `INSERT … ON DUPLICATE KEY UPDATE`.
//
// # The harm
//
// The idempotent writer serves every copy that may meet a row already on
// the target — a resumed or chunk-resumed copy, `schema add-table`, the
// VStream COPY (serial and fanned out), a restore. Its ON DUPLICATE KEY
// UPDATE matches through any unique key under the TARGET's collation, so
// where the target key equates two values the source keeps apart (`'A'` and
// `'a'` on a `_ci` target key from a `_bin` source, or from a Postgres
// source on its deterministic default collation), the second source row
// silently replaced the first, or — on a table whose every column is the
// key — vanished. Measured 2026-10-12 on MySQL 8.4 and MariaDB 11.4, both
// cores, every key shape: two source rows, one target row, exit 0. A plain
// INSERT (the fresh copy's fast path) meets the same pair as a loud 1062.
//
// # The rule
//
// The applier's, armed by the source's collation — here the copy's own
// table, which is the source's shape — against the target's columns as the
// target's catalog has them now. One difference, in
// [planCopyKeyCollationGuard]: a Postgres default-collation column is armed
// in the primary key too.
//
// # Cost
//
// One catalog read per table per writer (the target's column collations),
// cached; only when a column is armed, one more for the keys (primary and
// unique together), SELECT DATABASE() when the writer has no schema, and the
// collation-expressibility probe. Nothing per row: the guard is the update list's first
// assignment, evaluated by the server only on a conflict, and rendered once
// per table — a non-conflicting insert runs exactly as before.

import (
	"context"
	"database/sql"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// keyCollationReplayAmbiguousMarker names the plain cores' refusal to treat
// a retried batch's duplicate key as its own already-landed rows on a table
// whose target key equates keys the source keeps apart
// ([RowWriter.refuseAmbiguousReplay]).
const keyCollationReplayAmbiguousMarker = "KEY-COLLATION-REPLAY-AMBIGUOUS"

// copyKeyCollationGuard is one table's guard, rendered once: the update
// list's guarded first assignment and the column it assigns. The zero value
// (nil plan) is an unarmed table and renders byte-identical SQL to before.
type copyKeyCollationGuard struct {
	plan         *keyCollationPlan
	schema       string
	assign, hang string
}

// copyKeyCollationGuardEntry caches a table's guard against the source shape
// it was computed from.
type copyKeyCollationGuardEntry struct {
	source *ir.Table
	guard  copyKeyCollationGuard
}

// copyKeyCollationGuardFor returns the guard for an idempotent copy of table
// (the source's shape) into the writer's database, reading the catalog
// through q — the caller's pinned connection, which has already ridden the
// connection-acquire retry.
func (w *RowWriter) copyKeyCollationGuardFor(ctx context.Context, q queryRower, table *ir.Table) (copyKeyCollationGuard, error) {
	if v, ok := w.keyCollationGuards.Load(table.Name); ok {
		if e := v.(copyKeyCollationGuardEntry); e.source == table {
			return e.guard, nil
		}
	}
	guard, err := w.resolveCopyKeyCollationGuard(ctx, q, table)
	if err != nil {
		return copyKeyCollationGuard{}, err
	}
	w.keyCollationGuards.Store(table.Name, copyKeyCollationGuardEntry{source: table, guard: guard})
	return guard, nil
}

// copyKeyCollationGuardOwnConn is [RowWriter.copyKeyCollationGuardFor] for a
// caller with no connection pinned yet (the fan-out, which resolves once
// before its workers start): a cached guard costs nothing, and otherwise one
// connection is acquired through the same transient retry the workers use.
func (w *RowWriter) copyKeyCollationGuardOwnConn(ctx context.Context, table *ir.Table) (copyKeyCollationGuard, error) {
	if v, ok := w.keyCollationGuards.Load(table.Name); ok {
		if e := v.(copyKeyCollationGuardEntry); e.source == table {
			return e.guard, nil
		}
	}
	conn, err := w.acquireConnWithRetry(ctx, table)
	if err != nil {
		return copyKeyCollationGuard{}, fmt.Errorf("mysql: idempotent insert into %q: pin connection: %w", table.Name, err)
	}
	defer func() { _ = conn.Close() }()
	return w.copyKeyCollationGuardFor(ctx, conn, table)
}

func (w *RowWriter) resolveCopyKeyCollationGuard(ctx context.Context, q queryRower, table *ir.Table) (copyKeyCollationGuard, error) {
	colTypes, err := targetColumnCollations(ctx, q, w.schema, table.Name)
	if err != nil {
		return copyKeyCollationGuard{}, fmt.Errorf("mysql: copy into %q: read the target's column collations for the key-collation guard: %w", table.Name, err)
	}
	compareAs := w.flavor.keyGuardCollations()
	if len(armedColumns(table, colTypes, compareAs)) == 0 {
		return copyKeyCollationGuard{}, nil
	}
	// Armed: the keys, and the database's name for the messages.
	schema := w.schema
	if schema == "" {
		if err := q.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil {
			return copyKeyCollationGuard{}, fmt.Errorf("mysql: copy into %q: resolve the database for the key-collation guard: %w", table.Name, err)
		}
	}
	qn := qualifiedName(schema, table.Name)
	keys, err := uniqueKeyColumns(ctx, q, schema, table.Name)
	if err != nil {
		return copyKeyCollationGuard{}, fmt.Errorf("mysql: copy into %s: read the unique keys for the key-collation guard: %w", qn, err)
	}
	plan := planCopyKeyCollationGuard(table, colTypes, keys.pk, keys.uniques, compareAs)
	if plan == nil {
		return copyKeyCollationGuard{}, nil
	}
	if err := checkKeyCollationsExpressible(ctx, q, qn, plan); err != nil {
		return copyKeyCollationGuard{}, err
	}
	cols := nonGeneratedColumns(table.Columns)
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	assign, hang, ok := plan.renderFor(quoteIdent(table.Name), names, w.upsert)
	if !ok {
		need, _ := keyCollationNeeds(plan)
		return copyKeyCollationGuard{}, &terminalMySQLError{err: fmt.Errorf("%w: %s: mysql: copy into %s: a key column "+
			"whose collation the source declares differently from the target is not among the copied columns "+
			"(a generated column), so sluice cannot check that an upsert does not merge two source rows. Refusing "+
			"before any row of the table is written; give the target's key column(s) %s, or exclude the table",
			ir.ErrKeyCollationMerge, keyCollationGuardUnsupportedMarker, qn, need)}
	}
	return copyKeyCollationGuard{plan: plan, schema: schema, assign: assign, hang: hang}, nil
}

// refuseAmbiguousReplay is the plain cores' half of the guard. Their retry
// arms treat a duplicate key on a re-sent batch as the batch's own rows,
// landed by an attempt whose ack was lost: the batched core tolerates the
// retry's 1062 (ADR-0108), LOAD DATA counts the replay's duplicate-key
// skips as re-sent rows. Both arguments hold only when the target key
// compares as the source's does — on an armed table the duplicate may be a
// second source row the target equates with a first, and accepting it drops
// that row at exit 0 (measured: the whole batch, on the batched core). So on
// an armed table the arm refuses instead. The cost is stated: a genuine
// lost-ack replay on an armed table refuses too, loudly, where it would have
// converged; a fresh run's first attempt then reports any real merge as a
// duplicate key. nil on an unarmed table. Called only on the retry arm, never
// on the hot path.
func (w *RowWriter) refuseAmbiguousReplay(ctx context.Context, q queryRower, table *ir.Table) error {
	guard, err := w.copyKeyCollationGuardFor(ctx, q, table)
	if err != nil {
		return err
	}
	if guard.plan == nil {
		return nil
	}
	return fmt.Errorf("%s: mysql: copy into %s.%s: a batch re-sent after a transient met a duplicate key. sluice "+
		"accepts that as the batch's own rows landed by the lost attempt only where the target's key compares as "+
		"the source's does; here the target's collation equates keys the source keeps apart, so the duplicate may "+
		"be two source rows merging, and accepting it would drop one. Refusing rather than guessing; run the copy "+
		"again (a first attempt reports a genuine collision as a duplicate key), or give the target key the "+
		"source's collation", keyCollationReplayAmbiguousMarker, quoteIdent(guard.schema), quoteIdent(table.Name))
}

// targetColumnCollations reads the target table's character columns as the
// guard compares them — each one's character set and collation, carried on
// an [ir.Varchar] because [armedFor] reads nothing else of a target type —
// in one narrow catalog query. schema "" is the session's database. A
// column with no collation (not a character column) is left out, which is
// how [armedFor] already reads it.
func targetColumnCollations(ctx context.Context, q queryRower, schema, table string) (map[string]*ir.Column, error) {
	rows, err := q.QueryContext(ctx, `SELECT column_name, character_set_name, collation_name FROM information_schema.columns
		WHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE()) AND table_name = ? AND collation_name IS NOT NULL`, schema, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]*ir.Column{}
	for rows.Next() {
		var name, charset, collation string
		if err := rows.Scan(&name, &charset, &collation); err != nil {
			return nil, err
		}
		out[name] = &ir.Column{Name: name, Type: ir.Varchar{Charset: charset, Collation: collation}}
	}
	return out, rows.Err()
}

// targetUniqueKeys is a target table's primary key and its other unique
// keys, each as its columns in index order.
type targetUniqueKeys struct {
	pk      []string
	uniques [][]string
}

// uniqueKeyColumns reads the target's primary key and other unique keys in
// one catalog query; a functional key part is skipped, as
// [nonPKUniqueKeyColumns] skips it.
func uniqueKeyColumns(ctx context.Context, q queryRower, schema, table string) (targetUniqueKeys, error) {
	var out targetUniqueKeys
	rows, err := q.QueryContext(ctx, `SELECT index_name, column_name FROM information_schema.statistics
		WHERE table_schema = ? AND table_name = ? AND non_unique = 0
		ORDER BY index_name = 'PRIMARY' DESC, index_name, seq_in_index`, schema, table)
	if err != nil {
		return out, err
	}
	defer func() { _ = rows.Close() }()
	last := ""
	for rows.Next() {
		var idx string
		var col sql.NullString
		if err := rows.Scan(&idx, &col); err != nil {
			return out, err
		}
		switch {
		case !col.Valid:
			continue
		case idx == "PRIMARY":
			out.pk = append(out.pk, col.String)
		default:
			if idx != last || len(out.uniques) == 0 {
				out.uniques = append(out.uniques, nil)
				last = idx
			}
			out.uniques[len(out.uniques)-1] = append(out.uniques[len(out.uniques)-1], col.String)
		}
	}
	return out, rows.Err()
}

// copyKeyCollationMergeRefusal attributes a guarded copy statement's failure
// ([isKeyCollationMergeRaise]); anything else is returned unchanged. The
// remedy is the copy's: these are two rows of one source snapshot, so the
// target key has to tell them apart before the copy can land both.
func copyKeyCollationMergeRefusal(err error, guard copyKeyCollationGuard, table string) error {
	if guard.plan == nil || !isKeyCollationMergeRaise(err) {
		return err
	}
	return &ir.KeyCollationMergeError{
		Schema: guard.schema, Table: table, Cause: err,
		Remedy: copyKeyCollationMergeRemedy(guard.plan, guard.schema, table),
	}
}

// copyKeyCollationMergeRemedy names, per armed key column, the collation the
// target key needs ([keyCollationNeeds] — the applier's remedy computes the
// same list). It never says "the source's collation": a Postgres source's
// cannot be declared on MySQL, and a MySQL 8 source's 0900 name does not
// exist on MariaDB 10.11 (review 2026-10-12, finding 2). For a byte-armed
// key it says to resume the copy, not recreate the table: sluice creates
// that key under the target's default collation, which equates the values,
// so `--restart-from-scratch` would meet the same pair again.
func copyKeyCollationMergeRemedy(plan *keyCollationPlan, schema, table string) string {
	collations, recopyAligns := keyCollationNeeds(plan)
	alter := fmt.Sprintf("`ALTER TABLE %s.%s MODIFY <column> …` with %s", quoteIdent(schema), quoteIdent(table), collations)
	lead := "These are two rows of the source the copy is reading, and the target table cannot hold both. " +
		"Give the target's key column(s) " + collations + " (" + alter + ")"
	if recopyAligns {
		return lead + " and run the copy again, or exclude the table"
	}
	return lead + " and resume the copy (for `migrate`, `--resume`) rather than recreating the table: " +
		"sluice creates that key under the target's default collation, which equates the " +
		"values, so a re-copy from scratch meets the same pair. Or exclude the table"
}
