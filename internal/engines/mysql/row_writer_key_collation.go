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
// Two catalog reads per table per writer, cached: the target's column
// collations and its keys (primary and unique together — read whether or
// not a column is armed, since a functional key part arms by its own
// collation); SELECT DATABASE() when the writer has no schema; two more
// only for a table with a functional key part (its column names and hidden
// columns); and, when a column is armed, the collation-expressibility probe.
// Nothing per row: the guard is the update list's first
// assignment, evaluated by the server only on a conflict, and rendered once
// per table — a non-conflicting insert runs exactly as before.

import (
	"context"
	"database/sql"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// keyCollationReplayAmbiguousMarker names the LOAD DATA core's refusal to
// treat a replayed segment's duplicate-key skips as its own already-landed
// rows on a table whose target key equates keys the source keeps apart
// ([RowWriter.refuseAmbiguousReplay]). The batched core no longer raises it
// for a retry: it re-sends the batch as the guarded upsert instead
// ([RowWriter.replayArmedBatchAsGuardedUpsert]).
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
	// The keys are read whether or not a column is armed: a functional key
	// part arms its key by its own collation (key_collation_functional.go).
	// The catalog queries take "" as the session's database.
	keys, err := uniqueKeyColumns(ctx, q, w.flavor, w.schema, table.Name)
	if err != nil {
		return copyKeyCollationGuard{}, fmt.Errorf("mysql: copy into %q: read the unique keys for the key-collation guard: %w", table.Name, err)
	}
	if len(keys.unresolved) > 0 {
		return copyKeyCollationGuard{}, keyCollationFunctionalUnsupported(qualifiedName(w.schema, table.Name), keys.unresolved)
	}
	plan := planCopyKeyCollationGuard(table, colTypes, keys, compareAs)
	if plan == nil {
		return copyKeyCollationGuard{}, nil
	}
	// Armed: the database's name for the messages.
	schema := w.schema
	if schema == "" {
		if err := q.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil {
			return copyKeyCollationGuard{}, fmt.Errorf("mysql: copy into %q: resolve the database for the key-collation guard: %w", table.Name, err)
		}
	}
	qn := qualifiedName(schema, table.Name)
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

// replayArmedBatchAsGuardedUpsert is the batched core's half of the guard.
// Its retry arm treats a duplicate key on a re-sent batch as the batch's
// own rows, landed by an attempt whose ack was lost (ADR-0108). That holds
// only when the target key compares as the source's does — on an armed
// table the duplicate may be a second source row the target equates with a
// first, and accepting it dropped the whole batch at exit 0 (measured).
//
// On an armed table the batch is therefore re-sent as the idempotent
// writer's GUARDED upsert ([buildBatchUpsert] with the copy guard), which
// tells the two apart exactly: a row matching one already landed with the
// same key under the source's collation is the batch's own and the upsert
// rewrites it with identical values; a match the source's collation keeps
// apart raises KEY-COLLATION-MERGE and rolls the statement back. So a
// genuine lost-ack replay converges — nearly every Postgres → MySQL
// character-keyed table is armed, and refusing every ADR-0108 reparent or
// storage-grow retry there would have been a loud wedge on a working
// configuration (review 2026-10-12, finding 3a) — and only a real merge
// refuses. The upsert also re-sends any row an interrupted attempt did not
// land, so unlike the 1062 tolerance it does not rest on the batch having
// been atomic (the SDL-1 premise [verifyTransactionalTolerance] checks).
//
// armed is false on an unarmed table, where the caller keeps the ADR-0108
// tolerance unchanged. A table with no upsert key cannot take the upsert;
// it never reaches a retry (flushWithReparentRetry refuses a keyless
// table), and if it did it refuses KEY-COLLATION-REPLAY-AMBIGUOUS rather
// than guess. Called only on the retry arm, never on the hot path.
func (w *RowWriter) replayArmedBatchAsGuardedUpsert(ctx context.Context, c *sql.Conn, table *ir.Table, rowCount int, args []any) (armed bool, err error) {
	guard, err := w.copyKeyCollationGuardFor(ctx, c, table)
	if err != nil {
		return true, err
	}
	if guard.plan == nil {
		return false, nil
	}
	keyCols, ok := effectiveUpsertKeyColumns(table)
	if !ok {
		return true, w.refuseAmbiguousReplay(ctx, c, table)
	}
	if _, err := c.ExecContext(ctx, buildBatchUpsert(table, rowCount, keyCols, w.upsert, guard), args...); err != nil {
		return true, copyKeyCollationMergeRefusal(err, guard, table.Name)
	}
	return true, nil
}

// refuseAmbiguousReplay is the LOAD DATA core's half of the guard. Its
// replay counts duplicate-key skips as re-sent rows, landed by an attempt
// whose ack was lost; on an armed table a skip may instead be a second
// source row the target equates with a first, and accepting it drops that
// row at exit 0 (measured). LOAD DATA has no upsert form to re-send the
// segment through, so a replay that SKIPPED rows on an armed table refuses
// instead ([RowWriter.reportLoadDataWarnings] consults it only when the
// replay's own numbers show a skip — rows inserted short of the segment, or
// a duplicate-key warning; a replay whose warnings are all something else
// skipped nothing and is not ambiguous). The cost is stated: a genuine
// lost-ack replay on an armed table refuses too, loudly, where it would
// have converged; a fresh run's first attempt then reports any real merge
// as a duplicate key. nil on an unarmed table. Called only on the retry
// arm, never on the hot path.
func (w *RowWriter) refuseAmbiguousReplay(ctx context.Context, q queryRower, table *ir.Table) error {
	guard, err := w.copyKeyCollationGuardFor(ctx, q, table)
	if err != nil {
		return err
	}
	if guard.plan == nil {
		return nil
	}
	need, _ := keyCollationNeeds(guard.plan)
	return fmt.Errorf("%s: mysql: copy into %s.%s: a batch re-sent after a transient met a duplicate key. sluice "+
		"accepts that as the batch's own rows landed by the lost attempt only where the target's key compares as "+
		"the source's does; here the target's collation equates keys the source keeps apart, so the duplicate may "+
		"be two source rows merging, and accepting it would drop one. Refusing rather than guessing; run the copy "+
		"again (a first attempt reports a genuine collision as a duplicate key), or give the target's key column(s) %s",
		keyCollationReplayAmbiguousMarker, quoteIdent(guard.schema), quoteIdent(table.Name), need)
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
// keys, each as its columns in index order, with each part's index prefix
// length (pkSub / uniqueSubs, parallel to the columns; 0 for the whole
// column). A functional part stands in its key as the first column it reads
// ("" for one reading none), with the part itself in uniqueFns (parallel to
// uniques; nil for a plain column; key_collation_functional.go). unresolved
// lists the functional parts naming a column the table does not have, which
// the callers refuse. A primary key never has a functional part: MySQL
// refuses one.
type targetUniqueKeys struct {
	pk         []string
	pkSub      []int
	uniques    [][]string
	uniqueSubs [][]int
	uniqueFns  [][]*functionalKeyPart
	unresolved []*functionalKeyPart
}

// uniqueKeyColumns reads the target's primary key and other unique keys,
// with each key part's SUB_PART (an index prefix: characters for a character
// column, bytes for a binary one), in one catalog query, and — only when a
// key has a functional part — the table's column names (to resolve the
// expression's spelling of them) and the collation each functional part
// compares under ([functionalKeyCollations]). MariaDB has no functional key
// parts and no EXPRESSION column (flavor_mariadb.go); its query selects NULL.
func uniqueKeyColumns(ctx context.Context, q queryRower, flavor Flavor, schema, table string) (targetUniqueKeys, error) {
	var out targetUniqueKeys
	expr := "expression"
	if flavor == FlavorMariaDB {
		expr = "NULL"
	}
	rows, err := q.QueryContext(ctx, `SELECT index_name, column_name, `+expr+`, sub_part, seq_in_index FROM information_schema.statistics
		WHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE()) AND table_name = ? AND non_unique = 0
		ORDER BY index_name = 'PRIMARY' DESC, index_name, seq_in_index`, schema, table)
	if err != nil {
		return out, err
	}
	defer func() { _ = rows.Close() }()
	last := ""
	parts := map[string]int{} // each key's part count
	var fns []*functionalKeyPart
	for rows.Next() {
		var idx string
		var col, expression sql.NullString
		var sub sql.NullInt64
		var seq int
		if err := rows.Scan(&idx, &col, &expression, &sub, &seq); err != nil {
			return out, err
		}
		n := 0
		if sub.Valid {
			n = int(sub.Int64)
		}
		parts[idx]++
		name := col.String
		var fn *functionalKeyPart
		if !col.Valid {
			fn = &functionalKeyPart{index: idx, expr: expression.String, seq: seq - 1}
			if column, steps, ok := parseFunctionalKeyPart(expression.String); ok {
				fn.columns, fn.readable, fn.steps = []string{column}, true, steps
			} else {
				fn.columns = expressionColumns(expression.String)
			}
			name = ""
			if len(fn.columns) > 0 {
				name = fn.columns[0]
			}
			fns = append(fns, fn)
		}
		if idx == "PRIMARY" {
			out.pk = append(out.pk, name)
			out.pkSub = append(out.pkSub, n)
			continue
		}
		if idx != last || len(out.uniques) == 0 {
			out.uniques = append(out.uniques, nil)
			out.uniqueSubs = append(out.uniqueSubs, nil)
			out.uniqueFns = append(out.uniqueFns, nil)
			last = idx
		}
		i := len(out.uniques) - 1
		out.uniques[i] = append(out.uniques[i], name)
		out.uniqueSubs[i] = append(out.uniqueSubs[i], n)
		out.uniqueFns[i] = append(out.uniqueFns[i], fn)
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	if len(fns) == 0 {
		return out, nil
	}
	for _, f := range fns {
		f.alone = parts[f.index] == 1
	}
	columns, err := tableColumns(ctx, q, schema, table)
	if err != nil {
		return out, err
	}
	out.unresolved = out.resolveFunctionalColumns(columns)
	hidden, known := functionalKeyCollations(ctx, q, flavor, schema, table)
	out.resolveFunctionalCollations(hidden, known)
	return out, nil
}

// tableColumns is every column of the target table as the catalog spells
// it, with its generation expression ("" for a stored value).
func tableColumns(ctx context.Context, q queryRower, schema, table string) ([]tableColumn, error) {
	rows, err := q.QueryContext(ctx, `SELECT column_name, COALESCE(generation_expression, '') FROM information_schema.columns
		WHERE table_schema = COALESCE(NULLIF(?, ''), DATABASE()) AND table_name = ?`, schema, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []tableColumn
	for rows.Next() {
		var c tableColumn
		if err := rows.Scan(&c.name, &c.generation); err != nil {
			return nil, err
		}
		out = append(out, c)
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
// exist on MariaDB 10.11 (review 2026-10-12, finding 2).
//
// What converges depends on what the two values are to the SOURCE:
//
//   - a key the source compares exactly (a MySQL-family collation, or a
//     deterministic Postgres one compared as bytes): the two are different
//     source rows — both in the snapshot, or one the source has since
//     replaced — and the target key must tell them apart. For a byte-armed
//     key it says to resume the copy, not recreate the table: sluice creates
//     that key under the target's default collation, which equates the
//     values, so a re-copy from scratch meets the same pair again;
//   - a NONDETERMINISTIC key (review 2026-10-12 #2, finding 4): the byte
//     comparison is stricter than the source's, so the pair may be one the
//     source equates — a row whose key the source changed between this
//     copy's attempts (a resumed copy of a live source, add-table or a
//     restore over rows already there), where the target holds a stale row;
//     a re-copy from scratch converges there and a resume repeats the
//     refusal. Or it is a pair the source keeps apart ('e'/'é' at level 2),
//     where the target key has to tell them apart as above. The text names
//     both, since only the operator can compare under the source's collation.
func copyKeyCollationMergeRemedy(plan *keyCollationPlan, schema, table string) string {
	collations, recopyAligns := keyCollationNeeds(plan)
	if plan.armsFunctional() {
		return "These are two different rows of the source (both in what the copy read, or one the source has since " +
			"replaced), and the target table cannot hold both. Give the target " + collations + " and resume the " +
			"copy (for `migrate`, `--resume`) rather than recreating the table: sluice recreates that key from the " +
			"source's expression, under the target's default collation, so a re-copy from scratch meets the same " +
			"pair. Or exclude the table"
	}
	alter := fmt.Sprintf("`ALTER TABLE %s.%s MODIFY <column> …` with %s", quoteIdent(schema), quoteIdent(table), collations)
	give := "give the target's key column(s) " + collations + " (" + alter + ")"
	if keyCollationPlanNondet(plan) {
		return "The two key values are equal under the target's collation and differ byte for byte, and the source compares this " +
			"key under a nondeterministic collation, so compare them under it. If the source equates them, the target holds a " +
			"stale row whose key the source changed between this copy's attempts: re-copy the table from scratch " +
			"(a fresh `migrate`, or `--restart-from-scratch` for a sync), which converges — resuming repeats this " +
			"refusal. If the source keeps them apart, they are two source rows the target key cannot hold under its " +
			"default collation: " + give + " and resume the copy rather than recreating the table, since sluice " +
			"recreates that key under the default. Or exclude the table"
	}
	lead := "These are two different rows of the source (both in what the copy read, or one the source has since " +
		"replaced), and the target table cannot hold both. G" + give[1:]
	if recopyAligns {
		return lead + " and run the copy again, or exclude the table"
	}
	return lead + " and resume the copy (for `migrate`, `--resume`) rather than recreating the table: " +
		"sluice creates that key under the target's default collation, which equates the " +
		"values, so a re-copy from scratch meets the same pair. Or exclude the table"
}

// keyCollationPlanNondet reports whether any armed KEY column compares a
// nondeterministic (or possibly nondeterministic) source collation as bytes.
func keyCollationPlanNondet(plan *keyCollationPlan) bool {
	for _, key := range plan.keys {
		for _, c := range key {
			if ac, ok := plan.armed[c]; ok && ac.nondet {
				return true
			}
		}
	}
	return false
}
