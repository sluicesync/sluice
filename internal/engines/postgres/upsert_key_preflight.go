// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// The DEFERRABLE-key refusal (Bug 211).
//
// Every idempotent write sluice makes into Postgres is an
// `INSERT … ON CONFLICT (key) DO UPDATE`, and PostgreSQL refuses a
// non-immediate (DEFERRABLE) unique constraint as an ON CONFLICT arbiter:
//
//	ERROR: ON CONFLICT does not support deferrable unique constraints/
//	       exclusion constraints as arbiters   (SQLSTATE 55000)
//
// v0.103.1 made a Postgres target CARRY a source's `PRIMARY KEY …
// DEFERRABLE`. That was the right fix — dropping the attribute landed a
// STRICTER constraint than the source's, so a bulk key shift that
// commits on the source aborted on the target — and it is exactly what
// made this apply path illegal. The failure shape was as bad as it gets
// for an availability bug: the stream died on the FIRST change to such a
// table, with zero retries, every warm resume died on the same change,
// and every other table in the stream (plain PKs included) stalled
// behind it.
//
// The answer is not to stop carrying the attribute — that would trade an
// availability defect for a silent fidelity one. It is to carry it
// faithfully and refuse loudly where the carried shape is incompatible
// with what sluice needs to do next. [ChangeApplier.PreflightUpsertKeys]
// is that refusal, hoisted to sync start (target schema created, first
// change not yet applied); [loadConflictKey]'s own refusal is the
// defence-in-depth floor for the apply paths that have no preflight
// (broker replay, chain restore, `schema add-table`).

// deferrableUpsertKeyTable names one target table whose every usable
// unique key is DEFERRABLE, together with the constraints/indexes that
// were rejected for it.
type deferrableUpsertKeyTable struct {
	Schema      string
	Table       string
	Constraints []string
}

// deferrableUpsertKeyError is the inner error of the coded refusal, kept
// as a concrete type so the preflight can errors.As per-table refusals
// out of [loadConflictKey] and re-report them as ONE message naming
// every offending table.
type deferrableUpsertKeyError struct {
	tables []deferrableUpsertKeyTable
}

func (e *deferrableUpsertKeyError) Error() string {
	parts := make([]string, len(e.tables))
	for i, t := range e.tables {
		parts[i] = fmt.Sprintf(
			"%s (constraint %s)",
			qualifyForRemedy(t.Schema, t.Table), strings.Join(t.Constraints, ", "),
		)
	}
	return fmt.Sprintf(
		"postgres: target table(s) %s have no key sluice can apply changes idempotently against: every unique "+
			"constraint it could key on is DEFERRABLE, and PostgreSQL refuses a deferrable constraint as an ON CONFLICT "+
			"arbiter (\"ON CONFLICT does not support deferrable unique constraints/exclusion constraints as "+
			"arbiters\", SQLSTATE 55000) — so the INSERT … ON CONFLICT (key) DO UPDATE that makes a replayed "+
			"change idempotent cannot be built. sluice carries a source's DEFERRABLE key to the target "+
			"faithfully (dropping it would land a stricter constraint than the source's); it cannot upsert "+
			"through one. Make the target constraint immediate, or give the table an immediate NOT NULL UNIQUE "+
			"index sluice can key on, or take the table out of scope with --exclude-table, then re-run",
		strings.Join(parts, ", "),
	)
}

// deferrableUpsertKeyHint is the remedy that rides the coded error. It is
// the same advice for one table or twenty, so it carries no names.
const deferrableUpsertKeyHint = "recreate the target constraint as immediate " +
	"(ALTER TABLE … DROP CONSTRAINT …, then re-add it WITHOUT DEFERRABLE), pre-create the target table with " +
	"an immediate primary key, add an immediate NOT NULL UNIQUE index, or exclude the table from the sync"

// errDeferrableUpsertKey is the single-table refusal [loadConflictKey]
// raises on first sight of an unarbitrable table.
func errDeferrableUpsertKey(schema, table string, constraints []string) error {
	return errDeferrableUpsertKeys([]deferrableUpsertKeyTable{{Schema: schema, Table: table, Constraints: constraints}})
}

// errDeferrableUpsertKeys is the aggregated refusal the preflight raises
// after scanning every in-scope target table.
func errDeferrableUpsertKeys(tables []deferrableUpsertKeyTable) error {
	return sluicecode.Wrap(
		sluicecode.CodeTargetDeferrableKey,
		deferrableUpsertKeyHint,
		&deferrableUpsertKeyError{tables: tables},
	)
}

// PreflightUpsertKeys implements [ir.UpsertKeyPreflighter]: it resolves
// the ON CONFLICT arbiter for every in-scope table in the applier's
// target schema and refuses, naming all of them at once, if any table's
// only unique keys are DEFERRABLE.
//
// It runs at sync start AFTER the target schema exists (cold start
// creates the tables; a warm resume inherits them) and BEFORE the first
// change is applied — the latest moment the answer is knowable and the
// earliest it is actionable. inScope is the stream's table filter, so a
// deferrable-PK table the stream never touches is never refused over; a
// nil inScope means every table in the schema.
//
// Scope note: only the applier's OWN schema is walked. A multi-database
// stream routing changes into other target schemas (ADR-0142) is covered
// by [loadConflictKey]'s per-table refusal instead, one change later than
// this would have been.
func (a *ChangeApplier) PreflightUpsertKeys(ctx context.Context, inScope func(table string) bool) error {
	schema := a.schema
	if schema == "" {
		schema = "public"
	}
	tables, err := listSchemaTables(ctx, a.db, schema)
	if err != nil {
		return err
	}

	tx, err := a.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("postgres: applier: upsert-key preflight: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var offenders []deferrableUpsertKeyTable
	for _, table := range tables {
		if inScope != nil && !inScope(table) {
			continue
		}
		if _, err := loadConflictKey(ctx, tx, schema, table); err != nil {
			var blocked *deferrableUpsertKeyError
			if errors.As(err, &blocked) {
				offenders = append(offenders, blocked.tables...)
				continue
			}
			return fmt.Errorf("postgres: applier: upsert-key preflight: %s: %w", table, err)
		}
	}
	if len(offenders) == 0 {
		return nil
	}
	return errDeferrableUpsertKeys(offenders)
}

// blockingDeferrableIndex reports the name of the unique index over
// EXACTLY keyCols when that index exists on the target and is NOT
// immediate — the shape PG refuses as an ON CONFLICT arbiter. It returns
// "" when the index is immediate (usable) AND when no index matches
// keyCols at all: a missing index is a different, pre-existing failure
// (SQLSTATE 42P10, "no unique or exclusion constraint matching") this
// check has no opinion about, and manufacturing a deferrability refusal
// for it would misname the problem.
//
// This is the idempotent COPY writer's counterpart to the applier's
// [loadConflictKey] exclusion. It reads the CATALOG rather than the IR
// because the writer's key comes from the source schema while the
// arbiter has to satisfy the target: a target pre-created with an
// immediate primary key is usable no matter what the source declared.
func blockingDeferrableIndex(ctx context.Context, db *sql.DB, schema, table string, keyCols []string) (string, error) {
	if len(keyCols) == 0 {
		return "", nil
	}
	if schema == "" {
		schema = "public"
	}
	const q = `
		SELECT cl.relname AS index_name, a.attname, ix.indimmediate, u.ord
		FROM   pg_index ix
		JOIN   pg_class      tcl ON tcl.oid = ix.indrelid
		JOIN   pg_class      cl  ON cl.oid  = ix.indexrelid
		JOIN   pg_namespace  n   ON n.oid   = tcl.relnamespace
		JOIN   LATERAL pg_catalog.unnest(ix.indkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE
		JOIN   pg_attribute  a   ON a.attrelid = ix.indrelid AND a.attnum = u.attnum
		WHERE  n.nspname = $1
		  AND  tcl.relname = $2
		  AND  ix.indisunique
		  AND  ix.indpred  IS NULL
		  AND  ix.indexprs IS NULL
		ORDER  BY cl.relname, u.ord`

	rows, err := db.QueryContext(ctx, q, schema, table)
	if err != nil {
		return "", fmt.Errorf("postgres: read unique indexes for %s: %w", qualifyForRemedy(schema, table), err)
	}
	defer rows.Close()

	type idx struct {
		cols      []string
		immediate bool
	}
	byIndex := map[string]*idx{}
	var order []string
	for rows.Next() {
		var name, col string
		var immediate bool
		var ord int
		if err := rows.Scan(&name, &col, &immediate, &ord); err != nil {
			return "", err
		}
		i, ok := byIndex[name]
		if !ok {
			i = &idx{}
			byIndex[name] = i
			order = append(order, name)
		}
		i.cols = append(i.cols, col)
		i.immediate = immediate
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	sort.Strings(order)
	blocked := ""
	for _, name := range order {
		if !sameColumns(byIndex[name].cols, keyCols) {
			continue
		}
		if byIndex[name].immediate {
			// A usable arbiter exists; nothing to refuse even if a
			// deferrable duplicate over the same columns also exists.
			return "", nil
		}
		if blocked == "" {
			blocked = name
		}
	}
	return blocked, nil
}

// sameColumns reports whether two column lists cover the same SET of
// columns. Order-insensitive on purpose: PG's arbiter inference compares
// the ON CONFLICT list against an index's attribute set (bitmapset), not
// its order, so `ON CONFLICT (b, a)` does infer against `UNIQUE (a, b)`.
// Comparing order-sensitively here would let a reordered composite key
// slip past the check and fail with the raw SQLSTATE 55000 this refusal
// exists to replace.
func sameColumns(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x := append([]string(nil), a...)
	y := append([]string(nil), b...)
	sort.Strings(x)
	sort.Strings(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

// listSchemaTables returns the ordinary + partitioned table names in the
// named schema, sorted, so the preflight walks them deterministically
// (and reports offenders in a stable order).
func listSchemaTables(ctx context.Context, db *sql.DB, schema string) ([]string, error) {
	const q = `
		SELECT c.relname
		FROM   pg_class c
		JOIN   pg_namespace n ON n.oid = c.relnamespace
		WHERE  n.nspname = $1
		  AND  c.relkind IN ('r', 'p')`

	rows, err := db.QueryContext(ctx, q, schema)
	if err != nil {
		return nil, fmt.Errorf("postgres: list tables in schema %q: %w", schema, err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}
