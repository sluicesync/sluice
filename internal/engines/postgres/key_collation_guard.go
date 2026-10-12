// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// The applier backstop for KEY-COLLATION-FIRST-BOUNDARY (option 3), the
// Postgres half; the MySQL applier carries the twin (engines/mysql
// key_collation_guard.go, whose file comment has the full rule).
//
// On Postgres the harm needs a NONDETERMINISTIC target collation on the
// conflict key: `INSERT … ON CONFLICT (k)` matches an existing row whose key
// equals the incoming one under that collation (`'a'` and `'A'` under an
// ICU `ks-level2` collation), and the DO UPDATE overwrites it — or, on a
// table whose every column is a key column, DO NOTHING drops the incoming
// row. A deterministic target collation compares bytes, so it never matches
// two different keys, and its tables carry no guard. Another unique index's
// conflict is not the arbiter's and raises unique_violation, loud already.
//
// Armed, by the operator's choice (b), only where the SOURCE's current
// declaration of the key column — the applier's active schema version,
// which the Postgres reader stamps from pg_attribute.attcollation on every
// Relation (the change stream itself carries none) — names a different
// collation than the target's. The guard compares the two keys under the
// source's collation: a named DETERMINISTIC source collation compares
// bytes, through `COLLATE "C"`; a nondeterministic one compares under its
// own name. A source key on the database default collation carries no
// determinism at the boundary (the reader spells it as the SchemaReader
// does, which the seed is held to raw) and cannot be told from a stream
// that carries no collation at all, so it is not armed — stated residual. Equal there → the same
// source key: a re-applied or re-delivered INSERT, absorbed as before.
// Different → the source has two rows: refuse.
//
// No round trip is added: the check is the DO UPDATE's own WHERE, a CASE
// whose else-branch is an invalid integer cast carrying the marker —
// `(('KEY-COLLATION-MERGE' || pg_catalog.left(EXCLUDED.k::text, 0))::int IS NULL)`. The
// cast's argument depends on the row, so the planner cannot fold it, and
// CASE evaluates the branch only when the keys differ; measured on
// Postgres 16 (the whole multi-row statement rolls back, an identical
// re-apply passes, and the error text keeps the marker without the key's
// value). The classifier attributes that error to
// [ir.KeyCollationMergeError] by the marker.
//
// A source collation the target does not have refuses loudly before any
// row of the table is applied (KEY-COLLATION-GUARD-UNSUPPORTED).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// keyCollationMergeMarker is the literal the guard's raise carries.
const keyCollationMergeMarker = "KEY-COLLATION-MERGE"

// keyCollationGuardUnsupportedMarker names the loud refusal when the guard
// cannot be expressed on this target.
const keyCollationGuardUnsupportedMarker = "KEY-COLLATION-GUARD-UNSUPPORTED"

// keyCollationExistingAlias is the alias the guarded INSERT gives the target
// table, so the DO UPDATE's WHERE can name the existing row.
const keyCollationExistingAlias = "sluice_existing"

// pgKeyCollationPlan is the guard for one table's conflict key.
type pgKeyCollationPlan struct {
	// compare maps each armed conflict-key column to the collation it is
	// compared under ("C" for a deterministic source).
	compare map[string]string
	// marker is the armed column whose incoming value the raise reads.
	marker string
}

// targetKeyCollation is a target column's collation ("" the default) and
// determinism.
type targetKeyCollation struct {
	name          string
	deterministic bool
}

// planPGKeyCollationGuard decides which conflict-key columns are armed.
func planPGKeyCollationGuard(source *ir.Table, key []string, target map[string]targetKeyCollation) *pgKeyCollationPlan {
	if source == nil || len(key) == 0 {
		return nil
	}
	srcCols := map[string]ir.Type{}
	for _, c := range source.Columns {
		if c != nil {
			srcCols[c.Name] = c.Type
		}
	}
	p := &pgKeyCollationPlan{compare: map[string]string{}}
	for _, col := range key {
		tc, ok := target[col]
		if !ok || tc.deterministic {
			continue // a deterministic target key never matches two different values
		}
		st, ok := srcCols[col]
		if !ok {
			continue
		}
		cs, name, det, known := sourceKeyCollation(st)
		if !known || cs != "" {
			continue // unknown, or a MySQL-family collation Postgres cannot express
		}
		switch {
		case det == ir.CollationDeterministic:
			p.compare[col] = "C"
		case name != tc.name:
			p.compare[col] = name
		default:
			continue // the same nondeterministic collation: no merge the source would not make
		}
		if p.marker == "" {
			p.marker = col
		}
	}
	if len(p.compare) == 0 {
		return nil
	}
	return p
}

// sourceKeyCollation reads a source column's declared collation; known is
// false when the boundary carried none (a change stream that cannot see
// it, a non-character column).
func sourceKeyCollation(t ir.Type) (charset, name string, det ir.CollationDeterminism, known bool) {
	switch v := t.(type) {
	case ir.Varchar:
		charset, name, det = v.Charset, v.Collation, v.Determinism
	case ir.Char:
		charset, name, det = v.Charset, v.Collation, v.Determinism
	case ir.Text:
		charset, name, det = v.Charset, v.Collation, v.Determinism
	default:
		return "", "", ir.CollationDeterminismUnknown, false
	}
	return charset, name, det, name != "" || det != ir.CollationDeterminismUnknown
}

// where renders the DO UPDATE's WHERE: true (DO UPDATE runs) when every
// armed key column is equal under the source's collation, the marker's
// raise otherwise. allKey: the table's columns are all conflict-key columns,
// so the guarded DO UPDATE stands in for DO NOTHING and must write nothing
// when the keys agree.
func (p *pgKeyCollationPlan) where(allKey bool) string {
	cols := make([]string, 0, len(p.compare))
	for c := range p.compare {
		cols = append(cols, c)
	}
	sortStrings(cols)
	eq := make([]string, len(cols))
	for i, c := range cols {
		coll := quoteIdent(p.compare[c])
		eq[i] = quoteIdent(keyCollationExistingAlias) + "." + quoteIdent(c) + " COLLATE " + coll +
			" IS NOT DISTINCT FROM EXCLUDED." + quoteIdent(c) + " COLLATE " + coll
	}
	agree := "true"
	if allKey {
		agree = "false"
	}
	raise := "(('" + keyCollationMergeMarker + "' || pg_catalog.left(EXCLUDED." + quoteIdent(p.marker) + "::text, 0))::int IS NULL)"
	return " WHERE CASE WHEN " + strings.Join(eq, " AND ") + " THEN " + agree + " ELSE " + raise + " END"
}

// sortStrings sorts in place (a tiny helper so the rendered SQL is stable).
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// pgKeyCollationGuardEntry caches a table's plan against the source shape it
// was computed from.
type pgKeyCollationGuardEntry struct {
	source *ir.Table
	plan   *pgKeyCollationPlan
}

// keyCollationPlanFor returns the guard plan for an insert into schema.table
// (routed) of a change from srcSchema.table, or nil when nothing is armed.
func (a *ChangeApplier) keyCollationPlanFor(ctx context.Context, srcSchema, schema, table string, key []string) (*pgKeyCollationPlan, error) {
	source, _ := a.ActiveSchema(srcSchema, table)
	qn := schemaTableKey(schema, table)
	a.cacheMu.RLock()
	e, hit := a.keyCollationGuardCache[qn]
	a.cacheMu.RUnlock()
	if hit && e.source == source {
		return e.plan, nil
	}
	var plan *pgKeyCollationPlan
	if source != nil && len(key) > 0 {
		target, err := targetKeyCollations(ctx, a.db, schema, table)
		if err != nil {
			return nil, fmt.Errorf("postgres: applier: read the key collations of %s for the key-collation guard: %w", qn, err)
		}
		plan = planPGKeyCollationGuard(source, key, target)
		if plan != nil {
			if err := checkPGCollationsExist(ctx, a.db, qn, plan); err != nil {
				return nil, err
			}
		}
	}
	a.cacheMu.Lock()
	if a.keyCollationGuardCache == nil {
		a.keyCollationGuardCache = map[string]pgKeyCollationGuardEntry{}
	}
	a.keyCollationGuardCache[qn] = pgKeyCollationGuardEntry{source: source, plan: plan}
	a.cacheMu.Unlock()
	return plan, nil
}

// targetKeyCollations reads every column's collation on the target table.
func targetKeyCollations(ctx context.Context, db *sql.DB, schema, table string) (map[string]targetKeyCollation, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT a.attname,
		       COALESCE(CASE WHEN coll.collname = 'default' THEN '' ELSE coll.collname END, ''),
		       COALESCE(coll.collisdeterministic, true)
		FROM   pg_attribute a
		JOIN   pg_class      cl ON cl.oid = a.attrelid
		JOIN   pg_namespace  n  ON n.oid  = cl.relnamespace
		LEFT JOIN pg_collation coll ON coll.oid = a.attcollation AND a.attcollation <> 0
		WHERE  n.nspname  = $1
		  AND  cl.relname = $2
		  AND  a.attnum   > 0
		  AND  NOT a.attisdropped`, schema, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]targetKeyCollation{}
	for rows.Next() {
		var name string
		var tc targetKeyCollation
		if err := rows.Scan(&name, &tc.name, &tc.deterministic); err != nil {
			return nil, err
		}
		out[name] = tc
	}
	return out, rows.Err()
}

// checkPGCollationsExist refuses loudly when the target has no collation the
// guard must compare under.
func checkPGCollationsExist(ctx context.Context, db *sql.DB, qn string, plan *pgKeyCollationPlan) error {
	for col, coll := range plan.compare {
		var probe sql.NullString
		if err := db.QueryRowContext(ctx, "SELECT ''::text COLLATE "+quoteIdent(coll)).Scan(&probe); err != nil {
			return &terminalPGError{err: fmt.Errorf("%w: %s: postgres: applier: %s: the source declares key column %q "+
				"with collation %q, which this target does not have (%v), so sluice cannot check that an upsert does "+
				"not merge two source rows the target's collation equates. Refusing before any row of the table is "+
				"applied; create the collation on the target, or give the key a collation that compares as the "+
				"source's does", ir.ErrKeyCollationMerge, keyCollationGuardUnsupportedMarker, qn, col, coll, err)}
		}
	}
	return nil
}

// isKeyCollationMergeRaise reports whether err is the guard's raise: the
// invalid integer cast (22P02) whose text carries the marker.
func isKeyCollationMergeRaise(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "22P02" && strings.Contains(pgErr.Message, keyCollationMergeMarker)
}
