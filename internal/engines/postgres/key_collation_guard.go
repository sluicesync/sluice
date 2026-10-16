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
// own name. A source key on the database default collation is stamped
// deterministic by both Postgres readers (the catalog's own answer for the
// 'default' row on the CDC side), so it compares bytes too; an empty name
// with no determinism means the boundary carried no collation at all (a
// schema decoded from an older binary) and arms nothing, while a NAMED
// collation with no determinism (not established for the rows: see
// planPGKeyCollationGuard) compares bytes. Equal there → the
// same source key: a re-applied or re-delivered INSERT, absorbed as before.
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
	// targetHasSource lists the armed columns the target already declares
	// under the source's collation name, compared as bytes only because that
	// collation is not established for the rows
	// (KEY-COLLATION-PG-UNESTABLISHED-REMEDY): for them, giving the target
	// key the source's collation cannot converge.
	targetHasSource []string
}

// targetKeyCollation is a target column's collation ("" the default) and
// determinism.
type targetKeyCollation struct {
	name          string
	deterministic bool
}

// planPGKeyCollationGuard decides which conflict-key columns are armed.
// unexpressible lists, as "column (charset collation)", each NONDETERMINISTIC
// target key column whose source declares a MySQL-family collation: Postgres
// has no collation that compares as it does, so the guard cannot be written
// and the caller refuses (KEY-COLLATION-GUARD-UNSUPPORTED) rather than apply
// unguarded (review LOW). A deterministic target key needs no guard whatever
// the source says.
func planPGKeyCollationGuard(source *ir.Table, key []string, target map[string]targetKeyCollation) (plan *pgKeyCollationPlan, unexpressible []string) {
	if source == nil || len(key) == 0 {
		return nil, nil
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
		if !known {
			continue // the boundary carried no collation: stated residual
		}
		if cs != "" {
			unexpressible = append(unexpressible, fmt.Sprintf("%s (%s %s)", col, cs, name))
			continue
		}
		switch {
		case det == ir.CollationDeterministic:
			p.compare[col] = "C"
		case det == ir.CollationDeterminismUnknown:
			// Named but not established: the CDC reader read the collation
			// from a catalog row newer than the rows it describes
			// (KEY-COLLATION-PG-LOOSENED-REPLAY), or the schema predates the
			// determinism stamp. Compared as bytes, as the MySQL guard does:
			// the rows may have been written under a stricter collation than
			// the one read, so even the target's own name is no proof that
			// two byte-different keys are one source row.
			p.compare[col] = "C"
			if name == tc.name {
				p.targetHasSource = append(p.targetHasSource, col)
			}
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
		return nil, unexpressible
	}
	return p, unexpressible
}

// sourceKeyCollation reads a source column's declared collation; known is
// false when the boundary carried none (a change stream that cannot see
// it, a non-character column). A domain over a character type has its base
// type's collation.
func sourceKeyCollation(t ir.Type) (charset, name string, det ir.CollationDeterminism, known bool) {
	switch v := t.(type) {
	case ir.Domain:
		return sourceKeyCollation(v.BaseType)
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
	return " WHERE " + p.predicate(allKey)
}

// predicate is [pgKeyCollationPlan.where] without the keyword, for a
// statement whose WHERE carries another predicate too (the copy writer's
// Neki shard-key guard).
func (p *pgKeyCollationPlan) predicate(allKey bool) string {
	agree := "true"
	if allKey {
		agree = "false"
	}
	return p.predicateAgreeing(agree)
}

// predicateAgreeing is [pgKeyCollationPlan.predicate] with agree as the value
// when the keys are equal under the source's collation — the conflict-identity
// guard's own check (unique_key_conflict_guard.go), so it runs only for a row
// this guard passed and a collation merge is reported as one.
func (p *pgKeyCollationPlan) predicateAgreeing(agree string) string {
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
	raise := "(('" + keyCollationMergeMarker + "' || pg_catalog.left(EXCLUDED." + quoteIdent(p.marker) + "::text, 0))::int IS NULL)"
	return "CASE WHEN " + strings.Join(eq, " AND ") + " THEN " + agree + " ELSE " + raise + " END"
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
	source := a.guardSourceShape(srcSchema, table)
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
		var unexpressible []string
		plan, unexpressible = planPGKeyCollationGuard(source, key, target)
		if len(unexpressible) > 0 {
			return nil, keyCollationGuardUnexpressible(qn, unexpressible)
		}
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

// keyCollationGuardUnexpressible is the GUARD-UNSUPPORTED refusal for the
// unexpressible columns [planPGKeyCollationGuard] returned, shared by the
// applier and the copy writer.
func keyCollationGuardUnexpressible(qn string, unexpressible []string) error {
	return &terminalPGError{err: fmt.Errorf("%w: %s: postgres: %s: the target compares key "+
		"column(s) %s under a nondeterministic collation, and the source declares a MySQL-family collation "+
		"Postgres has no equivalent for, so sluice cannot check that an upsert does not merge two source "+
		"rows the target's collation equates. Refusing before the table's first upsert; give the "+
		"target key a deterministic collation (the database default, or \"C\"), or re-copy the table. Nothing of this upsert was applied",
		ir.ErrKeyCollationMerge, keyCollationGuardUnsupportedMarker, qn, strings.Join(unexpressible, ", "))}
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
			return &terminalPGError{err: fmt.Errorf("%w: %s: postgres: %s: the source declares key column %q "+
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

// applyKeyCollationMergeError attributes a guarded CDC insert's failure
// ([isKeyCollationMergeRaise]) to [ir.KeyCollationMergeError], naming the
// table and the apply path's remedy; anything else is returned unchanged.
// Shared by the serial dispatch and the pipelined batch, which attributes a
// flushed error to the statement it queued — the classifier's own
// attribution ([classifyApplierError]) sees neither the table nor the plan,
// so through it the refusal named no table and the generic remedy
// (KEY-COLLATION-PG-UNESTABLISHED-REMEDY, the wider half).
func applyKeyCollationMergeError(err error, plan *pgKeyCollationPlan, schema, table string) error {
	if plan == nil || !isKeyCollationMergeRaise(err) {
		return err
	}
	return &ir.KeyCollationMergeError{
		Schema: schema, Table: table, Cause: err,
		Remedy: applyKeyCollationMergeRemedy(plan, schema, table),
	}
}

// applyKeyCollationMergeRemedy is the CDC apply path's remedy. It names the
// collation the guard compared each armed column under, as the copy's
// remedy does ([copyKeyCollationMergeRemedy]) — never "the source's
// collation", which the target may already have: a key armed only because
// its collation was not established for the rows
// ([pgKeyCollationPlan.targetHasSource]) carries the source's name on the
// target already, and is compared as bytes because the rows may have been
// written under a stricter collation. There, altering the key to the
// source's collation changes nothing, so only the re-copy is offered: it
// copies the source's rows as they stand now, under the collation they now
// have.
func applyKeyCollationMergeRemedy(plan *pgKeyCollationPlan, schema, table string) string {
	if len(plan.targetHasSource) > 0 {
		cols := append([]string(nil), plan.targetHasSource...)
		sortStrings(cols)
		for i, c := range cols {
			cols[i] = quoteIdent(c)
		}
		return fmt.Sprintf("The target's key column(s) %s of %s.%s already carry the source's collation; sluice "+
			"compared them byte for byte because the change stream could not establish the collation these rows "+
			"were written under (the column was altered after them), so giving the target key the source's "+
			"collation cannot converge. Re-copy the table, which copies the source's rows as they stand now: %s",
			strings.Join(cols, ", "), quoteIdent(schema), quoteIdent(table), keyCollationRecopy)
	}
	return fmt.Sprintf("Give the target's key column(s) %s (`ALTER TABLE %s.%s ALTER COLUMN <column> TYPE <type> "+
		"COLLATE …`) and restart, or re-copy the table: %s recreates it with the source's collation",
		keyCollationNeeds(plan), quoteIdent(schema), quoteIdent(table), keyCollationRecopy)
}

// keyCollationNeeds names, per armed column in name order, the collation the
// guard compared it under (`"k" COLLATE "C"`).
func keyCollationNeeds(plan *pgKeyCollationPlan) string {
	cols := make([]string, 0, len(plan.compare))
	for c := range plan.compare {
		cols = append(cols, c)
	}
	sortStrings(cols)
	need := make([]string, len(cols))
	for i, c := range cols {
		need[i] = quoteIdent(c) + " COLLATE " + quoteIdent(plan.compare[c])
	}
	return strings.Join(need, ", ")
}

// keyCollationRecopy names the re-copy for each command that reaches the
// applier's remedy, each with the flag that command accepts (Bug 308; the
// MySQL twin carries the same text). `restore` replaces the remedy with its
// own (backup.restoreKeyCollationRemedy).
const keyCollationRecopy = "`sluice sync start --restart-from-scratch` (or, replaying a backup chain, " +
	"`sluice sync from-backup run --reset-target-data --yes`)"

// guardSourceShape is the source shape the key-collation guard arms from:
// the active schema version for srcSchema.table, or, when none is active, the
// shape a replay seeded through [ChangeApplier.SeedSourceShapes].
func (a *ChangeApplier) guardSourceShape(srcSchema, table string) *ir.Table {
	if source, ok := a.ActiveSchema(srcSchema, table); ok && source != nil {
		return source
	}
	return a.seededShapes.Lookup(srcSchema, table)
}

// SeedSourceShapes implements [ir.GuardSourceShapeSeeder].
func (a *ChangeApplier) SeedSourceShapes(tables []*ir.Table) {
	a.seededShapes.Seed(tables)
}
