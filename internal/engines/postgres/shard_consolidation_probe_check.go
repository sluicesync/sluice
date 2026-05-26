// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// ADR-0064 Shape A catalog expansion: CHECK constraint probes for
// the PG ChangeApplier (ADR-0054 §4 takeover-stream contract).

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// ProbeAddCheck implements [ir.ShardConsolidationProber] for PG
// (ADR-0064). Applied when ALL named CHECK constraints exist on the
// target; NotApplied when NONE exist; Inconsistent on partial.
func (a *ChangeApplier) ProbeAddCheck(ctx context.Context, table *ir.Table, checks []*ir.CheckConstraint) (ir.ProbeOutcome, error) {
	if len(checks) == 0 {
		return ir.ProbeOutcomeApplied, nil
	}
	schemaName := a.probeSchemaFor(table)
	present, err := a.countCheckConstraintsPresent(ctx, schemaName, table.Name, checks)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, err
	}
	return classifyProbeCount(present, len(checks)), nil
}

// ProbeDropCheck inverts ProbeAddCheck (Applied when NONE exist).
func (a *ChangeApplier) ProbeDropCheck(ctx context.Context, table *ir.Table, checks []*ir.CheckConstraint) (ir.ProbeOutcome, error) {
	if len(checks) == 0 {
		return ir.ProbeOutcomeApplied, nil
	}
	schemaName := a.probeSchemaFor(table)
	present, err := a.countCheckConstraintsPresent(ctx, schemaName, table.Name, checks)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, err
	}
	switch present {
	case 0:
		return ir.ProbeOutcomeApplied, nil
	case len(checks):
		return ir.ProbeOutcomeNotApplied, nil
	default:
		return ir.ProbeOutcomeInconsistent, nil
	}
}

// ProbeModifyCheck implements the MODIFY-shape probe. Outcomes:
//
//   - oldName present + newConstraint.Name absent → NotApplied (prior
//     holder crashed between DROP and ADD on a same-name modify;
//     OR before the DROP fired on a rename-modify).
//   - oldName absent + newConstraint.Name present and its observed
//     expression matches → Applied.
//   - oldName absent + newConstraint.Name present but observed
//     expression differs → Inconsistent + error naming the mismatch.
//     Mirrors the v0.76.0 ProbeAlterColumnType v2 silent-divergence
//     catch on the type-alter shape (a DROP+ADD where the ADD landed
//     a different expression must NOT pass the existence-only check).
//   - Both present → Inconsistent (partial state — the DROP half
//     didn't fire even though the ADD did, or the operator
//     intervened).
//   - Both absent → Inconsistent (catastrophic — the constraint
//     should exist under one of the two names).
func (a *ChangeApplier) ProbeModifyCheck(ctx context.Context, table *ir.Table, oldName string, newConstraint *ir.CheckConstraint) (ir.ProbeOutcome, error) {
	if oldName == "" {
		return ir.ProbeOutcomeInconsistent, errors.New("postgres: probe modify check: oldName is required")
	}
	if newConstraint == nil || newConstraint.Name == "" {
		return ir.ProbeOutcomeInconsistent, errors.New("postgres: probe modify check: newConstraint must have a Name")
	}
	schemaName := a.probeSchemaFor(table)
	oldPresent, newPresent, newExpr, err := pgCheckPairPresence(ctx, a.db, schemaName, table.Name, oldName, newConstraint.Name)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, fmt.Errorf("postgres: probe modify check: %w", err)
	}
	switch {
	case oldPresent && !newPresent:
		return ir.ProbeOutcomeNotApplied, nil
	case !oldPresent && newPresent:
		if checkExprsEquivalent(newExpr, newConstraint.Expr) {
			return ir.ProbeOutcomeApplied, nil
		}
		return ir.ProbeOutcomeInconsistent, fmt.Errorf(
			"postgres: probe modify check %s.%s.%s: observed expression %q does not match recorded %q",
			schemaName, table.Name, newConstraint.Name, newExpr, newConstraint.Expr,
		)
	case oldPresent && newPresent:
		return ir.ProbeOutcomeInconsistent, fmt.Errorf(
			"postgres: probe modify check %s.%s: both %q (pre) and %q (post) constraints exist",
			schemaName, table.Name, oldName, newConstraint.Name,
		)
	default: // both absent
		return ir.ProbeOutcomeInconsistent, fmt.Errorf(
			"postgres: probe modify check %s.%s: neither %q nor %q constraint exists",
			schemaName, table.Name, oldName, newConstraint.Name,
		)
	}
}

// countCheckConstraintsPresent returns the number of named CHECK
// constraints present on the target's pg_constraint (contype='c').
func (a *ChangeApplier) countCheckConstraintsPresent(ctx context.Context, schemaName, tableName string, checks []*ir.CheckConstraint) (int, error) {
	names := make([]string, 0, len(checks))
	for _, c := range checks {
		if c == nil || c.Name == "" {
			continue
		}
		names = append(names, c.Name)
	}
	if len(names) == 0 {
		return 0, nil
	}
	const q = `SELECT COUNT(*) FROM pg_catalog.pg_constraint con
		JOIN pg_catalog.pg_class     rel ON rel.oid     = con.conrelid
		JOIN pg_catalog.pg_namespace nsp ON nsp.oid     = rel.relnamespace
		WHERE nsp.nspname = $1 AND rel.relname = $2 AND con.contype = 'c' AND con.conname = ANY($3)`
	var n int
	if err := a.db.QueryRowContext(ctx, q, schemaName, tableName, pgTextArray(names)).Scan(&n); err != nil {
		return 0, fmt.Errorf("postgres: probe checks: %w", err)
	}
	return n, nil
}

// pgCheckPairPresence reports whether oldName and newName each
// exist as CHECK constraints on schemaName.tableName, and returns
// the observed (pg_get_constraintdef) expression text for newName
// when present. One query for both names so the probe is a single
// round-trip.
func pgCheckPairPresence(ctx context.Context, db sqlExecQueryer, schemaName, tableName, oldName, newName string) (oldPresent, newPresent bool, newExpr string, err error) {
	const q = `
		SELECT con.conname,
		       pg_catalog.pg_get_constraintdef(con.oid, true)
		FROM   pg_catalog.pg_constraint con
		JOIN   pg_catalog.pg_class      rel ON rel.oid     = con.conrelid
		JOIN   pg_catalog.pg_namespace  nsp ON nsp.oid     = rel.relnamespace
		WHERE  nsp.nspname = $1 AND rel.relname = $2 AND con.contype = 'c' AND con.conname = ANY($3)`
	rows, err := db.QueryContext(ctx, q, schemaName, tableName, []string{oldName, newName})
	if err != nil {
		return false, false, "", err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name, def string
		if scanErr := rows.Scan(&name, &def); scanErr != nil {
			return false, false, "", scanErr
		}
		switch name {
		case oldName:
			oldPresent = true
		case newName:
			newPresent = true
			newExpr = extractCheckExprBody(def)
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return false, false, "", rowsErr
	}
	return oldPresent, newPresent, newExpr, nil
}

// extractCheckExprBody strips PG's `CHECK ((...))` wrapper from a
// pg_get_constraintdef body, returning the inner expression text.
// PG renders a CHECK constraint definition as `CHECK ((<expr>))` —
// the outer pair are syntactic constants, the inner pair come from
// the original SQL. We strip exactly one outer "CHECK (" / ")" pair;
// any nested parens stay as part of the expression text.
//
// On malformed input (no CHECK prefix), returns the body unchanged
// so the caller's equality comparison falls back to literal match.
func extractCheckExprBody(def string) string {
	const prefix = "CHECK ("
	trimmed := strings.TrimSpace(def)
	if !strings.HasPrefix(trimmed, prefix) || !strings.HasSuffix(trimmed, ")") {
		return trimmed
	}
	return strings.TrimSpace(trimmed[len(prefix) : len(trimmed)-1])
}

// checkExprsEquivalent reports whether two CHECK expression texts
// are equivalent for probe-comparison purposes. PG normalizes
// expression text in pg_get_constraintdef (always-double-parens,
// canonical operator spacing); the source IR Expr is the
// quote-stripped text from the SchemaReader, which uses the
// source's original spacing. Equality is whitespace-collapsed +
// outer-paren-stripped on both sides; that's enough to catch the
// "expression changed" silent-divergence case without false
// positives from cosmetic spelling differences PG round-trips.
func checkExprsEquivalent(observed, recorded string) bool {
	return normalizeCheckExprForCompare(observed) == normalizeCheckExprForCompare(recorded)
}

// normalizeCheckExprForCompare collapses whitespace and strips
// matched outer parens until neither side has further wrapping.
// Used by checkExprsEquivalent.
func normalizeCheckExprForCompare(s string) string {
	out := strings.Join(strings.Fields(s), " ")
	for len(out) >= 2 && out[0] == '(' && out[len(out)-1] == ')' &&
		isMatchedOuterParens(out) {
		out = strings.TrimSpace(out[1 : len(out)-1])
	}
	return out
}

// isMatchedOuterParens reports whether the first and last paren of
// s match each other (so stripping them yields a balanced
// expression). Tracks paren depth across the string; the outer
// parens match only if depth never drops to 0 between them.
func isMatchedOuterParens(s string) bool {
	if len(s) < 2 {
		return false
	}
	depth := 0
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && i < len(s)-1 {
			return false
		}
	}
	return true
}
