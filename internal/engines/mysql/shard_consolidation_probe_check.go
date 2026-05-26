// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// ADR-0064 Shape A catalog expansion: CHECK constraint probes for
// the MySQL ChangeApplier (ADR-0054 §4 takeover-stream contract).

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// ProbeAddCheck implements [ir.ShardConsolidationProber] for MySQL
// (ADR-0064). Applied when ALL named CHECK constraints exist on the
// target; NotApplied when NONE exist; Inconsistent on partial.
func (a *ChangeApplier) ProbeAddCheck(ctx context.Context, table *ir.Table, checks []*ir.CheckConstraint) (ir.ProbeOutcome, error) {
	if len(checks) == 0 {
		return ir.ProbeOutcomeApplied, nil
	}
	present, err := a.countCheckConstraintsPresent(ctx, table.Name, checks)
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
	present, err := a.countCheckConstraintsPresent(ctx, table.Name, checks)
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

// ProbeModifyCheck implements the MODIFY-shape probe. Outcomes
// mirror the PG impl:
//
//   - oldName present + newConstraint.Name absent → NotApplied.
//   - oldName absent + newConstraint.Name present and expression
//     matches → Applied.
//   - oldName absent + newConstraint.Name present and expression
//     differs → Inconsistent + error.
//   - Both present OR both absent → Inconsistent + error.
func (a *ChangeApplier) ProbeModifyCheck(ctx context.Context, table *ir.Table, oldName string, newConstraint *ir.CheckConstraint) (ir.ProbeOutcome, error) {
	if oldName == "" {
		return ir.ProbeOutcomeInconsistent, errors.New("mysql: probe modify check: oldName is required")
	}
	if newConstraint == nil || newConstraint.Name == "" {
		return ir.ProbeOutcomeInconsistent, errors.New("mysql: probe modify check: newConstraint must have a Name")
	}
	oldPresent, newPresent, newExpr, err := mysqlCheckPairPresence(ctx, a, table.Name, oldName, newConstraint.Name)
	if err != nil {
		return ir.ProbeOutcomeInconsistent, fmt.Errorf("mysql: probe modify check: %w", err)
	}
	switch {
	case oldPresent && !newPresent:
		return ir.ProbeOutcomeNotApplied, nil
	case !oldPresent && newPresent:
		if mysqlCheckExprsEquivalent(newExpr, newConstraint.Expr) {
			return ir.ProbeOutcomeApplied, nil
		}
		return ir.ProbeOutcomeInconsistent, fmt.Errorf(
			"mysql: probe modify check %s.%s: observed expression %q does not match recorded %q",
			table.Name, newConstraint.Name, newExpr, newConstraint.Expr,
		)
	case oldPresent && newPresent:
		return ir.ProbeOutcomeInconsistent, fmt.Errorf(
			"mysql: probe modify check %s: both %q (pre) and %q (post) constraints exist",
			table.Name, oldName, newConstraint.Name,
		)
	default: // both absent
		return ir.ProbeOutcomeInconsistent, fmt.Errorf(
			"mysql: probe modify check %s: neither %q nor %q constraint exists",
			table.Name, oldName, newConstraint.Name,
		)
	}
}

// countCheckConstraintsPresent returns the number of named CHECK
// constraints present on the named table. CHECK constraint names
// in MySQL are unique per schema, but a CHECK constraint is owned
// by a single table; scoping the probe to the table via
// information_schema.TABLE_CONSTRAINTS avoids false-positive matches
// against a same-named CHECK on a sibling table in the same schema
// (the lease primitive is per-table — the probe must mirror that
// scope).
func (a *ChangeApplier) countCheckConstraintsPresent(ctx context.Context, tableName string, checks []*ir.CheckConstraint) (int, error) {
	placeholders := make([]string, 0, len(checks))
	args := []any{tableName}
	for _, c := range checks {
		if c == nil || c.Name == "" {
			continue
		}
		placeholders = append(placeholders, "?")
		args = append(args, c.Name)
	}
	if len(placeholders) == 0 {
		return 0, nil
	}
	q := fmt.Sprintf(`SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = DATABASE()
		  AND TABLE_NAME = ?
		  AND CONSTRAINT_TYPE = 'CHECK'
		  AND CONSTRAINT_NAME IN (%s)`,
		strings.Join(placeholders, ","))
	var n int
	if err := a.db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("mysql: probe checks: %w", err)
	}
	return n, nil
}

// mysqlCheckPairPresence returns presence + post-constraint
// expression text for (oldName, newName) in a single round-trip.
// Scoped to the named table via a JOIN with
// information_schema.TABLE_CONSTRAINTS so a same-named CHECK on a
// sibling table doesn't bleed into this probe. CHECK_CLAUSE carries
// the expression body (with the wrapping parentheses MySQL adds).
func mysqlCheckPairPresence(ctx context.Context, a *ChangeApplier, tableName, oldName, newName string) (oldPresent, newPresent bool, newExpr string, err error) {
	const q = `SELECT tc.CONSTRAINT_NAME, cc.CHECK_CLAUSE
		FROM   information_schema.TABLE_CONSTRAINTS tc
		JOIN   information_schema.CHECK_CONSTRAINTS cc
		       ON cc.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
		      AND cc.CONSTRAINT_NAME   = tc.CONSTRAINT_NAME
		WHERE  tc.CONSTRAINT_SCHEMA = DATABASE()
		  AND  tc.TABLE_NAME        = ?
		  AND  tc.CONSTRAINT_TYPE   = 'CHECK'
		  AND  tc.CONSTRAINT_NAME   IN (?, ?)`
	rows, err := a.db.QueryContext(ctx, q, tableName, oldName, newName)
	if err != nil {
		return false, false, "", err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name, clause string
		if scanErr := rows.Scan(&name, &clause); scanErr != nil {
			return false, false, "", scanErr
		}
		switch name {
		case oldName:
			oldPresent = true
		case newName:
			newPresent = true
			newExpr = clause
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return false, false, "", rowsErr
	}
	return oldPresent, newPresent, newExpr, nil
}

// mysqlCheckExprsEquivalent reports whether two CHECK expression
// texts are equivalent for probe-comparison. MySQL's
// CHECK_CONSTRAINTS.CHECK_CLAUSE adds outer parens around the
// expression; equality is whitespace-collapsed + outer-paren-stripped
// on both sides — enough to catch the "expression changed"
// silent-divergence case without false positives from cosmetic
// spelling differences MySQL round-trips.
func mysqlCheckExprsEquivalent(observed, recorded string) bool {
	return normalizeMySQLCheckExprForCompare(observed) == normalizeMySQLCheckExprForCompare(recorded)
}

func normalizeMySQLCheckExprForCompare(s string) string {
	out := strings.Join(strings.Fields(s), " ")
	for len(out) >= 2 && out[0] == '(' && out[len(out)-1] == ')' &&
		isMatchedOuterParensMySQL(out) {
		out = strings.TrimSpace(out[1 : len(out)-1])
	}
	return out
}

// isMatchedOuterParensMySQL is a per-package copy of the same
// helper used in the PG probe. The two impls are tiny; keeping
// them per-package avoids cross-engine imports.
func isMatchedOuterParensMySQL(s string) bool {
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
