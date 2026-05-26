// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// ADR-0065 — Shape A recognized-shape catalog: CHECK constraint
// changes (task #22). Per-shape DDL emit for PG.
//
// Each method is idempotent on the post-state via detect-then-emit
// against pg_catalog.pg_constraint (contype = 'c'). PG supports
// `ADD CONSTRAINT IF NOT EXISTS` only via the trick of detecting
// presence and skipping; sluice does that uniformly so the
// takeover-stream's re-apply path on a `NotApplied` probe is safe.
//
// Cross-dialect Expr values run through the existing translator
// (translateCheckExpr) at the writer boundary; the applier also
// runs a refuse-loudly pre-flight check against a list of
// well-known untranslated MySQL→PG tokens so the operator gets a
// clear error instead of a SQLSTATE 42601 syntax-error from the
// server.

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// AlterAddCheck implements [ir.ShapeDeltaApplier] for Postgres
// (ADR-0065). Emits `ALTER TABLE <t> ADD CONSTRAINT <name>
// CHECK (<expr>)` per constraint. Detect-then-emit on
// pg_constraint for idempotency. Cross-dialect Expr is routed
// through translateCheckExpr; the pre-flight refuse-loudly check
// surfaces unrecognized cross-dialect tokens before the SQL fires.
func (w *SchemaWriter) AlterAddCheck(ctx context.Context, table *ir.Table, checks []*ir.CheckConstraint) error {
	if len(checks) == 0 {
		return nil
	}
	schemaName := w.schema
	if table.Schema != "" {
		schemaName = table.Schema
	}
	qualified := w.qualifyTable(table)
	for _, chk := range checks {
		if chk == nil || chk.Name == "" {
			return errors.New("postgres: alter add check: constraint name is required")
		}
		exists, err := pgCheckConstraintExists(ctx, w.db, schemaName, table.Name, chk.Name)
		if err != nil {
			return fmt.Errorf("alter add check: probe %q: %w", chk.Name, err)
		}
		if exists {
			continue
		}
		exprText := translateCheckExpr(chk, table, w.emitOpts())
		if err := refuseUntranslatedCheckExprPG(chk, exprText); err != nil {
			return fmt.Errorf("alter add check %q on %s.%s: %w",
				chk.Name, table.Schema, table.Name, err)
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)",
			qualified, quoteIdent(chk.Name), exprText)
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("alter add check %q on %s.%s: %w",
				chk.Name, table.Schema, table.Name, err)
		}
	}
	return nil
}

// AlterDropCheck implements [ir.ShapeDeltaApplier] for Postgres
// (ADR-0065). PG supports `DROP CONSTRAINT IF EXISTS` natively
// (since 9.0), making the call idempotent across re-runs.
func (w *SchemaWriter) AlterDropCheck(ctx context.Context, table *ir.Table, checks []*ir.CheckConstraint) error {
	if len(checks) == 0 {
		return nil
	}
	qualified := w.qualifyTable(table)
	for _, chk := range checks {
		if chk == nil || chk.Name == "" {
			return errors.New("postgres: alter drop check: constraint name is required")
		}
		stmt := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s",
			qualified, quoteIdent(chk.Name))
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("alter drop check %q on %s.%s: %w",
				chk.Name, table.Schema, table.Name, err)
		}
	}
	return nil
}

// AlterModifyCheck implements [ir.ShapeDeltaApplier] for Postgres
// (ADR-0065). Neither PG nor MySQL supports in-place expression
// rewrite of an existing CHECK constraint without dropping +
// re-adding. The applier emits a refuse-loudly pre-flight check on
// the new constraint BEFORE the DROP fires so an untranslated
// cross-dialect Expr doesn't leave the target with the DROP
// applied but no replacement ADD landed.
func (w *SchemaWriter) AlterModifyCheck(ctx context.Context, table *ir.Table, oldConstraint, newConstraint *ir.CheckConstraint) error {
	if oldConstraint == nil || newConstraint == nil {
		return errors.New("postgres: alter modify check: oldConstraint and newConstraint must be non-nil")
	}
	if oldConstraint.Name == "" || newConstraint.Name == "" {
		return errors.New("postgres: alter modify check: constraint names must be non-empty")
	}
	// Pre-flight refuse-loudly check on the NEW expression. We want
	// the operator-visible refusal to fire BEFORE the DROP commits
	// (catalog-level changes auto-commit on PG too — the operator
	// would otherwise need to manually restore the dropped CHECK).
	exprText := translateCheckExpr(newConstraint, table, w.emitOpts())
	if err := refuseUntranslatedCheckExprPG(newConstraint, exprText); err != nil {
		return fmt.Errorf("alter modify check %q on %s.%s: %w",
			newConstraint.Name, table.Schema, table.Name, err)
	}
	if err := w.AlterDropCheck(ctx, table, []*ir.CheckConstraint{oldConstraint}); err != nil {
		return err
	}
	return w.AlterAddCheck(ctx, table, []*ir.CheckConstraint{newConstraint})
}

// pgCheckConstraintExists reports whether a CHECK constraint named
// constraintName is present on schemaName.tableName. Probes
// pg_catalog.pg_constraint with contype = 'c' so the check is
// scoped to CHECKs (not foreign keys or uniques with the same
// name in pg_constraint).
func pgCheckConstraintExists(ctx context.Context, db sqlExecQueryer, schemaName, tableName, constraintName string) (bool, error) {
	const q = `SELECT EXISTS(
		SELECT 1 FROM pg_catalog.pg_constraint con
		JOIN pg_catalog.pg_class       rel ON rel.oid       = con.conrelid
		JOIN pg_catalog.pg_namespace   nsp ON nsp.oid       = rel.relnamespace
		WHERE nsp.nspname = $1 AND rel.relname = $2 AND con.conname = $3 AND con.contype = 'c'
	)`
	rows, err := db.QueryContext(ctx, q, schemaName, tableName, constraintName)
	if err != nil {
		return false, fmt.Errorf("pg_catalog.pg_constraint probe: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if rowsErr := rows.Err(); rowsErr != nil {
			return false, rowsErr
		}
		return false, nil
	}
	var present bool
	if scanErr := rows.Scan(&present); scanErr != nil {
		return false, fmt.Errorf("pg_catalog.pg_constraint scan: %w", scanErr)
	}
	return present, rows.Err()
}

// untranslatedMySQLToPGTokens lists tokens that survive
// translateExprForMySQL→PG passes unchanged when the source CHECK
// expression uses MySQL-only constructs. A post-translation
// expression containing any of these is almost-certainly going to
// fail at the PG parser with SQLSTATE 42601; refusing pre-emit
// gives a cleaner operator-visible error and avoids leaving a
// partially-modified constraint.
//
// The list is intentionally conservative — it covers the common
// MySQL-only spellings (json_extract, IF(...), date_format with
// MySQL format specifiers). Operators with novel cross-dialect
// expressions can bypass via --expr-override per ADR-0016.
var untranslatedMySQLToPGTokens = []string{
	"json_extract(",
	"json_unquote(",
	"date_format(",
	"str_to_date(",
}

// refuseUntranslatedCheckExprPG returns a refuse-loudly error when
// the post-translation CHECK Expr text contains a well-known
// MySQL-only token that PG cannot parse. Only fires on
// cross-dialect cases (chk.ExprDialect != "" and != "postgres");
// same-dialect Exprs (PG → PG or untagged) pass through unchanged.
func refuseUntranslatedCheckExprPG(chk *ir.CheckConstraint, exprText string) error {
	if chk == nil || chk.ExprDialect == "" || chk.ExprDialect == dialectName {
		return nil
	}
	lower := strings.ToLower(exprText)
	for _, tok := range untranslatedMySQLToPGTokens {
		if strings.Contains(lower, tok) {
			return fmt.Errorf(
				"refuse loudly: CHECK constraint %q expression carries untranslated "+
					"%s-dialect token %q in cross-engine apply (post-translation expr: %q). "+
					"Operator recovery: drop the source-side change and re-issue with "+
					"--expr-override=<constraint-name>=<pg-equivalent-expr>, OR run "+
					"the drained model (sluice sync stop --wait / migrate / start --resume)",
				chk.Name, chk.ExprDialect, tok, exprText,
			)
		}
	}
	return nil
}
