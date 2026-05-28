// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// ADR-0065 — Shape A recognized-shape catalog: CHECK constraint
// changes (task #22). Per-shape DDL emit for MySQL 8.0+ (MySQL 5.7
// silently ignored CHECKs — sluice supports 8.0+ where CHECK is
// enforced).
//
// Each method is idempotent on the post-state via detect-then-emit
// against information_schema.CHECK_CONSTRAINTS — MySQL has no
// `ADD CONSTRAINT IF NOT EXISTS` for CHECKs in 8.0.x, so probing
// before emit is the portable pattern.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// AlterAddCheck implements [ir.ShapeDeltaApplier] for MySQL
// (ADR-0065). Emits `ALTER TABLE <t> ADD CONSTRAINT <name>
// CHECK (<expr>)` per constraint, detect-then-emit on
// information_schema.CHECK_CONSTRAINTS for idempotency.
func (w *SchemaWriter) AlterAddCheck(ctx context.Context, table *ir.Table, checks []*ir.CheckConstraint) error {
	if len(checks) == 0 {
		return nil
	}
	for _, chk := range checks {
		if chk == nil || chk.Name == "" {
			return errors.New("mysql: alter add check: constraint name is required")
		}
		exists, err := mysqlCheckConstraintExists(ctx, w.db, w.schema, table.Name, chk.Name)
		if err != nil {
			return fmt.Errorf("alter add check: probe %q: %w", chk.Name, err)
		}
		if exists {
			continue
		}
		exprText := translateCheckExpr(chk)
		if err := refuseUntranslatedCheckExprMySQL(chk, exprText); err != nil {
			return fmt.Errorf("alter add check %q on %s: %w", chk.Name, table.Name, err)
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)",
			quoteIdent(table.Name), quoteIdent(chk.Name), exprText)
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("alter add check %q on %s: %w", chk.Name, table.Name, err)
		}
	}
	return nil
}

// AlterDropCheck implements [ir.ShapeDeltaApplier] for MySQL
// (ADR-0065). MySQL 8.0+ does not support `DROP CONSTRAINT IF
// EXISTS` for CHECKs (added in 8.0.19's `DROP CHECK <name>` but
// the IF EXISTS qualifier landed only in 8.0.29 and even then only
// for some constraint kinds). Detect-then-DROP is the portable
// pattern across 8.0.x.
func (w *SchemaWriter) AlterDropCheck(ctx context.Context, table *ir.Table, checks []*ir.CheckConstraint) error {
	if len(checks) == 0 {
		return nil
	}
	for _, chk := range checks {
		if chk == nil || chk.Name == "" {
			return errors.New("mysql: alter drop check: constraint name is required")
		}
		exists, err := mysqlCheckConstraintExists(ctx, w.db, w.schema, table.Name, chk.Name)
		if err != nil {
			return fmt.Errorf("alter drop check: probe %q: %w", chk.Name, err)
		}
		if !exists {
			continue
		}
		stmt := fmt.Sprintf("ALTER TABLE %s DROP CHECK %s",
			quoteIdent(table.Name), quoteIdent(chk.Name))
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("alter drop check %q on %s: %w", chk.Name, table.Name, err)
		}
	}
	return nil
}

// AlterModifyCheck implements [ir.ShapeDeltaApplier] for MySQL
// (ADR-0065). Emits DROP + ADD; pre-flight refuse-loudly check on
// the NEW expression fires BEFORE the DROP so an untranslatable
// cross-dialect Expr doesn't leave the target without either
// constraint.
func (w *SchemaWriter) AlterModifyCheck(ctx context.Context, table *ir.Table, oldConstraint, newConstraint *ir.CheckConstraint) error {
	if oldConstraint == nil || newConstraint == nil {
		return errors.New("mysql: alter modify check: oldConstraint and newConstraint must be non-nil")
	}
	if oldConstraint.Name == "" || newConstraint.Name == "" {
		return errors.New("mysql: alter modify check: constraint names must be non-empty")
	}
	exprText := translateCheckExpr(newConstraint)
	if err := refuseUntranslatedCheckExprMySQL(newConstraint, exprText); err != nil {
		return fmt.Errorf("alter modify check %q on %s: %w", newConstraint.Name, table.Name, err)
	}
	if err := w.AlterDropCheck(ctx, table, []*ir.CheckConstraint{oldConstraint}); err != nil {
		return err
	}
	return w.AlterAddCheck(ctx, table, []*ir.CheckConstraint{newConstraint})
}

// mysqlCheckConstraintExists reports whether a CHECK constraint
// with the given name exists on schema.tableName. The lease
// primitive is per-table, so the existence check must mirror that
// scope; joining CHECK_CONSTRAINTS with TABLE_CONSTRAINTS gives the
// table-scoped filter MySQL's CHECK_CONSTRAINTS table doesn't
// expose directly.
func mysqlCheckConstraintExists(ctx context.Context, db *sql.DB, schema, tableName, constraintName string) (bool, error) {
	const q = `SELECT EXISTS(SELECT 1 FROM information_schema.TABLE_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = ? AND TABLE_NAME = ?
		  AND CONSTRAINT_TYPE = 'CHECK' AND CONSTRAINT_NAME = ?)`
	var present bool
	if err := db.QueryRowContext(ctx, q, schema, tableName, constraintName).Scan(&present); err != nil {
		return false, fmt.Errorf("information_schema.TABLE_CONSTRAINTS probe: %w", err)
	}
	return present, nil
}

// untranslatedPGToMySQLTokens lists tokens that survive
// translateExprForMySQL passes unchanged when the source CHECK
// expression uses PG-only constructs. A post-translation expression
// containing any of these is almost-certainly going to fail at the
// MySQL parser; refusing pre-emit gives a cleaner operator-visible
// error and avoids leaving a partially-modified constraint.
//
// The list is conservative — covers the common PG-only spellings.
//
// The four POSIX-regex operators (`~`, `~*`, `!~`, `!~*`) have no
// MySQL equivalent in a CHECK predicate (MySQL spells it `REGEXP` /
// `RLIKE`), so a cross-dialect CHECK carrying any of them is
// untranslatable. Bare `~` is listed last and subsumes the other
// three as a substring, but all four are spelled out so the intent
// reads clearly (Bug 77: v0.85.0 shipped only `~*`, so a plain
// `col ~ '...'` regex CHECK reached MySQL verbatim and failed with an
// opaque Error 1064 instead of this refuse-loudly).
var untranslatedPGToMySQLTokens = []string{
	" ->> ",
	"->>'",
	" -> ",
	"->'",
	"::", // PG cast syntax (e.g. "x::text")
	" similar to ",
	"!~*", // PG negated case-insensitive regex
	"~*",  // PG case-insensitive regex
	"!~",  // PG negated regex
	"~",   // PG regex (case-sensitive); also catches the three above
}

// refuseUntranslatedCheckExprMySQL returns a refuse-loudly error
// when either the SOURCE CHECK Expr text or the POST-translation
// text contains a well-known PG-only token that MySQL cannot
// reliably execute. Only fires on cross-dialect cases
// (chk.ExprDialect != "" and != "mysql"). Mirrors the PG-side
// fix — checking input catches the case where the translator
// produces a MySQL idiom with subtle semantic differences (and
// would otherwise let SQL through that fails at runtime with a
// generic MySQL parse error rather than sluice's actionable
// refuse-loudly + --expr-override hint).
func refuseUntranslatedCheckExprMySQL(chk *ir.CheckConstraint, exprText string) error {
	if chk == nil || chk.ExprDialect == "" || chk.ExprDialect == dialectName {
		return nil
	}
	lowerInput := strings.ToLower(chk.Expr)
	lowerOutput := strings.ToLower(exprText)
	for _, tok := range untranslatedPGToMySQLTokens {
		if strings.Contains(lowerInput, tok) || strings.Contains(lowerOutput, tok) {
			return fmt.Errorf(
				"refuse loudly: CHECK constraint %q expression carries untranslated "+
					"%s-dialect token %q in cross-engine apply (source expr: %q, post-translation expr: %q). "+
					"Operator recovery: drop the CHECK on the source before migrating "+
					"(ALTER TABLE ... DROP CONSTRAINT %s), then re-create an equivalent "+
					"MySQL CHECK on the target post-migration using MySQL syntax "+
					"(e.g. REGEXP instead of the PG ~ operator). sluice does not "+
					"auto-translate dialect-specific CHECK predicates",
				chk.Name, chk.ExprDialect, tok, chk.Expr, exprText, chk.Name,
			)
		}
	}
	return nil
}
