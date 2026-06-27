// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Compile-time proof the SQLite write side satisfies the orchestrator's
// target contracts (ADR-0134).
var (
	_ ir.SchemaWriter   = (*SchemaWriter)(nil)
	_ ir.RowWriter      = (*RowWriter)(nil)
	_ ir.TableTruncator = (*RowWriter)(nil)
	_ ir.TableDropper   = (*RowWriter)(nil)
)

// SchemaWriter applies an IR [ir.Schema] to a SQLite target file
// (ADR-0134). It implements [ir.SchemaWriter], but SQLite's inability to
// `ALTER TABLE ADD` a FOREIGN KEY or CHECK after creation re-maps the
// three phases:
//
//   - CreateTablesWithoutConstraints emits the FULL inline CREATE TABLE —
//     columns, NOT NULL, DEFAULT, generated columns, PRIMARY KEY, CHECK,
//     and FOREIGN KEY — because SQLite can't add the constraint-y parts
//     later. (The method named "WithoutConstraints" deliberately INCLUDES
//     them for SQLite; see ADR-0134 §3.)
//   - CreateIndexes creates secondary indexes (CREATE INDEX works post-hoc).
//   - CreateConstraints is a FK-integrity VERIFICATION pass: the FKs are
//     already inline, so it runs `PRAGMA foreign_key_check` and refuses
//     loudly if the loaded data violates any FK.
//
// The writer holds an open *sql.DB (opened writable, FK-enforcement off
// for the unordered bulk-copy); callers Close it to release the pool.
type SchemaWriter struct {
	db   *sql.DB
	path string
}

// Close releases the underlying connection pool.
func (w *SchemaWriter) Close() error {
	if w.db == nil {
		return nil
	}
	return w.db.Close()
}

// CreateTablesWithoutConstraints emits the full inline CREATE TABLE for
// every table in s (PK / UNIQUE / CHECK / FK all inline — SQLite cannot
// add them later, ADR-0134 §3). PG-only constructs SQLite cannot represent
// (EXCLUDE constraints, row-level security) are refused LOUDLY here rather
// than silently dropped.
func (w *SchemaWriter) CreateTablesWithoutConstraints(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("sqlite: CreateTablesWithoutConstraints: schema is nil")
	}
	for _, table := range s.Tables {
		if err := refuseUnrepresentableTableFeatures(table); err != nil {
			return err
		}
		stmt, err := emitTableDef(table)
		if err != nil {
			return err
		}
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqlite: create table %q: %w", table.Name, err)
		}
	}
	return nil
}

// refuseUnrepresentableTableFeatures loud-refuses table-level features
// SQLite has no equivalent for (the loud-failure tenet — never a silent
// drop). EXCLUDE constraints and row-level security are PG-only.
func refuseUnrepresentableTableFeatures(table *ir.Table) error {
	if table == nil {
		return errors.New("sqlite: nil table in schema")
	}
	if len(table.ExcludeConstraints) > 0 {
		return fmt.Errorf(
			"sqlite: table %q has %d EXCLUDE constraint(s); SQLite has no EXCLUDE constraint and "+
				"cannot represent them — refusing rather than silently dropping",
			table.Name, len(table.ExcludeConstraints),
		)
	}
	if table.RLSEnabled || len(table.Policies) > 0 {
		return fmt.Errorf(
			"sqlite: table %q uses row-level security (%d policies); SQLite has no RLS and cannot "+
				"enforce them — refusing rather than silently dropping the access policy",
			table.Name, len(table.Policies),
		)
	}
	return nil
}

// CreateIndexes creates every non-PK secondary index across the schema
// (CREATE INDEX is valid post-hoc on SQLite). Partial/expression indexes
// carry their predicate/expression verbatim. Idempotent via IF NOT EXISTS.
func (w *SchemaWriter) CreateIndexes(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("sqlite: CreateIndexes: schema is nil")
	}
	for _, table := range s.Tables {
		for _, idx := range table.Indexes {
			stmt, err := emitCreateIndex(table.Name, idx)
			if err != nil {
				return err
			}
			if _, err := w.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("sqlite: create index %q on %q: %w", idx.Name, table.Name, err)
			}
		}
	}
	return nil
}

// CreateConstraints is the FK-integrity VERIFICATION pass for SQLite: all
// constraints are already inline in CREATE TABLE (ADR-0134 §3), so there
// is nothing to add. Instead it runs `PRAGMA foreign_key_check` on the
// whole database — the bulk copy ran with FK enforcement OFF so a child
// row could land before its parent — and refuses LOUDLY (naming the
// violating table + rowid + parent) if any FK is actually violated. This
// is the loud-failure surface that replaces PG's validating ADD CONSTRAINT.
func (w *SchemaWriter) CreateConstraints(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("sqlite: CreateConstraints: schema is nil")
	}
	rows, err := w.db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("sqlite: foreign_key_check on %q: %w", w.path, err)
	}
	defer func() { _ = rows.Close() }()

	var violations []string
	for rows.Next() {
		// PRAGMA foreign_key_check yields: table, rowid, referred-table,
		// fkid. rowid is NULL for a WITHOUT ROWID / view child row.
		var (
			child  string
			rowid  sql.NullInt64
			parent string
			fkid   int
		)
		if err := rows.Scan(&child, &rowid, &parent, &fkid); err != nil {
			return fmt.Errorf("sqlite: scan foreign_key_check row: %w", err)
		}
		rid := "?"
		if rowid.Valid {
			rid = fmt.Sprintf("%d", rowid.Int64)
		}
		violations = append(violations,
			fmt.Sprintf("%s(rowid=%s) → %s (fk #%d)", child, rid, parent, fkid))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("sqlite: iterate foreign_key_check: %w", err)
	}
	if len(violations) > 0 {
		return fmt.Errorf(
			"sqlite: %d foreign-key violation(s) after bulk copy into %q — the copied data does not "+
				"satisfy the inline FOREIGN KEY constraints: %s",
			len(violations), w.path, strings.Join(violations, "; "),
		)
	}
	return nil
}

// SyncIdentitySequences is a no-op for SQLite. A single-column INTEGER
// PRIMARY KEY is a rowid alias that auto-continues from max(rowid)+1 on a
// NULL insert, and AUTOINCREMENT's sqlite_sequence is updated by SQLite on
// explicit-value insert — so no post-copy sequence bump is needed (the
// same shape as MySQL InnoDB, verified — ADR-0134 §4).
func (w *SchemaWriter) SyncIdentitySequences(context.Context, *ir.Schema) error {
	return nil
}

// CreateViews emits CREATE VIEW IF NOT EXISTS for every regular view in
// s.Views (verbatim body — a non-portable cross-dialect body fails loudly
// at CREATE VIEW). SQLite has NO materialized views, so a materialized
// view is refused LOUDLY (ADR-0134 §5).
func (w *SchemaWriter) CreateViews(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("sqlite: CreateViews: schema is nil")
	}
	for _, view := range s.Views {
		if view == nil || view.Name == "" {
			continue
		}
		if view.Materialized {
			return fmt.Errorf(
				"sqlite: view %q is a materialized view; SQLite has no materialized-view support and "+
					"cannot represent it — refusing rather than silently creating a plain view",
				view.Name,
			)
		}
		if _, err := w.db.ExecContext(ctx, emitCreateView(view)); err != nil {
			return fmt.Errorf("sqlite: create view %q: %w", view.Name, err)
		}
	}
	return nil
}
