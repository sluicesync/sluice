// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// SchemaWriter applies an IR Schema to a MySQL database in three
// phases. The phases are deliberately separate so that the bulk-load
// step (between phases 1 and 2) can run against tables without indexes
// or constraints — typically several times faster than loading into a
// fully-constrained schema.
//
//	phase 1: CreateTablesWithoutConstraints
//	         CREATE TABLE for every table, columns + PRIMARY KEY only.
//
//	(bulk-load step happens here, outside the SchemaWriter)
//
//	phase 2: CreateIndexes
//	         ALTER TABLE ADD INDEX for every non-PRIMARY index.
//
//	phase 3: CreateConstraints
//	         ALTER TABLE ADD CONSTRAINT for every foreign key.
//
// SchemaWriter holds an open *sql.DB; callers should call Close when
// finished to release the connection pool.
type SchemaWriter struct {
	db     *sql.DB
	schema string
}

// Close releases the underlying connection pool.
func (w *SchemaWriter) Close() error {
	if w.db == nil {
		return nil
	}
	return w.db.Close()
}

// CreateTablesWithoutConstraints emits CREATE TABLE for every table
// in s in deterministic (alphabetical) order. The PRIMARY KEY is
// included inline; secondary indexes and foreign keys are deferred to
// later phases.
func (w *SchemaWriter) CreateTablesWithoutConstraints(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return fmt.Errorf("mysql: CreateTablesWithoutConstraints: schema is nil")
	}
	for _, table := range orderedTables(s) {
		stmt, err := emitTableDef(table)
		if err != nil {
			return err
		}
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("mysql: create table %q: %w", table.Name, wrapDDLError(err))
		}
	}
	return nil
}

// CreateIndexes adds every non-PRIMARY index across the schema. Each
// index is added with its own ALTER TABLE statement so a failure on
// one doesn't leave others in an indeterminate state.
//
// The order is (table name, index name) lexicographic — chosen for
// deterministic output more than any operational reason; index
// creation order doesn't affect correctness.
func (w *SchemaWriter) CreateIndexes(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return fmt.Errorf("mysql: CreateIndexes: schema is nil")
	}
	for _, table := range orderedTables(s) {
		// GitHub #25: skip the index that emitTableDef already emitted
		// inline (the supporting key for a non-PK AUTO_INCREMENT
		// column). Re-creating it here would fail with "duplicate
		// index" on the second pass. Tables without the inline pattern
		// see the entire index list as before.
		skipName := ""
		if inline := inlineAutoIncrementIndex(table); inline != nil {
			skipName = inline.Name
		}
		indexes := append([]*ir.Index(nil), table.Indexes...)
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i].Name < indexes[j].Name
		})
		for _, idx := range indexes {
			if idx.Name == skipName {
				continue
			}
			stmt, err := emitCreateIndex(table.Name, idx)
			if err != nil {
				return err
			}
			if _, err := w.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("mysql: create index %q on %q: %w", idx.Name, table.Name, wrapDDLError(err))
			}
		}
	}
	return nil
}

// CreateConstraints adds every foreign-key constraint across the
// schema. All referenced tables must already exist (which they do
// after CreateTablesWithoutConstraints).
//
// The order is (child table name, constraint name) lexicographic.
func (w *SchemaWriter) CreateConstraints(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return fmt.Errorf("mysql: CreateConstraints: schema is nil")
	}
	for _, table := range orderedTables(s) {
		fks := append([]*ir.ForeignKey(nil), table.ForeignKeys...)
		sort.Slice(fks, func(i, j int) bool {
			return fks[i].Name < fks[j].Name
		})
		for _, fk := range fks {
			stmt, err := emitAddForeignKey(table.Name, fk)
			if err != nil {
				return err
			}
			if _, err := w.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("mysql: add foreign key %q on %q: %w", fk.Name, table.Name, wrapDDLError(err))
			}
		}
	}
	return nil
}

// SyncIdentitySequences is a no-op on MySQL. InnoDB's AUTO_INCREMENT
// counter is automatically advanced past explicit-value INSERTs at
// transaction commit time, so a post-bulk-copy sync isn't needed —
// the next user-initiated INSERT picks up where the bulk-copied IDs
// left off without any extra work.
func (w *SchemaWriter) SyncIdentitySequences(_ context.Context, _ *ir.Schema) error {
	return nil
}

// CreateViews emits `CREATE OR REPLACE VIEW` for every view in s in
// declaration order. View definitions are emitted verbatim — Phase 1
// punts on cross-engine view-body translation (see [ir.View]), so a
// PG-source-dialect definition applied to a MySQL target will fail
// loudly at apply time rather than silently corrupt the view body.
//
// The orchestrator is responsible for retrying on view-to-view
// dependency failures (one view referencing another that hasn't been
// created yet); CreateViews itself emits in declared order with no
// dependency analysis. See [pipeline.runViewsPhase].
//
// MySQL's view-body parser stores the SELECT in a re-canonicalised
// form (backtick-quoted identifiers, charset introducers, etc.) — when
// a sluice-managed view is round-tripped, the text the source reader
// returns differs slightly from the operator's original DDL but
// parses to the same logical view. `schema diff` accepts the round-
// tripped form as equal.
func (w *SchemaWriter) CreateViews(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return fmt.Errorf("mysql: CreateViews: schema is nil")
	}
	for _, view := range s.Views {
		if view == nil || view.Name == "" {
			continue
		}
		if view.Materialized {
			// MySQL has no materialized-view concept. The schema
			// reader on PG sources tags matviews; the writer surface
			// here surfaces a clear error rather than silently
			// emitting a regular view with the matview's SELECT (the
			// loud-failure tenet).
			return fmt.Errorf("mysql: CreateViews: view %q is materialized; MySQL has no materialized view support", view.Name)
		}
		stmt := emitCreateView(view)
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("mysql: create view %q: %w", view.Name, wrapDDLError(err))
		}
	}
	return nil
}

// emitCreateView returns the `CREATE OR REPLACE VIEW <name> AS
// <definition>` statement for a regular view. Identifier quoting
// follows the engine's existing conventions; the definition body is
// emitted verbatim per Phase 1's no-translation policy.
func emitCreateView(v *ir.View) string {
	return "CREATE OR REPLACE VIEW " + quoteIdent(v.Name) + " AS " + v.Definition + ";"
}

// orderedTables returns s.Tables sorted alphabetically by name. The
// returned slice is independent of s.Tables; callers may sort or
// modify it without affecting the schema.
func orderedTables(s *ir.Schema) []*ir.Table {
	out := append([]*ir.Table(nil), s.Tables...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

// PreviewDDL returns every statement [SchemaWriter] would execute on
// s, in execution order, without touching the target database. Used by
// `sluice schema preview` (ADR-0024) to surface the target schema for
// operator inspection before any migration runs. The CREATE TABLE
// statements have their trailing semicolons stripped — the preview
// formatter re-adds them for human readability.
func (w *SchemaWriter) PreviewDDL(_ context.Context, s *ir.Schema) ([]ir.DDLStatement, error) {
	if s == nil {
		return nil, fmt.Errorf("mysql: PreviewDDL: schema is nil")
	}

	out := make([]ir.DDLStatement, 0, len(s.Tables)*2)

	// Phase 1: tables.
	for _, table := range orderedTables(s) {
		stmt, err := emitTableDef(table)
		if err != nil {
			return nil, err
		}
		out = append(out, ir.DDLStatement{
			Table: table.Name,
			Kind:  "CREATE TABLE",
			SQL:   trimTrailingSemicolon(stmt),
		})
	}

	// Phase 2: secondary indexes. Skip the inline-emitted
	// AUTO_INCREMENT-supporting index (GitHub #25, same logic as
	// CreateIndexes above).
	for _, table := range orderedTables(s) {
		skipName := ""
		if inline := inlineAutoIncrementIndex(table); inline != nil {
			skipName = inline.Name
		}
		indexes := append([]*ir.Index(nil), table.Indexes...)
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i].Name < indexes[j].Name
		})
		for _, idx := range indexes {
			if idx.Name == skipName {
				continue
			}
			stmt, err := emitCreateIndex(table.Name, idx)
			if err != nil {
				return nil, err
			}
			out = append(out, ir.DDLStatement{
				Table: table.Name,
				Kind:  "ALTER TABLE",
				SQL:   trimTrailingSemicolon(stmt),
			})
		}
	}

	// Phase 3: foreign keys.
	for _, table := range orderedTables(s) {
		fks := append([]*ir.ForeignKey(nil), table.ForeignKeys...)
		sort.Slice(fks, func(i, j int) bool {
			return fks[i].Name < fks[j].Name
		})
		for _, fk := range fks {
			stmt, err := emitAddForeignKey(table.Name, fk)
			if err != nil {
				return nil, err
			}
			out = append(out, ir.DDLStatement{
				Table: table.Name,
				Kind:  "ALTER TABLE",
				SQL:   trimTrailingSemicolon(stmt),
			})
		}
	}

	// Phase 4: views. Emitted last so all referenced base tables
	// exist by the time the view is created.
	for _, view := range s.Views {
		if view == nil || view.Name == "" || view.Materialized {
			continue
		}
		out = append(out, ir.DDLStatement{
			Table: view.Name,
			Kind:  "CREATE VIEW",
			SQL:   trimTrailingSemicolon(emitCreateView(view)),
		})
	}

	return out, nil
}

// trimTrailingSemicolon removes a single trailing ';' from s, if
// present. MySQL DDL emitters terminate every statement with a
// semicolon for executability; preview output adds them back at format
// time so the wire shape is decoupled from the rendering shape.
func trimTrailingSemicolon(s string) string {
	return strings.TrimRight(s, ";")
}

// EmitColumnDef satisfies [ir.ColumnDDLPreviewer]. Returns the MySQL
// column-def fragment (“ `name` TYPE [GENERATED ...] [NOT NULL]
// [DEFAULT ...] [COMMENT '...']“) suitable for inlining into an
// `ALTER TABLE ... ADD COLUMN` suggestion in the schema-diff
// renderer (ADR-0029). MySQL's emitter doesn't need table context
// for any IR type; the table parameter is accepted for interface
// symmetry with the Postgres implementation and silently ignored.
func (w *SchemaWriter) EmitColumnDef(_ context.Context, _ *ir.Table, col *ir.Column) (string, error) {
	return emitColumnDef(col)
}

// AlterAddColumn implements [ir.SchemaDeltaApplier] for MySQL. Used
// by Phase 3 chain restore to apply column-add deltas captured on
// incremental manifests against the target. MySQL gained
// `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` in 8.0.29; we probe
// information_schema.columns first instead, so the call is
// idempotent across re-runs and works on older 8.0.x and 5.7
// servers too.
func (w *SchemaWriter) AlterAddColumn(ctx context.Context, table *ir.Table, cols []*ir.Column) error {
	if len(cols) == 0 {
		return nil
	}
	for _, col := range cols {
		exists, err := columnExists(ctx, w.db, w.schema, table.Name, col.Name)
		if err != nil {
			return fmt.Errorf("alter add column: probe %q: %w", col.Name, err)
		}
		if exists {
			continue
		}
		def, err := emitColumnDef(col)
		if err != nil {
			return fmt.Errorf("alter add column: emit %q: %w", col.Name, err)
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s",
			quoteIdent(table.Name), def)
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("alter add column %q on %s: %w",
				col.Name, table.Name, err)
		}
	}
	return nil
}

// columnExists reports whether table.column is already present in
// the target's information_schema.columns. Used by [AlterAddColumn]
// for idempotency on servers without `ADD COLUMN IF NOT EXISTS`.
func columnExists(ctx context.Context, db *sql.DB, schema, table, col string) (bool, error) {
	const q = `SELECT EXISTS(SELECT 1 FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ? AND column_name = ?)`
	var exists bool
	if err := db.QueryRowContext(ctx, q, schema, table, col).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}
