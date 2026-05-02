package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

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
			return fmt.Errorf("mysql: create table %q: %w", table.Name, err)
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
		indexes := append([]*ir.Index(nil), table.Indexes...)
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i].Name < indexes[j].Name
		})
		for _, idx := range indexes {
			stmt, err := emitCreateIndex(table.Name, idx)
			if err != nil {
				return err
			}
			if _, err := w.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("mysql: create index %q on %q: %w", idx.Name, table.Name, err)
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
				return fmt.Errorf("mysql: add foreign key %q on %q: %w", fk.Name, table.Name, err)
			}
		}
	}
	return nil
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
