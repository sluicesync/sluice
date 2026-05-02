package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/orware/sluice/internal/ir"
)

// SchemaWriter applies an IR Schema to a PostgreSQL database in three
// phases (per the [ir.SchemaWriter] contract). Phase 1 is broken into
// two sub-steps because Postgres requires custom enum types to exist
// before tables that reference them:
//
//	phase 1a: CREATE TYPE ... AS ENUM for every enum column
//	phase 1b: CREATE TABLE for every table with columns + PK only
//
//	(bulk-load step happens here, outside the SchemaWriter)
//
//	phase 2:  CREATE INDEX for every non-PK index
//	phase 3:  ALTER TABLE ADD CONSTRAINT for every foreign key
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

// CreateTablesWithoutConstraints emits CREATE TYPE statements for any
// enum columns, then CREATE TABLE for every table in s, in
// deterministic (alphabetical) order.
func (w *SchemaWriter) CreateTablesWithoutConstraints(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: CreateTablesWithoutConstraints: schema is nil")
	}

	// Phase 1a: enum types. We walk all columns and emit one
	// CREATE TYPE per enum column. Two columns sharing values across
	// tables get separate types — same naming convention as
	// emitColumnDef expects.
	for _, table := range orderedTables(s) {
		for _, col := range table.Columns {
			enum, ok := col.Type.(ir.Enum)
			if !ok {
				continue
			}
			stmt := emitCreateEnumType(w.schema, table.Name, col.Name, enum.Values)
			if _, err := w.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("postgres: create enum type for %s.%s: %w", table.Name, col.Name, err)
			}
		}
	}

	// Phase 1b: tables.
	for _, table := range orderedTables(s) {
		stmt, err := emitTableDef(w.schema, table)
		if err != nil {
			return err
		}
		if _, err := w.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("postgres: create table %q: %w", table.Name, err)
		}
	}
	return nil
}

// CreateIndexes adds every non-PK index across the schema.
func (w *SchemaWriter) CreateIndexes(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: CreateIndexes: schema is nil")
	}
	for _, table := range orderedTables(s) {
		indexes := append([]*ir.Index(nil), table.Indexes...)
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i].Name < indexes[j].Name
		})
		for _, idx := range indexes {
			stmt, err := emitCreateIndex(w.schema, table.Name, idx)
			if err != nil {
				return err
			}
			if _, err := w.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("postgres: create index %q on %q: %w", idx.Name, table.Name, err)
			}
		}
	}
	return nil
}

// CreateConstraints adds every foreign-key constraint across the
// schema. All referenced tables must already exist.
func (w *SchemaWriter) CreateConstraints(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: CreateConstraints: schema is nil")
	}
	for _, table := range orderedTables(s) {
		fks := append([]*ir.ForeignKey(nil), table.ForeignKeys...)
		sort.Slice(fks, func(i, j int) bool {
			return fks[i].Name < fks[j].Name
		})
		for _, fk := range fks {
			stmt, err := emitAddForeignKey(w.schema, table.Name, fk)
			if err != nil {
				return err
			}
			if _, err := w.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("postgres: add foreign key %q on %q: %w", fk.Name, table.Name, err)
			}
		}
	}
	return nil
}

// orderedTables returns s.Tables sorted alphabetically by name. The
// returned slice is independent of s.Tables.
func orderedTables(s *ir.Schema) []*ir.Table {
	out := append([]*ir.Table(nil), s.Tables...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}
