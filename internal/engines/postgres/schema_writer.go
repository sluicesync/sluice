package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

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
	// hasPostGIS is set at engine open time via detectPostGIS. When
	// true, ir.Geometry columns emit as `geometry(<subtype>, <srid>)`;
	// when false, they're rejected with a clear "install postgis"
	// error rather than silently coerced.
	hasPostGIS bool
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
	opts := emitOpts{HasPostGIS: w.hasPostGIS}
	for _, table := range orderedTables(s) {
		stmt, err := emitTableDef(w.schema, table, opts)
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

// SyncIdentitySequences advances every identity column's sequence
// past the maximum value present in the target table. Runs after
// bulk-copy completes so user-initiated INSERTs against IDs that
// would have been auto-generated don't collide with bulk-copied IDs.
//
// Implementation is two queries per identity column: a MAX read,
// followed by a conditional setval. The split (vs. a single SQL
// statement) avoids the Postgres edge case where setval(seq, 0)
// errors on a sequence with the default minvalue=1; an empty
// target table simply skips the setval and leaves the sequence at
// its default.
func (w *SchemaWriter) SyncIdentitySequences(ctx context.Context, s *ir.Schema) error {
	if s == nil {
		return errors.New("postgres: SyncIdentitySequences: schema is nil")
	}
	for _, table := range orderedTables(s) {
		for _, col := range table.Columns {
			intT, isInt := col.Type.(ir.Integer)
			if !isInt || !intT.AutoIncrement {
				continue
			}
			if err := w.syncOneIdentity(ctx, table, col.Name); err != nil {
				return fmt.Errorf("postgres: sync identity %s.%s.%s: %w",
					w.schema, table.Name, col.Name, err)
			}
		}
	}
	return nil
}

// syncOneIdentity reads MAX(<col>) on the target table; if non-NULL,
// runs setval on the column's underlying sequence so that next
// nextval returns MAX+1.
func (w *SchemaWriter) syncOneIdentity(ctx context.Context, table *ir.Table, column string) error {
	qualified := quoteIdent(w.schema) + "." + quoteIdent(table.Name)

	// Step 1: read MAX(<col>). NULL on empty table.
	maxQuery := fmt.Sprintf("SELECT MAX(%s) FROM %s", quoteIdent(column), qualified)
	var maxVal sql.NullInt64
	if err := w.db.QueryRowContext(ctx, maxQuery).Scan(&maxVal); err != nil {
		return fmt.Errorf("read max: %w", err)
	}
	if !maxVal.Valid {
		// Empty table — sequence's default is already correct.
		return nil
	}

	// Step 2: setval(seq, max). The third arg defaults to true →
	// next nextval returns max+1. The WHERE clause guards against
	// pg_get_serial_sequence returning NULL (defensive; should not
	// fire for any standard IDENTITY column).
	const setvalQuery = `
		SELECT setval(pg_get_serial_sequence($1, $2), $3, true)
		WHERE pg_get_serial_sequence($1, $2) IS NOT NULL`
	tableArg := w.schema + "." + table.Name
	if _, err := w.db.ExecContext(ctx, setvalQuery, tableArg, column, maxVal.Int64); err != nil {
		return fmt.Errorf("setval: %w", err)
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

// PreviewDDL returns every statement [SchemaWriter] would execute on
// s, in execution order, without touching the target database. Used by
// `sluice schema preview` (ADR-0024) to surface the target schema for
// operator inspection before any migration runs. The CREATE TABLE
// statements have their trailing semicolons stripped — the preview
// formatter re-adds them for human readability.
//
// Implementing this on the same struct as [SchemaWriter] keeps the
// schema-emit logic in one place: PreviewDDL routes through the same
// emitTableDef / emitCreateIndex / emitAddForeignKey helpers the
// execute path uses. The trade-off is that PreviewDDL needs the
// hasPostGIS flag — set by the engine's OpenSchemaWriter at construct
// time — so geometry columns render with the right
// `geometry(<subtype>, <srid>)` form. Operators previewing a target
// without PostGIS still see the same loud rejection the actual
// schema-write phase would raise.
func (w *SchemaWriter) PreviewDDL(_ context.Context, s *ir.Schema) ([]ir.DDLStatement, error) {
	if s == nil {
		return nil, errors.New("postgres: PreviewDDL: schema is nil")
	}

	out := make([]ir.DDLStatement, 0, len(s.Tables)*2)
	opts := emitOpts{HasPostGIS: w.hasPostGIS}

	// Phase 1a: enum types, in deterministic table+column order.
	for _, table := range orderedTables(s) {
		for _, col := range table.Columns {
			enum, ok := col.Type.(ir.Enum)
			if !ok {
				continue
			}
			out = append(out, ir.DDLStatement{
				Table: table.Name,
				Kind:  "CREATE TYPE",
				SQL:   trimTrailingSemicolon(emitCreateEnumType(w.schema, table.Name, col.Name, enum.Values)),
			})
		}
	}

	// Phase 1b: tables.
	for _, table := range orderedTables(s) {
		stmt, err := emitTableDef(w.schema, table, opts)
		if err != nil {
			return nil, err
		}
		out = append(out, ir.DDLStatement{
			Table: table.Name,
			Kind:  "CREATE TABLE",
			SQL:   trimTrailingSemicolon(stmt),
		})
	}

	// Phase 2: indexes.
	for _, table := range orderedTables(s) {
		indexes := append([]*ir.Index(nil), table.Indexes...)
		sort.Slice(indexes, func(i, j int) bool {
			return indexes[i].Name < indexes[j].Name
		})
		for _, idx := range indexes {
			stmt, err := emitCreateIndex(w.schema, table.Name, idx)
			if err != nil {
				return nil, err
			}
			out = append(out, ir.DDLStatement{
				Table: table.Name,
				Kind:  "CREATE INDEX",
				SQL:   trimTrailingSemicolon(stmt),
			})
		}
	}

	// Phase 3: foreign-key constraints.
	for _, table := range orderedTables(s) {
		fks := append([]*ir.ForeignKey(nil), table.ForeignKeys...)
		sort.Slice(fks, func(i, j int) bool {
			return fks[i].Name < fks[j].Name
		})
		for _, fk := range fks {
			stmt, err := emitAddForeignKey(w.schema, table.Name, fk)
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

	return out, nil
}

// trimTrailingSemicolon removes a single trailing ';' from s, if
// present. Postgres DDL emitters terminate every statement with a
// semicolon for executability; preview output adds them back at format
// time so the wire shape is decoupled from the rendering shape.
func trimTrailingSemicolon(s string) string {
	return strings.TrimRight(s, ";")
}
