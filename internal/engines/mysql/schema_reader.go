package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// SchemaReader reads schema metadata from a MySQL database via
// information_schema. It implements [ir.SchemaReader].
//
// The reader holds an open *sql.DB; callers should call Close when
// done to release the connection pool.
type SchemaReader struct {
	db     *sql.DB
	schema string // database name to read
}

// Close releases the underlying connection pool.
func (r *SchemaReader) Close() error {
	if r.db == nil {
		return nil
	}
	return r.db.Close()
}

// ReadSchema queries information_schema and returns a fully-populated
// IR [ir.Schema] for the database the reader is bound to.
//
// The implementation runs a small number of broad queries (one per
// concept: tables, columns, indexes, foreign keys) rather than per-table
// round-trips. This keeps reads fast on large schemas and keeps the
// query surface auditable.
func (r *SchemaReader) ReadSchema(ctx context.Context) (*ir.Schema, error) {
	tables, err := r.readTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("mysql: read tables: %w", err)
	}
	if len(tables) == 0 {
		return &ir.Schema{}, nil
	}

	if err := r.populateColumns(ctx, tables); err != nil {
		return nil, fmt.Errorf("mysql: read columns: %w", err)
	}
	if err := r.populateIndexes(ctx, tables); err != nil {
		return nil, fmt.Errorf("mysql: read indexes: %w", err)
	}
	if err := r.populateForeignKeys(ctx, tables); err != nil {
		return nil, fmt.Errorf("mysql: read foreign keys: %w", err)
	}

	out := &ir.Schema{Tables: make([]*ir.Table, 0, len(tables))}
	for _, name := range sortedKeys(tables) {
		out.Tables = append(out.Tables, tables[name])
	}
	return out, nil
}

// readTables loads the table list and returns a map keyed by table
// name. The map's values are skeleton [ir.Table] structs; populate*
// methods fill them in.
func (r *SchemaReader) readTables(ctx context.Context) (map[string]*ir.Table, error) {
	const q = `
		SELECT table_name, IFNULL(table_comment, '')
		FROM   information_schema.tables
		WHERE  table_schema = ?
		  AND  table_type   = 'BASE TABLE'
		ORDER  BY table_name`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]*ir.Table{}
	for rows.Next() {
		var name, comment string
		if err := rows.Scan(&name, &comment); err != nil {
			return nil, err
		}
		out[name] = &ir.Table{Name: name, Comment: comment}
	}
	return out, rows.Err()
}

// populateColumns fills in Column lists for each table.
func (r *SchemaReader) populateColumns(ctx context.Context, tables map[string]*ir.Table) error {
	const q = `
		SELECT
			table_name,
			column_name,
			ordinal_position,
			column_default,
			is_nullable,
			LOWER(data_type),
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			datetime_precision,
			IFNULL(character_set_name, ''),
			IFNULL(collation_name, ''),
			LOWER(column_type),
			IFNULL(extra, ''),
			IFNULL(column_comment, '')
		FROM   information_schema.columns
		WHERE  table_schema = ?
		ORDER  BY table_name, ordinal_position`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			tableName  string
			colName    string
			ordinal    int
			defaultVal sql.NullString
			isNullable string
			meta       columnMeta
			comment    string
		)
		if err := rows.Scan(
			&tableName,
			&colName,
			&ordinal,
			&defaultVal,
			&isNullable,
			&meta.DataType,
			nullableInt64(&meta.CharMaxLen),
			nullableInt64(&meta.NumPrec),
			nullableInt64(&meta.NumScale),
			nullableInt64(&meta.DTPrec),
			&meta.Charset,
			&meta.Collation,
			&meta.ColumnType,
			&meta.Extra,
			&comment,
		); err != nil {
			return err
		}

		t, ok := tables[tableName]
		if !ok {
			// Column refers to a table we didn't see — likely created
			// between the two queries. Skip; the reader is a snapshot.
			continue
		}

		typ, err := translateType(meta)
		if err != nil {
			return fmt.Errorf("table %q column %q: %w", tableName, colName, err)
		}

		t.Columns = append(t.Columns, &ir.Column{
			Name:     colName,
			Type:     typ,
			Nullable: strings.EqualFold(isNullable, "YES"),
			Default:  translateDefault(defaultVal, meta.Extra),
			Comment:  comment,
		})
	}
	return rows.Err()
}

// populateIndexes fills in Index lists for each table, separating the
// primary key from secondary indexes.
func (r *SchemaReader) populateIndexes(ctx context.Context, tables map[string]*ir.Table) error {
	const q = `
		SELECT
			table_name,
			index_name,
			non_unique,
			LOWER(IFNULL(index_type, '')),
			column_name,
			seq_in_index,
			IFNULL(sub_part, 0),
			IFNULL(collation, '')
		FROM   information_schema.statistics
		WHERE  table_schema = ?
		ORDER  BY table_name, index_name, seq_in_index`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Group rows by (table, index_name).
	type key struct{ table, name string }
	collected := map[key]*ir.Index{}

	for rows.Next() {
		var (
			tableName string
			indexName string
			nonUnique int
			indexType string
			colName   string
			seq       int
			subPart   int64
			collation string
		)
		if err := rows.Scan(
			&tableName, &indexName, &nonUnique, &indexType,
			&colName, &seq, &subPart, &collation,
		); err != nil {
			return err
		}
		if _, ok := tables[tableName]; !ok {
			continue
		}

		k := key{table: tableName, name: indexName}
		idx, ok := collected[k]
		if !ok {
			idx = &ir.Index{
				Name:   indexName,
				Unique: nonUnique == 0,
				Kind:   indexKindFrom(indexType),
			}
			collected[k] = idx
		}
		idx.Columns = append(idx.Columns, ir.IndexColumn{
			Column: colName,
			Desc:   collation == "D",
			Length: int(subPart),
		})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Attach indexes to their tables; PRIMARY becomes Table.PrimaryKey.
	for k, idx := range collected {
		t := tables[k.table]
		if k.name == "PRIMARY" {
			t.PrimaryKey = idx
			continue
		}
		t.Indexes = append(t.Indexes, idx)
	}
	return nil
}

// populateForeignKeys fills in ForeignKey lists for each table.
func (r *SchemaReader) populateForeignKeys(ctx context.Context, tables map[string]*ir.Table) error {
	const q = `
		SELECT
			k.table_name,
			k.constraint_name,
			k.column_name,
			IFNULL(k.referenced_table_schema, ''),
			IFNULL(k.referenced_table_name,   ''),
			IFNULL(k.referenced_column_name,  ''),
			k.ordinal_position,
			IFNULL(rc.update_rule, 'NO ACTION'),
			IFNULL(rc.delete_rule, 'NO ACTION')
		FROM   information_schema.key_column_usage k
		JOIN   information_schema.referential_constraints rc
		  ON   rc.constraint_schema = k.constraint_schema
		 AND   rc.constraint_name   = k.constraint_name
		WHERE  k.table_schema = ?
		  AND  k.referenced_table_name IS NOT NULL
		ORDER  BY k.table_name, k.constraint_name, k.ordinal_position`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return err
	}
	defer rows.Close()

	type key struct{ table, name string }
	collected := map[key]*ir.ForeignKey{}

	for rows.Next() {
		var (
			tableName, name, col, refSchema, refTable, refCol string
			ordinal                                           int
			updateRule, deleteRule                            string
		)
		if err := rows.Scan(
			&tableName, &name, &col,
			&refSchema, &refTable, &refCol,
			&ordinal,
			&updateRule, &deleteRule,
		); err != nil {
			return err
		}
		if _, ok := tables[tableName]; !ok {
			continue
		}
		k := key{table: tableName, name: name}
		fk, ok := collected[k]
		if !ok {
			// MySQL has a flat scope: its `TABLE_SCHEMA` column is a
			// database name, not a namespace. The IR contract on
			// ir.ForeignKey.ReferencedSchema says it must be empty for
			// flat-scope engines, so we deliberately drop refSchema
			// here. Propagating it leaks the source database name
			// into target dialects that *do* have namespaced schemas
			// — e.g. emitting `REFERENCES "source_db"."users"(...)`
			// against a Postgres target where no such schema exists.
			// Cross-database FKs in MySQL are rare and not supported
			// by InnoDB enforcement; if real-world cases appear we
			// can revisit with a typed translation policy.
			_ = refSchema
			fk = &ir.ForeignKey{
				Name:            name,
				ReferencedTable: refTable,
				OnUpdate:        fkActionFrom(updateRule),
				OnDelete:        fkActionFrom(deleteRule),
			}
			collected[k] = fk
		}
		fk.Columns = append(fk.Columns, col)
		fk.ReferencedColumns = append(fk.ReferencedColumns, refCol)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for k, fk := range collected {
		t := tables[k.table]
		t.ForeignKeys = append(t.ForeignKeys, fk)
	}
	return nil
}

// loadTableSchema reads just the column list for a single table from
// information_schema. It is the per-table flavour of populateColumns,
// added so the CDC reader can refresh its schema cache on a single
// table after a DDL event without re-scanning the entire database.
//
// Indexes and foreign keys are not loaded — the CDC dispatcher only
// needs column names and types to decode row events.
func loadTableSchema(ctx context.Context, db *sql.DB, schema, table string) (*tableSchema, error) {
	const q = `
		SELECT
			column_name,
			column_default,
			is_nullable,
			LOWER(data_type),
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			datetime_precision,
			IFNULL(character_set_name, ''),
			IFNULL(collation_name, ''),
			LOWER(column_type),
			IFNULL(extra, ''),
			IFNULL(column_comment, '')
		FROM   information_schema.columns
		WHERE  table_schema = ?
		  AND  table_name   = ?
		ORDER  BY ordinal_position`

	rows, err := db.QueryContext(ctx, q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := &tableSchema{Schema: schema, Name: table}
	for rows.Next() {
		var (
			colName    string
			defaultVal sql.NullString
			isNullable string
			meta       columnMeta
			comment    string
		)
		if err := rows.Scan(
			&colName,
			&defaultVal,
			&isNullable,
			&meta.DataType,
			nullableInt64(&meta.CharMaxLen),
			nullableInt64(&meta.NumPrec),
			nullableInt64(&meta.NumScale),
			nullableInt64(&meta.DTPrec),
			&meta.Charset,
			&meta.Collation,
			&meta.ColumnType,
			&meta.Extra,
			&comment,
		); err != nil {
			return nil, err
		}

		typ, err := translateType(meta)
		if err != nil {
			return nil, fmt.Errorf("table %q column %q: %w", table, colName, err)
		}
		out.Columns = append(out.Columns, &ir.Column{
			Name:     colName,
			Type:     typ,
			Nullable: strings.EqualFold(isNullable, "YES"),
			Default:  translateDefault(defaultVal, meta.Extra),
			Comment:  comment,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out.Columns) == 0 {
		return nil, fmt.Errorf("mysql: table %s.%s has no columns (does it exist?)", schema, table)
	}
	return out, nil
}

// translateDefault converts the (column_default, extra) pair from
// information_schema into an [ir.DefaultValue]. MySQL signals
// expression defaults with the "DEFAULT_GENERATED" token in extra;
// that distinction is preserved in the IR rather than collapsed.
func translateDefault(def sql.NullString, extra string) ir.DefaultValue {
	if !def.Valid {
		return ir.DefaultNone{}
	}
	if strings.Contains(strings.ToUpper(extra), "DEFAULT_GENERATED") {
		return ir.DefaultExpression{Expr: def.String}
	}
	return ir.DefaultLiteral{Value: def.String}
}

// indexKindFrom maps MySQL's index_type string to an IR IndexKind.
func indexKindFrom(s string) ir.IndexKind {
	switch s {
	case "btree":
		return ir.IndexKindBTree
	case "hash":
		return ir.IndexKindHash
	case "fulltext":
		return ir.IndexKindFullText
	case "spatial":
		return ir.IndexKindSpatial
	default:
		return ir.IndexKindUnspecified
	}
}

// fkActionFrom maps a referential_constraints rule string to an IR
// FKAction. Unknown rules become FKActionNoAction (MySQL's default).
func fkActionFrom(s string) ir.FKAction {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "RESTRICT":
		return ir.FKActionRestrict
	case "CASCADE":
		return ir.FKActionCascade
	case "SET NULL":
		return ir.FKActionSetNull
	case "SET DEFAULT":
		return ir.FKActionSetDefault
	default:
		return ir.FKActionNoAction
	}
}

// nullableInt64 returns a Scan target that captures a possibly-NULL
// information_schema integer column into the supplied **int64. After
// Scan, *out is nil for SQL NULL or points at the value otherwise.
func nullableInt64(out **int64) any {
	*out = nil
	return &nullableInt64Scanner{out: out}
}

type nullableInt64Scanner struct{ out **int64 }

func (s *nullableInt64Scanner) Scan(src any) error {
	if src == nil {
		*s.out = nil
		return nil
	}
	var n sql.NullInt64
	if err := n.Scan(src); err != nil {
		return err
	}
	if !n.Valid {
		*s.out = nil
		return nil
	}
	v := n.Int64
	*s.out = &v
	return nil
}

// sortedKeys returns the keys of m in lexicographic order. Used to
// produce deterministic output for golden-file tests.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// Tiny inline sort to avoid a sort.Strings dependency in this file.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
