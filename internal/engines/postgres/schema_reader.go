package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// SchemaReader reads schema metadata from a single PostgreSQL schema
// (namespace) via pg_catalog and information_schema. It implements
// [ir.SchemaReader].
//
// Scope:
//   - Single schema (the one specified in the DSN, default "public").
//     Multi-schema reads are a future extension.
//   - Tables, columns, primary key, secondary indexes, foreign keys.
//   - Enum types resolved to their value list.
//   - Single-dimension arrays of built-in element types.
//
// Out of scope (for now):
//   - Composite, range, and domain types.
//   - Comments on tables and columns.
//   - Geometry / PostGIS types.
//
// Each of the above can be added incrementally without changing the
// reader's overall shape.
type SchemaReader struct {
	db     *sql.DB
	schema string
}

// Close releases the underlying connection pool.
func (r *SchemaReader) Close() error {
	if r.db == nil {
		return nil
	}
	return r.db.Close()
}

// ReadSchema queries pg_catalog and information_schema and returns a
// fully populated IR Schema for the database/schema the reader is
// bound to.
//
// The implementation issues a small number of broad queries (one per
// concept: tables, columns, indexes, foreign keys, plus enum/attmap
// auxiliaries) rather than per-table round-trips.
func (r *SchemaReader) ReadSchema(ctx context.Context) (*ir.Schema, error) {
	tables, err := r.readTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: read tables: %w", err)
	}
	if len(tables) == 0 {
		return &ir.Schema{}, nil
	}

	enumValues, err := r.readEnumValues(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: read enum values: %w", err)
	}

	// PostGIS's geometry_columns view holds per-column subtype +
	// SRID that information_schema flattens away. Lookup is
	// best-effort — the view exists only when PostGIS is installed,
	// and a schema with no geometry columns has no rows there.
	// Either way, the translator degrades gracefully via
	// GeometryUnspecified + SRID=0 when no info shows up.
	geomInfo, err := r.readGeometryColumnInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: read geometry columns: %w", err)
	}

	if err := r.populateColumns(ctx, tables, enumValues, geomInfo); err != nil {
		return nil, fmt.Errorf("postgres: read columns: %w", err)
	}
	if err := r.populateIndexes(ctx, tables); err != nil {
		return nil, fmt.Errorf("postgres: read indexes: %w", err)
	}
	if err := r.populateForeignKeys(ctx, tables); err != nil {
		return nil, fmt.Errorf("postgres: read foreign keys: %w", err)
	}

	out := &ir.Schema{Tables: make([]*ir.Table, 0, len(tables))}
	for _, name := range sortedKeys(tables) {
		out.Tables = append(out.Tables, tables[name])
	}
	return out, nil
}

// readTables loads the table list for the bound schema.
//
// sluice's own bookkeeping tables (sluice_cdc_state from continuous
// sync, sluice_migrate_state from resumable migrations) are excluded
// — they're persisted on the target as a side effect of running
// sluice itself, not user data, and including them would surface as
// "your migration has an extra table" surprises in cross-engine
// re-migrations.
func (r *SchemaReader) readTables(ctx context.Context) (map[string]*ir.Table, error) {
	const q = `
		SELECT table_name
		FROM   information_schema.tables
		WHERE  table_schema = $1
		  AND  table_type   = 'BASE TABLE'
		  AND  table_name NOT IN ('sluice_cdc_state', 'sluice_migrate_state')
		ORDER  BY table_name`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := map[string]*ir.Table{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[name] = &ir.Table{Schema: r.schema, Name: name}
	}
	return out, rows.Err()
}

// readEnumValues fetches every enum type in the bound schema along
// with its ordered value list. Returned as a map keyed by enum type
// name (the udt_name in information_schema.columns).
func (r *SchemaReader) readEnumValues(ctx context.Context) (map[string][]string, error) {
	const q = `
		SELECT t.typname, e.enumlabel
		FROM   pg_enum e
		JOIN   pg_type t      ON t.oid = e.enumtypid
		JOIN   pg_namespace n ON n.oid = t.typnamespace
		WHERE  n.nspname = $1
		ORDER  BY t.typname, e.enumsortorder`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := map[string][]string{}
	for rows.Next() {
		var typname, label string
		if err := rows.Scan(&typname, &label); err != nil {
			return nil, err
		}
		out[typname] = append(out[typname], label)
	}
	return out, rows.Err()
}

// readGeometryColumnInfo loads per-column PostGIS subtype + SRID
// metadata from the geometry_columns view. The view is created by
// the PostGIS extension; when PostGIS isn't installed, the SELECT
// raises a "relation does not exist" error which we convert to an
// empty map (no geometry info available — translator falls back to
// the GeometryUnspecified+SRID=0 path).
//
// Map key shape: "<table>.<column>". One key per geometry-typed
// column.
func (r *SchemaReader) readGeometryColumnInfo(ctx context.Context) (map[string]geometryColumnInfo, error) {
	const q = `
		SELECT f_table_name, f_geometry_column, type, srid
		FROM   geometry_columns
		WHERE  f_table_schema = $1`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		// PostGIS not installed → relation doesn't exist. Treat as
		// "no geometry info" rather than escalating; the translator
		// has a fallback path. We do this by string-matching the
		// driver's error message because pgx's structured error
		// types vary across versions.
		if isUndefinedRelationErr(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := map[string]geometryColumnInfo{}
	for rows.Next() {
		var (
			tableName, columnName string
			subtype               string
			srid                  int64
		)
		if err := rows.Scan(&tableName, &columnName, &subtype, &srid); err != nil {
			return nil, err
		}
		out[tableName+"."+columnName] = geometryColumnInfo{
			Subtype: subtype,
			SRID:    int(srid),
		}
	}
	return out, rows.Err()
}

// isUndefinedRelationErr returns true when err looks like Postgres's
// "relation X does not exist" / SQLSTATE 42P01. The schema reader's
// PostGIS lookup uses this to degrade gracefully when the extension
// isn't installed.
func isUndefinedRelationErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// Postgres surfaces this through pgx as a string starting with
	// "ERROR: relation \"...\" does not exist (SQLSTATE 42P01)".
	return strings.Contains(msg, "does not exist") &&
		(strings.Contains(msg, "geometry_columns") || strings.Contains(msg, "42P01"))
}

// populateColumns fills in Column lists for each table.
func (r *SchemaReader) populateColumns(ctx context.Context, tables map[string]*ir.Table, enumValues map[string][]string, geomInfo map[string]geometryColumnInfo) error {
	const q = `
		SELECT
			table_name,
			column_name,
			ordinal_position,
			column_default,
			is_nullable,
			LOWER(data_type),
			udt_name,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			datetime_precision,
			is_identity,
			COALESCE(is_generated, 'NEVER'),
			COALESCE(generation_expression, '')
		FROM   information_schema.columns
		WHERE  table_schema = $1
		ORDER  BY table_name, ordinal_position`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var (
			tableName, colName   string
			ordinal              int
			columnDefault        sql.NullString
			isNullable, dataType string
			udtName              string
			charMaxLen, numPrec  sql.NullInt64
			numScale, dtPrec     sql.NullInt64
			isIdentity           string
			isGenerated, genExpr string
		)
		if err := rows.Scan(
			&tableName, &colName, &ordinal,
			&columnDefault, &isNullable,
			&dataType, &udtName,
			&charMaxLen, &numPrec, &numScale, &dtPrec,
			&isIdentity,
			&isGenerated, &genExpr,
		); err != nil {
			return err
		}

		t, ok := tables[tableName]
		if !ok {
			continue
		}

		meta := columnMeta{
			DataType:        dataType,
			UDTName:         udtName,
			CharMaxLen:      nullInt64ToPtr(charMaxLen),
			NumPrec:         nullInt64ToPtr(numPrec),
			NumScale:        nullInt64ToPtr(numScale),
			DTPrec:          nullInt64ToPtr(dtPrec),
			IsAutoIncrement: isAutoIncrement(isIdentity, columnDefault),
		}

		// Resolve enum values for USER-DEFINED columns.
		if dataType == "user-defined" || dataType == "USER-DEFINED" {
			if values, ok := enumValues[udtName]; ok {
				meta.EnumValues = values
			}
			// PostGIS geometry: look up subtype + SRID from the
			// per-column info we read out of geometry_columns. A
			// missing entry is fine — the translator handles
			// GeometryInfo=nil by emitting GeometryUnspecified.
			if udtName == "geometry" {
				if info, ok := geomInfo[tableName+"."+colName]; ok {
					info := info
					meta.GeometryInfo = &info
				}
			}
		}

		// Resolve element type for arrays. For now, only built-in
		// element types are supported (the most common case).
		if dataType == "array" || dataType == "ARRAY" {
			elemDataType, ok := arrayElementDataType(udtName)
			if !ok {
				return fmt.Errorf("postgres: array column %s.%s has unsupported element type %q",
					tableName, colName, udtName)
			}
			meta.ArrayElement = &columnMeta{
				DataType: elemDataType,
				UDTName:  strings.TrimPrefix(udtName, "_"),
			}
		}

		typ, err := translateType(meta)
		if err != nil {
			return fmt.Errorf("table %q column %q: %w", tableName, colName, err)
		}

		col := &ir.Column{
			Name:     colName,
			Type:     typ,
			Nullable: strings.EqualFold(isNullable, "YES"),
			Default:  translateDefault(columnDefault, meta.IsAutoIncrement),
		}
		// Postgres only supports STORED generated columns today;
		// is_generated = 'ALWAYS' implies STORED. The expression
		// passes through verbatim — translation policy is "loud
		// failure beats silent corruption", so non-portable
		// expressions surface as a target rejection at apply time
		// rather than a guess at translation.
		if strings.EqualFold(isGenerated, "ALWAYS") && genExpr != "" {
			col.GeneratedExpr = genExpr
			col.GeneratedStored = true
		}
		t.Columns = append(t.Columns, col)
	}
	return rows.Err()
}

// populateIndexes fills in Index lists. PRIMARY indexes go to the
// table's PrimaryKey field; everything else goes into Indexes.
//
// The query unnests pg_index.indkey to produce one row per (index,
// column, position), which the loop groups into IR Index values.
func (r *SchemaReader) populateIndexes(ctx context.Context, tables map[string]*ir.Table) error {
	const q = `
		SELECT
			cl.relname AS table_name,
			i.relname  AS index_name,
			am.amname  AS method,
			ix.indisunique,
			ix.indisprimary,
			COALESCE(a.attname, ''),
			u.ord
		FROM   pg_index ix
		JOIN   pg_class      cl ON cl.oid = ix.indrelid
		JOIN   pg_class      i  ON i.oid  = ix.indexrelid
		JOIN   pg_am         am ON am.oid = i.relam
		JOIN   pg_namespace  n  ON n.oid  = cl.relnamespace
		JOIN   LATERAL unnest(ix.indkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE
		LEFT JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = u.attnum
		WHERE  n.nspname = $1
		  AND  cl.relkind = 'r'
		ORDER  BY cl.relname, i.relname, u.ord`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	type key struct{ table, name string }
	collected := map[key]*ir.Index{}
	primary := map[string]string{} // table → primary index name

	for rows.Next() {
		var (
			tableName, indexName, method, colName string
			isUnique, isPrimary                   bool
			ord                                   int
		)
		if err := rows.Scan(
			&tableName, &indexName, &method,
			&isUnique, &isPrimary,
			&colName, &ord,
		); err != nil {
			return err
		}
		if _, ok := tables[tableName]; !ok {
			continue
		}
		// Skip expression-only entries (no underlying column).
		if colName == "" {
			continue
		}

		k := key{table: tableName, name: indexName}
		idx, ok := collected[k]
		if !ok {
			idx = &ir.Index{
				Name:   indexName,
				Unique: isUnique,
				Kind:   indexKindFrom(method),
			}
			collected[k] = idx
			if isPrimary {
				primary[tableName] = indexName
			}
		}
		idx.Columns = append(idx.Columns, ir.IndexColumn{Column: colName})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for k, idx := range collected {
		t := tables[k.table]
		if primary[k.table] == idx.Name {
			t.PrimaryKey = idx
			continue
		}
		t.Indexes = append(t.Indexes, idx)
	}
	return nil
}

// populateForeignKeys fills in ForeignKey lists. Uses pg_constraint
// directly so we get conkey/confkey pairs without needing the
// ordinal-position bookkeeping that information_schema would require.
func (r *SchemaReader) populateForeignKeys(ctx context.Context, tables map[string]*ir.Table) error {
	const q = `
		SELECT
			con.conname,
			cl.relname  AS table_name,
			pcl.relname AS referenced_table,
			con.confupdtype,
			con.confdeltype,
			fk_col.attname,
			ref_col.attname,
			u.ord
		FROM   pg_constraint con
		JOIN   pg_class cl   ON cl.oid  = con.conrelid
		JOIN   pg_class pcl  ON pcl.oid = con.confrelid
		JOIN   pg_namespace n ON n.oid = cl.relnamespace
		JOIN   LATERAL unnest(con.conkey, con.confkey) WITH ORDINALITY AS u(k_attnum, f_attnum, ord) ON TRUE
		LEFT JOIN pg_attribute fk_col  ON fk_col.attrelid  = con.conrelid  AND fk_col.attnum  = u.k_attnum
		LEFT JOIN pg_attribute ref_col ON ref_col.attrelid = con.confrelid AND ref_col.attnum = u.f_attnum
		WHERE  n.nspname = $1
		  AND  con.contype = 'f'
		ORDER  BY cl.relname, con.conname, u.ord`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	type key struct{ table, name string }
	collected := map[key]*ir.ForeignKey{}

	for rows.Next() {
		var (
			name, tableName, refTable string
			updType, delType          string
			fkCol, refCol             sql.NullString
			ord                       int
		)
		if err := rows.Scan(
			&name, &tableName, &refTable,
			&updType, &delType,
			&fkCol, &refCol, &ord,
		); err != nil {
			return err
		}
		if _, ok := tables[tableName]; !ok {
			continue
		}

		k := key{table: tableName, name: name}
		fk, ok := collected[k]
		if !ok {
			fk = &ir.ForeignKey{
				Name:             name,
				ReferencedSchema: r.schema,
				ReferencedTable:  refTable,
				OnUpdate:         fkActionFromCode(updType),
				OnDelete:         fkActionFromCode(delType),
			}
			collected[k] = fk
		}
		if fkCol.Valid {
			fk.Columns = append(fk.Columns, fkCol.String)
		}
		if refCol.Valid {
			fk.ReferencedColumns = append(fk.ReferencedColumns, refCol.String)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for k, fk := range collected {
		tables[k.table].ForeignKeys = append(tables[k.table].ForeignKeys, fk)
	}
	return nil
}

// isAutoIncrement detects SERIAL, BIGSERIAL, and IDENTITY columns.
//
//   - is_identity = 'YES'    → modern GENERATED ... AS IDENTITY column.
//   - column_default starts with 'nextval(' → legacy SERIAL/BIGSERIAL.
//
// Either is mapped to the IR's Integer.AutoIncrement = true.
func isAutoIncrement(isIdentity string, columnDefault sql.NullString) bool {
	if strings.EqualFold(isIdentity, "YES") {
		return true
	}
	if columnDefault.Valid && strings.HasPrefix(columnDefault.String, "nextval(") {
		return true
	}
	return false
}

// translateDefault converts a Postgres column_default string into an
// IR DefaultValue. Auto-increment columns return DefaultNone — the
// AutoIncrement flag on Integer is the canonical representation.
//
// For non-auto-increment defaults, the implementation strips the
// trailing ::type cast that Postgres adds, then classifies the result:
//
//   - quoted strings    → DefaultLiteral
//   - numeric literals  → DefaultLiteral
//   - boolean literals  → DefaultLiteral
//   - everything else   → DefaultExpression (verbatim)
func translateDefault(d sql.NullString, autoIncrement bool) ir.DefaultValue {
	if !d.Valid || d.String == "" {
		return ir.DefaultNone{}
	}
	if autoIncrement {
		return ir.DefaultNone{}
	}

	s := stripTypeCast(d.String)

	// Quoted string literal: 'value' (with doubled inner quotes).
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		inner := s[1 : len(s)-1]
		inner = strings.ReplaceAll(inner, "''", "'")
		return ir.DefaultLiteral{Value: inner}
	}

	// Numeric literal (loose check — anything that looks like one).
	if looksLikeNumber(s) {
		return ir.DefaultLiteral{Value: s}
	}

	// Boolean literal.
	if s == "true" || s == "false" {
		return ir.DefaultLiteral{Value: s}
	}

	return ir.DefaultExpression{Expr: s}
}

// stripTypeCast removes a trailing ::sometype that Postgres adds to
// column_default values. Returns the input unchanged if no cast is
// present.
//
// The suffix may be a parameterised, qualified, or quoted type name —
// e.g. `timestamp(0) without time zone`, `pg_catalog."text"`, or
// `numeric(20,2)`. The character set the matcher accepts is
// deliberately narrow so something like `(x + y)::int` (where the
// suffix is just `int`) still strips, while `array[1]::int[]` (with
// brackets) does not.
func stripTypeCast(s string) string {
	idx := strings.LastIndex(s, "::")
	if idx < 0 {
		return s
	}
	suffix := s[idx+2:]
	for _, r := range suffix {
		// De-Morgan'd: each clause is "r is NOT in <range>"; the
		// cumulative AND yields "r is none of the allowed chars".
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') &&
			(r < '0' || r > '9') && r != '_' && r != ' ' && r != '"' &&
			r != '.' && r != '(' && r != ')' && r != ',' {
			return s
		}
	}
	return s[:idx]
}

// looksLikeNumber reports whether s parses as a simple integer or
// decimal literal. Whitespace and signs are accepted; expressions
// (anything with parens, operators, etc.) are not.
func looksLikeNumber(s string) bool {
	if s == "" {
		return false
	}
	dotSeen := false
	for i, r := range s {
		switch {
		case r == '-' && i == 0:
		case r == '.':
			if dotSeen {
				return false
			}
			dotSeen = true
		case r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

// indexKindFrom maps a Postgres index access method name to an IR
// IndexKind. Methods we don't currently model (spgist, brin) become
// IndexKindUnspecified.
func indexKindFrom(method string) ir.IndexKind {
	switch method {
	case "btree":
		return ir.IndexKindBTree
	case "hash":
		return ir.IndexKindHash
	case "gin":
		return ir.IndexKindGIN
	case "gist":
		return ir.IndexKindGIST
	default:
		return ir.IndexKindUnspecified
	}
}

// fkActionFromCode maps the single-character codes used in
// pg_constraint.confupdtype / confdeltype to IR FKAction values.
//
//	'a' = NO ACTION (the default)
//	'r' = RESTRICT
//	'c' = CASCADE
//	'n' = SET NULL
//	'd' = SET DEFAULT
func fkActionFromCode(code string) ir.FKAction {
	switch code {
	case "r":
		return ir.FKActionRestrict
	case "c":
		return ir.FKActionCascade
	case "n":
		return ir.FKActionSetNull
	case "d":
		return ir.FKActionSetDefault
	default:
		return ir.FKActionNoAction
	}
}

// arrayElementDataType maps a Postgres array udt_name (which has a
// leading underscore — e.g. "_int4") to the corresponding scalar
// data_type that translateType would produce for the element.
//
// Returns false for udt_names sluice doesn't yet model — typically
// arrays of user-defined types (enums, composites) which would need
// a separate resolution pass.
func arrayElementDataType(udtName string) (string, bool) {
	t, ok := builtinArrayElement[udtName]
	return t, ok
}

var builtinArrayElement = map[string]string{
	"_bool":        "boolean",
	"_int2":        "smallint",
	"_int4":        "integer",
	"_int8":        "bigint",
	"_float4":      "real",
	"_float8":      "double precision",
	"_numeric":     "numeric",
	"_text":        "text",
	"_varchar":     "character varying",
	"_bpchar":      "character",
	"_char":        "character",
	"_bytea":       "bytea",
	"_date":        "date",
	"_time":        "time without time zone",
	"_timetz":      "time with time zone",
	"_timestamp":   "timestamp without time zone",
	"_timestamptz": "timestamp with time zone",
	"_json":        "json",
	"_jsonb":       "jsonb",
	"_uuid":        "uuid",
	"_inet":        "inet",
	"_cidr":        "cidr",
	"_macaddr":     "macaddr",
	"_macaddr8":    "macaddr8",
}

// nullInt64ToPtr converts a sql.NullInt64 to *int64 (nil for NULL,
// pointer to value otherwise). Used to populate columnMeta's
// nullable numeric fields.
func nullInt64ToPtr(n sql.NullInt64) *int64 {
	if !n.Valid {
		return nil
	}
	v := n.Int64
	return &v
}

// sortedKeys returns the keys of m in lexicographic order.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
