// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

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

	// enabledExtensions is the set of extension names the operator
	// opted into via `--enable-pg-extension` (ADR-0032). Populated by
	// [EnableExtensions]; nil / empty means "no extension passthrough,
	// existing loud-failure path preserved." Used by populateColumns
	// to route extension-owned column types through pgExtensionCatalog
	// instead of the existing user-defined / loud-failure dispatch.
	enabledExtensions map[string]bool
}

// Close releases the underlying connection pool.
func (r *SchemaReader) Close() error {
	if r.db == nil {
		return nil
	}
	return r.db.Close()
}

// SetSchema implements [ir.SchemaSetter]. Called by the pipeline
// orchestrator when `--target-schema NAME` is set (ADR-0031) so the
// reader queries pg_catalog / information_schema for the named
// schema rather than the DSN's default. Empty input is a no-op
// (preserves the DSN-derived default).
//
// On the source-read path (Migrator / Streamer / Previewer / Differ
// reading the source DSN), the orchestrator deliberately does NOT
// call SetSchema — only target-side reads (Differ's actual-target
// schema read) get the override. The flag is target-namespacing
// for multi-source aggregation, not a source-side schema selector.
func (r *SchemaReader) SetSchema(name string) {
	if name == "" {
		return
	}
	r.schema = name
}

// EnableExtensions implements [ir.ExtensionAware] for PG (ADR-0032).
// Validates each requested extension name against pgExtensionCatalog
// (refusing unknown names with the recognised set listed) and
// preflights presence on the connected database via pg_extension.
// Both checks fire at construction time — the orchestrator threads
// this from the operator's `--enable-pg-extension` allowlist before
// any schema read or write.
//
// Empty / nil extensions is a no-op (today's default: no extension
// passthrough; unrecognised extension types continue to surface as
// loud refusals at type-resolution time).
//
// The presence preflight is mandatory even when no columns of the
// extension's type exist on this side — the check catches the
// operator-typo case where the flag was misspelled or pointed at the
// wrong DSN. Refusing an unused-but-enabled extension is the
// loud-failure-friendly default.
func (r *SchemaReader) EnableExtensions(ctx context.Context, extensions []string) error {
	enabled, err := validateAndPreflightExtensions(ctx, r.db, extensions)
	if err != nil {
		return err
	}
	r.enabledExtensions = enabled
	return nil
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
	views, err := r.readViews(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: read views: %w", err)
	}
	if len(tables) == 0 && len(views) == 0 {
		return &ir.Schema{}, nil
	}

	if len(tables) > 0 {
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
		if err := r.populateCheckConstraints(ctx, tables); err != nil {
			return nil, fmt.Errorf("postgres: read check constraints: %w", err)
		}
	}

	out := &ir.Schema{
		Tables: make([]*ir.Table, 0, len(tables)),
		Views:  views,
	}
	for _, name := range sortedKeys(tables) {
		out.Tables = append(out.Tables, tables[name])
	}
	return out, nil
}

// readViews loads regular views (pg_views) and materialized views
// (pg_matviews) for the bound schema. PG canonicalises the view body
// via pg_get_viewdef when serving these catalog views, so the text we
// get back is reformatted relative to the operator's source — that's
// fine for Phase 1's round-trip-on-same-engine goal but means
// cross-engine `schema diff` against this side will see canonicalised
// text on every line. Documented in [ir.View].
//
// Materialized-view CDC refresh is a Phase 2 future enhancement; the
// Phase 1 writer emits `WITH DATA` so the target's matview is
// populated immediately from the just-loaded target tables.
func (r *SchemaReader) readViews(ctx context.Context) ([]*ir.View, error) {
	const q = `
		SELECT viewname AS name, definition, false AS materialized
		FROM   pg_views
		WHERE  schemaname = $1
		UNION ALL
		SELECT matviewname AS name, definition, true AS materialized
		FROM   pg_matviews
		WHERE  schemaname = $1
		ORDER  BY name`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []*ir.View
	for rows.Next() {
		var (
			name, definition string
			materialized     bool
		)
		if err := rows.Scan(&name, &definition, &materialized); err != nil {
			return nil, err
		}
		out = append(out, &ir.View{
			Schema:            r.schema,
			Name:              name,
			Definition:        definition,
			DefinitionDialect: dialectName,
			Materialized:      materialized,
		})
	}
	return out, rows.Err()
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
//
// Per-column collation is read via a LEFT JOIN to pg_attribute /
// pg_collation rather than information_schema.columns.collation_name
// because the latter only surfaces explicit-on-domain collations and
// misses the column-level setting Postgres stores on
// pg_attribute.attcollation. The collation comparison feeds
// `sluice schema diff` (gated by `--ignore-charset-collation`); on
// the migrate / sync paths the value rides through to the writer's
// emit so an explicit `COLLATE "<name>"` clause can round-trip if the
// target supports the same collation name.
//
// Postgres has no per-column "charset" concept — the database's
// server_encoding is global — so the Charset field on the IR types
// stays empty for PG sources. MySQL writers accept that as "use the
// table / database default."
func (r *SchemaReader) populateColumns(ctx context.Context, tables map[string]*ir.Table, enumValues map[string][]string, geomInfo map[string]geometryColumnInfo) error {
	// COALESCE(a.atttypmod, -1) supplies the per-column typmod for
	// extension-owned types whose modifiers ride on atttypmod
	// (pgvector dimension; future PostGIS subtype/SRID). -1 is the
	// "no typmod" sentinel pgattribute uses; per-extension catalog
	// entries decode it into the IR's Modifiers vector.
	const q = `
		SELECT
			c.table_name,
			c.column_name,
			c.ordinal_position,
			c.column_default,
			c.is_nullable,
			LOWER(c.data_type),
			c.udt_name,
			c.character_maximum_length,
			c.numeric_precision,
			c.numeric_scale,
			c.datetime_precision,
			c.is_identity,
			COALESCE(c.is_generated, 'NEVER'),
			COALESCE(c.generation_expression, ''),
			COALESCE(coll.collname, ''),
			COALESCE(a.atttypmod, -1)
		FROM   information_schema.columns c
		LEFT JOIN pg_class      cl   ON cl.relname    = c.table_name
		                            AND cl.relnamespace = (
		                                  SELECT oid FROM pg_namespace WHERE nspname = c.table_schema)
		LEFT JOIN pg_attribute  a    ON a.attrelid    = cl.oid
		                            AND a.attname     = c.column_name
		                            AND a.attnum      > 0
		                            AND NOT a.attisdropped
		LEFT JOIN pg_collation  coll ON coll.oid       = a.attcollation
		                            AND coll.oid      <> 0
		                            AND coll.collname <> 'default'
		WHERE  c.table_schema = $1
		ORDER  BY c.table_name, c.ordinal_position`

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
			collation            string
			attTypmod            int32
		)
		if err := rows.Scan(
			&tableName, &colName, &ordinal,
			&columnDefault, &isNullable,
			&dataType, &udtName,
			&charMaxLen, &numPrec, &numScale, &dtPrec,
			&isIdentity,
			&isGenerated, &genExpr,
			&collation,
			&attTypmod,
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
			Collation:       collation,
			AttTypmod:       attTypmod,
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

			// ADR-0032: when the operator opted into one or more
			// extensions via `--enable-pg-extension`, route the udt
			// name through pgExtensionCatalog. Recognised → emit as
			// ir.ExtensionType (carrying typmod-derived Modifiers);
			// unrecognised → fall through to the existing dispatch
			// (enum resolution / loud failure on user-defined types
			// the IR doesn't model).
			if ext, name, ok := lookupExtensionForType(udtName, r.enabledExtensions); ok {
				meta.ExtensionName = ext
				meta.ExtensionTypeName = name
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
			col.GeneratedExprDialect = dialectName
		}
		t.Columns = append(t.Columns, col)
	}
	return rows.Err()
}

// populateIndexes fills in Index lists. PRIMARY indexes go to the
// table's PrimaryKey field; everything else goes into Indexes.
//
// The query unnests pg_index.indkey to produce one row per (index,
// column, position), which the loop groups into IR Index values. A
// parallel unnest over pg_index.indclass with `pg_opclass.opcname`
// joined in carries the per-column operator class so extension
// access methods that lack a default opclass (pgvector's hnsw —
// which requires `vector_l2_ops` / `vector_cosine_ops` /
// `vector_ip_ops`) round-trip cleanly under ADR-0032 / Bug 47.
func (r *SchemaReader) populateIndexes(ctx context.Context, tables map[string]*ir.Table) error {
	const q = `
		SELECT
			cl.relname AS table_name,
			i.relname  AS index_name,
			am.amname  AS method,
			ix.indisunique,
			ix.indisprimary,
			COALESCE(a.attname, ''),
			u.ord,
			COALESCE(opc.opcname, '') AS opclass
		FROM   pg_index ix
		JOIN   pg_class      cl ON cl.oid = ix.indrelid
		JOIN   pg_class      i  ON i.oid  = ix.indexrelid
		JOIN   pg_am         am ON am.oid = i.relam
		JOIN   pg_namespace  n  ON n.oid  = cl.relnamespace
		JOIN   LATERAL unnest(ix.indkey)   WITH ORDINALITY AS u(attnum, ord) ON TRUE
		LEFT JOIN LATERAL unnest(ix.indclass) WITH ORDINALITY AS uc(opcoid, ord) ON uc.ord = u.ord
		LEFT JOIN pg_attribute a   ON a.attrelid = ix.indrelid AND a.attnum = u.attnum
		LEFT JOIN pg_opclass   opc ON opc.oid    = uc.opcoid
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
			opclass                               string
		)
		if err := rows.Scan(
			&tableName, &indexName, &method,
			&isUnique, &isPrimary,
			&colName, &ord, &opclass,
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
			kind := indexKindFrom(method)
			idx = &ir.Index{
				Name:   indexName,
				Unique: isUnique,
				Kind:   kind,
			}
			// ADR-0032: preserve extension-introduced access-method
			// names (ivfflat, hnsw) verbatim so the same-engine writer
			// can re-emit `USING <method>`. Only fires when the
			// operator opted into the owning extension; otherwise the
			// IR keeps its existing IndexKindUnspecified shape and the
			// writer falls through to the default (btree).
			if kind == ir.IndexKindUnspecified &&
				extensionAccessMethodEnabled(method, r.enabledExtensions) {
				idx.Method = method
			}
			collected[k] = idx
			if isPrimary {
				primary[tableName] = indexName
			}
		}
		// Bug 47: only carry opclass forward for extension-introduced
		// access methods. btree/hash/gin/gist all have sensible default
		// opclasses for built-in types; emitting the opcname there
		// would (a) be redundant (b) clutter the DDL diffs (c) risk
		// surfacing internal opcname differences across PG versions.
		// Extension AMs (pgvector's ivfflat / hnsw) genuinely need the
		// opclass on every column entry — hnsw rejects the index at
		// CREATE without it.
		col := ir.IndexColumn{Column: colName}
		if opclass != "" && idx.Method != "" {
			col.OperatorClass = opclass
		}
		idx.Columns = append(idx.Columns, col)
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

// populateCheckConstraints fills in CheckConstraint lists for each
// table.
//
// We query pg_constraint directly rather than information_schema.
// check_constraints because:
//   - pg_constraint exposes the structural contype filter (`'c'` for
//     CHECK, `'n'` for the implicit NOT-NULL check Postgres synthesizes
//     on NOT NULL columns). information_schema.check_constraints
//     surfaces both kinds blended together, so callers there have to
//     pattern-match the expression text — the wrong layer.
//   - pg_get_expr(conbin, conrelid) returns the canonical, non-quoted
//     expression form, sparing us a strip-`CHECK (...)`-wrapper step.
//
// Both column-scoped (`qty INT CHECK (qty >= 0)`) and table-scoped
// (`CHECK (start_date <= end_date)`) declarations land here — PG
// stores both as table-level pg_constraint rows, and the IR mirrors
// that shape.
func (r *SchemaReader) populateCheckConstraints(ctx context.Context, tables map[string]*ir.Table) error {
	const q = `
		SELECT
			cl.relname AS table_name,
			con.conname,
			pg_get_expr(con.conbin, con.conrelid)
		FROM   pg_constraint con
		JOIN   pg_class      cl ON cl.oid = con.conrelid
		JOIN   pg_namespace  n  ON n.oid  = cl.relnamespace
		WHERE  n.nspname    = $1
		  AND  con.contype  = 'c'
		ORDER  BY cl.relname, con.conname`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tableName, name, expr string
		if err := rows.Scan(&tableName, &name, &expr); err != nil {
			return err
		}
		t, ok := tables[tableName]
		if !ok {
			continue
		}
		t.CheckConstraints = append(t.CheckConstraints, &ir.CheckConstraint{
			Name:        name,
			Expr:        expr,
			ExprDialect: dialectName,
		})
	}
	return rows.Err()
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

	return ir.DefaultExpression{Expr: s, Dialect: dialectName}
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
