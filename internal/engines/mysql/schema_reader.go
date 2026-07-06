// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// SchemaReader reads schema metadata from a MySQL database via
// information_schema. It implements [ir.SchemaReader].
//
// The reader holds an open *sql.DB; callers should call Close when
// done to release the connection pool.
type SchemaReader struct {
	db     *sql.DB
	schema string // database name to read

	// flavor records the engine flavor (vanilla MySQL, PlanetScale)
	// the reader was opened under. Threaded in by [Engine.OpenSchemaReader]
	// so optional surfaces (ADR-0056 diagnose probe, future flavor-
	// specific probes) can declare flavor-accurate capabilities
	// without re-deriving them. Zero value = FlavorVanilla, which
	// preserves the historical behaviour of every reader that
	// pre-dates this field.
	flavor Flavor

	// multiDB is set by [SchemaReader.SetMultiDatabaseScope] when this
	// reader is reading one database of a multi-database fan-out run
	// (ADR-0074). It flips two behaviours, both NO-OP in the default
	// single-database mode (back-compat):
	//
	//   - every emitted [ir.Table.Schema] / [ir.View.Schema] is stamped
	//     with r.schema (the source database name), so the orchestrator
	//     can route each database to its same-named target namespace.
	//
	//   - the flat-scope foreign-key carve-out is lifted:
	//     [ir.ForeignKey.ReferencedSchema] is POPULATED with the
	//     referenced table's database, and a FK referencing a database
	//     OUTSIDE the selected set (per dbInScope) is REFUSED LOUDLY at
	//     read time rather than silently flattened.
	multiDB bool

	// dbInScope reports whether a referenced database name is part of
	// the multi-database run's selected set. Consulted only when
	// multiDB is true; a nil predicate (the single-database default)
	// disables the FK carve-out entirely.
	dbInScope func(database string) bool
}

// SetMultiDatabaseScope implements [ir.MultiDatabaseScoper]. It marks
// the reader as reading one database (`database`) of a multi-database
// fan-out run (ADR-0074) and supplies the `inScope` predicate the
// flat-scope FK carve-out uses to refuse out-of-scope cross-database
// references. Called by the orchestrator before [ReadSchema]; a nil
// inScope leaves the reader in single-database mode.
func (r *SchemaReader) SetMultiDatabaseScope(database string, inScope func(database string) bool) {
	if inScope == nil {
		return
	}
	r.multiDB = true
	r.dbInScope = inScope
	// The reader already reads r.schema (set from the DSN's DBName at
	// open); database is the same value, threaded for clarity and as a
	// defensive cross-check against a mis-cloned DSN.
	if database != "" {
		r.schema = database
	}
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
// concept: tables, columns, indexes, foreign keys, views) rather than
// per-table round-trips. This keeps reads fast on large schemas and
// keeps the query surface auditable.
func (r *SchemaReader) ReadSchema(ctx context.Context) (*ir.Schema, error) {
	tables, err := r.readTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("mysql: read tables: %w", err)
	}
	views, err := r.readViews(ctx)
	if err != nil {
		return nil, fmt.Errorf("mysql: read views: %w", err)
	}
	if len(tables) == 0 && len(views) == 0 {
		return &ir.Schema{}, nil
	}

	if len(tables) > 0 {
		if err := r.populateColumns(ctx, tables); err != nil {
			return nil, fmt.Errorf("mysql: read columns: %w", err)
		}
		if err := r.populateIndexes(ctx, tables); err != nil {
			return nil, fmt.Errorf("mysql: read indexes: %w", err)
		}
		if err := r.populateForeignKeys(ctx, tables); err != nil {
			return nil, fmt.Errorf("mysql: read foreign keys: %w", err)
		}
		if err := r.populateCheckConstraints(ctx, tables); err != nil {
			return nil, fmt.Errorf("mysql: read check constraints: %w", err)
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

// readViews loads the view list for the bound database. MySQL stores
// views in information_schema.views; the VIEW_DEFINITION column carries
// the SELECT body MySQL parsed at CREATE time (with backtick-quoted
// identifiers and the storage-form decorations that
// [normalizeMySQLExpressionText] strips elsewhere). Phase 1 emits the
// definition verbatim on same-engine pairs and relies on the loud-
// failure tenet on cross-engine — view body translation is a future
// Phase 3 effort.
//
// Materialized views don't exist in MySQL; View.Materialized is always
// false on MySQL sources.
//
// `CHECK_OPTION`, `IS_UPDATABLE`, `DEFINER`, and `SECURITY_TYPE` are
// metadata Phase 1 ignores — the goal is "round-trip the SELECT body",
// not preserve the operator's full DDL surface. A future enhancement
// could persist these on the IR if real-world demand surfaces.
func (r *SchemaReader) readViews(ctx context.Context) ([]*ir.View, error) {
	const q = `
		SELECT table_name, view_definition
		FROM   information_schema.views
		WHERE  table_schema = ?
		ORDER  BY table_name`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*ir.View
	for rows.Next() {
		var name, definition string
		if err := rows.Scan(&name, &definition); err != nil {
			return nil, err
		}
		out = append(out, &ir.View{
			// Schema is empty in single-database mode (flat scope); in
			// multi-database mode (ADR-0074) it carries the source
			// database name so the orchestrator routes the view to its
			// same-named target namespace.
			Schema:            r.namespaceName(),
			Name:              name,
			Definition:        definition,
			DefinitionDialect: dialectName,
			Materialized:      false,
		})
	}
	return out, rows.Err()
}

// readTables loads the table list and returns a map keyed by table
// name. The map's values are skeleton [ir.Table] structs; populate*
// methods fill them in.
//
// sluice's own bookkeeping tables (sluice_cdc_state from continuous
// sync, sluice_migrate_state + sluice_migrate_table_progress from
// resumable migrations — ADR-0082) are excluded
// — they're persisted on the target as a side effect of running
// sluice itself, not user data, and including them would surface as
// "your migration has an extra table" surprises in cross-engine
// re-migrations.
func (r *SchemaReader) readTables(ctx context.Context) (map[string]*ir.Table, error) {
	const q = `
		SELECT table_name, IFNULL(table_comment, '')
		FROM   information_schema.tables
		WHERE  table_schema = ?
		  AND  table_type   = 'BASE TABLE'
		  AND  table_name NOT IN ('sluice_cdc_state', 'sluice_migrate_state', 'sluice_migrate_table_progress')
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
		// Schema is empty in single-database mode (flat scope); in
		// multi-database mode (ADR-0074) it carries the source database
		// name so the orchestrator routes the table to its same-named
		// target namespace.
		out[name] = &ir.Table{Schema: r.namespaceName(), Name: name, Comment: comment}
	}
	return out, rows.Err()
}

// namespaceName returns the database name to stamp onto emitted
// [ir.Table.Schema] / [ir.View.Schema]: the source database name in
// multi-database mode (ADR-0074), or the empty string in the default
// single-database mode (flat scope, byte-identical back-compat).
func (r *SchemaReader) namespaceName() string {
	if r.multiDB {
		return r.schema
	}
	return ""
}

// populateColumns fills in Column lists for each table.
func (r *SchemaReader) populateColumns(ctx context.Context, tables map[string]*ir.Table) error {
	// `srs_id` is geometry-only (NULL on non-geometry columns).
	// IFNULL(..., 0) keeps the scan tidy — 0 is also the "no SRID
	// declared" value for geometry columns, so the cast is
	// semantically correct for both cases. MySQL added this column
	// in 8.0; pre-8.0 servers would error here, but sluice's
	// supported MySQL baseline is 8.0+.
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
			IFNULL(srs_id, 0),
			column_type,
			IFNULL(extra, ''),
			IFNULL(column_comment, ''),
			IFNULL(generation_expression, '')
		FROM   information_schema.columns
		WHERE  table_schema = ?
		ORDER  BY table_name, ordinal_position`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Binary-family columns whose literal default information_schema reports
	// as a hex literal (`0x…`) get a second, authoritative read from SHOW
	// CREATE TABLE after the scan — information_schema C-string-truncates any
	// such default at its first NUL byte (see recoverFromShowCreate).
	// Collected here so the recovery runs one SHOW CREATE per affected table,
	// not per column (and shares that fetch with table-comment recovery).
	var pending []pendingBinaryDefault

	for rows.Next() {
		var (
			tableName  string
			colName    string
			ordinal    int
			defaultVal sql.NullString
			isNullable string
			meta       columnMeta
			comment    string
			genExpr    string
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
			&meta.SrsID,
			&meta.ColumnType,
			&meta.Extra,
			&comment,
			&genExpr,
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

		col := &ir.Column{
			Name:     colName,
			Type:     typ,
			Nullable: strings.EqualFold(isNullable, "YES"),
			Default:  translateDefault(defaultVal, meta.Extra, typ),
			Comment:  comment,
		}
		applyGenerated(col, genExpr, meta.Extra)
		t.Columns = append(t.Columns, col)

		if binaryLiteralDefaultNeedsRecovery(typ, meta.Extra, defaultVal) {
			pending = append(pending, pendingBinaryDefault{table: tableName, col: col})
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// One SHOW CREATE pass recovers BOTH the NUL-truncated binary defaults
	// collected above AND any NUL-truncated table comments (set in
	// readTables) — a table needing either is re-read exactly once.
	return r.recoverFromShowCreate(ctx, tables, pending)
}

// populateIndexes fills in Index lists for each table, separating the
// primary key from secondary indexes.
//
// MySQL 8.0.13+ supports functional (expression) indexes — e.g.
// `CREATE INDEX idx ON t ((LOWER(email)))` — and stores those entries
// in information_schema.statistics with COLUMN_NAME = NULL and the
// expression text in EXPRESSION. We scan COLUMN_NAME into a
// sql.NullString so the NULL doesn't blow up the read, and route
// NULL-column entries through the IR's expression-entry shape.
func (r *SchemaReader) populateIndexes(ctx context.Context, tables map[string]*ir.Table) error {
	const q = `
		SELECT
			table_name,
			index_name,
			non_unique,
			LOWER(IFNULL(index_type, '')),
			column_name,
			IFNULL(expression, ''),
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
	collected := map[tableObjectKey]*ir.Index{}

	for rows.Next() {
		var (
			tableName  string
			indexName  string
			nonUnique  int
			indexType  string
			colName    sql.NullString
			expression string
			seq        int
			subPart    int64
			collation  string
		)
		if err := rows.Scan(
			&tableName, &indexName, &nonUnique, &indexType,
			&colName, &expression, &seq, &subPart, &collation,
		); err != nil {
			return err
		}
		if _, ok := tables[tableName]; !ok {
			continue
		}

		k := tableObjectKey{table: tableName, name: indexName}
		idx, ok := collected[k]
		if !ok {
			idx = &ir.Index{
				Name:   indexName,
				Unique: nonUnique == 0,
				Kind:   indexKindFrom(indexType),
			}
			collected[k] = idx
		}
		entry := ir.IndexColumn{
			Desc:   collation == "D",
			Length: int(subPart),
		}
		if colName.Valid {
			entry.Column = colName.String
		} else {
			// Functional/expression index entry. The raw EXPRESSION text
			// carries MySQL's stored-form decorations (backtick
			// identifier quotes, charset introducers, escaped
			// apostrophes); normalize them at the read boundary so the
			// IR holds portable expression text — same approach as
			// generated columns and CHECK constraints. The dialect tag
			// lets the cross-engine writer (PG) apply the ADR-0016
			// translator to MySQL idioms in the expression body
			// (json_unquote/json_extract → ->>, IFNULL → COALESCE,
			// etc.) instead of emitting them verbatim.
			entry.Expression = normalizeMySQLExpressionText(expression)
			entry.ExpressionDialect = dialectName
		}
		idx.Columns = append(idx.Columns, entry)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Attach indexes to their tables; PRIMARY becomes Table.PrimaryKey.
	// Sorted drain, not map order — see [sortedTableObjectKeys]
	// (task #41 determinism requirement).
	for _, k := range sortedTableObjectKeys(collected) {
		idx := collected[k]
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

	collected := map[tableObjectKey]*ir.ForeignKey{}

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
		k := tableObjectKey{table: tableName, name: name}
		fk, ok := collected[k]
		if !ok {
			// Flat-scope carve-out (ADR-0074). MySQL has a flat scope:
			// its `TABLE_SCHEMA` column is a database name, not a
			// namespace, and the IR contract on
			// ir.ForeignKey.ReferencedSchema says it must be empty for
			// flat-scope engines. So in SINGLE-database mode we
			// deliberately DROP refSchema here — propagating it would
			// leak the source database name into target dialects that
			// *do* have namespaced schemas (e.g. emitting
			// `REFERENCES "source_db"."users"(...)` against a Postgres
			// target where no such schema exists).
			//
			// In MULTI-database mode the carve-out is LIFTED: the
			// migrated set spans N databases, each routed to a
			// same-named target namespace, so a cross-database FK CAN
			// be honoured — but only if its referenced database is part
			// of the selected set. A FK referencing a database OUTSIDE
			// the set is REFUSED LOUDLY (the loud-failure tenet): sluice
			// cannot guarantee the referent exists on the target, and
			// silently dropping the reference to a flat one would create
			// a broken / divergent constraint. See refusedSchema below.
			refQualified := ""
			if r.multiDB {
				refDB := refSchema
				if refDB == "" {
					// A same-database FK reports its own database in
					// referenced_table_schema; an empty value would be a
					// MySQL anomaly. Default to the child's database so a
					// same-DB FK is qualified consistently.
					refDB = r.schema
				}
				if r.dbInScope != nil && !r.dbInScope(refDB) {
					return fmt.Errorf(
						"mysql: foreign key %q on table %q references table %q in database %q, "+
							"which is OUTSIDE the selected multi-database set — sluice cannot guarantee "+
							"the referenced database exists on the target (remedy: add %q via "+
							"--include-database, or drop the table from scope via --exclude-table %q)",
						name, tableName, refTable, refDB, refDB, tableName,
					)
				}
				refQualified = refDB
			}
			fk = &ir.ForeignKey{
				Name:             name,
				ReferencedSchema: refQualified,
				ReferencedTable:  refTable,
				OnUpdate:         fkActionFrom(updateRule),
				OnDelete:         fkActionFrom(deleteRule),
			}
			collected[k] = fk
		}
		fk.Columns = append(fk.Columns, col)
		fk.ReferencedColumns = append(fk.ReferencedColumns, refCol)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Sorted drain, not map order — see [sortedTableObjectKeys]
	// (task #41 determinism requirement).
	for _, k := range sortedTableObjectKeys(collected) {
		t := tables[k.table]
		t.ForeignKeys = append(t.ForeignKeys, collected[k])
	}
	return nil
}

// populateCheckConstraints fills in CheckConstraint lists for each
// table. CHECKs declared inline on a column (e.g. `qty INT CHECK
// (qty >= 0)`) and at the table level (e.g. `CHECK (start_date <=
// end_date)`) both land here — MySQL normalizes both forms into
// information_schema.check_constraints as table-level entries, and
// the IR mirrors that shape.
//
// Requires MySQL 8.0.16+, which is sluice's baseline. Earlier
// versions silently parsed-and-discarded CHECK clauses; sluice's
// MySQL contract excludes them.
//
// The expression text MySQL stores in CHECK_CLAUSE has been parsed
// and reformatted with backtick-quoted identifiers — for the source
// text qty >= 0, MySQL stores the column name wrapped in backticks.
// Postgres rejects backticks at apply time, so we strip them here at
// the read boundary — same dialect-quoting normalization as the
// generated-column path uses.
func (r *SchemaReader) populateCheckConstraints(ctx context.Context, tables map[string]*ir.Table) error {
	const q = `
		SELECT
			tc.table_name,
			cc.constraint_name,
			cc.check_clause
		FROM   information_schema.check_constraints cc
		JOIN   information_schema.table_constraints  tc
		  ON   tc.constraint_schema = cc.constraint_schema
		 AND   tc.constraint_name   = cc.constraint_name
		WHERE  tc.table_schema    = ?
		  AND  tc.constraint_type = 'CHECK'
		ORDER  BY tc.table_name, cc.constraint_name`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, name, clause string
		if err := rows.Scan(&tableName, &name, &clause); err != nil {
			return err
		}
		t, ok := tables[tableName]
		if !ok {
			continue
		}
		t.CheckConstraints = append(t.CheckConstraints, &ir.CheckConstraint{
			Name:        name,
			Expr:        normalizeMySQLExpressionText(clause),
			ExprDialect: dialectName,
		})
	}
	return rows.Err()
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
			column_type,
			IFNULL(extra, ''),
			IFNULL(column_comment, ''),
			IFNULL(generation_expression, '')
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
			genExpr    string
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
			&genExpr,
		); err != nil {
			return nil, err
		}

		typ, err := translateType(meta)
		if err != nil {
			return nil, fmt.Errorf("table %q column %q: %w", table, colName, err)
		}
		col := &ir.Column{
			Name:     colName,
			Type:     typ,
			Nullable: strings.EqualFold(isNullable, "YES"),
			Default:  translateDefault(defaultVal, meta.Extra, typ),
			Comment:  comment,
		}
		applyGenerated(col, genExpr, meta.Extra)
		out.Columns = append(out.Columns, col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out.Columns) == 0 {
		return nil, fmt.Errorf("mysql: table %s.%s has no columns (does it exist?)", schema, table)
	}

	// Bug 88: the CDC reader's DELETE emit path narrows the BEFORE-image
	// to PK columns to avoid silent zero-match on MINIMAL / NOBLOB
	// sources (see filterDeleteBefore in cdc_reader.go). Load the PK
	// column-name list now so the cached *tableSchema carries it.
	// loadPrimaryKeyDB is a *sql.DB-flavoured sibling of the applier's
	// loadPrimaryKey (which takes *sql.Tx).
	pk, err := loadPrimaryKeyDB(ctx, db, schema, table)
	if err != nil {
		return nil, fmt.Errorf("mysql: loadTableSchema %s.%s: pk lookup: %w", schema, table, err)
	}
	out.PrimaryKey = pk

	return out, nil
}

// loadPrimaryKeyDB reads the PK column-name list for the named table
// via a *sql.DB rather than a *sql.Tx. Mirror of the applier's
// loadPrimaryKey (change_applier.go), which uses a *sql.Tx for
// symmetry with the data write tx; the CDC reader's schema cache
// loads metadata outside any data tx, so a *sql.DB is correct here.
//
// Returns an empty slice (not nil) for tables with no PK; the
// caller's [filterDeleteBefore] falls back to the full Before-image
// in that case (same semantics as PG's REPLICA IDENTITY FULL on a
// PK-less relation).
func loadPrimaryKeyDB(ctx context.Context, db *sql.DB, schema, table string) ([]string, error) {
	const q = `
		SELECT column_name
		FROM   information_schema.statistics
		WHERE  table_schema = ?
		  AND  table_name   = ?
		  AND  index_name   = 'PRIMARY'
		ORDER  BY seq_in_index`

	rows, err := db.QueryContext(ctx, q, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	pk := make([]string, 0, 4)
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		pk = append(pk, col)
	}
	return pk, rows.Err()
}

// applyGenerated populates the IR column's GeneratedExpr / GeneratedStored
// fields from MySQL's information_schema.columns metadata.
//
// information_schema reports the expression in GENERATION_EXPRESSION
// (empty / NULL on plain columns) and the storage class as a token in
// EXTRA: "VIRTUAL GENERATED" or "STORED GENERATED". When the storage
// class isn't explicit (some sluice-supported MySQL flavors are
// inconsistent), default to STORED — that matches the project's
// "verbatim passthrough plus loud failure on mismatch" translation
// policy: a STORED column is always replicable; a VIRTUAL one might
// be silently lossy if the target dialect doesn't support virtual
// columns.
//
// The expression is run through normalizeMySQLExpressionText to
// strip MySQL's stored-form decorations (backtick identifier quotes,
// charset introducers, C-style apostrophe escapes). Stripping is
// dialect-quoting normalization, not function/operator translation
// — verbatim passthrough still applies to the substantive
// expression body. See [normalizeMySQLExpressionText] for the full
// rationale. In the rare case where the source identifier would
// actually need quoting on the target (a reserved word or unusual
// case), the operator must rewrite the source column or drop the
// GENERATED clause via the per-column mappings hook.
func applyGenerated(col *ir.Column, genExpr, extra string) {
	if genExpr == "" {
		return
	}
	col.GeneratedExpr = normalizeMySQLExpressionText(genExpr)
	col.GeneratedExprDialect = dialectName
	upper := strings.ToUpper(extra)
	switch {
	case strings.Contains(upper, "VIRTUAL GENERATED"):
		col.GeneratedStored = false
	case strings.Contains(upper, "STORED GENERATED"):
		col.GeneratedStored = true
	default:
		col.GeneratedStored = true
	}
}

// stripMySQLIdentifierQuotes removes backtick characters from s.
// Used to normalise the dialect-specific identifier quoting MySQL
// embeds in stored generation expressions; see applyGenerated for
// the rationale. An identifier that contains an embedded backtick
// (escaped as a doubled backtick in the source — exceedingly rare
// in real-world schemas) collapses to a single backtick after
// stripping, which is at worst a target-side parse error, not
// silent corruption — same loud-failure outcome as any other
// non-portable expression.
func stripMySQLIdentifierQuotes(s string) string {
	if !strings.ContainsRune(s, '`') {
		return s
	}
	return strings.ReplaceAll(s, "`", "")
}

// normalizeMySQLExpressionText folds a parsed-and-reformatted
// information_schema expression (CHECK_CLAUSE, GENERATION_EXPRESSION)
// back toward portable SQL. Used at the read boundary so the IR
// holds expression text that's grammatical in both MySQL and
// Postgres without further translation.
//
// Three normalizations apply:
//
//   - Backtick identifier quotes are stripped. MySQL stores `qty`
//     where the source had qty; Postgres rejects backticks at apply
//     time. (Same as stripMySQLIdentifierQuotes.)
//
//   - Charset introducers — MySQL's `_latin1'foo'` / `_utf8mb4'foo'`
//     prefixes that the parser inserts on every string literal — are
//     stripped. The introducer is a MySQL-internal artifact;
//     Postgres rejects it as a syntax error. Stripping is dialect-
//     decoration removal, not function/operator translation, so
//     verbatim passthrough still applies to the substantive
//     expression body.
//
//   - The backslash-escaped form MySQL wraps every string literal's
//     delimiters in (so 'open' is stored as backslash-apostrophe-
//     open-backslash-apostrophe) is rewritten to the bare
//     apostrophe form. MySQL accepts both; Postgres with
//     standard_conforming_strings=on (default since 9.1) only
//     accepts the bare form.
//
// Other non-portable constructs (function-name differences, operator
// spelling, etc.) are intentionally NOT translated — verbatim
// passthrough plus loud failure on the target is the v1 policy.
func normalizeMySQLExpressionText(s string) string {
	s = stripMySQLIdentifierQuotes(s)
	s = stripMySQLCharsetIntroducers(s)
	s = convertMySQLEscapedApostrophes(s)
	return s
}

// stripMySQLCharsetIntroducers removes _<charset>' prefixes from
// string literals in MySQL's stored expression text. The introducer
// is a MySQL-internal artifact: `_latin1'open'` is the parser's
// canonical form for the source text 'open' under a latin1
// connection. Other dialects don't accept it.
//
// We walk character by character and only strip the introducer when
// it actually precedes a string literal opener — either a bare `'`
// or the C-style escape `\'` MySQL also stores. The dual recognition
// is needed because [convertMySQLEscapedApostrophes] runs after this
// pass (it only recognises `'` as a literal opener; the introducer
// strip has to fire first or `_latin1\'foo\'` would never match).
//
// A column or alias that happens to start with an underscore (rare
// but possible after backtick-stripping) is unaffected — the strip
// only fires when the trailing apostrophe is present.
func stripMySQLCharsetIntroducers(s string) string {
	if !strings.Contains(s, "_") {
		return s
	}
	var sb strings.Builder
	sb.Grow(len(s))
	i := 0
	for i < len(s) {
		// Identify a candidate introducer: an underscore that follows
		// a non-identifier character (or string start) and is itself
		// followed by a charset name and a string-literal opener.
		if s[i] == '_' && (i == 0 || !isIdentRune(rune(s[i-1]))) {
			j := i + 1
			for j < len(s) && isIdentRune(rune(s[j])) {
				j++
			}
			if j > i+1 && j < len(s) {
				switch {
				case s[j] == '\'':
					// `_latin1'foo'` → `'foo'`: skip the introducer.
					i = j
					continue
				case s[j] == '\\' && j+1 < len(s) && s[j+1] == '\'':
					// `_latin1\'foo\'` → `\'foo\'`: skip the
					// introducer. The escape itself is rewritten by
					// the next pass.
					i = j
					continue
				}
			}
		}
		sb.WriteByte(s[i])
		i++
	}
	return sb.String()
}

// convertMySQLEscapedApostrophes rewrites the C-style \' escape
// MySQL uses around string-literal delimiters in stored expression
// text to a bare apostrophe '. MySQL's information_schema wraps both
// the opening and closing delimiters of every literal in this
// escape — for the source text 'open' the parser stores \'open\'.
// Postgres with standard_conforming_strings=on (default since 9.1)
// rejects \' as a syntax error, so we drop the leading backslash at
// the read boundary.
//
// We only rewrite \' (backslash directly before apostrophe) — bare
// backslashes are left alone so non-portable expressions still fail
// loudly on the target rather than be silently rewritten. Literals
// containing embedded apostrophes are stored with a more elaborate
// escape sequence (\'foo\\\'bar\'); those round-trip imperfectly,
// which is acceptable v1 behaviour given the verbatim-passthrough
// policy — the target rejects malformed expressions loudly rather
// than silently corrupting data.
func convertMySQLEscapedApostrophes(s string) string {
	if !strings.Contains(s, `\'`) {
		return s
	}
	return strings.ReplaceAll(s, `\'`, `'`)
}

// isIdentRune reports whether r is a character that can appear in a
// MySQL unquoted identifier or charset name (letter, digit, or
// underscore).
func isIdentRune(r rune) bool {
	return (r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9') ||
		r == '_'
}

// translateDefault converts the (column_default, extra) pair from
// information_schema into an [ir.DefaultValue]. MySQL signals
// expression defaults with the "DEFAULT_GENERATED" token in extra;
// that distinction is preserved in the IR rather than collapsed.
//
// Two read-boundary normalizations apply before the value reaches the
// IR, mirroring how generated-column and CHECK expressions are folded
// toward portable text in [normalizeMySQLExpressionText]:
//
//   - A MySQL bit-literal default (`b'0'`, `B'1010'`) on a BIT(1)
//     column (IR type [ir.Boolean]) is converted to its decimal value
//     as a dialect-neutral [ir.DefaultLiteral] (validation-rig catalog
//     #4). information_schema reports the literal verbatim; emitting
//     `'b”0”'` as a string literal fails on every target (MySQL Error
//     1067, Postgres SQLSTATE 22P02) because neither accepts it as a
//     TINYINT / boolean default. The decimal form (`0`, `1`) is
//     accepted by MySQL TINYINT and Postgres BOOLEAN alike.
//
//   - A bit-literal default on a BIT(N>1) column (IR type [ir.Bit])
//     is preserved as a tagged bit-literal [ir.DefaultExpression] —
//     the decimal collapse (catalog Bug 62) was wrong for a real bit
//     column: BIT(8) DEFAULT b'10100101' must land as the bit value
//     0xA5, not the decimal string '165'. The writers render it in
//     each target's bit-literal syntax (`b'…'` MySQL, `B'…'` PG).
//
//   - Expression-form defaults carry MySQL's stored-form backtick
//     identifier quotes, charset introducers (`_utf8mb4'x'`), and
//     C-style apostrophe escapes (`\'x\'`); a cross-engine Postgres
//     target rejects all three (SQLSTATE 42601 — catalog #6, Bug 64).
//     These are the same MySQL-internal decorations
//     [normalizeMySQLExpressionText] strips for generated / CHECK /
//     index expressions, so the DEFAULT-expression path uses the
//     identical helper — the IR holds portable, backtick-free
//     expression text at all four expression positions, symmetric by
//     construction. Backtick stripping is dialect-quoting
//     normalization, not function/operator translation; the writers'
//     reserved-word re-quoting (ADR-0045's uniform
//     requote(translate(expr)) composition) re-adds the target's
//     quoting where a referenced column name is reserved, identically
//     for the cross-engine and same-engine paths — exactly as it
//     already does for generated / CHECK / index. (Pre-Bug-64 the
//     backtick strip was deliberately skipped here under the obsolete
//     belief the writer needed to see the source backticks; ADR-0045
//     made the writer composition uniform across all four positions,
//     so the reader-side strip is now the correct, symmetric locus.)
func translateDefault(def sql.NullString, extra string, typ ir.Type) ir.DefaultValue {
	if !def.Valid {
		return ir.DefaultNone{}
	}
	if bits, ok := bitLiteralBits(def.String); ok {
		// BIT(N>1) → ir.Bit: preserve the bit string so the writers can
		// emit it as a proper bit literal in each target's syntax. The
		// `bit` dialect tag is recognised by the bit-aware default path
		// in both writers (catalog Bug 62).
		if _, isBit := typ.(ir.Bit); isBit {
			return ir.DefaultExpression{Expr: "b'" + bits + "'", Dialect: bitLiteralDialect}
		}
		// BIT(1) → ir.Boolean: the decimal form is what TINYINT(1) /
		// BOOLEAN accept (catalog #4 — unchanged behaviour).
		return ir.DefaultLiteral{Value: bitsToDecimal(bits)}
	}
	if strings.Contains(strings.ToUpper(extra), "DEFAULT_GENERATED") {
		// Tag the dialect so a cross-engine writer (e.g. PG) routes the
		// expression through its translator. Without the tag,
		// MySQL-spelled defaults like `(UUID())`, `(RAND() * 100)`, or
		// `(DATE_ADD(...))` would emit verbatim and fail loud on PG —
		// see Bugs 28/29/30.
		expr := normalizeMySQLExpressionText(def.String)
		return ir.DefaultExpression{Expr: expr, Dialect: "mysql"}
	}
	// Binary-column literal default (Bug: BINARY(N) DEFAULT round-trip).
	// MySQL 8 reports a BINARY/VARBINARY column's literal default in
	// information_schema.COLUMN_DEFAULT as a bare hex literal — e.g.
	// `BINARY(14) NOT NULL DEFAULT '19700101000000'` comes back as
	// `0x3139373030313031303030303030` (extra is EMPTY — it is a literal,
	// not a DEFAULT_GENERATED expression). Carried as a plain
	// ir.DefaultLiteral it would be re-quoted by the writer as the STRING
	// `'0x3139…'` — a 30-char value on a 14-byte column — and MySQL rejects
	// the re-emitted DDL with Error 1067 (Invalid default value). Preserve
	// it as a tagged hex-literal DefaultExpression so the writer renders it
	// BARE (`DEFAULT 0x3139…`), which MySQL round-trips byte-exactly.
	//
	// The recognition is gated on the column being a binary-family type: a
	// genuine string default whose text happens to look like hex — e.g.
	// `VARCHAR(20) DEFAULT '0x1234'` — reports the IDENTICAL surface form
	// `0x1234` in COLUMN_DEFAULT and must stay a quoted string literal. The
	// column type is the only signal that disambiguates the two.
	//
	// This produces only a PROVISIONAL default: information_schema
	// C-string-truncates a binary default at its first NUL byte (a leading-NUL
	// default reports the bare `0x`; a mid/trailing-NUL default reports a
	// well-formed but SHORT literal, e.g. 0x2700 → `0x27`). populateColumns
	// re-reads any such column's true bytes from SHOW CREATE TABLE and
	// overwrites this value — see binaryLiteralDefaultNeedsRecovery /
	// recoverFromShowCreate (Finding C). translateDefault itself stays
	// pure (no DB handle) and is exercised directly by the reader unit tests.
	if isBinaryFamilyType(typ) {
		if hexDef, ok := hexLiteralDefault(def.String); ok {
			return ir.DefaultExpression{Expr: hexDef, Dialect: hexLiteralDialect}
		}
	}
	return ir.DefaultLiteral{Value: def.String}
}

// hexLiteralDialect tags a MySQL hex-literal default (`0x<hex>`) on a
// BINARY/VARBINARY column so the writers' default path renders it bare
// (MySQL's `DEFAULT 0x…` form) rather than quoting it as a string. Kept
// distinct from the "mysql" dialect tag: routing a hex literal through the
// general MySQL→PG expression translator would emit it verbatim, and it is
// value-neutral (only its surface syntax is MySQL-specific), mirroring the
// bit-literal precedent.
const hexLiteralDialect = "hexbytes"

// isBinaryFamilyType reports whether t is a fixed- or variable-width MySQL
// binary string type (BINARY / VARBINARY). BLOB is excluded — MySQL forbids
// a DEFAULT on BLOB, so a hex-literal default never arises there.
func isBinaryFamilyType(t ir.Type) bool {
	switch t.(type) {
	case ir.Binary, ir.Varbinary:
		return true
	default:
		return false
	}
}

// hexLiteralDefault recognises the bare MySQL hex-literal form
// `0x<hex-digits>` that information_schema.COLUMN_DEFAULT reports for a
// BINARY/VARBINARY column's literal default, and returns it verbatim for
// re-emission. Requires the `0x`/`0X` prefix followed by a non-empty, EVEN
// number of hex digits (a byte-aligned literal — the only well-formed form
// MySQL emits). Anything else — including the truncated `0x` MySQL reports
// for a non-UTF8/leading-NUL default it cannot represent — returns ok=false
// so the caller falls back to the verbatim literal path.
func hexLiteralDefault(raw string) (string, bool) {
	s := strings.TrimSpace(raw)
	if len(s) < 3 || s[0] != '0' || (s[1] != 'x' && s[1] != 'X') {
		return "", false
	}
	digits := s[2:]
	if len(digits)%2 != 0 {
		return "", false
	}
	for i := 0; i < len(digits); i++ {
		c := digits[i]
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
		if !isHex {
			return "", false
		}
	}
	return s, true
}

// bitLiteralDialect tags a bit-literal [ir.DefaultExpression] so the
// writers' bit-aware default path recognises it and renders the
// literal in each target's bit syntax (`b'…'` MySQL, `B'…'` PG). It is
// deliberately distinct from the "mysql" dialect tag — a bit literal
// is dialect-neutral in value; only its surface syntax differs, and
// routing it through the general MySQL→PG expression translator (which
// has no bit-literal rule) would emit it verbatim and fail on PG.
const bitLiteralDialect = "bit"

// bitLiteralBits recognises a MySQL bit-literal default of the form
// b'<bits>' / B'<bits>' (optionally wrapped in the parenthesised
// `(b'<bits>')` form information_schema reports for the
// DEFAULT_GENERATED variant) and returns the raw binary-digit string
// (no quotes, no `b` prefix).
//
// MySQL columns declared `bit(N) DEFAULT b'…'` report the literal
// verbatim in information_schema.COLUMN_DEFAULT. Anything that isn't a
// well-formed binary-digit literal returns ok=false and the caller
// falls back to the verbatim path (loud failure on the target beats a
// silent guess).
func bitLiteralBits(raw string) (string, bool) {
	s := strings.TrimSpace(raw)
	// Tolerate the parenthesised `(b'…')` form MySQL reports for the
	// DEFAULT_GENERATED variant.
	if len(s) >= 2 && s[0] == '(' && s[len(s)-1] == ')' {
		s = strings.TrimSpace(s[1 : len(s)-1])
	}
	if len(s) < 3 || (s[0] != 'b' && s[0] != 'B') || s[1] != '\'' || s[len(s)-1] != '\'' {
		return "", false
	}
	bits := s[2 : len(s)-1]
	if bits == "" {
		return "", false
	}
	// 64-bit BIT is MySQL's maximum; anything wider is malformed source
	// we won't try to translate. Also reject non-binary digits.
	if len(bits) > 64 {
		return "", false
	}
	for i := 0; i < len(bits); i++ {
		if bits[i] != '0' && bits[i] != '1' {
			return "", false
		}
	}
	return bits, true
}

// bitsToDecimal converts a validated binary-digit string (as returned
// by [bitLiteralBits]) to its unsigned decimal value. Used for the
// BIT(1) → ir.Boolean default path (catalog #4): TINYINT(1) / BOOLEAN
// accept `0`/`1`, not `b'0'`.
func bitsToDecimal(bits string) string {
	var val uint64
	for i := 0; i < len(bits); i++ {
		val <<= 1
		if bits[i] == '1' {
			val |= 1
		}
	}
	return strconv.FormatUint(val, 10)
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
