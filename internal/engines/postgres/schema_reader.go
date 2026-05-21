// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
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

	// verbatimPassthrough enables the ADR-0047 verbatim tier for
	// UNcatalogued USER-DEFINED types: capture
	// pg_catalog.format_type(...) and uncatalogued index AM / opclass
	// verbatim instead of refusing. Set by
	// [SetVerbatimExtensionPassthrough] — the orchestrator turns it on
	// only for provably-same-engine PG → PG or for a PG backup (the
	// determination tiers are the named concept in verbatim_tier.go).
	// false (the default) preserves tier (c): the existing loud
	// refusal for uncatalogued user-defined types is unchanged.
	verbatimPassthrough bool

	// tableScope, when non-nil, restricts readTables to the tables the
	// operator's filter admits, so out-of-scope tables are never read
	// and their (possibly unsupported) column types are never
	// validated (catalog Bug 76). nil means "no scoping" — every base
	// table in the schema is read, the historical behaviour. Set via
	// [SetTableScope] before ReadSchema.
	tableScope func(tableName string) bool
}

// SetTableScope implements [ir.TableScoper]. The pipeline calls this
// with the engine-neutral projection of the operator's table filter
// before [ReadSchema], so per-column type validation is scoped to the
// to-be-migrated table set (catalog Bug 76). A nil predicate clears
// scoping. Idempotent; must be called before any read.
func (r *SchemaReader) SetTableScope(allow func(tableName string) bool) {
	r.tableScope = allow
}

// SetVerbatimExtensionPassthrough implements [ir.VerbatimExtensionAware]
// (ADR-0047). The orchestrator calls this with enabled=true only for a
// provably-same-engine PG → PG run or a PG backup; cross-engine /
// non-PG paths never call it, so the field stays false and the
// existing loud refusal for uncatalogued user-defined types is
// preserved. Idempotent; called at construction time before any read.
func (r *SchemaReader) SetVerbatimExtensionPassthrough(enabled bool) {
	r.verbatimPassthrough = enabled
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

		// PostGIS's geometry_columns / geography_columns views hold the
		// per-column subtype + SRID that information_schema flattens
		// away. Lookup is best-effort — the views exist only when
		// PostGIS is installed, and a schema with no spatial columns
		// has no rows there. Either way, the translator degrades
		// gracefully via GeometryUnspecified + SRID=0 when no info
		// shows up.
		geomInfo, err := r.readGeometryColumnInfo(ctx)
		if err != nil {
			return nil, fmt.Errorf("postgres: read geometry columns: %w", err)
		}
		geogInfo, err := r.readGeographyColumnInfo(ctx)
		if err != nil {
			return nil, fmt.Errorf("postgres: read geography columns: %w", err)
		}

		if err := r.populateColumns(ctx, tables, enumValues, geomInfo, geogInfo); err != nil {
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
		if err := r.populateExcludeConstraints(ctx, tables); err != nil {
			return nil, fmt.Errorf("postgres: read exclude constraints: %w", err)
		}
		if err := r.populateComments(ctx, tables); err != nil {
			return nil, fmt.Errorf("postgres: read comments: %w", err)
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
		// catalog Bug 76: skip tables the operator's filter excludes so
		// their columns are never type-validated by populateColumns
		// (which keys off this map). A scoped-out table with an
		// unsupported column type must not abort an otherwise-valid
		// migration. The pipeline's post-read TableFilter remains the
		// authoritative prune; this is the loud-failure-scoping push-down.
		if r.tableScope != nil && !r.tableScope(name) {
			continue
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
		SELECT f_table_name, f_geometry_column, type, srid, coord_dimension
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
			coordDim              int
		)
		if err := rows.Scan(&tableName, &columnName, &subtype, &srid, &coordDim); err != nil {
			return nil, err
		}
		hasZ, hasM := dimensionFlagsFromCoordDim(subtype, coordDim)
		out[tableName+"."+columnName] = geometryColumnInfo{
			Subtype: subtype,
			SRID:    int(srid),
			HasZ:    hasZ,
			HasM:    hasM,
		}
	}
	return out, rows.Err()
}

// dimensionFlagsFromCoordDim maps PostGIS's two-channel dimension
// encoding (the type column's optional Z / M / ZM suffix plus the
// coord_dimension column) to the IR's orthogonal HasZ / HasM flags.
// Bug 53: pre-fix the reader only consulted the type column, missing
// the canonical Z and ZM cases where PostGIS records the dimension
// in coord_dimension and leaves the type column as the 2D base name.
//
// PostGIS's encoding rules per the catalog reference:
//
//   - coord_dimension = 2: 2D (XY), no dimensional flags.
//   - coord_dimension = 3: 3D — either XYZ (Z only) or XYM (M only).
//     The two are distinguished by whether the type column ends in
//     "M": "POINTM" → M; "POINT" with coord_dimension=3 → Z.
//   - coord_dimension = 4: 4D (XYZM), both flags.
//
// The returned flags are layered on top of the type-string parsing
// done by parseGeometrySubtype; the translator OR-merges the two
// sources so neither alone is load-bearing for the M-suffix case.
func dimensionFlagsFromCoordDim(typeName string, coordDim int) (hasZ, hasM bool) {
	upper := strings.ToUpper(typeName)
	typeHasM := strings.HasSuffix(upper, "M") && !strings.HasSuffix(upper, "ZM")
	switch coordDim {
	case 4:
		return true, true
	case 3:
		if typeHasM {
			return false, true
		}
		return true, false
	}
	return false, false
}

// readGeographyColumnInfo is the parallel of [readGeometryColumnInfo]
// for PostGIS `geography` columns. PostGIS exposes them via the
// geography_columns view (same columns: f_table_schema /
// f_table_name / f_geography_column / type / srid). The result is
// keyed the same way ("<table>.<column>"); the resulting entries
// carry IsGeography=true so the translator selects the geography IR
// shape.
//
// As with the geometry lookup: PostGIS-absent → "relation doesn't
// exist" → empty map (the translator's existing graceful-degradation
// path handles missing entries).
func (r *SchemaReader) readGeographyColumnInfo(ctx context.Context) (map[string]geometryColumnInfo, error) {
	const q = `
		SELECT f_table_name, f_geography_column, type, srid, coord_dimension
		FROM   geography_columns
		WHERE  f_table_schema = $1`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
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
			coordDim              int
		)
		if err := rows.Scan(&tableName, &columnName, &subtype, &srid, &coordDim); err != nil {
			return nil, err
		}
		hasZ, hasM := dimensionFlagsFromCoordDim(subtype, coordDim)
		out[tableName+"."+columnName] = geometryColumnInfo{
			Subtype:     subtype,
			SRID:        int(srid),
			IsGeography: true,
			HasZ:        hasZ,
			HasM:        hasM,
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
		(strings.Contains(msg, "geometry_columns") ||
			strings.Contains(msg, "geography_columns") ||
			strings.Contains(msg, "42P01"))
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
func (r *SchemaReader) populateColumns(ctx context.Context, tables map[string]*ir.Table, enumValues map[string][]string, geomInfo, geogInfo map[string]geometryColumnInfo) error {
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
			COALESCE(a.atttypmod, -1),
			COALESCE(pg_catalog.format_type(a.atttypid, a.atttypmod), '')
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
			formatType           string
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
			&formatType,
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
			FormatType:      formatType,
		}

		// Resolve enum values for USER-DEFINED columns.
		if dataType == "user-defined" || dataType == "USER-DEFINED" {
			if values, ok := enumValues[udtName]; ok {
				meta.EnumValues = values
				// Bug 19c: carry the source enum type name so a
				// same-engine PG → PG migration preserves it verbatim
				// instead of synthesizing a per-column name.
				meta.EnumTypeName = udtName
			}
			// PostGIS geometry / geography: look up subtype + SRID from
			// the per-column info we read out of geometry_columns /
			// geography_columns. A missing entry is fine — the
			// translator handles GeometryInfo=nil by emitting
			// GeometryUnspecified. The geography_columns entry carries
			// IsGeography=true, which propagates through to
			// [ir.Geometry.IsGeography].
			switch udtName {
			case "geometry":
				if info, ok := geomInfo[tableName+"."+colName]; ok {
					info := info
					meta.GeometryInfo = &info
				}
			case "geography":
				if info, ok := geogInfo[tableName+"."+colName]; ok {
					info := info
					meta.GeometryInfo = &info
				} else {
					// Fallback: the operator may have a `geography`
					// column declared without a typmod (no row in
					// geography_columns? — rare, since PostGIS always
					// populates the view). Synthesize a minimal entry
					// so the translator dispatches to the geography
					// branch with default subtype/SRID rather than
					// falling through to the user-defined hint path.
					meta.GeometryInfo = &geometryColumnInfo{IsGeography: true}
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
			} else if verbatimTierFor(r.verbatimPassthrough) == verbatimTierVerbatim {
				// ADR-0047 tier (b): the column is USER-DEFINED and NOT
				// catalogued (the catalog lookup above missed), and the
				// run carries a same-engine-PG guarantee (live PG → PG
				// or a PG backup). Flag it for verbatim capture; the
				// translator emits ir.VerbatimType with the exact
				// format_type spelling. Catalogued types never reach
				// here (the lookup above set ExtensionName). Enum /
				// geometry still win in translateType (they have
				// first-class IR shapes); the verbatim flag is the
				// last-resort carry before the loud refusal, so it does
				// not regress them. Tier (c) (verbatimPassthrough
				// false) leaves this unset → the existing loud refusal
				// in translateType is preserved unchanged.
				meta.VerbatimEligible = true
			}
		}

		// Bug 17: core PG types with no rich cross-engine IR shape
		// (tsvector, tsquery) are carried verbatim on a same-engine-PG
		// run, mirroring the ADR-0047 USER-DEFINED verbatim tier. The
		// USER-DEFINED branch above already set (or deliberately
		// withheld) VerbatimEligible per the catalog lookup; only the
		// non-USER-DEFINED path needs this. The flag is consulted by
		// translateType strictly as a last resort, AFTER every
		// first-class core-type case has returned, so it never shadows
		// a mapped type — it only converts the final "unsupported
		// data_type" refusal into a verbatim carry. Cross-engine
		// (verbatimPassthrough false) leaves it unset, preserving the
		// loud refusal (tsvector has no MySQL equivalent).
		isUserDefined := dataType == "user-defined" || dataType == "USER-DEFINED"
		if !isUserDefined &&
			verbatimTierFor(r.verbatimPassthrough) == verbatimTierVerbatim {
			meta.VerbatimEligible = true
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

		// ADR-0044 Tier-3 schema-read gate (DEFAULT case). When the
		// classified default is an expression (not a literal /
		// auto-increment), scan it for a catalog-declared
		// extension-owned function (uuid-ossp's uuid_generate_v4,
		// pgcrypto's digest, …). If the owning extension was not
		// opted into via --enable-pg-extension, refuse loudly and
		// early here rather than letting the verbatim passthrough
		// fail with a raw PG parse error at CREATE TABLE apply time.
		// Core functions (gen_random_uuid(), now(), …) are in no
		// catalog set and so are never gated.
		if de, ok := col.Default.(ir.DefaultExpression); ok {
			if err := extensionFunctionDefaultGate(
				tableName, colName, "DEFAULT", de.Expr, r.enabledExtensions,
			); err != nil {
				return err
			}
		}

		// Postgres only supports STORED generated columns today;
		// is_generated = 'ALWAYS' implies STORED. The expression
		// passes through verbatim — translation policy is "loud
		// failure beats silent corruption", so non-portable
		// expressions surface as a target rejection at apply time
		// rather than a guess at translation.
		if strings.EqualFold(isGenerated, "ALWAYS") && genExpr != "" {
			// ADR-0044: gate generated-column expressions identically
			// to DEFAULTs — the recon confirmed both ride the same
			// verbatim passthrough, so leaving generated ungated would
			// be a silent bypass of the Tier-3 opt-in.
			if err := extensionFunctionDefaultGate(
				tableName, colName, "GENERATED", genExpr, r.enabledExtensions,
			); err != nil {
				return err
			}
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
			COALESCE(opc.opcname, '') AS opclass,
			CASE
				WHEN a.attname IS NULL
				THEN pg_get_indexdef(ix.indexrelid, u.ord::int, true)
				ELSE ''
			END AS expr,
			-- Number of *key* columns (Bug 19b). Columns at ordinal
			-- position > indnkeyatts are non-key INCLUDE payload, not
			-- part of the index key — flattening them into the key
			-- list silently changes index semantics.
			ix.indnkeyatts,
			-- Per-column ordering bits (Bug 19a). pg_index.indoption is
			-- an int2vector parallel to indkey; bit 0 (1) = DESC, bit 1
			-- (2) = NULLS FIRST. Only meaningful for key columns.
			COALESCE(uo.opt, 0) AS indoption,
			-- Partial-index WHERE predicate (Bug 19a), rendered in PG
			-- dialect with table-qualified column refs resolved. Empty
			-- string for a full (non-partial) index.
			COALESCE(pg_get_expr(ix.indpred, ix.indrelid), '') AS predicate,
			-- ADR-0047: is the access method owned by an extension
			-- (pg_depend deptype 'e')? An extension-owned AM that is
			-- NOT one of the ADR-0032 catalogued ones is carried
			-- verbatim under the verbatim tier.
			EXISTS (
				SELECT 1 FROM pg_depend d
				WHERE  d.classid = 'pg_am'::regclass
				  AND  d.objid   = am.oid
				  AND  d.deptype = 'e'
			) AS am_ext_owned,
			-- ADR-0047 / Bug 47 invariant: an opclass is carried
			-- verbatim ONLY when it is genuinely extension-owned
			-- (pg_depend deptype 'e'). Core / default opclasses stay
			-- unpopulated so a non-empty OperatorClass remains an
			-- honest "extension-owned" marker the cross-engine refusal
			-- keys on.
			COALESCE((
				SELECT EXISTS (
					SELECT 1 FROM pg_depend d
					WHERE  d.classid = 'pg_opclass'::regclass
					  AND  d.objid   = opc.oid
					  AND  d.deptype = 'e'
				)
			), false) AS opclass_ext_owned
		FROM   pg_index ix
		JOIN   pg_class      cl ON cl.oid = ix.indrelid
		JOIN   pg_class      i  ON i.oid  = ix.indexrelid
		JOIN   pg_am         am ON am.oid = i.relam
		JOIN   pg_namespace  n  ON n.oid  = cl.relnamespace
		JOIN   LATERAL unnest(ix.indkey)   WITH ORDINALITY AS u(attnum, ord) ON TRUE
		LEFT JOIN LATERAL unnest(ix.indclass) WITH ORDINALITY AS uc(opcoid, ord) ON uc.ord = u.ord
		LEFT JOIN LATERAL unnest(ix.indoption) WITH ORDINALITY AS uo(opt, ord) ON uo.ord = u.ord
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
			exprText                              string
			nKeyAtts                              int
			indOption                             int
			predicate                             string
			amExtOwned, opclassExtOwned           bool
		)
		if err := rows.Scan(
			&tableName, &indexName, &method,
			&isUnique, &isPrimary,
			&colName, &ord, &opclass, &exprText,
			&nKeyAtts, &indOption, &predicate,
			&amExtOwned, &opclassExtOwned,
		); err != nil {
			return err
		}
		if _, ok := tables[tableName]; !ok {
			continue
		}
		// Expression-index entry (catalog Bug 65): the indkey slot is
		// the `0` sentinel, so the pg_attribute join yields no column
		// name. Before ADR-0045 this entry was silently dropped, losing
		// the whole functional index from the IR — a schema-fidelity
		// loss that violates the loud-failure tenet. D3 (locked):
		// full-carry. pg_get_indexdef(idx, ord, true) renders just this
		// key's expression text (PG dialect); surface it into the IR's
		// existing IndexColumn.Expression field tagged
		// ExpressionDialect="postgres", the PG-source analogue of the
		// MySQL-source post-Bug-16 path. The opclass (Bug 47) still
		// comes from the independent opc.opcname join below, so an
		// opclass-bearing expression index is not regressed.
		isExpr := colName == "" && strings.TrimSpace(exprText) != ""
		if colName == "" && !isExpr {
			// Genuinely empty (defensive — shouldn't happen for a real
			// index column). Preserve the historical skip.
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
			// ADR-0047 tier (b): an UNcatalogued, extension-owned
			// access method (TimescaleD's `hypercore`, an in-house
			// AM, …) on a same-engine-PG / PG-backup run is carried
			// verbatim so the PG writer re-emits `USING <method>`.
			// Gated on amExtOwned so a core PG AM (btree/gin/…) never
			// gets pinned into Method (those round-trip via Kind, and
			// pinning the bareword would clutter diffs / regress the
			// Bug 47-style "only-non-core is explicit" property). The
			// catalogued-AM branch above already won for the ADR-0032
			// seven; this is the long-tail carry below the catalog.
			// !extensionAccessMethodRegistered excludes a CATALOGUED
			// extension's AM (pgvector ivfflat/hnsw): when catalogued
			// but not enabled it must DROP per the loud-failure default,
			// never be poached into Method by the verbatim tier (which
			// is uncatalogued-only, ADR-0047 §Scope). Without this the
			// v0.68.0 CI gate caught TestMigrate_PG_PgTrgm_NotEnabled_
			// DropsOpclass — the symmetric opclass case is fixed below.
			if idx.Method == "" && kind == ir.IndexKindUnspecified &&
				amExtOwned && !extensionAccessMethodRegistered(method) &&
				verbatimTierFor(r.verbatimPassthrough) == verbatimTierVerbatim {
				idx.Method = method
			}
			// Bug 19a: a partial index's WHERE predicate. pg_get_expr
			// renders it in PG dialect with table-qualified column refs;
			// tag the dialect so a cross-dialect target runs the
			// ADR-0016 translator (same policy as expression-index
			// bodies). Empty for a full index.
			if p := strings.TrimSpace(predicate); p != "" {
				idx.Predicate = p
				idx.PredicateDialect = dialectName
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
		//
		// pg_trgm extension (Tier 2 lite, no new column type, no new
		// AM) introduces operator classes (`gin_trgm_ops` /
		// `gist_trgm_ops`) that ride on core PG `gin` / `gist`. The
		// `idx.Method != ""` gate above only fires for extension-
		// introduced AMs, so we additionally consult
		// extensionOperatorClassEnabled here to capture opclasses
		// owned by an enabled extension even when the AM is core. The
		// double-gate preserves Bug 47's "no spurious opclass on
		// built-in indexes" property: a non-extension opclass (or one
		// whose owning extension wasn't enabled) is dropped, just
		// like before.
		// Bug 19b: ordinals beyond indnkeyatts are non-key INCLUDE
		// payload columns, not part of the index key. They carry a
		// real column name (never expressions — PG forbids expressions
		// in INCLUDE) and have no ordering/opclass semantics, so they
		// go to a separate slot. Flattening them into the key list
		// silently changes the index's comparison/uniqueness scope.
		if nKeyAtts > 0 && ord > nKeyAtts && colName != "" {
			idx.IncludeColumns = append(idx.IncludeColumns, colName)
			continue
		}

		col := ir.IndexColumn{Column: colName}
		if isExpr {
			col.Column = ""
			col.Expression = strings.TrimSpace(exprText)
			col.ExpressionDialect = dialectName
		}
		// Bug 19a: per-column ordering from pg_index.indoption. Bit 0
		// (value 1) = DESC; bit 1 (value 2) = NULLS FIRST. PG's
		// implicit defaults are NULLS LAST for ASC and NULLS FIRST for
		// DESC; only record NullsFirst when the stored ordering
		// deviates from that default so emitted DDL stays minimal and
		// diff-stable (the writer emits the clause only when non-nil).
		col.Desc = indOption&1 != 0
		nullsFirst := indOption&2 != 0
		defaultNullsFirst := col.Desc // ASC→NULLS LAST(false); DESC→NULLS FIRST(true)
		if nullsFirst != defaultNullsFirst {
			nf := nullsFirst
			col.NullsFirst = &nf
		}
		switch {
		case opclass != "" && idx.Method != "":
			col.OperatorClass = opclass
		case opclass != "" && extensionOperatorClassEnabled(opclass, r.enabledExtensions):
			col.OperatorClass = opclass
		case opclass != "" && opclassExtOwned &&
			!extensionOperatorClassRegistered(opclass) &&
			verbatimTierFor(r.verbatimPassthrough) == verbatimTierVerbatim:
			// ADR-0047 tier (b): an UNcatalogued, genuinely
			// extension-owned operator class (pg_depend deptype 'e')
			// on a same-engine-PG / PG-backup run. Carry it verbatim.
			// !extensionOperatorClassRegistered is load-bearing: a
			// CATALOGUED extension's opclass (pg_trgm's gin_trgm_ops,
			// PostGIS's gist_geometry_ops_2d, …) must NOT be poached by
			// the verbatim tier — it belongs to the ADR-0032 path
			// (enabled → case above; not-enabled → the drop+WARN case
			// below, the pre-existing Bug 47 / loud-failure default).
			// The verbatim tier is uncatalogued-only (ADR-0047 §Scope);
			// omitting this guard regressed TestMigrate_PG_PgTrgm_
			// NotEnabled_DropsOpclass (v0.68.0 CI gate, caught pre-tag).
			// so the same-engine writer re-emits `<col> <opclass>`.
			// opclassExtOwned is the Bug 47 invariant made literal:
			// only EXTENSION-owned opclasses ever set OperatorClass,
			// so a non-empty value passing through the IR is by
			// construction extension-introduced — which is exactly
			// what makes a verbatim backup correctly refuse a
			// cross-engine restore for free (the existing non-empty-
			// OperatorClass cross-engine signal). Core / default
			// opclasses have opclassExtOwned=false and stay
			// unpopulated, unchanged.
			col.OperatorClass = opclass
		case opclass != "" && extensionOperatorClassRegistered(opclass):
			// Operator-owned extension opclass on a core-PG access
			// method (pg_trgm's `gin_trgm_ops` / `gist_trgm_ops`),
			// but the owning extension is not in the operator's
			// `--enable-pg-extension` allowlist. Drop the opclass
			// from the IR per the loud-failure default, but emit
			// a WARN so the operator can attribute the inevitable
			// CREATE INDEX failure on the target to the missing
			// flag rather than to a sluice-side bug.
			ext := extensionOwningOperatorClass(opclass)
			slog.WarnContext(
				ctx,
				"postgres: schema reader: dropping extension-owned index opclass; pass --enable-pg-extension to preserve it",
				slog.String("index", idx.Name),
				slog.String("column", colName),
				slog.String("opclass", opclass),
				slog.String("extension", ext),
				slog.String("hint", "--enable-pg-extension "+ext),
			)
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

// populateExcludeConstraints fills in ExcludeConstraint lists for
// each table. ADR-0053: closes the silent-fidelity-loss class where
// pre-ADR sluice never queried contype='x' and therefore silently
// dropped every EXCLUDE constraint from the IR — landing target
// tables missing the semantic invariant.
//
// pg_get_constraintdef(oid, true) returns the canonical PG form
// (e.g. "EXCLUDE USING gist (col WITH &&) WHERE (...)" with optional
// DEFERRABLE/INITIALLY DEFERRED modifiers), MINUS the
// `ALTER TABLE ... ADD CONSTRAINT <name>` wrapper. The PG writer
// re-emits it inline as `CONSTRAINT <name> <Definition>` in the
// CREATE TABLE body (mirroring the CheckConstraint precedent —
// inline rather than post-create-ALTER).
//
// Same-engine PG → PG carries faithfully; cross-engine targets
// refuse loudly via checkCrossEngineSupportable (MySQL has no
// EXCLUDE equivalent).
func (r *SchemaReader) populateExcludeConstraints(ctx context.Context, tables map[string]*ir.Table) error {
	const q = `
		SELECT
			cl.relname AS table_name,
			con.conname,
			pg_catalog.pg_get_constraintdef(con.oid, true)
		FROM   pg_constraint con
		JOIN   pg_class      cl ON cl.oid = con.conrelid
		JOIN   pg_namespace  n  ON n.oid  = cl.relnamespace
		WHERE  n.nspname    = $1
		  AND  con.contype  = 'x'
		ORDER  BY cl.relname, con.conname`

	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var tableName, name, definition string
		if err := rows.Scan(&tableName, &name, &definition); err != nil {
			return err
		}
		t, ok := tables[tableName]
		if !ok {
			continue
		}
		if definition == "" {
			return fmt.Errorf(
				"postgres: pg_get_constraintdef returned empty for "+
					"EXCLUDE constraint %q on table %q — this is a sluice "+
					"bug; please report it",
				name, tableName,
			)
		}
		t.ExcludeConstraints = append(t.ExcludeConstraints, &ir.ExcludeConstraint{
			Name:       name,
			Definition: definition,
		})
	}
	return rows.Err()
}

// populateComments fills in table- and column-level comments
// (catalog Bug 19d). Dropping them silently is a loud-failure-tenet
// violation; PG → PG should round-trip them via COMMENT ON. One query
// for table comments (obj_description over pg_class) and one for
// column comments (col_description over pg_attribute), both scoped to
// the bound schema's ordinary tables.
func (r *SchemaReader) populateComments(ctx context.Context, tables map[string]*ir.Table) error {
	const tableQ = `
		SELECT cl.relname, obj_description(cl.oid, 'pg_class')
		FROM   pg_class     cl
		JOIN   pg_namespace n ON n.oid = cl.relnamespace
		WHERE  n.nspname  = $1
		  AND  cl.relkind = 'r'
		  AND  obj_description(cl.oid, 'pg_class') IS NOT NULL`

	rows, err := r.db.QueryContext(ctx, tableQ, r.schema)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var tableName, comment string
		if err := rows.Scan(&tableName, &comment); err != nil {
			return err
		}
		if t, ok := tables[tableName]; ok {
			t.Comment = comment
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	const colQ = `
		SELECT cl.relname, a.attname,
		       col_description(cl.oid, a.attnum)
		FROM   pg_class      cl
		JOIN   pg_namespace  n ON n.oid = cl.relnamespace
		JOIN   pg_attribute  a ON a.attrelid = cl.oid
		WHERE  n.nspname  = $1
		  AND  cl.relkind = 'r'
		  AND  a.attnum   > 0
		  AND  NOT a.attisdropped
		  AND  col_description(cl.oid, a.attnum) IS NOT NULL`

	crows, err := r.db.QueryContext(ctx, colQ, r.schema)
	if err != nil {
		return err
	}
	defer func() { _ = crows.Close() }()
	for crows.Next() {
		var tableName, colName, comment string
		if err := crows.Scan(&tableName, &colName, &comment); err != nil {
			return err
		}
		t, ok := tables[tableName]
		if !ok {
			continue
		}
		for _, col := range t.Columns {
			if col.Name == colName {
				col.Comment = comment
				break
			}
		}
	}
	return crows.Err()
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
//
// Bug 61: PostgreSQL renders multi-argument function-call defaults
// with a per-argument `::type` cast inside the call, e.g.
// `crypt('s'::text, gen_salt('bf'::text))`. A naive `LastIndex(s,"::")`
// finds the *innermost* cast (`'bf'::text`); because the suffix
// charset accepts `)`, the value truncates to
// `crypt('s'::text, gen_salt('bf'` — corrupting the IR and producing
// a SQLSTATE 42601 on the target. The cast PostgreSQL appends to the
// whole default sits at the *top level* (paren-depth 0, outside any
// string literal). So the scan only considers a `::` that is at
// depth 0 and not inside a single-quoted literal, and walks from the
// right so a genuine trailing cast still wins over an earlier
// top-level one. Casts nested inside the argument list are left in
// place — the cross-dialect translator already handles `'x'::text`
// fragments, and same-dialect PG→PG emits them verbatim (valid).
func stripTypeCast(s string) string {
	idx := topLevelCastIndex(s)
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

// topLevelCastIndex returns the byte offset of the last `::` operator
// that occurs at parenthesis-depth 0 and outside any single-quoted
// string literal, or -1 if there is none. PostgreSQL doubles embedded
// single quotes inside string literals (`'O”Brien'`), so a `'` only
// toggles literal state when it is not the second half of a doubled
// pair. `::` inside a literal or inside a function-argument list is
// skipped — those are not the cast PostgreSQL appended to the whole
// default expression (Bug 61).
func topLevelCastIndex(s string) int {
	depth := 0
	inStr := false
	last := -1
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inStr:
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++ // skip the doubled (escaped) quote
					continue
				}
				inStr = false
			}
		case c == '\'':
			inStr = true
		case c == '(':
			depth++
		case c == ')':
			if depth > 0 {
				depth--
			}
		case c == ':' && depth == 0 && i+1 < len(s) && s[i+1] == ':':
			last = i
			i++ // consume the second ':'
		}
	}
	return last
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
// IndexKind. Unknown methods become IndexKindUnspecified, which the
// writer dispatch translates back to PG's default (btree).
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
	case "spgist":
		return ir.IndexKindSPGist
	case "brin":
		return ir.IndexKindBRIN
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
