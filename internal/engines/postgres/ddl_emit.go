// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// emitOpts carries writer-derived flags that change how a few IR
// types render — whether the target database has the postgis
// extension installed, and the schema namespace user-defined types
// (enums) live in. Zero value (no PostGIS, empty TargetSchema)
// preserves the writer's pre-extension behaviour for all callers.
//
// TargetSchema is the operator-supplied `--target-schema` namespace
// (ADR-0031). When non-empty, every reference to a user-defined type
// (today: enums) is qualified with it — both the column-type ident
// (`"<schema>"."<typname>"`) and the `::cast` suffix on enum DEFAULT
// expressions. Without this, PG's parser searches the session's
// `search_path` for the bare type name, which doesn't include the
// per-source namespace, and CREATE TABLE fails with SQLSTATE 42704
// "type does not exist" (Bug 45). Empty TargetSchema keeps the
// pre-ADR-0031 unqualified shape — relies on the type living in
// `search_path`'s default schema.
type emitOpts struct {
	HasPostGIS   bool
	TargetSchema string

	// EnabledExtensions is the set of operator-opted-into PG
	// extensions (ADR-0032). Passed through every emit-helper so the
	// column-type and index-method renderers can dispatch through
	// pgExtensionCatalog without reaching back to the SchemaWriter.
	// nil / empty means "no extension passthrough" — ir.ExtensionType
	// columns surface a clear refusal naming the missing flag.
	EnabledExtensions map[string]bool
}

// emitColumnType returns the Postgres DDL fragment for a column type
// other than [ir.Enum]. Enum types are referenced by name in the
// column definition; the schema writer creates the CREATE TYPE
// statement separately and tracks the names — see [enumTypeName] and
// [emitColumnDef].
//
// Returns an error for IR types Postgres has no native form for, or
// for unsupported edge cases (Geometry without PostGIS, etc.).
func emitColumnType(t ir.Type, opts emitOpts) (string, error) {
	switch v := t.(type) {
	// ---- Boolean / numeric ----
	case ir.Boolean:
		return "BOOLEAN", nil
	case ir.Integer:
		return emitIntegerType(v), nil
	case ir.Decimal:
		if v.Unconstrained {
			// Bare arbitrary-precision numeric (catalog Bug 69). PG's
			// native unbounded form — emitting NUMERIC(0,0) here was the
			// 22023 hard-fail.
			return "NUMERIC", nil
		}
		return fmt.Sprintf("NUMERIC(%d,%d)", v.Precision, v.Scale), nil
	case ir.Float:
		if v.Precision == ir.FloatSingle {
			return "REAL", nil
		}
		return "DOUBLE PRECISION", nil

	// ---- Character / binary ----
	case ir.Char:
		if v.Length <= 0 {
			// Bug 107 (v0.92.1): CHAR(0) reaches sluice via the same
			// schema-reader path as VARCHAR(0) (MySQL allows both as
			// effectively-empty-marker columns); PG refuses both at
			// CREATE TABLE with `length for type char must be at
			// least 1` (SQLSTATE 22023). Loud-refuse at emit time
			// with the actionable recovery hint so the operator
			// doesn't get a raw PG error after partial-table copy.
			return "", fmt.Errorf(
				"postgres: column type CHAR(0) has no cross-engine PG translation " +
					"(PG refuses zero-length char/varchar at CREATE TABLE — SQLSTATE 22023). " +
					"This usually means a MySQL marker column; recovery: --type-override=TABLE.COL=text " +
					"(land it as PG TEXT) or --type-override=TABLE.COL=boolean (if the column is used as a flag). " +
					"See docs/operator/migrating-legacy-mysql.md for the legacy-MySQL migration story",
			)
		}
		return fmt.Sprintf("CHAR(%d)", v.Length), nil
	case ir.Varchar:
		if v.Length <= 0 {
			// Bug 107 (v0.92.1). MySQL allows VARCHAR(0) — useful only
			// as a marker (the column exists or it doesn't) — and a
			// surprising number of long-lived MySQL schemas (e.g. the
			// 20+ year-old WHMCS-shaped corpus) carry one or two.
			// PG refuses VARCHAR(0) at CREATE TABLE with `length for
			// type varchar must be at least 1` (SQLSTATE 22023). Pre-
			// v0.92.1 sluice forwarded the VARCHAR(0) into the PG
			// schema-apply DDL and crashed with that raw error AFTER
			// the cold-start preamble had already run. Loud-refusing
			// here at emit time keeps the error operator-actionable
			// and names the recovery flag.
			return "", fmt.Errorf(
				"postgres: column type VARCHAR(0) has no cross-engine PG translation " +
					"(PG refuses zero-length varchar at CREATE TABLE — SQLSTATE 22023). " +
					"VARCHAR(0) is a MySQL idiom for a marker column (exists/doesn't exist); recovery: " +
					"--type-override=TABLE.COL=text (land it as PG TEXT — the most common workaround) " +
					"or --type-override=TABLE.COL=boolean (if the column is used as a flag). " +
					"See docs/operator/migrating-legacy-mysql.md for the legacy-MySQL migration story",
			)
		}
		return fmt.Sprintf("VARCHAR(%d)", v.Length), nil
	case ir.Text:
		// Postgres TEXT is unbounded; the IR's TextSize buckets
		// don't translate. All sizes collapse to TEXT.
		return "TEXT", nil
	case ir.Binary, ir.Varbinary, ir.Blob:
		// Postgres has only one binary type, BYTEA.
		return "BYTEA", nil
	case ir.Bit:
		// Fixed-width / varying bit string. PG's bit(N) round-trips
		// MySQL BIT(N) losslessly (catalog Bug 62). A varying source
		// (PG `bit varying`) must emit BIT VARYING(N) — emitting fixed
		// BIT(N) loud-rejected any shorter value with SQLSTATE 22026
		// (catalog Bug 75, exposed once the value path became
		// faithful). BIT(1) never reaches here — the MySQL reader maps
		// the conventional single-bit column to ir.Boolean (→ BOOLEAN).
		if v.Varying {
			return fmt.Sprintf("BIT VARYING(%d)", v.Length), nil
		}
		return fmt.Sprintf("BIT(%d)", v.Length), nil

	// ---- Temporal ----
	case ir.Date:
		return "DATE", nil
	case ir.Time:
		base := emitWithPrecision("TIME", v.Precision)
		if v.WithTimeZone {
			return base + " WITH TIME ZONE", nil
		}
		return base, nil
	case ir.Interval:
		// PG-native duration type (the `--type-override col=interval`
		// target for a MySQL TIME duration). PG parses the carried
		// textual value ("838:59:59", "-12:00:00").
		return "INTERVAL", nil
	case ir.DateTime:
		return emitWithPrecision("TIMESTAMP", v.Precision), nil
	case ir.Timestamp:
		base := emitWithPrecision("TIMESTAMP", v.Precision)
		if v.WithTimeZone {
			return base + " WITH TIME ZONE", nil
		}
		return base, nil

	// ---- Structured ----
	case ir.JSON:
		if v.Binary {
			return "JSONB", nil
		}
		return "JSON", nil

	// ---- Identity / network (extension types) ----
	case ir.UUID:
		return "UUID", nil
	case ir.Inet:
		return "INET", nil
	case ir.Cidr:
		return "CIDR", nil
	case ir.Macaddr:
		return "MACADDR", nil

	// ---- Composite (extension) ----
	case ir.Array:
		elem, err := emitColumnType(v.Element, opts)
		if err != nil {
			return "", fmt.Errorf("postgres: array element: %w", err)
		}
		// Multi-dim arrays not modelled; we always emit single-dim.
		return elem + "[]", nil

	// MySQL SET has no native PG equivalent. Default policy is to
	// land it as TEXT[]; the membership constraint enforcing the
	// source's value list is emitted separately by emitTableDef as
	// a table-level CHECK so the constraint name is operator-friendly
	// (anonymous column-level CHECKs lose their identity in error
	// messages).
	case ir.Set:
		return "TEXT[]", nil

	// ---- Cases that need column context (handled by emitColumnDef) ----
	case ir.Enum:
		return "", errors.New("postgres: Enum DDL emission requires column context (table+column); use emitColumnDef")

	// Bug 113 round-trip carry (v0.95.2). When a column's type is a
	// DOMAIN, emit the operator-declared DOMAIN identifier (NOT the
	// base type's DDL spelling); the writer's Phase 1a' has already
	// emitted `CREATE DOMAIN <name> AS <base> CHECK (...)` before any
	// table reference, so the column's CREATE TABLE clause just
	// references the name. The target schema's `<target-schema>.<name>`
	// qualification is applied when opts.TargetSchema is non-empty
	// (mirrors the enum-type qualification rule). Empty TargetSchema
	// emits the unqualified ident — the pre-ADR-0031 shape relying on
	// the DOMAIN living in the search_path's default schema.
	case ir.Domain:
		if opts.TargetSchema == "" {
			return quoteIdent(v.Name), nil
		}
		return quoteIdent(opts.TargetSchema) + "." + quoteIdent(v.Name), nil

	// PostGIS-aware GEOMETRY emission. With the extension detected
	// at writer-open time, geometry(<subtype>, <srid>) carries the
	// IR's subtype and SRID into a typed PostGIS column. Without it,
	// the column is rejected — sluice doesn't try to install
	// extensions implicitly.
	case ir.Geometry:
		if !opts.HasPostGIS {
			return "", errors.New("postgres: GEOMETRY requires PostGIS; install with `CREATE EXTENSION postgis;` before running sluice")
		}
		typeName := "geometry"
		if v.IsGeography {
			typeName = "geography"
		}
		return fmt.Sprintf("%s(%s, %d)", typeName, postgisSubtypeName(v), v.SRID), nil

	// ADR-0032: PG → PG extension passthrough. ExtensionType columns
	// dispatch through pgExtensionCatalog so each extension's emit
	// helper renders the column-type DDL (`vector(384)` for pgvector
	// with dimension 384, etc.). The operator must have opted into
	// the extension via `--enable-pg-extension`; without it, surface
	// a clear refusal naming the flag rather than silently emitting
	// DDL the target may not be able to parse.
	case ir.ExtensionType:
		if !opts.EnabledExtensions[v.Extension] {
			return "", fmt.Errorf(
				"postgres: column type %s is owned by extension %q which "+
					"is not enabled; pass --enable-pg-extension %s "+
					"on this command to opt in (ADR-0032)",
				v.String(), v.Extension, v.Extension,
			)
		}
		return emitExtensionColumn(v)

	// ADR-0047: verbatim passthrough for an UNcatalogued PG extension
	// type. There is NO catalog dispatch by construction — the schema
	// reader captured the exact pg_catalog.format_type spelling and the
	// writer re-emits it literally in the column-type position. This
	// path is only ever populated for a provably-same-engine PG → PG
	// run or a PG backup (the lineage marker + the loud restore-time
	// engine gate enforce PG-restore-only); a cross-engine target never
	// receives ir.VerbatimType (checkCrossEngineSupportable refuses it
	// before any DDL emits — the loud-failure default is preserved).
	// A target PG instance missing the owning extension fails at its
	// own CREATE with a clear "type does not exist" — loud and
	// acceptable, consistent with ADR-0035's PostGIS-absent behaviour.
	case ir.VerbatimType:
		if v.Definition == "" {
			return "", errors.New(
				"postgres: ir.VerbatimType has an empty Definition — " +
					"cannot emit a column with no type spelling (ADR-0047; " +
					"this indicates a corrupt IR / backup manifest)",
			)
		}
		return v.Definition, nil
	}
	return "", fmt.Errorf("postgres: unknown IR type %T", t)
}

// emitIntegerType returns the Postgres integer type for an [ir.Integer],
// expanding unsigned widths to the next signed rank (Postgres has no
// unsigned integers) and using GENERATED IDENTITY for auto-increment.
//
// BIGINT UNSIGNED maps UNIFORMLY to PG BIGINT — for PK, FK-child, and
// standalone columns alike (Bug 11). The earlier policy split the two
// cases (NUMERIC(20,0) for plain, BIGINT for AUTO_INCREMENT) because
// PG's `GENERATED ... AS IDENTITY` is only valid on smallint/integer/
// bigint, never numeric — so an AUTO_INCREMENT PK *had* to stay BIGINT
// while a plain column widened to NUMERIC(20,0). That divergence made
// a `bigint unsigned` FK child (→ NUMERIC(20,0)) type-incompatible with
// the `bigint unsigned AUTO_INCREMENT` PK it referenced (→ BIGINT
// IDENTITY): ADD FOREIGN KEY failed SQLSTATE 42804. Since the universal
// Rails/Laravel/Django/Sequelize/Prisma schema is exactly
// `id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY` + `*_id BIGINT
// UNSIGNED` FKs, every default ORM schema hit this.
//
// Mapping uniformly to BIGINT makes the PK and FK types match by
// construction (no FK/schema-graph machinery) and keeps IDENTITY valid.
// The tradeoff: PG has no uint64, so values in (2^63-1, 2^64-1] are not
// representable — a deliberate, documented range narrowing (the
// industry-standard pragmatic mapping, cf. pgloader). It is surfaced
// LOUDLY via the bigint-unsigned narrowing notice at both `schema
// preview` and `migrate` preflight (see translate.UnsignedBigintNotices
// / Refuses... ) so it is never silent. Operators needing the full
// 2^64 range override per-column with `--type-override
// TABLE.COL=decimal(20,0)` (PG numeric(20,0), non-IDENTITY).
func emitIntegerType(i ir.Integer) string {
	width := effectiveWidth(i)
	typeName := postgresIntName(width)
	if i.AutoIncrement {
		return typeName + " GENERATED BY DEFAULT AS IDENTITY"
	}
	return typeName
}

// effectiveWidth returns the width Postgres should use for an integer
// column, widening one rank for unsigned values (so the original
// numeric range still fits). The 64-bit unsigned case maps to 64 (PG
// BIGINT) rather than a wider numeric — see [emitIntegerType] for the
// Bug 11 rationale and the loud range-narrowing notice that surfaces
// the (2^63, 2^64) loss to the operator.
func effectiveWidth(i ir.Integer) int8 {
	if !i.Unsigned {
		return i.Width
	}
	switch i.Width {
	case 8:
		return 16
	case 16, 24:
		return 32
	case 32, 64:
		return 64
	}
	return i.Width
}

// postgresIntName maps a signed integer width to the Postgres type
// name. 8/16 → SMALLINT (Postgres has no 8-bit type), 24/32 → INTEGER,
// 64 → BIGINT.
func postgresIntName(width int8) string {
	switch width {
	case 8, 16:
		return "SMALLINT"
	case 24, 32:
		return "INTEGER"
	case 64:
		return "BIGINT"
	}
	return "BIGINT"
}

// emitWithPrecision renders TYPE(N), or TYPE when precision is zero.
func emitWithPrecision(typeName string, precision int) string {
	if precision == 0 {
		return typeName
	}
	return fmt.Sprintf("%s(%d)", typeName, precision)
}

// quoteSQLString returns s wrapped in single quotes, with interior
// single quotes escaped by doubling.
func quoteSQLString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// emitDefault renders a DEFAULT clause body for column c. Returns
// ("", false) if no DEFAULT clause should be emitted.
//
// For [ir.DefaultExpression] with a non-empty Dialect tag that doesn't
// match this writer's dialect, the expression body is routed through
// [translateExprForPG] so cross-engine MySQL-spelled defaults (e.g.
// `(UUID())`, `(RAND() * 100)`, `DATE_ADD(...)`) translate to PG
// equivalents instead of failing loud on the target. v0.11.3 fix for
// Bugs 28/29/30; pre-fix the DEFAULT path was the only IR-expression
// path that bypassed the translator (generated + CHECK + index already
// routed through it).
//
// table and c are carried only so the SQLite-source loud-drop path
// ([translateDefaultExpr]) can name the table+column it dropped a
// non-portable DEFAULT from; table may be nil (the EmitColumnDef diff-
// renderer call site passes nil for non-enum columns).
func emitDefault(table *ir.Table, c *ir.Column, opts emitOpts) (string, bool) {
	switch v := c.Default.(type) {
	case nil, ir.DefaultNone:
		return "", false
	case ir.DefaultLiteral:
		return quoteSQLString(v.Value), true
	case ir.DefaultExpression:
		return translateDefaultExpr(table, c, v, opts)
	}
	return "", false
}

// translateDefaultExpr returns the DEFAULT-expression body to emit and
// whether a DEFAULT clause should be emitted at all, applying the cross-
// dialect translation pass when the IR's Dialect tag indicates a different
// source dialect. Same gating shape as [translateGeneratedExpr] and
// [translateCheckExpr]; an empty / matching dialect tag emits verbatim —
// same behaviour as before v0.11.3.
//
// The lone case that returns ok=false is a SQLite-source DEFAULT with no
// portable Postgres form (see below); every other path returns ok=true.
//
// The DEFAULT-expression context doesn't carry table-level bool-column
// information (defaults are evaluated per-row at INSERT time, not over
// other column values), so the [ExprContext] passed to the translator
// is the zero value — bool-idiom rewrites stay no-ops on this path.
func translateDefaultExpr(table *ir.Table, c *ir.Column, d ir.DefaultExpression, opts emitOpts) (string, bool) {
	if d.Dialect == bitLiteralDialect {
		// Bit-literal default on a bit(N) column (catalog Bug 62). The
		// reader emits the MySQL spelling `b'…'`; PG's bit-string
		// literal is `B'…'` (uppercase prefix). Value is identical;
		// only the surface prefix differs. Anything not in the expected
		// `b'…'` shape falls through verbatim (loud failure on target
		// beats a silent guess) — bitLiteralBits already validated the
		// digits at the read boundary. This special-case arm is checked
		// first so the dialect-guard below never sees it.
		if strings.HasPrefix(d.Expr, "b'") {
			return "B'" + d.Expr[2:], true
		}
		return d.Expr, true
	}
	// SQLite-source DEFAULT (D1/SQLite migration robustness, Chunk A). A
	// small, well-known set of portable SQLite "current instant" spellings
	// (datetime('now'), CURRENT_TIMESTAMP, …) translate to the matching PG
	// keyword; any other SQLite-only expression (julianday/strftime/
	// unixepoch/arbitrary, the double-quoted-string misfeature like
	// `"draft"`, …) is DROPPED with a loud warn rather than emitted
	// verbatim. Emitting verbatim aborted the ENTIRE migration at CREATE
	// TABLE — e.g. a Flyway/Goose history table's `installed_on TEXT NOT
	// NULL DEFAULT (datetime('now'))` failed PG with `function
	// datetime(unknown) does not exist`, and because create-tables failed
	// NO data loaded for ANY table. A DEFAULT is non-data metadata (it
	// only affects future inserts, never the migrated rows, which carry
	// explicit values), so dropping it with a named, loud warning is far
	// better than failing the whole migration — loud, never silent.
	if d.Dialect == sqliteSourceDialect {
		if pg, ok := translateSQLiteDefaultExpr(d.Expr); ok {
			return pg, true
		}
		slog.Warn(
			fmt.Sprintf(
				"postgres: dropped non-portable SQLite DEFAULT on %s.%s "+
					"(SQLite expression %q has no Postgres equivalent); the column has "+
					"no DEFAULT on the target — set one explicitly if your application "+
					"relies on it",
				quoteIdent(tableNameForLog(table)), quoteIdent(c.Name), d.Expr,
			),
			slog.String("table", tableNameForLog(table)),
			slog.String("column", c.Name),
			slog.String("expression", d.Expr),
		)
		return "", false
	}
	// Translate ONLY from the one engine this writer's translator accepts
	// (MySQL); self / untagged / any unknown dialect emits verbatim
	// (ADR-0133 §2). This closes the DEFAULT-path silent-mistranslate for
	// any other source: a non-MySQL default must NOT be fed through
	// translateExprForPG — it passes through and a non-portable default
	// fails loudly at target DDL time instead.
	if d.Dialect != translatableSourceDialect {
		return d.Expr, true
	}
	// Cross-dialect DEFAULT body (Bug 64, PG side). Before ADR-0045
	// this arm translated operator/function spellings but, unlike the
	// generated / CHECK / index sites, did NOT re-quote PG reserved-
	// word column references the source reader de-quoted — so a MySQL
	// source default referencing a column named `order` / `user` broke
	// CREATE TABLE with SQLSTATE 42601. Bring it onto the uniform
	// cross-dialect composition: requote(translate(expr)). Same-dialect
	// (and the bit-literal special case) returned above unchanged.
	return requotePGReservedIdents(
		translateExprForPG(d.Expr, ExprContext{EnabledPGExtensions: opts.EnabledExtensions}),
	), true
}

// bitLiteralDialect mirrors the MySQL reader's bit-literal dialect tag
// (mysql.bitLiteralDialect). Package-local copy: the postgres writer
// can't import the mysql engine package (engine packages are peers,
// wired only through the IR + registry). The IR's DefaultExpression
// dialect tag is the cross-package contract; this constant names the
// value the PG writer recognises on it (catalog Bug 62).
const bitLiteralDialect = "bit"

// setDefaultToArrayLiteral converts a MySQL-style comma-separated
// SET default ("a,b" or "" for the empty default) to a Postgres
// TEXT[] array literal expression. Used by emitColumnDef when the
// column's IR type is ir.Set, so the operator's source-side
// `DEFAULT 'a,b'` doesn't get dropped on the floor when the column
// translates to TEXT[].
//
// MySQL SET members are non-empty strings; the empty-set default
// is represented by an empty source-string, which maps to PG's
// '{}'::TEXT[] empty-array literal.
func setDefaultToArrayLiteral(commaSeparated string) string {
	if commaSeparated == "" {
		return "'{}'::TEXT[]"
	}
	parts := strings.Split(commaSeparated, ",")
	quoted := make([]string, len(parts))
	for i, p := range parts {
		quoted[i] = quoteSQLString(p)
	}
	return "ARRAY[" + strings.Join(quoted, ",") + "]::TEXT[]"
}

// emitSetCheckConstraint produces a CHECK-constraint fragment that
// enforces a SET column's value list at the table level:
//
//	CONSTRAINT "<table>_<column>_set" CHECK ("<column>" <@ ARRAY[...]::TEXT[])
//
// `<@` is PG's array-containment operator: the constraint passes
// when every element of the column value is a member of the
// declared SET. That matches MySQL SET semantics — any subset of
// the declared values is valid, including the empty array.
//
// Empty-values lists produce a CHECK against an empty array, which
// is degenerate (the column can only be the empty set) but
// well-formed; the source DDL was already strange in that case.
func emitSetCheckConstraint(tableName, columnName string, values []string) string {
	literal := "'{}'::TEXT[]"
	if len(values) > 0 {
		quoted := make([]string, len(values))
		for i, v := range values {
			quoted[i] = quoteSQLString(v)
		}
		literal = "ARRAY[" + strings.Join(quoted, ",") + "]::TEXT[]"
	}
	return fmt.Sprintf(
		"CONSTRAINT %s CHECK (%s <@ %s)",
		quoteIdent(setCheckName(tableName, columnName)),
		quoteIdent(columnName),
		literal,
	)
}

// setCheckName generates the CHECK-constraint name for a SET
// column. Same shape as enumTypeName so the two policies stay
// recognisable side-by-side.
func setCheckName(tableName, columnName string) string {
	return tableName + "_" + columnName + "_set"
}

// emitGeneratedEnumCheckConstraint produces a table-level CHECK
// fragment enforcing the value-list of an enum-typed STORED
// generated column. Bug 25's workaround: PG rejects
// `(body)::enum_type` inside a STORED generated column because
// `enum_in()` is STABLE not IMMUTABLE. Sluice sidesteps by
// emitting the column as TEXT and adding this CHECK so the value-
// set guarantee survives. The CHECK is `column_name IN ('a','b',...)`
// — straightforward IN-list, no enum dependency.
//
// Empty values lists produce a CHECK against an empty IN-list,
// which PG treats as always-false (the column can hold no value).
// That matches the source's enum-with-no-values shape, which is
// already degenerate.
func emitGeneratedEnumCheckConstraint(tableName, columnName string, values []string) string {
	if len(values) == 0 {
		return fmt.Sprintf(
			"CONSTRAINT %s CHECK (false)",
			quoteIdent(generatedEnumCheckName(tableName, columnName)),
		)
	}
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = quoteSQLString(v)
	}
	return fmt.Sprintf(
		"CONSTRAINT %s CHECK (%s IN (%s))",
		quoteIdent(generatedEnumCheckName(tableName, columnName)),
		quoteIdent(columnName),
		strings.Join(quoted, ","),
	)
}

// generatedEnumCheckName generates the CHECK-constraint name for a
// generated enum column. The `_enum_chk` suffix distinguishes it
// from setCheckName's `_set` suffix and from enumTypeName's `_enum`
// suffix, keeping the three policies easy to tell apart in pg_dump
// output.
func generatedEnumCheckName(tableName, columnName string) string {
	return tableName + "_" + columnName + "_enum_chk"
}

// emitColumnDef returns the full DDL fragment for a single column,
// suitable for inclusion in a CREATE TABLE column list:
//
//	"name" TYPE [NOT NULL] [DEFAULT ...]
//
// For Enum columns, table is consulted for the per-column generated
// type name. Other IR types ignore the table argument.
func emitColumnDef(table *ir.Table, c *ir.Column, opts emitOpts) (string, error) {
	if c == nil {
		return "", errors.New("postgres: emitColumnDef: column is nil")
	}

	var typeStr string
	if enum, isEnum := c.Type.(ir.Enum); isEnum {
		if table == nil {
			return "", fmt.Errorf("postgres: emitColumnDef: Enum column %q requires a table context", c.Name)
		}
		// Bug 25: enum-typed STORED generated columns can't reference
		// the enum type — the cast `(body)::enum_type` calls
		// `enum_in()` which is STABLE, not IMMUTABLE, and PG's
		// generated-column body must be IMMUTABLE. We sidestep by
		// emitting the column as TEXT (no enum type, no cast) and
		// letting emitTableDef append a table-level CHECK constraint
		// that enforces the value list. Loses the named enum type but
		// always works; matches sluice's "translate, don't wrap in
		// target-side functions" philosophy. Same pattern as the SET
		// → TEXT[] + CHECK fallback in emitSetCheckConstraint.
		if c.IsGenerated() {
			typeStr = "TEXT"
		} else {
			typeStr = qualifiedEnumTypeRef(enum, opts.TargetSchema, table.Name, c.Name)
		}
	} else {
		var err error
		typeStr, err = emitColumnType(c.Type, opts)
		if err != nil {
			return "", fmt.Errorf("postgres: column %q: %w", c.Name, err)
		}
	}

	var sb strings.Builder
	sb.WriteString(quoteIdent(c.Name))
	sb.WriteByte(' ')
	sb.WriteString(typeStr)
	if c.IsGenerated() {
		// SQLite source (ADR-0133 follow-up): a generated column is
		// DATA-load-bearing (its value is COMPUTED on the target), so a body
		// with no provably-portable translation must be refused LOUDLY here —
		// emitting it verbatim is unsafe because PG may silently accept a
		// syntactically-valid but semantically-divergent body and compute a
		// wrong value.
		if err := refuseNonPortableSQLiteExprPG("generated column", c.Name, c.GeneratedExpr, c.GeneratedExprDialect); err != nil {
			return "", err
		}
		// Postgres only supports STORED generated columns. If a
		// MySQL source provides a VIRTUAL column (GeneratedStored=
		// false), the closest correct PG representation is STORED
		// — the invariant survives but the storage tradeoff
		// changes (STORED takes disk; VIRTUAL doesn't). Emit a
		// warning so the operator sees the silent promotion.
		// Source-engine VIRTUAL columns are rare on production
		// schemas; refusing here would force operators into the
		// per-column mappings hook for what's almost always a
		// benign translation.
		if !c.GeneratedStored {
			slog.Warn(
				"postgres: promoting source-engine VIRTUAL generated column to STORED (postgres has no VIRTUAL support)",
				slog.String("table", tableNameForLog(table)),
				slog.String("column", c.Name),
			)
		}
		sb.WriteString(" GENERATED ALWAYS AS (")
		body := translateGeneratedExpr(c, table, opts)
		// Bug 25 (v0.10.1): for enum-typed generated columns we
		// emit as TEXT (above) and rely on a table-level CHECK
		// constraint for the value-list enforcement — no cast here.
		// For non-enum generated columns the body emits verbatim.
		// (The v0.9.2 enum-cast wrapper that used to live here is
		// gone because it triggered PG's "generation expression is
		// not immutable" error: enum_in is STABLE not IMMUTABLE.)
		sb.WriteString(body)
		sb.WriteString(") STORED")
	}
	if !c.Nullable {
		sb.WriteString(" NOT NULL")
	}
	// DEFAULT is mutually exclusive with GENERATED in Postgres — the
	// parser rejects the combination. Generated columns arrive with
	// Default = DefaultNone from the schema reader, so emitDefault
	// returns ok=false and the clause is skipped naturally; no
	// special case needed here.
	if dflt, ok := emitDefault(table, c, opts); ok {
		sb.WriteString(" DEFAULT ")
		// SET columns translate the comma-separated MySQL literal
		// to a TEXT[] array literal so the source DEFAULT survives
		// the type rewrite. Other default shapes (DefaultExpression,
		// DefaultNone) flow through emitDefault unchanged.
		if _, isSet := c.Type.(ir.Set); isSet {
			if lit, ok := c.Default.(ir.DefaultLiteral); ok {
				sb.WriteString(setDefaultToArrayLiteral(lit.Value))
			} else {
				sb.WriteString(dflt)
			}
		} else {
			sb.WriteString(dflt)
		}
		// Postgres enum columns need an explicit type cast on the
		// default — without it, CREATE TABLE rejects with "column X
		// is of type Y_enum but default expression is of type text"
		// (Bug 23). The cast applies whenever the default's emitted
		// form is shape-equivalent to a string literal; that covers
		// both DefaultLiteral (the common path) and DefaultExpression
		// for the MySQL `DEFAULT ('pending')` parenthesised form,
		// which information_schema reports with EXTRA=
		// "DEFAULT_GENERATED" → DefaultExpression. Anything that
		// isn't a simple quoted string (function calls, true
		// expressions like CURRENT_TIMESTAMP) is left alone — the
		// cast wouldn't be safe there and the loud-failure tenet
		// surfaces the operator-driven gap.
		if enum, isEnum := c.Type.(ir.Enum); isEnum && enumDefaultShouldCast(c.Default, dflt) {
			sb.WriteString("::")
			sb.WriteString(qualifiedEnumTypeRef(enum, opts.TargetSchema, table.Name, c.Name))
		}
	}
	return sb.String(), nil
}

// tableNameForLog returns a non-empty table name for log lines, even
// when the caller hasn't supplied a table context. Used by the
// generated-column writer's STORED-promotion warning so the log line
// stays useful for the only callsite that bypasses table context
// (defensive — emitColumnDef's contract says table is required for
// Enum, but generated columns can land on non-Enum types).
func tableNameForLog(t *ir.Table) string {
	if t == nil {
		return "<unknown>"
	}
	return t.Name
}

// enumDefaultShouldCast reports whether the cast suffix should be
// appended to an enum column's emitted default (Bug 23). Returns
// true for DefaultLiteral (always) and for DefaultExpression whose
// emitted form is a single-quoted string literal (the MySQL
// `DEFAULT ('pending')` parenthesised form, which the schema reader
// translates to DefaultExpression because information_schema's EXTRA
// column carries DEFAULT_GENERATED).
//
// `emitted` is the rendered form returned by [emitDefault] — for a
// DefaultLiteral that's already `'pending'`; for a DefaultExpression
// it's the raw expression text. We test the rendered form so the
// caller doesn't have to thread both shapes through.
func enumDefaultShouldCast(d ir.DefaultValue, emitted string) bool {
	if _, ok := d.(ir.DefaultLiteral); ok {
		return true
	}
	if _, ok := d.(ir.DefaultExpression); !ok {
		return false
	}
	// String-literal shape: opens with `'`, closes with `'`, contains
	// no unescaped intermediate quotes that would extend the literal.
	// The quote-tolerant check covers `'O''Brien'` (an enum value
	// with an apostrophe — rare but valid). Anything else (e.g.
	// `CURRENT_TIMESTAMP`, `(NOW())`, function calls) returns false.
	s := strings.TrimSpace(emitted)
	if len(s) < 2 || s[0] != '\'' || s[len(s)-1] != '\'' {
		return false
	}
	// Walk the body checking that any embedded `'` characters are
	// doubled. The walker matches the SQL standard escaping rule.
	body := s[1 : len(s)-1]
	for i := 0; i < len(body); i++ {
		if body[i] != '\'' {
			continue
		}
		if i+1 >= len(body) || body[i+1] != '\'' {
			return false
		}
		i++ // skip the doubled quote
	}
	return true
}

// enumTypeName generates a deterministic Postgres enum type name for
// the given table+column pair. Two columns with the same enum values
// in different tables get separate types — slight inefficiency, but
// no risk of name collisions or unintended sharing. This is the
// fallback used only when the IR enum carries no source-side type
// name (a MySQL source — MySQL enums are column-inline with no type
// identity); see [resolveEnumTypeName].
func enumTypeName(tableName, columnName string) string {
	return tableName + "_" + columnName + "_enum"
}

// resolveEnumTypeName returns the Postgres type name to use for an
// enum column. A same-engine PG source carries the original type name
// on [ir.Enum.TypeName]; preserving it verbatim keeps casts,
// shared-enum tables, and app code referencing the type by name
// working (catalog Bug 19c). A MySQL source has no enum type identity,
// so TypeName is empty and we fall back to the deterministic
// table+column synthesis.
func resolveEnumTypeName(enum ir.Enum, tableName, columnName string) string {
	if enum.TypeName != "" {
		return enum.TypeName
	}
	return enumTypeName(tableName, columnName)
}

// qualifiedEnumTypeRef returns the schema-qualified enum type
// reference for use as a column type ident or `::cast` suffix.
//
// When targetSchema is non-empty, returns `"<schema>"."<typname>"`
// — required under ADR-0031's `--target-schema=NAME` (Bug 45).
// PG's parser searches the session's `search_path` for unqualified
// type idents; per-source schemas (`customer_svc`, `billing_svc`)
// aren't in `search_path`, so the bare ident fails with SQLSTATE
// 42704 "type does not exist". The qualifier mirrors what
// emitCreateEnumType already emits on the type-creation side, so
// CREATE TABLE / DEFAULT cast resolve to the same type the
// CREATE TYPE statement just declared.
//
// When targetSchema is empty, returns the unqualified ident — the
// pre-ADR-0031 shape, relying on the type living in `search_path`'s
// default schema (the DSN's default, typically `public`).
func qualifiedEnumTypeRef(enum ir.Enum, targetSchema, tableName, columnName string) string {
	name := resolveEnumTypeName(enum, tableName, columnName)
	if targetSchema == "" {
		return quoteIdent(name)
	}
	return quoteIdent(targetSchema) + "." + quoteIdent(name)
}

// maxPGIdentifierLen is PostgreSQL's NAMEDATALEN-1 = 63 bytes ceiling
// on identifier length. Identifiers longer than this are silently
// truncated at CREATE time — which is the failure mode behind
// GitHub issue #26: two prepended index names that share their first
// 63 chars truncate to the same PG identifier and the second CREATE
// fires `SQLSTATE 42P07: relation "<truncated_name>" already exists`.
const maxPGIdentifierLen = 63

// indexNamingConventionPrefixes is the set of index-naming
// convention prefixes that operator schemas use to mean "this index
// is already scoped to a single table." When `pgIndexName` sees a
// source name shaped like `ix_<table>_<rest>` / `idx_<table>_<rest>`
// / `fk_<table>_<rest>` / `uq_<table>_<rest>` / etc., the table-name
// portion is already encoded; sluice prepending another table prefix
// would (a) double the table-name presence in the identifier and (b)
// push past 63 chars on long table names.
//
// Coverage drawn from the conventions of SQLAlchemy/Alembic, Django,
// Rails AR / ActiveRecord, Hibernate, Diesel, and operator-written
// hand schemas. The list is intentionally generous on the read side
// — false positives (treating an unconventional name as already-
// scoped) are emit-verbatim, which is the same behavior the explicit
// `<table>_` prefix check already handles.
var indexNamingConventionPrefixes = []string{
	"ix_", "idx_", "uix_", "uidx_", "uniq_", "uq_",
	"pk_", "fk_", "chk_", "ck_",
}

// pgIndexName disambiguates a source-side index name against the
// schema-scoped Postgres namespace. The rule (after GitHub #26):
//
//  1. If sourceName already starts with `<tableName>_`, emit verbatim.
//  2. If sourceName matches a known convention prefix
//     (`ix_<table>_`, `idx_<table>_`, etc., per
//     [indexNamingConventionPrefixes]) AND the convention-prefix +
//     tableName segment matches, emit verbatim. This covers
//     real-world SQLAlchemy / Alembic / Rails / Django shapes where
//     the table name is already encoded in the index name.
//  3. Otherwise, prepend `<tableName>_`. If the result would exceed
//     PG's 63-char NAMEDATALEN limit, emit sourceName verbatim
//     instead — the operator's source-declared name fits PG (it's <=
//     64 chars on MySQL or natively under 63 on PG), and avoiding
//     the truncation collision is the load-bearing concern. The
//     historical reason for prefixing (sibling-table name disambig)
//     is preserved for short names; long names sacrifice it.
//
// Pre-v0.49.0 emitted `<tableName>_<sourceName>` unconditionally
// when (1) didn't match, causing the GitHub #26 truncation collision
// on long names that share their first 63 chars after prepend.
func pgIndexName(tableName, sourceName string) string {
	if sourceName == "" {
		return ""
	}
	prefix := tableName + "_"
	if strings.HasPrefix(sourceName, prefix) {
		return sourceName
	}
	// Convention-prefix detection: if source name is shaped like
	// `<convention_prefix><tableName>_<rest>`, treat as already
	// table-scoped.
	for _, conv := range indexNamingConventionPrefixes {
		if strings.HasPrefix(sourceName, conv+tableName+"_") {
			return sourceName
		}
		// Edge case: source name is exactly `<conv><tableName>`
		// (table name suffix with no trailing column part) — still
		// already-table-scoped.
		if sourceName == conv+tableName {
			return sourceName
		}
	}
	full := prefix + sourceName
	if len(full) > maxPGIdentifierLen {
		// Prepending would overflow → emit verbatim. The
		// historical disambiguation against sibling-table indexes
		// (`idx_fk_film_id` on multiple tables) is sacrificed for
		// the truncation-collision-free path, which is the more
		// urgent failure mode.
		return sourceName
	}
	return full
}

// validatePGIndexName refuses an effective PG index identifier that
// exceeds PostgreSQL's 63-byte NAMEDATALEN-1 ceiling (roadmap item 43,
// the `pgIndexName` >63-byte latent silent-collapse class flagged in the
// Bug #114 RCA).
//
// Why this is a value-fidelity issue: PG silently TRUNCATES any
// identifier longer than 63 BYTES at CREATE time. Two distinct indexes
// whose names share the same first 63 bytes both truncate to the same
// catalog identifier — and because sluice emits `CREATE INDEX IF NOT
// EXISTS` ([SchemaWriter.buildOneIndex]), the second create silently
// no-ops. Result: an index the source declared is silently MISSING on
// the target. No error, no row-data loss, but a missing index is a
// silent schema-fidelity loss. Per the "contain Postgres complexity,
// surface explicitly" tenet the safe behaviour is to fail loudly here
// rather than auto-truncate or auto-rename (either would silently
// transform the operator's schema).
//
// The limit is BYTES, not runes: PG counts bytes against NAMEDATALEN, so
// a multibyte UTF-8 name whose rune count is <=63 but whose byte length
// is >63 still truncates. Go's len() on a string is the byte count, so
// the check is byte-correct as written.
//
// effectiveName is the identifier pgIndexName resolved (either the
// table-prefixed form or a verbatim source name); sourceName and
// tableName are carried only for an actionable error message. Names
// <=63 bytes pass through unchanged (no behaviour change).
//
// Note on same-prefix collisions among <=63-byte names: two index names
// that both already fit 63 bytes cannot share a 63-byte truncation
// prefix unless they are byte-identical for their full length — in which
// case they are the SAME catalog identifier and the IR carries one, not
// two (index names are unique per table on every source engine sluice
// reads). The only way two DISTINCT indexes collide on a 63-byte prefix
// is if at least one exceeds 63 bytes, which this refusal already
// catches. So the >63-byte refusal is sufficient to close the collision
// class; no separate same-prefix check is needed.
func validatePGIndexName(effectiveName, sourceName, tableName string) error {
	if len(effectiveName) <= maxPGIdentifierLen {
		return nil
	}
	return fmt.Errorf(
		"postgres: index name %q on table %q is %d bytes, exceeding PostgreSQL's %d-byte identifier limit; "+
			"PostgreSQL would silently truncate it, risking a collision with another index that shares the "+
			"same first %d bytes (the second CREATE INDEX IF NOT EXISTS would silently no-op, leaving the "+
			"index missing) — shorten or alias the source index name (source name: %q) to %d bytes or fewer",
		effectiveName, tableName, len(effectiveName), maxPGIdentifierLen, maxPGIdentifierLen, sourceName, maxPGIdentifierLen,
	)
}

// emitCreateDomainType produces a CREATE DOMAIN statement for a single
// DOMAIN. Bug 113 round-trip carry (v0.95.2). Emits the schema-
// qualified DOMAIN name + the base type's DDL spelling + one CHECK
// clause per recorded constraint. Source ordering of CHECKs is
// preserved verbatim — PG evaluates them in catalog order, which is
// the source's declaration order after read-back.
//
// Returns a non-nil error when the base type can't be rendered
// (e.g. a nested DOMAIN whose base is itself a USER-DEFINED type
// the IR doesn't model — exceedingly rare in practice).
func emitCreateDomainType(d ir.Domain, schema string, opts emitOpts) (string, error) {
	if d.BaseType == nil {
		return "", fmt.Errorf("postgres: emitCreateDomainType: DOMAIN %q has nil BaseType", d.Name)
	}
	baseDDL, err := emitColumnType(d.BaseType, opts)
	if err != nil {
		return "", fmt.Errorf("postgres: emitCreateDomainType: DOMAIN %q base type: %w", d.Name, err)
	}
	out := fmt.Sprintf(
		"CREATE DOMAIN %s.%s AS %s",
		quoteIdent(schema),
		quoteIdent(d.Name),
		baseDDL,
	)
	for _, c := range d.Checks {
		// pg_get_constraintdef-stripped Body holds the bare expression
		// (no `CHECK (...)` wrapper). Re-wrap on emit; preserve the
		// constraint name when the source declared one.
		if c.Name != "" {
			out += fmt.Sprintf(" CONSTRAINT %s CHECK (%s)", quoteIdent(c.Name), c.Body)
		} else {
			out += fmt.Sprintf(" CHECK (%s)", c.Body)
		}
	}
	return out + ";", nil
}

// emitCreateEnumType produces a CREATE TYPE statement for a single
// enum, named by the table+column generator. Values are quoted and
// comma-separated.
func emitCreateEnumType(enum ir.Enum, schema, tableName, columnName string) string {
	parts := make([]string, len(enum.Values))
	for i, v := range enum.Values {
		parts[i] = quoteSQLString(v)
	}
	return fmt.Sprintf(
		"CREATE TYPE %s.%s AS ENUM (%s);",
		quoteIdent(schema),
		quoteIdent(resolveEnumTypeName(enum, tableName, columnName)),
		strings.Join(parts, ", "),
	)
}

// emitCommentStatements returns the COMMENT ON TABLE / COMMENT ON
// COLUMN statements for a table's table-level and column-level
// comments (catalog Bug 19d). PG models comments as standalone
// catalog statements (unlike MySQL's inline COMMENT clause), so they
// can't ride in the CREATE TABLE body. `COMMENT ON` is idempotent
// (it overwrites), so re-running the schema-write phase is safe.
// Returns nil when the table carries no comments — the common case.
func emitCommentStatements(schema string, table *ir.Table) []string {
	if table == nil {
		return nil
	}
	qualified := quoteIdent(schema) + "." + quoteIdent(table.Name)
	var out []string
	if table.Comment != "" {
		out = append(out, fmt.Sprintf(
			"COMMENT ON TABLE %s IS %s;",
			qualified, quoteSQLString(table.Comment),
		))
	}
	for _, col := range table.Columns {
		if col == nil || col.Comment == "" {
			continue
		}
		out = append(out, fmt.Sprintf(
			"COMMENT ON COLUMN %s.%s IS %s;",
			qualified, quoteIdent(col.Name), quoteSQLString(col.Comment),
		))
	}
	return out
}

// inlineUniqueKeyForCopy returns the non-null UNIQUE index that
// [emitTableDef] promotes inline as a `CONSTRAINT <name> UNIQUE (cols)`
// at CREATE TABLE time for a PK-LESS table, so the cold-start VStream
// COPY's idempotent writer has a unique index for `ON CONFLICT (cols)`
// to infer against while rows land (Bug 125 cross-engine symmetry).
// Returns nil when:
//
//   - the table has a PRIMARY KEY (the PK already serves as the upsert
//     conflict key — no promotion needed), or
//   - no non-null UNIQUE index qualifies (a truly-keyless table; the
//     idempotent writer refuses it loudly at copy time).
//
// The index it picks is exactly the one [effectiveUpsertKeyColumns]
// keys the upsert on (both call [pickNonNullUniqueIndex]), so the
// promoted-inline key and the writer's conflict key are the same by
// construction. The index-build phases skip whatever this returns (via
// [inlineSkipIndexNames]) so they don't re-create it as a duplicate.
//
// Unlike MySQL, PG has no auto-increment-supporting-index special case
// (PG identity columns don't require a backing unique index at CREATE
// time), so there is no double-create guard here.
func inlineUniqueKeyForCopy(table *ir.Table) *ir.Index {
	if table == nil || table.PrimaryKey != nil {
		return nil
	}
	return pickNonNullUniqueIndex(table)
}

// emitTableDef produces a CREATE TABLE statement with columns,
// table-level CHECK constraints (for SET columns), and the primary
// key inline. The table is schema-qualified.
//
// Body parts assembled into a single comma-separated block so the
// trailing-comma logic stays in one place — MySQL SET columns add
// CHECK lines that complicate the per-line rule.
func emitTableDef(schema string, table *ir.Table, opts emitOpts) (string, error) {
	if table == nil {
		return "", errors.New("postgres: emitTableDef: table is nil")
	}
	if len(table.Columns) == 0 {
		return "", fmt.Errorf("postgres: emitTableDef: table %q has no columns", table.Name)
	}

	parts := make([]string, 0, len(table.Columns)+2)

	for _, col := range table.Columns {
		def, err := emitColumnDef(table, col, opts)
		if err != nil {
			return "", err
		}
		parts = append(parts, def)
	}

	// SET columns get a table-level CHECK constraint enforcing the
	// declared value list. Emitted in declaration order so the DDL
	// is stable across runs.
	for _, col := range table.Columns {
		set, ok := col.Type.(ir.Set)
		if !ok {
			continue
		}
		parts = append(parts, emitSetCheckConstraint(table.Name, col.Name, set.Values))
	}

	// Bug 25: enum-typed STORED generated columns emit as TEXT (see
	// emitColumnDef) and rely on a table-level CHECK to enforce the
	// value list. Same shape as SET columns above. Non-generated
	// enum columns keep using the native PG enum type and don't
	// need this CHECK — the type itself constrains the values.
	for _, col := range table.Columns {
		enum, ok := col.Type.(ir.Enum)
		if !ok || !col.IsGenerated() {
			continue
		}
		parts = append(parts, emitGeneratedEnumCheckConstraint(table.Name, col.Name, enum.Values))
	}

	// User-declared CHECK constraints emit inline alongside the
	// generated SET checks. Order is the IR's preserved source order
	// so the target's pg_dump shape stays diffable against the source.
	for _, chk := range table.CheckConstraints {
		clause, err := emitCheckConstraint(chk, table, opts)
		if err != nil {
			return "", fmt.Errorf("table %q: %w", table.Name, err)
		}
		parts = append(parts, clause)
	}

	// ADR-0053: EXCLUDE constraints emit inline alongside CHECKs.
	// Each ir.ExcludeConstraint.Definition is the pg_get_constraintdef
	// body (no `ALTER TABLE ... ADD CONSTRAINT <name>` wrapper);
	// prefix with `CONSTRAINT <name>` to produce a valid CREATE TABLE
	// clause. Empty Definition is a sluice-bug condition the reader
	// should have refused — surface loudly if it ever reaches the
	// writer.
	for _, ex := range table.ExcludeConstraints {
		if ex.Definition == "" {
			return "", fmt.Errorf(
				"postgres: ExcludeConstraint %q on %s has empty "+
					"Definition — this is a sluice bug; the reader should "+
					"have refused upstream",
				ex.Name, table.Name,
			)
		}
		parts = append(parts, fmt.Sprintf(
			"CONSTRAINT %s %s", quoteIdent(ex.Name), ex.Definition,
		))
	}

	if table.PrimaryKey != nil {
		parts = append(parts, "PRIMARY KEY "+emitIndexColumnList(table.PrimaryKey.Columns, opts))
	}

	// Bug 125 cross-engine symmetry: for a PK-less table, promote a
	// non-null UNIQUE key inline as a CONSTRAINT so the cold-start
	// VStream COPY's idempotent `ON CONFLICT (cols)` has a real matching
	// unique index to infer against while rows land. PG (unlike MySQL's
	// ON DUPLICATE KEY UPDATE) cannot infer a conflict target without a
	// physically-present unique index over exactly these columns. The
	// index-build phases skip this same index ([inlineSkipIndexNames]) so
	// it isn't re-created as a duplicate. nil when a PK exists or no
	// non-null UNIQUE qualifies.
	if idx := inlineUniqueKeyForCopy(table); idx != nil {
		name := pgIndexName(table.Name, idx.Name)
		// Roadmap item 43: the inline unique-key constraint name lands
		// in the same 63-byte-limited PG identifier namespace as a
		// CREATE INDEX name, so refuse a >63-byte name here too.
		if err := validatePGIndexName(name, idx.Name, table.Name); err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf(
			"CONSTRAINT %s UNIQUE %s",
			quoteIdent(name),
			emitIndexColumnList(idx.Columns, opts),
		))
	}

	var sb strings.Builder
	// IF NOT EXISTS keeps schema phase 1 idempotent: re-running
	// CreateTablesWithoutConstraints during a resume is a no-op when
	// the table is already there. Postgres has supported this since
	// 9.1 (every version sluice targets).
	sb.WriteString("CREATE TABLE IF NOT EXISTS ")
	sb.WriteString(quoteIdent(schema))
	sb.WriteByte('.')
	sb.WriteString(quoteIdent(table.Name))
	sb.WriteString(" (\n")
	for i, p := range parts {
		sb.WriteString("  ")
		sb.WriteString(p)
		if i < len(parts)-1 {
			sb.WriteByte(',')
		}
		sb.WriteByte('\n')
	}
	sb.WriteString(");")
	return sb.String(), nil
}

// emitIndexColumnList renders a parenthesised, comma-separated list
// of index columns. Postgres doesn't support prefix-length on indexes
// the way MySQL does, so c.Length is ignored (lossy if the source
// used it; documented).
//
// Functional/expression entries (Expression non-empty, Column empty)
// render as `(expression_text)` — Postgres expression-index syntax
// requires the expression to be parenthesised, which combined with
// the outer column-list parens produces the canonical double-parens
// shape `((lower(email)))`. The expression body runs through the
// ADR-0016 translator when the source dialect tag differs from PG,
// so a MySQL-source `json_unquote(json_extract(j,'$.k'))` index
// rewrites to `(j->>'k')` instead of failing at CREATE INDEX.
// Same-dialect / untagged expressions pass through verbatim.
func emitIndexColumnList(cols []ir.IndexColumn, opts emitOpts) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		var entry string
		if c.Expression != "" {
			entry = "(" + translateIndexExpr(c, opts) + ")"
		} else {
			entry = quoteIdent(c.Column)
		}
		// Per-column operator class — populated by the schema reader
		// for (a) extension-introduced access methods that need an
		// explicit opclass (pgvector's hnsw rejects the index at
		// CREATE without one), (b) extension-owned opclasses on core
		// AMs (pg_trgm's gin_trgm_ops / gist_trgm_ops), (c) Bug 115
		// (v0.95.0) — operator-explicit NON-DEFAULT core opclasses
		// (text_pattern_ops / varchar_pattern_ops / jsonb_path_ops
		// and similar) that pre-fix dropped silently into the
		// default opclass on round-trip. Default-opclass cases on
		// built-in types leave this empty and emit nothing extra.
		if c.OperatorClass != "" {
			entry += " " + c.OperatorClass
		}
		if c.Desc {
			entry += " DESC"
		}
		// NULLS FIRST/LAST — only set by the reader when the stored
		// ordering deviates from PG's default for the sort direction
		// (NULLS LAST for ASC, NULLS FIRST for DESC), so emitting it
		// here always reflects a genuine non-default and keeps DDL
		// diff-stable for the common case.
		if c.NullsFirst != nil {
			if *c.NullsFirst {
				entry += " NULLS FIRST"
			} else {
				entry += " NULLS LAST"
			}
		}
		parts[i] = entry
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// translateIndexExpr returns the index expression to emit, applying
// the cross-dialect translation pass when the IR's dialect tag
// indicates a different source dialect (see ADR-0016). An empty /
// matching dialect tag emits verbatim — same behaviour as before
// the translation layer applied to indexes.
//
// The bool-idiom rewrite (v0.8.0) is gated on a per-table column
// context that index expressions don't carry; index expressions are
// rendered in [emitCreateIndex] where the table is in scope, but the
// list helper isn't. Since index expressions almost never reference
// bool-mapped columns directly (they reference columns to derive
// search keys), the simpler context-free pass covers the observed
// cases. If a bool-context-aware index emit is needed later, the
// caller can build [ExprContext] and route through this helper.
func translateIndexExpr(c ir.IndexColumn, opts emitOpts) string {
	// SQLite source (ADR-0133 follow-up): translate the portable subset. A
	// non-portable expression-index body is WARN-skipped for the WHOLE index
	// in emitCreateIndex (an index is a performance object, not a correctness
	// one), so ok=false here is a defensive verbatim — the index was already
	// filtered out before this renders.
	if c.ExpressionDialect == sqliteSourceDialect {
		if pg, ok := translate.SQLiteExprToPG(c.Expression); ok {
			return requotePGReservedIdents(pg)
		}
		return c.Expression
	}
	// Translate ONLY from the one engine this writer's translator accepts
	// (MySQL); self / untagged / any unknown dialect emits verbatim
	// (ADR-0133 §2).
	if c.ExpressionDialect != translatableSourceDialect {
		return c.Expression
	}
	// Cross-dialect index expression: re-quote PG reserved-word column
	// refs the source reader de-quoted (catalog Bug 63). Same-dialect
	// PG→PG returned above with the pg_get_expr text verbatim.
	return requotePGReservedIdents(
		translateExprForPG(c.Expression, ExprContext{EnabledPGExtensions: opts.EnabledExtensions}),
	)
}

// emitCreateIndex produces a CREATE INDEX statement (UNIQUE if
// applicable). Postgres uses CREATE INDEX rather than ALTER TABLE
// ADD INDEX (which doesn't exist here).
func emitCreateIndex(schema, tableName string, idx *ir.Index, opts emitOpts) (string, error) {
	if idx == nil {
		return "", errors.New("postgres: emitCreateIndex: index is nil")
	}
	if strings.EqualFold(idx.Name, "PRIMARY") {
		return "", errors.New("postgres: emitCreateIndex: PRIMARY index is inline in CREATE TABLE")
	}
	if len(idx.Columns) == 0 {
		return "", fmt.Errorf("postgres: emitCreateIndex: index %q has no columns", idx.Name)
	}

	// SQLite source (ADR-0133 follow-up): if any carried "sqlite"-dialect
	// predicate/expression body can't be translated to Postgres, WARN-skip
	// the WHOLE index rather than emit a non-portable body that aborts the
	// migration at CREATE INDEX. An index is a performance object, not a
	// correctness one — matching ADR-0133's read-time WARN-skip choice for
	// expression-index bodies it can't parse. Signalled to the caller as an
	// empty statement with a nil error.
	if offending, portable := sqliteIndexPortablePG(idx); !portable {
		slog.Warn(
			"postgres: skipping index carrying a non-portable SQLite expression "+
				"(the index is NOT created on the target — recreate it there if needed)",
			slog.String("table", tableName),
			slog.String("index", idx.Name),
			slog.String("expression", offending),
		)
		return "", nil
	}

	var sb strings.Builder
	sb.WriteString("CREATE ")
	if idx.Unique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX ")
	if idx.Name != "" {
		// Postgres index names are schema-scoped (a single namespace
		// per schema). MySQL index names are table-scoped (the same
		// name on two tables is fine). Cross-engine, that means a
		// MySQL source with `idx_fk_film_id` on both `inventory` and
		// `film_actor` would collide on the PG target. Prefixing
		// with the table name disambiguates without losing the
		// human-readable original.
		name := pgIndexName(tableName, idx.Name)
		// Roadmap item 43: refuse a >63-byte effective name loudly
		// rather than let PG silently truncate it into a collision
		// (the IF NOT EXISTS second-create silent no-op → missing
		// index, a silent schema-fidelity loss).
		if err := validatePGIndexName(name, idx.Name, tableName); err != nil {
			return "", err
		}
		sb.WriteString(quoteIdent(name))
		sb.WriteByte(' ')
	}
	sb.WriteString("ON ")
	sb.WriteString(quoteIdent(schema))
	sb.WriteByte('.')
	sb.WriteString(quoteIdent(tableName))
	sb.WriteByte(' ')
	if method := resolveIndexMethod(idx); method != "" {
		sb.WriteString("USING ")
		sb.WriteString(method)
		sb.WriteByte(' ')
	}
	sb.WriteString(emitIndexColumnList(idx.Columns, opts))
	// Covering index: non-key payload columns ride in INCLUDE (...),
	// kept distinct from the key columns above. Flattening them into
	// the key list silently changes index semantics (catalog Bug 19b).
	if len(idx.IncludeColumns) > 0 {
		sb.WriteString(" INCLUDE (")
		for i, c := range idx.IncludeColumns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(quoteIdent(c))
		}
		sb.WriteByte(')')
	}
	// Partial index: the WHERE predicate. Dropping it silently turns a
	// partial index into a full one (catalog Bug 19a). Same-dialect
	// PG-source text passes through verbatim; a cross-dialect source
	// runs the ADR-0016 translator first.
	if pred := strings.TrimSpace(idx.Predicate); pred != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(translateIndexPredicate(idx, opts))
	}
	sb.WriteByte(';')
	return sb.String(), nil
}

// translateIndexPredicate returns the partial-index WHERE predicate to
// emit, applying the ADR-0016 cross-dialect translation pass when the
// IR's PredicateDialect indicates a non-PG source. Same policy as
// [translateIndexExpr] for expression-index bodies: same-dialect /
// untagged text passes through verbatim (the PG pg_get_expr output);
// a MySQL-source predicate is rewritten and PG-reserved idents the
// source reader de-quoted are re-quoted.
func translateIndexPredicate(idx *ir.Index, opts emitOpts) string {
	// SQLite source (ADR-0133 follow-up): translate the portable subset. A
	// non-portable predicate WARN-skips the whole partial index in
	// emitCreateIndex, so ok=false here is a defensive verbatim.
	if idx.PredicateDialect == sqliteSourceDialect {
		if pg, ok := translate.SQLiteExprToPG(idx.Predicate); ok {
			return requotePGReservedIdents(pg)
		}
		return idx.Predicate
	}
	// Translate ONLY from the one engine this writer's translator accepts
	// (MySQL); self / untagged / any unknown dialect emits verbatim
	// (ADR-0133 §2).
	if idx.PredicateDialect != translatableSourceDialect {
		return idx.Predicate
	}
	return requotePGReservedIdents(
		translateExprForPG(idx.Predicate, ExprContext{EnabledPGExtensions: opts.EnabledExtensions}),
	)
}

// refuseNonPortableSQLiteExprPG returns a loud, table/column-named refusal when
// a "sqlite"-dialect DATA-LOAD-BEARING body (a generated column or a CHECK)
// has no provably-portable Postgres translation. This is the F6 backstop: for
// a construct that is SYNTACTICALLY valid on PG but SEMANTICALLY divergent
// (the modulo `%` operator, a rounding CAST, …), emitting it verbatim would be
// SILENTLY accepted by PG and compute a wrong value in a STORED generated
// column — so we refuse rather than trust the target to reject it. Non-sqlite
// dialects and translatable bodies return nil. (A non-portable INDEX body is a
// performance object, so it stays WARN-skip — see sqliteIndexPortablePG.)
func refuseNonPortableSQLiteExprPG(kind, name, expr, dialect string) error {
	if dialect != sqliteSourceDialect {
		return nil
	}
	if _, ok := translate.SQLiteExprToPG(expr); ok {
		return nil
	}
	return fmt.Errorf(
		"refuse loudly: %s %q carries a non-portable SQLite expression %q with no "+
			"provably-equivalent Postgres translation. Emitting it verbatim is unsafe — "+
			"Postgres may SILENTLY accept a syntactically-valid but semantically-divergent "+
			"body (e.g. the modulo operator or a rounding CAST) and compute a WRONG value. "+
			"Operator recovery: drop or rewrite the %s on the SQLite source before migrating, "+
			"or re-create an equivalent Postgres %s on the target post-migration. sluice does "+
			"not guess non-portable SQLite expression translations",
		kind, name, expr, kind, kind,
	)
}

// sqliteIndexPortablePG reports whether every "sqlite"-dialect predicate and
// expression body carried on idx translates to Postgres. When portable is
// false, offending names the first body that doesn't (for the WARN);
// non-SQLite / empty bodies are trivially portable (the verbatim path handles
// them). Used by emitCreateIndex to WARN-skip a non-portable SQLite index.
func sqliteIndexPortablePG(idx *ir.Index) (offending string, portable bool) {
	if idx.PredicateDialect == sqliteSourceDialect && strings.TrimSpace(idx.Predicate) != "" {
		if _, ok := translate.SQLiteExprToPG(idx.Predicate); !ok {
			return "WHERE " + idx.Predicate, false
		}
	}
	for _, c := range idx.Columns {
		if c.ExpressionDialect == sqliteSourceDialect && c.Expression != "" {
			if _, ok := translate.SQLiteExprToPG(c.Expression); !ok {
				return c.Expression, false
			}
		}
	}
	return "", true
}

// resolveIndexMethod picks the access-method name to emit for an
// index. Prefers the IR's verbatim Method field when populated
// (extension-introduced methods like pgvector's ivfflat/hnsw under
// ADR-0032); falls back to the canonical postgresIndexMethod
// dispatch on Kind otherwise. Returns "" when neither yields a
// method, leaving PG to use its default (btree).
func resolveIndexMethod(idx *ir.Index) string {
	if idx == nil {
		return ""
	}
	if idx.Method != "" {
		return idx.Method
	}
	return postgresIndexMethod(idx.Kind)
}

// postgresIndexMethod maps an [ir.IndexKind] to a Postgres access
// method name. Returns "" when the IR's IndexKind has no clean
// Postgres equivalent — Postgres defaults to btree, which is what we
// want for IndexKindUnspecified.
func postgresIndexMethod(k ir.IndexKind) string {
	switch k {
	case ir.IndexKindBTree:
		return "btree"
	case ir.IndexKindHash:
		return "hash"
	case ir.IndexKindGIN:
		return "gin"
	case ir.IndexKindGIST:
		return "gist"
	case ir.IndexKindSPGist:
		return "spgist"
	case ir.IndexKindBRIN:
		return "brin"
	}
	// FullText/Spatial don't have direct Postgres builtin equivalents
	// (FTS uses tsvector + GIN; Spatial needs PostGIS). Fall through
	// to default (btree).
	return ""
}

// emitAddForeignKey produces an ALTER TABLE ... ADD CONSTRAINT
// statement for a foreign key on the given child table.
func emitAddForeignKey(schema, childTable string, fk *ir.ForeignKey) (string, error) {
	if fk == nil {
		return "", errors.New("postgres: emitAddForeignKey: fk is nil")
	}
	if len(fk.Columns) == 0 || len(fk.ReferencedColumns) == 0 {
		return "", fmt.Errorf("postgres: emitAddForeignKey: fk %q has no columns", fk.Name)
	}
	if len(fk.Columns) != len(fk.ReferencedColumns) {
		return "", fmt.Errorf("postgres: emitAddForeignKey: fk %q column count mismatch (%d vs %d)",
			fk.Name, len(fk.Columns), len(fk.ReferencedColumns))
	}

	refSchema := fk.ReferencedSchema
	if refSchema == "" {
		refSchema = schema
	}

	var sb strings.Builder
	sb.WriteString("ALTER TABLE ")
	sb.WriteString(quoteIdent(schema))
	sb.WriteByte('.')
	sb.WriteString(quoteIdent(childTable))
	sb.WriteString(" ADD")
	if fk.Name != "" {
		sb.WriteString(" CONSTRAINT ")
		sb.WriteString(quoteIdent(fk.Name))
	}
	sb.WriteString(" FOREIGN KEY ")
	sb.WriteString(emitColumnList(fk.Columns))
	sb.WriteString(" REFERENCES ")
	sb.WriteString(quoteIdent(refSchema))
	sb.WriteByte('.')
	sb.WriteString(quoteIdent(fk.ReferencedTable))
	sb.WriteByte(' ')
	sb.WriteString(emitColumnList(fk.ReferencedColumns))

	if fk.OnDelete != ir.FKActionNoAction {
		sb.WriteString(" ON DELETE ")
		sb.WriteString(fk.OnDelete.String())
	}
	if fk.OnUpdate != ir.FKActionNoAction {
		sb.WriteString(" ON UPDATE ")
		sb.WriteString(fk.OnUpdate.String())
	}
	sb.WriteByte(';')
	return sb.String(), nil
}

// emitCheckConstraint renders a CHECK clause for inclusion in a
// CREATE TABLE column list:
//
//	CONSTRAINT "name" CHECK (expr)
//
// or, when the constraint is unnamed in the IR:
//
//	CHECK (expr)
//
// The expression is passed through verbatim from the source dialect
// (with engine-specific identifier quoting normalized at the read
// boundary). Non-portable expressions — MySQL's IF(...) versus PG's
// CASE, function names that differ between dialects — fail loudly at
// CREATE TABLE time on the target rather than be guessed-at, which
// matches the project's verbatim-passthrough translation policy.
//
// Bug 77 symmetric (task #73): the cross-dialect refuse-loudly
// pre-flight (shared with the Shape A AlterAddCheck path) fires here
// too, so a MySQL-source CHECK carrying an untranslatable predicate
// (e.g. a json_extract call the translator could not rewrite) is
// rejected with an operator-actionable error at CREATE-TABLE time
// rather than emitted verbatim and failing on the PG parser with an
// opaque SQLSTATE 42601.
func emitCheckConstraint(c *ir.CheckConstraint, tbl *ir.Table, opts emitOpts) (string, error) {
	exprText := translateCheckExpr(c, tbl, opts)
	if err := refuseUntranslatedCheckExprPG(c, exprText); err != nil {
		return "", err
	}
	var sb strings.Builder
	if c.Name != "" {
		sb.WriteString("CONSTRAINT ")
		sb.WriteString(quoteIdent(c.Name))
		sb.WriteByte(' ')
	}
	sb.WriteString("CHECK (")
	sb.WriteString(exprText)
	sb.WriteByte(')')
	return sb.String(), nil
}

// translateGeneratedExpr returns the generated-column expression to
// emit, applying the cross-dialect translation pass when the IR's
// dialect tag indicates a different source dialect (see ADR-0016).
// An empty / matching dialect tag emits verbatim — same behaviour as
// before the translation layer landed. tbl supplies the bool-column
// context for the v0.8.0 bool-idiom rewrite; nil is permitted and
// disables that rewrite. The outer column's IR type also informs the
// COALESCE rewrite direction (v0.9.1 / Bug 17 residual): integer-
// typed generated columns whose body returns bool get the bool side
// cast to int, instead of converting the int literal to bool.
func translateGeneratedExpr(c *ir.Column, tbl *ir.Table, opts emitOpts) string {
	// SQLite source (ADR-0133 follow-up): route the portable subset through
	// the shared SQLite→PG expression translator. A generated column is
	// DATA-load-bearing (its value is computed on the target), so a
	// non-portable body (ok=false) emits VERBATIM — the target REJECTS it
	// loudly at CREATE TABLE. We NEVER warn-DROP a gencol body, which would
	// silently change the column's meaning.
	if c.GeneratedExprDialect == sqliteSourceDialect {
		if pg, ok := translate.SQLiteExprToPG(c.GeneratedExpr); ok {
			return requotePGReservedIdents(pg)
		}
		return c.GeneratedExpr
	}
	// Translate ONLY from the one engine this writer's translator accepts
	// (MySQL); self / untagged / any unknown dialect emits verbatim
	// (ADR-0133 §2).
	if c.GeneratedExprDialect != translatableSourceDialect {
		return c.GeneratedExpr
	}
	ctx := exprContextForTable(tbl)
	ctx.EnabledPGExtensions = opts.EnabledExtensions
	if _, isInt := c.Type.(ir.Integer); isInt {
		ctx.OuterColumnIsInteger = true
	}
	// Cross-dialect body: the source reader stripped its engine's
	// identifier quotes for IR portability, which de-quotes any
	// reserved-word column reference (a MySQL `order` / `key`).
	// translateExprForPG rewrites function/operator spellings but not
	// bare idents, so re-quote PG reserved words here or CREATE TABLE
	// fails with SQLSTATE 42601 (catalog Bug 63). Same-dialect PG→PG
	// returns above before this point — the PG reader already emits
	// properly-quoted refs from pg_get_expr.
	return requotePGReservedIdents(translateExprForPG(c.GeneratedExpr, ctx))
}

// translateCheckExpr returns the CHECK-constraint expression to emit,
// applying the cross-dialect translation pass when the IR's dialect
// tag indicates a different source dialect. tbl supplies the bool-
// column context for the v0.8.0 bool-idiom rewrite; nil is permitted
// and disables that rewrite.
func translateCheckExpr(c *ir.CheckConstraint, tbl *ir.Table, opts emitOpts) string {
	// SQLite source (ADR-0133 follow-up): a CHECK constraint is
	// DATA-load-bearing (it enforces validity), so the portable subset
	// translates and a non-portable body emits VERBATIM for a loud target
	// reject — never warn-DROP (dropping a CHECK silently removes an
	// integrity guarantee).
	if c.ExprDialect == sqliteSourceDialect {
		if pg, ok := translate.SQLiteExprToPG(c.Expr); ok {
			return requotePGReservedIdents(pg)
		}
		return c.Expr
	}
	// Translate ONLY from the one engine this writer's translator accepts
	// (MySQL); self / untagged / any unknown dialect emits verbatim
	// (ADR-0133 §2).
	if c.ExprDialect != translatableSourceDialect {
		return c.Expr
	}
	ctx := exprContextForTable(tbl)
	ctx.EnabledPGExtensions = opts.EnabledExtensions
	// Cross-dialect CHECK body: re-quote PG reserved-word column refs
	// the source reader de-quoted (catalog Bug 63). Same-dialect PG→PG
	// returned above with the pg_get_expr text verbatim.
	return requotePGReservedIdents(translateExprForPG(c.Expr, ctx))
}

// exprContextForTable builds the [ExprContext] for tbl, populating
// the BoolColumns set with the unquoted names of every column whose
// IR type is ir.Boolean. Returns the zero value when tbl is nil so
// callers without a table can still invoke translateExprForPG with a
// safe fallback.
func exprContextForTable(tbl *ir.Table) ExprContext {
	if tbl == nil {
		return ExprContext{}
	}
	var bools map[string]bool
	for _, col := range tbl.Columns {
		if _, ok := col.Type.(ir.Boolean); !ok {
			continue
		}
		if bools == nil {
			bools = make(map[string]bool, 4)
		}
		bools[col.Name] = true
	}
	return ExprContext{BoolColumns: bools}
}

// emitColumnList renders a parenthesised, comma-separated list of
// quoted column names.
func emitColumnList(cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = quoteIdent(c)
	}
	return "(" + strings.Join(parts, ", ") + ")"
}
