// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/orware/sluice/internal/ir"
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
		return fmt.Sprintf("NUMERIC(%d,%d)", v.Precision, v.Scale), nil
	case ir.Float:
		if v.Precision == ir.FloatSingle {
			return "REAL", nil
		}
		return "DOUBLE PRECISION", nil

	// ---- Character / binary ----
	case ir.Char:
		return fmt.Sprintf("CHAR(%d)", v.Length), nil
	case ir.Varchar:
		return fmt.Sprintf("VARCHAR(%d)", v.Length), nil
	case ir.Text:
		// Postgres TEXT is unbounded; the IR's TextSize buckets
		// don't translate. All sizes collapse to TEXT.
		return "TEXT", nil
	case ir.Binary, ir.Varbinary, ir.Blob:
		// Postgres has only one binary type, BYTEA.
		return "BYTEA", nil

	// ---- Temporal ----
	case ir.Date:
		return "DATE", nil
	case ir.Time:
		return emitWithPrecision("TIME", v.Precision), nil
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
				v.String(), v.Extension, v.Extension)
		}
		return emitExtensionColumn(v)
	}
	return "", fmt.Errorf("postgres: unknown IR type %T", t)
}

// emitIntegerType returns the Postgres integer type for an [ir.Integer],
// expanding unsigned widths to the next signed rank (Postgres has no
// unsigned integers) and using GENERATED IDENTITY for auto-increment.
//
// BIGINT UNSIGNED that isn't auto-increment widens to NUMERIC(20,0).
// For BIGINT UNSIGNED that IS auto-increment we emit BIGINT IDENTITY
// and accept the small risk that values may exceed MaxInt64 — in
// practice an extreme corner case.
func emitIntegerType(i ir.Integer) string {
	if i.Unsigned && i.Width == 64 && !i.AutoIncrement {
		return "NUMERIC(20,0)"
	}
	width := effectiveWidth(i)
	typeName := postgresIntName(width)
	if i.AutoIncrement {
		return typeName + " GENERATED BY DEFAULT AS IDENTITY"
	}
	return typeName
}

// effectiveWidth returns the width Postgres should use for an integer
// column, widening one rank for unsigned values (so the original
// numeric range still fits).
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

// emitDefault renders a DEFAULT clause body for the given default
// value. Returns ("", false) if no DEFAULT clause should be emitted.
//
// For [ir.DefaultExpression] with a non-empty Dialect tag that doesn't
// match this writer's dialect, the expression body is routed through
// [translateExprForPG] so cross-engine MySQL-spelled defaults (e.g.
// `(UUID())`, `(RAND() * 100)`, `DATE_ADD(...)`) translate to PG
// equivalents instead of failing loud on the target. v0.11.3 fix for
// Bugs 28/29/30; pre-fix the DEFAULT path was the only IR-expression
// path that bypassed the translator (generated + CHECK + index already
// routed through it).
func emitDefault(d ir.DefaultValue) (string, bool) {
	switch v := d.(type) {
	case nil, ir.DefaultNone:
		return "", false
	case ir.DefaultLiteral:
		return quoteSQLString(v.Value), true
	case ir.DefaultExpression:
		return translateDefaultExpr(v), true
	}
	return "", false
}

// translateDefaultExpr returns the DEFAULT-expression body to emit,
// applying the cross-dialect translation pass when the IR's Dialect
// tag indicates a different source dialect. Same gating shape as
// [translateGeneratedExpr] and [translateCheckExpr]; an empty / matching
// dialect tag emits verbatim — same behaviour as before v0.11.3.
//
// The DEFAULT-expression context doesn't carry table-level bool-column
// information (defaults are evaluated per-row at INSERT time, not over
// other column values), so the [ExprContext] passed to the translator
// is the zero value — bool-idiom rewrites stay no-ops on this path.
func translateDefaultExpr(d ir.DefaultExpression) string {
	if d.Dialect == "" || d.Dialect == dialectName {
		return d.Expr
	}
	return translateExprForPG(d.Expr, ExprContext{})
}

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
	if _, isEnum := c.Type.(ir.Enum); isEnum {
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
			typeStr = qualifiedEnumTypeRef(opts.TargetSchema, table.Name, c.Name)
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
			slog.Warn("postgres: promoting source-engine VIRTUAL generated column to STORED (postgres has no VIRTUAL support)",
				slog.String("table", tableNameForLog(table)),
				slog.String("column", c.Name),
			)
		}
		sb.WriteString(" GENERATED ALWAYS AS (")
		body := translateGeneratedExpr(c, table)
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
	if dflt, ok := emitDefault(c.Default); ok {
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
		if _, isEnum := c.Type.(ir.Enum); isEnum && enumDefaultShouldCast(c.Default, dflt) {
			sb.WriteString("::")
			sb.WriteString(qualifiedEnumTypeRef(opts.TargetSchema, table.Name, c.Name))
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
// no risk of name collisions or unintended sharing.
func enumTypeName(tableName, columnName string) string {
	return tableName + "_" + columnName + "_enum"
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
func qualifiedEnumTypeRef(targetSchema, tableName, columnName string) string {
	name := enumTypeName(tableName, columnName)
	if targetSchema == "" {
		return quoteIdent(name)
	}
	return quoteIdent(targetSchema) + "." + quoteIdent(name)
}

// pgIndexName disambiguates a source-side index name against the
// schema-scoped Postgres namespace. If the source name already
// begins with the table name (the common case for sluice-emitted
// names), it's returned verbatim; otherwise it gets a `<table>_`
// prefix. The two-form rule preserves human-readable names like
// `idx_users_email` while disambiguating MySQL-style sibling index
// names like `idx_fk_film_id` (which appears on multiple tables in
// real-world schemas).
func pgIndexName(tableName, sourceName string) string {
	if sourceName == "" {
		return ""
	}
	prefix := tableName + "_"
	if strings.HasPrefix(sourceName, prefix) {
		return sourceName
	}
	return prefix + sourceName
}

// emitCreateEnumType produces a CREATE TYPE statement for a single
// enum, named by the table+column generator. Values are quoted and
// comma-separated.
func emitCreateEnumType(schema, tableName, columnName string, values []string) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = quoteSQLString(v)
	}
	return fmt.Sprintf(
		"CREATE TYPE %s.%s AS ENUM (%s);",
		quoteIdent(schema),
		quoteIdent(enumTypeName(tableName, columnName)),
		strings.Join(parts, ", "),
	)
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
		parts = append(parts, emitCheckConstraint(chk, table))
	}

	if table.PrimaryKey != nil {
		parts = append(parts, "PRIMARY KEY "+emitIndexColumnList(table.PrimaryKey.Columns))
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
func emitIndexColumnList(cols []ir.IndexColumn) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		var entry string
		if c.Expression != "" {
			entry = "(" + translateIndexExpr(c) + ")"
		} else {
			entry = quoteIdent(c.Column)
		}
		// Per-column operator class — populated by the schema reader
		// only for extension-introduced access methods that need an
		// explicit opclass (pgvector's hnsw rejects the index at
		// CREATE without one). Default-opclass cases (btree/hash/gin
		// over built-ins) leave this empty and emit nothing extra.
		if c.OperatorClass != "" {
			entry += " " + c.OperatorClass
		}
		if c.Desc {
			entry += " DESC"
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
func translateIndexExpr(c ir.IndexColumn) string {
	if c.ExpressionDialect == "" || c.ExpressionDialect == dialectName {
		return c.Expression
	}
	return translateExprForPG(c.Expression, ExprContext{})
}

// emitCreateIndex produces a CREATE INDEX statement (UNIQUE if
// applicable). Postgres uses CREATE INDEX rather than ALTER TABLE
// ADD INDEX (which doesn't exist here).
func emitCreateIndex(schema, tableName string, idx *ir.Index) (string, error) {
	if idx == nil {
		return "", errors.New("postgres: emitCreateIndex: index is nil")
	}
	if strings.EqualFold(idx.Name, "PRIMARY") {
		return "", errors.New("postgres: emitCreateIndex: PRIMARY index is inline in CREATE TABLE")
	}
	if len(idx.Columns) == 0 {
		return "", fmt.Errorf("postgres: emitCreateIndex: index %q has no columns", idx.Name)
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
		sb.WriteString(quoteIdent(pgIndexName(tableName, idx.Name)))
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
	sb.WriteString(emitIndexColumnList(idx.Columns))
	sb.WriteByte(';')
	return sb.String(), nil
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
func emitCheckConstraint(c *ir.CheckConstraint, tbl *ir.Table) string {
	var sb strings.Builder
	if c.Name != "" {
		sb.WriteString("CONSTRAINT ")
		sb.WriteString(quoteIdent(c.Name))
		sb.WriteByte(' ')
	}
	sb.WriteString("CHECK (")
	sb.WriteString(translateCheckExpr(c, tbl))
	sb.WriteByte(')')
	return sb.String()
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
func translateGeneratedExpr(c *ir.Column, tbl *ir.Table) string {
	if c.GeneratedExprDialect == "" || c.GeneratedExprDialect == dialectName {
		return c.GeneratedExpr
	}
	ctx := exprContextForTable(tbl)
	if _, isInt := c.Type.(ir.Integer); isInt {
		ctx.OuterColumnIsInteger = true
	}
	return translateExprForPG(c.GeneratedExpr, ctx)
}

// translateCheckExpr returns the CHECK-constraint expression to emit,
// applying the cross-dialect translation pass when the IR's dialect
// tag indicates a different source dialect. tbl supplies the bool-
// column context for the v0.8.0 bool-idiom rewrite; nil is permitted
// and disables that rewrite.
func translateCheckExpr(c *ir.CheckConstraint, tbl *ir.Table) string {
	if c.ExprDialect == "" || c.ExprDialect == dialectName {
		return c.Expr
	}
	return translateExprForPG(c.Expr, exprContextForTable(tbl))
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
