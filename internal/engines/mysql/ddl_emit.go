// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/orware/sluice/internal/ir"
)

// emitColumnType returns the MySQL DDL fragment representing t. The
// fragment is the type plus any annotations that conventionally live
// adjacent to the type (CHARACTER SET, COLLATE, AUTO_INCREMENT). The
// column-level NOT NULL and DEFAULT clauses are emitted separately
// by emitColumnDef.
//
// Returns an error for IR types MySQL has no native representation for
// (Inet, Cidr, Macaddr, Array). Translation policy is responsible for
// rewriting these to MySQL-compatible types before the writer sees
// them; an error here means an upstream contract was violated.
func emitColumnType(t ir.Type) (string, error) {
	switch v := t.(type) {
	// ---- Numeric / boolean ----
	case ir.Boolean:
		return "TINYINT(1)", nil
	case ir.Integer:
		return emitIntegerType(v), nil
	case ir.Decimal:
		return fmt.Sprintf("DECIMAL(%d,%d)", v.Precision, v.Scale), nil
	case ir.Float:
		if v.Precision == ir.FloatSingle {
			return "FLOAT", nil
		}
		return "DOUBLE", nil

	// ---- Character ----
	case ir.Char:
		return emitCharType("CHAR", v.Length, v.Charset, v.Collation), nil
	case ir.Varchar:
		return emitCharType("VARCHAR", v.Length, v.Charset, v.Collation), nil
	case ir.Text:
		return emitTextType(v.Size, v.Charset, v.Collation), nil

	// ---- Binary ----
	case ir.Binary:
		return fmt.Sprintf("BINARY(%d)", v.Length), nil
	case ir.Varbinary:
		return fmt.Sprintf("VARBINARY(%d)", v.Length), nil
	case ir.Blob:
		return emitBlobType(v.Size), nil

	// ---- Temporal ----
	case ir.Date:
		return "DATE", nil
	case ir.Time:
		return emitWithPrecision("TIME", v.Precision), nil
	case ir.DateTime:
		return emitWithPrecision("DATETIME", v.Precision), nil
	case ir.Timestamp:
		// MySQL TIMESTAMP is always zoned (stored as UTC, converted
		// on retrieval). DateTime-without-zone IR values would map
		// to DATETIME, not TIMESTAMP.
		return emitWithPrecision("TIMESTAMP", v.Precision), nil

	// ---- Structured ----
	case ir.JSON:
		return "JSON", nil

	// ---- Categorical (extension) ----
	case ir.Enum:
		return "ENUM(" + emitStringList(v.Values) + ")", nil
	case ir.Set:
		return "SET(" + emitStringList(v.Values) + ")", nil

	// ---- Identity / spatial (extension) ----
	case ir.UUID:
		// MySQL has no native UUID type. CHAR(36) is the readable
		// canonical form; a translator can opt into BINARY(16) via
		// per-column override if storage matters.
		return "CHAR(36)", nil
	case ir.Geometry:
		return emitGeometryType(v.Subtype), nil

	// ---- PG-native types without native MySQL equivalents ----
	// Pre-v0.7.0 these returned an error pointing operators at
	// `--type-override`. v0.7.0 auto-emits a sensible default so
	// PG→MySQL migrations of these column types don't require
	// per-column intervention. Operators wanting strict syntactic
	// validation (e.g. CHECK regex on Inet) still use --type-override
	// to a custom shape; operators wanting tighter or looser sizing
	// likewise. The schema-preview command (ADR-0024) surfaces the
	// auto-emit choice so it isn't silent.
	case ir.Array:
		// Array values arrive at the writer as JSON-shaped strings
		// (Bug 14 fix in v0.5.0); MySQL JSON is the closest semantic
		// match — structured, queryable via JSON_EXTRACT, indexable
		// on virtual generated columns.
		return "JSON", nil
	case ir.Inet, ir.Cidr:
		// Max IPv6 + CIDR mask in canonical form is 43 chars; round
		// up to 45 for headroom. No CHECK constraint emitted in
		// v0.7.0 — operators wanting strict validation use
		// --type-override TABLE.COL=varchar:length=N with their own
		// CHECK shape.
		return "VARCHAR(45)", nil
	case ir.Macaddr:
		// EUI-64 in canonical form is 23 chars; round up to 30.
		return "VARCHAR(30)", nil

	// ---- PG extension passthrough types (ADR-0032) ----
	// Most PG extension types have no MySQL equivalent (pgvector,
	// pg_trgm, postgis). The cross-engine refusal in
	// pipeline.checkCrossEngineSupportable normally fires before
	// MySQL's writer is invoked. The carve-outs:
	//
	//   - hstore: a key/value-pair type that maps cleanly to MySQL
	//     JSON. The wire-format conversion (PG `"k"=>"v"` →
	//     `{"k":"v"}`) lives in row_writer.go::prepareValue; the
	//     emit side just declares JSON.
	//   - citext: case-insensitive text. Maps to VARCHAR with the
	//     case-insensitive collation `utf8mb4_0900_ai_ci`. The
	//     default length is 255 (citext on PG is unbounded; operators
	//     wanting a different length use --type-override).
	//
	// Other extension names get the loud-failure refusal pointing at
	// --type-override. The IR remains ir.ExtensionType throughout —
	// the rewrite is engine-local to the writer.
	case ir.ExtensionType:
		switch v.Extension {
		case "hstore":
			return "JSON", nil
		case "citext":
			return "VARCHAR(255) COLLATE utf8mb4_0900_ai_ci", nil
		}
		return "", fmt.Errorf(
			"mysql: column type %s is from a PG extension; cross-engine "+
				"translation is not supported for this extension — supply "+
				"--type-override TABLE.COL=<MySQL_type> to opt in (ADR-0032)",
			v.String())
	}

	return "", fmt.Errorf("mysql: unknown IR type %T", t)
}

// emitIntegerType returns the MySQL integer type DDL, including the
// UNSIGNED and AUTO_INCREMENT modifiers when applicable.
func emitIntegerType(i ir.Integer) string {
	var sb strings.Builder
	switch i.Width {
	case 8:
		sb.WriteString("TINYINT")
	case 16:
		sb.WriteString("SMALLINT")
	case 24:
		sb.WriteString("MEDIUMINT")
	case 32:
		sb.WriteString("INT")
	case 64:
		sb.WriteString("BIGINT")
	default:
		// Unrecognised widths fall back to BIGINT — the safest
		// container. The schema reader never produces other widths,
		// so this path is reachable only via hand-built IR.
		sb.WriteString("BIGINT")
	}
	if i.Unsigned {
		sb.WriteString(" UNSIGNED")
	}
	if i.AutoIncrement {
		sb.WriteString(" AUTO_INCREMENT")
	}
	return sb.String()
}

// emitCharType returns CHAR(N)/VARCHAR(N) plus optional charset/collation.
func emitCharType(prefix string, length int, charset, collation string) string {
	out := fmt.Sprintf("%s(%d)", prefix, length)
	return appendCharsetCollation(out, charset, collation)
}

// emitTextType returns the appropriate TEXT family keyword for the
// given size, plus optional charset/collation.
func emitTextType(size ir.TextSize, charset, collation string) string {
	var keyword string
	switch size {
	case ir.TextTiny:
		keyword = "TINYTEXT"
	case ir.TextRegular:
		keyword = "TEXT"
	case ir.TextMedium:
		keyword = "MEDIUMTEXT"
	case ir.TextLong:
		keyword = "LONGTEXT"
	default:
		keyword = "TEXT"
	}
	return appendCharsetCollation(keyword, charset, collation)
}

// emitBlobType returns the appropriate BLOB family keyword.
func emitBlobType(size ir.BlobSize) string {
	switch size {
	case ir.BlobTiny:
		return "TINYBLOB"
	case ir.BlobRegular:
		return "BLOB"
	case ir.BlobMedium:
		return "MEDIUMBLOB"
	case ir.BlobLong:
		return "LONGBLOB"
	default:
		return "BLOB"
	}
}

// emitWithPrecision renders TYPE(N), or TYPE when precision is zero.
// MySQL accepts both forms; omitting the precision produces a slightly
// shorter, more conventional DDL.
func emitWithPrecision(typeName string, precision int) string {
	if precision == 0 {
		return typeName
	}
	return fmt.Sprintf("%s(%d)", typeName, precision)
}

// emitGeometryType returns the MySQL spatial type for the given subtype.
func emitGeometryType(subtype ir.GeometrySubtype) string {
	switch subtype {
	case ir.GeometryPoint:
		return "POINT"
	case ir.GeometryLineString:
		return "LINESTRING"
	case ir.GeometryPolygon:
		return "POLYGON"
	case ir.GeometryMultiPoint:
		return "MULTIPOINT"
	case ir.GeometryMultiLineString:
		return "MULTILINESTRING"
	case ir.GeometryMultiPolygon:
		return "MULTIPOLYGON"
	case ir.GeometryCollection:
		return "GEOMETRYCOLLECTION"
	default:
		return "GEOMETRY"
	}
}

// appendCharsetCollation appends CHARACTER SET / COLLATE clauses to
// a type expression when the values are non-empty. Useful for
// character types where the charset/collation are conventionally
// part of the type spec rather than the column-level annotations.
func appendCharsetCollation(typeExpr, charset, collation string) string {
	if charset == "" && collation == "" {
		return typeExpr
	}
	var sb strings.Builder
	sb.WriteString(typeExpr)
	if charset != "" {
		sb.WriteString(" CHARACTER SET ")
		sb.WriteString(charset)
	}
	if collation != "" {
		sb.WriteString(" COLLATE ")
		sb.WriteString(collation)
	}
	return sb.String()
}

// emitStringList formats a sequence of strings as a comma-separated
// list of single-quoted SQL string literals. Used for ENUM/SET values.
func emitStringList(values []string) string {
	parts := make([]string, len(values))
	for i, v := range values {
		parts[i] = quoteSQLString(v)
	}
	return strings.Join(parts, ",")
}

// quoteSQLString returns s wrapped in single quotes, with interior
// single quotes escaped by doubling. Suitable for use as a SQL string
// literal or inside ENUM/SET value lists.
func quoteSQLString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// pgToMySQLDefaultExpr maps PG-canonical default expressions to
// their MySQL equivalents. Anything not in this map passes through
// verbatim — loud failure on the target beats silent corruption,
// per the project's translation policy.
//
// Lookup keys are lowercased + trimmed; PG's pg_get_expr output
// isn't case-stable across versions, and incidental whitespace
// can survive cast-stripping, so normalising before lookup keeps
// the table tiny.
//
// Entries:
//   - now() → CURRENT_TIMESTAMP. PG's "current timestamp" function
//     name; MySQL spells it CURRENT_TIMESTAMP (the bare keyword form,
//     no parens). emitDefault then routes through
//     matchTimestampDefaultPrecision to add a column-precision suffix
//     when the target column declares one.
//   - gen_random_uuid() → (UUID()). PG's built-in UUID generator;
//     MySQL's canonical equivalent is UUID() wrapped in the outer
//     parens MySQL 8.0+ requires for function-call expression
//     defaults. Closes Bug 42 — symmetric reverse of v0.11.3's
//     Bug 28 fix that translated MySQL's UUID() → gen_random_uuid()
//     for PG targets. Both PG's gen_random_uuid()::text and MySQL's
//     UUID() return canonical hyphenated 36-char form, so the
//     PG UUID column retargets to MySQL CHAR(36) and the default
//     produces semantically equivalent values.
//   - random() → (RAND()). PG's argless random()-in-[0,1) function;
//     MySQL spells it RAND() with the outer parens MySQL 8.0+
//     requires for expression defaults. Symmetric reverse of
//     v0.11.3's Bug 29 fix (MySQL RAND() → PG random()). Same
//     [0, 1) semantics on both sides.
//   - uuid_generate_v1() / uuid_generate_v1mc() / uuid_generate_v4()
//     → (UUID()). PG's uuid-ossp generators (ADR-0044 §3). MySQL has
//     a single UUID generator, UUID() (a v1 time-based UUID); the
//     uuid-ossp version distinction does NOT survive cross-engine.
//     Fidelity note: a DEFAULT means "generate *a* UUID" — in
//     practice operators rely on uniqueness, not the RFC-4122
//     version, so the version-agnostic mapping is the honest one.
//     v5 (name-based / namespace) is intentionally NOT mapped — it
//     is not a bare generator (it takes a namespace + name) and has
//     no MySQL equivalent; it falls through verbatim and the
//     cross-engine refusal arm catches the pgcrypto/exotic-uuid case.
var pgToMySQLDefaultExpr = map[string]string{
	"now()":                "CURRENT_TIMESTAMP",
	"gen_random_uuid()":    "(UUID())",
	"random()":             "(RAND())",
	"uuid_generate_v1()":   "(UUID())",
	"uuid_generate_v1mc()": "(UUID())",
	"uuid_generate_v4()":   "(UUID())",
}

// emitDefault renders a DEFAULT clause body for the given default
// value, given the column type the default belongs to. Returns
// ("", false) if no DEFAULT clause should be emitted.
//
// The column type is consulted only for the narrow case where the
// IR carries a literal whose canonical form differs across dialects:
// a Postgres BOOLEAN default of `true`/`false` arrives as a
// DefaultLiteral with that string value, but MySQL stores boolean as
// TINYINT and rejects the string `'true'`/`'false'` under strict mode.
// Translating to `1`/`0` here keeps the IR engine-neutral while
// letting the writer emit something MySQL accepts.
//
// DefaultExpression values are checked against pgToMySQLDefaultExpr
// for known PG-canonical forms (e.g. now() → CURRENT_TIMESTAMP),
// then fall through to verbatim emission.
func emitDefault(d ir.DefaultValue, t ir.Type) (string, bool) {
	switch v := d.(type) {
	case nil, ir.DefaultNone:
		return "", false
	case ir.DefaultLiteral:
		if _, isBool := t.(ir.Boolean); isBool {
			switch strings.ToLower(strings.TrimSpace(v.Value)) {
			case "true", "t":
				return "1", true
			case "false", "f":
				return "0", true
			}
		}
		return quoteSQLString(v.Value), true
	case ir.DefaultExpression:
		normalized := strings.ToLower(strings.TrimSpace(v.Expr))
		expr := v.Expr
		if translated, ok := pgToMySQLDefaultExpr[normalized]; ok {
			expr = translated
		}
		expr = matchTimestampDefaultPrecision(expr, t)
		expr = wrapMySQLExpressionDefault(expr)
		return expr, true
	}
	return "", false
}

// wrapMySQLExpressionDefault wraps a DEFAULT expression body in outer
// parens when MySQL 8.0+ requires it for function-call expression
// defaults. Closes Bug 44 — pre-fix the writer emitted
// `DEFAULT uuid()` (no outer parens) for an IR `DefaultExpression{Expr:
// "uuid()"}`, which MySQL rejects with Error 1064. The MySQL 8.0.13+
// expression-default syntax requires `DEFAULT (uuid())` for function
// calls; only the special temporal keywords (CURRENT_TIMESTAMP family,
// NOW(), LOCALTIME, etc.) are accepted bare.
//
// Three cases:
//
//  1. Already outer-wrapped — `(UUID())`, `(RAND())`, `(RAND() * 100)`:
//     pass through. The Bug 42 translation entries (`gen_random_uuid()`
//     → `(UUID())`) emit pre-wrapped expressions; same for
//     operator-supplied defaults that come through pgToMySQLDefaultExpr
//     pre-wrapped, and for cases where MySQL itself returns the
//     parens-preserved shape from INFORMATION_SCHEMA.
//
//  2. Bare-keyword temporal forms — `CURRENT_TIMESTAMP`,
//     `CURRENT_TIMESTAMP(6)`, `LOCALTIMESTAMP`, etc.: pass through bare.
//     Wrapping these is a syntax error: MySQL rejects
//     `DEFAULT (CURRENT_TIMESTAMP)` because the function-as-default
//     wrap rule excludes the special temporal keywords.
//
//  3. Function-call shape — `uuid()`, `rand()`, `some_func()`: wrap in
//     outer parens. Bug 44's exact failure shape.
//
// The "starts with `(`" check is naive — it doesn't verify the parens
// actually balance — but real schemas don't surface pathological shapes
// like `(a) + (b)`. If one does, the wrap path stays loud-failure on
// the target rather than guessing.
func wrapMySQLExpressionDefault(expr string) string {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return expr
	}
	if strings.HasPrefix(trimmed, "(") && strings.HasSuffix(trimmed, ")") {
		return expr
	}
	if isMySQLBareDefaultKeyword(trimmed) {
		return expr
	}
	return "(" + trimmed + ")"
}

// isMySQLBareDefaultKeyword reports whether s is one of MySQL's
// special temporal keywords accepted bare as a DEFAULT body. The
// bare-vs-wrapped distinction matters: `DEFAULT CURRENT_TIMESTAMP`
// and `DEFAULT NOW()` are valid, but `DEFAULT (CURRENT_TIMESTAMP)`
// and `DEFAULT (NOW())` are not — MySQL's grammar treats the temporal
// keywords as a separate production from the function-call form.
//
// Recognises (case-insensitive, with optional empty parens or
// `(N)` precision suffix):
//   - CURRENT_TIMESTAMP / NOW / LOCALTIME / LOCALTIMESTAMP
//   - CURRENT_DATE / CURRENT_TIME (no precision; date and time
//     accept the empty-parens form for symmetry)
func isMySQLBareDefaultKeyword(s string) bool {
	upper := strings.ToUpper(s)
	if i := strings.IndexByte(upper, '('); i >= 0 {
		if !strings.HasSuffix(upper, ")") {
			return false
		}
		inner := strings.TrimSpace(upper[i+1 : len(upper)-1])
		for _, c := range inner {
			if c < '0' || c > '9' {
				return false
			}
		}
		upper = upper[:i]
	}
	switch upper {
	case "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME",
		"LOCALTIME", "LOCALTIMESTAMP", "NOW":
		return true
	}
	return false
}

// matchTimestampDefaultPrecision rewrites a bare CURRENT_TIMESTAMP
// default expression to include the column's declared fractional-
// second precision, e.g. CURRENT_TIMESTAMP → CURRENT_TIMESTAMP(6) on
// a TIMESTAMP(6) column. MySQL rejects "Invalid default value for X"
// when the function call's precision doesn't match the column's
// declared precision; the most common path that hits this is a PG
// source migrating "TIMESTAMPTZ DEFAULT now()" to MySQL — PG reports
// TIMESTAMPTZ as ir.Timestamp{Precision:6} (PG's default) and the
// translator turns now() into CURRENT_TIMESTAMP, but without this
// adjustment the precisions disagree on the target.
//
// Inputs that already carry a parenthesised precision (e.g.
// CURRENT_TIMESTAMP(3)) pass through unchanged — the caller is
// telling us the precision they want.
func matchTimestampDefaultPrecision(expr string, t ir.Type) string {
	if !strings.EqualFold(strings.TrimSpace(expr), "CURRENT_TIMESTAMP") {
		return expr
	}
	prec := temporalPrecision(t)
	if prec <= 0 {
		return expr
	}
	return fmt.Sprintf("CURRENT_TIMESTAMP(%d)", prec)
}

// temporalPrecision returns the fractional-second precision declared
// on a temporal IR type, or 0 for non-temporal types.
func temporalPrecision(t ir.Type) int {
	switch v := t.(type) {
	case ir.Timestamp:
		return v.Precision
	case ir.DateTime:
		return v.Precision
	case ir.Time:
		return v.Precision
	}
	return 0
}

// emitColumnDef returns the full DDL fragment for a single column,
// suitable for inclusion in a CREATE TABLE column list:
//
//	`name` TYPE [GENERATED ALWAYS AS (expr) STORED|VIRTUAL] [NOT NULL] [DEFAULT ...] [COMMENT '...']
//
// For generated columns the GENERATED clause sits between the type
// and NOT NULL — that's where MySQL's grammar accepts it. The
// expression is passed through verbatim from the source dialect;
// non-portable expressions (e.g. PG-specific functions) will error
// at CREATE TABLE time rather than be guessed-at, in line with the
// project's verbatim-passthrough translation policy.
func emitColumnDef(c *ir.Column) (string, error) {
	if c == nil {
		return "", fmt.Errorf("mysql: emitColumnDef: column is nil")
	}
	typeStr, err := emitColumnType(c.Type)
	if err != nil {
		return "", fmt.Errorf("mysql: column %q: %w", c.Name, err)
	}

	var sb strings.Builder
	sb.WriteString(quoteIdent(c.Name))
	sb.WriteByte(' ')
	sb.WriteString(typeStr)
	if c.IsGenerated() {
		sb.WriteString(" GENERATED ALWAYS AS (")
		sb.WriteString(translateGeneratedExpr(c))
		sb.WriteByte(')')
		if c.GeneratedStored {
			sb.WriteString(" STORED")
		} else {
			sb.WriteString(" VIRTUAL")
		}
	}
	if !c.Nullable {
		sb.WriteString(" NOT NULL")
	}
	// `SRID <n>` is a MySQL 8.0+ column attribute on spatial types
	// (POINT/POLYGON/etc.). Emitted only when the IR carries a
	// non-zero SRID — SRID 0 is MySQL's "no spatial reference
	// declared" sentinel, identical to omitting the clause. Closes
	// the writer half of Bug 26 (PG → MySQL): a PG `geometry(POINT,
	// 4326)` column lands on MySQL as `POINT NOT NULL SRID 4326`
	// and ST_SRID(loc) returns 4326 instead of 0.
	if geom, ok := c.Type.(ir.Geometry); ok && geom.SRID != 0 {
		fmt.Fprintf(&sb, " SRID %d", geom.SRID)
	}
	// DEFAULT and AUTO_INCREMENT are mutually exclusive with GENERATED
	// in MySQL — the parser rejects the combination. Generated columns
	// arrive with Default = DefaultNone from the schema reader, so
	// emitDefault returns ok=false and the clause is skipped naturally;
	// we don't need a special case here.
	if dflt, ok := emitDefault(c.Default, c.Type); ok {
		// MySQL forbids DEFAULT on BLOB, TEXT, GEOMETRY, and JSON
		// columns — the server rejects CREATE TABLE with Error 1101
		// ("can't have a default value"). Cross-engine PG → MySQL
		// hits this whenever a PG source carries `jsonb NOT NULL
		// DEFAULT '{}'::jsonb` (and the symmetric shapes on text /
		// bytea / geometry); pre-fix the migration died at CREATE
		// TABLE on the target with no recovery path.
		//
		// Smallest correct fix: drop the DEFAULT clause for these
		// types and warn loudly so the operator knows the column
		// will not auto-populate on the target. The follow-up note
		// fires when the column is also NOT NULL — that's the
		// failure-prone shape (INSERTs without an explicit value
		// will fail on the target).
		if mysqlForbidsDefault(c.Type) {
			logSuppressedDefault(c, dflt)
		} else {
			sb.WriteString(" DEFAULT ")
			sb.WriteString(dflt)
		}
	}
	if c.Comment != "" {
		sb.WriteString(" COMMENT ")
		sb.WriteString(quoteSQLString(c.Comment))
	}
	return sb.String(), nil
}

// inlineAutoIncrementIndex returns the secondary index that
// [emitTableDef] emits inline at CREATE TABLE time to satisfy MySQL's
// "every auto column must be a key" rule when the AUTO_INCREMENT
// column is NOT in the primary key. Returns nil when:
//
//   - the table has no AUTO_INCREMENT column (the common case)
//   - the AUTO_INCREMENT column IS in the primary key (also common —
//     the standard "id BIGINT AUTO_INCREMENT PRIMARY KEY" pattern;
//     PK itself satisfies the rule)
//   - no secondary index in table.Indexes has the auto column as its
//     leading column (sluice can't satisfy the rule from what's
//     declared; MySQL will reject the CREATE TABLE downstream and
//     the existing error path surfaces it — same as pre-v0.49.0)
//
// When non-nil, the returned index gets emitted inline by
// [emitTableDef] AND skipped by [CreateIndexes] (phase 2) to avoid a
// double-create on the same index name.
//
// GitHub issue #25: pre-v0.49.0 sluice's three-phase apply always
// deferred secondary indexes to phase 2, which made CREATE TABLE land
// without the auto column's supporting key and MySQL rejected with
// Error 1075 (Incorrect table definition; there can be only one auto
// column and it must be defined as a key).
//
// MySQL allows at most one AUTO_INCREMENT column per table, so at
// most one supporting index is needed.
func inlineAutoIncrementIndex(table *ir.Table) *ir.Index {
	if table == nil || len(table.Indexes) == 0 {
		return nil
	}
	pkCols := make(map[string]struct{})
	if table.PrimaryKey != nil {
		for _, c := range table.PrimaryKey.Columns {
			pkCols[c.Column] = struct{}{}
		}
	}
	var autoColName string
	for _, col := range table.Columns {
		intT, ok := col.Type.(ir.Integer)
		if !ok || !intT.AutoIncrement {
			continue
		}
		if _, inPK := pkCols[col.Name]; inPK {
			return nil
		}
		autoColName = col.Name
		break // MySQL: at most one AUTO_INCREMENT column per table.
	}
	if autoColName == "" {
		return nil
	}
	// Pick the first index whose leading column is the auto column.
	// Prefer unique indexes when both shapes are present — matches the
	// real-world `UNIQUE KEY uq_<table>_<col> (<col>)` operator
	// pattern (GitHub #25's example schema).
	var fallback *ir.Index
	for _, idx := range table.Indexes {
		if len(idx.Columns) == 0 || idx.Columns[0].Column != autoColName {
			continue
		}
		if idx.Unique {
			return idx
		}
		if fallback == nil {
			fallback = idx
		}
	}
	return fallback
}

// emitTableDef returns a CREATE TABLE statement with columns and
// the primary key inline (plus, when [inlineAutoIncrementIndex]
// matches, the supporting unique index required to satisfy MySQL's
// auto-column-is-key invariant). Secondary indexes and foreign keys
// are otherwise emitted separately by Phase 2 and Phase 3.
//
// The statement is terminated with a semicolon for readability; the
// driver doesn't require it but it keeps logged statements consistent
// with what a human would write.
func emitTableDef(table *ir.Table) (string, error) {
	if table == nil {
		return "", fmt.Errorf("mysql: emitTableDef: table is nil")
	}
	if len(table.Columns) == 0 {
		return "", fmt.Errorf("mysql: emitTableDef: table %q has no columns", table.Name)
	}

	var sb strings.Builder
	// IF NOT EXISTS keeps schema phase 1 idempotent: re-running
	// CreateTablesWithoutConstraints during a resume is a no-op when
	// the table is already there. MySQL has supported this for as
	// long as sluice cares about (5.x+).
	sb.WriteString("CREATE TABLE IF NOT EXISTS ")
	sb.WriteString(quoteIdent(table.Name))
	sb.WriteString(" (\n")

	hasPK := table.PrimaryKey != nil
	inlineIdx := inlineAutoIncrementIndex(table)
	hasInlineIdx := inlineIdx != nil
	for i, col := range table.Columns {
		def, err := emitColumnDef(col)
		if err != nil {
			return "", err
		}
		sb.WriteString("  ")
		sb.WriteString(def)
		if i < len(table.Columns)-1 || hasPK || hasInlineIdx || len(table.CheckConstraints) > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('\n')
	}

	if hasPK {
		sb.WriteString("  PRIMARY KEY ")
		sb.WriteString(emitIndexColumnList(table.PrimaryKey.Columns))
		if hasInlineIdx || len(table.CheckConstraints) > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('\n')
	}

	// GitHub #25: emit the supporting unique index for a non-PK
	// AUTO_INCREMENT column inline so MySQL accepts the CREATE TABLE.
	// Phase 2 (CreateIndexes) skips this same index via the same
	// [inlineAutoIncrementIndex] detector.
	if hasInlineIdx {
		sb.WriteString("  ")
		if inlineIdx.Unique {
			sb.WriteString("UNIQUE KEY ")
		} else {
			sb.WriteString("KEY ")
		}
		sb.WriteString(quoteIdent(inlineIdx.Name))
		sb.WriteByte(' ')
		sb.WriteString(emitIndexColumnList(inlineIdx.Columns))
		if len(table.CheckConstraints) > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('\n')
	}

	// CHECK constraints emit inline: they don't reference other tables
	// (unlike foreign keys), so deferring them past the bulk-copy
	// phase would gain nothing and lose diff-readability against the
	// source's pg_dump shape.
	for i, chk := range table.CheckConstraints {
		sb.WriteString("  ")
		sb.WriteString(emitCheckConstraint(chk))
		if i < len(table.CheckConstraints)-1 {
			sb.WriteByte(',')
		}
		sb.WriteByte('\n')
	}

	sb.WriteString(") ENGINE=InnoDB")
	if table.Comment != "" {
		sb.WriteString(" COMMENT=")
		sb.WriteString(quoteSQLString(table.Comment))
	}
	sb.WriteByte(';')

	return sb.String(), nil
}

// emitIndexColumnList renders a parenthesised, comma-separated list
// of index columns with optional prefix length and direction.
//
// Functional/expression entries (Expression non-empty, Column empty)
// render as `(expression_text)` — MySQL 8.0.13+ syntax requires the
// expression to be parenthesised, which combined with the outer
// parens of the column list produces the canonical double-parens
// shape `((LOWER(email)))`. Verbatim-passthrough policy applies: the
// expression text is preserved as-is, so non-portable constructs fail
// loudly on the target rather than be silently rewritten.
func emitIndexColumnList(cols []ir.IndexColumn) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		var entry string
		if c.Expression != "" {
			entry = "(" + c.Expression + ")"
		} else {
			entry = quoteIdent(c.Column)
			if c.Length > 0 {
				entry += fmt.Sprintf("(%d)", c.Length)
			}
		}
		if c.Desc {
			entry += " DESC"
		}
		parts[i] = entry
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// emitCreateIndex renders an ALTER TABLE ... ADD [UNIQUE] INDEX
// statement for a non-primary index. PRIMARY indexes are inline in
// the CREATE TABLE statement and must not be passed here.
func emitCreateIndex(tableName string, idx *ir.Index) (string, error) {
	if idx == nil {
		return "", fmt.Errorf("mysql: emitCreateIndex: index is nil")
	}
	if strings.EqualFold(idx.Name, "PRIMARY") {
		return "", fmt.Errorf("mysql: emitCreateIndex: PRIMARY index is inline in CREATE TABLE")
	}
	if len(idx.Columns) == 0 {
		return "", fmt.Errorf("mysql: emitCreateIndex: index %q has no columns", idx.Name)
	}

	var sb strings.Builder
	sb.WriteString("ALTER TABLE ")
	sb.WriteString(quoteIdent(tableName))
	sb.WriteString(" ADD ")

	switch {
	case idx.Kind == ir.IndexKindFullText:
		sb.WriteString("FULLTEXT INDEX ")
	case idx.Kind == ir.IndexKindSpatial:
		sb.WriteString("SPATIAL INDEX ")
	case idx.Unique:
		sb.WriteString("UNIQUE INDEX ")
	default:
		sb.WriteString("INDEX ")
	}

	if idx.Name != "" {
		sb.WriteString(quoteIdent(idx.Name))
		sb.WriteByte(' ')
	}
	sb.WriteString(emitIndexColumnList(idx.Columns))

	// Storage type: MySQL accepts USING BTREE / USING HASH for
	// regular indexes. FULLTEXT and SPATIAL ignore it.
	switch idx.Kind {
	case ir.IndexKindBTree:
		sb.WriteString(" USING BTREE")
	case ir.IndexKindHash:
		sb.WriteString(" USING HASH")
	}

	sb.WriteByte(';')
	return sb.String(), nil
}

// emitAddForeignKey renders an ALTER TABLE ... ADD CONSTRAINT
// statement for a foreign key on the given child table.
func emitAddForeignKey(childTable string, fk *ir.ForeignKey) (string, error) {
	if fk == nil {
		return "", fmt.Errorf("mysql: emitAddForeignKey: fk is nil")
	}
	if len(fk.Columns) == 0 || len(fk.ReferencedColumns) == 0 {
		return "", fmt.Errorf("mysql: emitAddForeignKey: fk %q has no columns", fk.Name)
	}
	if len(fk.Columns) != len(fk.ReferencedColumns) {
		return "", fmt.Errorf("mysql: emitAddForeignKey: fk %q column count mismatch (%d vs %d)",
			fk.Name, len(fk.Columns), len(fk.ReferencedColumns))
	}

	var sb strings.Builder
	sb.WriteString("ALTER TABLE ")
	sb.WriteString(quoteIdent(childTable))
	sb.WriteString(" ADD")
	if fk.Name != "" {
		sb.WriteString(" CONSTRAINT ")
		sb.WriteString(quoteIdent(fk.Name))
	}
	sb.WriteString(" FOREIGN KEY ")
	sb.WriteString(emitColumnList(fk.Columns))
	sb.WriteString(" REFERENCES ")
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

// emitCheckConstraint renders a CHECK clause inline within a CREATE
// TABLE column list:
//
//	CONSTRAINT `name` CHECK (expr)
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
func emitCheckConstraint(c *ir.CheckConstraint) string {
	var sb strings.Builder
	if c.Name != "" {
		sb.WriteString("CONSTRAINT ")
		sb.WriteString(quoteIdent(c.Name))
		sb.WriteByte(' ')
	}
	sb.WriteString("CHECK (")
	sb.WriteString(translateCheckExpr(c))
	sb.WriteByte(')')
	return sb.String()
}

// translateGeneratedExpr returns the generated-column expression to
// emit, applying the cross-dialect translation pass when the IR's
// dialect tag indicates a different source dialect (see ADR-0016).
// An empty / matching dialect tag emits verbatim — same behaviour as
// before the translation layer landed.
func translateGeneratedExpr(c *ir.Column) string {
	if c.GeneratedExprDialect == "" || c.GeneratedExprDialect == dialectName {
		return c.GeneratedExpr
	}
	return translateExprForMySQL(c.GeneratedExpr)
}

// translateCheckExpr returns the CHECK-constraint expression to emit,
// applying the cross-dialect translation pass when the IR's dialect
// tag indicates a different source dialect.
func translateCheckExpr(c *ir.CheckConstraint) string {
	if c.ExprDialect == "" || c.ExprDialect == dialectName {
		return c.Expr
	}
	return translateExprForMySQL(c.Expr)
}

// emitColumnList renders a parenthesised, comma-separated list of
// quoted column names. Used for FK column lists where IndexColumn's
// extras (length, direction) are not applicable.
func emitColumnList(cols []string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = quoteIdent(c)
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// mysqlForbidsDefault reports whether the given IR type maps to a
// MySQL column type that rejects a DEFAULT clause at CREATE TABLE
// (Error 1101: "BLOB, TEXT, GEOMETRY or JSON column ... can't have a
// default value"). The four families are MySQL's hard-coded set —
// the restriction is documented at
// https://dev.mysql.com/doc/refman/8.0/en/data-type-defaults.html.
// ir.Array also maps because emitColumnType routes it to JSON.
func mysqlForbidsDefault(t ir.Type) bool {
	switch t.(type) {
	case ir.JSON, ir.Text, ir.Blob, ir.Geometry, ir.Array:
		return true
	}
	// PG-extension types that translate to forbidding MySQL types:
	// hstore → JSON falls under the JSON case via the writer's
	// translator (emitColumnType emits "JSON"), but the IR shape
	// here is still ir.ExtensionType. Match on that.
	if ext, ok := t.(ir.ExtensionType); ok && ext.Extension == "hstore" {
		return true
	}
	return false
}

// logSuppressedDefault emits a WARN that names the column whose
// DEFAULT clause was suppressed because MySQL rejects DEFAULTs on
// the column's type family. The follow-up note fires when the
// column is also NOT NULL — without the default, INSERTs that don't
// specify a value will fail on the target. Operator workflow: drop
// the NOT NULL on the source, or supply the value at write time.
func logSuppressedDefault(c *ir.Column, suppressed string) {
	slog.Warn("cross-engine: dropping DEFAULT on MySQL forbidding-type column; MySQL forbids DEFAULTs on JSON/TEXT/BLOB/GEOMETRY (Error 1101)",
		slog.String("column", c.Name),
		slog.String("type", fmt.Sprintf("%T", c.Type)),
		slog.String("suppressed_default", suppressed),
	)
	if !c.Nullable {
		slog.Warn("cross-engine: column is NOT NULL; INSERTs without an explicit value will fail. Consider DROP NOT NULL on source or supply the value at write time",
			slog.String("column", c.Name),
		)
	}
}

// typeName returns a human-readable name for an IR type, used in
// error messages.
func typeName(t ir.Type) string {
	return fmt.Sprintf("%T", t)
}
