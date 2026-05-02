package mysql

import (
	"fmt"
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

	// ---- Unsupported on MySQL ----
	case ir.Array, ir.Inet, ir.Cidr, ir.Macaddr:
		return "", fmt.Errorf("mysql: %s has no native MySQL type; translate before writing", typeName(t))
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
// their MySQL equivalents. The most common case is now() (PG's
// "current timestamp" function name), which MySQL spells
// CURRENT_TIMESTAMP. Anything not in this map passes through
// verbatim — loud failure on the target beats silent corruption,
// per the project's translation policy.
//
// Lookup keys are lowercased + trimmed; PG's pg_get_expr output
// isn't case-stable across versions, and incidental whitespace
// can survive cast-stripping, so normalising before lookup keeps
// the table tiny.
var pgToMySQLDefaultExpr = map[string]string{
	"now()": "CURRENT_TIMESTAMP",
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
		if translated, ok := pgToMySQLDefaultExpr[normalized]; ok {
			return translated, true
		}
		return v.Expr, true
	}
	return "", false
}

// emitColumnDef returns the full DDL fragment for a single column,
// suitable for inclusion in a CREATE TABLE column list:
//
//	`name` TYPE [NOT NULL] [DEFAULT ...] [COMMENT '...']
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
	if !c.Nullable {
		sb.WriteString(" NOT NULL")
	}
	if dflt, ok := emitDefault(c.Default, c.Type); ok {
		sb.WriteString(" DEFAULT ")
		sb.WriteString(dflt)
	}
	if c.Comment != "" {
		sb.WriteString(" COMMENT ")
		sb.WriteString(quoteSQLString(c.Comment))
	}
	return sb.String(), nil
}

// emitTableDef returns a CREATE TABLE statement with columns and
// the primary key inline. Secondary indexes and foreign keys are
// emitted separately by Phase 2 and Phase 3.
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
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(quoteIdent(table.Name))
	sb.WriteString(" (\n")

	for i, col := range table.Columns {
		def, err := emitColumnDef(col)
		if err != nil {
			return "", err
		}
		sb.WriteString("  ")
		sb.WriteString(def)
		if i < len(table.Columns)-1 || table.PrimaryKey != nil {
			sb.WriteByte(',')
		}
		sb.WriteByte('\n')
	}

	if table.PrimaryKey != nil {
		sb.WriteString("  PRIMARY KEY ")
		sb.WriteString(emitIndexColumnList(table.PrimaryKey.Columns))
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
func emitIndexColumnList(cols []ir.IndexColumn) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		entry := quoteIdent(c.Column)
		if c.Length > 0 {
			entry += fmt.Sprintf("(%d)", c.Length)
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

// typeName returns a human-readable name for an IR type, used in
// error messages.
func typeName(t ir.Type) string {
	return fmt.Sprintf("%T", t)
}
