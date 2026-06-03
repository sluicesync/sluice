// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// columnMeta is the subset of information_schema.columns the type
// translator needs. Keeping it as a plain struct keeps the translator
// pure (no database dependency) and trivially testable.
type columnMeta struct {
	// DataType is information_schema.columns.data_type — e.g. "int",
	// "varchar", "tinyint". It is normalised and lowercase.
	DataType string

	// ColumnType is information_schema.columns.column_type — e.g.
	// "int(11) unsigned", "tinyint(1)", "enum('a','b')". This is the
	// rich form the translator inspects for unsigned-ness, ENUM/SET
	// values, and BIT widths. Lowercase.
	ColumnType string

	// CharMaxLen is character_maximum_length, or nil when not applicable.
	CharMaxLen *int64

	// NumPrec is numeric_precision, or nil when not applicable.
	NumPrec *int64

	// NumScale is numeric_scale, or nil when not applicable.
	NumScale *int64

	// DTPrec is datetime_precision, or nil when not applicable.
	DTPrec *int64

	// Charset is character_set_name (empty when not applicable).
	Charset string

	// Collation is collation_name (empty when not applicable).
	Collation string

	// SrsID is information_schema.columns.srs_id — the spatial
	// reference system ID for geometry columns (0 / unset for non-
	// geometry columns and for geometry columns without an explicit
	// SRID). Used to populate ir.Geometry.SRID so the cross-engine
	// emit path lands `geometry(POINT, 4326)` on PG instead of
	// `geometry(POINT, 0)` (Bug 26). MySQL stores this in
	// information_schema as of 8.0; older versions ignore the
	// column.
	SrsID int

	// Extra is information_schema.columns.extra — contains tokens like
	// "auto_increment" and "DEFAULT_GENERATED". Lowercase.
	Extra string
}

// translateType maps a single information_schema.columns row to an IR
// type. It is a pure function, deliberately, so it can be exhaustively
// tested without a database.
//
// Unrecognised types are surfaced as an error rather than silently
// converted to a fallback — this is a deliberate choice from the
// project's "contain complexity" tenet. A user encountering an
// unsupported type should see an explicit message naming the type.
func translateType(c columnMeta) (ir.Type, error) {
	// information_schema.columns.column_type is read raw (no LOWER()
	// in the query) so embedded ENUM/SET values keep their source-
	// side casing. The keyword tokens we match here (unsigned,
	// enum, set, bit) are normalised lowercase via strings.ToLower
	// at each call site.
	unsigned := strings.Contains(strings.ToLower(c.ColumnType), "unsigned")
	autoIncrement := strings.Contains(strings.ToLower(c.Extra), "auto_increment")

	switch c.DataType {
	// ---- Integer family ----

	case "tinyint":
		// tinyint(1) is the conventional MySQL boolean. Other display
		// widths are 8-bit signed/unsigned integers.
		if displayWidth(c.ColumnType) == 1 && !unsigned && !autoIncrement {
			return ir.Boolean{}, nil
		}
		return ir.Integer{Width: 8, Unsigned: unsigned, AutoIncrement: autoIncrement}, nil
	case "smallint":
		return ir.Integer{Width: 16, Unsigned: unsigned, AutoIncrement: autoIncrement}, nil
	case "mediumint":
		return ir.Integer{Width: 24, Unsigned: unsigned, AutoIncrement: autoIncrement}, nil
	case "int", "integer":
		return ir.Integer{Width: 32, Unsigned: unsigned, AutoIncrement: autoIncrement}, nil
	case "bigint":
		return ir.Integer{Width: 64, Unsigned: unsigned, AutoIncrement: autoIncrement}, nil
	case "year":
		// MySQL YEAR is a 1-byte integer storing 1901-2155 plus 0000.
		// Loss is in name only; data is preserved.
		return ir.Integer{Width: 16}, nil

	// ---- Decimal / float ----

	case "decimal", "numeric":
		return ir.Decimal{Precision: int(int64Ptr(c.NumPrec)), Scale: int(int64Ptr(c.NumScale))}, nil
	case "float":
		return ir.Float{Precision: ir.FloatSingle}, nil
	case "double", "real":
		return ir.Float{Precision: ir.FloatDouble}, nil

	// ---- Bit ----

	case "bit":
		w := bitWidth(c.ColumnType)
		if w == 1 {
			// The conventional single-bit column is a boolean; this is
			// the validated round-trip (bit(1) → TINYINT(1) / BOOLEAN).
			return ir.Boolean{}, nil
		}
		// BIT(N) for N > 1 is a fixed-width bit string. Modelling it as
		// Varbinary (pre-v0.65.1) mis-typed the column on every target
		// (VARBINARY(1) on MySQL → Error 1067; BYTEA on PG) and forced
		// the bit-literal default into a decimal string (catalog Bug
		// 62). ir.Bit round-trips losslessly MySQL BIT(N) ↔ PG bit(N).
		return ir.Bit{Length: w}, nil

	// ---- Strings ----

	case "char":
		return ir.Char{Length: int(int64Ptr(c.CharMaxLen)), Charset: c.Charset, Collation: c.Collation}, nil
	case "varchar":
		return ir.Varchar{Length: int(int64Ptr(c.CharMaxLen)), Charset: c.Charset, Collation: c.Collation}, nil
	case "tinytext":
		return ir.Text{Size: ir.TextTiny, Charset: c.Charset, Collation: c.Collation}, nil
	case "text":
		return ir.Text{Size: ir.TextRegular, Charset: c.Charset, Collation: c.Collation}, nil
	case "mediumtext":
		return ir.Text{Size: ir.TextMedium, Charset: c.Charset, Collation: c.Collation}, nil
	case "longtext":
		return ir.Text{Size: ir.TextLong, Charset: c.Charset, Collation: c.Collation}, nil

	// ---- Binary ----

	case "binary":
		return ir.Binary{Length: int(int64Ptr(c.CharMaxLen))}, nil
	case "varbinary":
		return ir.Varbinary{Length: int(int64Ptr(c.CharMaxLen))}, nil
	case "tinyblob":
		return ir.Blob{Size: ir.BlobTiny}, nil
	case "blob":
		return ir.Blob{Size: ir.BlobRegular}, nil
	case "mediumblob":
		return ir.Blob{Size: ir.BlobMedium}, nil
	case "longblob":
		return ir.Blob{Size: ir.BlobLong}, nil

	// ---- Temporal ----

	case "date":
		return ir.Date{}, nil
	case "time":
		return ir.Time{Precision: int(int64Ptr(c.DTPrec))}, nil
	case "datetime":
		return ir.DateTime{Precision: int(int64Ptr(c.DTPrec))}, nil
	case "timestamp":
		// MySQL TIMESTAMP is always stored as UTC and converted on
		// retrieval, so semantically it is a zoned timestamp.
		return ir.Timestamp{Precision: int(int64Ptr(c.DTPrec)), WithTimeZone: true}, nil

	// ---- Categorical (extension types) ----

	case "enum":
		values, err := parseEnumOrSet(c.ColumnType, "enum")
		if err != nil {
			return nil, err
		}
		warnIfLikelyTruncatedEnumLabel(c.ColumnType, "enum", values)
		return ir.Enum{Values: values}, nil
	case "set":
		values, err := parseEnumOrSet(c.ColumnType, "set")
		if err != nil {
			return nil, err
		}
		warnIfLikelyTruncatedEnumLabel(c.ColumnType, "set", values)
		return ir.Set{Values: values}, nil

	// ---- JSON ----

	case "json":
		return ir.JSON{Binary: true}, nil

	// ---- Geometry (extension type) ----
	//
	// Each geometry case threads c.SrsID through to ir.Geometry.SRID
	// so the cross-engine emit path lands `geometry(POINT, 4326)`
	// on PG instead of dropping the SRID (Bug 26). Source columns
	// without an explicit SRID get 0, which both engines treat as
	// "no spatial reference system" — same semantics as the pre-fix
	// state.

	case "geometry":
		return ir.Geometry{Subtype: ir.GeometryUnspecified, SRID: c.SrsID}, nil
	case "point":
		return ir.Geometry{Subtype: ir.GeometryPoint, SRID: c.SrsID}, nil
	case "linestring":
		return ir.Geometry{Subtype: ir.GeometryLineString, SRID: c.SrsID}, nil
	case "polygon":
		return ir.Geometry{Subtype: ir.GeometryPolygon, SRID: c.SrsID}, nil
	case "multipoint":
		return ir.Geometry{Subtype: ir.GeometryMultiPoint, SRID: c.SrsID}, nil
	case "multilinestring":
		return ir.Geometry{Subtype: ir.GeometryMultiLineString, SRID: c.SrsID}, nil
	case "multipolygon":
		return ir.Geometry{Subtype: ir.GeometryMultiPolygon, SRID: c.SrsID}, nil
	case "geometrycollection", "geomcollection":
		return ir.Geometry{Subtype: ir.GeometryCollection, SRID: c.SrsID}, nil
	}

	return nil, fmt.Errorf("mysql: unsupported data_type %q (column_type %q)", c.DataType, c.ColumnType)
}

// displayWidth extracts the display width N from a column_type of the
// form "tinyint(N)", "int(N) unsigned", etc. Returns 0 when no width
// is present.
func displayWidth(columnType string) int {
	open := strings.IndexByte(columnType, '(')
	if open < 0 {
		return 0
	}
	closeIdx := strings.IndexByte(columnType[open:], ')')
	if closeIdx < 0 {
		return 0
	}
	inner := columnType[open+1 : open+closeIdx]
	n, err := atoiPositive(inner)
	if err != nil {
		return 0
	}
	return n
}

// bitWidth extracts N from a "bit(N)" column_type. Returns 1 when the
// width is missing, matching MySQL's documented default for BIT.
func bitWidth(columnType string) int {
	w := displayWidth(columnType)
	if w == 0 {
		return 1
	}
	return w
}

// parseEnumOrSet pulls the value list out of an ENUM/SET column_type.
//
// MySQL formats these as enum('red','green','blue') and similar; values
// containing escaped single quotes (doubled inside the literal) are
// handled by the inner loop. The kind prefix (`enum`/`set`) is matched
// case-insensitively against the column_type string — the *values*
// inside the parens preserve their source-side casing.
func parseEnumOrSet(columnType, kind string) ([]string, error) {
	expected := kind + "("
	lower := strings.ToLower(columnType)
	idx := strings.Index(lower, expected)
	if idx != 0 {
		return nil, fmt.Errorf("mysql: malformed %s column_type %q", strings.ToUpper(kind), columnType)
	}
	body := columnType[len(expected):]
	if !strings.HasSuffix(body, ")") {
		return nil, fmt.Errorf("mysql: %s column_type missing closing paren: %q", strings.ToUpper(kind), columnType)
	}
	body = strings.TrimSuffix(body, ")")

	var values []string
	for body != "" {
		if body[0] != '\'' {
			return nil, fmt.Errorf("mysql: malformed %s value list near %q", strings.ToUpper(kind), body)
		}
		// Find the closing quote, accounting for doubled-quote escapes.
		var sb strings.Builder
		i := 1
		for i < len(body) {
			if body[i] == '\'' {
				if i+1 < len(body) && body[i+1] == '\'' {
					sb.WriteByte('\'')
					i += 2
					continue
				}
				break
			}
			sb.WriteByte(body[i])
			i++
		}
		if i >= len(body) {
			return nil, fmt.Errorf("mysql: unterminated %s value: %q", strings.ToUpper(kind), columnType)
		}
		values = append(values, sb.String())

		body = body[i+1:]
		switch {
		case body == "":
			// done
		case strings.HasPrefix(body, ","):
			body = body[1:]
		default:
			return nil, fmt.Errorf("mysql: malformed %s value list (expected , or end) near %q", strings.ToUpper(kind), body)
		}
	}
	return values, nil
}

// atoiPositive parses a non-negative integer from s, returning an
// error for negative values, leading whitespace, or non-digit input.
func atoiPositive(s string) (int, error) {
	if s == "" {
		return 0, errors.New("empty input")
	}
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("non-digit in %q", s)
		}
		n = n*10 + int(r-'0')
	}
	return n, nil
}

// int64Ptr returns *p, or 0 if p is nil. Used to translate
// information_schema's nullable numeric columns into the IR's int
// fields without per-call nil checks.
func int64Ptr(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

// warnIfLikelyTruncatedEnumLabel emits an INFO-level warning when an
// ENUM/SET label looks like a MySQL-truncated 4-byte UTF-8 sequence
// (Bug 106 / v0.92.2). MySQL's data dictionary silently substitutes
// `?` for supplementary-plane characters at CREATE TABLE time
// regardless of the column's charset; mysqldump reproduces the same
// loss. There is no recovery from sluice's side — the original code
// point is gone before sluice ever sees the column. The warning gives
// operators visibility so they can investigate before the runtime
// row-INSERT loud-fails ("invalid input value for enum ..." on PG).
//
// Heuristic (deliberately narrow to keep false positives low): warn
// only when the column_type string contains a `?` character. A bare
// `?` is legitimate as an ENUM label (e.g. enum('y','n','?')) but
// rare enough that surfacing it once per column at read time is
// acceptable, and the warning text names the recovery path so an
// operator with a legitimate `?` label can ignore it knowingly.
func warnIfLikelyTruncatedEnumLabel(columnType, kind string, values []string) {
	if !strings.Contains(columnType, "?") {
		return
	}
	slog.Warn(
		"mysql: "+kind+" labels contain '?' — likely MySQL data-dictionary truncation of 4-byte UTF-8 (Bug 106)",
		slog.String("column_type", columnType),
		slog.Any("labels", values),
		slog.String("hint", "MySQL's data dictionary silently truncates supplementary-plane (4-byte UTF-8) characters in ENUM/SET labels to '?' at CREATE TABLE time, regardless of CHARSET=utf8mb4. mysqldump reproduces the same loss. If this column's source rows contain non-'?' values, the target write will loud-fail at row INSERT. Recovery: widen this column to VARCHAR/TEXT via --type-override; or fix the source ENUM labels to ASCII before migration; or ignore this warning if your data legitimately uses '?' as a label."),
	)
}
