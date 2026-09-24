// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// DEFAULTs on MySQL's large-object column families (GC-36 item 4).
//
// MySQL rejects a plain literal DEFAULT on a TEXT, BLOB, JSON or GEOMETRY
// column (Error 1101), and before 8.0.13 it rejected every DEFAULT there.
// This writer used to drop every such DEFAULT with a WARN. That was a
// metadata loss on CREATE TABLE (the copied rows carry their own values),
// but on a forwarded `ADD COLUMN` it was silent corruption: the target
// fills every row it already holds with the column's DEFAULT, the source
// fill emits no row event, so every pre-existing target row held NULL
// where the source row held the declared default — at exit 0.
//
// What the servers actually accept, measured on MySQL 8.0.46, 8.4.11 and
// MariaDB 11.4.13 by adding each form to a table that already held rows
// and reading the rows back:
//
//	form                                 MySQL >= 8.0.13   MariaDB
//	DEFAULT 'txt' on TEXT/JSON           Error 1101        accepted
//	DEFAULT ('txt') on TEXT/JSON         accepted          accepted (stored as 'txt')
//	DEFAULT (X'00AB00') on BLOB          accepted          accepted (stored as X'00ab00')
//	DEFAULT (ST_GeomFromText(…))         accepted          accepted
//
// so the one parenthesised form serves both server families. MariaDB
// has accepted it since 10.2.1, well below sluice's 10.11 floor.
//
// # The backslash wart (MYSQL-EXPRESSION-DEFAULT-BACKSLASH)
//
// MySQL stores an expression DEFAULT as TEXT, printing each string
// literal with backslash escapes (`'` → `\'`, `\` → `\\`, NUL → `\0`,
// LF → `\n`, CR → `\r`, 0x1A → `\Z`; `"`, TAB and everything else raw),
// and re-parses that text when the table definition is next opened —
// under the sql_mode of whichever session opens it. Measured on 8.0.46 and
// 8.4.11: after a reload, a NO_BACKSLASH_ESCAPES session's first insert
// evaluates `DEFAULT ('x\\y')` to `x\\y` (two backslashes, for every later
// insert too), and `DEFAULT ('it''s')` fails that insert with Error 1064.
// Even `_utf8mb4 X'…'` does not escape it: MySQL folds the hex literal
// back into an escaped string literal before storing it. A string holding
// any character that printer escapes is therefore spelled
// `CONVERT(UNHEX('<hex>') USING utf8mb4)`, which MySQL keeps as a function
// call over a hex-digit literal — nothing to escape, so nothing a later
// session can re-read differently. MariaDB normalises a constant DEFAULT
// to a literal and does not share the defect (measured), so it keeps the
// ordinary quoted spelling.

// lobFamily names the MySQL large-object family a column's type lands in
// on this writer — the families whose DEFAULT MySQL restricts.
type lobFamily int

const (
	lobNone lobFamily = iota
	lobText
	lobBlob
	lobJSON
	lobArray  // emitted as JSON; the default is a PG array literal
	lobHstore // emitted as JSON; the default is hstore text
	lobGeometry
)

// mysqlLOBFamily reports which large-object family t is EMITTED as, or
// lobNone. It asks the storage type (Bug 233: a DOMAIN over text emits
// LONGTEXT) and follows every emitColumnType arm that lands on TEXT,
// BLOB, JSON or GEOMETRY — including the wide-VARCHAR down-map to a TEXT
// tier (Bug 72), which the pre-GC-36 type check missed, so such a
// column's DEFAULT reached MySQL as a bare literal and died with 1101.
func mysqlLOBFamily(t ir.Type) lobFamily {
	switch v := ir.UnwrapDomain(t).(type) {
	case ir.Text:
		return lobText
	case ir.Varchar:
		if _, downmap := mysqlTextTierForWideVarchar(v.Length); downmap {
			return lobText
		}
	case ir.Blob:
		return lobBlob
	case ir.JSON:
		return lobJSON
	case ir.Array:
		return lobArray
	case ir.Geometry:
		return lobGeometry
	case ir.ExtensionType:
		if v.Extension == "hstore" {
			return lobHstore
		}
	}
	return lobNone
}

// lobFamilyOfColumn is [mysqlLOBFamily] for a column, reading through a
// retarget or `--type-override` that rewrote an array or hstore into
// JSON: the default is still the source's array / hstore literal, and
// [ir.Column.SourceColumnType] is where the rewrite parked the type that
// says so (the same provenance the row writer's array lane reads —
// columnIsArrayLike). A forwarded ADD COLUMN reaches this writer that way.
func lobFamilyOfColumn(c *ir.Column) lobFamily {
	fam := mysqlLOBFamily(c.Type)
	if fam == lobJSON && c.SourceColumnType != nil {
		if src := mysqlLOBFamily(c.SourceColumnType); src == lobArray || src == lobHstore {
			return src
		}
	}
	return fam
}

// lobDefaultForm is what the TARGET server accepts as a DEFAULT on a
// large-object column. The zero value is the conservative one: an emitter
// that never learned the server version (stdEmitter, a failed probe)
// treats the target as unable to hold the default, which drops it with a
// WARN on CREATE TABLE and refuses a forwarded ADD COLUMN — loud either
// way, never a silently NULL-filled column.
type lobDefaultForm int

const (
	// lobDefaultNone: MySQL before 8.0.13, or a version sluice could not read.
	lobDefaultNone lobDefaultForm = iota
	// lobDefaultMySQL: MySQL 8.0.13+ (PlanetScale / Vitess report the
	// underlying MySQL version) — parenthesised expressions only.
	lobDefaultMySQL
	// lobDefaultMariaDB: MariaDB 10.2.1+ — literals and expressions.
	lobDefaultMariaDB
)

// lobDefaultFormFor classifies a SELECT VERSION() string.
func lobDefaultFormFor(version string) lobDefaultForm {
	if major, minor, ok := parseMariaDBVersion(version); ok {
		if major > 10 || (major == 10 && minor >= 2) {
			return lobDefaultMariaDB
		}
		return lobDefaultNone
	}
	m := mysqlVersionPattern.FindStringSubmatch(version)
	if m == nil {
		return lobDefaultNone
	}
	var major, minor, patch int
	_, _ = fmt.Sscanf(m[1]+" "+m[2]+" "+m[3], "%d %d %d", &major, &minor, &patch)
	if major > 8 || (major == 8 && (minor > 0 || patch >= 13)) {
		return lobDefaultMySQL
	}
	return lobDefaultNone
}

// fitDefaultToColumn turns the DEFAULT body [mysqlEmitter.emitDefault]
// rendered into the form the target accepts for c's column family. lost is
// non-empty when the default cannot be carried faithfully, and says why;
// the body is then meaningless and the caller must not emit it.
func (m mysqlEmitter) fitDefaultToColumn(c *ir.Column, rendered string) (body, lost string) {
	fam := lobFamilyOfColumn(c)
	if fam == lobNone {
		return rendered, ""
	}
	if m.lobDefaults == lobDefaultNone {
		return "", "the target server accepts no DEFAULT on a TEXT/BLOB/JSON/GEOMETRY column " +
			"(MySQL before 8.0.13, or a server whose version sluice could not read)"
	}
	lit, isLiteral := c.Default.(ir.DefaultLiteral)
	if !isLiteral {
		// An expression body is already in MySQL spelling; it only needs the
		// outer parentheses a large-object column insists on.
		return parenthesizeDefault(rendered), ""
	}
	switch fam {
	case lobText:
		return m.lobStringDefault(lit.Value), ""
	case lobBlob:
		b, err := blobDefaultLiteralBytes(lit.Value)
		if err != nil {
			return "", err.Error()
		}
		return "(X'" + strings.ToUpper(hex.EncodeToString(b)) + "')", ""
	case lobJSON:
		if !json.Valid([]byte(lit.Value)) {
			return "", fmt.Sprintf("the default %q is not a JSON document", lit.Value)
		}
		return m.lobStringDefault(lit.Value), ""
	case lobArray:
		doc, err := arrayDefaultJSON(lit.Value, arrayElementType(c))
		if err != nil {
			return "", err.Error()
		}
		return m.lobStringDefault(doc), ""
	case lobHstore:
		pairs, err := parseHstoreText(lit.Value)
		if err != nil {
			return "", fmt.Sprintf("the hstore default %q does not parse: %v", lit.Value, err)
		}
		doc, err := json.Marshal(pairs)
		if err != nil {
			return "", err.Error()
		}
		return m.lobStringDefault(string(doc)), ""
	default: // lobGeometry
		return "", "a geometry literal DEFAULT has no MySQL spelling sluice can prove equivalent"
	}
}

// arrayDefaultJSON renders a Postgres array-literal DEFAULT as the JSON
// document a copied row of the same column lands as. A row's elements
// reach the writer already decoded by the Postgres reader
// (decodeValueFromText, which this package cannot call), and
// json.Marshal renders each Go leaf; the literal's tokens are text. So
// only the element families whose decoded leaf is recoverable from the
// token alone are carried — strings stay strings, integers become JSON
// numbers, t/f become booleans — and every other family (float, numeric,
// uuid, temporal, json, bytea, …) is refused rather than rendered in a
// shape the copied rows might not share. A multi-dimensional literal is
// refused by the parser. The independent check is the pre-existing-row
// gate, which compares the target against Postgres's own to_jsonb.
func arrayDefaultJSON(literal string, elem ir.Type) (string, error) {
	tokens, err := pgArrayLiteralTokens(literal)
	if err != nil {
		return "", fmt.Errorf("the default %q is not a one-dimensional array literal: %w", literal, err)
	}
	leaves := make([]any, len(tokens))
	for i, tok := range tokens {
		s, isString := tok.(string)
		if !isString {
			continue // a NULL element stays JSON null
		}
		switch ir.UnwrapDomain(elem).(type) {
		case ir.Text, ir.Varchar, ir.Char:
			leaves[i] = s
		case ir.Integer:
			if _, err := strconv.ParseInt(s, 10, 64); err != nil {
				if _, uerr := strconv.ParseUint(s, 10, 64); uerr != nil {
					return "", fmt.Errorf("array default element %q is not an integer", s)
				}
			}
			leaves[i] = json.Number(s)
		case ir.Boolean:
			switch s {
			case "t", "true":
				leaves[i] = true
			case "f", "false":
				leaves[i] = false
			default:
				return "", fmt.Errorf("array default element %q is not a boolean", s)
			}
		default:
			return "", fmt.Errorf("an array DEFAULT of %T elements has no JSON rendering sluice can prove matches "+
				"the rows it copies", elem)
		}
	}
	doc, err := json.Marshal(leaves)
	if err != nil {
		return "", err
	}
	return string(doc), nil
}

// lobStringDefault spells a string DEFAULT for a large-object column. See
// the MYSQL-EXPRESSION-DEFAULT-BACKSLASH note above for the UNHEX arm.
func (m mysqlEmitter) lobStringDefault(s string) string {
	if m.lobDefaults == lobDefaultMySQL && strings.ContainsAny(s, mysqlExpressionPrinterEscapes) {
		return "(CONVERT(UNHEX('" + strings.ToUpper(hex.EncodeToString([]byte(s))) + "') USING utf8mb4))"
	}
	return "(" + m.quoteSQLString(s) + ")"
}

// mysqlExpressionPrinterEscapes is the set of characters MySQL's stored-
// expression printer spells with a backslash (measured; see the
// MYSQL-EXPRESSION-DEFAULT-BACKSLASH note above).
const mysqlExpressionPrinterEscapes = "\\'\x00\n\r\x1a"

// parenthesizeDefault wraps a DEFAULT body in the parentheses MySQL
// requires on a large-object column, unless it already carries them.
func parenthesizeDefault(body string) string {
	t := strings.TrimSpace(body)
	if strings.HasPrefix(t, "(") && strings.HasSuffix(t, ")") {
		return t
	}
	return "(" + t + ")"
}

// blobDefaultLiteralBytes returns the bytes a DefaultLiteral on a bytes
// column stands for.
//
// # BLOB-DEFAULT-LITERAL-ENCODING
//
// [ir.DefaultLiteral] carries no encoding, and a bytes default has had two:
// a raw-bytes literal (a SQLite or mydumper BLOB default), and Postgres's
// bytea text, which a manifest written before GC-36 still carries (`\x…`
// hex, or the escape format). Every current reader that knows the bytes
// hands them over as a hex DefaultExpression instead — Postgres for bytea,
// MySQL and MariaDB for BINARY/VARBINARY, MariaDB for BLOB (a MySQL BLOB
// default is only ever an expression) — so a non-empty literal
// reaching this function is one of the two ambiguous shapes. They coincide
// exactly when the literal holds no backslash: bytea's escape format
// treats every other byte as itself, and the hex format starts with one.
// So a backslash-free literal is its own bytes, and any other is refused
// rather than read one way or the other by looking at it (the ambiguity
// audit B-2 / roadmap item 135 closed for row values; `\xdead` is both six
// bytes and two).
func blobDefaultLiteralBytes(s string) ([]byte, error) {
	if strings.Contains(s, `\`) {
		return nil, fmt.Errorf("the bytes default %q holds a backslash, so it is either raw bytes or a "+
			"PostgreSQL bytea rendering and nothing records which", s)
	}
	return []byte(s), nil
}

// logSuppressedDefault emits the CREATE-path WARN for a DEFAULT the target
// cannot hold (see [mysqlEmitter.fitDefaultToColumn]). On CREATE TABLE the
// copied rows carry their own values, so only later inserts that omit the
// column lose the default; the NOT NULL follow-up names the shape where
// those inserts fail outright. The forwarded ADD COLUMN path refuses
// instead ([refuseLOBDefaultNotCarried]), because there the default is the
// value of every row the target already holds.
func logSuppressedDefault(tableName string, c *ir.Column, suppressed, reason string) {
	slog.Warn(
		"mysql: LOB-DEFAULT-NOT-CARRIED: dropping the column's DEFAULT; the target column has none — "+
			"re-create it on the target if your application relies on it",
		slog.String("table", tableName),
		slog.String("column", c.Name),
		slog.String("type", fmt.Sprintf("%T", c.Type)),
		slog.String("suppressed_default", suppressed),
		slog.String("reason", reason),
	)
	if !c.Nullable {
		slog.Warn(
			"mysql: column is NOT NULL; INSERTs without an explicit value will fail. Consider DROP NOT NULL on source or supply the value at write time",
			slog.String("table", tableName),
			slog.String("column", c.Name),
		)
	}
}

// lobDefaultLoss reports why c's DEFAULT cannot be carried onto this
// target, or "" when it can (or c has none to carry).
func (m mysqlEmitter) lobDefaultLoss(c *ir.Column) string {
	rendered, ok := m.emitDefault(c.Default, c.Type)
	if !ok {
		return ""
	}
	_, lost := m.fitDefaultToColumn(c, rendered)
	return lost
}

// refuseLOBDefaultNotCarried is the ADD COLUMN refusal: adding the column
// without its DEFAULT would fill every row the target table already holds
// with NULL where the source rows hold the declared default.
func refuseLOBDefaultNotCarried(table, column, reason string) error {
	return sluicecode.Wrap(
		sluicecode.CodeValueUnrepresentable,
		"add the column on the target yourself with a value for the existing rows, then resume; "+
			"or exclude the table",
		fmt.Errorf("mysql: LOB-DEFAULT-NOT-CARRIED: refusing ADD COLUMN %s.%s: %s — adding it without the DEFAULT "+
			"would leave every row the target already holds NULL where the source holds the default",
			quoteIdent(table), quoteIdent(column), reason),
	)
}
