// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// Binary column-default recovery — working around a MySQL
// information_schema limitation (Finding C, the silent-loss residue of the
// v0.99.186 BINARY-default fix).
//
// MySQL's information_schema.COLUMN_DEFAULT is a text column, and MySQL
// C-string-truncates a BINARY/VARBINARY column's literal default at its
// FIRST NUL byte before storing it there. So the metadata read is lossy for
// ANY binary default that contains a 0x00 anywhere:
//
//	BINARY(1)  DEFAULT 0x00        → COLUMN_DEFAULT "0x"          (empty)
//	BINARY(4)  DEFAULT 0x00FF00FF  → COLUMN_DEFAULT "0x"          (empty)
//	BINARY(2)  DEFAULT 0x2700      → COLUMN_DEFAULT "0x27"        (well-formed but SHORT)
//	BINARY(2)  DEFAULT 0xFF00      → COLUMN_DEFAULT "0xFF"        (well-formed but SHORT)
//	BINARY(3)  DEFAULT 0xFFEEDD    → COLUMN_DEFAULT "0xFFEEDD"    (no NUL — faithful)
//
// The bare-"0x" cases fall through v0.99.186's hexLiteralDefault (no digits)
// and would emit a wrong `'0x'` literal; the truncated-but-well-formed cases
// (0x2700 → "0x27") are the more dangerous ones — hexLiteralDefault ACCEPTS
// them and every emitter renders a value that is silently short of the real
// default. There is no signal in COLUMN_DEFAULT alone that distinguishes a
// truncation from a genuine short value, so the class cannot be detected from
// the metadata; the only safe move is to re-read the true bytes.
//
// SHOW CREATE TABLE reports the byte-exact stored default (BINARY zero-padded
// to its declared width, VARBINARY as-written) in one of two forms, empirically
// enumerated on MySQL 8.0:
//
//   - hex literal `0x<even-hex>` — used when the value contains any byte ≥ 0x80.
//   - single-quoted escaped string `'…'` — used otherwise. The escape set MySQL
//     emits is: `\0`→0x00, `\b`→0x08, `\t`→0x09, `\n`→0x0A, `\r`→0x0D,
//     `\\`→0x5C, and a doubled single-quote →0x27 (the SQL-standard quote
//     escape). Every other byte < 0x80 — including 0x01, 0x1A, 0x7F, `"`, space,
//     `%`, `_` and all printable ASCII — is emitted RAW (MySQL does NOT emit
//     `\Z`/`\"`/`\'` here in this position). The decoder
//     below also accepts those documented escapes and treats an unknown `\x` as
//     the literal `x` (MySQL's own string-literal rule), so it is robust to any
//     form MySQL might emit.
//
// The recovered bytes are re-encoded to the same `0x<hex>` hexLiteralDialect
// DefaultExpression the v0.99.186 emitters already consume (MySQL bare `0x…`,
// PG `'\x…'::bytea`, SQLite `X'…'`, with the fixed-width NUL-padding on each
// side). Because SHOW CREATE already zero-pads BINARY(N) to width, the emitters'
// pad-to-width is a byte-identical no-op on the recovered literal.
//
// Residual safety net: if SHOW CREATE is unavailable, or a column's default
// clause is not one of the two forms above, the default is DROPPED with a loud
// WARN naming the column (a DEFAULT-less target column is safe; a wrong-bytes
// default is not) — never silently carried.

// pendingBinaryDefault records a binary-family column whose literal default was
// reported by information_schema as a (possibly NUL-truncated) hex literal and
// therefore needs the authoritative SHOW CREATE TABLE re-read.
type pendingBinaryDefault struct {
	table string
	col   *ir.Column
}

// binaryLiteralDefaultNeedsRecovery reports whether a column's default requires
// the SHOW CREATE TABLE re-read: a BINARY/VARBINARY column with a NON-NULL,
// NON-expression (not DEFAULT_GENERATED) default that information_schema
// rendered as a hex literal (`0x…`, including the truncated bare `0x`).
//
// A VARBINARY empty-string default reports the empty string, not a `0x…`
// literal, so it is correctly excluded — an empty default carries no NUL and is
// never truncated; translateDefault's DefaultLiteral path handles it faithfully.
func binaryLiteralDefaultNeedsRecovery(typ ir.Type, extra string, def sql.NullString) bool {
	if !def.Valid || !isBinaryFamilyType(typ) {
		return false
	}
	if strings.Contains(strings.ToUpper(extra), "DEFAULT_GENERATED") {
		return false
	}
	return hasHexLiteralPrefix(def.String)
}

// hasHexLiteralPrefix reports whether s begins with the `0x`/`0X` prefix MySQL
// uses for a binary column's literal default in information_schema.
func hasHexLiteralPrefix(s string) bool {
	s = strings.TrimSpace(s)
	return len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')
}

// recoverBinaryLiteralDefaults re-reads each pending column's true default
// bytes from SHOW CREATE TABLE (once per table, cached) and overwrites the
// column's provisional Default with a byte-exact hexLiteralDialect
// DefaultExpression. A column whose default cannot be recovered or parsed is
// dropped with a loud WARN (loud-failure tenet — a wrong default is silent
// corruption; a missing default is not).
func (r *SchemaReader) recoverBinaryLiteralDefaults(ctx context.Context, pending []pendingBinaryDefault) error {
	if len(pending) == 0 {
		return nil
	}
	ddlByTable := map[string]string{}
	failedTable := map[string]bool{}

	for _, p := range pending {
		ddl, have := ddlByTable[p.table]
		if !have && !failedTable[p.table] {
			var err error
			ddl, err = r.showCreateTable(ctx, p.table)
			if err != nil {
				failedTable[p.table] = true
				slog.WarnContext(
					ctx,
					"mysql: could not re-read SHOW CREATE TABLE to recover binary column defaults; "+
						"dropping their DEFAULT (target column becomes DEFAULT-less) rather than carrying a "+
						"NUL-truncated wrong value from information_schema",
					slog.String("table", r.qualifiedName(p.table)),
					slog.Any("error", err),
				)
			} else {
				ddlByTable[p.table] = ddl
			}
		}
		if failedTable[p.table] {
			p.col.Default = ir.DefaultNone{}
			continue
		}

		raw, ok := parseShowCreateColumnDefault(ddl, p.col.Name)
		if !ok || len(raw) == 0 {
			slog.WarnContext(
				ctx,
				"mysql: could not parse the binary column's DEFAULT clause from SHOW CREATE TABLE; "+
					"dropping the DEFAULT (target column becomes DEFAULT-less) rather than carrying a "+
					"NUL-truncated wrong value from information_schema",
				slog.String("column", r.qualifiedName(p.table)+"."+p.col.Name),
			)
			p.col.Default = ir.DefaultNone{}
			continue
		}
		p.col.Default = ir.DefaultExpression{Expr: bytesToHexLiteral(raw), Dialect: hexLiteralDialect}
	}
	return nil
}

// qualifiedName renders schema.table for log messages, using the reader's bound
// database name.
func (r *SchemaReader) qualifiedName(table string) string {
	if r.schema == "" {
		return table
	}
	return r.schema + "." + table
}

// showCreateTable returns the CREATE TABLE statement MySQL emits for the named
// table in the reader's bound database. Identifiers are backtick-quoted with
// embedded backticks doubled.
func (r *SchemaReader) showCreateTable(ctx context.Context, table string) (string, error) {
	q := "SHOW CREATE TABLE `" + mysqlQuoteIdent(r.schema) + "`.`" + mysqlQuoteIdent(table) + "`"
	var name, createStmt string
	if err := r.db.QueryRowContext(ctx, q).Scan(&name, &createStmt); err != nil {
		return "", err
	}
	return createStmt, nil
}

// mysqlQuoteIdent escapes an identifier for use inside a backtick-quoted MySQL
// identifier by doubling any embedded backtick.
func mysqlQuoteIdent(ident string) string {
	if !strings.ContainsRune(ident, '`') {
		return ident
	}
	return strings.ReplaceAll(ident, "`", "``")
}

// parseShowCreateColumnDefault extracts colName's literal DEFAULT value from a
// SHOW CREATE TABLE statement and decodes it to raw bytes. Returns ok=false when
// the column line isn't found or its DEFAULT clause is neither a `0x…` hex
// literal nor a single-quoted string.
func parseShowCreateColumnDefault(createStmt, colName string) (raw []byte, ok bool) {
	// Each column definition is on its own line, indented, beginning with the
	// backtick-quoted column name. Match the exact quoted name (backticks
	// doubled, as MySQL emits) so a name that is a prefix of another
	// (`log` vs `log_ts`) can't collide.
	prefix := "`" + mysqlQuoteIdent(colName) + "` "
	for _, line := range strings.Split(createStmt, "\n") {
		trimmed := strings.TrimLeft(line, " \t")
		if !strings.HasPrefix(trimmed, prefix) {
			continue
		}
		// The type of a binary column carries no `DEFAULT` token and any
		// COMMENT clause follows the DEFAULT, so the first ` DEFAULT ` is the
		// column's real default clause.
		idx := strings.Index(trimmed, " DEFAULT ")
		if idx < 0 {
			return nil, false
		}
		rest := strings.TrimLeft(trimmed[idx+len(" DEFAULT "):], " ")
		return parseMySQLDefaultLiteralBytes(rest)
	}
	return nil, false
}

// parseMySQLDefaultLiteralBytes decodes the value token at the start of s (the
// text immediately after a `DEFAULT ` keyword in SHOW CREATE output) into raw
// bytes. It handles the two forms MySQL uses for a binary column's literal
// default: a `0x<even-hex>` hex literal and a single-quoted escaped string.
// Trailing text (a `,`, further column attributes, end-of-line) is ignored.
func parseMySQLDefaultLiteralBytes(s string) (raw []byte, ok bool) {
	if s == "" {
		return nil, false
	}
	if s[0] == '\'' {
		return decodeMySQLQuotedString(s)
	}
	if len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		return decodeMySQLHexToken(s)
	}
	return nil, false
}

// decodeMySQLHexToken decodes a leading `0x<hex>` token (the hex form SHOW
// CREATE uses for binary defaults containing a byte ≥ 0x80). Hex digits are
// read until the first non-hex byte; the payload must be non-empty and
// even-length (byte-aligned), as MySQL always emits.
func decodeMySQLHexToken(s string) (raw []byte, ok bool) {
	digits := s[2:]
	end := 0
	for end < len(digits) && isHexDigit(digits[end]) {
		end++
	}
	digits = digits[:end]
	if digits == "" || len(digits)%2 != 0 {
		return nil, false
	}
	b, err := hex.DecodeString(digits)
	if err != nil {
		return nil, false
	}
	return b, true
}

// decodeMySQLQuotedString decodes a MySQL single-quoted string literal (starting
// at the opening `'`) into raw bytes, stopping at the closing delimiter. It
// recognises the backslash escapes SHOW CREATE emits (`\0 \b \t \n \r \\`) plus
// the other documented MySQL string escapes (`\Z \' \"`), the SQL-standard
// doubled single-quote (which decodes to one `'`), and passes any other raw
// byte through. An
// unknown `\x` escape decodes to the literal `x`, matching MySQL's own
// string-literal parsing. Returns ok=false for a dangling backslash or a string
// with no closing quote.
func decodeMySQLQuotedString(s string) (raw []byte, ok bool) {
	if s == "" || s[0] != '\'' {
		return nil, false
	}
	out := make([]byte, 0, len(s))
	for i := 1; i < len(s); {
		c := s[i]
		switch c {
		case '\\':
			if i+1 >= len(s) {
				return nil, false // dangling backslash
			}
			out = append(out, decodeMySQLStringEscape(s[i+1]))
			i += 2
		case '\'':
			if i+1 < len(s) && s[i+1] == '\'' {
				out = append(out, '\'') // doubled '' → literal quote
				i += 2
				continue
			}
			return out, true // closing delimiter
		default:
			out = append(out, c)
			i++
		}
	}
	return nil, false // no closing quote
}

// decodeMySQLStringEscape maps the byte following a backslash to the byte MySQL
// means by it. Unknown escapes yield the character itself (backslash dropped),
// matching MySQL's string-literal rule.
func decodeMySQLStringEscape(c byte) byte {
	switch c {
	case '0':
		return 0x00
	case 'b':
		return 0x08
	case 't':
		return 0x09
	case 'n':
		return 0x0A
	case 'r':
		return 0x0D
	case 'Z':
		return 0x1A
	default:
		// `\\`→\, `\'`→', `\"`→", and any other `\x`→x.
		return c
	}
}

// isHexDigit reports whether c is an ASCII hex digit.
func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// bytesToHexLiteral renders raw bytes as the `0x<HEX>` literal the
// hexLiteralDialect emitters consume (uppercase, matching MySQL's own
// information_schema spelling).
func bytesToHexLiteral(raw []byte) string {
	return "0x" + strings.ToUpper(hex.EncodeToString(raw))
}
