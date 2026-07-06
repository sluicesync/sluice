// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// This file is the WRITE-side counterpart to types.go: it renders an IR
// [ir.Type] into the SQLite DECLARED type the reader's resolveColumnType
// reads BACK to the same IR type, and an [ir.Table] into a full inline
// CREATE TABLE (ADR-0134). It is the faithful inverse of the reader's
// affinity + ADR-0129 declared-temporal/bool mapping:
//
//	Boolean  → BOOLEAN     (declared-bool match; value 0/1)
//	Integer  → INTEGER     (INTEGER affinity; width/sign not preserved)
//	Float    → REAL        (REAL affinity)
//	Decimal  → TEXT        (TEXT affinity; Bug 162 — preserves the exact decimal)
//	Char/Varchar/Text → TEXT (TEXT affinity; length not enforced)
//	Blob/Binary/Varbinary → BLOB (BLOB affinity)
//	Date     → DATE        Time → TIME    DateTime/Timestamp → DATETIME
//	JSON/UUID/Enum/Set → TEXT
//
// Anything SQLite cannot faithfully hold (geometry, inet/cidr/macaddr,
// bit, interval, array, domain, verbatim/unknown extension types) is
// REFUSED LOUDLY at emit time naming the IR type — never coerced to a
// silently-wrong text column (the loud-failure tenet, mirroring the
// reader's per-row refusals).

// emitColumnType maps an IR type to its SQLite declared type, or returns
// a loud refusal for a type SQLite has no faithful storage for.
func emitColumnType(t ir.Type) (string, error) {
	switch t.(type) {
	case ir.Boolean:
		// "BOOLEAN" is read back as ir.Boolean (ADR-0129 declared-bool
		// match); values store as 0/1 INTEGER.
		return "BOOLEAN", nil
	case ir.Integer:
		// SQLite integers are 64-bit signed. Width/unsigned are not
		// representable (and not preserved on read-back). uint64 values
		// beyond int64 are refused at value-encode time, not here.
		return "INTEGER", nil
	case ir.Float:
		return "REAL", nil
	case ir.Decimal:
		// TEXT affinity — NOT NUMERIC/DECIMAL (Bug 162). SQLite's NUMERIC
		// affinity coerces a bound decimal to REAL when the text→REAL→text
		// round-trip is "reversible" at SQLite's 15-digit text precision, so
		// an ordinary money value like `19.99` is silently stored as the
		// binary float 19.989999999999998 — and sluice's reader, which
		// formats a REAL with the shortest-exact FormatFloat(-1), reads it
		// back as `19.989999999999998`, not `19.99`. That is a SILENT value
		// corruption, and the `.db` is the deliverable (X→SQLite→D1). The
		// only way SQLite preserves the exact decimal text is TEXT affinity
		// (it stores text verbatim, no coercion). The cost is a documented
		// type downgrade: the column reads back as ir.Text rather than
		// ir.Decimal — the same value-faithful trade as JSON/UUID→TEXT, and
		// the right one, since silent value loss is never acceptable. The
		// decimal value is bound as its exact string by encodeDecimal. See
		// ADR-0134 §2.
		return "TEXT", nil
	case ir.Char, ir.Varchar, ir.Text:
		// TEXT affinity. SQLite does not enforce a declared length, so
		// Char/Varchar widen to ir.Text on a SQLite round-trip — values
		// are preserved.
		return "TEXT", nil
	case ir.Binary, ir.Varbinary, ir.Blob:
		return "BLOB", nil
	case ir.Date:
		return "DATE", nil
	case ir.Time:
		// SQLite is tz-naive. A tz-aware timetz value carries its text
		// verbatim (value_encode.go); the declared TIME reads back as
		// ir.Time.
		return "TIME", nil
	case ir.DateTime, ir.Timestamp:
		// DATETIME reads back as ir.Timestamp (no tz). A tz-aware source
		// timestamp is stored as its UTC ISO instant (instant-faithful;
		// the display zone is dropped — SQLite has no tz type, ADR-0134).
		return "DATETIME", nil
	case ir.JSON:
		// SQLite has no native JSON type (JSONSupport=None). Emitting a
		// "JSON"-spelled type would resolve to NUMERIC affinity on
		// read-back (the reader has no JSON resolution) and then refuse
		// the JSON-object text — so emit TEXT, which preserves the raw
		// JSON value exactly and reads back as ir.Text (ADR-0134).
		return "TEXT", nil
	case ir.UUID:
		return "TEXT", nil
	case ir.Enum, ir.Set:
		// Enum value (string) / Set members (comma-joined) carry as TEXT.
		return "TEXT", nil
	default:
		return "", fmt.Errorf(
			"sqlite: no faithful SQLite target type for IR %s; refusing to coerce it to a "+
				"silently-wrong column (use --type-override to carry it as text/blob if a lossy "+
				"carry is acceptable)",
			t.String(),
		)
	}
}

// emitColumnDef renders one column's inline CREATE TABLE fragment:
//
//	"name" TYPE [PRIMARY KEY] [GENERATED] [NOT NULL] [DEFAULT ...]
//
// inlinePK is true for the single-column INTEGER primary key, which MUST
// be declared inline as `INTEGER PRIMARY KEY` to become SQLite's rowid
// alias (the auto-continuing identity the reader reports as
// Integer.AutoIncrement). For a rowid alias NOT NULL is deliberately
// omitted so a future NULL insert auto-assigns (the verified
// auto-increment behaviour, ADR-0134 §4); explicit-id bulk-copy rows are
// unaffected.
func emitColumnDef(c *ir.Column, inlinePK bool) (string, error) {
	if c == nil {
		return "", errors.New("sqlite: emitColumnDef: column is nil")
	}
	typeStr, err := emitColumnType(c.Type)
	if err != nil {
		return "", fmt.Errorf("sqlite: column %q: %w", c.Name, err)
	}

	var sb strings.Builder
	sb.WriteString(quoteIdent(c.Name))
	sb.WriteByte(' ')
	sb.WriteString(typeStr)

	if inlinePK {
		sb.WriteString(" PRIMARY KEY")
	}

	if c.IsGenerated() {
		// SQLite generated columns: `... AS (expr) STORED|VIRTUAL`. The
		// expression emits VERBATIM in its source dialect — SQLite is the
		// target and sluice has no SQLite expression translator, so a
		// non-portable body fails LOUDLY at CREATE TABLE rather than being
		// guessed at (the verbatim/loud-failure policy, ADR-0133 §2).
		sb.WriteString(" AS (")
		sb.WriteString(c.GeneratedExpr)
		sb.WriteString(") ")
		if c.GeneratedStored {
			sb.WriteString("STORED")
		} else {
			sb.WriteString("VIRTUAL")
		}
		// Generated columns carry NOT NULL (if any) but never a DEFAULT
		// (SQLite rejects DEFAULT on a generated column; the reader emits
		// DefaultNone for them).
		if !c.Nullable {
			sb.WriteString(" NOT NULL")
		}
		return sb.String(), nil
	}

	// NOT NULL — but never on the rowid-alias PK (see inlinePK above).
	if !c.Nullable && !inlinePK {
		sb.WriteString(" NOT NULL")
	}
	if dflt, ok := emitDefault(c.Default, c.Type); ok {
		sb.WriteString(" DEFAULT ")
		sb.WriteString(dflt)
	}
	return sb.String(), nil
}

// emitDefault renders a column DEFAULT clause. A literal is quoted as a
// SQL string (SQLite applies column affinity to a quoted numeric default,
// so '5' on an INTEGER column stores 5 — and a re-read recovers the
// literal); an expression emits verbatim in its source dialect (a
// non-portable function fails loudly at CREATE TABLE). Defaults affect
// only post-migration inserts, never the explicit-value migrated rows.
func emitDefault(d ir.DefaultValue, colType ir.Type) (string, bool) {
	switch v := d.(type) {
	case nil, ir.DefaultNone:
		return "", false
	case ir.DefaultLiteral:
		return quoteSQLString(v.Value), true
	case ir.DefaultExpression:
		if v.Expr == "" {
			return "", false
		}
		if v.Dialect == hexLiteralDialect {
			// Hex-literal default on a BINARY/VARBINARY column (BINARY(N)
			// DEFAULT round-trip, SQLite side). The MySQL reader tags the
			// stored `0x<hex>` form (the raw source bytes); the SQLite target
			// type is BLOB (BINARY/VARBINARY → BLOB, emitColumnType), whose
			// literal is `X'<hex>'`. Value is identical — only the surface
			// syntax differs. It is critical NOT to take the generic
			// wrapSQLiteExpressionDefault path here: that emits `(0x<hex>)`,
			// which SQLite parses as an INTEGER (overflowing for any
			// multi-byte value), silently landing a wrong default.
			//
			// FIXED-width BINARY(N) is NUL-padded to N bytes: MySQL reports
			// the UNpadded literal but STORES it right-padded (a same-engine
			// MySQL target re-pads on INSERT); a width-agnostic SQLite BLOB
			// cannot re-pad, so the zero-fill is baked into the literal here.
			// VARBINARY (padBytes 0) is stored as-written, never padded.
			//
			// Anything not in the expected `0x…` shape falls through to the
			// wrap path (loud failure on target beats a silent guess) — the
			// reader's hexLiteralDefault already validated the digits.
			if lit, ok := sqliteBlobHexLiteral(v.Expr, binaryPadBytes(colType)); ok {
				return lit, true
			}
		}
		return wrapSQLiteExpressionDefault(v.Expr), true
	}
	return "", false
}

// hexLiteralDialect mirrors the MySQL reader's hex-literal dialect tag
// (mysql.hexLiteralDialect). Package-local copy: engine packages are peers,
// wired only through the IR + registry, so the SQLite writer can't import the
// mysql package; the IR's DefaultExpression dialect tag is the cross-package
// contract and this constant names the value the SQLite writer recognises on
// it. Tags a MySQL BINARY/VARBINARY hex-literal DEFAULT (`0x<hex>`) so the
// DEFAULT path renders it as a SQLite BLOB literal `X'<hex>'` rather than the
// generic `(0x…)` wrap, which SQLite would parse as an overflowing integer.
const hexLiteralDialect = "hexbytes"

// sqliteBlobHexLiteral converts a MySQL hex-literal default `0x<hex>` (as the
// reader tags it) into the SQLite BLOB literal `X'<hex>'`, right-padding the
// payload with NUL bytes to padBytes when the column is a FIXED-width BINARY
// (padBytes>0; 0 = VARBINARY / no padding). The `0x`/`0X` prefix is stripped
// and the remaining digits — already validated even-length hex by the
// reader's hexLiteralDefault — are wrapped in `X'…'`. Returns ok=false for a
// payload lacking the `0x` prefix (the caller then falls to the generic wrap
// path, failing loudly rather than guessing). A blob literal is a bare SQLite
// DEFAULT literal (no surrounding parens needed).
func sqliteBlobHexLiteral(hexExpr string, padBytes int) (string, bool) {
	s := strings.TrimSpace(hexExpr)
	if len(s) < 2 || s[0] != '0' || (s[1] != 'x' && s[1] != 'X') {
		return "", false
	}
	return "X'" + padBinaryHex(s[2:], padBytes) + "'", true
}

// binaryPadBytes returns the fixed-width byte count a hex-literal default must
// be NUL-padded to for column type t: the declared width of a fixed
// ir.Binary(N), or 0 for ir.Varbinary (and anything else) — VARBINARY is
// stored as-written, never zero-filled. See sqliteBlobHexLiteral for why the
// zero-fill has to be baked into the cross-engine literal.
func binaryPadBytes(t ir.Type) int {
	if b, ok := t.(ir.Binary); ok {
		return b.Length
	}
	return 0
}

// padBinaryHex right-pads a hex-digit payload with trailing "00" byte-pairs to
// padBytes bytes (2*padBytes hex digits), reproducing MySQL's BINARY(N)
// zero-fill on a width-agnostic target. A payload already at or beyond the
// target width is returned unchanged (defensive — MySQL never reports a
// default wider than the column). padBytes<=0 (VARBINARY) is a no-op.
func padBinaryHex(digits string, padBytes int) string {
	want := padBytes * 2
	if want <= len(digits) {
		return digits
	}
	return digits + strings.Repeat("0", want-len(digits))
}

// wrapSQLiteExpressionDefault re-parenthesises an expression DEFAULT body.
// SQLite's DEFAULT grammar accepts only a literal value, a signed number,
// or a PARENTHESISED expression — a bare `DEFAULT datetime('now')` or
// `DEFAULT 'a' || 'b'` is a syntax error (probed on modernc). The IR
// carries expression bodies with PRAGMA's outer parens already stripped,
// so wrap unless the body already leads with one. The prefix/suffix check
// is naive (same shape as the MySQL writer's wrapMySQLExpressionDefault):
// a pathological `(a)||(b)` body emits unwrapped and fails LOUDLY at
// CREATE TABLE rather than being guessed at; wrapping is uniformly valid
// for every bare shape the reader produces (keywords, blobs, functions —
// all probed), so no bare-keyword carve-out is needed.
func wrapSQLiteExpressionDefault(expr string) string {
	trimmed := strings.TrimSpace(expr)
	if strings.HasPrefix(trimmed, "(") && strings.HasSuffix(trimmed, ")") {
		return expr
	}
	return "(" + trimmed + ")"
}

// emitCheckConstraint renders an inline CHECK clause for the CREATE TABLE
// body. The expression emits VERBATIM (source dialect); a non-portable
// predicate fails loudly on SQLite's parser at CREATE TABLE rather than
// being silently dropped or mistranslated (ADR-0134 §3 / ADR-0133 §2).
func emitCheckConstraint(c *ir.CheckConstraint) string {
	var sb strings.Builder
	if c.Name != "" {
		sb.WriteString("CONSTRAINT ")
		sb.WriteString(quoteIdent(c.Name))
		sb.WriteByte(' ')
	}
	sb.WriteString("CHECK (")
	sb.WriteString(c.Expr)
	sb.WriteByte(')')
	return sb.String()
}

// emitForeignKey renders an inline FOREIGN KEY clause for the CREATE TABLE
// body. SQLite cannot ADD a FK after creation, so every FK is emitted
// inline here (ADR-0134 §3). ON DELETE/UPDATE NO ACTION is SQLite's
// default and omitted to keep the DDL minimal.
func emitForeignKey(fk *ir.ForeignKey) (string, error) {
	if fk == nil {
		return "", errors.New("sqlite: emitForeignKey: fk is nil")
	}
	if len(fk.Columns) == 0 || len(fk.ReferencedColumns) == 0 {
		return "", fmt.Errorf("sqlite: emitForeignKey: fk %q has no columns", fk.Name)
	}
	if len(fk.Columns) != len(fk.ReferencedColumns) {
		return "", fmt.Errorf("sqlite: emitForeignKey: fk %q column count mismatch (%d vs %d)",
			fk.Name, len(fk.Columns), len(fk.ReferencedColumns))
	}

	var sb strings.Builder
	if fk.Name != "" {
		sb.WriteString("CONSTRAINT ")
		sb.WriteString(quoteIdent(fk.Name))
		sb.WriteByte(' ')
	}
	sb.WriteString("FOREIGN KEY ")
	sb.WriteString(quoteColumnList(fk.Columns))
	sb.WriteString(" REFERENCES ")
	sb.WriteString(quoteIdent(fk.ReferencedTable))
	sb.WriteByte(' ')
	sb.WriteString(quoteColumnList(fk.ReferencedColumns))
	if fk.OnDelete != ir.FKActionNoAction {
		sb.WriteString(" ON DELETE ")
		sb.WriteString(fk.OnDelete.String())
	}
	if fk.OnUpdate != ir.FKActionNoAction {
		sb.WriteString(" ON UPDATE ")
		sb.WriteString(fk.OnUpdate.String())
	}
	return sb.String(), nil
}

// emitTableDef renders the full inline CREATE TABLE for a SQLite target:
// columns, generated columns, NOT NULL, DEFAULT, PRIMARY KEY, UNIQUE,
// CHECK, and FOREIGN KEY — ALL inline, because SQLite cannot ALTER-ADD the
// constraint-y parts later (ADR-0134 §3). IF NOT EXISTS keeps the schema
// phase idempotent across a resume.
func emitTableDef(table *ir.Table) (string, error) {
	if table == nil {
		return "", errors.New("sqlite: emitTableDef: table is nil")
	}
	if len(table.Columns) == 0 {
		return "", fmt.Errorf("sqlite: emitTableDef: table %q has no columns", table.Name)
	}

	// A single-column INTEGER primary key is emitted inline on the column
	// (`INTEGER PRIMARY KEY`) so it becomes SQLite's rowid alias — the
	// auto-continuing identity the reader reports as Integer.AutoIncrement.
	// A composite or non-integer PK uses a table-level PRIMARY KEY clause.
	inlinePKCol := soleIntegerPKColumn(table)

	parts := make([]string, 0, len(table.Columns)+len(table.CheckConstraints)+len(table.ForeignKeys)+2)
	for _, col := range table.Columns {
		def, err := emitColumnDef(col, col.Name == inlinePKCol)
		if err != nil {
			return "", err
		}
		parts = append(parts, def)
	}

	// Cross-engine collation-drop policy (docs/type-mapping.md "Charsets
	// and collations"): SQLite's collation namespace (BINARY / NOCASE /
	// RTRIM) shares no names with MySQL or PG, and the SQLite reader
	// never populates ir Collation — so EVERY carried collation is
	// foreign here. String columns land with SQLite's default BINARY
	// collation; this WARN — one per table — makes the drop visible
	// instead of silent. Passing "sqlite" (a dialect no collation ever
	// carries) classifies all of them as dropped.
	if dropped := translate.DroppedCollationColumns(table, "sqlite"); len(dropped) > 0 {
		slog.Warn(
			"sqlite: dropping cross-engine column collations (no SQLite equivalent; the target columns use SQLite's default BINARY collation, which may change sort/comparison semantics)",
			slog.String("table", table.Name),
			slog.String("columns", strings.Join(dropped, ", ")),
		)
	}

	// Table-level PRIMARY KEY for the composite / non-integer case only.
	if table.PrimaryKey != nil && inlinePKCol == "" {
		parts = append(parts, "PRIMARY KEY "+quoteIndexColumnList(table.PrimaryKey.Columns))
	}

	// User CHECK constraints, in the IR's preserved source order.
	for _, chk := range table.CheckConstraints {
		parts = append(parts, emitCheckConstraint(chk))
	}

	// Foreign keys — inline (SQLite can't ADD them later).
	for _, fk := range table.ForeignKeys {
		clause, err := emitForeignKey(fk)
		if err != nil {
			return "", err
		}
		parts = append(parts, clause)
	}

	var sb strings.Builder
	sb.WriteString("CREATE TABLE IF NOT EXISTS ")
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
	sb.WriteString(")")
	return sb.String(), nil
}

// soleIntegerPKColumn returns the column name of a single-column INTEGER
// primary key (the rowid-alias case), or "" when the table has no PK, a
// composite PK, an expression PK entry, or a non-integer PK column.
func soleIntegerPKColumn(table *ir.Table) string {
	if table.PrimaryKey == nil || len(table.PrimaryKey.Columns) != 1 {
		return ""
	}
	name := table.PrimaryKey.Columns[0].Column
	if name == "" {
		return "" // expression PK entry — not a column reference
	}
	for _, c := range table.Columns {
		if c.Name != name {
			continue
		}
		if _, ok := c.Type.(ir.Integer); ok {
			return name
		}
		return ""
	}
	return ""
}

// emitCreateIndex renders a CREATE INDEX for a non-PK secondary index.
// SQLite supports post-hoc index creation, partial indexes (WHERE
// predicate), and expression index entries — all carried VERBATIM in
// their source dialect (a non-portable expression/predicate fails loudly
// at CREATE INDEX). IF NOT EXISTS keeps the index phase idempotent.
func emitCreateIndex(tableName string, idx *ir.Index) (string, error) {
	if idx == nil {
		return "", errors.New("sqlite: emitCreateIndex: index is nil")
	}
	if idx.Name == "" {
		return "", fmt.Errorf("sqlite: emitCreateIndex: index on %q has no name", tableName)
	}
	if len(idx.Columns) == 0 {
		return "", fmt.Errorf("sqlite: emitCreateIndex: index %q has no columns", idx.Name)
	}

	var sb strings.Builder
	sb.WriteString("CREATE ")
	if idx.Unique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX IF NOT EXISTS ")
	sb.WriteString(quoteIdent(idx.Name))
	sb.WriteString(" ON ")
	sb.WriteString(quoteIdent(tableName))
	sb.WriteByte(' ')
	sb.WriteString(emitIndexColumnList(idx.Columns))
	if idx.Predicate != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(idx.Predicate)
	}
	return sb.String(), nil
}

// emitCreateView renders CREATE VIEW IF NOT EXISTS for a regular view.
// The body emits VERBATIM (a non-portable cross-dialect body fails loudly
// at CREATE VIEW). Materialized views are rejected upstream — SQLite has
// none (ADR-0134 §5).
func emitCreateView(v *ir.View) string {
	body := strings.TrimRight(strings.TrimSpace(v.Definition), ";")
	return "CREATE VIEW IF NOT EXISTS " + quoteIdent(v.Name) + " AS " + body
}

// emitIndexColumnList renders an index/PK column list, honouring DESC and
// carrying an expression entry verbatim. Per-column collation / NULLS
// ordering / operator class are PG-isms SQLite doesn't take here; a plain
// column or DESC column covers the round-trip cases.
func emitIndexColumnList(cols []ir.IndexColumn) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		var seg string
		if c.Expression != "" {
			seg = "(" + c.Expression + ")"
		} else {
			seg = quoteIdent(c.Column)
		}
		if c.Desc {
			seg += " DESC"
		}
		parts[i] = seg
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

// quoteIndexColumnList is the plain-column form used for the table-level
// PRIMARY KEY clause (PK columns are always real columns, never
// expressions, in the IR).
func quoteIndexColumnList(cols []ir.IndexColumn) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = quoteIdent(c.Column)
	}
	return "(" + strings.Join(names, ", ") + ")"
}

// quoteColumnList renders a parenthesised, comma-separated list of quoted
// column names (foreign-key column / referenced-column lists).
func quoteColumnList(names []string) string {
	quoted := make([]string, len(names))
	for i, n := range names {
		quoted[i] = quoteIdent(n)
	}
	return "(" + strings.Join(quoted, ", ") + ")"
}

// quoteSQLString single-quotes a SQL string literal, doubling any embedded
// single quote. (quoteIdent is shared with the reader, in row_reader.go.)
func quoteSQLString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
