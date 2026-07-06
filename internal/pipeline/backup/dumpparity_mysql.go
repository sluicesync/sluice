// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Restore-parity oracle comparator, MySQL dialect (roadmap item 51,
// Phase 2 — the MySQL→MySQL flavour of the shipped PG oracle).
//
// The oracle migrates the same MySQL source twice — once through
// sluice's Migrator, once through `mysqldump | mysql` (the reference
// implementation of "everything a MySQL schema can contain") — then
// compares `mysqldump --no-data` of the two targets. As with the PG
// side, raw text-diff of dump output is ordering- and version-fragile,
// so the comparison works on statement *sets* keyed by object identity;
// every divergence is either a documented degradation (matched by
// DumpParityAllowlistMySQL, each entry cited) or a latent bug — there is
// no third category.
//
// Why this can't reuse ParseSchemaDump verbatim (the "MySQL grammar
// differs" part of the spec): pg_dump splits a table's constraints and
// secondary indexes into their OWN top-level statements (ALTER TABLE …
// ADD CONSTRAINT, CREATE INDEX), so the PG comparator gets fine-grained,
// order-insensitive, identity-keyed diffing for free. mysqldump does the
// opposite — every column, index, and constraint is INLINE inside the
// single `CREATE TABLE` statement, and the dump is peppered with
// `/*!NNNNN … */` version-guard executable comments (a CHECK constraint
// on MySQL 8.0.16+ lives inside one). A whole-CREATE-TABLE body diff
// would be far too coarse: one AUTO_INCREMENT counter or one
// re-rendered default would flag the entire table and allowlisting it
// would mask every real column-level gap in that table. So this parser
// DECOMPOSES each CREATE TABLE into its component elements (one per
// column / index / constraint, plus the trailing table-options blob) and
// keys each by identity — recovering the same granularity the PG side
// gets structurally.
//
// The diff / allowlist / result-type machinery is engine-neutral and
// lives in dumpparity.go; this file only adds the MySQL text→statement
// parser (splitter + conditional-comment unwrap + CREATE TABLE
// decomposition + keyer). Everything here is a pure function over dump
// text so it gets unit pins without Docker; the harness that produces
// the dumps lives in migrate_dump_parity_mysql_integration_test.go.
//
// Both dumps compared are mysqldump output of the SAME mysqldump binary
// against the SAME server, so the oracle compares CATALOG STATE (as
// re-rendered by mysqldump), not sluice's emitted DDL vs mysqldump's:
// any cosmetic rendering difference in what sluice CREATE-issued washes
// out because MySQL normalized it into the catalog and mysqldump
// re-emits both targets identically. A surviving divergence therefore
// means the STORED schema genuinely differs.
//
// v1 limitations (deliberate, documented):
//   - The seed avoids stored routines / triggers / views, so the
//     splitter never has to reason about a `BEGIN … END` body or a
//     DELIMITER change; a splitter bug there can't silently eat a
//     statement the floor guard wouldn't catch.
//   - Element granularity is the object KEY (column / index / constraint
//     / the whole options blob). Allowlisting a body mismatch on
//     `TABLE orders OPTIONS` masks every option divergence on that
//     table, not just the cited one — the harness logs both bodies for
//     every allowlisted mismatch so the full delta stays visible in CI.

package backup

import "strings"

// ParseMySQLSchemaDump splits a `mysqldump --no-data` text dump into
// identity-keyed statements. CREATE TABLE statements are decomposed into
// their inline elements (see decomposeMySQLCreateTable); every other
// top-level statement that survives the preamble filter is kept whole
// and keyed by its leading tokens.
func ParseMySQLSchemaDump(dump string) []dumpStatement {
	clean := stripMySQLComments(dump)
	var out []dumpStatement
	for _, raw := range splitMySQLStatements(clean) {
		body := collapseWS(raw)
		if body == "" || isMySQLPreamble(body) {
			continue
		}
		if table, ok := mysqlCreateTableName(body); ok {
			out = append(out, decomposeMySQLCreateTable(table, body)...)
			continue
		}
		out = append(out, dumpStatement{Key: mysqlStatementKey(body), Body: body})
	}
	return out
}

// CountMySQLColumns returns how many parsed elements are column
// definitions. The vacuous-pass guard's counting primitive for the
// MySQL side (mirroring CountCreateStatements on the PG side): the seed
// declares how many columns each target must yield at minimum, and a
// parser/normalizer bug that eats CREATE TABLE elements trips the floor
// instead of reading as parity. Columns are the dominant, eat-prone
// class — a splitter that mis-handles the parenthesized element list
// loses them first.
func CountMySQLColumns(stmts []dumpStatement) int {
	n := 0
	for _, s := range stmts {
		if strings.Contains(s.Key, " COLUMN ") {
			n++
		}
	}
	return n
}

// CountMySQLTables returns how many parsed tables are present. Every
// mysqldump CREATE TABLE emits a trailing `ENGINE=…` options blob, so
// each table yields exactly one OPTIONS element — counting them counts
// tables. Complements the column floor so an entire dropped table trips
// the guard even if its columns landed elsewhere.
func CountMySQLTables(stmts []dumpStatement) int {
	n := 0
	for _, s := range stmts {
		if strings.HasSuffix(s.Key, " OPTIONS") {
			n++
		}
	}
	return n
}

// stripMySQLComments removes MySQL comment syntax from dump text while
// respecting string and identifier quoting so a `*/` or `--` inside a
// literal is left intact:
//
//   - line comments: `-- ` to end-of-line, and `#` to end-of-line;
//   - plain block comments `/* … */` (NOT `/*!`) are removed entirely
//     (MySQL block comments do not nest);
//   - executable conditional comments `/*!NNNNN … */` are UNWRAPPED —
//     the `/*!`, the optional 5-digit version, and the closing `*/` are
//     dropped but the inner SQL is KEPT, because MySQL executes it. This
//     is load-bearing: mysqldump wraps a MySQL-8.0.16+ CHECK constraint
//     and the header FOREIGN_KEY_CHECKS/charset SETs in these guards, so
//     eliding them would silently eat a CHECK (a vacuous-loss the floor
//     guard exists to catch) — instead we unwrap and let the CHECK land
//     as an inline element and the SETs fall out as preamble.
//
// A backslash inside a single- or double-quoted string escapes the next
// character (MySQL's default lexer), so an embedded quote doesn't end
// the literal early. Backtick identifiers use doubled-backtick escaping,
// not backslashes.
func stripMySQLComments(text string) string {
	var out strings.Builder
	i, n := 0, len(text)
	for i < n {
		c := text[i]
		switch {
		case c == '\'' || c == '"':
			// String literal with backslash escapes.
			out.WriteByte(c)
			i++
			for i < n {
				if text[i] == '\\' && i+1 < n {
					out.WriteByte(text[i])
					out.WriteByte(text[i+1])
					i += 2
					continue
				}
				out.WriteByte(text[i])
				if text[i] == c {
					i++
					break
				}
				i++
			}
		case c == '`':
			// Backtick identifier; doubled backtick is an escaped
			// backtick and re-enters the quoted state on the next pass.
			out.WriteByte(c)
			i++
			for i < n {
				out.WriteByte(text[i])
				if text[i] == '`' {
					i++
					break
				}
				i++
			}
		case c == '-' && i+1 < n && text[i+1] == '-':
			for i < n && text[i] != '\n' {
				i++
			}
		case c == '#':
			for i < n && text[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && text[i+1] == '*':
			if i+2 < n && text[i+2] == '!' {
				// Conditional/executable comment: drop `/*!` + optional
				// version digits, keep inner, drop the closing `*/`.
				i += 3
				for i < n && text[i] >= '0' && text[i] <= '9' {
					i++
				}
				out.WriteByte(' ')
				// Copy inner content up to the matching `*/`, respecting
				// nested string/identifier quoting so a `*/` inside a
				// literal doesn't close the guard early. Re-run the strip
				// over the inner so a nested plain comment is handled.
				out.WriteString(stripMySQLComments(mysqlConditionalInner(text, &i)))
				out.WriteByte(' ')
			} else {
				// Plain comment: elide to the closing `*/`.
				i += 2
				for i+1 < n && (text[i] != '*' || text[i+1] != '/') {
					i++
				}
				i += 2
				out.WriteByte(' ')
			}
		default:
			out.WriteByte(c)
			i++
		}
	}
	return out.String()
}

// mysqlConditionalInner returns the raw inner text of a `/*! … */`
// conditional comment starting at *i (positioned just past the version
// digits) and advances *i past the closing `*/`. Quoting is respected so
// a `*/` inside a string/identifier literal doesn't terminate early.
func mysqlConditionalInner(text string, i *int) string {
	start := *i
	n := len(text)
	for *i < n {
		c := text[*i]
		switch {
		case c == '\'' || c == '"':
			*i++
			for *i < n {
				if text[*i] == '\\' && *i+1 < n {
					*i += 2
					continue
				}
				q := text[*i]
				*i++
				if q == c {
					break
				}
			}
		case c == '`':
			*i++
			for *i < n {
				q := text[*i]
				*i++
				if q == '`' {
					break
				}
			}
		case c == '*' && *i+1 < n && text[*i+1] == '/':
			inner := text[start:*i]
			*i += 2
			return inner
		default:
			*i++
		}
	}
	return text[start:*i]
}

// splitMySQLStatements splits comment-stripped SQL into top-level
// statements on semicolons, respecting single-/double-quoted string
// literals (with backslash escapes) and backtick identifiers. Comments
// are already gone by the time this runs.
func splitMySQLStatements(text string) []string {
	var out []string
	var cur strings.Builder
	flush := func() {
		if s := strings.TrimSpace(cur.String()); s != "" {
			out = append(out, s)
		}
		cur.Reset()
	}
	i, n := 0, len(text)
	for i < n {
		c := text[i]
		switch c {
		case '\'', '"':
			quote := c
			cur.WriteByte(c)
			i++
			for i < n {
				if text[i] == '\\' && i+1 < n {
					cur.WriteByte(text[i])
					cur.WriteByte(text[i+1])
					i += 2
					continue
				}
				cur.WriteByte(text[i])
				if text[i] == quote {
					i++
					break
				}
				i++
			}
		case '`':
			cur.WriteByte(c)
			i++
			for i < n {
				cur.WriteByte(text[i])
				if text[i] == '`' {
					i++
					break
				}
				i++
			}
		case ';':
			flush()
			i++
		default:
			cur.WriteByte(c)
			i++
		}
	}
	flush()
	return out
}

// isMySQLPreamble reports whether a normalized statement is dump session
// scaffolding rather than a schema object: the SET guards (unwrapped
// from `/*!40101 … */`), the DROP TABLE / LOCK / UNLOCK / USE noise
// mysqldump interleaves, and any stray unwrapped-guard leftover. These
// appear identically on both targets and carry no fidelity signal.
func isMySQLPreamble(body string) bool {
	upper := strings.ToUpper(body)
	for _, p := range []string{
		"SET ", "DROP TABLE", "LOCK TABLES", "UNLOCK TABLES", "USE ",
		"DROP DATABASE", "CREATE DATABASE", "/*", "DELIMITER",
	} {
		if strings.HasPrefix(upper, p) {
			return true
		}
	}
	return false
}

// mysqlCreateTableName returns the table name of a `CREATE TABLE`
// statement (backticks stripped) and true, or ("", false) for any other
// statement. `IF NOT EXISTS` and a `db`.`tbl` qualifier are tolerated;
// only the bare table name is keyed on (the harness dumps one database
// per target, so a schema qualifier carries no identity signal).
func mysqlCreateTableName(body string) (string, bool) {
	toks := tokenizeMySQL(body)
	if len(toks) < 3 || !strings.EqualFold(toks[0], "CREATE") || !strings.EqualFold(toks[1], "TABLE") {
		return "", false
	}
	i := 2
	if i+2 < len(toks) && strings.EqualFold(toks[i], "IF") &&
		strings.EqualFold(toks[i+1], "NOT") && strings.EqualFold(toks[i+2], "EXISTS") {
		i += 3
	}
	if i >= len(toks) {
		return "", false
	}
	return unqualifyMySQLName(toks[i]), true
}

// decomposeMySQLCreateTable splits a normalized CREATE TABLE statement
// into per-element dumpStatements: one per column, index, and
// constraint, plus a single trailing OPTIONS element carrying the
// (AUTO_INCREMENT-stripped) table options. This is what gives the MySQL
// oracle the same identity-keyed, order-insensitive granularity the PG
// oracle gets from pg_dump's separate ALTER/CREATE statements.
func decomposeMySQLCreateTable(table, body string) []dumpStatement {
	open := strings.IndexByte(body, '(')
	if open < 0 {
		return []dumpStatement{{Key: "CREATE TABLE " + table, Body: body}}
	}
	closeIdx := matchMySQLParen(body, open)
	if closeIdx < 0 {
		return []dumpStatement{{Key: "CREATE TABLE " + table, Body: body}}
	}
	inner := body[open+1 : closeIdx]
	options := strings.TrimSpace(body[closeIdx+1:])
	defCharset, defCollation := mysqlTableDefaults(options)

	var out []dumpStatement
	for _, el := range splitMySQLElements(inner) {
		el = strings.TrimSpace(el)
		if el == "" {
			continue
		}
		key := mysqlElementKey(el)
		out = append(out, dumpStatement{
			Key:  table + " " + key,
			Body: normalizeMySQLElement(key, el, defCharset, defCollation),
		})
	}
	// The trailing table options (ENGINE / CHARSET / COLLATE / ROW_FORMAT
	// / COMMENT …). AUTO_INCREMENT=<n> is stripped: it is a data-derived
	// counter that legitimately differs (the sluice target's counter
	// advanced past the copied rows; the mysqldump oracle carries the
	// SOURCE's counter) — comparing it would be a guaranteed false diff.
	out = append(out, dumpStatement{
		Key:  table + " OPTIONS",
		Body: stripAutoIncrementOption(options),
	})
	return out
}

// mysqlElementKey classifies one CREATE TABLE element and returns its
// identity suffix (the table name is prepended by the caller): a column
// keys on `COLUMN <name>`, an index on `KEY <name>` (unique/fulltext/
// spatial-ness lives in the body so a demotion surfaces as a body
// mismatch under the same key), a primary key on `PRIMARY KEY`, and a
// named constraint (FK or CHECK) on `CONSTRAINT <name>`.
func mysqlElementKey(el string) string {
	toks := tokenizeMySQL(el)
	if len(toks) == 0 {
		return "COLUMN"
	}
	up := func(i int) string {
		if i >= len(toks) {
			return ""
		}
		return strings.ToUpper(toks[i])
	}
	switch up(0) {
	case "PRIMARY":
		return "PRIMARY KEY"
	case "UNIQUE", "FULLTEXT", "SPATIAL":
		// UNIQUE KEY <name> / FULLTEXT KEY <name> / SPATIAL KEY <name>
		// (or … INDEX <name>). The name is the token after KEY/INDEX.
		if len(toks) >= 3 {
			return "KEY " + unqualifyMySQLName(toks[2])
		}
		return "KEY"
	case "KEY", "INDEX":
		if len(toks) >= 2 {
			return "KEY " + unqualifyMySQLName(toks[1])
		}
		return "KEY"
	case "CONSTRAINT":
		if len(toks) >= 2 {
			return "CONSTRAINT " + unqualifyMySQLName(toks[1])
		}
		return "CONSTRAINT"
	case "FOREIGN":
		return "FOREIGN KEY"
	case "CHECK":
		return "CHECK"
	default:
		// A column definition: the first token is the (backtick-quoted)
		// column name.
		return "COLUMN " + unqualifyMySQLName(toks[0])
	}
}

// mysqlStatementKey keys a non-CREATE-TABLE statement (e.g. a standalone
// CREATE INDEX or CREATE VIEW) by its first three tokens with any
// backtick-quoted name unqualified — precise enough to diff, coarse
// enough to survive an unrecognized shape.
func mysqlStatementKey(body string) string {
	toks := tokenizeMySQL(body)
	parts := make([]string, 0, 3)
	for i := 0; i < 3 && i < len(toks); i++ {
		t := toks[i]
		if strings.HasPrefix(t, "`") {
			t = unqualifyMySQLName(t)
		} else {
			t = strings.ToUpper(t)
		}
		parts = append(parts, t)
	}
	return strings.Join(parts, " ")
}

// splitMySQLElements splits a CREATE TABLE parenthesized element list on
// top-level commas, respecting nested parens (a DECIMAL(10,2) type, an
// index column list, a CHECK body) and string/identifier quoting.
func splitMySQLElements(inner string) []string {
	var out []string
	var cur strings.Builder
	depth := 0
	i, n := 0, len(inner)
	for i < n {
		c := inner[i]
		switch {
		case c == '\'' || c == '"':
			quote := c
			cur.WriteByte(c)
			i++
			for i < n {
				if inner[i] == '\\' && i+1 < n {
					cur.WriteByte(inner[i])
					cur.WriteByte(inner[i+1])
					i += 2
					continue
				}
				cur.WriteByte(inner[i])
				if inner[i] == quote {
					i++
					break
				}
				i++
			}
		case c == '`':
			cur.WriteByte(c)
			i++
			for i < n {
				cur.WriteByte(inner[i])
				if inner[i] == '`' {
					i++
					break
				}
				i++
			}
		case c == '(':
			depth++
			cur.WriteByte(c)
			i++
		case c == ')':
			depth--
			cur.WriteByte(c)
			i++
		case c == ',' && depth == 0:
			out = append(out, cur.String())
			cur.Reset()
			i++
		default:
			cur.WriteByte(c)
			i++
		}
	}
	if s := cur.String(); strings.TrimSpace(s) != "" {
		out = append(out, s)
	}
	return out
}

// matchMySQLParen returns the index of the `)` matching the `(` at open,
// respecting nested parens and quoting, or -1 if unbalanced.
func matchMySQLParen(s string, open int) int {
	depth := 0
	i, n := open, len(s)
	for i < n {
		c := s[i]
		switch c {
		case '\'', '"':
			quote := c
			i++
			for i < n {
				if s[i] == '\\' && i+1 < n {
					i += 2
					continue
				}
				q := s[i]
				i++
				if q == quote {
					break
				}
			}
		case '`':
			i++
			for i < n {
				q := s[i]
				i++
				if q == '`' {
					break
				}
			}
		case '(':
			depth++
			i++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
			i++
		default:
			i++
		}
	}
	return -1
}

// tokenizeMySQL splits s into whitespace-delimited tokens, keeping a
// backtick-quoted identifier (which may contain spaces) as a single
// token. A `(` ends the current token, so `email(16)` and `KEY n(col)`
// yield a bare leading identifier — callers use this only for
// leading-keyword classification and name extraction.
func tokenizeMySQL(s string) []string {
	var out []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			out = append(out, cur.String())
			cur.Reset()
		}
	}
	i, n := 0, len(s)
	for i < n {
		c := s[i]
		switch c {
		case '`':
			cur.WriteByte(c)
			i++
			for i < n {
				cur.WriteByte(s[i])
				if s[i] == '`' {
					i++
					break
				}
				i++
			}
		case ' ', '\t', '\n', '\r':
			flush()
			i++
		case '(':
			flush()
			i++
		default:
			cur.WriteByte(c)
			i++
		}
	}
	flush()
	return out
}

// unqualifyMySQLName strips backtick quoting, a trailing `(`/`,`, and any
// `db`.`tbl` qualifier, returning the bare (last-segment) name.
func unqualifyMySQLName(tok string) string {
	tok = strings.TrimRight(tok, "(,")
	// Split on an unquoted dot to drop a schema/table qualifier.
	if idx := lastUnquotedDot(tok); idx >= 0 {
		tok = tok[idx+1:]
	}
	tok = strings.TrimSpace(tok)
	tok = strings.TrimPrefix(tok, "`")
	tok = strings.TrimSuffix(tok, "`")
	return tok
}

// lastUnquotedDot returns the index of the last `.` that sits outside a
// backtick-quoted segment, or -1.
func lastUnquotedDot(s string) int {
	last := -1
	inBacktick := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '`':
			inBacktick = !inBacktick
		case '.':
			if !inBacktick {
				last = i
			}
		}
	}
	return last
}

// stripAutoIncrementOption removes an `AUTO_INCREMENT=<n>` table option
// (case-insensitive) from an options blob, leaving every other option in
// place and order.
func stripAutoIncrementOption(options string) string {
	fields := strings.Fields(options)
	kept := fields[:0]
	for _, f := range fields {
		if u := strings.ToUpper(f); strings.HasPrefix(u, "AUTO_INCREMENT=") {
			continue
		}
		kept = append(kept, f)
	}
	return strings.Join(kept, " ")
}

// collapseWS collapses every whitespace run to a single space and trims.
func collapseWS(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// mysqlCharsetDefaultCollation maps a MySQL charset to its default
// collation. Used by the element normalizer to recognize a column-level
// COLLATE clause that merely restates the table charset's default (which
// mysqlqdump omits). The map is a pinned-MySQL-8.0 convenience — the CI
// harness runs against the prebaked mysql:8.0 image, where these are the
// server defaults — not an exhaustive catalog. A charset absent from the
// map simply doesn't get its default-collation clause normalized, so an
// unrecognized charset can only ever produce a LOUD (never silent) extra
// diff, never a masked one.
var mysqlCharsetDefaultCollation = map[string]string{
	"utf8mb4": "utf8mb4_0900_ai_ci",
	"utf8mb3": "utf8mb3_general_ci",
	"utf8":    "utf8_general_ci",
	"latin1":  "latin1_swedish_ci",
	"ascii":   "ascii_general_ci",
	"binary":  "binary",
}

// mysqlTableDefaults extracts a table's default charset and collation
// from its options blob (`… DEFAULT CHARSET=utf8mb4 COLLATE=…`). When the
// options omit COLLATE (mysqldump omits it when the collation is the
// charset default), the collation is filled from
// mysqlCharsetDefaultCollation so the element normalizer can still
// recognize a redundant column-level COLLATE.
func mysqlTableDefaults(options string) (charset, collation string) {
	for _, f := range strings.Fields(options) {
		switch {
		case len(f) > len("CHARSET=") && strings.EqualFold(f[:len("CHARSET=")], "CHARSET="):
			charset = f[len("CHARSET="):]
		case len(f) > len("COLLATE=") && strings.EqualFold(f[:len("COLLATE=")], "COLLATE="):
			collation = f[len("COLLATE="):]
		}
	}
	if collation == "" {
		collation = mysqlCharsetDefaultCollation[strings.ToLower(charset)]
	}
	return charset, collation
}

// normalizeMySQLElement suppresses the two cosmetic over-specifications
// the MySQL oracle surfaced on the sluice-migrated target — differences
// where the STORED schema is identical but sluice's writer emits an
// attribute mysqldump omits as redundant. The normalization is applied
// symmetrically to BOTH targets' dumps (it is a no-op on the mysqldump
// oracle side, which never emits the redundant form), and it strips ONLY
// values that equal the applicable default — so a genuinely non-default
// collation (dropped or changed) or a non-BTREE index method still
// surfaces as a real divergence. The two classes:
//
//   - A column-level `CHARACTER SET <cs>` / `COLLATE <co>` clause whose
//     charset equals the table default (and collation equals that
//     charset's default): sluice materializes the resolved column
//     charset/collation on every character column; mysqldump emits it
//     only when it differs from the table default. Same effective
//     collation, different catalog text — documented in
//     docs/type-mapping.md "Charsets and collations". A genuine
//     non-default collation (e.g. region_code's latin1_bin) is NOT
//     stripped and stays under comparison.
//   - `USING BTREE` on a secondary index: BTREE is InnoDB's default (and
//     only) regular-index method, which mysqldump omits; sluice emits it
//     explicitly. Same index, different catalog text. A `USING HASH`
//     (MEMORY engine) is NOT stripped and stays under comparison.
func normalizeMySQLElement(key, body, defCharset, defCollation string) string {
	switch {
	case strings.HasPrefix(key, "COLUMN "):
		return stripColumnDefaultCollation(body, defCharset, defCollation)
	case key == "PRIMARY KEY" || strings.HasPrefix(key, "KEY "):
		return stripDefaultIndexMethod(body)
	}
	return body
}

// stripColumnDefaultCollation removes a column-level `CHARACTER SET <cs>`
// clause when cs equals the table default charset, and a `COLLATE <co>`
// clause when co equals that charset's default collation. A column's
// collation can equal the table charset's default only if the column IS
// in that charset, so keying the COLLATE strip on the TABLE default is
// sound — it can never strip a genuinely different collation.
func stripColumnDefaultCollation(body, defCharset, defCollation string) string {
	if defCharset == "" {
		return body
	}
	toks := strings.Fields(body)
	out := toks[:0]
	for i := 0; i < len(toks); i++ {
		if i+2 < len(toks) && strings.EqualFold(toks[i], "CHARACTER") &&
			strings.EqualFold(toks[i+1], "SET") && strings.EqualFold(toks[i+2], defCharset) {
			i += 2
			continue
		}
		if defCollation != "" && i+1 < len(toks) &&
			strings.EqualFold(toks[i], "COLLATE") && strings.EqualFold(toks[i+1], defCollation) {
			i++
			continue
		}
		out = append(out, toks[i])
	}
	return strings.Join(out, " ")
}

// stripDefaultIndexMethod removes a redundant `USING BTREE` clause (the
// InnoDB default regular-index method) from an index element body. A
// non-default method such as `USING HASH` is left intact so a genuine
// method difference still surfaces.
func stripDefaultIndexMethod(body string) string {
	toks := strings.Fields(body)
	out := toks[:0]
	for i := 0; i < len(toks); i++ {
		if strings.EqualFold(toks[i], "USING") && i+1 < len(toks) && strings.EqualFold(toks[i+1], "BTREE") {
			i++
			continue
		}
		out = append(out, toks[i])
	}
	return strings.Join(out, " ")
}
