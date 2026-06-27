// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"log/slog"
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// sqliteDialect is the IR dialect tag stamped on every schema-feature
// expression this engine carries (generated-column bodies, CHECK
// constraints, partial-index predicates, expression-index columns).
//
// It is deliberately NOT one of the two known cross-dialect translators'
// inputs ("mysql"/"postgres"). The PG and MySQL writers translate ONLY from
// the specific other engine they know (PG iff "mysql", MySQL iff "postgres",
// ADR-0133 §2); every other tag — including this one — emits VERBATIM. So a
// SQLite expression is carried faithfully and a non-portable construct fails
// loudly at target DDL time rather than being silently mistranslated.
const sqliteDialect = "sqlite"

// SQLite schema-feature extraction (ADR-0133)
//
// SQLite exposes columns/indexes/FKs via PRAGMAs, but the EXPRESSION bodies of
// generated columns, CHECK constraints, partial-index predicates, and
// expression-index columns live only in the verbatim `sql` text recorded in
// sqlite_master (the `CREATE TABLE` / `CREATE INDEX` the user wrote). These
// helpers parse that text with a small SQLite-aware tokenizer so the bodies can
// be carried into the IR's existing fields.
//
// The tokenizer is paren / string / quoted-identifier / comment aware: it skips
// the contents of '…' string literals, "…" / `…` / [—] quoted identifiers, and
// `--` / block comments, so a keyword (CHECK, AS, WHERE) is only ever matched as
// a standalone token — never inside a string, a column named `checkout`, or a
// quoted identifier `"CHECK"`. Identifier quoting is stripped on bare idents at
// the read boundary, matching the PG/MySQL readers' normalization convention
// (the IR holds engine-neutral expression text); the substantive expression
// body is preserved verbatim.

// sqlTokKind classifies a token produced by [tokenizeSQLite].
type sqlTokKind int

const (
	// tokWord is an unquoted run of identifier/keyword/number bytes.
	tokWord sqlTokKind = iota
	// tokString is a single-quoted string literal '…' (doubled '' escape).
	tokString
	// tokIdent is a quoted identifier: "…", `…`, or [—].
	tokIdent
	// tokPunct is a single punctuation byte (parens, comma, operators, …).
	tokPunct
)

// sqlTok is one lexical token, recorded by byte offset into the source so the
// raw substring (e.g. a balanced expression body) can be sliced out verbatim.
type sqlTok struct {
	kind sqlTokKind
	off  int // byte offset of the token start
	end  int // byte offset just past the token end
}

func (t sqlTok) text(src string) string { return src[t.off:t.end] }

// tokenizeSQLite splits s into tokens, dropping whitespace and comments. String
// literals and quoted identifiers are each returned as a single token (with
// their delimiters) so callers can both skip past them and recover their inner
// text.
func tokenizeSQLite(s string) []sqlTok {
	var toks []sqlTok
	i, n := 0, len(s)
	for i < n {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case c == '-' && i+1 < n && s[i+1] == '-':
			// Line comment: to end of line (or input).
			i += 2
			for i < n && s[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && s[i+1] == '*':
			// Block comment: to the closing */ (or input end if unterminated).
			i += 2
			for i < n && (s[i] != '*' || i+1 >= n || s[i+1] != '/') {
				i++
			}
			if i < n {
				i += 2 // consume the closing */
			}
		case c == '\'':
			j := skipQuoted(s, i, '\'')
			toks = append(toks, sqlTok{tokString, i, j})
			i = j
		case c == '"' || c == '`':
			j := skipQuoted(s, i, c)
			toks = append(toks, sqlTok{tokIdent, i, j})
			i = j
		case c == '[':
			// Bracket-quoted identifier: ends at the first ']' (no escape form).
			j := i + 1
			for j < n && s[j] != ']' {
				j++
			}
			if j < n {
				j++ // include the closing ]
			}
			toks = append(toks, sqlTok{tokIdent, i, j})
			i = j
		case isWordByte(c):
			j := i + 1
			for j < n && isWordByte(s[j]) {
				j++
			}
			toks = append(toks, sqlTok{tokWord, i, j})
			i = j
		default:
			toks = append(toks, sqlTok{tokPunct, i, i + 1})
			i++
		}
	}
	return toks
}

// skipQuoted returns the index just past the closing quote q for the literal
// opening at index i. A doubled quote (q immediately followed by q) is an
// embedded escape, not a terminator (the SQLite rule for ', ", and `). An
// unterminated literal consumes to the end of input.
func skipQuoted(s string, i int, q byte) int {
	n := len(s)
	j := i + 1
	for j < n {
		if s[j] == q {
			if j+1 < n && s[j+1] == q {
				j += 2
				continue
			}
			return j + 1
		}
		j++
	}
	return n
}

// isWordByte reports whether c can appear in an unquoted identifier / keyword /
// numeric token. UTF-8 continuation/lead bytes (>= 0x80) are treated as word
// bytes so non-ASCII identifiers tokenize as one unit.
func isWordByte(c byte) bool {
	return c == '_' || c == '$' ||
		(c >= '0' && c <= '9') ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		c >= 0x80
}

// matchParen returns the token index of the ')' that closes the '(' at openIdx,
// honouring nested parens (string/ident/comment content never reaches here —
// the tokenizer already collapsed it). ok is false for an unbalanced list.
func matchParen(toks []sqlTok, src string, openIdx int) (closeIdx int, ok bool) {
	depth := 0
	for k := openIdx; k < len(toks); k++ {
		if toks[k].kind != tokPunct {
			continue
		}
		switch src[toks[k].off] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return k, true
			}
		}
	}
	return 0, false
}

// firstPunctTok returns the index of the first top-level punctuation token whose
// byte equals ch, or -1. Used to find the opening '(' of a CREATE TABLE column
// list or a CREATE INDEX column list.
func firstPunctTok(toks []sqlTok, src string, ch byte) int {
	for k := range toks {
		if toks[k].kind == tokPunct && src[toks[k].off] == ch {
			return k
		}
	}
	return -1
}

// unquoteIdentToken returns the bare identifier text of a token, stripping a
// single layer of "…" / `…` / [—] quoting (matching the read-boundary
// normalization). A tokWord is returned as-is.
func unquoteIdentToken(t sqlTok, src string) string {
	s := t.text(src)
	if t.kind != tokIdent || len(s) < 2 {
		return s
	}
	switch s[0] {
	case '"', '`':
		inner := s[1 : len(s)-1]
		q := s[0]
		return strings.ReplaceAll(inner, string([]byte{q, q}), string(q))
	case '[':
		return s[1 : len(s)-1]
	}
	return s
}

// isBareIdent reports whether s is a simple unquoted SQL identifier
// (letter/underscore/non-ASCII start, then word bytes). Quoting is only
// stripped from bodies for bare idents; anything needing the quotes (spaces,
// leading digit, empty) keeps them verbatim so the target sees the original.
func isBareIdent(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if i == 0 && (c >= '0' && c <= '9') {
			return false
		}
		if !isWordByte(c) {
			return false
		}
	}
	return true
}

// stripSQLiteIdentQuotes removes SQLite identifier quoting ("…", `…`, [—]) from
// the BARE identifiers in an expression, leaving string literals ('…') and any
// non-bare quoted text untouched. This matches the PG/MySQL readers'
// read-boundary convention of holding engine-neutral expression text in the IR;
// the substantive expression body (functions, operators) is preserved verbatim
// — non-portable constructs still fail loudly at target DDL time.
func stripSQLiteIdentQuotes(s string) string {
	if !strings.ContainsAny(s, "\"`[") {
		return s
	}
	var sb strings.Builder
	sb.Grow(len(s))
	i, n := 0, len(s)
	for i < n {
		c := s[i]
		switch c {
		case '\'':
			j := skipQuoted(s, i, '\'')
			sb.WriteString(s[i:j]) // string literal: verbatim
			i = j
		case '"', '`':
			j := skipQuoted(s, i, c)
			inner := s[i+1 : j-1]
			if !strings.ContainsRune(inner, rune(c)) && isBareIdent(inner) {
				sb.WriteString(inner)
			} else {
				sb.WriteString(s[i:j])
			}
			i = j
		case '[':
			j := i + 1
			for j < n && s[j] != ']' {
				j++
			}
			end := j
			if j < n {
				end = j + 1
			}
			inner := s[i+1 : j]
			if isBareIdent(inner) {
				sb.WriteString(inner)
			} else {
				sb.WriteString(s[i:end])
			}
			i = end
		default:
			sb.WriteByte(c)
			i++
		}
	}
	return sb.String()
}

// tableBodyParens returns the open/close token indices of a CREATE TABLE /
// CREATE INDEX column-list parenthesised block, or ok=false when the SQL has no
// balanced top-level list (e.g. a CREATE TABLE … AS SELECT, which carries none
// of these features).
func tableBodyParens(toks []sqlTok, src string) (open, closeIdx int, ok bool) {
	open = firstPunctTok(toks, src, '(')
	if open < 0 {
		return 0, 0, false
	}
	closeIdx, ok = matchParen(toks, src, open)
	if !ok {
		return 0, 0, false
	}
	return open, closeIdx, true
}

// extractCheckConstraints parses every CHECK clause — table-level and
// column-level — from a CREATE TABLE SQL, in declaration order. Each carries
// its constraint name (when written `CONSTRAINT n CHECK(…)`, else ""), the
// balanced expression text (quote-stripped), and the "sqlite" dialect tag. The
// scan only matches CHECK as a standalone keyword token, so `checkout` columns,
// string defaults containing CHECK, and `"CHECK"`-quoted identifiers never
// false-positive.
func extractCheckConstraints(createSQL string) []*ir.CheckConstraint {
	toks := tokenizeSQLite(createSQL)
	open, closeIdx, ok := tableBodyParens(toks, createSQL)
	if !ok {
		return nil
	}
	var checks []*ir.CheckConstraint
	pendingName := ""
	for k := open + 1; k < closeIdx; k++ {
		t := toks[k]
		if t.kind == tokPunct {
			switch createSQL[t.off] {
			case '(':
				ci, ok := matchParen(toks, createSQL, k)
				if !ok {
					return checks
				}
				k = ci // skip the nested group
			case ',':
				pendingName = "" // a new top-level definition begins
			}
			continue
		}
		if t.kind != tokWord {
			continue
		}
		switch {
		case strings.EqualFold(t.text(createSQL), "CONSTRAINT"):
			if k+1 < closeIdx && (toks[k+1].kind == tokWord || toks[k+1].kind == tokIdent) {
				pendingName = unquoteIdentToken(toks[k+1], createSQL)
				k++ // consume the name token
			}
		case strings.EqualFold(t.text(createSQL), "CHECK"):
			if k+1 < closeIdx && toks[k+1].kind == tokPunct && createSQL[toks[k+1].off] == '(' {
				ci, ok := matchParen(toks, createSQL, k+1)
				if !ok {
					return checks
				}
				inner := createSQL[toks[k+1].end:toks[ci].off]
				checks = append(checks, &ir.CheckConstraint{
					Name:        pendingName,
					Expr:        strings.TrimSpace(stripSQLiteIdentQuotes(inner)),
					ExprDialect: sqliteDialect,
				})
				pendingName = ""
				k = ci
			}
		case strings.EqualFold(t.text(createSQL), "PRIMARY"),
			strings.EqualFold(t.text(createSQL), "UNIQUE"),
			strings.EqualFold(t.text(createSQL), "FOREIGN"):
			// A different named constraint consumes the pending CONSTRAINT name.
			pendingName = ""
		}
	}
	return checks
}

// extractGeneratedExpr returns the generation expression of the named column,
// extracted from the CREATE TABLE SQL (the `AS (…)` body). The column's storage
// class (VIRTUAL/STORED) comes from PRAGMA table_xinfo, not from here. ok is
// false when the column's definition or its `AS (…)` clause can't be located —
// the caller then carries the column's computed values as a plain column rather
// than emit a half-formed generated column.
func extractGeneratedExpr(createSQL, colName string) (string, bool) {
	toks := tokenizeSQLite(createSQL)
	open, closeIdx, ok := tableBodyParens(toks, createSQL)
	if !ok {
		return "", false
	}
	k := open + 1
	for k < closeIdx {
		name := ""
		if first := toks[k]; first.kind == tokWord || first.kind == tokIdent {
			name = unquoteIdentToken(first, createSQL)
		}
		entryEnd, expr, found := scanEntryForGeneratedAS(toks, createSQL, k, closeIdx)
		if found && strings.EqualFold(name, colName) {
			return strings.TrimSpace(stripSQLiteIdentQuotes(expr)), true
		}
		k = entryEnd + 1 // step over the separating comma (or past closeIdx)
	}
	return "", false
}

// scanEntryForGeneratedAS walks one top-level column/constraint definition
// (starting at start) until the next top-level comma or the list close, looking
// for a standalone `AS` token immediately followed by a balanced `(…)` — the
// SQLite generated-column body. entryEnd is the token index of the terminating
// comma (or closeIdx). Type parens (e.g. NUMERIC(10,2)) are skipped, so only the
// genuine `AS (…)` clause is captured.
func scanEntryForGeneratedAS(toks []sqlTok, src string, start, closeIdx int) (entryEnd int, expr string, found bool) {
	for k := start; k < closeIdx; k++ {
		t := toks[k]
		if t.kind == tokPunct {
			switch src[t.off] {
			case '(':
				ci, ok := matchParen(toks, src, k)
				if !ok {
					return closeIdx, expr, found
				}
				k = ci
			case ',':
				return k, expr, found
			}
			continue
		}
		if t.kind == tokWord && strings.EqualFold(t.text(src), "AS") &&
			k+1 < closeIdx && toks[k+1].kind == tokPunct && src[toks[k+1].off] == '(' {
			ci, ok := matchParen(toks, src, k+1)
			if ok {
				expr = src[toks[k+1].end:toks[ci].off]
				found = true
				k = ci
			}
		}
	}
	return closeIdx, expr, found
}

// extractIndexPredicate returns the WHERE predicate of a partial index from its
// CREATE INDEX SQL (everything after the standalone WHERE keyword that follows
// the column list), quote-stripped. ok is false when no WHERE tail is present.
func extractIndexPredicate(createIndexSQL string) (string, bool) {
	toks := tokenizeSQLite(createIndexSQL)
	_, closeIdx, ok := tableBodyParens(toks, createIndexSQL)
	if !ok {
		return "", false
	}
	for k := closeIdx + 1; k < len(toks); k++ {
		if toks[k].kind == tokWord && strings.EqualFold(toks[k].text(createIndexSQL), "WHERE") {
			pred := strings.TrimSpace(createIndexSQL[toks[k].end:])
			pred = strings.TrimSuffix(pred, ";")
			pred = strings.TrimSpace(stripSQLiteIdentQuotes(pred))
			return pred, pred != ""
		}
	}
	return "", false
}

// indexColEntry is one parsed entry of a CREATE INDEX column list: its
// expression/column text (quote-stripped, trailing COLLATE/ASC/DESC removed)
// and the descending flag.
type indexColEntry struct {
	text string
	desc bool
}

// extractIndexColumnEntries parses the parenthesised column list of a CREATE
// INDEX SQL into per-entry texts (in order), splitting on top-level commas. Each
// entry's trailing `ASC`/`DESC` and `COLLATE <name>` decorations are removed so
// an expression entry carries only the indexed expression (PG/MySQL want the
// direction on the IndexColumn, not inside the expression paren). ok is false
// when the list can't be located/balanced — the caller then WARN-skips the
// index rather than carry a guessed expression.
func extractIndexColumnEntries(createIndexSQL string) ([]indexColEntry, bool) {
	toks := tokenizeSQLite(createIndexSQL)
	open, closeIdx, ok := tableBodyParens(toks, createIndexSQL)
	if !ok {
		return nil, false
	}
	var entries []indexColEntry
	spanStart := toks[open].end
	flush := func(spanEnd int) {
		entries = append(entries, parseIndexColEntry(strings.TrimSpace(createIndexSQL[spanStart:spanEnd])))
	}
	for k := open + 1; k < closeIdx; k++ {
		t := toks[k]
		if t.kind != tokPunct {
			continue
		}
		switch createIndexSQL[t.off] {
		case '(':
			ci, ok := matchParen(toks, createIndexSQL, k)
			if !ok {
				return nil, false
			}
			k = ci
		case ',':
			flush(t.off)
			spanStart = t.end
		}
	}
	flush(toks[closeIdx].off)
	return entries, true
}

// parseIndexColEntry strips a single CREATE INDEX column-list entry's trailing
// `ASC`/`DESC` direction and `COLLATE <name>` clause, returning the remaining
// expression/column text (quote-stripped) and whether it was descending.
func parseIndexColEntry(raw string) indexColEntry {
	toks := tokenizeSQLite(raw)
	end := len(toks)
	desc := false
	if end > 0 && toks[end-1].kind == tokWord {
		switch {
		case strings.EqualFold(toks[end-1].text(raw), "DESC"):
			desc = true
			end--
		case strings.EqualFold(toks[end-1].text(raw), "ASC"):
			end--
		}
	}
	if end >= 2 && toks[end-2].kind == tokWord && strings.EqualFold(toks[end-2].text(raw), "COLLATE") {
		end -= 2
	}
	if end == 0 {
		return indexColEntry{text: strings.TrimSpace(stripSQLiteIdentQuotes(raw)), desc: desc}
	}
	body := strings.TrimSpace(raw[toks[0].off:toks[end-1].end])
	return indexColEntry{text: strings.TrimSpace(stripSQLiteIdentQuotes(body)), desc: desc}
}

// indexInfoEntry is the per-position result of PRAGMA index_info, normalized so
// the file and d1 readers feed [buildIndexColumns] the same shape: the column
// name, or isExpr=true when index_info returned a NULL name (an expression
// entry whose text lives in the CREATE INDEX SQL).
type indexInfoEntry struct {
	name   string
	isExpr bool
}

// buildIndexColumns combines the PRAGMA index_info entries with the CREATE INDEX
// SQL to produce IR index columns. A plain-column index (no NULL-name entries)
// is built from index_info alone — byte-identical to the pre-ADR-0133 reader.
// An expression index carries each NULL-name entry's expression (tagged
// "sqlite") parsed from the CREATE INDEX list; if that list can't be cleanly
// parsed into the same number of entries, the whole index is WARN-skipped
// (ok=false) rather than carrying a guessed expression (ADR-0133 §A.4).
func buildIndexColumns(ctx context.Context, tableName, indexName string, entries []indexInfoEntry, createIndexSQL string) (cols []ir.IndexColumn, exprCount int, ok bool) {
	hasExpr := false
	for _, e := range entries {
		if e.isExpr {
			hasExpr = true
			break
		}
	}
	if !hasExpr {
		cols = make([]ir.IndexColumn, len(entries))
		for i, e := range entries {
			cols[i] = ir.IndexColumn{Column: e.name}
		}
		return cols, 0, true
	}

	parsed, pok := extractIndexColumnEntries(createIndexSQL)
	if !pok || len(parsed) != len(entries) {
		slog.WarnContext(
			ctx,
			"sqlite: could not cleanly parse expression-index column list; skipping this index (it is NOT carried to the target — recreate it there if needed)",
			slog.String("table", tableName),
			slog.String("index", indexName),
		)
		return nil, 0, false
	}
	cols = make([]ir.IndexColumn, len(entries))
	for i, e := range entries {
		if e.isExpr {
			cols[i] = ir.IndexColumn{
				Expression:        parsed[i].text,
				ExpressionDialect: sqliteDialect,
				Desc:              parsed[i].desc,
			}
			exprCount++
		} else {
			cols[i] = ir.IndexColumn{Column: e.name}
		}
	}
	return cols, exprCount, true
}

// warnTableVerbatim emits ONE WARN per table that carries any "sqlite"-dialect
// generated-column or CHECK body (ADR-0133 §A.5). The carried text is verbatim
// SQLite SQL: portable constructs work on the target, non-portable ones are
// rejected loudly at target DDL time, and the residual edge — a bare operator
// whose meaning differs under SQLite's loose typing — is what this WARN tells
// the operator to verify.
func warnTableVerbatim(ctx context.Context, t *ir.Table, generatedCols []string) {
	if len(generatedCols) == 0 && len(t.CheckConstraints) == 0 {
		return
	}
	slog.WarnContext(
		ctx,
		"sqlite: schema-feature expressions carried VERBATIM from SQLite; verify them on the target "+
			"(non-portable constructs are rejected loudly at target DDL time; a bare operator may behave "+
			"differently under SQLite's loose typing)",
		slog.String("table", t.Name),
		slog.Any("generated_columns", generatedCols),
		slog.Int("check_constraints", len(t.CheckConstraints)),
	)
}

// warnIndexVerbatim emits ONE WARN per index that carries a "sqlite"-dialect
// partial-index predicate and/or expression-index column (ADR-0133 §A.5).
func warnIndexVerbatim(ctx context.Context, tableName, indexName string, hasPredicate bool, exprCount int) {
	if !hasPredicate && exprCount == 0 {
		return
	}
	slog.WarnContext(
		ctx,
		"sqlite: index predicate/expression carried VERBATIM from SQLite; verify it on the target "+
			"(non-portable constructs are rejected loudly at target DDL time)",
		slog.String("table", tableName),
		slog.String("index", indexName),
		slog.Bool("partial_predicate", hasPredicate),
		slog.Int("expression_columns", exprCount),
	)
}

// applyGeneratedAndChecks populates generated-column expressions (for the
// columns table_xinfo flagged generated, keyed name→stored in genStored) and
// table CHECK constraints on t from the CREATE TABLE SQL, then emits the
// per-table verbatim-carry WARN. Shared by the file and d1 schema readers.
func applyGeneratedAndChecks(ctx context.Context, t *ir.Table, createSQL string, genStored map[string]bool) {
	var genCols []string
	for _, col := range t.Columns {
		stored, isGen := genStored[col.Name]
		if !isGen {
			continue
		}
		expr, ok := extractGeneratedExpr(createSQL, col.Name)
		if !ok {
			// table_xinfo says generated but the AS(…) body couldn't be located.
			// Carry the column's computed values as a plain column (no data
			// loss — the value is still copied) rather than emit a half-formed
			// generated column; warn so the dropped derivation is visible.
			slog.WarnContext(
				ctx,
				"sqlite: column is generated but its generation expression could not be extracted "+
					"from the CREATE TABLE SQL; carrying its computed values as a plain column "+
					"(the generation expression is NOT carried)",
				slog.String("table", t.Name),
				slog.String("column", col.Name),
			)
			continue
		}
		col.GeneratedExpr = expr
		col.GeneratedStored = stored
		col.GeneratedExprDialect = sqliteDialect
		genCols = append(genCols, col.Name)
	}
	t.CheckConstraints = extractCheckConstraints(createSQL)
	warnTableVerbatim(ctx, t, genCols)
}
