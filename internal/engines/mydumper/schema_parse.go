// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mydumper

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"sluicesync.dev/sluice/internal/engines/mysql"
	"sluicesync.dev/sluice/internal/ir"
)

// This file is the bounded CREATE TABLE parser — the contained DDL-parse
// exception ADR-0161 §3 blesses against the IR-first no-DDL-grammar tenet.
// Its scope is EXACTLY the mydumper per-table schema-file shape: optional
// comments and SET statements plus ONE CREATE TABLE. Everything the parser
// does not recognise is a loud refusal naming the file and the offending
// token — never a guess. The MySQL-type→IR mapping is NOT re-implemented:
// each parsed column is folded into the same [mysql.ColumnMeta] the live
// engine's information_schema reader fills, and translated by
// [mysql.TranslateColumnType].

// schemaFileMaxBytes bounds a schema file read. A CREATE TABLE larger than
// this is not a plausible mydumper schema file.
const schemaFileMaxBytes = 64 << 20

// parseSchemaFile validates the statement inventory of one schema file's
// content (comments/SET + exactly one CREATE TABLE) and parses the CREATE
// TABLE into an IR table. file is the filename used in every error.
func parseSchemaFile(content, file string) (*ir.Table, error) {
	// Strip a UTF-8 BOM at file start (lossless, WARNed — the flatfile
	// engines' posture); anywhere else it falls to the keyword-less refusal.
	if strings.HasPrefix(content, utf8BOM) {
		content = strings.TrimPrefix(content, utf8BOM)
		slog.Warn("mydumper: stripped a UTF-8 byte-order mark at schema-file start", slog.String("file", file))
	}
	stmts, carry := splitMySQLChunk(content)
	if rest := strings.TrimSpace(carry); rest != "" {
		stmts = append(stmts, rest)
	}

	var createStmt string
	for _, stmt := range stmts {
		switch kw := statementKeyword(stmt); kw {
		case "":
			// Skippable ONLY when pure comment/whitespace; anything else is
			// torn/re-encoded content and refuses (audit 2026-07-15 CRITICAL-1).
			if err := errNonSQLFragment(stmt); err != nil {
				return nil, fmt.Errorf("mydumper: schema file %s: %w", file, err)
			}
		case "SET":
			if _, err := checkSetStatement(stmt); err != nil {
				return nil, fmt.Errorf("mydumper: schema file %s: %w", file, err)
			}
		case "CREATE":
			body := stripLeadingCommentsAndSpace(stmt)
			if !hasKeywordAt(body, len("CREATE"), "TABLE") {
				return nil, fmt.Errorf("mydumper: schema file %s contains a CREATE statement that is not "+
					"CREATE TABLE — outside the one-CREATE-TABLE-per-file shape this reader supports (ADR-0161)", file)
			}
			if createStmt != "" {
				return nil, fmt.Errorf("mydumper: schema file %s contains more than one CREATE TABLE — "+
					"outside the one-CREATE-TABLE-per-file shape this reader supports (ADR-0161)", file)
			}
			createStmt = body
		default:
			return nil, fmt.Errorf("mydumper: schema file %s contains a %s statement — only comments, "+
				"SET statements, and exactly one CREATE TABLE are supported (ADR-0161)", file, kw)
		}
	}
	if createStmt == "" {
		return nil, fmt.Errorf("mydumper: schema file %s contains no CREATE TABLE statement", file)
	}
	return parseCreateTable(createStmt, file)
}

// hasKeywordAt reports whether s carries the given keyword (followed by a
// non-identifier byte) after optional whitespace starting at offset.
func hasKeywordAt(s string, offset int, keyword string) bool {
	rest := strings.TrimLeft(s[offset:], " \t\r\n")
	if len(rest) < len(keyword) || !strings.EqualFold(rest[:len(keyword)], keyword) {
		return false
	}
	return len(rest) == len(keyword) || !isIdentByte(rest[len(keyword)])
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

type tokKind uint8

const (
	tokEOF tokKind = iota
	tokIdent
	tokString
	tokNumber
	tokBitLit // b'0101'  — text carries the bare bits
	tokHexLit // 0x1A2B / x'1A2B' — text carries the canonical 0x… spelling
	tokPunct
)

// token is one lexical unit of a CREATE TABLE statement. For tokString,
// text is the RAW source spelling (including quotes — the enum/set
// ColumnType reconstruction needs it verbatim) and val the decoded bytes.
type token struct {
	kind   tokKind
	text   string
	val    []byte
	quoted bool // ident was backtick-quoted (never a keyword)
	pos    int
}

func isIdentByte(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
		c == '_' || c == '$' || c >= 0x80
}

// tokenizeSQL lexes a single MySQL statement. Comments (all forms,
// including versioned `/*!` blocks — which in CREATE TABLE bodies carry
// only skippable physical options such as partition clauses) are dropped.
func tokenizeSQL(s, file string) ([]token, error) {
	var toks []token
	n := len(s)
	for i := 0; i < n; {
		next, err := skipSQLSpaceAndComments(s, i, file)
		if err != nil {
			return nil, err
		}
		if next >= n {
			break
		}
		i = next
		tok, end, err := scanSQLToken(s, i, file)
		if err != nil {
			return nil, err
		}
		toks = append(toks, tok)
		i = end
	}
	toks = append(toks, token{kind: tokEOF, pos: n})
	return toks, nil
}

// skipSQLSpaceAndComments advances i past whitespace, `#`/`-- ` line
// comments, and `/* */` block comments (versioned blocks included), and
// returns the index of the next token byte (or len(s)).
func skipSQLSpaceAndComments(s string, i int, file string) (int, error) {
	n := len(s)
	for i < n {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\r' || c == '\n':
			i++
		case c == '#':
			for i < n && s[i] != '\n' {
				i++
			}
		case c == '-' && i+1 < n && s[i+1] == '-' &&
			(i+2 >= n || s[i+2] == ' ' || s[i+2] == '\t' || s[i+2] == '\n' || s[i+2] == '\r'):
			for i < n && s[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && s[i+1] == '*':
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return 0, fmt.Errorf("mydumper: %s: unterminated block comment at offset %d", file, i)
			}
			i += 2 + end + 2
		default:
			return i, nil
		}
	}
	return i, nil
}

// scanSQLToken lexes the single token starting at s[i] (guaranteed a
// non-space, non-comment byte) and returns it plus the index past it.
func scanSQLToken(s string, i int, file string) (token, int, error) {
	n := len(s)
	c := s[i]
	switch {
	case c == '`':
		name, end, ok := scanBacktickIdent(s[i:])
		if !ok {
			return token{}, 0, fmt.Errorf("mydumper: %s: unterminated `identifier` at offset %d", file, i)
		}
		return token{kind: tokIdent, text: name, quoted: true, pos: i}, i + end, nil
	case c == '\'' || c == '"':
		raw, end, ok := scanSQLString(s[i:])
		if !ok {
			return token{}, 0, fmt.Errorf("mydumper: %s: unterminated string literal at offset %d", file, i)
		}
		return token{kind: tokString, text: s[i : i+end], val: raw, pos: i}, i + end, nil
	case (c == 'b' || c == 'B') && i+1 < n && s[i+1] == '\'':
		bits, end, err := scanQuotedDigits(s[i+1:], "01")
		if err != nil {
			return token{}, 0, fmt.Errorf("mydumper: %s: malformed bit literal at offset %d: %w", file, i, err)
		}
		return token{kind: tokBitLit, text: bits, pos: i}, i + 1 + end, nil
	case (c == 'x' || c == 'X') && i+1 < n && s[i+1] == '\'':
		hexDigits, end, err := scanQuotedDigits(s[i+1:], "0123456789abcdefABCDEF")
		if err != nil {
			return token{}, 0, fmt.Errorf("mydumper: %s: malformed hex literal at offset %d: %w", file, i, err)
		}
		return token{kind: tokHexLit, text: "0x" + hexDigits, pos: i}, i + 1 + end, nil
	case c == '0' && i+1 < n && (s[i+1] == 'x' || s[i+1] == 'X'):
		j := i + 2
		for j < n && isHexByte(s[j]) {
			j++
		}
		if j == i+2 {
			return token{}, 0, fmt.Errorf("mydumper: %s: malformed hex literal at offset %d", file, i)
		}
		return token{kind: tokHexLit, text: "0x" + s[i+2:j], pos: i}, j, nil
	case c >= '0' && c <= '9' || c == '.' && i+1 < n && s[i+1] >= '0' && s[i+1] <= '9':
		j := scanNumberEnd(s, i)
		return token{kind: tokNumber, text: s[i:j], pos: i}, j, nil
	case isIdentByte(c):
		j := i
		for j < n && isIdentByte(s[j]) {
			j++
		}
		return token{kind: tokIdent, text: s[i:j], pos: i}, j, nil
	default:
		// Punctuation the grammar uses: ( ) , . = ; anything else is
		// carried as a punct token too and refused where unexpected.
		return token{kind: tokPunct, text: string(c), pos: i}, i + 1, nil
	}
}

// scanNumberEnd returns the index past the numeric literal starting at
// s[i] (digits, decimal point, exponent with optional sign).
func scanNumberEnd(s string, i int) int {
	j := i
	for j < len(s) {
		d := s[j]
		if (d >= '0' && d <= '9') || d == '.' || d == 'e' || d == 'E' ||
			((d == '+' || d == '-') && j > i && (s[j-1] == 'e' || s[j-1] == 'E')) {
			j++
			continue
		}
		break
	}
	return j
}

// scanBacktickIdent decodes the backtick-quoted identifier at s[0]=='`'
// (doubled backticks escape) and reports the index past the closer.
func scanBacktickIdent(s string) (name string, end int, ok bool) {
	var sb strings.Builder
	for i := 1; i < len(s); i++ {
		if s[i] == '`' {
			if i+1 < len(s) && s[i+1] == '`' {
				sb.WriteByte('`')
				i++
				continue
			}
			return sb.String(), i + 1, true
		}
		sb.WriteByte(s[i])
	}
	return "", 0, false
}

// scanSQLString decodes the quoted string at s[0] (either quote char) with
// MySQL escape semantics — backslash escapes plus doubled-quote doubling —
// via the live engine's delimiter-aware decoder. The double-quoted form is
// mydumper ≥1.0's DEFAULT emit shape; it previously reused the single-quote
// decoder by transposing the two quote chars across the whole input, which
// copied the entire remaining statement tail per value — the worst offender
// of the Bug-191 O(rows × statement_size) class.
func scanSQLString(s string) (raw []byte, end int, ok bool) {
	if s == "" {
		return nil, 0, false
	}
	if s[0] == '\'' {
		return mysql.ScanQuotedString(s)
	}
	return mysql.ScanDoubleQuotedString(s)
}

// scanQuotedDigits scans the '…' body at s[0]=='\” allowing only the given
// digit set (bit/hex literal bodies), returning the digits and the index
// past the closing quote.
func scanQuotedDigits(s, digits string) (body string, end int, err error) {
	if s == "" || s[0] != '\'' {
		return "", 0, fmt.Errorf("expected opening quote")
	}
	for i := 1; i < len(s); i++ {
		if s[i] == '\'' {
			return s[1:i], i + 1, nil
		}
		if !strings.ContainsRune(digits, rune(s[i])) {
			return "", 0, fmt.Errorf("unexpected %q in literal body", s[i])
		}
	}
	return "", 0, fmt.Errorf("unterminated literal")
}

func isHexByte(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

// schemaParser is a recursive-descent parser over a tokenized CREATE TABLE
// statement. stmt is retained so balanced-paren expression bodies (CHECK,
// generated columns, DEFAULT expressions, functional index parts) can be
// captured as raw source text.
type schemaParser struct {
	toks []token
	i    int
	stmt string
	file string
}

func (p *schemaParser) peek() token { return p.toks[p.i] }

func (p *schemaParser) next() token {
	t := p.toks[p.i]
	if t.kind != tokEOF {
		p.i++
	}
	return t
}

// peekKeyword reports whether the next token is the given bare (unquoted)
// keyword, case-insensitively.
func (p *schemaParser) peekKeyword(kw string) bool {
	t := p.peek()
	return t.kind == tokIdent && !t.quoted && strings.EqualFold(t.text, kw)
}

// acceptKeyword consumes the next token when it is the given keyword.
func (p *schemaParser) acceptKeyword(kw string) bool {
	if p.peekKeyword(kw) {
		p.i++
		return true
	}
	return false
}

func (p *schemaParser) expectKeyword(kw string) error {
	if !p.acceptKeyword(kw) {
		return p.errAt(p.peek(), "expected %s", kw)
	}
	return nil
}

func (p *schemaParser) acceptPunct(ch string) bool {
	t := p.peek()
	if t.kind == tokPunct && t.text == ch {
		p.i++
		return true
	}
	return false
}

func (p *schemaParser) expectPunct(ch string) error {
	if !p.acceptPunct(ch) {
		return p.errAt(p.peek(), "expected %q", ch)
	}
	return nil
}

func (p *schemaParser) expectIdent() (token, error) {
	t := p.next()
	if t.kind != tokIdent {
		return t, p.errAt(t, "expected an identifier")
	}
	return t, nil
}

// errAt builds a loud parse error naming the file, byte offset, and token.
func (p *schemaParser) errAt(t token, format string, args ...any) error {
	tokText := t.text
	if t.kind == tokEOF {
		tokText = "<end of statement>"
	}
	return fmt.Errorf("mydumper: %s: CREATE TABLE parse error at offset %d (near %q): %s",
		p.file, t.pos, tokText, fmt.Sprintf(format, args...))
}

// captureParenGroup consumes a balanced parenthesized group starting at the
// current '(' token and returns the RAW source text between the outer
// parens. Used for expression bodies the IR carries as text.
func (p *schemaParser) captureParenGroup() (string, error) {
	open := p.peek()
	if err := p.expectPunct("("); err != nil {
		return "", err
	}
	depth := 1
	for {
		t := p.next()
		switch {
		case t.kind == tokEOF:
			return "", p.errAt(t, "unbalanced parentheses")
		case t.kind == tokPunct && t.text == "(":
			depth++
		case t.kind == tokPunct && t.text == ")":
			depth--
			if depth == 0 {
				return strings.TrimSpace(p.stmt[open.pos+1 : t.pos]), nil
			}
		}
	}
}

// parseCreateTable parses one CREATE TABLE statement (comments already
// tolerated by the tokenizer) into an IR table, running the charset gate
// before returning.
func parseCreateTable(stmt, file string) (*ir.Table, error) {
	toks, err := tokenizeSQL(stmt, file)
	if err != nil {
		return nil, err
	}
	p := &schemaParser{toks: toks, stmt: stmt, file: file}

	if err := p.expectKeyword("CREATE"); err != nil {
		return nil, err
	}
	if err := p.expectKeyword("TABLE"); err != nil {
		return nil, err
	}
	if p.acceptKeyword("IF") {
		if err := p.expectKeyword("NOT"); err != nil {
			return nil, err
		}
		if err := p.expectKeyword("EXISTS"); err != nil {
			return nil, err
		}
	}
	nameTok, err := p.expectIdent()
	if err != nil {
		return nil, err
	}
	tableName := nameTok.text
	if p.acceptPunct(".") { // `db`.`table` qualification
		nameTok, err = p.expectIdent()
		if err != nil {
			return nil, err
		}
		tableName = nameTok.text
	}

	t := &ir.Table{Name: tableName}
	b := &tableBuilder{p: p, table: t}

	if err := p.expectPunct("("); err != nil {
		return nil, err
	}
	for {
		if err := b.parseBodyItem(); err != nil {
			return nil, err
		}
		if p.acceptPunct(",") {
			continue
		}
		if p.acceptPunct(")") {
			break
		}
		return nil, p.errAt(p.peek(), "expected ',' or ')' after table body item")
	}
	if err := b.parseTableOptions(); err != nil {
		return nil, err
	}
	if len(t.Columns) == 0 {
		return nil, fmt.Errorf("mydumper: %s: CREATE TABLE %s has no columns", file, tableName)
	}
	// A charset violation returns the PARSED TABLE with the typed
	// refusal, so schema readers can defer it to first use (Bug 188);
	// every other error above returns a nil table as before.
	if err := b.checkCharsets(); err != nil {
		return t, err
	}
	return t, nil
}

// tableBuilder accumulates parsed body items onto the IR table plus the
// table-level charset context the column gate needs.
type tableBuilder struct {
	p     *schemaParser
	table *ir.Table

	tableCharset   string
	tableCollation string

	// columnCharsets records each string-family column's EXPLICIT charset
	// (empty = inherit the table default) for [checkCharsets].
	columnCharsets map[string]string
}

// parseBodyItem dispatches one table-body item: a key/constraint
// definition or a column definition.
func (b *tableBuilder) parseBodyItem() error {
	p := b.p
	switch {
	case p.acceptKeyword("PRIMARY"):
		if err := p.expectKeyword("KEY"); err != nil {
			return err
		}
		idx := &ir.Index{Name: "PRIMARY", Unique: true, Kind: ir.IndexKindBTree}
		if err := b.parseIndexTail(idx); err != nil {
			return err
		}
		if b.table.PrimaryKey != nil {
			return p.errAt(p.peek(), "duplicate PRIMARY KEY")
		}
		b.table.PrimaryKey = idx
		return nil

	case p.acceptKeyword("UNIQUE"):
		_ = p.acceptKeyword("KEY") || p.acceptKeyword("INDEX")
		idx := &ir.Index{Unique: true, Kind: ir.IndexKindBTree}
		return b.parseNamedIndex(idx)

	case p.acceptKeyword("KEY"), p.acceptKeyword("INDEX"):
		idx := &ir.Index{Kind: ir.IndexKindBTree}
		return b.parseNamedIndex(idx)

	case p.acceptKeyword("FULLTEXT"):
		_ = p.acceptKeyword("KEY") || p.acceptKeyword("INDEX")
		idx := &ir.Index{Kind: ir.IndexKindFullText}
		return b.parseNamedIndex(idx)

	case p.acceptKeyword("SPATIAL"):
		_ = p.acceptKeyword("KEY") || p.acceptKeyword("INDEX")
		idx := &ir.Index{Kind: ir.IndexKindSpatial}
		return b.parseNamedIndex(idx)

	case p.acceptKeyword("CONSTRAINT"):
		var name string
		if t := p.peek(); t.kind == tokIdent && !p.peekKeyword("FOREIGN") && !p.peekKeyword("CHECK") &&
			!p.peekKeyword("UNIQUE") && !p.peekKeyword("PRIMARY") {
			name = p.next().text
		}
		switch {
		case p.acceptKeyword("FOREIGN"):
			return b.parseForeignKey(name)
		case p.acceptKeyword("CHECK"):
			return b.parseCheck(name)
		case p.acceptKeyword("UNIQUE"):
			_ = p.acceptKeyword("KEY") || p.acceptKeyword("INDEX")
			idx := &ir.Index{Unique: true, Kind: ir.IndexKindBTree}
			return b.parseNamedIndex(idx)
		case p.acceptKeyword("PRIMARY"):
			if err := p.expectKeyword("KEY"); err != nil {
				return err
			}
			idx := &ir.Index{Name: "PRIMARY", Unique: true, Kind: ir.IndexKindBTree}
			if err := b.parseIndexTail(idx); err != nil {
				return err
			}
			b.table.PrimaryKey = idx
			return nil
		default:
			return p.errAt(p.peek(), "expected FOREIGN KEY, CHECK, UNIQUE, or PRIMARY KEY after CONSTRAINT")
		}

	case p.acceptKeyword("FOREIGN"):
		return b.parseForeignKey("")

	case p.acceptKeyword("CHECK"):
		return b.parseCheck("")

	default:
		return b.parseColumnDef()
	}
}

// parseNamedIndex parses `[name] [USING kind] (keyparts) [options…]`.
func (b *tableBuilder) parseNamedIndex(idx *ir.Index) error {
	p := b.p
	if t := p.peek(); t.kind == tokIdent && !p.peekKeyword("USING") {
		idx.Name = p.next().text
	}
	if err := b.parseIndexTail(idx); err != nil {
		return err
	}
	b.table.Indexes = append(b.table.Indexes, idx)
	return nil
}

// parseIndexTail parses `[USING kind] (keyparts) [USING kind]
// [KEY_BLOCK_SIZE [=] n] [COMMENT 'x'] [WITH PARSER x] [VISIBLE|INVISIBLE]`.
func (b *tableBuilder) parseIndexTail(idx *ir.Index) error {
	p := b.p
	if err := b.parseIndexUsing(idx); err != nil {
		return err
	}
	if err := p.expectPunct("("); err != nil {
		return err
	}
	for {
		part, err := b.parseKeyPart()
		if err != nil {
			return err
		}
		idx.Columns = append(idx.Columns, part)
		if p.acceptPunct(",") {
			continue
		}
		if p.acceptPunct(")") {
			break
		}
		return p.errAt(p.peek(), "expected ',' or ')' in key-part list")
	}
	for {
		switch {
		case p.peekKeyword("USING"):
			if err := b.parseIndexUsing(idx); err != nil {
				return err
			}
		case p.acceptKeyword("KEY_BLOCK_SIZE"):
			_ = p.acceptPunct("=")
			p.next() // the size value; physical option, not carried
		case p.acceptKeyword("COMMENT"):
			p.next() // the comment string; index comments are not carried
		case p.acceptKeyword("WITH"):
			if err := p.expectKeyword("PARSER"); err != nil {
				return err
			}
			p.next() // the parser plugin name; not carried
		case p.acceptKeyword("VISIBLE"), p.acceptKeyword("INVISIBLE"):
			// index visibility is a physical/optimizer toggle; not carried
		default:
			return nil
		}
	}
}

// parseIndexUsing consumes an optional `USING BTREE|HASH` clause.
func (b *tableBuilder) parseIndexUsing(idx *ir.Index) error {
	p := b.p
	if !p.acceptKeyword("USING") {
		return nil
	}
	t, err := p.expectIdent()
	if err != nil {
		return err
	}
	switch strings.ToUpper(t.text) {
	case "BTREE":
		idx.Kind = ir.IndexKindBTree
	case "HASH":
		idx.Kind = ir.IndexKindHash
	default:
		return p.errAt(t, "unsupported index USING %s", t.text)
	}
	return nil
}

// parseKeyPart parses one index key part: a column reference with optional
// prefix length and direction, or a parenthesized functional expression.
func (b *tableBuilder) parseKeyPart() (ir.IndexColumn, error) {
	p := b.p
	if p.peek().kind == tokPunct && p.peek().text == "(" {
		expr, err := p.captureParenGroup()
		if err != nil {
			return ir.IndexColumn{}, err
		}
		part := ir.IndexColumn{
			Expression:        mysql.NormalizeExpressionText(expr),
			ExpressionDialect: "mysql",
		}
		part.Desc = b.acceptDirection()
		return part, nil
	}
	col, err := p.expectIdent()
	if err != nil {
		return ir.IndexColumn{}, err
	}
	part := ir.IndexColumn{Column: col.text}
	if p.acceptPunct("(") {
		lenTok := p.next()
		if lenTok.kind != tokNumber {
			return ir.IndexColumn{}, p.errAt(lenTok, "expected a prefix length")
		}
		n, err := strconv.Atoi(lenTok.text)
		if err != nil {
			return ir.IndexColumn{}, p.errAt(lenTok, "bad prefix length: %v", err)
		}
		part.Length = n
		if err := p.expectPunct(")"); err != nil {
			return ir.IndexColumn{}, err
		}
	}
	part.Desc = b.acceptDirection()
	return part, nil
}

// acceptDirection consumes an optional ASC/DESC, reporting DESC.
func (b *tableBuilder) acceptDirection() bool {
	if b.p.acceptKeyword("DESC") {
		return true
	}
	_ = b.p.acceptKeyword("ASC")
	return false
}

// parseForeignKey parses `FOREIGN KEY [idx] (cols) REFERENCES tbl (cols)
// [MATCH …] [ON DELETE act] [ON UPDATE act]` (the FOREIGN keyword already
// consumed).
func (b *tableBuilder) parseForeignKey(name string) error {
	p := b.p
	if err := p.expectKeyword("KEY"); err != nil {
		return err
	}
	if t := p.peek(); t.kind == tokIdent { // optional index name
		p.next()
	}
	cols, err := b.parseColumnNameList()
	if err != nil {
		return err
	}
	if err := p.expectKeyword("REFERENCES"); err != nil {
		return err
	}
	refTok, err := p.expectIdent()
	if err != nil {
		return err
	}
	refTable := refTok.text
	if p.acceptPunct(".") {
		refTok, err = p.expectIdent()
		if err != nil {
			return err
		}
		refTable = refTok.text
	}
	refCols, err := b.parseColumnNameList()
	if err != nil {
		return err
	}
	fk := &ir.ForeignKey{
		Name:              name,
		Columns:           cols,
		ReferencedTable:   refTable,
		ReferencedColumns: refCols,
	}
	for {
		switch {
		case p.acceptKeyword("MATCH"):
			p.next() // FULL | PARTIAL | SIMPLE — parsed-and-ignored by MySQL itself
		case p.acceptKeyword("ON"):
			isDelete := false
			switch {
			case p.acceptKeyword("DELETE"):
				isDelete = true
			case p.acceptKeyword("UPDATE"):
			default:
				return p.errAt(p.peek(), "expected DELETE or UPDATE after ON")
			}
			act, err := b.parseFKAction()
			if err != nil {
				return err
			}
			if isDelete {
				fk.OnDelete = act
			} else {
				fk.OnUpdate = act
			}
		default:
			b.table.ForeignKeys = append(b.table.ForeignKeys, fk)
			return nil
		}
	}
}

// parseFKAction parses a referential action.
func (b *tableBuilder) parseFKAction() (ir.FKAction, error) {
	p := b.p
	switch {
	case p.acceptKeyword("CASCADE"):
		return ir.FKActionCascade, nil
	case p.acceptKeyword("RESTRICT"):
		return ir.FKActionRestrict, nil
	case p.acceptKeyword("NO"):
		if err := p.expectKeyword("ACTION"); err != nil {
			return 0, err
		}
		return ir.FKActionNoAction, nil
	case p.acceptKeyword("SET"):
		switch {
		case p.acceptKeyword("NULL"):
			return ir.FKActionSetNull, nil
		case p.acceptKeyword("DEFAULT"):
			return ir.FKActionSetDefault, nil
		}
		return 0, p.errAt(p.peek(), "expected NULL or DEFAULT after SET")
	}
	return 0, p.errAt(p.peek(), "expected a referential action")
}

// parseCheck parses `CHECK (expr) [[NOT] ENFORCED]` (the CHECK keyword
// already consumed). A NOT ENFORCED check is dropped with a WARN — it does
// not constrain the source's data, and carrying it to a target that always
// enforces would reject rows the source legitimately holds.
func (b *tableBuilder) parseCheck(name string) error {
	p := b.p
	expr, err := p.captureParenGroup()
	if err != nil {
		return err
	}
	enforced := true
	if p.acceptKeyword("NOT") {
		if err := p.expectKeyword("ENFORCED"); err != nil {
			return err
		}
		enforced = false
	} else {
		_ = p.acceptKeyword("ENFORCED")
	}
	if !enforced {
		slog.Warn("mydumper: dropping NOT ENFORCED CHECK constraint (it does not constrain the "+
			"source's rows; a target would enforce it)",
			slog.String("table", b.table.Name), slog.String("constraint", name))
		return nil
	}
	b.table.CheckConstraints = append(b.table.CheckConstraints, &ir.CheckConstraint{
		Name:        name,
		Expr:        mysql.NormalizeExpressionText(expr),
		ExprDialect: "mysql",
	})
	return nil
}

// parseColumnNameList parses `(col, col, …)`.
func (b *tableBuilder) parseColumnNameList() ([]string, error) {
	p := b.p
	if err := p.expectPunct("("); err != nil {
		return nil, err
	}
	var cols []string
	for {
		t, err := p.expectIdent()
		if err != nil {
			return nil, err
		}
		cols = append(cols, t.text)
		if p.acceptPunct(",") {
			continue
		}
		if p.acceptPunct(")") {
			return cols, nil
		}
		return nil, p.errAt(p.peek(), "expected ',' or ')' in column list")
	}
}

// parseTableOptions loosely scans the option tail after the closing ')'.
// Only the options the IR carries are interpreted (DEFAULT CHARSET /
// COLLATE — the charset gate's context — and COMMENT); everything else
// (ENGINE, AUTO_INCREMENT, ROW_FORMAT, STATS_*, …) is physical and skipped
// token-by-token.
func (b *tableBuilder) parseTableOptions() error {
	p := b.p
	for {
		t := p.peek()
		if t.kind == tokEOF {
			return nil
		}
		switch {
		case p.acceptKeyword("DEFAULT"):
			// DEFAULT CHARSET=… / DEFAULT CHARACTER SET=… / DEFAULT COLLATE=…
		case p.acceptKeyword("CHARSET"):
			cs, err := b.parseOptionValue()
			if err != nil {
				return err
			}
			b.tableCharset = strings.ToLower(cs)
		case p.acceptKeyword("CHARACTER"):
			if err := p.expectKeyword("SET"); err != nil {
				return err
			}
			cs, err := b.parseOptionValue()
			if err != nil {
				return err
			}
			b.tableCharset = strings.ToLower(cs)
		case p.acceptKeyword("COLLATE"):
			c, err := b.parseOptionValue()
			if err != nil {
				return err
			}
			b.tableCollation = strings.ToLower(c)
		case p.acceptKeyword("COMMENT"):
			_ = p.acceptPunct("=")
			ct := p.next()
			if ct.kind != tokString {
				return p.errAt(ct, "expected a string after COMMENT")
			}
			b.table.Comment = string(ct.val)
		default:
			p.next() // skip physical option tokens (ENGINE, =, InnoDB, …)
		}
	}
}

// parseOptionValue consumes `[=] value` where value is an ident, number,
// or string, returning its text.
func (b *tableBuilder) parseOptionValue() (string, error) {
	p := b.p
	_ = p.acceptPunct("=")
	t := p.next()
	switch t.kind {
	case tokIdent, tokNumber:
		return t.text, nil
	case tokString:
		return string(t.val), nil
	default:
		return "", p.errAt(t, "expected an option value")
	}
}

// utf8CompatibleCharsets is the charset allowlist for string-carrying
// columns: sets under which the dump's raw bytes are valid UTF-8 for the
// values MySQL permits in them. Anything else (latin1, cp1251, gbk, …)
// would need transcoding this reader refuses to do silently (ADR-0161 §5).
var utf8CompatibleCharsets = map[string]bool{
	"utf8":    true,
	"utf8mb3": true,
	"utf8mb4": true,
	"ascii":   true,
	"binary":  true,
}

// CharsetRefusalError is the ADR-0161 §5 unsupported-charset refusal,
// typed so callers can DEFER it to the table's first actual use instead
// of failing the whole schema read (Bug 188: one legacy latin1 table
// blocked migrating the REST of a dump even under --exclude-table,
// because the refusal fired inside ReadSchema — before the pipeline's
// table filter could route around it). parseCreateTable returns the
// PARSED TABLE alongside this error, so schema readers can carry the
// table and surface the refusal only when the table is actually read.
type CharsetRefusalError struct {
	File    string
	Table   string
	Column  string
	Charset string
}

func (e *CharsetRefusalError) Error() string {
	return fmt.Sprintf("mydumper: %s: table %s column %s has charset %s — the flat-file reader "+
		"carries dump bytes verbatim and supports only UTF-8-compatible charsets "+
		"(utf8mb4/utf8/utf8mb3/ascii/binary); convert the source column, migrate from the "+
		"live database instead, or --exclude-table the table (ADR-0161 §5)",
		e.File, e.Table, e.Column, e.Charset)
}

// checkCharsets refuses any string-family column whose EFFECTIVE charset
// (explicit column charset, else the table default, else the charset the
// table COLLATION implies, else the assumed utf8mb4) is not
// UTF-8-compatible. The returned error is always a *CharsetRefusalError
// so callers can defer it (see the type doc).
func (b *tableBuilder) checkCharsets() error {
	for _, col := range b.table.Columns {
		switch col.Type.(type) {
		case ir.Char, ir.Varchar, ir.Text, ir.Enum, ir.Set:
		default:
			continue
		}
		effective := b.columnCharsets[col.Name]
		if effective == "" {
			effective = b.tableCharset
		}
		if effective == "" {
			// A collation-only declaration (`COLLATE=latin1_swedish_ci`
			// with no CHARSET) still pins the charset — MySQL derives it
			// from the collation, and so must this gate (audit L-D0-1:
			// tableCollation was parsed but never consulted, so a
			// hand-edited latin1-collated schema passed as utf8mb4).
			// mydumper-written schemas always carry CHARSET, so this
			// branch only fires on hand-edited files.
			effective = charsetOfCollation(b.tableCollation)
		}
		if effective == "" {
			effective = "utf8mb4"
		}
		if !utf8CompatibleCharsets[effective] {
			return &CharsetRefusalError{
				File: b.p.file, Table: b.table.Name, Column: col.Name, Charset: effective,
			}
		}
	}
	return nil
}

// charsetOfCollation derives a collation's charset: MySQL collation names
// are `<charset>_<comparison rules>` (latin1_swedish_ci → latin1,
// utf8mb4_0900_ai_ci → utf8mb4), with the bare `binary` collation naming
// its charset outright. Empty in, empty out.
func charsetOfCollation(collation string) string {
	cs, _, _ := strings.Cut(collation, "_")
	return cs
}
