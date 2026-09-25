// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	mysqldrv "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// Catalog expression-text recovery (Bug 288 / GC-37 (a)) — the expression
// sibling of the literal-default recovery in text_default_recovery.go.
//
// information_schema renders four kinds of stored expression — an
// expression DEFAULT (COLUMN_DEFAULT with DEFAULT_GENERATED), a generated
// column (GENERATION_EXPRESSION), a CHECK (CHECK_CLAUSE) and a functional
// key part (STATISTICS.EXPRESSION) — and neither flavor renders a non-ASCII
// character in them faithfully. Measured 2026-09-24 on MySQL 8.0.46 / 8.4.x
// and MariaDB 10.11–12.3.
//
// # MySQL: every stored byte widened, whatever its literal's charset
//
// MySQL stores an expression as text whose string literals each carry a
// charset introducer, and each literal's bytes are in THAT charset.
// information_schema renders every stored byte as the character with that
// code point (ISO-8859-1 read, UTF-8 re-encoded) and then escapes each `\`
// and `'` once more. SHOW CREATE TABLE prints a literal either as its
// stored bytes read as UTF-8 — one '?' for each byte of a sequence that is
// not UTF-8 or is a four-byte one — or, for a STORED generated column, as
// its VALUE under a `_utf8mb4` introducer (`_latin1'<C3><A9>'` prints as
// `_utf8mb4'Ã©'` there, and as `_latin1'é'` in a VIRTUAL one). Measured:
//
//	declared (session)               stored bytes      information_schema          SHOW CREATE        true value
//	DEFAULT ('é')        (utf8mb4)   _utf8mb4'C3A9'    _utf8mb4\'Ã©\'             (_utf8mb4'é')      é
//	DEFAULT ('😀x')      (utf8mb4)   _utf8mb4'F09F…'   _utf8mb4\'ð\x9f\x98\x80x\'  (_utf8mb4'????x')  😀x
//	DEFAULT ('Ã©')       (latin1)    _latin1'C3A9'     _latin1\'Ã©\'              (_latin1'é')       Ã©
//	DEFAULT ('café')     (latin1)    _latin1'636166E9' _latin1\'café\'            (_latin1'caf?')    café
//	DEFAULT ('<80>')     (latin1)    _latin1'80'       _latin1\'\u0080\'          (_latin1'?')       €  (latin1 is cp1252)
//	DEFAULT ('茅')       (gbk)       _gbk'C3A9'        _gbk\'Ã©\'                 (_gbk'é')          茅
//
// So neither surface is the value: it is the literal's BYTES read in the
// literal's CHARSET. Every literal measured carries an introducer (N'…' is
// stored as _utf8mb3, a hex literal as ASCII 0x…). The recovery:
//
//  1. refuses an expression holding a literal in any charset but utf8mb4,
//     utf8mb3, utf8, binary or latin1 — a multi-byte charset like gbk can
//     carry a trailing 0x5C that no byte-level scan can place, and sluice
//     transcodes none of them;
//  2. undoes the widening (every character must be ≤ U+00FF) and then the
//     escape layer ([decodeISExpressionEscapes]), which yields the stored
//     bytes in SHOW CREATE spelling;
//  3. decodes each literal by its introducer: utf8mb4/utf8mb3/utf8/binary
//     bytes must be UTF-8 and are kept; latin1 bytes are code points, except
//     0x80–0x9F, where MySQL's latin1 is cp1252 and the recovery refuses
//     rather than carry a cp1252 table; a literal with no introducer that
//     holds a non-ASCII byte refuses (never measured). Bytes outside
//     literals (backticked identifiers) must be UTF-8;
//  4. binds the result to SHOW CREATE TABLE ([showCreateCarries]): at one
//     of the object's clause anchors on its own line, the text outside
//     literals must be equal and each literal must be printed either as its
//     stored bytes or as its decoded value (see above). Introducers are not
//     compared: SHOW CREATE changes them where it prints the value, and has
//     been measured to change them on an ASCII literal too.
//
// Where SHOW CREATE prints stored bytes, the binding checks step 2 — that
// the widening undo returns the bytes the server stores — and not step 3,
// the charset reading: both surfaces derive from the same bytes. Where it
// prints the value, it checks step 3 as well. Step 3 otherwise rests on the
// introducer, which MySQL defines as the literal's charset and which
// information_schema prints; that is why every charset the recovery cannot
// decode exactly refuses.
//
// # MariaDB: faithful but for characters beyond the BMP
//
// MariaDB converts an expression to UTF-8 when the table is created, so its
// SHOW CREATE TABLE is faithful whatever the session charset (measured
// under latin1: 'Ã©' stored and printed as Ã©). Its information_schema is
// faithful for the BMP and writes one '?' per byte of a character beyond it.
// A catalog expression holding a '?' is replaced by the text at one of the
// object's clause anchors on its SHOW CREATE line that it matches, where
// each character beyond the BMP must appear in the catalog as exactly one
// '?' per UTF-8 byte and every other character as itself. No match, or two
// different matches, refuses.
//
// The trigger is honest for both: MySQL on any byte ≥ 0x80 (a widened
// expression never needs a '?'), MariaDB on any '?'.

// exprSiteKind names where an expression lives in SHOW CREATE TABLE output.
type exprSiteKind int

const (
	// exprSiteDefault is a column's DEFAULT expression.
	exprSiteDefault exprSiteKind = iota
	// exprSiteGenerated is a generated column's expression.
	exprSiteGenerated
	// exprSiteCheck is a CHECK constraint: its CONSTRAINT line, or on
	// MariaDB a column-level CHECK inline on the column's line.
	exprSiteCheck
	// exprSiteIndex is a functional key part: the KEY line.
	exprSiteIndex
)

// pendingExprText is one catalog expression that must be recovered before
// it is carried (see the file comment).
type pendingExprText struct {
	table string
	kind  exprSiteKind
	// name is the column, constraint or index name.
	name string
	// catalog is the raw information_schema text.
	catalog string
	// set receives the recovered text in the catalog's OWN spelling (the
	// MySQL escape layer kept), so each call site re-runs its usual
	// translation on it.
	set func(recovered string)
}

// exprTextNeedsRecovery reports whether a raw catalog expression must be
// recovered before it is carried: on MySQL any byte ≥ 0x80, on MariaDB any
// '?'. Pure, so the scan sites can decide per row.
func exprTextNeedsRecovery(flavor Flavor, catalog string) bool {
	if flavor == FlavorMariaDB {
		return strings.ContainsRune(catalog, '?')
	}
	for i := 0; i < len(catalog); i++ {
		if catalog[i] >= utf8.RuneSelf {
			return true
		}
	}
	return false
}

// exprDefaultCatalogText returns a COLUMN_DEFAULT that is an expression,
// not a literal: on MySQL one marked DEFAULT_GENERATED, on MariaDB (which
// has no such token) anything but a quoted literal or the bare NULL
// keyword. Literal defaults are text_default_recovery.go's.
func exprDefaultCatalogText(flavor Flavor, extra string, def sql.NullString) (string, bool) {
	if !def.Valid {
		return "", false
	}
	if flavor == FlavorMariaDB {
		if def.String == "NULL" || strings.HasPrefix(def.String, "'") {
			return "", false
		}
		return def.String, true
	}
	if !strings.Contains(strings.ToUpper(extra), "DEFAULT_GENERATED") {
		return "", false
	}
	return def.String, true
}

// appendColumnExprPending queues a scanned column's expression default and
// generation expression for recovery when they need it. The setters re-run
// the same translations the scan applied, on the recovered text. Shared by
// both information_schema.columns readers (the seed and the CDC boundary
// projection) so they cannot disagree about one catalog row.
func appendColumnExprPending(pending []pendingExprText, flavor Flavor, table string, col *ir.Column, def sql.NullString, extra, genExpr string) []pendingExprText {
	if text, ok := exprDefaultCatalogText(flavor, extra, def); ok && exprTextNeedsRecovery(flavor, text) {
		pending = append(pending, pendingExprText{
			table: table, kind: exprSiteDefault, name: col.Name, catalog: text,
			set: func(recovered string) {
				col.Default = flavor.translateColumnDefault(sql.NullString{String: recovered, Valid: true}, extra, col.Type)
			},
		})
	}
	if genExpr != "" && exprTextNeedsRecovery(flavor, genExpr) {
		pending = append(pending, pendingExprText{
			table: table, kind: exprSiteGenerated, name: col.Name, catalog: genExpr,
			set: func(recovered string) { applyGenerated(col, recovered, extra, flavor) },
		})
	}
	return pending
}

// recoverExprTexts recovers every pending expression, one SHOW CREATE
// TABLE per table, and hands each its recovered text. Any failure is an
// error: carrying the catalog text is the silent corruption this exists to
// prevent, and the read needs nothing the schema read does not.
//
// onGone, when non-nil, is called for a table dropped between the catalog
// read and its SHOW CREATE TABLE (error 1146) instead of failing: the
// caller drops what it read for that table. A nil onGone keeps the failure,
// which a single-table or snapshot read wants.
func recoverExprTexts(ctx context.Context, db *sql.DB, schema string, flavor Flavor, pending []pendingExprText, onGone func(table string)) error {
	byTable := map[string][]pendingExprText{}
	for _, p := range pending {
		byTable[p.table] = append(byTable[p.table], p)
	}
	for _, table := range sortedKeys(byTable) {
		sr := &SchemaReader{db: db, schema: schema, flavor: flavor}
		ddl, err := sr.showCreateTable(ctx, table)
		if err != nil {
			var me *mysqldrv.MySQLError
			if onGone != nil && errors.As(err, &me) && me.Number == 1146 {
				slog.WarnContext(ctx, "mysql: a table was dropped while its non-ASCII expression text was being recovered; "+
					"it is left out of this read", slog.String("table", schema+"."+table))
				onGone(table)
				continue
			}
			return fmt.Errorf("mysql: re-read %s.%s from SHOW CREATE TABLE to recover its non-ASCII expression text "+
				"(information_schema does not render it faithfully — Bug 288): %w", schema, table, err)
		}
		for _, p := range byTable[table] {
			got, err := recoverExprText(flavor, ddl, p)
			if err != nil {
				return fmt.Errorf("mysql: %s of %s.%s: %w", exprSiteLabel(p), schema, table, err)
			}
			p.set(got)
		}
	}
	return nil
}

// recoverExprText is [recoverExprTexts] for one expression against its
// table's SHOW CREATE TABLE text. Split out so it is unit-testable without a
// server.
func recoverExprText(flavor Flavor, ddl string, p pendingExprText) (string, error) {
	line, ok := exprSiteLine(ddl, p.kind, p.name)
	if !ok {
		return "", fmt.Errorf("SHOW CREATE TABLE has no line for it, so its information_schema text %q "+
			"cannot be checked (information_schema does not render non-ASCII expression text faithfully — Bug 288)", p.catalog)
	}
	anchors := exprAnchors(line, p.kind)
	if flavor == FlavorMariaDB {
		return recoverMariaDBExpr(line, anchors, p.catalog)
	}
	return recoverMySQLExpr(line, anchors, p.catalog)
}

// recoverMySQLExpr is the MySQL arm (see the file comment, steps 1–4).
func recoverMySQLExpr(line string, anchors []int, catalog string) (string, error) {
	spelled, err := mysqlISExprStoredBytes(catalog)
	if err != nil {
		return "", err
	}
	toks, err := tokenizeMySQLExpr(spelled)
	if err != nil {
		return "", fmt.Errorf("its information_schema text %q: %w (Bug 288)", catalog, err)
	}
	value, err := decodeMySQLExprTokens(toks)
	if err != nil {
		return "", fmt.Errorf("its information_schema text %q: %w (Bug 288)", catalog, err)
	}
	for _, a := range anchors {
		lineToks, err := tokenizeMySQLExpr(line[a:])
		if err == nil && showCreateCarries(lineToks, toks) {
			return escapeISExpressionLayer(value), nil
		}
	}
	return "", fmt.Errorf("its information_schema text decodes to %q, which its SHOW CREATE TABLE line %q does not carry "+
		"(as the stored bytes or as their value); refusing rather than carrying either (Bug 288)", value, line)
}

// mysqlISLiteralIntroducer matches a charset introducer in information_schema
// spelling (the escape layer turns the literal's opening quote into \').
var mysqlISLiteralIntroducer = regexp.MustCompile(`_([A-Za-z0-9]+)\\'`)

// mysqlExprLiteralCharsets are the literal charsets the MySQL arm decodes.
var mysqlExprLiteralCharsets = map[string]bool{
	"utf8mb4": true, "utf8mb3": true, "utf8": true, "binary": true, "latin1": true,
}

// mysqlISExprStoredBytes returns the stored bytes of a MySQL expression in
// SHOW CREATE spelling: steps 1 and 2 of the file comment.
func mysqlISExprStoredBytes(catalog string) (string, error) {
	for _, m := range mysqlISLiteralIntroducer.FindAllStringSubmatch(catalog, -1) {
		if !mysqlExprLiteralCharsets[strings.ToLower(m[1])] {
			return "", fmt.Errorf("its information_schema text %q holds a literal in character set %s; sluice reads non-ASCII "+
				"expression text only in utf8mb4, utf8mb3, binary and latin1 literals (Bug 288)", catalog, m[1])
		}
	}
	b := make([]byte, 0, len(catalog))
	for _, r := range catalog {
		if r > 0xFF {
			return "", fmt.Errorf("its information_schema text %q holds %q, which is not the one-character-per-byte "+
				"rendering MySQL's information_schema gives a stored expression (Bug 288)", catalog, r)
		}
		b = append(b, byte(r))
	}
	return decodeISExpressionEscapes(string(b)), nil
}

// escapeISExpressionLayer re-applies information_schema's escape layer
// (every `\` and `'` escaped once), the exact inverse of
// [decodeISExpressionEscapes], so the recovered text enters the call sites'
// usual translation in the spelling they expect.
func escapeISExpressionLayer(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `'`, `\'`)
}

// exprTok is one token of a MySQL expression in SHOW CREATE spelling: a run
// of text outside string literals (identifiers included), or one string
// literal with its charset introducer split off.
type exprTok struct {
	lit  bool
	text string // non-literal text, introducers removed
	// intro is the literal's introducer as spelled ("" if none), content its
	// bytes between the quotes, escapes kept.
	intro, content string
}

// tokenizeMySQLExpr splits s into [exprTok]s. Backticked identifiers (“
// doubled inside) are text; a literal's introducer is taken off the end of
// the text before it.
func tokenizeMySQLExpr(s string) ([]exprTok, error) {
	var toks []exprTok
	var text strings.Builder
	flush := func() {
		if text.Len() > 0 {
			toks = append(toks, exprTok{text: text.String()})
			text.Reset()
		}
	}
	for i := 0; i < len(s); {
		switch c := s[i]; c {
		case '`':
			j := i + 1
			for j < len(s) && (s[j] != '`' || (j+1 < len(s) && s[j+1] == '`')) {
				if s[j] == '`' {
					j++
				}
				j++
			}
			if j >= len(s) {
				return nil, errors.New("it holds an unterminated identifier")
			}
			text.WriteString(s[i : j+1])
			i = j + 1
		case '\'':
			_, n, ok := scanMySQLQuotedString(s[i:])
			if !ok {
				return nil, errors.New("it holds an unterminated string literal")
			}
			pre := text.String()
			intro := ""
			j := len(pre)
			for j > 0 && isIdentRune(rune(pre[j-1])) {
				j--
			}
			if j < len(pre) && pre[j] == '_' && (j == 0 || !isIdentRune(rune(pre[j-1]))) {
				intro = pre[j+1:]
				text.Reset()
				text.WriteString(pre[:j])
			}
			flush()
			toks = append(toks, exprTok{lit: true, intro: intro, content: s[i+1 : i+n-1]})
			i += n
		default:
			text.WriteByte(c)
			i++
		}
	}
	flush()
	return toks, nil
}

// decodeMySQLExprTokens is step 3 of the file comment: the expression's
// value in UTF-8, each literal decoded by its introducer.
func decodeMySQLExprTokens(toks []exprTok) (string, error) {
	var b strings.Builder
	for _, t := range toks {
		if !t.lit {
			if !utf8.ValidString(t.text) {
				return "", fmt.Errorf("it holds the bytes %q outside a string literal, which are not UTF-8", t.text)
			}
			b.WriteString(t.text)
			continue
		}
		v, err := decodeMySQLLiteralBytes(strings.ToLower(t.intro), t.content)
		if err != nil {
			return "", err
		}
		if t.intro != "" {
			b.WriteString("_" + t.intro)
		}
		b.WriteString("'" + v + "'")
	}
	return b.String(), nil
}

// showCreateCarries is step 4 of the file comment: the SHOW CREATE tokens
// from an anchor carry the stored expression. Text must be equal (the last
// token a prefix — the line goes on past the expression); each literal must
// be printed either as its stored bytes or as its value, which SHOW CREATE
// does under a different introducer (measured: a STORED generated column's
// `_latin1'<C3><A9>'` prints as `_utf8mb4'Ã©'`, a VIRTUAL one's as
// `_latin1'é'`). Introducers are therefore not compared; the value's charset
// comes from information_schema's.
func showCreateCarries(line, stored []exprTok) bool {
	if len(line) < len(stored) {
		return false
	}
	for i, s := range stored {
		l := line[i]
		if l.lit != s.lit {
			return false
		}
		if !s.lit {
			if l.text == s.text || (i == len(stored)-1 && strings.HasPrefix(l.text, s.text)) {
				continue
			}
			return false
		}
		if l.content == renderAsShowCreate(s.content) {
			continue
		}
		v, err := decodeMySQLLiteralBytes(strings.ToLower(s.intro), s.content)
		if err != nil || l.content != renderAsShowCreate(v) {
			return false
		}
	}
	return true
}

// decodeMySQLLiteralBytes decodes one literal's spelled bytes by its
// charset. Escape sequences are ASCII and pass through untouched; only the
// non-ASCII bytes are read.
func decodeMySQLLiteralBytes(charset, content string) (string, error) {
	ascii := true
	for i := 0; i < len(content); i++ {
		if content[i] >= utf8.RuneSelf {
			ascii = false
			break
		}
	}
	if ascii {
		return content, nil
	}
	switch charset {
	case "utf8mb4", "utf8mb3", "utf8", "binary":
		if !utf8.ValidString(content) {
			return "", fmt.Errorf("its _%s literal holds the bytes %q, which are not UTF-8", charset, content)
		}
		return content, nil
	case "latin1":
		var b strings.Builder
		for i := 0; i < len(content); i++ {
			c := content[i]
			if c >= 0x80 && c <= 0x9F {
				return "", fmt.Errorf("its _latin1 literal holds the byte 0x%02X, which MySQL's latin1 (cp1252) maps outside "+
					"Latin-1; sluice does not carry a cp1252 table", c)
			}
			b.WriteRune(rune(c))
		}
		return b.String(), nil
	case "":
		return "", fmt.Errorf("it holds a non-ASCII string literal %q with no charset introducer", content)
	default:
		return "", fmt.Errorf("it holds a non-ASCII _%s literal, and sluice decodes only utf8mb4, utf8mb3, binary and latin1", charset)
	}
}

// renderAsShowCreate is how SHOW CREATE TABLE prints stored bytes: UTF-8
// sequences of up to three bytes as themselves, and one '?' for each byte of
// a four-byte sequence or of anything that is not UTF-8 (measured).
func renderAsShowCreate(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		r, n := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && n <= 1:
			b.WriteByte('?')
			i++
		case n == 4:
			b.WriteString("????")
			i += n
		default:
			b.WriteString(s[i : i+n])
			i += n
		}
	}
	return b.String()
}

// recoverMariaDBExpr is the MariaDB arm (see the file comment).
func recoverMariaDBExpr(line string, anchors []int, catalog string) (string, error) {
	want := []rune(catalog)
	distinct := map[string]bool{}
	for _, a := range anchors {
		have := []rune(line[a:])
		if n, ok := mariaDBQuestionMarkMatch(have, want); ok {
			distinct[string(have[:n])] = true
		}
	}
	cands := make([]string, 0, len(distinct))
	for c := range distinct {
		cands = append(cands, c)
	}
	sort.Strings(cands)
	switch len(cands) {
	case 1:
		return cands[0], nil
	case 0:
		return "", fmt.Errorf("its information_schema text %q (where each character outside the Basic Multilingual "+
			"Plane is written as one '?' per byte) matches nothing at its clause on its SHOW CREATE TABLE line %q (Bug 288)", catalog, line)
	default:
		return "", fmt.Errorf("its information_schema text %q matches %d different texts on its SHOW CREATE TABLE "+
			"line %q, so sluice cannot tell which is the expression (Bug 288)", catalog, len(cands), line)
	}
}

// mariaDBQuestionMarkMatch reports how many runes of have the lossy want
// spells, reading from have's start: every character as itself, except that
// a character beyond the BMP must be exactly one '?' per UTF-8 byte in want.
func mariaDBQuestionMarkMatch(have, want []rune) (int, bool) {
	i, j := 0, 0
	for j < len(want) {
		if i >= len(have) {
			return 0, false
		}
		if have[i] > 0xFFFF {
			n := utf8.RuneLen(have[i])
			for k := 0; k < n; k++ {
				if j+k >= len(want) || want[j+k] != '?' {
					return 0, false
				}
			}
			i, j = i+1, j+n
			continue
		}
		if have[i] != want[j] {
			return 0, false
		}
		i, j = i+1, j+1
	}
	return i, true
}

func exprSiteLabel(p pendingExprText) string {
	switch p.kind {
	case exprSiteCheck:
		return fmt.Sprintf("CHECK constraint %q", p.name)
	case exprSiteIndex:
		return fmt.Sprintf("functional key part of index %q", p.name)
	case exprSiteGenerated:
		return fmt.Sprintf("generated expression of column %q", p.name)
	default:
		return fmt.Sprintf("DEFAULT of column %q", p.name)
	}
}

// exprSiteLine returns the SHOW CREATE TABLE line that defines the object:
// a column's definition line, a CONSTRAINT … CHECK line (falling back to
// the column line, where MariaDB prints a column-level CHECK), or an index
// line. Each definition is on its own line in SHOW CREATE output; string
// literals carry newlines escaped.
func exprSiteLine(ddl string, kind exprSiteKind, name string) (string, bool) {
	quoted := "`" + mysqlQuoteIdent(name) + "`"
	lines := strings.Split(ddl, "\n")
	find := func(match func(trimmed string) bool) (string, bool) {
		for _, l := range lines {
			if t := strings.TrimLeft(l, " \t"); match(t) {
				return t, true
			}
		}
		return "", false
	}
	columnLine := func(t string) bool { return strings.HasPrefix(t, quoted+" ") }
	switch kind {
	case exprSiteCheck:
		if l, ok := find(func(t string) bool { return strings.HasPrefix(t, "CONSTRAINT "+quoted+" CHECK ") }); ok {
			return l, true
		}
		return find(columnLine)
	case exprSiteIndex:
		return find(func(t string) bool {
			for _, kw := range []string{"KEY ", "UNIQUE KEY ", "FULLTEXT KEY ", "SPATIAL KEY "} {
				if strings.HasPrefix(t, kw+quoted+" ") {
					return true
				}
			}
			return false
		})
	default:
		return find(columnLine)
	}
}

// exprAnchors returns the byte offsets on the object's line at which its
// expression may begin: after `DEFAULT (` (MySQL) or `DEFAULT ` (MariaDB),
// after `AS (`, after `CHECK (`, or for a functional key part after any `(`
// (a multi-part key lists `((a),(b))`). Every occurrence is a candidate; the
// arms decide which one the catalog text matches.
func exprAnchors(line string, kind exprSiteKind) []int {
	var keys []string
	switch kind {
	case exprSiteDefault:
		keys = []string{" DEFAULT (", " DEFAULT "}
	case exprSiteGenerated:
		keys = []string{" AS ("}
	case exprSiteCheck:
		keys = []string{"CHECK ("}
	default:
		keys = []string{"("}
	}
	seen := map[int]bool{}
	var out []int
	for _, k := range keys {
		for from := 0; ; {
			idx := strings.Index(line[from:], k)
			if idx < 0 {
				break
			}
			at := from + idx + len(k)
			if !seen[at] {
				seen[at] = true
				out = append(out, at)
			}
			from = from + idx + 1
		}
	}
	sort.Ints(out)
	return out
}

// decodeMySQLISExprForCompare returns a MySQL catalog expression's value
// with the widening undone and each literal decoded by its charset (steps
// 1–3), for a comparison that has no SHOW CREATE line to bind to — the
// Shape A CHECK probe. ok=false leaves the caller comparing the text as
// read, which compares unequal to a recovered expression: loud, never
// carried.
func decodeMySQLISExprForCompare(catalog string) (string, bool) {
	spelled, err := mysqlISExprStoredBytes(catalog)
	if err != nil {
		return "", false
	}
	toks, err := tokenizeMySQLExpr(spelled)
	if err != nil {
		return "", false
	}
	value, err := decodeMySQLExprTokens(toks)
	if err != nil {
		return "", false
	}
	return escapeISExpressionLayer(value), true
}
