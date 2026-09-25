// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"sluicesync.dev/sluice/internal/ir"
)

// Catalog expression-text recovery (Bug 288 / GC-37 (a)) — the expression
// sibling of the literal-default recovery in text_default_recovery.go.
//
// information_schema renders four kinds of stored expression — an
// expression DEFAULT (COLUMN_DEFAULT with DEFAULT_GENERATED), a generated
// column (GENERATION_EXPRESSION), a CHECK (CHECK_CLAUSE) and a functional
// key part (STATISTICS.EXPRESSION) — and neither flavor renders a non-ASCII
// character in them faithfully. The two flavors fail in OPPOSITE places,
// measured 2026-09-24 on MySQL 8.0.46 / 8.4.x and MariaDB 11.4:
//
//	declared (utf8mb4 session)        MySQL information_schema     MySQL SHOW CREATE    MariaDB I_S   MariaDB SHOW CREATE
//	DEFAULT ('é')                     _utf8mb4\'Ã©\'   (C383C2A9)   (_utf8mb4'é')        'é'           'é'
//	DEFAULT (concat('ß','中'))        …\'Ã\x9f\'…\'ä¸­\'…           faithful             faithful      faithful
//	DEFAULT ('😀x')                   _utf8mb4\'ð\x9f\x98\x80x\'   (_utf8mb4'????x')    '????x'       '😀x'
//	DEFAULT ('?')                     _utf8mb4\'?\'                (_utf8mb4'?')        '?'           '?'
//
// MySQL's information_schema text is the stored expression's BYTES read as
// ISO-8859-1 and re-encoded as UTF-8 — every byte ≥ 0x80 becomes the
// two-byte UTF-8 of U+0080..U+00FF (0x9F becomes U+009F, not cp1252's Ÿ, so
// it is Latin-1 proper). That is lossless, so undoing it returns the stored
// bytes exactly, 4-byte characters included; a table created from a latin1
// session stores `_latin1'<E9>'`, and the undo returns that byte, which is
// not UTF-8 and is refused rather than carried. The same double encoding is
// on GENERATION_EXPRESSION, CHECK_CLAUSE and STATISTICS.EXPRESSION
// (measured). SHOW CREATE TABLE prints the same expression faithfully for
// the Basic Multilingual Plane and as one '?' per BYTE for anything beyond.
//
// MariaDB is the reverse: information_schema is faithful for the BMP and
// writes one '?' per byte for a character beyond it, and SHOW CREATE TABLE
// is faithful throughout.
//
// So the recovery reads the faithful surface for each flavor and binds it
// to the other, which is its independent evidence:
//
//   - MySQL (every non-MariaDB flavor, vtgate included): a catalog
//     expression holding any byte ≥ 0x80 is un-double-encoded, and the
//     result — decoded out of information_schema's escape layer into the
//     SHOW CREATE spelling ([decodeISExpressionEscapes]) — must appear on
//     that object's own SHOW CREATE TABLE line, character for character
//     except that a character beyond the BMP may appear there as 1 to 4
//     '?'. A result that is not UTF-8, or that SHOW CREATE does not carry,
//     is refused, naming the object.
//   - MariaDB: a catalog expression holding a '?' is replaced by the
//     SHOW CREATE text on that object's line that it matches, where each
//     run of 1 to 4 '?' in the catalog may stand for one character beyond
//     the BMP. No match, or two different matches, is refused.
//
// The trigger is honest for both: a double-encoded MySQL expression always
// carries a byte ≥ 0x80 (and never needs a '?'), and a MariaDB one loses
// characters only to '?'. A genuine '?' on MariaDB costs one SHOW CREATE per
// table and matches itself.

// exprSiteKind names where an expression lives in SHOW CREATE TABLE output.
type exprSiteKind int

const (
	// exprSiteColumn is a column's DEFAULT expression or generation
	// expression: the column's own definition line.
	exprSiteColumn exprSiteKind = iota
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
			table: table, kind: exprSiteColumn, name: col.Name, catalog: text,
			set: func(recovered string) {
				col.Default = flavor.translateColumnDefault(sql.NullString{String: recovered, Valid: true}, extra, col.Type)
			},
		})
	}
	if genExpr != "" && exprTextNeedsRecovery(flavor, genExpr) {
		pending = append(pending, pendingExprText{
			table: table, kind: exprSiteColumn, name: col.Name, catalog: genExpr,
			set: func(recovered string) { applyGenerated(col, recovered, extra, flavor) },
		})
	}
	return pending
}

// undoISLatin1 reverses MySQL information_schema's double encoding of a
// stored expression: every character of catalog must be ≤ U+00FF (it is the
// Latin-1 reading of a byte), and the bytes must form UTF-8. ok=false means
// the premise did not hold for this text, which the caller refuses.
func undoISLatin1(catalog string) (string, bool) {
	b := make([]byte, 0, len(catalog))
	for _, r := range catalog {
		if r > 0xFF {
			return "", false
		}
		b = append(b, byte(r))
	}
	if !utf8.Valid(b) {
		return "", false
	}
	return string(b), true
}

// recoverExprTexts recovers every pending expression, one SHOW CREATE
// TABLE per table, and hands each its recovered text. Any failure is an
// error: carrying the catalog text is the silent corruption this exists to
// prevent, and the read needs nothing the schema read does not.
func recoverExprTexts(ctx context.Context, db *sql.DB, schema string, flavor Flavor, pending []pendingExprText) error {
	byTable := map[string][]pendingExprText{}
	for _, p := range pending {
		byTable[p.table] = append(byTable[p.table], p)
	}
	for _, table := range sortedKeys(byTable) {
		sr := &SchemaReader{db: db, schema: schema, flavor: flavor}
		ddl, err := sr.showCreateTable(ctx, table)
		if err != nil {
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
	if flavor != FlavorMariaDB {
		undone, ok := undoISLatin1(p.catalog)
		if !ok {
			return "", fmt.Errorf("its information_schema text %q is not the Latin-1 re-encoding of UTF-8 that MySQL "+
				"stores for a utf8mb4 expression (an expression written under another charset introducer, or a server "+
				"that renders it differently), so sluice cannot read it faithfully (Bug 288)", p.catalog)
		}
		if !lostCharContains([]rune(line), []rune(decodeISExpressionEscapes(undone))) {
			return "", fmt.Errorf("its information_schema text decodes to %q, which SHOW CREATE TABLE does not carry "+
				"(%q); refusing rather than carrying either (Bug 288)", decodeISExpressionEscapes(undone), line)
		}
		return undone, nil
	}
	cands := lostCharCandidates([]rune(line), []rune(p.catalog))
	switch len(cands) {
	case 1:
		return cands[0], nil
	case 0:
		return "", fmt.Errorf("its information_schema text %q (where '?' may stand for a character outside the Basic "+
			"Multilingual Plane) matches nothing on its SHOW CREATE TABLE line %q (Bug 288)", p.catalog, line)
	default:
		return "", fmt.Errorf("its information_schema text %q matches %d different texts on its SHOW CREATE TABLE "+
			"line %q, so sluice cannot tell which is the expression (Bug 288)", p.catalog, len(cands), line)
	}
}

func exprSiteLabel(p pendingExprText) string {
	switch p.kind {
	case exprSiteCheck:
		return fmt.Sprintf("CHECK constraint %q", p.name)
	case exprSiteIndex:
		return fmt.Sprintf("functional key part of index %q", p.name)
	default:
		return fmt.Sprintf("expression on column %q", p.name)
	}
}

// exprSiteLine returns the SHOW CREATE TABLE line that defines the object:
// a column's definition line, a CONSTRAINT … CHECK line (falling back to
// the column line, where MariaDB prints a column-level CHECK), or a KEY
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
		return find(func(t string) bool { return strings.Contains(t, "KEY "+quoted+" ") })
	default:
		return find(columnLine)
	}
}

// maxLostCharQuestionMarks is the most '?' either flavor writes for one
// character beyond the BMP: one per UTF-8 byte.
const maxLostCharQuestionMarks = 4

// lostCharPrefixEnds returns every offset e such that want matches
// have[start:e] exactly, where the two are renderings of one text and
// ONE side is lossy: each character beyond the BMP on the faithful side may
// appear on the lossy side as a run of 1 to [maxLostCharQuestionMarks]
// '?'. wantLossy says which side that is. Memoized; the inputs are one
// SHOW CREATE line and one expression.
func lostCharPrefixEnds(have, want []rune, start int, wantLossy bool) []int {
	type key struct{ i, j int }
	seen := map[key]bool{}
	ends := map[int]bool{}
	var walk func(i, j int)
	walk = func(i, j int) {
		k := key{i, j}
		if seen[k] {
			return
		}
		seen[k] = true
		if j == len(want) {
			ends[i] = true
			return
		}
		if wantLossy {
			if i < len(have) && have[i] > 0xFFFF {
				for n := 1; n <= maxLostCharQuestionMarks && j+n <= len(want) && want[j+n-1] == '?'; n++ {
					walk(i+1, j+n)
				}
				return
			}
		} else if want[j] > 0xFFFF {
			for n := 1; n <= maxLostCharQuestionMarks && i+n <= len(have) && have[i+n-1] == '?'; n++ {
				walk(i+n, j+1)
			}
			return
		}
		if i < len(have) && have[i] == want[j] {
			walk(i+1, j+1)
		}
	}
	walk(start, 0)
	out := make([]int, 0, len(ends))
	for e := range ends {
		out = append(out, e)
	}
	sort.Ints(out)
	return out
}

// lostCharContains reports whether the faithful text want appears somewhere
// in the lossy text have (MySQL: have is the SHOW CREATE line).
func lostCharContains(have, want []rune) bool {
	for s := 0; s < len(have); s++ {
		if len(lostCharPrefixEnds(have, want, s, false)) > 0 {
			return true
		}
	}
	return false
}

// lostCharCandidates returns the distinct substrings of the faithful text
// have that the lossy text want matches (MariaDB: have is the SHOW CREATE
// line, want the information_schema text).
func lostCharCandidates(have, want []rune) []string {
	distinct := map[string]bool{}
	for s := 0; s < len(have); s++ {
		for _, e := range lostCharPrefixEnds(have, want, s, true) {
			distinct[string(have[s:e])] = true
		}
	}
	out := make([]string, 0, len(distinct))
	for c := range distinct {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}
