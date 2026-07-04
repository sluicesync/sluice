// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

// SQLite → canonical (Postgres / MySQL) EXPRESSION translator (ADR-0133
// follow-up; roadmap item 49 "SQLite→canonical expression translator").
//
// ADR-0133's schema-feature carry lands SQLite generated-column bodies,
// CHECK constraints, and partial/expression-index bodies in the IR tagged
// dialect "sqlite", emitted VERBATIM by the PG/MySQL writers — so a
// non-portable SQLite construct is rejected loudly at target DDL time. This
// package translates the PROVABLY-PORTABLE subset of those bodies to the
// target's canonical spelling, so the common cases (a `a || '-' || b`
// generated column, a `length(x) > 0` CHECK, a `substr(x,1,3)` index) land
// working on the target instead of failing.
//
// # Contract
//
// [SQLiteExprToPG] / [SQLiteExprToMySQL] parse a SQLite expression body and
// return (translated, ok). ok is false when the body contains ANYTHING
// outside the conservative allowlist below — the caller then keeps the
// existing loud-fail / WARN-skip behaviour (never a silent guess).
//
// # Allowlist (only PROVABLY-equivalent constructs — value-fidelity-critical)
//
// The allowlist is deliberately narrow: a construct is admitted only if its
// SQLite semantics are provably identical to the target's for EVERY operand
// shape, not just the representative case (the Bug-74 lesson). A shrink pass
// (value-fidelity review) removed several constructs that a per-representative
// test would have passed but that silently diverge on some operand.
//
//   - column references (bare, backtick- and bracket-quoted idents) and
//     numeric/string literals (string literals containing a backslash and
//     ALL double-quoted tokens refuse on MySQL — see below)
//   - operators: + - *  = <> != < <= > >=  AND OR NOT  IS [NOT] NULL,
//     string concat || (PG keeps ||; MySQL becomes CONCAT(...)), and `/` on
//     Postgres ONLY (see below)
//   - functions: abs, coalesce, ifnull(→coalesce), nullif,
//     length (PG length / MySQL CHAR_LENGTH), trim/ltrim/rtrim (1-arg),
//     substr/substring (literal start ≥ 1, literal len ≥ 0 — see resolveName),
//     min/max (scalar ≥2-arg → LEAST/GREATEST, MySQL ONLY — see resolveName),
//     cast(x AS text|real) on both and cast(x AS numeric) on PG only (see
//     sqliteCastType), and the current-instant keywords CURRENT_TIMESTAMP /
//     CURRENT_DATE / CURRENT_TIME.
//
// Excluded (return ok=false), each for a proven silent-divergence:
//
//   - `%` on BOTH targets: SQLite coerces operands to INTEGER first, PG/MySQL
//     do not (7.5 % 2 → 1 vs 1.5).
//   - `/` on MySQL: SQLite/PG do integer division for integer operands, MySQL
//     `/` is always decimal. (PG keeps it — it matches SQLite, incl. negatives.)
//   - upper/lower: SQLite folds ASCII only; PG/MySQL fold Unicode.
//   - round: SQLite is half-away-from-zero; PG/MySQL round-half-to-even on
//     floats and the operand can't be proven decimal-not-float.
//   - cast AS integer (truncate vs round), cast AS blob (byte semantics),
//     cast AS numeric on MySQL (bare DECIMAL rounds to 0 decimals).
//   - a string literal containing a backslash, on MySQL ONLY (SEC-1): SQLite
//     treats a backslash inside a '…' literal as an ordinary character; MySQL
//     under its default sql_mode treats it as an escape introducer, and the
//     expression lands in the TARGET SCHEMA where future sessions — whose
//     sql_mode sluice does not control — re-parse it. No re-escaping can be
//     faithful under a mode sluice can't pin, so refuse (see stringNode). PG
//     stays permissive: under standard_conforming_strings=on (server default
//     since 9.1, pinned on every sluice PG session) backslash is literal,
//     matching SQLite, and PG stores the parsed tree, not the text.
//   - any "…" double-quoted token, on MySQL ONLY (SEC-1 review gap 1):
//     SQLite reads it as an identifier (or, via the double-quoted-string
//     misfeature, a string); MySQL under its default sql_mode (no
//     ANSI_QUOTES) reads it as a STRING LITERAL with backslash-escape
//     semantics — silently wrong regardless of content (see identNode). PG
//     reads "…" as an identifier, matching SQLite's primary meaning, and an
//     unknown column fails loudly (42703) — permissive there.
//   - a 0x… hex integer literal, on MySQL ONLY: SQLite reads an INTEGER;
//     MySQL reads a hexadecimal literal whose value is context-dependent — a
//     BINARY STRING in string context (bytes \x1a where SQLite stores '26')
//     — and the context can't be proven from the body. PG keeps it: PG 16+
//     parses 0x as an integer exactly like SQLite, and older servers reject
//     the spelling loudly at DDL time (see verbatimNode).
//   - substr with a non-literal or < 1 start (negative counts from the end in
//     SQLite, not in SUBSTRING); min/max on PG (LEAST/GREATEST skip NULLs, but
//     SQLite scalar min/max propagate NULL).
//   - strftime/julianday/unixepoch/date/time/datetime-with-args (format-string
//     + epoch-base translation is out of scope — a wrong strftime format
//     silently corrupts a STORED gencol), glob/typeof/hex/unicode/char/
//     randomblob/likelihood/printf/format, the double-quoted-string misfeature,
//     and any token or function not named above.
//
// # Fully-parenthesised emit
//
// The body is parsed to a small AST using SQLite's operator precedence, then
// re-emitted fully parenthesised so the target's precedence rules cannot
// change the meaning. Emit is target-aware (the `/`, min/max, and cast cases
// above differ by target).
//
// # Tokenizer note (import-cycle)
//
// A minimal SQLite-aware tokenizer is duplicated here rather than imported
// from internal/engines/sqlite/sql_features.go: the engine packages depend on
// internal/translate (for translateExprForPG/MySQL and the affinity/notice
// helpers), so translate importing an engine package would invert the
// dependency direction (and, with the sqlite engine, risk a cycle). The
// tokenizer is small, string/quoted-ident/comment aware, and additionally
// recognises numeric literals (which sql_features.go's body-slicing tokenizer
// does not need to) so `3.14` parses as one token.

import (
	"strconv"
	"strings"
)

// SQLiteExprToPG translates a SQLite expression body to its Postgres
// canonical form. ok is false for anything outside the portability allowlist.
func SQLiteExprToPG(expr string) (string, bool) { return translateSQLiteExpr(expr, sqPG) }

// SQLiteExprToMySQL translates a SQLite expression body to its MySQL
// canonical form. ok is false for anything outside the portability allowlist
// (including the `/` and `%` operators, whose MySQL semantics diverge from
// SQLite's).
func SQLiteExprToMySQL(expr string) (string, bool) { return translateSQLiteExpr(expr, sqMySQL) }

// SQLiteExprHasBackslashStringLiteral reports whether any '…' string literal
// in the SQLite expression body contains a backslash — the one construct that
// is valid on MySQL but silently CHANGES MEANING there (see stringNode). The
// target writers' refusal boundaries use it to NAME the backslash wart
// instead of the generic non-portable message, and the MySQL DEFAULT
// verbatim-carry path uses it directly (SQLite-dialect DEFAULT bodies never
// route through the translator at all). Token boundaries are computed under
// SQLite's lexing rules (doubled-quote escaping, backslash NOT an escape) —
// the same rules the source used to parse the text.
func SQLiteExprHasBackslashStringLiteral(expr string) bool {
	for _, tok := range tokenizeSQLiteExpr(expr) {
		if tok.kind == sqString && strings.Contains(tok.text, `\`) {
			return true
		}
	}
	return false
}

// SQLiteExprHasDoubleQuotedToken reports whether the SQLite expression body
// contains a "…" double-quoted token — an identifier under SQLite's rules (or
// a string via the double-quoted-string misfeature), but a STRING LITERAL
// with backslash-escape semantics under MySQL's default sql_mode (no
// ANSI_QUOTES), so it silently changes meaning there regardless of content
// (see identNode). Sibling of [SQLiteExprHasBackslashStringLiteral]; the
// MySQL writer's refusal boundary uses it to name this wart.
func SQLiteExprHasDoubleQuotedToken(expr string) bool {
	for _, tok := range tokenizeSQLiteExpr(expr) {
		if tok.kind == sqIdent && strings.HasPrefix(tok.text, `"`) {
			return true
		}
	}
	return false
}

// SQLiteExprHasHexLiteral reports whether the SQLite expression body
// contains a 0x… hex integer literal. SQLite reads it as an INTEGER; MySQL
// reads a hexadecimal literal as a context-dependent BINARY STRING (the
// translator refuses it there — see verbatimNode), and PG only gained 0x
// integer literals in PG 16 (older servers reject the spelling at DDL
// time). The PG writer's DEFAULT arm uses this to keep hex defaults on its
// loud warn-drop path instead of emitting a version-dependent spelling.
func SQLiteExprHasHexLiteral(expr string) bool {
	for _, tok := range tokenizeSQLiteExpr(expr) {
		if tok.kind == sqWord && isHex0xWord(tok.text) {
			return true
		}
	}
	return false
}

// SQLiteExprMySQLDefaultVerbatimSafe reports whether a "sqlite"-dialect
// DEFAULT body that the SQLite→MySQL translator REFUSED can still be
// carried verbatim to MySQL without risking a silently divergent VALUE.
// MySQL parses much of what the translator refuses — `||` as logical OR,
// `/` as decimal division, `%` as float modulo, upper/round with different
// semantics inside accepted syntax — so "the parser rejects a non-portable
// spelling loudly" does NOT hold in general; the MySQL writer warn-drops
// anything outside this allowlist (PG-arm parity). The safe residues, each
// probed:
//
//   - a single "…" double-quoted token with no backslash (`"draft"` — the
//     bare misfeature form SQLite accepts in DEFAULT position; MySQL reads
//     the same string value)
//   - a x'…' blob literal (MySQL's hex-string literal, same bytes)
//   - a single bare word that is not a 0x… hex literal (an unknown
//     keyword: MySQL rejects it loudly in DEFAULT position, never
//     silently)
//
// Backslash-bearing shapes are also excluded here as belt-and-braces,
// though the SEC-1 refusal boundary (the MySQL writer's
// refuseBackslashSQLiteDefaultMySQL) aborts those before any emit
// decision is reached.
func SQLiteExprMySQLDefaultVerbatimSafe(expr string) bool {
	toks := tokenizeSQLiteExpr(expr)
	switch len(toks) {
	case 1:
		t := toks[0]
		switch t.kind {
		case sqIdent:
			return strings.HasPrefix(t.text, `"`) && !strings.Contains(t.text, `\`)
		case sqWord:
			return !isHex0xWord(t.text)
		}
		return false
	case 2:
		// x'…' blob literal (the tokenizer splits it as word + string).
		return toks[0].kind == sqWord && strings.EqualFold(toks[0].text, "x") &&
			toks[1].kind == sqString && !strings.Contains(toks[1].text, `\`)
	}
	return false
}

// sqTarget selects the emit dialect.
type sqTarget int

const (
	sqPG sqTarget = iota
	sqMySQL
)

func translateSQLiteExpr(expr string, t sqTarget) (string, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", false
	}
	toks := tokenizeSQLiteExpr(expr)
	if len(toks) == 0 {
		return "", false
	}
	p := &sqParser{toks: toks}
	node, ok := p.parseExpr()
	if !ok || !p.atEnd() {
		// Anything the grammar didn't consume — an unknown function, a
		// non-allowlisted operator, a trailing token — means the body is
		// outside the provable subset. Loud-fail (ok=false).
		return "", false
	}
	return node.emit(t)
}

// ---- AST ----

// sqNode is one node of the parsed SQLite expression. emit renders the
// target's canonical form; it returns ok=false when the (already-parsed)
// construct has no faithful spelling on that specific target (e.g. `/` on
// MySQL).
type sqNode interface {
	emit(t sqTarget) (string, bool)
}

// verbatimNode carries text that is identical on both targets: a bare column
// reference, a plain numeric literal, NULL, or a current-instant keyword.
// String literals and quoted identifiers are NOT verbatim — see stringNode
// and identNode. One carve-out (value-fidelity review of the DEFAULT
// classification fix): a 0x… HEX literal is an integer on SQLite, but on
// MySQL a hexadecimal literal is a CONTEXT-DEPENDENT value — a BINARY
// STRING in string context (a varchar position gets bytes \x1a where SQLite
// stores '26') and a number only in numeric context. The context can't be
// proven from the expression body, so hex refuses on MySQL. PG keeps it:
// PG's 0x literal (PG 16+) is an integer exactly like SQLite's, and an
// older server rejects it LOUDLY at DDL time rather than diverging.
type verbatimNode struct{ text string }

func (n verbatimNode) emit(t sqTarget) (string, bool) {
	if t == sqMySQL && isHex0xWord(n.text) {
		return "", false
	}
	return n.text, true
}

// isHex0xWord reports whether a word token is a 0x/0X hex integer literal.
// The tokenizer only produces digit-led words for numbers, so the prefix
// test cannot collide with a column reference (identifiers can't start
// with a digit).
func isHex0xWord(s string) bool {
	return len(s) > 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')
}

// stringNode carries a '…' single-quoted string literal (delimiters included,
// interior quotes doubled — SQLite's own spelling, which is also valid on both
// targets). It is not a verbatimNode because the literal's MEANING is
// target-dependent (SEC-1): SQLite treats a backslash inside a string literal
// as an ordinary character, but MySQL's default sql_mode treats it as an
// escape introducer — and the translated body lands in the target SCHEMA (a
// generated column / CHECK / index body), where it is re-parsed under FUTURE
// sessions' sql_mode, which sluice does not control. So a backslash-bearing
// literal has no MySQL spelling sluice can prove faithful: 'C:\temp' silently
// becomes "C:<TAB>emp", and a literal ending in \ swallows its closing quote,
// shifting the following expression text into string position — the
// expression-content-escaping-its-DDL-position class SECURITY.md keeps in
// scope. Refuse on MySQL; the writers' refusal boundaries name the reason via
// [SQLiteExprHasBackslashStringLiteral].
//
// Postgres is asymmetric ON PURPOSE: under standard_conforming_strings=on
// (the server default since PG 9.1, and pinned on every sluice PG session —
// see the postgres engine's connect.go) a '…' literal treats backslash
// literally, exactly like SQLite, and PG stores the parsed expression tree
// (pg_node_tree) rather than the raw text, so only sluice's own DDL session's
// setting matters. Backslash literals therefore stay portable to PG.
type stringNode struct{ text string }

func (n stringNode) emit(t sqTarget) (string, bool) {
	if t == sqMySQL && strings.Contains(n.text, `\`) {
		return "", false
	}
	return n.text, true
}

// identNode carries a quoted-identifier token, delimiters included. Backtick
// and bracket forms emit verbatim: backtick is MySQL's own identifier quoting
// (and a loud parse error on PG); bracket is a loud parse error on both. A
// DOUBLE-quoted token refuses on MySQL (SEC-1 review gap 1) REGARDLESS of
// content: under MySQL's default sql_mode (no ANSI_QUOTES — sluice's pinned
// session mode does not enable it, and the body is re-parsed under future
// sessions' modes sluice does not control) `"…"` lexes as a STRING LITERAL
// with backslash-escape semantics, not an identifier. So a surviving "…"
// token silently changes meaning either way: an intended identifier becomes a
// string (vacating e.g. a CHECK), SQLite's double-quoted-string misfeature
// becomes an escape-decoded string, and a trailing backslash swallows the
// closing quote (the DDL-position escape). Note the SQLite reader de-quotes
// BARE quoted identifiers at the read boundary (stripSQLiteIdentQuotes), so
// any "…" that reaches this translator is by construction the non-bare /
// misfeature residue — there is no MySQL-portable use to preserve.
//
// PG keeps "…" verbatim: it is a well-formed PG identifier there, matching
// SQLite's primary meaning, and an unknown column fails loudly (42703) —
// never silently.
type identNode struct{ text string }

func (n identNode) emit(t sqTarget) (string, bool) {
	if t == sqMySQL && strings.HasPrefix(n.text, `"`) {
		return "", false
	}
	return n.text, true
}

// binaryNode is an arithmetic / comparison / logical binary operator. op is
// already normalised (== → =, != → <>).
type binaryNode struct {
	op   string
	l, r sqNode
}

func (n binaryNode) emit(t sqTarget) (string, bool) {
	switch n.op {
	case "%":
		// SQLite `%` first coerces BOTH operands to INTEGER; neither PG nor
		// MySQL does (7.5 % 2 → 1 in SQLite, 1.5 in PG/MySQL). Not
		// provably-equivalent on EITHER target — refuse.
		return "", false
	case "/":
		// SQLite integer/integer is integer division (truncate toward zero);
		// PG matches (incl. negatives), MySQL `/` is always decimal division.
		// Keep for PG, refuse for MySQL.
		if t == sqMySQL {
			return "", false
		}
	}
	ls, ok := n.l.emit(t)
	if !ok {
		return "", false
	}
	rs, ok := n.r.emit(t)
	if !ok {
		return "", false
	}
	return "(" + ls + " " + n.op + " " + rs + ")", true
}

// unaryNode is a prefix NOT / unary +/-.
type unaryNode struct {
	op string
	x  sqNode
}

func (n unaryNode) emit(t sqTarget) (string, bool) {
	xs, ok := n.x.emit(t)
	if !ok {
		return "", false
	}
	if n.op == "NOT" {
		return "(NOT " + xs + ")", true
	}
	return "(" + n.op + xs + ")", true
}

// isNullNode is a postfix `IS [NOT] NULL`.
type isNullNode struct {
	x       sqNode
	negated bool
}

func (n isNullNode) emit(t sqTarget) (string, bool) {
	xs, ok := n.x.emit(t)
	if !ok {
		return "", false
	}
	if n.negated {
		return "(" + xs + " IS NOT NULL)", true
	}
	return "(" + xs + " IS NULL)", true
}

// concatNode is a `||` string-concatenation chain. PG keeps the `||` operator
// (IMMUTABLE, NULL-propagating — matching SQLite); MySQL has no `||`
// concatenation operator by default, so it becomes CONCAT(...), which shares
// SQLite's any-NULL-arg → NULL semantics.
type concatNode struct{ parts []sqNode }

func (n concatNode) emit(t sqTarget) (string, bool) {
	parts := make([]string, len(n.parts))
	for i, p := range n.parts {
		s, ok := p.emit(t)
		if !ok {
			return "", false
		}
		parts[i] = s
	}
	if t == sqMySQL {
		return "CONCAT(" + strings.Join(parts, ", ") + ")", true
	}
	return "(" + strings.Join(parts, " || ") + ")", true
}

// funcNode is an allowlisted scalar function call. name is lower-cased and its
// arity was validated at parse time.
type funcNode struct {
	name string
	args []sqNode
}

func (n funcNode) emit(t sqTarget) (string, bool) {
	args := make([]string, len(n.args))
	for i, a := range n.args {
		s, ok := a.emit(t)
		if !ok {
			return "", false
		}
		args[i] = s
	}
	name, ok := n.resolveName(t)
	if !ok {
		return "", false
	}
	return name + "(" + strings.Join(args, ", ") + ")", true
}

// castNode is `CAST(x AS <affinity>)`. affinity is one of the five SQLite
// affinities, lower-cased.
type castNode struct {
	x        sqNode
	affinity string
}

func (n castNode) emit(t sqTarget) (string, bool) {
	xs, ok := n.x.emit(t)
	if !ok {
		return "", false
	}
	ty, ok := sqliteCastType(n.affinity, t)
	if !ok {
		return "", false
	}
	return "CAST(" + xs + " AS " + ty + ")", true
}

// resolveName maps this call to the target's canonical function spelling, or
// ok=false when the call is not PROVABLY-equivalent on that target. The
// value-fidelity-critical exclusions (a green representative test would have
// missed each — the Bug-74 lesson):
//
//   - length: PG length counts CHARACTERS; MySQL LENGTH counts BYTES, so the
//     match is CHAR_LENGTH — the one function whose target spelling differs.
//   - min/max (scalar ≥2-arg): become LEAST/GREATEST, but ONLY on MySQL.
//     SQLite scalar min/max return NULL if ANY argument is NULL; PG
//     LEAST/GREATEST SKIP NULLs (max(5,NULL) → NULL in SQLite, 5 in PG). MySQL
//     LEAST/GREATEST propagate NULL, matching SQLite.
//   - substr/substring: only a LITERAL start ≥ 1 (and, if present, a literal
//     len ≥ 0) is portable — SQLite's negative start counts from the end,
//     which PG SUBSTRING does not, and a non-literal start could be negative
//     at runtime (statically undetectable).
//
// Excluded entirely (removed from validSQLiteFunc, so a call never reaches
// here): upper/lower (SQLite folds ASCII only; PG/MySQL fold Unicode) and
// round (SQLite is half-away-from-zero; PG/MySQL round-half-to-even on floats,
// and the operand can't be proven decimal-not-float statically).
func (n funcNode) resolveName(t sqTarget) (string, bool) {
	switch n.name {
	case "abs":
		return "ABS", true
	case "coalesce", "ifnull":
		return "COALESCE", true
	case "nullif":
		return "NULLIF", true
	case "length":
		if t == sqMySQL {
			return "CHAR_LENGTH", true
		}
		return "LENGTH", true
	case "trim":
		return "TRIM", true
	case "ltrim":
		return "LTRIM", true
	case "rtrim":
		return "RTRIM", true
	case "substr", "substring":
		if !portableSubstrArgs(n.args) {
			return "", false
		}
		return "SUBSTRING", true
	case "min":
		if t == sqPG {
			return "", false
		}
		return "LEAST", true
	case "max":
		if t == sqPG {
			return "", false
		}
		return "GREATEST", true
	}
	return "", false
}

// sqliteCastType maps a SQLite CAST affinity to the target's type keyword, or
// ok=false when the cast is not provably-equivalent. Only TEXT and REAL are
// safe on both; NUMERIC is safe on PG only. INTEGER and BLOB are excluded on
// BOTH:
//
//   - INTEGER: SQLite truncates toward zero; PG/MySQL ROUND
//     (cast(2.9 as integer) → 2 vs 3).
//   - NUMERIC on MySQL: bare DECIMAL is DECIMAL(10,0), which rounds to zero
//     decimals (cast(2.5 as numeric) → 2.5 vs 3). PG NUMERIC is faithful.
//   - BLOB: cross-engine byte semantics are murky (PG bytea escape handling
//     vs MySQL BINARY), not provably-equivalent.
func sqliteCastType(affinity string, t sqTarget) (string, bool) {
	switch affinity {
	case "text":
		if t == sqMySQL {
			return "CHAR", true
		}
		return "TEXT", true
	case "real":
		if t == sqMySQL {
			return "DOUBLE", true // MySQL 8.0.17+
		}
		return "DOUBLE PRECISION", true
	case "numeric":
		if t == sqMySQL {
			return "", false
		}
		return "NUMERIC", true
	}
	// integer, blob → excluded on both targets.
	return "", false
}

// validSQLiteFunc reports whether an allowlisted function accepts argc args.
// The arity bounds keep the aggregate forms out (1-arg min/max is the
// aggregate, not the scalar LEAST/GREATEST) and reject the non-portable
// 2-arg trim(x, chars) form. upper/lower/round are intentionally absent —
// they are not provably-equivalent on any target (see resolveName).
func validSQLiteFunc(lower string, argc int) bool {
	switch lower {
	case "abs", "length", "trim", "ltrim", "rtrim":
		return argc == 1
	case "coalesce":
		return argc >= 2
	case "ifnull", "nullif":
		return argc == 2
	case "substr", "substring":
		return argc == 2 || argc == 3
	case "min", "max":
		return argc >= 2
	}
	return false
}

// portableSubstrArgs reports whether a substr/substring call's start/len are
// LITERAL integers safe to translate to PG/MySQL SUBSTRING: start ≥ 1, and (if
// present) len ≥ 0. A negative literal parses as a unaryNode (not a
// verbatimNode), so it fails the literal check; a non-literal (column/expr)
// start also fails — its runtime value could be negative, which SQLite counts
// from the end but SUBSTRING does not.
func portableSubstrArgs(args []sqNode) bool {
	if len(args) < 2 {
		return false
	}
	start, ok := intLiteralValue(args[1])
	if !ok || start < 1 {
		return false
	}
	if len(args) == 3 {
		if n, ok := intLiteralValue(args[2]); !ok || n < 0 {
			return false
		}
	}
	return true
}

// intLiteralValue returns the value of a node that is a bare non-negative
// integer literal (all ASCII digits — a decimal, exponent, or sign fails).
func intLiteralValue(n sqNode) (int, bool) {
	v, ok := n.(verbatimNode)
	if !ok {
		return 0, false
	}
	if v.text == "" {
		return 0, false
	}
	for i := 0; i < len(v.text); i++ {
		if v.text[i] < '0' || v.text[i] > '9' {
			return 0, false
		}
	}
	x, err := strconv.Atoi(v.text)
	if err != nil {
		return 0, false
	}
	return x, true
}

func isSQLiteAffinity(a string) bool {
	switch a {
	case "integer", "text", "real", "blob", "numeric":
		return true
	}
	return false
}

// ---- Parser (precedence-climbing over SQLite's operator precedence) ----
//
// Lowest → highest: OR < AND < NOT < comparison(= <> < <= > >=, IS [NOT] NULL)
// < add(+ -) < mul(* / %) < concat(||) < unary(+ -) < primary. Every parse
// method returns ok=false on the first unrecognised token; translateSQLiteExpr
// additionally requires ALL tokens be consumed, so an unknown tail rejects.

type sqParser struct {
	toks []sqTok
	pos  int
}

func (p *sqParser) atEnd() bool { return p.pos >= len(p.toks) }

func (p *sqParser) peekWord(w string) bool {
	if p.pos >= len(p.toks) {
		return false
	}
	t := p.toks[p.pos]
	return t.kind == sqWord && strings.EqualFold(t.text, w)
}

func (p *sqParser) peekPunct(s string) bool {
	if p.pos >= len(p.toks) {
		return false
	}
	t := p.toks[p.pos]
	return t.kind == sqPunct && t.text == s
}

func (p *sqParser) parseExpr() (sqNode, bool) { return p.parseOr() }

func (p *sqParser) parseOr() (sqNode, bool) {
	left, ok := p.parseAnd()
	if !ok {
		return nil, false
	}
	for p.peekWord("OR") {
		p.pos++
		right, ok := p.parseAnd()
		if !ok {
			return nil, false
		}
		left = binaryNode{op: "OR", l: left, r: right}
	}
	return left, true
}

func (p *sqParser) parseAnd() (sqNode, bool) {
	left, ok := p.parseNot()
	if !ok {
		return nil, false
	}
	for p.peekWord("AND") {
		p.pos++
		right, ok := p.parseNot()
		if !ok {
			return nil, false
		}
		left = binaryNode{op: "AND", l: left, r: right}
	}
	return left, true
}

func (p *sqParser) parseNot() (sqNode, bool) {
	if p.peekWord("NOT") {
		p.pos++
		x, ok := p.parseNot()
		if !ok {
			return nil, false
		}
		return unaryNode{op: "NOT", x: x}, true
	}
	return p.parseComparison()
}

func (p *sqParser) parseComparison() (sqNode, bool) {
	left, ok := p.parseAdd()
	if !ok {
		return nil, false
	}
	for {
		if op, isCmp := p.peekCompareOp(); isCmp {
			p.pos++
			right, ok := p.parseAdd()
			if !ok {
				return nil, false
			}
			left = binaryNode{op: op, l: left, r: right}
			continue
		}
		if p.peekWord("IS") {
			p.pos++
			negated := false
			if p.peekWord("NOT") {
				p.pos++
				negated = true
			}
			// Only `IS [NOT] NULL` is portable; `a IS b` (SQLite's
			// IS-NOT-DISTINCT-FROM shorthand) has no direct PG/MySQL form.
			if !p.peekWord("NULL") {
				return nil, false
			}
			p.pos++
			left = isNullNode{x: left, negated: negated}
			continue
		}
		break
	}
	return left, true
}

func (p *sqParser) peekCompareOp() (string, bool) {
	if p.pos >= len(p.toks) {
		return "", false
	}
	t := p.toks[p.pos]
	if t.kind != sqPunct {
		return "", false
	}
	switch t.text {
	case "=", "==":
		return "=", true
	case "<>", "!=":
		return "<>", true
	case "<":
		return "<", true
	case "<=":
		return "<=", true
	case ">":
		return ">", true
	case ">=":
		return ">=", true
	}
	return "", false
}

func (p *sqParser) parseAdd() (sqNode, bool) {
	left, ok := p.parseMul()
	if !ok {
		return nil, false
	}
	for p.peekPunct("+") || p.peekPunct("-") {
		op := p.toks[p.pos].text
		p.pos++
		right, ok := p.parseMul()
		if !ok {
			return nil, false
		}
		left = binaryNode{op: op, l: left, r: right}
	}
	return left, true
}

func (p *sqParser) parseMul() (sqNode, bool) {
	left, ok := p.parseConcat()
	if !ok {
		return nil, false
	}
	for p.peekPunct("*") || p.peekPunct("/") || p.peekPunct("%") {
		op := p.toks[p.pos].text
		p.pos++
		right, ok := p.parseConcat()
		if !ok {
			return nil, false
		}
		left = binaryNode{op: op, l: left, r: right}
	}
	return left, true
}

func (p *sqParser) parseConcat() (sqNode, bool) {
	left, ok := p.parseUnary()
	if !ok {
		return nil, false
	}
	if !p.peekPunct("||") {
		return left, true
	}
	parts := []sqNode{left}
	for p.peekPunct("||") {
		p.pos++
		right, ok := p.parseUnary()
		if !ok {
			return nil, false
		}
		parts = append(parts, right)
	}
	return concatNode{parts: parts}, true
}

func (p *sqParser) parseUnary() (sqNode, bool) {
	if p.peekPunct("-") || p.peekPunct("+") {
		op := p.toks[p.pos].text
		p.pos++
		x, ok := p.parseUnary()
		if !ok {
			return nil, false
		}
		return unaryNode{op: op, x: x}, true
	}
	return p.parsePrimary()
}

func (p *sqParser) parsePrimary() (sqNode, bool) {
	if p.pos >= len(p.toks) {
		return nil, false
	}
	t := p.toks[p.pos]
	switch t.kind {
	case sqPunct:
		if t.text == "(" {
			p.pos++
			e, ok := p.parseExpr()
			if !ok {
				return nil, false
			}
			if !p.peekPunct(")") {
				return nil, false
			}
			p.pos++
			return e, true
		}
		return nil, false
	case sqString:
		p.pos++
		return stringNode{text: t.text}, true
	case sqIdent:
		p.pos++
		return identNode{text: t.text}, true
	case sqWord:
		up := strings.ToUpper(t.text)
		switch up {
		case "NULL":
			p.pos++
			return verbatimNode{text: "NULL"}, true
		case "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME":
			p.pos++
			return verbatimNode{text: up}, true
		case "CAST":
			return p.parseCast()
		}
		// A word immediately followed by `(` is a function call.
		if p.pos+1 < len(p.toks) && p.toks[p.pos+1].kind == sqPunct && p.toks[p.pos+1].text == "(" {
			return p.parseFuncCall(t.text)
		}
		// Otherwise a bare column identifier or a numeric literal — both
		// render identically on either target.
		p.pos++
		return verbatimNode{text: t.text}, true
	}
	return nil, false
}

func (p *sqParser) parseCast() (sqNode, bool) {
	p.pos++ // consume CAST
	if !p.peekPunct("(") {
		return nil, false
	}
	p.pos++
	x, ok := p.parseExpr()
	if !ok {
		return nil, false
	}
	if !p.peekWord("AS") {
		return nil, false
	}
	p.pos++
	if p.pos >= len(p.toks) || p.toks[p.pos].kind != sqWord {
		return nil, false
	}
	affinity := strings.ToLower(p.toks[p.pos].text)
	if !isSQLiteAffinity(affinity) {
		return nil, false
	}
	p.pos++
	if !p.peekPunct(")") {
		return nil, false
	}
	p.pos++
	return castNode{x: x, affinity: affinity}, true
}

func (p *sqParser) parseFuncCall(name string) (sqNode, bool) {
	p.pos++ // consume the name
	if !p.peekPunct("(") {
		return nil, false
	}
	p.pos++
	var args []sqNode
	if !p.peekPunct(")") {
		for {
			a, ok := p.parseExpr()
			if !ok {
				return nil, false
			}
			args = append(args, a)
			if p.peekPunct(",") {
				p.pos++
				continue
			}
			break
		}
	}
	if !p.peekPunct(")") {
		return nil, false
	}
	p.pos++
	lower := strings.ToLower(name)
	if !validSQLiteFunc(lower, len(args)) {
		return nil, false
	}
	return funcNode{name: lower, args: args}, true
}

// ---- Tokenizer (minimal, duplicated — see the package doc) ----

type sqKind int

const (
	sqWord   sqKind = iota // identifier / keyword / numeric literal
	sqString               // '…' single-quoted string literal
	sqIdent                // "…" / `…` / […] quoted identifier
	sqPunct                // one punctuation byte, or a merged 2-byte operator
)

type sqTok struct {
	kind sqKind
	text string
}

// tokenizeSQLiteExpr splits a SQLite expression into tokens, dropping
// whitespace and comments. Single-quoted strings and quoted identifiers are
// each one token (delimiters included). Numeric literals — including a leading
// dot, an exponent, or a 0x hex form — are one word token so `3.14` doesn't
// fragment on the dot. Punctuation is one byte, except the known two-byte
// operators (`||`, `<=`, `>=`, `<>`, `!=`, `==`) which merge.
func tokenizeSQLiteExpr(s string) []sqTok {
	var toks []sqTok
	i, n := 0, len(s)
	for i < n {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
			i++
		case c == '-' && i+1 < n && s[i+1] == '-':
			i += 2
			for i < n && s[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && s[i+1] == '*':
			i += 2
			for i < n && (s[i] != '*' || i+1 >= n || s[i+1] != '/') {
				i++
			}
			if i < n {
				i += 2
			}
		case c == '\'':
			j := sqSkipQuoted(s, i, '\'')
			toks = append(toks, sqTok{sqString, s[i:j]})
			i = j
		case c == '"' || c == '`':
			j := sqSkipQuoted(s, i, c)
			toks = append(toks, sqTok{sqIdent, s[i:j]})
			i = j
		case c == '[':
			j := i + 1
			for j < n && s[j] != ']' {
				j++
			}
			if j < n {
				j++
			}
			toks = append(toks, sqTok{sqIdent, s[i:j]})
			i = j
		case sqIsDigit(c) || (c == '.' && i+1 < n && sqIsDigit(s[i+1])):
			j := sqScanNumber(s, i)
			toks = append(toks, sqTok{sqWord, s[i:j]})
			i = j
		case sqIsIdentStart(c):
			j := i + 1
			for j < n && sqIsIdentByte(s[j]) {
				j++
			}
			toks = append(toks, sqTok{sqWord, s[i:j]})
			i = j
		default:
			op, adv := sqReadPunct(s, i)
			toks = append(toks, sqTok{sqPunct, op})
			i += adv
		}
	}
	return toks
}

// sqSkipQuoted returns the index just past the closing quote q for the literal
// opening at i, honouring the doubled-quote escape.
func sqSkipQuoted(s string, i int, q byte) int {
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

// sqScanNumber returns the index just past the numeric literal starting at i.
func sqScanNumber(s string, i int) int {
	n := len(s)
	j := i
	if s[j] == '0' && j+1 < n && (s[j+1] == 'x' || s[j+1] == 'X') {
		j += 2
		for j < n && sqIsHex(s[j]) {
			j++
		}
		return j
	}
	for j < n && sqIsDigit(s[j]) {
		j++
	}
	if j < n && s[j] == '.' {
		j++
		for j < n && sqIsDigit(s[j]) {
			j++
		}
	}
	if j < n && (s[j] == 'e' || s[j] == 'E') {
		k := j + 1
		if k < n && (s[k] == '+' || s[k] == '-') {
			k++
		}
		if k < n && sqIsDigit(s[k]) {
			j = k
			for j < n && sqIsDigit(s[j]) {
				j++
			}
		}
	}
	return j
}

// sqReadPunct returns the operator token at i and how many bytes it spans,
// merging the known two-byte operators.
func sqReadPunct(s string, i int) (op string, width int) {
	if i+1 < len(s) {
		switch s[i : i+2] {
		case "||", "<=", ">=", "<>", "!=", "==":
			return s[i : i+2], 2
		}
	}
	return s[i : i+1], 1
}

func sqIsDigit(c byte) bool { return c >= '0' && c <= '9' }

func sqIsHex(c byte) bool {
	return sqIsDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func sqIsIdentStart(c byte) bool {
	return c == '_' ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		c >= 0x80
}

func sqIsIdentByte(c byte) bool {
	return sqIsIdentStart(c) || sqIsDigit(c) || c == '$'
}
