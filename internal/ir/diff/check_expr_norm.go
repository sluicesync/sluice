// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

// Canonicalization of a CHECK-constraint expression, for matching the
// constraints sluice's own DDL emitters synthesize against what the
// target catalog hands back (Bug 237(b) / roadmap item 156).
//
// # Why any of this is needed
//
// No engine returns a CHECK expression as it was given. Measured, on the
// two targets sluice writes today:
//
//	emitted  "flags" <@ ARRAY['email','sms']::TEXT[]
//	PG reads (flags <@ ARRAY['email'::text, 'sms'::text])
//
//	emitted  "g_mood" IN ('happy','sad')
//	PG reads (g_mood = ANY (ARRAY['happy'::text, 'sad'::text]))
//
//	emitted  `pct` >= 0 AND `pct` <= 100
//	MySQL reads ((pct >= 0) and (pct <= 100))
//
//	emitted  REGEXP_LIKE(`email`, '^[a-z]+@example[.]com$')
//	MySQL reads regexp_like(email,'^[a-z]+@example[.]com$')
//
// Identifier quoting, keyword case, `::type` casts, whitespace,
// redundant parens, and PG's `IN` → `= ANY (ARRAY[…])` rewrite are all
// noise the catalog adds. The value lists and the operators are not.
//
// # Where it is and is NOT used
//
// ONLY to match [ir.CheckConstraint.SluiceEmitted] entries. Source-
// declared CHECKs keep the byte-exact trim-and-compare they have always
// had: those are matched by NAME on both sides, and softening their
// expression comparison would trade a class of real drift for nothing.
//
// # The named wart: parentheses are DROPPED, not balanced
//
// Stripping redundant parens properly needs an expression parser.
// [canonicalCheckExpr] instead removes every paren and bracket, so two
// predicates that differ ONLY in grouping canonicalize equal — e.g.
// `a AND (b OR c)` and `(a AND b) OR c`.
//
// That is sound HERE and only here, because the shapes sluice emits have
// no grouping freedom: `<@`, `IN`, and `REGEXP_LIKE` are single
// operators with no boolean structure at all, and the DOMAIN range shape
// is a pure conjunction, where regrouping cannot change meaning. A
// tamper that changes an operator, a column, or a literal still changes
// the token stream and is still reported.
// TestCanonicalCheckExpr_GroupingOnlyDifferencesAreTheOnlyCollision
// pins both halves: the collision exists, and no emitted shape can
// produce one.

import (
	"regexp"
	"strings"
)

// pgAnyArrayRewrite matches PostgreSQL's canonical rendering of an
// `IN (…)` list — `= ANY (ARRAY[…])` — so it can be folded back to the
// `IN (…)` form the emitter wrote. Applied AFTER case folding and
// whitespace removal, so the pattern sees the compact lowercase form.
var pgAnyArrayRewrite = regexp.MustCompile(`=any\(array\[(.*)\]\)`)

// Sentinel delimiters the fold pass wraps each DECODED literal value in.
//
// They exist because the passes run in sequence and a decoded literal is
// no longer self-delimiting: a doubled-quote literal decodes to a value
// carrying a bare apostrophe (`o'brien`), and a
// later pass re-scanning for quotes would take that interior quote as a
// terminator and start folding the rest of the value. Wrapping in bytes
// no SQL expression carries makes the second pass skip the span without
// re-interpreting it. A literal containing a raw NUL or SOH would defeat
// this; no CHECK predicate carries one, and the failure direction would
// be a spurious DIFFERENCE (a reported drift), not a spurious match.
const (
	literalOpen  = "\x00"
	literalClose = "\x01"
)

// canonicalCheckExpr reduces a CHECK expression to a form two engines'
// renderings of the SAME predicate share. Single-quoted string literals
// keep their VALUE exactly (a member list is exactly what a tamper would
// change); everything outside them is folded.
//
// Returns "" for an expression that is empty after folding, which the
// caller treats as un-matchable rather than as matching another empty.
func canonicalCheckExpr(expr string) string {
	folded := foldOutsideLiterals(expr)
	folded = pgAnyArrayRewrite.ReplaceAllString(folded, "in($1)")
	return stripGrouping(folded)
}

// foldOutsideLiterals lowercases, removes whitespace and identifier
// quoting, and drops `::type` casts and MySQL charset introducers —
// everywhere EXCEPT inside a single-quoted literal, whose VALUE is
// copied through unchanged (between [literalOpen] / [literalClose]).
//
// Literal scanning understands both escape conventions a CHECK
// expression can arrive under: SQL's doubled-apostrophe form and
// MySQL's backslash `\'`. Getting this wrong is not cosmetic — a mis-terminated
// literal would fold the rest of the expression's case and could make a
// tampered predicate canonicalize onto an honest one.
func foldOutsideLiterals(expr string) string {
	var sb strings.Builder
	sb.Grow(len(expr))
	for i := 0; i < len(expr); {
		c := expr[i]
		if c == '\'' {
			i = decodeLiteral(&sb, expr, i)
			continue
		}
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '"' || c == '`':
			// Identifier quoting: PG double-quotes, MySQL backticks. The
			// readers already strip most of it, but the EMITTED side has
			// not been through a reader.
			i++
		case c == ':' && i+1 < len(expr) && expr[i+1] == ':':
			i = skipCast(expr, i)
		case c == '_' && isCharsetIntroducer(expr, i):
			i = skipIdent(expr, i+1)
		default:
			sb.WriteByte(lowerASCII(c))
			i++
		}
	}
	return sb.String()
}

// decodeLiteral writes the VALUE of the single-quoted literal starting
// at expr[i] (which must be the opening quote) to sb, wrapped in the
// sentinel delimiters, and returns the index just past its closing
// quote.
//
// Decoding rather than copying is what makes the two escape conventions
// agree: an emitted literal that DOUBLES the apostrophe and a
// MySQL-rendered one that writes `\'` carry
// the same value and must canonicalize the same, or every regex DOMAIN
// whose pattern contains an apostrophe reports phantom drift.
//
// An UNTERMINATED literal decodes to end-of-string and is still closed
// with the sentinel: the expression is malformed, and the two sides then
// compare as the different things they are rather than collapsing onto a
// shared prefix.
func decodeLiteral(sb *strings.Builder, expr string, i int) int {
	sb.WriteString(literalOpen)
	i++
	for i < len(expr) {
		switch {
		case expr[i] == '\\' && i+1 < len(expr):
			// MySQL escapes the quote as \' — the backslash is the
			// escape, not a character of the value, so it is dropped and
			// its target taken literally.
			sb.WriteByte(expr[i+1])
			i += 2
		case expr[i] == '\'' && i+1 < len(expr) && expr[i+1] == '\'':
			sb.WriteByte('\'')
			i += 2
		case expr[i] == '\'':
			sb.WriteString(literalClose)
			return i + 1
		default:
			sb.WriteByte(expr[i])
			i++
		}
	}
	sb.WriteString(literalClose)
	return i
}

// skipCast consumes a `::type` or `::type[]` suffix starting at the
// first colon, returning the index just past it. A lone `:` that is not
// part of a cast is left for the caller (returning i+1 would silently
// swallow it).
func skipCast(expr string, i int) int {
	j := skipIdent(expr, i+2)
	if j == i+2 {
		// `::` with no identifier after it — not a cast we recognise.
		// Emit nothing and step past the colons rather than looping.
		return i + 2
	}
	for j+1 < len(expr) && expr[j] == '[' && expr[j+1] == ']' {
		j += 2
	}
	return j
}

// skipIdent returns the index just past the identifier characters
// starting at i (possibly i itself, when there are none).
func skipIdent(expr string, i int) int {
	for i < len(expr) && isIdentByte(expr[i]) {
		i++
	}
	return i
}

// isCharsetIntroducer reports whether expr[i] starts a MySQL charset
// introducer — `_utf8mb4'…'`, `_latin1'…'` — which information_schema
// prefixes to string literals in a CHECK_CLAUSE. It is recognised by
// SHAPE (underscore, identifier, opening quote) rather than by a list of
// charset names, so a server charset this code has never heard of is
// handled the same way.
func isCharsetIntroducer(expr string, i int) bool {
	j := skipIdent(expr, i+1)
	return j > i+1 && j < len(expr) && expr[j] == '\''
}

func isIdentByte(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func lowerASCII(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}

// stripGrouping removes every parenthesis and bracket that is not inside
// a sentinel-delimited literal value. See the file header for why this
// is sound for the emitted shapes and what it costs.
//
// It consumes the output of [foldOutsideLiterals], so it skips literal
// spans by their sentinels rather than by re-scanning for quotes — a
// decoded value can contain a bare quote, and re-parsing would take it
// as a terminator.
func stripGrouping(expr string) string {
	var sb strings.Builder
	sb.Grow(len(expr))
	inLiteral := false
	for i := 0; i < len(expr); i++ {
		switch {
		case expr[i] == literalOpen[0]:
			inLiteral = true
		case expr[i] == literalClose[0]:
			inLiteral = false
		case !inLiteral && (expr[i] == '(' || expr[i] == ')' || expr[i] == '[' || expr[i] == ']'):
			continue
		}
		sb.WriteByte(expr[i])
	}
	return sb.String()
}
