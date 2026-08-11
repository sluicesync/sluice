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
// # Where it is used (widened for Bug 241 — this section used to say
// # "SluiceEmitted only", and that stopped being true on 2026-08-11)
//
// Two consumers: matching [ir.CheckConstraint.SluiceEmitted] entries
// (the original use), and — since Bug 241 — the NAME-matched user-CHECK
// comparison in diffChecks, as a fallback after byte equality fails,
// because the two servers re-render one predicate differently (a numeric
// literal wearing a cast, `length` spelled `char_length`) and byte
// comparison reported phantom drift on a target `migrate` itself
// created.
//
// # The named wart: parentheses are DROPPED, not balanced
//
// Stripping redundant parens properly needs an expression parser.
// [canonicalCheckExpr] instead removes every paren and bracket, so two
// predicates that differ ONLY in grouping canonicalize equal — e.g.
// `a AND (b OR c)` and `(a AND b) OR c`.
//
// For the EMITTED shapes that is sound by construction: `<@`, `IN`, and
// `REGEXP_LIKE` are single operators with no boolean structure at all,
// and the DOMAIN range shape is a pure conjunction, where regrouping
// cannot change meaning. For the Bug 241 name-matched fallback the same
// collapse is a KNOWN, ACCEPTED false-clean window: a user predicate
// hand-regrouped on the target under the same name — `a AND (b OR c)`
// edited to `(a AND b) OR c` — reports in sync, as does a cast added or
// removed around a literal ([foldServerRenderings]'s own documented
// window). Both windows are pinned as documentation
// (TestCanonicalCheckExpr_GroupingOnlyDifferencesAreTheOnlyCollision's
// name-matched half, TestCanonicalCheckExpr_ServerRenderingFolds); the
// alternative — a paren-preserving comparison for the name-matched path
// — would re-open Bug 241 for any pair of renderings that differ in
// parenthesization, which is the commoner case by far. A tamper that
// changes an operator, a column, or a literal still changes the token
// stream and is still reported.

import (
	"strings"
)

// pgAnyArrayMarker is PostgreSQL's canonical rendering of an `IN (…)`
// list — `= ANY (ARRAY[…])` — in the compact lowercase form the fold
// pass produces. [foldPGAnyArrayLists] folds each such term back to the
// `IN (…)` form the emitter wrote.
const pgAnyArrayMarker = "=any(array["

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
	folded := foldOutsideLiterals(foldServerRenderings(expr))
	folded = foldPGAnyArrayLists(folded)
	return stripGrouping(folded)
}

// foldServerRenderings folds the spellings the two servers give the SAME
// predicate different renderings of (Bug 241): a `migrate`-created CHECK
// read back from each side compares its own server's DDL-time
// normalization against the other's, and the differences are decoration,
// not meaning:
//
//   - `cast(X as T)` → `(X)` — MySQL's spelling of the literal cast PG
//     renders as `(X)::T` (whose `::T` suffix [foldOutsideLiterals]
//     already strips). Folding BOTH spellings to the bare argument makes
//     `v > cast(0 as decimal(10,0))` and `v > (0)::numeric` one
//     predicate. The argument is preserved verbatim (and folded
//     recursively, for nested casts); only the cast decoration goes.
//   - `char_length(` → `length(` — MySQL renders `length()` back as
//     `char_length()`.
//
// The deliberate cost, stated: two predicates differing ONLY in the cast
// TYPE (`cast(0 as decimal)` vs `cast(0 as unsigned)`) or only in
// length-function choice canonicalize equal, so a hand-edit of exactly
// that much on a name-matched constraint reports clean. That window was
// weighed in the Bug 241 memo and accepted (operator decision,
// 2026-08-11): both sides of every migrate-created pair are renderings
// of ONE source predicate, and every REAL drift the filing enumerates —
// dropped names, widened ranges, changed members, changed columns —
// changes more than the decoration. The fold is pinned in both
// directions (phantom → clean; a genuinely-different predicate still
// differs).
//
// It runs on the RAW expression, BEFORE [foldOutsideLiterals], because
// both folds need real token boundaries: whitespace removal glues
// `... and char_length(` into `andchar_length(` and makes keyword
// detection unsound. Literals are copied verbatim (both escape
// conventions, via [rawLiteralEnd]) so nothing inside a value is folded.
func foldServerRenderings(expr string) string {
	var sb strings.Builder
	sb.Grow(len(expr))
	for i := 0; i < len(expr); {
		c := expr[i]
		switch {
		case c == '\'':
			end := rawLiteralEnd(expr, i)
			sb.WriteString(expr[i:end])
			i = end
		case isIdentByte(c) && (i == 0 || !isIdentByte(expr[i-1])):
			j := skipIdent(expr, i)
			switch {
			case strings.EqualFold(expr[i:j], "cast"):
				if inner, next, ok := splitCastCall(expr, j); ok {
					sb.WriteString("(")
					sb.WriteString(foldServerRenderings(inner))
					sb.WriteString(")")
					i = next
					continue
				}
			case strings.EqualFold(expr[i:j], "char_length"):
				if k := skipSpaces(expr, j); k < len(expr) && expr[k] == '(' {
					sb.WriteString("length")
					i = j
					continue
				}
			}
			sb.WriteString(expr[i:j])
			i = j
		default:
			sb.WriteByte(c)
			i++
		}
	}
	return sb.String()
}

// splitCastCall parses `( X as T )` starting just past a `cast` token:
// it returns X's raw text and the index just past the closing paren. The
// `as` separator is the FIRST one at paren depth 1 outside literals — a
// nested cast's own `as` sits at depth ≥ 2, and a CHECK expression has
// no aliasing, so no other bare `as` can appear at depth 1. Anything
// that does not complete the shape (no paren, no `as`, unbalanced)
// reports ok=false and the caller copies the text verbatim — the
// spurious-DIFFERENCE direction, never a spurious match.
func splitCastCall(expr string, i int) (inner string, next int, ok bool) {
	i = skipSpaces(expr, i)
	if i >= len(expr) || expr[i] != '(' {
		return "", 0, false
	}
	depth := 1
	asStart := -1
	j := i + 1
	for j < len(expr) {
		switch c := expr[j]; {
		case c == '\'':
			j = rawLiteralEnd(expr, j)
		case c == '(':
			depth++
			j++
		case c == ')':
			depth--
			if depth == 0 {
				if asStart < 0 {
					return "", 0, false
				}
				return expr[i+1 : asStart], j + 1, true
			}
			j++
		case depth == 1 && isIdentByte(c) && !isIdentByte(expr[j-1]):
			k := skipIdent(expr, j)
			if asStart < 0 && strings.EqualFold(expr[j:k], "as") {
				asStart = j
			}
			j = k
		default:
			j++
		}
	}
	return "", 0, false
}

// rawLiteralEnd returns the index just past the single-quoted literal
// starting at expr[i] — the scan twin of [decodeLiteral], for passes
// that copy raw text instead of decoding it. Both escape conventions;
// unterminated runs to end-of-string.
func rawLiteralEnd(expr string, i int) int {
	i++
	for i < len(expr) {
		switch {
		case expr[i] == '\\' && i+1 < len(expr):
			i += 2
		case expr[i] == '\'' && i+1 < len(expr) && expr[i+1] == '\'':
			i += 2
		case expr[i] == '\'':
			return i + 1
		default:
			i++
		}
	}
	return i
}

// skipSpaces returns the index of the first non-whitespace byte at or
// after i.
func skipSpaces(expr string, i int) int {
	for i < len(expr) && (expr[i] == ' ' || expr[i] == '\t' || expr[i] == '\n' || expr[i] == '\r') {
		i++
	}
	return i
}

// foldPGAnyArrayLists rewrites every `=any(array[…])` term in the folded
// expression to `in(…)`, scanning for the BALANCED close rather than
// matching a regex. The predecessor (`=any\(array\[(.*)\]\)`) was greedy
// (audit GAP 5): an expression carrying TWO such terms canonicalized by
// spanning from the first `array[` to the LAST `])`, folding everything
// between — including the `and`/`or` joining the terms — into one
// fictitious member list. A lazy regex would merely trade that trap for
// its mirror: a sentinel-wrapped literal VALUE may carry `])` verbatim
// (values are decoded before this pass runs), and `(.*?)` would
// terminate on it. So the scan tracks bracket depth and skips
// literal-sentinel spans, exactly like every other pass over this form.
//
// A term with no balanced `])` close is left unfolded — the expression
// is malformed for this shape, and the two sides then compare as the
// different things they are (a spurious DIFFERENCE at worst, never a
// spurious match).
func foldPGAnyArrayLists(s string) string {
	var sb strings.Builder
	for {
		i := strings.Index(s, pgAnyArrayMarker)
		if i < 0 {
			sb.WriteString(s)
			return sb.String()
		}
		sb.WriteString(s[:i])
		rest := s[i+len(pgAnyArrayMarker):]

		depth := 0
		end := -1 // index in rest of the depth-0 ']' whose next byte is ')'
	scan:
		for j := 0; j < len(rest); j++ {
			switch rest[j] {
			case literalOpen[0]:
				k := strings.IndexByte(rest[j+1:], literalClose[0])
				if k < 0 {
					// Unterminated literal sentinel — malformed; stop scanning.
					break scan
				}
				j += 1 + k
			case '[':
				depth++
			case ']':
				if depth > 0 {
					depth--
					continue
				}
				if j+1 < len(rest) && rest[j+1] == ')' {
					end = j
				}
				// A depth-0 ']' either closes the term or the shape is not
				// the one this fold handles; stop scanning either way.
				break scan
			}
		}
		if end < 0 {
			// No balanced close: emit the marker verbatim and keep scanning
			// after it so a later well-formed term still folds.
			sb.WriteString(pgAnyArrayMarker)
			s = rest
			continue
		}
		sb.WriteString("in(")
		sb.WriteString(rest[:end])
		sb.WriteString(")")
		s = rest[end+2:]
	}
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
