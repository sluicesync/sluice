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
	folded := foldOutsideLiterals(foldServerRenderings(foldBetweenSimple(expr)))
	folded = foldPGAnyArrayLists(folded)
	return stripGrouping(folded)
}

// foldBetweenSimple rewrites `X BETWEEN A AND B` to `X >= A and X <= B`
// — PostgreSQL's own DDL-time expansion of the form (DIFF-2, measured
// 2026-08-14: `v BETWEEN 1 AND 10` reads back from PG as
// `((v >= 1) AND (v <= 10))` while MySQL preserves `between`). The
// rewrite is semantics-exact (BETWEEN is defined as that expansion), so
// unlike the decoration folds it opens no false-clean window.
//
// It fires only on the SIMPLE shape — X a single identifier, A and B
// each a signed number, a quoted literal, or an identifier — because
// extracting a compound left operand needs precedence-aware parsing.
// Anything else (compound operands, NOT BETWEEN, BETWEEN SYMMETRIC) is
// left verbatim: the two sides then compare as different, the
// spurious-DIFFERENCE direction, never a spurious match. It runs FIRST,
// on the raw expression, so [foldServerRenderings]'s own token folds see
// its output.
func foldBetweenSimple(expr string) string {
	var sb strings.Builder
	sb.Grow(len(expr))
	i := 0
	for i < len(expr) {
		c := expr[i]
		switch {
		case c == '\'':
			end := rawLiteralEnd(expr, i)
			sb.WriteString(expr[i:end])
			i = end
		case isIdentByte(c) && (i == 0 || !isIdentByte(expr[i-1])):
			j := skipIdent(expr, i)
			if strings.EqualFold(expr[i:j], "between") {
				if out, next, ok := rewriteBetween(sb.String(), expr, j); ok {
					sb.Reset()
					sb.WriteString(out)
					i = next
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

// rewriteBetween attempts the simple-shape BETWEEN rewrite: written is
// the output emitted so far (its trailing identifier is the left
// operand), and j sits just past the `between` token. On success it
// returns the full replacement output and the index just past B.
func rewriteBetween(written, expr string, j int) (out string, next int, ok bool) {
	// Left operand: the trailing identifier of what was already written,
	// preceded by whitespace/boundary — and not a keyword (a `not`
	// before BETWEEN means NOT BETWEEN, which is a different predicate).
	end := len(written)
	for end > 0 && (written[end-1] == ' ' || written[end-1] == '\t' || written[end-1] == '\n' || written[end-1] == '\r') {
		end--
	}
	start := end
	for start > 0 && isIdentByte(written[start-1]) {
		start--
	}
	lhs := written[start:end]
	if lhs == "" || isBoolKeyword(lhs) {
		return "", 0, false
	}
	// The identifier must be the WHOLE left operand: if an operator
	// precedes it (`v + w between …`), the operand is compound and the
	// simple rewrite would bind only its last term — bail verbatim.
	p := start
	for p > 0 && (written[p-1] == ' ' || written[p-1] == '\t' || written[p-1] == '\n' || written[p-1] == '\r') {
		p--
	}
	if p > 0 && strings.IndexByte("+-*/%<>=|&^~!.:", written[p-1]) >= 0 {
		return "", 0, false
	}
	// A bare `not` before the operand is `not v between a and b` —
	// NOT BETWEEN under MySQL precedence; expanding the BETWEEN alone
	// would invert the meaning. Bail verbatim (the premise note in
	// [foldServerRenderings]'s doc).
	prevEnd := p
	prevStart := prevEnd
	for prevStart > 0 && isIdentByte(written[prevStart-1]) {
		prevStart--
	}
	if strings.EqualFold(written[prevStart:prevEnd], "not") {
		return "", 0, false
	}
	a, k, ok := simpleTermEnd(expr, skipSpaces(expr, j))
	if !ok {
		return "", 0, false
	}
	k = skipSpaces(expr, k)
	m := skipIdent(expr, k)
	if !strings.EqualFold(expr[k:m], "and") {
		return "", 0, false
	}
	b, next, ok := simpleTermEnd(expr, skipSpaces(expr, m))
	if !ok {
		return "", 0, false
	}
	return written[:start] + lhs + " >= " + a + " and " + lhs + " <= " + b, next, true
}

// simpleTermEnd parses one simple BETWEEN bound starting at i: a signed
// number, a single-quoted literal, or an identifier. Returns the raw
// term text and the index just past it.
func simpleTermEnd(expr string, i int) (term string, next int, ok bool) {
	if i >= len(expr) {
		return "", 0, false
	}
	start := i
	if expr[i] == '-' || expr[i] == '+' {
		i++
	}
	switch {
	case i < len(expr) && expr[i] == '\'':
		if i != start {
			return "", 0, false // a signed string literal is not a shape we fold
		}
		return expr[start:rawLiteralEnd(expr, i)], rawLiteralEnd(expr, i), true
	case i < len(expr) && isIdentByte(expr[i]):
		j := skipIdent(expr, i)
		tok := expr[i:j]
		if isBoolKeyword(tok) {
			return "", 0, false
		}
		// A call (identifier followed by an open paren) is compound.
		if k := skipSpaces(expr, j); k < len(expr) && expr[k] == '(' {
			return "", 0, false
		}
		// Allow a decimal tail on a number (`10.5`).
		if j+1 < len(expr) && expr[j] == '.' && isIdentByte(expr[j+1]) {
			j = skipIdent(expr, j+1)
		}
		return expr[start:j], j, true
	default:
		return "", 0, false
	}
}

// isBoolKeyword reports whether tok is a boolean/structural keyword that
// can never be a BETWEEN operand or bound.
func isBoolKeyword(tok string) bool {
	switch strings.ToLower(tok) {
	case "and", "or", "not", "between", "in", "like", "is", "null", "when", "then", "else", "end", "case":
		return true
	}
	return false
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
// The DIFF-2 measurement pass (2026-08-14) extended the fold set with
// the rest of the measured phantom families — a 30-family portable
// CHECK corpus applied identically to a real MySQL 8 and PostgreSQL 16,
// 13 of which re-rendered differently. The additional folds, each
// carrying its measured pair as a verbatim pin:
//
//   - `substring(` → `substr(` (MySQL renders substr; PG a quoted
//     `"substring"` call) and `ceiling(` → `ceil(` (MySQL renders
//     ceiling; PG keeps ceil) — spelling renames like char_length.
//   - `trim(BOTH FROM x)` → `trim(x)` — PG's rendering of plain trim.
//     LEADING/TRAILING/pad-character forms are unmeasured and verbatim.
//   - `round(x, 0)` → `round(x)` — MySQL materializes the default
//     scale. A non-zero scale is meaning and is preserved.
//   - `mod(a, b)` → `(a % b)` — MySQL renders the call as the operator.
//   - `~~` → `like`, `!~~` → `not like` — PG's operator spellings
//     (`~~*`/`!~~*` ILIKE forms are unmeasured and verbatim), plus
//     NOT-pushdown of a single parenthesized comparison
//     (`not(v = 5)` → `v <> 5`), because MySQL's catalog SIMPLIFIES
//     the negation while PG preserves it. CHECK-semantics exact (see
//     [negatedComparison] on NULL). A NOT over a conjunction or
//     anything but one comparison stays verbatim.
//   - `X between A and B` → `X >= A and X <= B` for simple operands
//     ([foldBetweenSimple], run first) — PG's own expansion.
//   - a quoted NUMERIC literal unquotes (`'-100000'` ≡ `-100000`) —
//     PG's rendering of negative literals; see [decodeLiteral].
//
// The deliberate cost, stated: two predicates differing ONLY in the cast
// TYPE (`cast(0 as decimal)` vs `cast(0 as unsigned)`), only in
// length/substr/ceil-function spelling, only in mod-vs-% spelling, only
// in an explicit zero round scale, only in BETWEEN-vs-its-expansion, in
// a pushed-down negation, or in string-vs-number literal spelling of
// the same number, canonicalize equal — so a hand-edit of exactly that
// much on a name-matched constraint reports clean. The between /
// not-pushdown / mod / round-scale-0 collapses are semantics-exact
// GIVEN GROUPED OPERANDS (premise below); the numeric-literal collapse
// is NOT semantics-exact in every typing context and is a stated
// window (see [decodeLiteral]). The original Bug 241 window reasoning
// (operator decision, 2026-08-11) covers the rest: both sides of every
// migrate-created pair are renderings of ONE source predicate, and
// every REAL drift changes more than the decoration. Each fold is
// pinned in both directions (phantom → clean; a genuinely-different
// predicate still differs).
//
// PREMISE, stated per the premise-naming rule: the structural rewrites
// (NOT-pushdown, BETWEEN expansion) treat a textual group as the whole
// parse operand, which on RAW, UNGROUPED input could misbind under
// MySQL's low NOT/BETWEEN precedence (`not(v = 5) = w`,
// `not v between 1 and 2`). Both diff inputs are catalog renderings
// (MySQL information_schema, PG pg_get_constraintdef) or sluice's own
// emitter, all of which fully parenthesize — and both rewrites also
// carry a cheap syntactic bail for exactly those ungrouped shapes (a
// comparison operator immediately after a pushed-down group; a bare
// `not` before the BETWEEN operand), pinned, so a rendering violating
// the premise degrades to verbatim (spurious difference), never to a
// meaning-changed rewrite.
//
// It runs on the RAW expression, BEFORE [foldOutsideLiterals], because
// both folds need real token boundaries: whitespace removal glues
// `... and char_length(` into `andchar_length(` and makes keyword
// detection unsound. Literals are copied verbatim (via [rawLiteralEnd],
// whose only escape is the SQL-standard `”`) so nothing inside a value
// is folded.
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
		case c == '!' && strings.HasPrefix(expr[i:], "!~~") && !strings.HasPrefix(expr[i:], "!~~*"):
			// PG's operator spelling of NOT LIKE (DIFF-2). The `!~~*`
			// (NOT ILIKE) guard: unmeasured, left verbatim.
			sb.WriteString(" not like ")
			i += 3
		case c == '~' && strings.HasPrefix(expr[i:], "~~") && !strings.HasPrefix(expr[i:], "~~*"):
			// PG's operator spelling of LIKE. `~~*` (ILIKE) unmeasured.
			sb.WriteString(" like ")
			i += 2
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
			case strings.EqualFold(expr[i:j], "substring"):
				// MySQL renders substring() back as substr(); PG keeps
				// (and double-quotes) "substring"(). Both fold to the
				// substr spelling. The `(`-lookahead skips PG's closing
				// identifier quote.
				if callParenFollows(expr, j) {
					sb.WriteString("substr")
					i = j
					continue
				}
			case strings.EqualFold(expr[i:j], "ceiling"):
				// MySQL renders ceil() back as ceiling(); PG keeps ceil().
				if callParenFollows(expr, j) {
					sb.WriteString("ceil")
					i = j
					continue
				}
			case strings.EqualFold(expr[i:j], "trim"):
				// PG renders trim(x) back as TRIM(BOTH FROM x). Only the
				// argumentless BOTH FROM prefix is folded (measured);
				// LEADING/TRAILING and pad-character forms stay verbatim.
				if next, ok := trimBothFromArgStart(expr, j); ok {
					sb.WriteString("trim(")
					i = next
					continue
				}
			case strings.EqualFold(expr[i:j], "round"):
				// MySQL materializes round(x)'s default scale as
				// round(x, 0); PG keeps round(x). Fold the explicit
				// zero scale away. A non-zero scale is meaning, not
				// decoration, and is preserved.
				if inner, next, ok := callGroup(expr, j); ok {
					if args := splitTopLevelArgs(inner); len(args) == 2 && strings.TrimSpace(args[1]) == "0" {
						sb.WriteString("round(")
						sb.WriteString(foldServerRenderings(args[0]))
						sb.WriteString(")")
						i = next
						continue
					}
				}
			case strings.EqualFold(expr[i:j], "mod"):
				// MySQL renders mod(a, b) back as (a % b); PG keeps the
				// call form. Both fold to the operator spelling.
				if inner, next, ok := callGroup(expr, j); ok {
					if args := splitTopLevelArgs(inner); len(args) == 2 {
						sb.WriteString("(")
						sb.WriteString(foldServerRenderings(args[0]))
						sb.WriteString(" % ")
						sb.WriteString(foldServerRenderings(args[1]))
						sb.WriteString(")")
						i = next
						continue
					}
				}
			case strings.EqualFold(expr[i:j], "not"):
				// NOT-pushdown, for a single parenthesized comparison:
				// MySQL's catalog SIMPLIFIES `NOT (v = 5)` to `(v <> 5)`
				// and spells NOT LIKE as `not((x like p))`, while PG
				// preserves `NOT (…)` / spells NOT LIKE `!~~` — so the
				// negation must be normalized onto the comparison for
				// the two renderings of one predicate to meet. Fires
				// only when the group holds exactly one top-level
				// comparison and no AND/OR; anything else stays
				// verbatim (spurious-difference direction).
				if inner, next, ok := callGroup(expr, j); ok && !comparisonFollows(expr, next) {
					if lhs, op, rhs, cok := singleComparison(foldServerRenderings(inner)); cok {
						if neg, nok := negatedComparison(op); nok {
							sb.WriteString(lhs)
							sb.WriteString(" ")
							sb.WriteString(neg)
							sb.WriteString(" ")
							sb.WriteString(rhs)
							i = next
							continue
						}
					}
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

// comparisonFollows reports whether a comparison-operator byte is the
// first non-space content at i — the ungrouped-precedence bail for the
// NOT-pushdown fold: raw `not(v = 5) = w` parses as NOT((v=5)=w) under
// MySQL precedence, so pushing the inner group down would change
// meaning. See the premise note in [foldServerRenderings]'s doc.
func comparisonFollows(expr string, i int) bool {
	i = skipSpaces(expr, i)
	return i < len(expr) && (expr[i] == '=' || expr[i] == '<' || expr[i] == '>' || expr[i] == '!')
}

// callParenFollows reports whether a call's opening paren follows the
// identifier ending at j, allowing whitespace and a closing `"` (PG
// double-quotes the "substring" identifier in its rendering).
func callParenFollows(expr string, j int) bool {
	k := skipSpaces(expr, j)
	if k < len(expr) && expr[k] == '"' {
		k = skipSpaces(expr, k+1)
	}
	return k < len(expr) && expr[k] == '('
}

// trimBothFromArgStart matches `( BOTH FROM ` just past a trim token and
// returns the index of the argument that follows.
func trimBothFromArgStart(expr string, j int) (next int, ok bool) {
	k := skipSpaces(expr, j)
	if k >= len(expr) || expr[k] != '(' {
		return 0, false
	}
	k = skipSpaces(expr, k+1)
	m := skipIdent(expr, k)
	if !strings.EqualFold(expr[k:m], "both") {
		return 0, false
	}
	m = skipSpaces(expr, m)
	n := skipIdent(expr, m)
	if !strings.EqualFold(expr[m:n], "from") {
		return 0, false
	}
	return skipSpaces(expr, n), true
}

// callGroup parses a balanced `( … )` group starting at the first
// non-space past i, returning the inner text and the index just past
// the close. Literal-aware; an unbalanced group reports ok=false and
// the caller copies verbatim.
func callGroup(expr string, i int) (inner string, next int, ok bool) {
	i = skipSpaces(expr, i)
	if i >= len(expr) || expr[i] != '(' {
		return "", 0, false
	}
	depth := 1
	j := i + 1
	for j < len(expr) {
		switch expr[j] {
		case '\'':
			j = rawLiteralEnd(expr, j)
		case '(':
			depth++
			j++
		case ')':
			depth--
			if depth == 0 {
				return expr[i+1 : j], j + 1, true
			}
			j++
		default:
			j++
		}
	}
	return "", 0, false
}

// splitTopLevelArgs splits inner on depth-0 commas, literal-aware.
func splitTopLevelArgs(inner string) []string {
	var args []string
	depth, start := 0, 0
	for i := 0; i < len(inner); {
		switch c := inner[i]; {
		case c == '\'':
			i = rawLiteralEnd(inner, i)
		case c == '(' || c == '[':
			depth++
			i++
		case c == ')' || c == ']':
			depth--
			i++
		case c == ',' && depth == 0:
			args = append(args, inner[start:i])
			i++
			start = i
		default:
			i++
		}
	}
	return append(args, inner[start:])
}

// singleComparison reports the one top-level comparison in inner —
// (lhs, op, rhs) with op one of =, <>, <, <=, >, >=, like, notlike —
// requiring exactly one comparison and no top-level AND/OR/NOT. A fully
// paren-wrapped inner is unwrapped first (MySQL's NOT LIKE rendering
// double-wraps: `not((x like p))`).
func singleComparison(inner string) (lhs, op, rhs string, ok bool) {
	inner = strings.TrimSpace(inner)
	for {
		if u, uok := unwrapOuterParens(inner); uok {
			inner = strings.TrimSpace(u)
			continue
		}
		break
	}
	opStart, opEnd := -1, -1
	depth := 0
	notStart := -1
	for i := 0; i < len(inner); {
		c := inner[i]
		switch {
		case c == '\'':
			i = rawLiteralEnd(inner, i)
		case c == '(' || c == '[':
			depth++
			i++
		case c == ')' || c == ']':
			depth--
			i++
		case depth == 0 && (c == '<' || c == '>' || c == '=' || c == '!'):
			w := 1
			if i+1 < len(inner) && (inner[i+1] == '=' || inner[i+1] == '>') {
				w = 2
			}
			if c == '!' && w == 1 {
				i++ // lone '!' — not a comparison we know
				continue
			}
			if opStart >= 0 {
				return "", "", "", false // second comparison
			}
			opStart, opEnd = i, i+w
			op = inner[i : i+w]
			i += w
		case depth == 0 && isIdentByte(c) && (i == 0 || !isIdentByte(inner[i-1])):
			j := skipIdent(inner, i)
			switch strings.ToLower(inner[i:j]) {
			case "and", "or":
				return "", "", "", false
			case "not":
				notStart = i
			case "like":
				if opStart >= 0 {
					return "", "", "", false
				}
				if notStart >= 0 {
					// `x not like p` — lhs ends before the not token.
					opStart = notStart
					op = "notlike"
				} else {
					opStart = i
					op = "like"
				}
				opEnd = j
			}
			i = j
		default:
			i++
		}
	}
	if opStart < 0 {
		return "", "", "", false
	}
	lhs, rhs = strings.TrimSpace(inner[:opStart]), strings.TrimSpace(inner[opEnd:])
	if lhs == "" || rhs == "" {
		return "", "", "", false
	}
	if op == "!=" {
		op = "<>"
	}
	return lhs, op, rhs, true
}

// unwrapOuterParens removes one layer of parens when they span the
// whole string as a single balanced group.
func unwrapOuterParens(s string) (string, bool) {
	if len(s) < 2 || s[0] != '(' {
		return s, false
	}
	depth := 0
	for i := 0; i < len(s); {
		switch s[i] {
		case '\'':
			i = rawLiteralEnd(s, i)
		case '(':
			depth++
			i++
		case ')':
			depth--
			if depth == 0 {
				if i == len(s)-1 {
					return s[1 : len(s)-1], true
				}
				return s, false
			}
			i++
		default:
			i++
		}
	}
	return s, false
}

// negatedComparison maps a comparison operator to its negation, for the
// NOT-pushdown fold. Comparisons involving NULL evaluate to UNKNOWN on
// both sides of the rewrite identically (NOT UNKNOWN is UNKNOWN, and a
// CHECK treats UNKNOWN as pass), so the pushdown is CHECK-semantics
// exact.
func negatedComparison(op string) (string, bool) {
	switch op {
	case "=":
		return "<>", true
	case "<>":
		return "=", true
	case "<":
		return ">=", true
	case "<=":
		return ">", true
	case ">":
		return "<=", true
	case ">=":
		return "<", true
	case "like":
		return "not like", true
	case "notlike":
		return "like", true
	}
	return "", false
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
// that copy raw text instead of decoding it. The only escape is the
// SQL-standard doubled apostrophe (`”`); a backslash is an ORDINARY
// value byte (see [decodeLiteral] for why). Unterminated runs to
// end-of-string.
func rawLiteralEnd(expr string, i int) int {
	i++
	for i < len(expr) {
		switch {
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
// Literal scanning takes the value in the SQL-standard spelling both
// diff inputs arrive under — apostrophes doubled (`”`), backslashes
// literal (see [decodeLiteral]). Getting this wrong is not cosmetic — a
// mis-terminated literal would fold the rest of the expression's case,
// and dropping a backslash would make a tampered predicate canonicalize
// onto an honest one (audit 2026-08-11, DIFF-1).
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
// # Backslash is an ordinary value byte, not an escape (audit 2026-08-11, DIFF-1)
//
// Both diff inputs reach this pass in SQL-STANDARD spelling — apostrophes
// doubled (`”`), everything else literal — so the only escape is `”`. The
// MySQL reader's [mysql.renormalizeMySQLExpr] decodes the server's
// C-style escaping and RE-EMITS each value with `”`-doubling only
// (`strings.ReplaceAll(raw, "'", "”")`), leaving backslashes bare; PG's
// `pg_get_constraintdef` is standard-conforming and does the same. So a
// backslash inside a literal is a CHARACTER OF THE VALUE.
//
// The pre-fix code treated `\X` as a MySQL escape and dropped the
// backslash. That made two genuinely different literals differing only by
// an interior backslash — a weakened regex `'^a\d+$'` (digits required)
// vs `'^ad+$'` (accepts `add`) — canonicalize EQUAL, so `schema diff`
// certified a tampered/weakened CHECK as "in sync"; and it mis-scanned a
// value ENDING in a backslash (`'a\'`), consuming the closing quote and
// folding the following expression text into the value (the phantom-drift
// mirror). Both directions are gone now that backslash is ordinary.
//
// An UNTERMINATED literal decodes to end-of-string and is still closed
// with the sentinel: the expression is malformed, and the two sides then
// compare as the different things they are rather than collapsing onto a
// shared prefix.
//
// # Numeric-literal unquoting (DIFF-2, measured 2026-08-14)
//
// PostgreSQL renders a negative numeric literal QUOTED and cast —
// `>= -100000` reads back as `>= '-100000'::integer` — while MySQL
// renders `-(100000)`, whose parens and sign survive as plain tokens. So
// a decoded value that is a bare number (optional sign, digits, optional
// decimal tail) is emitted WITHOUT sentinels, making `'-100000'` and
// `-100000` one token stream. The stated window: a predicate comparing
// the STRING '123' against one comparing the NUMBER 123 canonicalizes
// equal. That is NOT semantics-exact in every typing context — against
// a numeric column both spellings coerce to one comparison, but
// against a MySQL STRING column `s = '123'` compares as a string while
// `s = 123` coerces `s` numerically (`'0123'` satisfies one and not
// the other), and the column's type is unknowable at this layer. The
// window is accepted on the Bug 241 reasoning (both sides of a
// migrate-created pair render ONE predicate; only a hand-edit flipping
// exactly the quoting hits it) and kept deliberately narrow: textual,
// not numeric, normalization (`'0123'` ≠ `123`, `'1.0'` ≠ `1`). The
// fold is suppressed when the adjacent output byte is an
// identifier byte (a MySQL hex literal `x'12'` must not merge into a
// fictitious `x12` token) and for unterminated literals.
func decodeLiteral(sb *strings.Builder, expr string, i int) int {
	var val strings.Builder
	terminated := false
	i++
	for i < len(expr) {
		if expr[i] == '\'' {
			if i+1 < len(expr) && expr[i+1] == '\'' {
				val.WriteByte('\'')
				i += 2
				continue
			}
			terminated = true
			i++
			break
		}
		val.WriteByte(expr[i])
		i++
	}
	v := val.String()
	prev := byte(0)
	if s := sb.String(); s != "" {
		prev = s[len(s)-1]
	}
	nextIsIdent := i < len(expr) && isIdentByte(expr[i])
	if terminated && isBareNumericToken(v) && !isIdentByte(prev) && !nextIsIdent {
		sb.WriteString(v)
		return i
	}
	sb.WriteString(literalOpen)
	sb.WriteString(v)
	sb.WriteString(literalClose)
	return i
}

// isBareNumericToken reports whether v is exactly an optionally-signed
// integer or decimal — the only literal values the unquoting fold above
// touches.
func isBareNumericToken(v string) bool {
	i := 0
	if i < len(v) && (v[i] == '-' || v[i] == '+') {
		i++
	}
	digits := 0
	for i < len(v) && v[i] >= '0' && v[i] <= '9' {
		i++
		digits++
	}
	if digits == 0 {
		return false
	}
	if i < len(v) && v[i] == '.' {
		i++
		frac := 0
		for i < len(v) && v[i] >= '0' && v[i] <= '9' {
			i++
			frac++
		}
		if frac == 0 {
			return false
		}
	}
	return i == len(v)
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
