// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

import (
	"sort"
	"strconv"
	"strings"
)

// [canonicalCheckExpr] folds two CHECK expressions toward a common shape
// so one predicate written two ways compares equal. Every fold throws
// information away, and most of what it throws away is genuinely
// meaningless — a server's own parentheses, a `pg_catalog.` qualifier,
// the spelling of AND. Some of it is not.
//
// Audit 2026-09-09 RC-2 measured three pairs that canonicalize EQUAL and
// are not the same predicate, so `schema diff` reported IN SYNC for a
// target that had drifted (each reproduced before this file was written):
//
//	a-(b+c) > 0      vs  (a-b)+c > 0     both fold to  a-b+c>0
//	x::text < '5'    vs  x < 5           both fold to  x<5
//	"Amount" > 0     vs  amount > 0      both fold to  amount>0
//
// The first is arithmetic: `a-(b+c)` is `a-b-c`, which `(a-b)+c` is not.
// The second compares text where the other compares numbers, so `'10' <
// '5'` is true where `10 < 5` is false. The third names two DIFFERENT
// columns on PostgreSQL, where a quoted identifier keeps its case and a
// bare one folds to lower.
//
// # Why this is asymmetry-based rather than a ban on a shape
//
// The obvious guard — refuse to compare anything carrying a cast — is
// unusable, and understanding why is what shapes the rest of this file.
// PostgreSQL RENDERS ordinary CHECKs with casts: an authored `status =
// 'a'` reads back as `((status)::text = 'a'::text)`. Absorbing exactly
// that is why the fold exists (Bug 241), so banning the shape would
// report drift on essentially every PG CHECK sluice itself created.
//
// The question asked here is narrower and answerable. The two sides
// agreed only AFTER detail was discarded, so: did they discard the SAME
// detail? Two renderings of one predicate by one server always did,
// which is why a PG-vs-PG or MySQL-vs-MySQL comparison cannot trip this.
// Only an ASYMMETRY — one side carrying meaning-bearing text the other
// lacks — makes the equality untrustworthy.
//
// # What this does NOT close, stated rather than implied
//
// The cast collision above is still a collision, and this file does not
// look at casts at all. Separating "a cast that turns a numeric
// comparison into a lexicographic one" from "the cast PG adds to every
// rendering" needs the operand's TYPE, which needs a real expression
// parser rather than these string passes. It is filed as A0909-RC-2-CASTS
// with the measurement, and [TestCheckExprComparable_TheCastCollisionIsStillOpen]
// pins it as a KNOWN-OPEN gap so it cannot be quietly believed closed by
// the existence of this file.
//
// BOOLEAN regrouping — `a AND (b OR c)` against `(a AND b) OR c` — is
// also still open, and is left open ON PURPOSE rather than overlooked.
// It is [canonicalCheckExpr]'s pre-existing named wart, weighed there and
// accepted with its own pin, because `AND` and `OR` are each associative:
// a server that flattens `a AND (b AND c)` back to `a AND b AND c` when
// it re-renders would make an AND-preceded group asymmetric on a pair
// that means the same thing, which is Bug 241's phantom drift returning.
// `-` and `/` carry no such risk — neither is associative, so a group
// after one ALWAYS changes the reading, which is why they are the two
// this file grades.

// sameCheckPredicate reports whether two CHECK expressions, as the two
// catalogs rendered them, are the same predicate.
//
// It is the single door for that question: [canonicalCheckExpr] equality
// (with "" un-matchable, since an expression that folded to nothing tells
// us nothing) AND [checkExprComparable], which withholds the equality
// when it rests on discarded detail that differed between the two sides.
func sameCheckPredicate(a, b string) bool {
	ca := canonicalCheckExpr(a)
	if ca == "" || ca != canonicalCheckExpr(b) {
		return false
	}
	return checkExprComparable(a, b)
}

// checkExprComparable reports whether an equality between two RAW CHECK
// expressions — the catalog text, before folding — can be trusted to mean
// "the same predicate".
//
// It is consulted only once the canonical forms already match, and it can
// only ever WITHHOLD an equality: it never makes two differing canonical
// forms compare equal, so it cannot hide drift, only report drift that is
// not there. Both dimensions it grades are therefore chosen to be
// symmetric under one server's own rendering.
func checkExprComparable(a, b string) bool {
	if caseBearingQuotedIdents(a) != caseBearingQuotedIdents(b) {
		return false
	}
	return reassociatingGroups(a) == reassociatingGroups(b)
}

// caseBearingQuotedIdents renders the sorted multiset of quoted
// identifiers whose QUOTING IS MEANING-BEARING: those whose text is not
// already its own lowercase form. `"amount"` and bare `amount` are one
// column on every engine sluice speaks, so a lowercase quoted identifier
// is not counted and a target that dropped redundant quotes does not
// report drift.
//
// Both quote styles count, and that is load-bearing rather than
// defensive: MySQL renders `CHECK_CLAUSE` with backticks where PostgreSQL
// uses double quotes, so grading only the PG style would make every
// cross-engine pair asymmetric by construction and report phantom drift
// on a target `migrate` had just created correctly. Text inside a
// single-quoted literal is a VALUE, not an identifier, and is skipped.
func caseBearingQuotedIdents(expr string) string {
	var out []string
	for i := 0; i < len(expr); {
		switch expr[i] {
		case '\'':
			i = rawLiteralEnd(expr, i)
		case '"', '`':
			ident, next, ok := quotedIdentAt(expr, i)
			if !ok {
				// Unterminated. Fold nothing rather than guess at where
				// the identifier ended; an unparseable side grades as
				// whatever it has produced so far on both sides alike.
				return strings.Join(out, "\x1f")
			}
			if ident != strings.ToLower(ident) {
				out = append(out, ident)
			}
			i = next
		default:
			i++
		}
	}
	sort.Strings(out)
	return strings.Join(out, "\x1f")
}

// quotedIdentAt decodes the quoted identifier opening at i, honouring the
// doubled-quote escape, in both spellings — a doubled double-quote inside
// a double-quoted identifier, a doubled backtick inside a backticked one —
// and returns it with the index just past its closing quote. ok is false
// if the identifier is unterminated.
func quotedIdentAt(expr string, i int) (ident string, next int, ok bool) {
	q := expr[i]
	var b strings.Builder
	for j := i + 1; j < len(expr); {
		if expr[j] != q {
			b.WriteByte(expr[j])
			j++
			continue
		}
		if j+1 < len(expr) && expr[j+1] == q {
			b.WriteByte(q)
			j += 2
			continue
		}
		return b.String(), j + 1, true
	}
	return "", 0, false
}

// reassociatingGroups counts the parentheses sitting in a position where
// removing them RE-ASSOCIATES the expression, which is exactly what
// [stripGrouping] does to both sides: a group opening immediately after a
// `-` or a `/`, since `a-(b+c)` is not `a-b+c` and `a/(b*c)` is not
// `a/b*c`. A count per operator is enough — this is asked only of two
// expressions that already fold identically, so any difference in these
// positions means one side re-associated the other.
//
// A group after `+`, `*`, `AND`, `OR` or a function's open paren is NOT
// counted: dropping those preserves meaning, and counting them would make
// a server's own redundant parentheses look like drift.
//
// Text inside a single-quoted literal is skipped; a quoted identifier
// cannot contain a bare parenthesis position that matters here, since the
// byte before `(` would be the closing quote.
func reassociatingGroups(expr string) string {
	minus, div := 0, 0
	for i := 0; i < len(expr); {
		if expr[i] == '\'' {
			i = rawLiteralEnd(expr, i)
			continue
		}
		if expr[i] == '(' {
			if op, at, ok := precedingOperator(expr, i); ok && isBinaryOperatorAt(expr, at) {
				switch op {
				case '-':
					minus++
				case '/':
					div++
				}
			}
		}
		i++
	}
	return strconv.Itoa(minus) + ":" + strconv.Itoa(div)
}

// precedingOperator returns the last non-whitespace byte before i and its
// index. ok is false when i is at, or preceded only by, the start of the
// expression.
func precedingOperator(s string, i int) (op byte, at int, ok bool) {
	for j := i - 1; j >= 0; j-- {
		switch s[j] {
		case ' ', '\t', '\n', '\r':
		default:
			return s[j], j, true
		}
	}
	return 0, 0, false
}

// isBinaryOperatorAt reports whether the operator at index at is BINARY
// rather than unary, by asking whether anything that could end an operand
// precedes it.
//
// # This is the fix for a real phantom-drift regression, so it is worth
// # being precise about
//
// `reassociatingGroups` counts a group that re-associates its expression,
// and the first cut keyed on "the byte before the `(` is a `-`". That
// catches `a-(b+c)`, which is the point. It ALSO catches `-(100000)` —
// and a unary minus applied to a parenthesised literal means exactly what
// it means without the parentheses, so counting it is wrong.
//
// It was not hypothetical. `TestSchemaDiffAfterMigrate_MySQLToPostgres`
// went red on CI with six CHECK mismatches against a target `migrate`
// had just created, every one of the shape
//
//	expected  (ceiling(d) >= -(100000))
//	actual    (ceiling(d) >= ('-100000'::integer)::numeric)
//
// where MySQL's rendering parenthesises the negative literal and
// PostgreSQL's does not. One side counted a group, the other did not,
// they disagreed, and the comparison reported drift on a clean target —
// which is Bug 241, the exact defect the fold this guard sits on top of
// was built to fix, reintroduced by the guard.
//
// So: an operator is binary only when something that can END an operand
// sits before it — an identifier or number character, a closing
// parenthesis or bracket, or a closing quote. After another operator, an
// opening parenthesis, a comma, or the start of the expression, it is
// unary and the group it precedes changes nothing.
func isBinaryOperatorAt(s string, at int) bool {
	prev, _, ok := precedingOperator(s, at)
	if !ok {
		return false // start of expression: unary
	}
	switch {
	case prev >= 'a' && prev <= 'z', prev >= 'A' && prev <= 'Z', prev >= '0' && prev <= '9':
		return true
	case prev == '_' || prev == '$':
		return true
	case prev == ')' || prev == ']':
		return true
	case prev == '"' || prev == '`' || prev == '\'':
		return true
	default:
		return false
	}
}
