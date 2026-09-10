// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSameCheckPredicate_TheMeasuredRC2CollisionsAreRefused pins audit
// 2026-09-09 RC-2's two CLOSED collisions.
//
// Each case asserts BOTH halves, and the first half is the one that keeps
// this test honest: the canonical forms must still be EQUAL. Without it a
// future fold change could make the pair differ for an unrelated reason
// and the test would keep passing while [checkExprComparable] had stopped
// being the thing catching it — a green from the wrong guard, which is
// the failure shape the mutation-run protocol exists to prevent.
func TestSameCheckPredicate_TheMeasuredRC2CollisionsAreRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		a    string
		b    string
		why  string
	}{
		{
			name: "arithmetic regrouping",
			a:    "a - (b + c) > 0",
			b:    "(a - b) + c > 0",
			why:  "`a-(b+c)` is `a-b-c`; the parens the canonicalizer drops are the whole predicate",
		},
		{
			name: "division regrouping",
			a:    "a / (b * c) > 1",
			b:    "(a / b) * c > 1",
			why:  "`/` is not associative either",
		},
		{
			name: "PG quoted-identifier case",
			a:    `"Amount" > 0`,
			b:    `amount > 0`,
			why:  `on PostgreSQL "Amount" and amount are two different columns`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if canonicalCheckExpr(tc.a) != canonicalCheckExpr(tc.b) {
				t.Fatalf("PREMISE GONE: these no longer canonicalize equal (%q vs %q), so this test is no "+
					"longer exercising checkExprComparable — re-derive the collision before trusting the green",
					canonicalCheckExpr(tc.a), canonicalCheckExpr(tc.b))
			}
			if sameCheckPredicate(tc.a, tc.b) {
				t.Fatalf("%q and %q were certified the same predicate; they are not: %s", tc.a, tc.b, tc.why)
			}
			if sameCheckPredicate(tc.b, tc.a) {
				t.Fatalf("the refusal is not symmetric: %q vs %q", tc.b, tc.a)
			}
		})
	}
}

// TestCheckExprComparable_TheCastCollisionIsStillOpen pins the third RC-2
// collision as KNOWN-OPEN, cited by name from [checkExprComparable]'s
// header.
//
// `x::text < '5'` compares lexicographically where `x < 5` compares
// numerically, so '10' < '5' is true where 10 < 5 is false — a real
// divergence this file does NOT catch. It is not caught because
// PostgreSQL renders ordinary CHECKs with casts (`(status)::text =
// 'a'::text` for an authored `status = 'a'`), so telling a
// meaning-changing cast from a rendering cast needs the operand's type
// and therefore a real parser. Filed as A0909-RC-2-CASTS.
//
// This test EXPECTS the collision. It failing is good news — it means
// something grew the ability to separate the two, and the header's
// "what this does not close" section is then stale and must be rewritten.
func TestCheckExprComparable_TheCastCollisionIsStillOpen(t *testing.T) {
	const withCast, withoutCast = "x::text < '5'", "x < 5"
	if !sameCheckPredicate(withCast, withoutCast) {
		t.Fatalf("the documented cast collision no longer happens (%q vs %q) — A0909-RC-2-CASTS may be "+
			"closed; rewrite checkExprComparable's header rather than deleting this pin",
			canonicalCheckExpr(withCast), canonicalCheckExpr(withoutCast))
	}
}

// TestSameCheckPredicate_OneServersOwnRenderingsStayComparable is the
// no-phantom-drift half, and it is the half that decides whether this
// guard is shippable at all.
//
// [checkExprComparable] can only WITHHOLD an equality, so its entire risk
// is reporting drift on a target that is fine — which is Bug 241, the
// defect the canonicalizer was built to fix. Every pair here folds to the
// same predicate and must keep comparing equal.
func TestSameCheckPredicate_OneServersOwnRenderingsStayComparable(t *testing.T) {
	for _, tc := range []struct {
		name string
		a    string
		b    string
	}{
		{
			name: "PG's fully-parenthesised re-render of a plain comparison",
			a:    "amount > 0",
			b:    "((amount > 0))",
		},
		{
			name: "a group after + is not a regrouping",
			a:    "a + (b + c) > 0",
			b:    "(a + b) + c > 0",
		},
		{
			name: "a group after * is not a regrouping",
			a:    "a * (b * c) > 0",
			b:    "(a * b) * c > 0",
		},
		{
			name: "a function's own open paren is not a group",
			a:    "length(c) - abs(d) > 0",
			b:    "(length(c) - abs(d)) > 0",
		},
		{
			name: "redundant lowercase quoting is not meaning-bearing",
			a:    `"amount" > 0`,
			b:    `amount > 0`,
		},
		{
			name: "MySQL backticks vs PG double quotes on the same case-bearing column",
			a:    "`Amount` > 0",
			b:    `"Amount" > 0`,
		},
		{
			name: "a doubled quote inside an identifier is one quote, not a terminator",
			a:    `"A""B" > 0`,
			b:    `"A""B" > 0`,
		},
		{
			name: "parens and quotes inside a string literal are a value",
			a:    `c = 'a-(b+c) "Amount"'`,
			b:    `(c = 'a-(b+c) "Amount"')`,
		},
		{
			name: "PG's cast rendering of a MySQL string comparison",
			a:    "status = 'a'",
			b:    "(status)::text = 'a'::text",
		},
		{
			name: "the DOMAIN range shape's two renderings",
			a:    "((pct >= 0) and (pct <= 100))",
			b:    "pct >= 0 AND pct <= 100",
		},

		// THE SIX BELOW ARE VERBATIM FROM A RED CI RUN, and they are the
		// reason isBinaryOperatorAt exists.
		//
		// The first cut of reassociatingGroups counted any group whose
		// preceding byte was `-`, which catches `a-(b+c)` — the point —
		// and ALSO catches a UNARY minus on a parenthesised literal,
		// where the parentheses change nothing. MySQL renders a negative
		// literal as `-(100000)` and PostgreSQL as `'-100000'::integer`,
		// so the two sides counted differently and
		// TestSchemaDiffAfterMigrate_MySQLToPostgres reported six CHECK
		// mismatches against a target migrate had just created. That is
		// Bug 241 — phantom drift on a clean target — reintroduced by the
		// guard sitting on top of the fold that fixed it.
		//
		// Kept as real cross-engine renderings rather than reduced to a
		// minimal `-(1)` pair, because the minimal pair is what I would
		// have invented and these are what two servers actually emit.
		{
			name: "unary minus on a parenthesised literal, MySQL vs PG (ceiling)",
			a:    "(ceiling(d) >= -(100000))",
			b:    "(ceiling(d) >= ('-100000'::integer)::numeric)",
		},
		{
			name: "unary minus on a parenthesised literal, MySQL vs PG (floor)",
			a:    "(floor(d) >= -(100000))",
			b:    "(floor(d) >= ('-100000'::integer)::numeric)",
		},
		{
			name: "unary minus inside a multi-argument call",
			a:    "(greatest(v,w) >= -(100000))",
			b:    "(GREATEST(v, w) >= '-100000'::integer)",
		},
		{
			name: "unary minus as a function argument",
			a:    "(nullif(v,-(1)) is not null)",
			b:    "(NULLIF(v, '-1'::integer) IS NOT NULL)",
		},
		{
			name: "unary minus under a pg_catalog qualifier",
			a:    "(pg_catalog.ROUND(d) >= -(100000))",
			b:    "(round(d) >= ('-100000'::integer)::numeric)",
		},
		{
			name: "unary minus with a double-precision cast",
			a:    "(pg_catalog.ROUND(d2) >= -(100000))",
			b:    "(round(d2) >= ('-100000'::integer)::double precision)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !sameCheckPredicate(tc.a, tc.b) {
				t.Fatalf("phantom drift: %q and %q are the same predicate but were reported as differing\n"+
					"  canonical: %q vs %q\n  quoted idents: %q vs %q\n  regrouping: %q vs %q",
					tc.a, tc.b,
					canonicalCheckExpr(tc.a), canonicalCheckExpr(tc.b),
					caseBearingQuotedIdents(tc.a), caseBearingQuotedIdents(tc.b),
					reassociatingGroups(tc.a), reassociatingGroups(tc.b))
			}
		})
	}
}

// TestSameCheckPredicate_ReachesTheDiffReport walks the guard out to the
// surface an operator actually reads. A unit-level refusal that never
// changes what `schema diff` prints would be the pipeline-stub problem:
// proof the function agrees with itself and no evidence about the
// product.
//
// Both call paths through the guard are covered — the NAME-MATCHED pass
// in diffTableChecks and the emitted-prediction pass in
// [matchEmittedChecks] — because a refusal that reaches one and not the
// other is this project's most expensive recurring shape.
func TestSameCheckPredicate_ReachesTheDiffReport(t *testing.T) {
	table := func(c *ir.CheckConstraint) *ir.Table {
		return &ir.Table{
			Name:             "t",
			Columns:          []*ir.Column{{Name: "amount", Type: ir.Integer{}}},
			CheckConstraints: []*ir.CheckConstraint{c},
		}
	}

	t.Run("name-matched: an arithmetic regroup on the target is reported", func(t *testing.T) {
		expected := &ir.Schema{Tables: []*ir.Table{table(&ir.CheckConstraint{Name: "chk", Expr: "a - (b + c) > 0"})}}
		actual := &ir.Schema{Tables: []*ir.Table{table(&ir.CheckConstraint{Name: "chk", Expr: "(a - b) + c > 0"})}}
		if d := Schemas(expected, actual, Options{}); !d.HasChanges() {
			t.Fatal("a target CHECK regrouped from `a-(b+c)` to `(a-b)+c` was reported IN SYNC — the drift " +
				"tool certified a different predicate intact (A0909-RC-2)")
		}
	})

	t.Run("emitted: a case-differing column does not satisfy the prediction", func(t *testing.T) {
		expected := &ir.Schema{Tables: []*ir.Table{table(&ir.CheckConstraint{
			Name: "t_chk_0", Expr: `"Amount" > 0`, SluiceEmitted: true,
		})}}
		actual := &ir.Schema{Tables: []*ir.Table{table(&ir.CheckConstraint{Name: "t_chk_1", Expr: "amount > 0"})}}
		if d := Schemas(expected, actual, Options{}); !d.HasChanges() {
			t.Fatal("an emitted prediction over the quoted column \"Amount\" was satisfied by a constraint " +
				"over the DIFFERENT column amount — the emitted-match pass does not consult the guard")
		}
	})

	t.Run("emitted: a re-rendered prediction still matches", func(t *testing.T) {
		expected := &ir.Schema{Tables: []*ir.Table{table(&ir.CheckConstraint{
			Name: "t_chk_0", Expr: "status = 'a'", SluiceEmitted: true,
		})}}
		actual := &ir.Schema{Tables: []*ir.Table{table(&ir.CheckConstraint{
			Name: "t_chk_1", Expr: "((status)::text = 'a'::text)",
		})}}
		if d := Schemas(expected, actual, Options{}); d.HasChanges() {
			t.Fatalf("phantom drift on a target migrate just created: %+v", d.TablesMismatched)
		}
	})
}
