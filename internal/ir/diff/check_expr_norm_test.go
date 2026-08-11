// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCanonicalCheckExpr_MeasuredEmittedPairs is the derivation, pinned:
// every (emitted, read-back) pair below was MEASURED against a real
// PostgreSQL 16 / MySQL 8.0 target after `sluice migrate`, not reasoned
// from what a canonicalizer "should" do.
//
// All four sluice-emitted shapes, because the canonicalizer is a
// family-dispatched surface in exactly the Bug-74 sense — the noise a
// catalog adds differs per engine AND per constraint shape, and a
// normalizer green on the SET check says nothing about the DOMAIN range
// one.
func TestCanonicalCheckExpr_MeasuredEmittedPairs(t *testing.T) {
	for _, tc := range []struct {
		name    string
		emitted string
		readsAs string
	}{
		{
			"postgres SET membership",
			`"flags" <@ ARRAY['email','sms']::TEXT[]`,
			`(flags <@ ARRAY['email'::text, 'sms'::text])`,
		},
		{
			"postgres SET membership, empty member list",
			`"flags" <@ '{}'::TEXT[]`,
			`(flags <@ '{}'::text[])`,
		},
		{
			// PG rewrites IN (...) into = ANY (ARRAY[...]) on the way in
			// and never renders it back. This is the one pair that needs
			// a structural rewrite rather than token folding.
			"postgres generated-enum value list",
			`"g_mood" IN ('happy','sad')`,
			`(g_mood = ANY (ARRAY['happy'::text, 'sad'::text]))`,
		},
		{
			// Lowercased function name and no spacing; the backticks the
			// emitter writes are gone because MySQL's SchemaReader strips
			// them (normalizeMySQLExpressionText), which is also where
			// the charset introducer and the backslash-escaped delimiters
			// go.
			"mysql DOMAIN regex",
			"REGEXP_LIKE(`email`, '^[a-z]+@example[.]com$')",
			`regexp_like(email,'^[a-z]+@example[.]com$')`,
		},
		{
			"mysql DOMAIN range",
			"`pct` >= 0 AND `pct` <= 100",
			"((pct >= 0) and (pct <= 100))",
		},
		{
			// A pattern carrying an apostrophe: the emitter doubles it
			// and MySQL's reader converts its stored `\'` form back to
			// the doubled one, so the two agree here — but the decoder
			// accepts BOTH conventions anyway (see the literal-scanning
			// test), because which one arrives is the reader's choice and
			// not this file's to depend on.
			"mysql DOMAIN regex with an apostrophe in the pattern",
			"REGEXP_LIKE(`n`, 'o''brien')",
			`regexp_like(n,'o''brien')`,
		},
		{
			// DEFENCE IN DEPTH, not a measured pair: MySQL's SchemaReader
			// strips charset introducers today, so this form does not
			// reach here. It is handled because the alternative — a
			// reader change quietly making every DOMAIN regex report
			// drift — is a bad failure mode for a one-line skip.
			"mysql charset introducer, if one ever reaches the comparison",
			"REGEXP_LIKE(`n`, 'x')",
			`regexp_like(n,_utf8mb4'x')`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := canonicalCheckExpr(tc.emitted), canonicalCheckExpr(tc.readsAs)
			if a != b {
				t.Fatalf("the emitted predicate and the catalog's rendering of THE SAME CONSTRAINT do not "+
					"canonicalize equal, so `schema diff` reports it as drift on a target migrate created:\n"+
					"  emitted   %q -> %q\n  reads as  %q -> %q", tc.emitted, a, tc.readsAs, b)
			}
			if a == "" {
				t.Fatalf("both sides canonicalized to the empty string, which matches nothing — this pin would " +
					"pass for the wrong reason")
			}
		})
	}
}

// TestCanonicalCheckExpr_TamperIsStillVisible is the over-suppression
// half, and it is the half that decides whether this is a fix or a
// blindfold. Each case is a change an operator could really make to a
// constraint sluice emitted; every one must still canonicalize
// DIFFERENTLY, or `schema diff` would call a weakened target in sync.
func TestCanonicalCheckExpr_TamperIsStillVisible(t *testing.T) {
	const honestSet = `(flags <@ ARRAY['email'::text, 'sms'::text])`
	const honestRange = "((pct >= 0) and (pct <= 100))"
	for _, tc := range []struct {
		name     string
		honest   string
		tampered string
	}{
		{"a member added to the SET list", honestSet, `(flags <@ ARRAY['email'::text, 'sms'::text, 'fax'::text])`},
		{"a member removed", honestSet, `(flags <@ ARRAY['email'::text])`},
		{"a member's case changed", honestSet, `(flags <@ ARRAY['Email'::text, 'sms'::text])`},
		{"the constrained column swapped", honestSet, `(other <@ ARRAY['email'::text, 'sms'::text])`},
		{"a range bound widened", honestRange, "((pct >= 0) and (pct <= 200))"},
		{"a range bound's operator loosened", honestRange, "((pct >= 0) and (pct < 100))"},
		{"the conjunction turned into a disjunction", honestRange, "((pct >= 0) or (pct <= 100))"},
		{"a bound dropped entirely", honestRange, "(pct >= 0)"},
		{
			"a regex anchor removed",
			`regexp_like(email,'^[a-z]+@example[.]com$')`,
			`regexp_like(email,'[a-z]+@example[.]com$')`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if canonicalCheckExpr(tc.honest) == canonicalCheckExpr(tc.tampered) {
				t.Fatalf("a TAMPERED predicate canonicalizes onto the honest one, so the diff would report the "+
					"target in sync:\n  honest   %q\n  tampered %q", tc.honest, tc.tampered)
			}
		})
	}
}

// TestCanonicalCheckExpr_GroupingOnlyDifferencesAreTheOnlyCollision pins
// the named wart both ways.
//
// The canonicalizer removes every paren, so two predicates differing
// ONLY in grouping collide. That is stated at [canonicalCheckExpr] and
// asserted here rather than left as a claim — and the second half is the
// one that makes it safe: none of the shapes sluice EMITS has a
// grouping-only variant with a different meaning, because three of them
// are single operators and the fourth is a pure conjunction.
func TestCanonicalCheckExpr_GroupingOnlyDifferencesAreTheOnlyCollision(t *testing.T) {
	// The wart, demonstrated. If this ever stops colliding the
	// canonicalizer has grown a parser and the doc must be rewritten.
	if canonicalCheckExpr("a AND (b OR c)") != canonicalCheckExpr("(a AND b) OR c") {
		t.Error("the documented grouping collision no longer happens; [canonicalCheckExpr]'s named wart is stale")
	}
	// The reason it is tolerable: a pure conjunction cannot be regrouped
	// into a different meaning, which is the only boolean structure any
	// emitted shape has.
	if canonicalCheckExpr("((pct >= 0) and (pct <= 100))") != canonicalCheckExpr("pct >= 0 AND pct <= 100") {
		t.Error("the DOMAIN range shape's two renderings must canonicalize equal — that is the collision doing " +
			"its job")
	}
}

// TestCanonicalCheckExpr_LiteralScanning covers the boundary the whole
// thing rests on: a mis-terminated literal would fold the case of
// everything after it and could canonicalize a tampered predicate onto
// an honest one.
func TestCanonicalCheckExpr_LiteralScanning(t *testing.T) {
	for _, tc := range []struct {
		name string
		a    string
		b    string
		same bool
	}{
		{"literal case is preserved", `c IN ('Happy')`, `c IN ('happy')`, false},
		{"literal whitespace is preserved", `c IN ('a b')`, `c IN ('ab')`, false},
		{"literal parens are preserved", `c IN ('(x)')`, `c IN ('x')`, false},
		{"a cast-looking string inside a literal is preserved", `c IN ('a::text')`, `c IN ('a')`, false},
		{"doubled and backslash quote escapes agree", `c IN ('o''brien')`, `c IN ('o\'brien')`, true},
		{"an unterminated literal does not swallow the rest", `c IN ('a`, `c IN ('b`, false},
		{"keyword case outside literals folds", `c IN ('a')`, `c in ('a')`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := canonicalCheckExpr(tc.a) == canonicalCheckExpr(tc.b); got != tc.same {
				t.Fatalf("canonicalCheckExpr(%q)==canonicalCheckExpr(%q) is %v; want %v\n  %q\n  %q",
					tc.a, tc.b, got, tc.same, canonicalCheckExpr(tc.a), canonicalCheckExpr(tc.b))
			}
		})
	}
}

// TestCanonicalCheckExpr_AnyArrayFoldIsBalanced pins the audit GAP 5
// closure in the canonicalizer: the `= ANY (ARRAY[…])` → `IN (…)` fold
// must find the BALANCED close of each term, not a regex's idea of one.
// The greedy predecessor spanned from the FIRST `array[` to the LAST
// `])`, so an expression carrying two ANY terms folded them — and the
// AND joining them — into one fictitious member list; the lazy mirror
// would instead terminate inside a literal VALUE that carries `])`
// verbatim (values are sentinel-wrapped, not escaped, by the time the
// fold runs). Both traps, both directions.
func TestCanonicalCheckExpr_AnyArrayFoldIsBalanced(t *testing.T) {
	for _, tc := range []struct {
		name string
		a    string
		b    string
		same bool
	}{
		{
			"two ANY terms fold independently (the greedy trap)",
			`a = ANY (ARRAY['x','y']) AND b = ANY (ARRAY['p','q'])`,
			`a IN ('x','y') AND b IN ('p','q')`,
			true,
		},
		{
			"two folded terms are not one member list",
			`a = ANY (ARRAY['x','y']) AND b = ANY (ARRAY['p','q'])`,
			`a IN ('x','y','p','q')`,
			false,
		},
		{
			"a literal carrying '])' does not terminate the scan (the lazy trap)",
			`c = ANY (ARRAY['a])b','z'])`,
			`c IN ('a])b','z')`,
			true,
		},
		{
			"a tampered member in the second term is still visible",
			`a = ANY (ARRAY['x']) AND b = ANY (ARRAY['p'])`,
			`a IN ('x') AND b IN ('q')`,
			false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := canonicalCheckExpr(tc.a) == canonicalCheckExpr(tc.b); got != tc.same {
				t.Fatalf("canonicalCheckExpr(%q)==canonicalCheckExpr(%q) is %v; want %v\n  %q\n  %q",
					tc.a, tc.b, got, tc.same, canonicalCheckExpr(tc.a), canonicalCheckExpr(tc.b))
			}
		})
	}
}

// TestDiffChecks_EmittedMatchDoesNotStealANameClaimedConstraint pins the
// other GAP 5 trap, at the matcher: an emitted prediction must only
// consume actual-side constraints NO expected-side check claims by name.
// Pre-fix it walked every actual constraint in sorted-name order, so a
// SOURCE-DECLARED constraint whose name round-trips to the target and
// whose predicate happens to equal an emitted one was consumed by the
// expression match — its name-keyed expected twin then compared against
// nothing, and a clean target reported one missing + one extra.
func TestDiffChecks_EmittedMatchDoesNotStealANameClaimedConstraint(t *testing.T) {
	mk := func(name, expr string, sluice bool) *ir.CheckConstraint {
		return &ir.CheckConstraint{Name: name, Expr: expr, SluiceEmitted: sluice}
	}
	table := func(checks ...*ir.CheckConstraint) *ir.Table {
		return &ir.Table{
			Name:             "t",
			Columns:          []*ir.Column{{Name: "c", Type: ir.Text{Size: ir.TextLong}}},
			CheckConstraints: checks,
		}
	}
	// `op_chk` sorts BEFORE `t_chk_1`, so the pre-fix greedy walk paired
	// the emitted prediction with it first — the fixture encodes the
	// collision order deliberately.
	expected := &ir.Schema{Tables: []*ir.Table{table(
		mk("t_c_domain_chk", "`c` > 0", true),
		mk("op_chk", "(c > 0)", false),
	)}}
	actual := &ir.Schema{Tables: []*ir.Table{table(
		mk("op_chk", "(c > 0)", false),
		mk("t_chk_1", "(c > 0)", false),
	)}}
	if d := Schemas(expected, actual, Options{}); d.HasChanges() {
		t.Fatalf("a clean target reported drift: the emitted-expression match consumed a constraint the "+
			"name-keyed pass owns, orphaning its expected twin: %s %+v", d.Summary(), d.TablesMismatched)
	}
}

// TestDiffChecks_EmittedMatchStillFiresWhenTheNameRoundTrips pins the
// half the GAP 5 filter's first cut broke (caught by CI's MySQL→PG
// reconciliation pin, 2026-08-11): the POSTGRES writer names its
// synthesized checks predictably, so on a PG target the emitted expected
// check and the actual constraint share a NAME — and the target
// re-renders the predicate (`IN (…)` reads back as `= ANY (ARRAY[…])`),
// so the raw name-keyed comparison can never match them. The name-claimed
// exclusion must therefore apply only when the claimer is SOURCE-DECLARED;
// an emitted name-claimer still needs the canonical-expression match.
func TestDiffChecks_EmittedMatchStillFiresWhenTheNameRoundTrips(t *testing.T) {
	table := func(checks ...*ir.CheckConstraint) *ir.Table {
		return &ir.Table{
			Name:             "prefs",
			Columns:          []*ir.Column{{Name: "g_mood", Type: ir.Text{Size: ir.TextLong}}},
			CheckConstraints: checks,
		}
	}
	expected := &ir.Schema{Tables: []*ir.Table{table(
		&ir.CheckConstraint{
			Name:          "prefs_g_mood_enum_chk",
			Expr:          `"g_mood" IN ('happy','sad')`,
			SluiceEmitted: true,
		},
	)}}
	// The PG target's own rendering of the same predicate, under the SAME
	// name — the exact shape a MySQL→PG migrate leaves behind.
	actual := &ir.Schema{Tables: []*ir.Table{table(
		&ir.CheckConstraint{
			Name: "prefs_g_mood_enum_chk",
			Expr: `(g_mood = ANY (ARRAY['happy'::text, 'sad'::text]))`,
		},
	)}}
	if d := Schemas(expected, actual, Options{}); d.HasChanges() {
		t.Fatalf("a PG target migrate just created reported drift: the name-claimed exclusion is eating the "+
			"emitted prediction's own name-twin, which only the expression match can reconcile: %s %+v",
			d.Summary(), d.TablesMismatched)
	}
}

// TestDiffChecks_EmittedChecksAreMatchedByExpressionNotName is the
// comparison-level half: the same four states an operator can leave a
// sluice-emitted constraint in, graded through [Schemas].
//
// The MySQL name asymmetry is deliberately built into the fixture — the
// expected side calls it `t_c_domain_chk` and the target calls it
// `t_chk_1` — because a matcher that quietly fell back to name equality
// would pass a same-name fixture and fail every real MySQL target.
func TestDiffChecks_EmittedChecksAreMatchedByExpressionNotName(t *testing.T) {
	table := func(name string, checks ...*ir.CheckConstraint) *ir.Table {
		return &ir.Table{
			Name:             name,
			Columns:          []*ir.Column{{Name: "c", Type: ir.Text{Size: ir.TextLong}}},
			CheckConstraints: checks,
		}
	}
	emitted := func() *ir.CheckConstraint {
		return &ir.CheckConstraint{Name: "t_c_domain_chk", Expr: "REGEXP_LIKE(`c`, '^a+$')", SluiceEmitted: true}
	}
	expected := &ir.Schema{Tables: []*ir.Table{table("t", emitted())}}

	t.Run("the target holds it under the engine's own name", func(t *testing.T) {
		actual := &ir.Schema{Tables: []*ir.Table{table(
			"t",
			&ir.CheckConstraint{Name: "t_chk_1", Expr: `regexp_like(c,_utf8mb4'^a+$')`},
		)}}
		if d := Schemas(expected, actual, Options{}); d.HasChanges() {
			t.Fatalf("reported drift on a constraint sluice itself emitted: %s %+v", d.Summary(), d.TablesMismatched)
		}
	})

	t.Run("the operator DROPPED it", func(t *testing.T) {
		actual := &ir.Schema{Tables: []*ir.Table{table("t")}}
		td := findTable(t, Schemas(expected, actual, Options{}), "t")
		if len(td.ChecksMissing) != 1 || td.ChecksMissing[0] != "t_c_domain_chk" {
			t.Fatalf("a DROPPED sluice-emitted CHECK must still report missing; got %+v", td)
		}
	})

	t.Run("the operator WEAKENED it", func(t *testing.T) {
		actual := &ir.Schema{Tables: []*ir.Table{table(
			"t",
			&ir.CheckConstraint{Name: "t_chk_1", Expr: `regexp_like(c,_utf8mb4'^a*$')`},
		)}}
		td := findTable(t, Schemas(expected, actual, Options{}), "t")
		if len(td.ChecksMissing) != 1 || len(td.ChecksExtra) != 1 {
			t.Fatalf("a TAMPERED sluice-emitted CHECK must report as both missing (what sluice would emit) and "+
				"extra (what is there); got missing=%v extra=%v", td.ChecksMissing, td.ChecksExtra)
		}
	})

	t.Run("a source-declared check is NOT expression-matched", func(t *testing.T) {
		// Same predicate, no SluiceEmitted marker: this is an ordinary
		// constraint and the name is its identity, so a rename is drift.
		exp := &ir.Schema{Tables: []*ir.Table{table(
			"t",
			&ir.CheckConstraint{Name: "named_by_the_operator", Expr: "REGEXP_LIKE(`c`, '^a+$')"},
		)}}
		actual := &ir.Schema{Tables: []*ir.Table{table(
			"t",
			&ir.CheckConstraint{Name: "renamed_on_the_target", Expr: `regexp_like(c,_utf8mb4'^a+$')`},
		)}}
		td := findTable(t, Schemas(exp, actual, Options{}), "t")
		if len(td.ChecksMissing) != 1 || len(td.ChecksExtra) != 1 {
			t.Fatalf("the expression matcher has leaked onto SOURCE-DECLARED constraints, whose identity is "+
				"their name; got missing=%v extra=%v", td.ChecksMissing, td.ChecksExtra)
		}
	})
}

// TestDiffChecks_EmittedMatchIsOneToOne pins that two emitted checks
// sharing a predicate consume two target-side constraints rather than
// both matching the same one and leaving a phantom extra behind.
func TestDiffChecks_EmittedMatchIsOneToOne(t *testing.T) {
	mk := func(name, expr string, sluice bool) *ir.CheckConstraint {
		return &ir.CheckConstraint{Name: name, Expr: expr, SluiceEmitted: sluice}
	}
	expected := &ir.Schema{Tables: []*ir.Table{{
		Name:    "t",
		Columns: []*ir.Column{{Name: "c", Type: ir.Text{Size: ir.TextLong}}},
		CheckConstraints: []*ir.CheckConstraint{
			mk("t_a_domain_chk", "`c` > 0", true),
			mk("t_b_domain_chk", "`c` > 0", true),
		},
	}}}
	actual := &ir.Schema{Tables: []*ir.Table{{
		Name:    "t",
		Columns: []*ir.Column{{Name: "c", Type: ir.Text{Size: ir.TextLong}}},
		CheckConstraints: []*ir.CheckConstraint{
			mk("t_chk_1", "(c > 0)", false),
			mk("t_chk_2", "(c > 0)", false),
		},
	}}}
	if d := Schemas(expected, actual, Options{}); d.HasChanges() {
		t.Fatalf("two identical emitted predicates did not pair one-to-one with the two the target holds: %s %+v",
			d.Summary(), d.TablesMismatched)
	}
}

func findTable(t *testing.T, d SchemaDiff, name string) TableDiff {
	t.Helper()
	for _, td := range d.TablesMismatched {
		if td.Name == name {
			return td
		}
	}
	t.Fatalf("no table diff for %q; the drift this case exists to grade was not reported at all: %s",
		name, d.Summary())
	return TableDiff{}
}
