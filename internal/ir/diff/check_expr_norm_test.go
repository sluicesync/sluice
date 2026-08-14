// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

import (
	"strings"
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
		{
			// DIFF-1: the pre-fix backslash-escape arm dropped the `\`, so a
			// regex weakened from "digits required" to "any run of d's"
			// canonicalized onto the original and reported IN SYNC.
			"a regex backslash class weakened",
			`(c ~ '^a\d+$')`,
			`(c ~ '^ad+$')`,
		},
		{
			// The emitted-check carrier of the same defect: a REGEXP_LIKE
			// DOMAIN pattern tampered by removing a backslash class.
			"an emitted REGEXP_LIKE pattern backslash removed",
			`regexp_like(c,'^a\d+$')`,
			`regexp_like(c,'^ad+$')`,
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

// TestSchemaDiff_BackslashWeakenedCheckIsReportedAsDrift is the DIFF-1
// pin at the Schemas() level, where the finding OBSERVED the false
// "in sync": a target whose same-named CHECK has been weakened by
// removing a regex backslash class (`^a\d+$` "digits required" → `^ad+$`
// "any run of d's") must report drift, not certify the weakened
// constraint intact. Byte comparison fails on the tamper and the Bug-241
// canonical fallback must NOT fold them equal.
func TestSchemaDiff_BackslashWeakenedCheckIsReportedAsDrift(t *testing.T) {
	table := func(expr string) *ir.Table {
		return &ir.Table{
			Name:             "t",
			Columns:          []*ir.Column{{Name: "c", Type: ir.Text{Size: ir.TextRegular}}},
			CheckConstraints: []*ir.CheckConstraint{{Name: "c_pat", Expr: expr}},
		}
	}
	expected := &ir.Schema{Tables: []*ir.Table{table(`(c ~ '^a\d+$')`)}}
	actual := &ir.Schema{Tables: []*ir.Table{table(`(c ~ '^ad+$')`)}}
	if d := Schemas(expected, actual, Options{}); !d.HasChanges() {
		t.Fatal("a target CHECK weakened by dropping a regex backslash class was reported IN SYNC — " +
			"the drift-detection tool certified a weakened constraint intact (DIFF-1)")
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
	// For the EMITTED shapes the collision is tolerable by construction: a
	// pure conjunction cannot be regrouped into a different meaning, which
	// is the only boolean structure any emitted shape has.
	if canonicalCheckExpr("((pct >= 0) and (pct <= 100))") != canonicalCheckExpr("pct >= 0 AND pct <= 100") {
		t.Error("the DOMAIN range shape's two renderings must canonicalize equal — that is the collision doing " +
			"its job")
	}

	// ACCEPTED WINDOW, pinned as documentation (found by the pre-v0.120.0
	// value-fidelity review): since Bug 241 routed NAME-MATCHED user CHECKs
	// through this canonicalizer, the grouping collapse applies to
	// arbitrary user predicates too — a hand-regroup on the target under
	// the same constraint name reports IN SYNC. Weighed against the
	// alternative (a paren-preserving comparison would re-open Bug 241 for
	// any renderings differing in parenthesization, the commoner case) and
	// accepted; if this pin surprises a future reader, the trade lives in
	// the file header's "named wart" section.
	t.Run("name-matched user CHECKs inherit the grouping window", func(t *testing.T) {
		table := func(expr string) *ir.Table {
			return &ir.Table{
				Name:             "t",
				Columns:          []*ir.Column{{Name: "a", Type: ir.Boolean{}}},
				CheckConstraints: []*ir.CheckConstraint{{Name: "user_chk", Expr: expr}},
			}
		}
		expected := &ir.Schema{Tables: []*ir.Table{table("a AND (b OR c)")}}
		actual := &ir.Schema{Tables: []*ir.Table{table("(a AND b) OR c")}}
		if d := Schemas(expected, actual, Options{}); d.HasChanges() {
			t.Fatal("the documented name-matched grouping window has closed — the comparison grew a parser " +
				"or stopped canonicalizing; rewrite the file header's named-wart section to match")
		}
	})
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
		{"an apostrophe (SQL-standard doubling) is kept in the value", `c IN ('o''brien')`, `c IN ('obrien')`, false},
		// DIFF-1 (audit 2026-08-11): both diff inputs reach this pass in
		// SQL-standard spelling (the IR keeps backslashes BARE and each
		// engine re-escapes only at its own emit boundary — see
		// mysql.escapeExprLiteralBackslashes), so a backslash is a VALUE
		// character. Dropping it, as the pre-fix MySQL-escape arm did, made a
		// weakened regex canonicalize onto the original.
		{"a backslash is a literal value char, not an escape", `c ~ '^a\d+$'`, `c ~ '^ad+$'`, false},
		{"a value ending in a backslash does not swallow the closing quote", `c = 'a\' and x > 1`, `c = 'b\' and x > 1`, false},
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

// TestCanonicalCheckExpr_ServerRenderingFolds pins the Bug 241 fold set
// (operator decision 2026-08-11, option (a) of the memo): the two
// measured server-rendering divergences fold — MySQL's `cast(X as T)`
// against PG's `(X)::T`, and `char_length()` against `length()` — while
// everything that constitutes REAL drift stays visible. The accepted
// false-match window (a hand-edit of ONLY the cast type or only the
// length-function choice) is pinned deliberately as documentation of the
// trade, not discovered later as a surprise.
func TestCanonicalCheckExpr_ServerRenderingFolds(t *testing.T) {
	for _, tc := range []struct {
		name string
		a    string
		b    string
		same bool
	}{
		{
			"the Bug 241 NUMERIC arm: cast form vs :: form",
			`(v > cast(0 as decimal(10,0)))`,
			`(v > (0)::numeric)`,
			true,
		},
		{
			"the Bug 241 VARCHAR arm: char_length vs length",
			`(char_length(v) > 2)`,
			`(length(v) > 2)`,
			true,
		},
		{
			"keyword context glues after whitespace-fold — the reason the pass runs on raw text",
			`(a and char_length(v) > 2)`,
			`(a and length(v) > 2)`,
			true,
		},
		{
			"nested casts fold recursively",
			`cast(cast(a as char) as decimal(10,0))`,
			`a`,
			true,
		},
		{
			"a widened range is still drift",
			`(v > 0)`,
			`(v > -1000)`,
			false,
		},
		{
			"the cast ARGUMENT is preserved, literals included",
			`cast('a' as char)`,
			`cast('b' as char)`,
			false,
		},
		{
			"ACCEPTED WINDOW, pinned as documentation: a cast-type-only edit folds equal",
			`(v > cast(0 as decimal(10,0)))`,
			`(v > cast(0 as unsigned))`,
			true,
		},
		{
			"a column whose name contains 'as' is not mangled",
			`(class > 0)`,
			`(cl > 0)`,
			false,
		},
		{
			"a COLUMN named char_length (no call parens) is not folded",
			`(char_length > 2)`,
			`(length > 2)`,
			false,
		},
		{
			"a malformed cast (no 'as') is left verbatim — spurious difference, never spurious match",
			`(v > cast(0))`,
			`(v > 0)`,
			false,
		},

		// The DIFF-2 fold matrix (measured 2026-08-14). Each "same:true"
		// pair below is a VERBATIM (mysql-rendering, pg-rendering) pair
		// captured from a real MySQL 8 and PostgreSQL 16 of one applied
		// predicate — the measurement is the vector, not a hand guess.
		{
			"DIFF-2 mod: MySQL renders mod(a,b) as (a %% b), PG keeps the call",
			`((v % 3) >= 0)`,
			`(mod(v, 3) >= 0)`,
			true,
		},
		{
			"DIFF-2 between: PG expands to >= AND <=, MySQL preserves between",
			`(v between 1 and 10)`,
			`((v >= 1) AND (v <= 10))`,
			true,
		},
		{
			"DIFF-2 not-pushdown: MySQL simplifies NOT (v = 5) to (v <> 5)",
			`(v <> 5)`,
			`(NOT (v = 5))`,
			true,
		},
		{
			"DIFF-2 like: PG spells LIKE as ~~ with cast decoration",
			`(s like 'a%')`,
			`((s)::text ~~ 'a%'::text)`,
			true,
		},
		{
			"DIFF-2 not like: MySQL wraps prefix-not, PG spells !~~",
			`(not((s like 'z%')))`,
			`((s)::text !~~ 'z%'::text)`,
			true,
		},
		{
			"DIFF-2 substring: MySQL renders substr, PG a quoted \"substring\" call",
			`(substr(s,1,2) <> 'zz')`,
			`("substring"((s)::text, 1, 2) <> 'zz'::text)`,
			true,
		},
		{
			"DIFF-2 trim: PG renders TRIM(BOTH FROM x)",
			`(trim(s) = s)`,
			`(TRIM(BOTH FROM s) = (s)::text)`,
			true,
		},
		{
			"DIFF-2 nullif: PG quotes the negative literal, MySQL parenthesizes the sign",
			`(nullif(v,-(1)) is not null)`,
			`(NULLIF(v, '-1'::integer) IS NOT NULL)`,
			true,
		},
		{
			"DIFF-2 round: MySQL materializes the default scale as round(d,0)",
			`(round(d,0) >= -(100000))`,
			`(round(d) >= ('-100000'::integer)::numeric)`,
			true,
		},
		{
			"DIFF-2 floor: the quoted-negative-literal decoration alone",
			`(floor(d) >= -(100000))`,
			`(floor(d) >= ('-100000'::integer)::numeric)`,
			true,
		},
		{
			"DIFF-2 ceil: MySQL renders ceiling, PG keeps ceil",
			`(ceiling(d) >= -(100000))`,
			`(ceil(d) >= ('-100000'::integer)::numeric)`,
			true,
		},
		{
			"DIFF-2 greatest: the literal decoration on a preserved function",
			`(greatest(v,w) >= -(100000))`,
			`(GREATEST(v, w) >= '-100000'::integer)`,
			true,
		},

		// Guard directions: each fold's MEANING axis still reports.
		{
			"DIFF-2 guard: a different mod divisor is still drift",
			`((v % 3) >= 0)`,
			`(mod(v, 4) >= 0)`,
			false,
		},
		{
			"DIFF-2 guard: a widened between bound is still drift",
			`(v between 1 and 10)`,
			`((v >= 1) AND (v <= 100))`,
			false,
		},
		{
			"DIFF-2 guard: negated vs un-negated comparison is still drift",
			`(v = 5)`,
			`(NOT (v = 5))`,
			false,
		},
		{
			"DIFF-2 guard: a changed LIKE pattern is still drift",
			`(s like 'a%')`,
			`((s)::text ~~ 'b%'::text)`,
			false,
		},
		{
			"DIFF-2 guard: LIKE vs NOT LIKE is still drift",
			`(s like 'z%')`,
			`((s)::text !~~ 'z%'::text)`,
			false,
		},
		{
			"DIFF-2 guard: a NON-zero round scale is meaning and is preserved",
			`(round(d,2) >= 0)`,
			`(round(d) >= 0)`,
			false,
		},
		{
			"Bug 252 DOUBLE arm: PG spells the cast as two-word double precision",
			`(round(d2,0) >= -(100000))`,
			`(round(d2) >= ('-100000'::integer)::double precision)`,
			true,
		},
		{
			"Bug 252 guard: a column NAMED like a multi-word type is not swallowed",
			`(x::int > double_precision)`,
			`(x > 0)`,
			false,
		},
		{
			"Bug 252 equality direction: the two-word cast is fully consumed (the boundary check's positive half)",
			`(x::double precision > 0)`,
			`(x > 0)`,
			true,
		},
		{
			"Bug 252 boundary: an ident byte after the type name falls back to single-word skipping",
			`(x::double precisionx > 0)`,
			`(x > 0)`,
			false,
		},
		{
			"DIFF-2 guard: different numeric literals do not collapse",
			`(v >= -100000)`,
			`(v >= '-100001'::integer)`,
			false,
		},
		{
			"DIFF-2 guard: a NON-numeric string literal keeps its literal identity",
			`(s <> 'abc')`,
			`(s <> abc)`,
			false,
		},
		{
			"DIFF-2 guard: not over a conjunction is NOT pushed down (and differs from the pushed form)",
			`(not(v = 5 and w = 6))`,
			`(v <> 5 and w <> 6)`,
			false,
		},
		{
			"DIFF-2 guard: NOT BETWEEN is not rewritten as BETWEEN's expansion",
			`(v not between 1 and 10)`,
			`(v >= 1 and v <= 10)`,
			false,
		},
		{
			"DIFF-2 guard: a compound BETWEEN operand is left verbatim, not misfolded",
			`(v + w between 1 and 10)`,
			`(w >= 1 and w <= 10)`,
			false,
		},
		{
			"DIFF-2 guard: ILIKE's ~~* spelling is NOT folded onto LIKE",
			`(s like 'a%')`,
			`((s)::text ~~* 'a%'::text)`,
			false,
		},
		{
			"DIFF-2 window, pinned as documentation: string-vs-number literal spelling collapses",
			`(v = 123)`,
			`(v = '123')`,
			true,
		},
		{
			"DIFF-2 premise bail: a comparison after a not-group is ungrouped precedence, left verbatim",
			`(not(v = 5) = w)`,
			`((v <> 5) = w)`,
			false,
		},
		{
			"DIFF-2 premise bail: a bare not before BETWEEN is NOT BETWEEN, left verbatim",
			`(not v between 1 and 2)`,
			`(v >= 1 and v <= 2)`,
			false,
		},
		{
			"DIFF-2 unquote guard: a hex literal does not merge into a fictitious identifier",
			`(v = x'12')`,
			`(v = x12)`,
			false,
		},
		{
			"DIFF-2 unquote guard: a hex literal stays distinct from the quoted digits",
			`(v = x'12')`,
			`(v = '12')`,
			false,
		},
		{
			"DIFF-2 unquote narrowness: no numeric normalization, only spelling",
			`(v = '0123')`,
			`(v = 123)`,
			false,
		},
		{
			"DIFF-2 trim bail: LEADING FROM is unmeasured and stays distinct from plain trim",
			`(TRIM(LEADING FROM s) = s)`,
			`(trim(s) = s)`,
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

// TestDiffChecks_TranslateExpectedHook pins the DIFF-2 translator-pair
// machinery at the comparison layer: when Options carries the target's
// dialect translation, a name-matched pair whose expected side is a
// SOURCE-dialect spelling of what the target holds compares clean —
// and a genuinely different predicate still reports THROUGH the
// translation. The hook must also respect the ok=false arm (foreign
// dialect → original text compares, today's behavior).
func TestDiffChecks_TranslateExpectedHook(t *testing.T) {
	table := func(expr, dialect string) *ir.Table {
		return &ir.Table{
			Name:    "m",
			Columns: []*ir.Column{{Name: "j", Type: ir.JSON{}}},
			CheckConstraints: []*ir.CheckConstraint{
				{Name: "ck_j", Expr: expr, ExprDialect: dialect},
			},
		}
	}
	// A stand-in for the PG writer's rewrite, shaped like the real one:
	// translates only the "mysql" dialect, rewriting the source spelling
	// to the target's.
	xlat := func(expr, dialect string) (string, bool) {
		if dialect != "mysql" {
			return "", false
		}
		return strings.ReplaceAll(expr, "json_extract(j, '$.k')", "(j -> 'k')"), true
	}
	opts := Options{TranslateExpectedCheckExpr: xlat}

	t.Run("a translated pair compares clean", func(t *testing.T) {
		expected := &ir.Schema{Tables: []*ir.Table{table(`(json_extract(j, '$.k') is not null)`, "mysql")}}
		actual := &ir.Schema{Tables: []*ir.Table{table(`((j -> 'k'::text) IS NOT NULL)`, "")}}
		if d := Schemas(expected, actual, opts); d.HasChanges() {
			t.Fatalf("a translator-pair CHECK phantom-reported through the hook: %+v", d.TablesMismatched)
		}
	})
	t.Run("a genuinely different predicate still reports through translation", func(t *testing.T) {
		expected := &ir.Schema{Tables: []*ir.Table{table(`(json_extract(j, '$.k') is not null)`, "mysql")}}
		actual := &ir.Schema{Tables: []*ir.Table{table(`((j -> 'OTHER'::text) IS NOT NULL)`, "")}}
		td := findTable(t, Schemas(expected, actual, opts), "m")
		if len(td.ChecksMismatched) != 1 {
			t.Fatalf("a genuinely different predicate went unreported under the translate hook — over-folding: %+v", td)
		}
	})
	t.Run("a foreign dialect declines and compares the original", func(t *testing.T) {
		expected := &ir.Schema{Tables: []*ir.Table{table(`(json_extract(j, '$.k') is not null)`, "sqlite")}}
		actual := &ir.Schema{Tables: []*ir.Table{table(`((j -> 'k'::text) IS NOT NULL)`, "")}}
		td := findTable(t, Schemas(expected, actual, opts), "m")
		if len(td.ChecksMismatched) != 1 {
			t.Fatalf("an untranslated foreign-dialect pair compared clean — the ok=false arm is broken: %+v", td)
		}
	})
	t.Run("nil hook preserves today's behavior", func(t *testing.T) {
		expected := &ir.Schema{Tables: []*ir.Table{table(`(json_extract(j, '$.k') is not null)`, "mysql")}}
		actual := &ir.Schema{Tables: []*ir.Table{table(`((j -> 'k'::text) IS NOT NULL)`, "")}}
		td := findTable(t, Schemas(expected, actual, Options{}), "m")
		if len(td.ChecksMismatched) != 1 {
			t.Fatalf("the nil-hook degraded mode changed — expected the historical phantom report: %+v", td)
		}
	})
}

// TestDiffChecks_NameMatchedPairComparesCanonically is the Bug 241 pin at
// the comparison level: a name-matched user CHECK whose two sides are the
// two servers' renderings of one predicate is clean, and one whose
// predicate genuinely changed still reports — under the same name, which
// is exactly where the byte comparison used to phantom.
func TestDiffChecks_NameMatchedPairComparesCanonically(t *testing.T) {
	table := func(expr string) *ir.Table {
		return &ir.Table{
			Name:    "m",
			Columns: []*ir.Column{{Name: "v", Type: ir.Decimal{Precision: 12, Scale: 3}}},
			CheckConstraints: []*ir.CheckConstraint{
				{Name: "ck_v", Expr: expr},
			},
		}
	}
	t.Run("two renderings of one predicate are clean", func(t *testing.T) {
		expected := &ir.Schema{Tables: []*ir.Table{table(`(v > (0)::numeric)`)}}
		actual := &ir.Schema{Tables: []*ir.Table{table(`(v > cast(0 as decimal(10,0)))`)}}
		if d := Schemas(expected, actual, Options{}); d.HasChanges() {
			t.Fatalf("phantom drift on a migrate-created CHECK pair (Bug 241): %s %+v", d.Summary(), d.TablesMismatched)
		}
	})
	t.Run("a genuinely changed predicate under the same name still reports", func(t *testing.T) {
		expected := &ir.Schema{Tables: []*ir.Table{table(`(v > (0)::numeric)`)}}
		actual := &ir.Schema{Tables: []*ir.Table{table(`(v > cast(-1000 as decimal(10,0)))`)}}
		td := findTable(t, Schemas(expected, actual, Options{}), "m")
		if len(td.ChecksMismatched) != 1 {
			t.Fatalf("a WEAKENED name-matched CHECK went unreported — the canonical comparison has over-folded: %+v", td)
		}
	})
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
