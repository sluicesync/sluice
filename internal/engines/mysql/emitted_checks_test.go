// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/translate"
)

// TestMaterializedTemporalPrecisionMatchesTheEmitter binds the two
// statements of "what precision does a MySQL target declare for a
// precision-less source temporal" so they cannot drift.
//
// One lives here ([effectiveTemporalPrecision]) and CREATES the column;
// the other lives in internal/translate
// ([translate.MySQLMaterializedTemporalPrecision]) and PREDICTS it for
// the shape comparison. Two facts adjacent to each other is the shape
// CLAUDE.md's premise-naming step warns about — so this drives the real
// emitter rather than comparing the constants, and asserts the property
// that actually matters: the DDL emitted from the materialized IR is
// BYTE-IDENTICAL to the DDL emitted from the bare one. A prediction that
// is off by a digit reports phantom drift on every bare temporal column
// AND makes the ADR-0166 pre-create gate refuse a `migrate` re-run.
func TestMaterializedTemporalPrecisionMatchesTheEmitter(t *testing.T) {
	if got := effectiveTemporalPrecision(0, true); got != translate.MySQLMaterializedTemporalPrecision {
		t.Fatalf("the emitter materializes precision %d and translate predicts %d",
			got, translate.MySQLMaterializedTemporalPrecision)
	}

	// Every temporal family, bare vs materialized, both zone states where
	// the type carries one — the compare-lane rule's own matrix, driven
	// through the DDL.
	for _, tc := range []struct {
		name       string
		bare       ir.Type
		compared   ir.Type
		wantColumn string
	}{
		{"time", ir.Time{PrecisionUnspecified: true}, ir.Time{Precision: 6}, "TIME(6)"},
		{"timetz", ir.Time{PrecisionUnspecified: true, WithTimeZone: true}, ir.Time{Precision: 6}, "TIME(6)"},
		{"timetz(3)", ir.Time{Precision: 3, WithTimeZone: true}, ir.Time{Precision: 3}, "TIME(3)"},
		{"datetime", ir.DateTime{PrecisionUnspecified: true}, ir.DateTime{Precision: 6}, "DATETIME(6)"},
		{
			"timestamptz",
			ir.Timestamp{PrecisionUnspecified: true, WithTimeZone: true},
			ir.Timestamp{Precision: 6, WithTimeZone: true},
			"TIMESTAMP(6)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bareDDL, err := emitColumnType(tc.bare)
			if err != nil {
				t.Fatalf("emitColumnType(%s): %v", tc.bare, err)
			}
			comparedDDL, err := emitColumnType(tc.compared)
			if err != nil {
				t.Fatalf("emitColumnType(%s): %v", tc.compared, err)
			}
			if bareDDL != tc.wantColumn {
				t.Fatalf("emitColumnType(%s) = %q; want %q", tc.bare, bareDDL, tc.wantColumn)
			}
			if comparedDDL != bareDDL {
				t.Fatalf("the compare-lane prediction %s emits %q but the source shape %s emits %q — the "+
					"expected side is predicting a column the writer does not create",
					tc.compared, comparedDDL, tc.bare, bareDDL)
			}
		})
	}
}

// TestMaterializedDecimalMatchesTheEmitter binds the compare-lane
// prediction for an unconstrained numeric
// ([translate.MySQLUnconstrainedDecimalPrecision] / Scale) to the DDL this
// emitter actually writes, the way TestMaterializedTemporalPrecisionMatchesTheEmitter
// binds the temporal one: the materialized IR must emit byte-identical DDL
// to the bare one. Found by the GC-44 anti-phantom family matrix.
func TestMaterializedDecimalMatchesTheEmitter(t *testing.T) {
	bare, err := emitColumnType(ir.Decimal{Unconstrained: true})
	if err != nil {
		t.Fatalf("emit bare: %v", err)
	}
	predicted, err := emitColumnType(ir.Decimal{
		Precision: translate.MySQLUnconstrainedDecimalPrecision,
		Scale:     translate.MySQLUnconstrainedDecimalScale,
	})
	if err != nil {
		t.Fatalf("emit predicted: %v", err)
	}
	if bare != predicted {
		t.Fatalf("the compare-lane prediction emits %q but an unconstrained numeric emits %q", predicted, bare)
	}
}

// TestMaterializedNetworkTypesMatchTheEmitter binds the compare lane's
// prediction for UUID and the network family on a MySQL-family target (any
// source — a MariaDB source's native UUID / INET4 / INET6 included) to the
// DDL this emitter writes, every member of the family.
func TestMaterializedNetworkTypesMatchTheEmitter(t *testing.T) {
	for _, src := range []ir.Type{
		ir.UUID{},
		ir.Inet{},
		ir.Inet{Family: ir.InetFamilyAny},
		ir.Cidr{},
		ir.Macaddr{Width: ir.MacaddrEUI48},
		ir.Macaddr{Width: ir.MacaddrEUI64},
	} {
		predicted := translate.RetargetForShapeCompare(&ir.Schema{Tables: []*ir.Table{{
			Name: "t", Columns: []*ir.Column{{Name: "c", Type: src}},
		}}}, "mariadb", "mysql").Tables[0].Columns[0].Type
		if predicted == src {
			// The catalog reads back the text form, never the source type;
			// a prediction left at the source type is the phantom itself.
			t.Errorf("%s: the compare lane did not materialize the writer's text form", src)
		}
		bare, err := emitColumnType(src)
		if err != nil {
			t.Fatalf("emit %s: %v", src, err)
		}
		got, err := emitColumnType(predicted)
		if err != nil {
			t.Fatalf("emit predicted %s: %v", predicted, err)
		}
		if got != bare {
			t.Errorf("%s: the compare lane predicts %s (%q) but the writer emits %q", src, predicted, got, bare)
		}
	}
}

// TestPredictEmittedChecks_DomainMatrix pins what the MySQL writer says
// it will synthesize, over BOTH translatable DOMAIN CHECK shapes plus
// the un-translatable control.
//
// Both shapes, not one: the regex arm and the range arm are separate
// translator arms producing separate clauses, and a prediction defect
// could plausibly reach one and not the other (the same reason the
// item-156 integration fixture carries both).
func TestPredictEmittedChecks_DomainMatrix(t *testing.T) {
	w := &SchemaWriter{emitter: newMySQLEmitter(nil), inlineCheckSupported: true}
	table := &ir.Table{
		Name: "dcheck",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Domain{
				Name:     "dc_email",
				BaseType: ir.Text{Size: ir.TextLong},
				Checks:   []ir.DomainCheck{{Name: "dc_email_check", Body: `VALUE ~ '^[a-z]+$'::text`}},
			}},
			{Name: "pct", Type: ir.Domain{
				Name:     "dc_pct",
				BaseType: ir.Integer{Width: 32},
				Checks:   []ir.DomainCheck{{Name: "dc_pct_check", Body: `VALUE >= 0 AND VALUE <= 100`}},
			}},
			// The control: an un-translatable body is DROPPED by the
			// emitter (with the v0.96.2 WARN), so the target genuinely
			// does not hold it and predicting it would invent a
			// permanent missing-on-target line.
			{Name: "optout", Type: ir.Domain{
				Name:     "dc_optout",
				BaseType: ir.Text{Size: ir.TextLong},
				Checks:   []ir.DomainCheck{{Name: "dc_optout_check", Body: `length(VALUE) > 5`}},
			}},
		},
	}

	got := w.PredictEmittedChecks(table)
	if len(got) != 2 {
		t.Fatalf("predicted %d check(s), want 2 (the regex and range domains; the un-translatable one is "+
			"dropped by the emitter): %+v", len(got), got)
	}
	want := map[string]string{
		"dcheck_email_domain_chk": "REGEXP_LIKE(`email`, '^[a-z]+$')",
		"dcheck_pct_domain_chk":   "`pct` >= 0 AND `pct` <= 100",
	}
	for _, c := range got {
		if !c.SluiceEmitted {
			t.Errorf("predicted check %q is not marked SluiceEmitted; the diff would then match it by NAME "+
				"against MySQL's positional <table>_chk_N, which no expected side can predict", c.Name)
		}
		wantExpr, ok := want[c.Name]
		if !ok {
			t.Errorf("unexpected predicted check %q => %q", c.Name, c.Expr)
			continue
		}
		if c.Expr != wantExpr {
			t.Errorf("predicted %q => %q; want %q", c.Name, c.Expr, wantExpr)
		}
		delete(want, c.Name)
	}
	for name := range want {
		t.Errorf("predicted check %q missing", name)
	}
}

// TestPredictEmittedChecks_HonoursTheServerVersionProbe is the
// unclosable-drift guard. On a server below 8.0.16 the emitter inlines
// NOTHING (older MySQL parsed and ignored CHECK, which would have
// re-introduced the Bug 113 silent-loss class), so a prediction there
// would put a permanent "missing on target" line in front of an operator
// with no action able to close it — the exact shape the row-level-security
// suppression exists to avoid.
//
// The zero value of inlineCheckSupported is false, which is also the
// probe-failed state, so this is the branch every unprobed construction
// takes.
func TestPredictEmittedChecks_HonoursTheServerVersionProbe(t *testing.T) {
	table := &ir.Table{
		Name: "dcheck",
		Columns: []*ir.Column{{Name: "email", Type: ir.Domain{
			Name:     "dc_email",
			BaseType: ir.Text{Size: ir.TextLong},
			Checks:   []ir.DomainCheck{{Name: "dc_email_check", Body: `VALUE ~ '^[a-z]+$'::text`}},
		}}},
	}
	old := &SchemaWriter{emitter: newMySQLEmitter(nil), inlineCheckSupported: false}
	if got := old.PredictEmittedChecks(table); len(got) != 0 {
		t.Fatalf("predicted %d check(s) against a pre-8.0.16 target that receives none: %+v", len(got), got)
	}
	// The control, so a version-gate that simply returned nil always
	// would fail here rather than read as correct.
	modern := &SchemaWriter{emitter: newMySQLEmitter(nil), inlineCheckSupported: true}
	if got := modern.PredictEmittedChecks(table); len(got) != 1 {
		t.Fatalf("predicted %d check(s) against an 8.0.16+ target; want 1", len(got))
	}
}

// TestRegexDomainCheckApostropheIsNotDoubleEscaped pins the escaping
// half of the PG→MySQL DOMAIN regex translation.
//
// PG renders a DOMAIN CHECK body through pg_get_constraintdef, so an
// apostrophe inside the pattern arrives DOUBLED — it is inside a SQL
// string literal. Re-quoting that for MySQL without un-doubling first
// compounds the escaping and lands a regex that requires two
// apostrophes where the source required one, so the target REFUSES a row
// PostgreSQL accepts. Shipped that way from v0.97.0; found 2026-08-10 by
// the item-156 comparison fixture, which is what a comparison surface is
// for.
//
// The backslash case is the control and must NOT be un-escaped: PG runs
// with standard_conforming_strings on, so a backslash in the body is
// already a literal backslash, and [mysqlEmitter.quoteSQLString] doubling
// it for MySQL is correct (the v0.97.1 fix).
func TestRegexDomainCheckApostropheIsNotDoubleEscaped(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{
			"an apostrophe in the pattern survives as ONE apostrophe",
			`VALUE ~ '^o''brien$'::text`,
			"REGEXP_LIKE(`n`, '^o''brien$')",
		},
		{
			"a backslash escape is still doubled for MySQL",
			`VALUE ~ '^[^@]+@[^@]+\.[^@]+$'::text`,
			"REGEXP_LIKE(`n`, '^[^@]+@[^@]+\\\\.[^@]+$')",
		},
		{
			"a pattern with neither passes through",
			`VALUE ~ '^[a-z]+$'::text`,
			"REGEXP_LIKE(`n`, '^[a-z]+$')",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := stdEmitter.domainCheckBodyForMySQL("n", ir.DomainCheck{Body: tc.body})
			if !ok {
				t.Fatalf("translator rejected %q", tc.body)
			}
			if got != tc.want {
				t.Fatalf("translated %q to\n  %s\nwant\n  %s\n\nA target CHECK that enforces a DIFFERENT "+
					"predicate than the source DOMAIN is the Bug 113 shape this translator exists to avoid",
					tc.body, got, tc.want)
			}
		})
	}
}

// TestDomainCheckBodyIsTheEmittedClauseWithoutTheWrapper binds the
// prediction's text to the emitter's: the body the predictor publishes
// must be exactly what the inline `CHECK (...)` clause carries, or the
// comparison is grading a predicate the writer never wrote.
func TestDomainCheckBodyIsTheEmittedClauseWithoutTheWrapper(t *testing.T) {
	for _, body := range []string{
		`VALUE ~ '^[a-z]+$'::text`,
		`VALUE >= 0 AND VALUE <= 100`,
	} {
		chk := ir.DomainCheck{Body: body}
		clause, ok := translateDomainCheckToMySQL("c", chk)
		if !ok {
			t.Fatalf("translator rejected %q", body)
		}
		predicted, ok := stdEmitter.domainCheckBodyForMySQL("c", chk)
		if !ok {
			t.Fatalf("body producer rejected %q", body)
		}
		if want := "CHECK (" + predicted + ")"; clause != want {
			t.Errorf("emitted clause %q is not the predicted body wrapped: %q", clause, want)
		}
	}
}
