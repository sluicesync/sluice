// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestLowerUpperLiteralInGenerated_Detect pins catalog Bug 20's
// residual: a GENERATED column whose body applies LOWER()/UPPER() to a
// bare string literal. MySQL accepts it (the generated value is the
// constant); on PG every MySQL generated column becomes STORED, and a
// STORED generated column's expression must have a determinable
// collation — `lower('ABC')` (even cast `lower('ABC'::text)`) does not,
// so PG rejects the CREATE TABLE with 42P22 mid-create-tables. The
// `::text` translator rewrite rescues the CHECK/DEFAULT positions but
// NOT a STORED generated column; that case must loud-refuse up front.
func TestLowerUpperLiteralInGenerated_Detect(t *testing.T) {
	mk := func(expr string) *ir.Schema {
		return &ir.Schema{Tables: []*ir.Table{{
			Name: "t",
			Columns: []*ir.Column{{
				Name: "lc", Type: ir.Varchar{Length: 20},
				GeneratedExpr: expr, GeneratedExprDialect: "mysql",
			}},
		}}}
	}
	for _, expr := range []string{
		"LOWER('ABC')",
		"lower('abc')",
		"UPPER('xy')",
		"CONCAT(LOWER('A'), x)", // nested still triggers
	} {
		sch := mk(expr)
		hits := ScanLowerUpperLiteralInGenerated(sch, "mysql", "postgres")
		if len(hits) != 1 {
			t.Fatalf("%q: want 1 hit; got %d", expr, len(hits))
		}
		for _, ctxID := range []string{"schema preview", "migrate"} {
			err := RefuseOnLowerUpperLiteralInGenerated(sch, "mysql", "postgres", ctxID)
			if err == nil {
				t.Fatalf("%s: %q must loud-refuse (Bug 20 STORED-gen-col)", ctxID, expr)
			}
			msg := err.Error()
			for _, want := range []string{"lc", "42P22", "--expr-override", "t"} {
				if !strings.Contains(msg, want) {
					t.Errorf("%s %q: refusal missing %q; got: %s", ctxID, expr, want, msg)
				}
			}
		}
	}
}

// TestLowerUpperLiteralInGenerated_NoFalsePositives pins the tight
// scope: a column-arg LOWER (collatable, fine), a non-generated
// column, a CHECK constraint (rescued by ::text — NOT this scanner's
// concern), a string literal that merely contains "lower(", and
// non-cross-engine pairs must NOT trip.
func TestLowerUpperLiteralInGenerated_NoFalsePositives(t *testing.T) {
	gen := func(expr string) *ir.Schema {
		return &ir.Schema{Tables: []*ir.Table{{
			Name: "t",
			Columns: []*ir.Column{{
				Name: "g", Type: ir.Varchar{Length: 20},
				GeneratedExpr: expr, GeneratedExprDialect: "mysql",
			}},
		}}}
	}
	noHit := []*ir.Schema{
		gen("LOWER(name)"),          // column arg — collatable, valid
		gen("UPPER(first || last)"), // compound — not a bare literal
		gen("LOWER('a'::text)"),     // already cast — not a bare literal
		gen("note || 'lower(x)'"),   // literal substring, no real call
		{Tables: []*ir.Table{{
			Name: "t", // CHECK is rescued by ::text, not here
			CheckConstraints: []*ir.CheckConstraint{{
				Name: "c", Expr: "LOWER(code) = LOWER('X')", ExprDialect: "mysql",
			}},
		}}},
	}
	for _, sch := range noHit {
		if h := ScanLowerUpperLiteralInGenerated(sch, "mysql", "postgres"); len(h) != 0 {
			t.Fatalf("FALSE POSITIVE: %+v", h)
		}
	}
	pos := gen("LOWER('ABC')")
	for _, p := range [][2]string{{"postgres", "mysql"}, {"mysql", "mysql"}, {"postgres", "postgres"}} {
		if err := RefuseOnLowerUpperLiteralInGenerated(pos, p[0], p[1], "migrate"); err != nil {
			t.Errorf("%s→%s must not be gated; got: %s", p[0], p[1], err.Error())
		}
	}
	if ScanLowerUpperLiteralInGenerated(nil, "mysql", "postgres") != nil {
		t.Error("nil schema must return nil")
	}
}
