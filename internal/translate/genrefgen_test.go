// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// genCol builds a mysql-dialect generated column.
func genCol(name, expr string) *ir.Column {
	return &ir.Column{
		Name: name, Type: ir.Integer{Width: 32},
		GeneratedExpr: expr, GeneratedExprDialect: "mysql",
	}
}

// plainCol builds an ordinary (non-generated) column.
func plainCol(name string) *ir.Column {
	return &ir.Column{Name: name, Type: ir.Integer{Width: 32}}
}

func schemaCols(cols ...*ir.Column) *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{Name: "t", Columns: cols}}}
}

// TestGeneratedColRefGeneratedCol_Detect pins catalog Bug 9: a
// generated column whose expression references ANOTHER generated
// column in the same table must loud-refuse at BOTH preview and
// migrate (MySQL permits it; PG rejects with 42P17 mid-create-tables).
func TestGeneratedColRefGeneratedCol_Detect(t *testing.T) {
	// col_58 references generated col_18 (the exact BUG-CATALOG shape).
	sch := schemaCols(
		plainCol("col_10"), plainCol("col_14"), plainCol("col_39"),
		genCol("col_18", "(col_10 - col_14)"),
		genCol("col_58", "(col_18 + col_39)"),
	)
	hits := ScanGeneratedColRefGeneratedCol(sch, "mysql", "postgres")
	if len(hits) != 1 {
		t.Fatalf("want 1 hit; got %d: %+v", len(hits), hits)
	}
	if hits[0].Column != "col_58" || hits[0].ReferencedColumn != "col_18" {
		t.Fatalf("hit = %+v; want col_58 -> col_18", hits[0])
	}
	for _, ctxID := range []string{"schema preview", "migrate"} {
		err := RefuseOnGeneratedColRefGeneratedCol(sch, "mysql", "postgres", ctxID)
		if err == nil {
			t.Fatalf("%s: must loud-refuse gen-col-ref-gen-col (Bug 9)", ctxID)
		}
		msg := err.Error()
		for _, want := range []string{"col_58", "col_18", "42P17", "--expr-override", "t"} {
			if !strings.Contains(msg, want) {
				t.Errorf("%s: refusal missing %q; got: %s", ctxID, want, msg)
			}
		}
		if ctxID == "migrate" && !strings.Contains(msg, "partially creating the target") {
			t.Errorf("migrate: refusal should warn about partial target; got: %s", msg)
		}
	}
}

// TestGeneratedColRefGeneratedCol_NoFalsePositives pins the
// conservative scope: a gen col referencing only ordinary columns is
// fine; a gen-col name inside a string literal or used as a function
// call is NOT a reference; substrings don't match; non-MySQL→PG pairs
// and nil schema short-circuit.
func TestGeneratedColRefGeneratedCol_NoFalsePositives(t *testing.T) {
	cases := []struct {
		name string
		sch  *ir.Schema
	}{
		{"gen refs only plain cols", schemaCols(
			plainCol("a"), plainCol("b"),
			genCol("g1", "(a + b)"), genCol("g2", "(a - b)"),
		)},
		{"gen-col name only inside string literal", schemaCols(
			genCol("status", "1"),
			genCol("note", "CONCAT('status', x)"),
			plainCol("x"),
		)},
		{"gen-col name only as a function call", schemaCols(
			genCol("length", "1"),
			genCol("n", "length(payload)"),
			plainCol("payload"),
		)},
		{"substring is not a whole-token match", schemaCols(
			genCol("col_1", "1"),
			genCol("col_18", "(col_180 + 1)"),
			plainCol("col_180"),
		)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if hits := ScanGeneratedColRefGeneratedCol(c.sch, "mysql", "postgres"); len(hits) != 0 {
				t.Fatalf("FALSE POSITIVE: %+v", hits)
			}
			if err := RefuseOnGeneratedColRefGeneratedCol(c.sch, "mysql", "postgres", "migrate"); err != nil {
				t.Fatalf("FALSE POSITIVE refusal: %s", err.Error())
			}
		})
	}
	// Scoping: non-cross-engine / nil short-circuit (mirrors gaps).
	pos := schemaCols(genCol("g1", "1"), genCol("g2", "(g1 + 1)"))
	for _, p := range [][2]string{{"postgres", "mysql"}, {"mysql", "mysql"}, {"postgres", "postgres"}} {
		if err := RefuseOnGeneratedColRefGeneratedCol(pos, p[0], p[1], "migrate"); err != nil {
			t.Errorf("%s→%s must not be gated; got: %s", p[0], p[1], err.Error())
		}
	}
	if ScanGeneratedColRefGeneratedCol(nil, "mysql", "postgres") != nil {
		t.Error("nil schema must return nil")
	}
}
