// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The Bug 242 pins: MariaDB's information_schema renders `c REGEXP
// '...'` as the INFIX operator — the exact IR spelling below is what the
// MariaDB escaping matrix measures the reader producing — and the
// MySQL→PG untranslatable-expression gate only scanned function-CALL
// idents, so the operator sailed through and died in PostgreSQL's parser
// (SQLSTATE 42601) after earlier tables had been created. MySQL 8
// renders the same source CHECK as regexp_like(), which the gate already
// refused cleanly; this is the flavor-sibling arm.

func bug242Schema(expr string) *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name:    "z_rx",
		Columns: []*ir.Column{{Name: "c", Type: ir.Varchar{Length: 40}}},
		CheckConstraints: []*ir.CheckConstraint{
			{Name: "z_rx_chk", Expr: expr, ExprDialect: "mysql"},
		},
	}}}
}

func TestScanUntranslatable_InfixRegexpOperator(t *testing.T) {
	cases := []struct {
		name string
		expr string
		want string // expected Function token; "" = must NOT flag
	}{
		{"the Bug 242 shape (MariaDB catalog spelling, measured IR)", `c regexp '^o''brien$'`, "infix-regexp"},
		{"uppercase source spelling", `c REGEXP '^x$'`, "infix-regexp"},
		{"NOT REGEXP carries the same operator token", `c not regexp '^x$'`, "infix-regexp"},
		{"rlike synonym", `c rlike '^x$'`, "infix-rlike"},
		{"regexp_like is the CALL form, not this arm", `regexp_like(c,'^x$')`, ""},
		{"the word regexp inside a literal is data", `c <> 'use regexp here'`, ""},
		{"an identifier containing the word does not match", `regexp_flags > 0`, ""},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			hits := ScanUntranslatableMySQLToPGExprs(bug242Schema(tc.expr), "mysql", "postgres", nil)
			switch {
			case tc.want == "" && len(hits) == 0:
				// regexp_like still flags via the function scan; assert
				// only that the INFIX token is absent.
			case tc.want == "":
				for _, h := range hits {
					if strings.HasPrefix(h.Function, "infix-") {
						t.Fatalf("expression %q flagged %q; the infix arm has over-matched", tc.expr, h.Function)
					}
				}
			default:
				found := false
				for _, h := range hits {
					if h.Function == tc.want {
						found = true
					}
				}
				if !found {
					t.Fatalf("expression %q did not flag %s (hits: %+v) — the Bug 242 shape would emit "+
						"invalid PostgreSQL DDL mid-pipeline again", tc.expr, tc.want, hits)
				}
			}
		})
	}
}

// TestRefuseOnUntranslatableExprs_InfixRegexpNamesTheRemedy pins the
// operator-facing refusal: the message must name the operator, the
// MariaDB provenance, and the `~` equivalent — the catalog's complaint
// was a raw SQLSTATE 42601 with none of those.
func TestRefuseOnUntranslatableExprs_InfixRegexpNamesTheRemedy(t *testing.T) {
	err := RefuseOnUntranslatableExprs(
		bug242Schema(`c regexp '^o''brien$'`), "mysql", "postgres", "migrate", nil,
	)
	if err == nil {
		t.Fatal("want the pre-DDL refusal, got nil — Bug 242's shape would die in PG's parser mid-pipeline")
	}
	for _, want := range []string{"REGEXP", "MariaDB", "~ 'pattern'", "--expr-override", "z_rx"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q:\n%s", want, err)
		}
	}
}

// TestRefuseOnLoudGaps_InfixRegexpRendering pins the message operators
// actually SEE. Callers run RefuseOnLoudGaps BEFORE the general
// backstop gate, so for the infix spellings the curated note here is
// the rendered surface — the v0.120.1 release notes claimed the refusal
// names the operator's MariaDB provenance while the shipped message
// carried no such word (it lived only in the shadowed backstop's
// what-switch, corrected in place 2026-08-11). This pin is what makes
// that claim true and keeps it true, and it holds the infix rendering:
// "the infix REGEXP operator", never call-parens on an operator.
func TestRefuseOnLoudGaps_InfixRegexpRendering(t *testing.T) {
	err := RefuseOnLoudGaps(
		bug242Schema(`c regexp '^o''brien$'`), "mysql", "postgres", "migrate", nil,
	)
	if err == nil {
		t.Fatal("want the curated pre-DDL refusal, got nil")
	}
	for _, want := range []string{"the infix REGEXP operator", "MariaDB", "~ 'pattern'", "--expr-override", "z_rx"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("rendered refusal does not mention %q:\n%s", want, err)
		}
	}
	if strings.Contains(err.Error(), "REGEXP()") {
		t.Errorf("call-parens rendered on an operator form:\n%s", err)
	}
}

// TestDetectGaps_InfixOperatorAdvisory pins the schema-preview twin: the
// advisory rides the SAME token scan as the refusal (one scanner, two
// consumers), fires on the operator form, and stays silent for the word
// inside a literal — noise the loud gate never produces.
func TestDetectGaps_InfixOperatorAdvisory(t *testing.T) {
	gaps := detectGaps(`c regexp '^x$'`, nil)
	found := false
	for _, g := range gaps {
		if g.Pattern == "REGEXP" {
			found = true
			if g.Severity != SeverityLoud {
				t.Errorf("REGEXP infix gap severity = %v; want SeverityLoud (PG cannot parse it at all)", g.Severity)
			}
		}
	}
	if !found {
		t.Fatalf("infix REGEXP produced no gap advisory: %+v", gaps)
	}
	for _, g := range detectGaps(`c <> 'use regexp here'`, nil) {
		if g.Pattern == "REGEXP" {
			t.Fatalf("the word regexp inside a literal produced an advisory: %+v", g)
		}
	}
}
