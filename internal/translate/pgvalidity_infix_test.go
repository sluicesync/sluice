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

		// ARCH-1 (audit 2026-08-11): the sibling operators Bug 242's
		// two-spelling list missed. Every expression below is a measured
		// catalog rendering (mysql:8.0/8.4 + mariadb 10.11/11.4/11.8/12.3).
		{"DIV renders infix on BOTH flavors (MariaDB spelling)", `c DIV 2 = 0`, "infix-div"},
		{"DIV, MySQL 8 spelling (parenthesized)", `(c DIV 2) = 0`, "infix-div"},
		{"div(a, b) is the PG-valid CALL form, not this arm", `div(c, 2) = 0`, ""},
		{"MOD keyword, MariaDB's rendering of both MOD and %", `c MOD 3 = 0`, "infix-mod"},
		{"mod(a, b) is the PG-valid CALL form, not this arm", `mod(c, 3) = 0`, ""},
		{"% needs no arm (PG parses it)", `(c % 3) = 0`, ""},
		{"XOR renders infix on BOTH flavors", `c > '0' xor c < '9'`, "infix-xor"},
		{"prefix ! survives MariaDB renderings", `!(c <=> '99')`, "prefix-!"},
		{"!= is inequality, never the prefix operator", `c != '9'`, ""},
		{"NOT REGEXP renders with BOTH the bang and the operator", `!(c regexp '^z')`, "prefix-!"},
		{"MEMBER OF renders on MySQL 8", `c member of (j)`, "infix-member-of"},
		{"a bare column named member is not the operator", `member > c`, ""},
		{"bare INTERVAL grammar (both flavors' rendering)", `(d + interval 1 day) > d`, "interval-bare"},
		{"INTERVAL inside date_add is translator territory", `date_add(d, interval 1 day) > d`, ""},
		{"INTERVAL inside date_sub is translator territory", `date_sub(d, interval 1 day) > d`, ""},
		{"a bare INTERVAL outside the guarded call still flags", `date_add(d, interval 1 day) > d + interval 2 day`, "interval-bare"},
		{"column-level COLLATE names a MySQL collation", `(c collate utf8mb4_bin) = 'x'`, "collate-mysql"},
		{"COLLATE inside a cast type spec is translator territory", `cast(c as char(5) collate utf8mb4_bin) = 'x'`, ""},
		{"the BINARY operator's rendering is translator territory", `cast(c as char charset binary) = 'x'`, ""},
		{"operator words inside literals are data", `c <> 'a div b mod c xor !'`, ""},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			hits := ScanUntranslatableMySQLToPGExprs(bug242Schema(tc.expr), "mysql", "postgres", nil)
			switch {
			case tc.want == "" && len(hits) == 0:
				// regexp_like still flags via the function scan; assert
				// only that the operator tokens are absent.
			case tc.want == "":
				for _, h := range hits {
					if strings.HasPrefix(h.Function, "infix-") || strings.HasPrefix(h.Function, "prefix-") ||
						h.Function == "interval-bare" || h.Function == "collate-mysql" {
						t.Fatalf("expression %q flagged %q; an operator arm has over-matched", tc.expr, h.Function)
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

// TestDetectGaps_OperatorFamilyAdvisories is the ARCH-1 extension of the
// advisory pin: every operator the 2026-08-11 measurement added fires an
// advisory on its measured catalog rendering, through the same shared
// token scan as the refusal, and the PG-parseable sibling spellings
// (`%`, call-form `mod()`) produce none.
func TestDetectGaps_OperatorFamilyAdvisories(t *testing.T) {
	cases := []struct {
		expr    string
		pattern string
		prefix  bool
	}{
		{`a DIV 2 = 0`, "DIV", false},
		{`a MOD 3 = 0`, "MOD", false},
		{`a > 0 xor b > 0`, "XOR", false},
		{`!(a <=> 99)`, "!", true},
	}
	for _, tc := range cases {
		gaps := detectGaps(tc.expr, nil)
		found := false
		for _, g := range gaps {
			if g.Pattern == tc.pattern {
				found = true
				if g.Severity != SeverityLoud {
					t.Errorf("%s gap severity = %v; want SeverityLoud (PG cannot parse it)", tc.pattern, g.Severity)
				}
				if g.Prefix != tc.prefix {
					t.Errorf("%s gap Prefix = %v; want %v (renderers must not call a prefix operator infix)", tc.pattern, g.Prefix, tc.prefix)
				}
			}
		}
		if !found {
			t.Errorf("%q produced no %s advisory: %+v", tc.expr, tc.pattern, gaps)
		}
	}
	for _, expr := range []string{`(a % 3) = 0`, `mod(a, 3) = 0`, `div(a, 2) = 0`, `a != 9`, `c <> 'a div b'`} {
		for _, g := range detectGaps(expr, nil) {
			switch g.Pattern {
			case "DIV", "MOD", "XOR", "!":
				t.Errorf("PG-parseable spelling %q produced a %s advisory (false positive)", expr, g.Pattern)
			}
		}
	}
}

// TestRefuseOnLoudGaps_OperatorFamilyRendering pins the rendered refusal
// for the ARCH-1 word operators and the prefix form: the message names
// the operator form correctly ("the infix DIV operator" / "the prefix !
// operator", never call-parens), the PG equivalent, and the
// --expr-override remedy.
func TestRefuseOnLoudGaps_OperatorFamilyRendering(t *testing.T) {
	err := RefuseOnLoudGaps(
		bug242Schema(`c DIV 2 = 0`), "mysql", "postgres", "migrate", nil,
	)
	if err == nil {
		t.Fatal("want the curated pre-DDL refusal for infix DIV, got nil — the rendering would die in PG's parser mid-pipeline")
	}
	for _, want := range []string{"the infix DIV operator", "div(a, b)", "--expr-override", "z_rx"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("DIV refusal does not mention %q:\n%s", want, err)
		}
	}
	if strings.Contains(err.Error(), "DIV()") {
		t.Errorf("call-parens rendered on an operator form:\n%s", err)
	}

	err = RefuseOnLoudGaps(
		bug242Schema(`!(c <=> '99')`), "mysql", "postgres", "migrate", nil,
	)
	if err == nil {
		t.Fatal("want the curated pre-DDL refusal for prefix !, got nil")
	}
	for _, want := range []string{"the prefix ! operator", "NOT (...)", "--expr-override"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("prefix-! refusal does not mention %q:\n%s", want, err)
		}
	}
	if strings.Contains(err.Error(), "the infix ! operator") {
		t.Errorf("a prefix operator rendered as infix (the v0.120.1 rendering nit, prefix edition):\n%s", err)
	}
}

// TestRefuseOnUntranslatableExprs_ConstructArms pins the general
// backstop's refusal for the measured grammar constructs that have no
// curated advisory twin (they are grammar, not operators): bare
// INTERVAL, column-level COLLATE, and MEMBER OF. Each message names the
// construct and a PG-side path forward.
func TestRefuseOnUntranslatableExprs_ConstructArms(t *testing.T) {
	cases := []struct {
		name  string
		expr  string
		wants []string
	}{
		{"bare INTERVAL", `(d + interval 1 day) > d`, []string{"INTERVAL", "interval '1 day'", "--expr-override"}},
		{"column-level COLLATE", `(c collate utf8mb4_bin) = 'x'`, []string{"COLLATE", "42704", "--expr-override"}},
		{"MEMBER OF", `c member of (j)`, []string{"MEMBER OF", "jsonb", "--expr-override"}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := RefuseOnUntranslatableExprs(
				bug242Schema(tc.expr), "mysql", "postgres", "migrate", nil,
			)
			if err == nil {
				t.Fatalf("want the pre-DDL refusal for %s, got nil — the rendering would die on the PG target mid-pipeline", tc.name)
			}
			for _, want := range tc.wants {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s refusal does not mention %q:\n%s", tc.name, want, err)
				}
			}
		})
	}
}
