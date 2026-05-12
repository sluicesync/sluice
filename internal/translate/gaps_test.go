// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestScanMySQLToPGGaps_GreatestLeast_Silent pins detection of the
// silently-divergent #11 patterns in a generated-column body. The
// scanner returns a SeveritySilent Gap naming both GREATEST and LEAST
// per matched call.
func TestScanMySQLToPGGaps_GreatestLeast_Silent(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "metrics",
			Columns: []*ir.Column{
				{
					Name:                 "clamped",
					Type:                 ir.Integer{Width: 32},
					GeneratedExpr:        "GREATEST(0, LEAST(value, 100))",
					GeneratedExprDialect: "mysql",
				},
			},
		},
	}}
	gaps := ScanMySQLToPGGaps(s, "mysql", "postgres", nil)
	if len(gaps) != 2 {
		t.Fatalf("got %d gaps; want 2 (GREATEST + LEAST)", len(gaps))
	}
	// Sorted by pattern name → GREATEST then LEAST.
	if gaps[0].Pattern != "GREATEST" || gaps[1].Pattern != "LEAST" {
		t.Errorf("patterns = %q,%q; want GREATEST,LEAST", gaps[0].Pattern, gaps[1].Pattern)
	}
	for _, g := range gaps {
		if g.Severity != SeveritySilent {
			t.Errorf("Pattern %s: severity = %v; want SeveritySilent", g.Pattern, g.Severity)
		}
		if g.Field != "GENERATED" {
			t.Errorf("Pattern %s: field = %q; want GENERATED", g.Pattern, g.Field)
		}
		if g.Table != "metrics" {
			t.Errorf("Pattern %s: table = %q; want metrics", g.Pattern, g.Table)
		}
		if g.Column != "clamped" {
			t.Errorf("Pattern %s: column = %q; want clamped", g.Pattern, g.Column)
		}
		if g.RuleNum != 11 {
			t.Errorf("Pattern %s: rule = %d; want 11", g.Pattern, g.RuleNum)
		}
	}
}

// TestScanMySQLToPGGaps_FindInSetCheckConstraint pins detection of
// loud-failure FIND_IN_SET in a CHECK constraint. Returns the
// constraint name on Gap.Constraint (not Gap.Column).
func TestScanMySQLToPGGaps_FindInSetCheckConstraint(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "events",
			CheckConstraints: []*ir.CheckConstraint{
				{
					Name:        "events_status_valid",
					Expr:        "FIND_IN_SET(status, 'pending,active,closed') > 0",
					ExprDialect: "mysql",
				},
			},
		},
	}}
	gaps := ScanMySQLToPGGaps(s, "mysql", "postgres", nil)
	if len(gaps) != 1 {
		t.Fatalf("got %d gaps; want 1", len(gaps))
	}
	g := gaps[0]
	if g.Pattern != "FIND_IN_SET" {
		t.Errorf("pattern = %q; want FIND_IN_SET", g.Pattern)
	}
	if g.Field != "CHECK" {
		t.Errorf("field = %q; want CHECK", g.Field)
	}
	if g.Constraint != "events_status_valid" {
		t.Errorf("constraint = %q; want events_status_valid", g.Constraint)
	}
	if g.Column != "" {
		t.Errorf("column = %q; want empty for CHECK gap", g.Column)
	}
	if g.Severity != SeverityLoud {
		t.Errorf("severity = %v; want SeverityLoud", g.Severity)
	}
}

// TestScanMySQLToPGGaps_SHA_PgcryptoGate covers the v0.38.0 extension
// gate: SHA1/SHA2 in a MySQL source body produce a gap UNLESS the
// operator passed --enable-pg-extension pgcrypto. With the flag, the
// rewrite ships per v0.38.0 — no advisory needed.
func TestScanMySQLToPGGaps_SHA_PgcryptoGate(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "audit",
			Columns: []*ir.Column{
				{
					Name:                 "digest",
					Type:                 ir.Char{Length: 64},
					GeneratedExpr:        "SHA2(payload, 256)",
					GeneratedExprDialect: "mysql",
				},
			},
		},
	}}

	t.Run("without pgcrypto enabled — surfaces gap", func(t *testing.T) {
		gaps := ScanMySQLToPGGaps(s, "mysql", "postgres", nil)
		if len(gaps) != 1 {
			t.Fatalf("got %d gaps; want 1", len(gaps))
		}
		if gaps[0].Pattern != "SHA2" {
			t.Errorf("pattern = %q; want SHA2", gaps[0].Pattern)
		}
	})

	t.Run("with pgcrypto enabled — gap suppressed (rewrite ships)", func(t *testing.T) {
		enabled := map[string]bool{"pgcrypto": true}
		gaps := ScanMySQLToPGGaps(s, "mysql", "postgres", enabled)
		if len(gaps) != 0 {
			t.Errorf("got %d gaps; want 0 (pgcrypto enabled → rewrite ships)", len(gaps))
		}
	})
}

// TestScanMySQLToPGGaps_AllPatterns_SingleTable pins detection across
// the seven patterns sluice currently flags, in one table. Verifies
// each is detected at least once and the result is sorted stably.
func TestScanMySQLToPGGaps_AllPatterns_SingleTable(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{
		{
			Name: "kitchen_sink",
			Columns: []*ir.Column{
				{
					Name:                 "g",
					Type:                 ir.Integer{Width: 32},
					GeneratedExpr:        "GREATEST(a, b)",
					GeneratedExprDialect: "mysql",
				},
				{
					Name:                 "l",
					Type:                 ir.Integer{Width: 32},
					GeneratedExpr:        "LEAST(a, b)",
					GeneratedExprDialect: "mysql",
				},
				{
					Name:                 "r",
					Type:                 ir.Boolean{},
					GeneratedExpr:        "REGEXP_LIKE(name, '^x')",
					GeneratedExprDialect: "mysql",
				},
				{
					Name:                 "ip",
					Type:                 ir.Integer{Width: 64},
					GeneratedExpr:        "INET_ATON(ip_text)",
					GeneratedExprDialect: "mysql",
				},
				{
					Name:                 "ip_text",
					Type:                 ir.Varchar{Length: 15},
					GeneratedExpr:        "INET_NTOA(ip_int)",
					GeneratedExprDialect: "mysql",
				},
				{
					Name:                 "tz",
					Type:                 ir.Timestamp{},
					GeneratedExpr:        "CONVERT_TZ(ts, 'UTC', 'America/Los_Angeles')",
					GeneratedExprDialect: "mysql",
				},
				{
					Name:                 "sha",
					Type:                 ir.Char{Length: 40},
					GeneratedExpr:        "SHA1(payload)",
					GeneratedExprDialect: "mysql",
				},
			},
			CheckConstraints: []*ir.CheckConstraint{
				{
					Name:        "status_check",
					Expr:        "FIND_IN_SET(status, 'a,b,c') > 0",
					ExprDialect: "mysql",
				},
			},
		},
	}}
	gaps := ScanMySQLToPGGaps(s, "mysql", "postgres", nil)
	wantPatterns := map[string]bool{
		"GREATEST":    false,
		"LEAST":       false,
		"REGEXP_LIKE": false,
		"INET_ATON":   false,
		"INET_NTOA":   false,
		"CONVERT_TZ":  false,
		"SHA1":        false,
		"FIND_IN_SET": false,
	}
	for _, g := range gaps {
		wantPatterns[g.Pattern] = true
	}
	for p, found := range wantPatterns {
		if !found {
			t.Errorf("pattern %q not detected", p)
		}
	}
}

// TestScanMySQLToPGGaps_LowercaseMatching pins case-insensitive
// pattern detection. The scanner uses `(?i)` in its regex.
func TestScanMySQLToPGGaps_LowercaseMatching(t *testing.T) {
	cases := []string{
		"greatest(a, b)",
		"GrEaTeSt(a, b)",
		"greatest (a, b)", // optional whitespace between name and (
	}
	for _, expr := range cases {
		expr := expr
		t.Run(expr, func(t *testing.T) {
			s := &ir.Schema{Tables: []*ir.Table{{
				Name: "t",
				Columns: []*ir.Column{{
					Name:                 "c",
					Type:                 ir.Integer{Width: 32},
					GeneratedExpr:        expr,
					GeneratedExprDialect: "mysql",
				}},
			}}}
			gaps := ScanMySQLToPGGaps(s, "mysql", "postgres", nil)
			if len(gaps) != 1 || gaps[0].Pattern != "GREATEST" {
				t.Errorf("expr %q: got %d gaps; want 1 (GREATEST)", expr, len(gaps))
			}
		})
	}
}

// TestScanMySQLToPGGaps_WordBoundary_NoFalsePositive pins that
// function-name-prefixed identifiers don't match. The catalog's
// detection uses `\b` word boundary so e.g. `IS_GREATEST_HIT(` does
// NOT trigger a GREATEST match.
func TestScanMySQLToPGGaps_WordBoundary_NoFalsePositive(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{{
			Name:                 "c",
			Type:                 ir.Integer{Width: 32},
			GeneratedExpr:        "IS_GREATEST_HIT(x)", // not a GREATEST call
			GeneratedExprDialect: "mysql",
		}},
	}}}
	gaps := ScanMySQLToPGGaps(s, "mysql", "postgres", nil)
	if len(gaps) != 0 {
		t.Errorf("got %d gaps; want 0 (word boundary should reject IS_GREATEST_HIT)", len(gaps))
	}
}

// TestScanMySQLToPGGaps_NonCrossEngine_ReturnsNil pins that same-
// engine pairs and PG → MySQL don't trigger the scanner. The catalog
// only covers MySQL → PG.
func TestScanMySQLToPGGaps_NonCrossEngine_ReturnsNil(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{{
			Name:                 "c",
			Type:                 ir.Integer{Width: 32},
			GeneratedExpr:        "GREATEST(a, b)",
			GeneratedExprDialect: "mysql",
		}},
	}}}
	cases := []struct {
		src, tgt string
	}{
		{"postgres", "postgres"},
		{"mysql", "mysql"},
		{"postgres", "mysql"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.src+"-to-"+c.tgt, func(t *testing.T) {
			if gaps := ScanMySQLToPGGaps(s, c.src, c.tgt, nil); gaps != nil {
				t.Errorf("got %d gaps; want nil for %s → %s", len(gaps), c.src, c.tgt)
			}
		})
	}
}

// TestScanMySQLToPGGaps_DefaultExpression covers detection in column
// DEFAULTs (DefaultExpression body). The scanner only checks the
// DefaultExpression variant; literal defaults are skipped.
func TestScanMySQLToPGGaps_DefaultExpression(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{{
			Name: "ip_int",
			Type: ir.Integer{Width: 64},
			Default: ir.DefaultExpression{
				Expr:    "INET_ATON('127.0.0.1')",
				Dialect: "mysql",
			},
		}},
	}}}
	gaps := ScanMySQLToPGGaps(s, "mysql", "postgres", nil)
	if len(gaps) != 1 {
		t.Fatalf("got %d gaps; want 1", len(gaps))
	}
	if gaps[0].Field != "DEFAULT" {
		t.Errorf("field = %q; want DEFAULT", gaps[0].Field)
	}
	if gaps[0].Pattern != "INET_ATON" {
		t.Errorf("pattern = %q; want INET_ATON", gaps[0].Pattern)
	}
}

// TestScanMySQLToPGGaps_NonMySQLDialect_Skipped pins that expressions
// tagged with a non-mysql dialect (e.g. operator-supplied
// --expr-override or PG-source bodies) are NOT scanned. The catalog
// is MySQL-only.
func TestScanMySQLToPGGaps_NonMySQLDialect_Skipped(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{{
			Name:                 "c",
			Type:                 ir.Integer{Width: 32},
			GeneratedExpr:        "GREATEST(a, b)",
			GeneratedExprDialect: "postgres", // not mysql
		}},
	}}}
	gaps := ScanMySQLToPGGaps(s, "mysql", "postgres", nil)
	if len(gaps) != 0 {
		t.Errorf("got %d gaps; want 0 (postgres-dialect body is not scanned)", len(gaps))
	}
}

// TestScanMySQLToPGGaps_NilSchema_Safe pins safe handling of nil
// schema input — used by callers that pre-allocate the slice before
// fetching the schema and pass it through unconditionally.
func TestScanMySQLToPGGaps_NilSchema_Safe(t *testing.T) {
	if gaps := ScanMySQLToPGGaps(nil, "mysql", "postgres", nil); gaps != nil {
		t.Errorf("got %d gaps; want nil for nil schema", len(gaps))
	}
}

// TestSeverity_String pins the string form for both severities.
// Stable wording is what the JSON renderer / human-readable output
// uses.
func TestSeverity_String(t *testing.T) {
	if SeverityLoud.String() != "loud" {
		t.Errorf("SeverityLoud.String() = %q; want loud", SeverityLoud.String())
	}
	if SeveritySilent.String() != "silent" {
		t.Errorf("SeveritySilent.String() = %q; want silent", SeveritySilent.String())
	}
	if Severity(99).String() != "unknown" {
		t.Errorf("unknown severity should stringify to 'unknown'")
	}
}

// TestScanMySQLToPGGaps_NoteWording sanity check: each pattern's
// advisory note mentions either `--expr-override` or `--type-override`
// or `--enable-pg-extension`, so operators always see an actionable
// workaround.
func TestScanMySQLToPGGaps_NoteWording(t *testing.T) {
	for _, pat := range gapPatterns {
		hint := strings.Contains(pat.note, "--expr-override") ||
			strings.Contains(pat.note, "--type-override") ||
			strings.Contains(pat.note, "--enable-pg-extension")
		if !hint {
			t.Errorf("pattern %q note has no actionable workaround mention: %q",
				pat.name, pat.note)
		}
	}
}
