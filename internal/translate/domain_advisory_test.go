// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// Roadmap item 155 — the behavioural half. The gate in domain_gate_test.go
// says every dispatch here reads the STORAGE type; these say the advisory
// surfaces actually fire for a DOMAIN-wrapped column, which is the thing an
// operator experiences.
//
// The pins are shaped as PAIRS — bare column and the same type behind a
// `CREATE DOMAIN` wrapper — and assert the two produce the SAME advisory.
// A pin on the domain case alone would pass a scanner that fired on
// everything; the equality is what makes the wrapper transparent rather
// than merely non-fatal.
//
// SCOPE: every PG-sourced advisory surface in this package — the two
// notice scanners and the three PG-sourced hint entries. The MySQL-sourced
// scanners are exempt with a reason (see translateDomainDispatchExemptions)
// and are deliberately not here.

// domainOver wraps a base type the way the PG schema reader does for a
// column declared as a `CREATE DOMAIN`.
func domainOver(name string, base ir.Type) ir.Type {
	return ir.Domain{Name: name, BaseType: base}
}

func TestScanWideVarcharNotices_SeesThroughADomain(t *testing.T) {
	// MySQL's emitColumnType recurses ir.Domain into its base, so BOTH
	// columns land as a TEXT tier on the target. Before item 155 only the
	// bare one was announced.
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "docs",
		Columns: []*ir.Column{
			{Name: "bare", Type: ir.Varchar{Length: 70000}},
			{Name: "wrapped", Type: domainOver("wide_body", ir.Varchar{Length: 70000})},
			{Name: "narrow_wrapped", Type: domainOver("short_note", ir.Varchar{Length: 255})},
		},
	}}}

	got := ScanWideVarcharNotices(schema, "postgres", "mysql")
	if len(got) != 2 {
		t.Fatalf("notices = %+v; want exactly 2 (bare + wrapped) — a DOMAIN over varchar(70000) is "+
			"down-mapped to TEXT just like the bare column, so it must be announced too", got)
	}
	for _, n := range got {
		if n.Length != 70000 {
			t.Errorf("notice %+v: Length = %d; want 70000 (the wrapper must not lose the declared length)", n, n.Length)
		}
	}
	if got[0].Column != "bare" || got[1].Column != "wrapped" {
		t.Errorf("notices = %+v; want columns [bare wrapped] — narrow_wrapped must NOT be flagged, or the "+
			"unwrap has made the scanner fire on everything", got)
	}
}

func TestScanUnconstrainedNumericNotices_SeesThroughADomain(t *testing.T) {
	schema := &ir.Schema{Tables: []*ir.Table{{
		Name: "ledger",
		Columns: []*ir.Column{
			{Name: "bare", Type: ir.Decimal{Unconstrained: true}},
			{Name: "wrapped", Type: domainOver("money_amt", ir.Decimal{Unconstrained: true})},
			{Name: "bounded_wrapped", Type: domainOver("rate", ir.Decimal{Precision: 10, Scale: 4})},
		},
	}}}

	got := ScanUnconstrainedNumericNotices(schema, "postgres", "mysql")
	if len(got) != 2 {
		t.Fatalf("notices = %+v; want exactly 2 (bare + wrapped) — a DOMAIN over bare `numeric` is widened "+
			"to DECIMAL(65,30) just like the bare column", got)
	}
	if got[0].Column != "bare" || got[1].Column != "wrapped" {
		t.Errorf("notices = %+v; want columns [bare wrapped] — bounded_wrapped must NOT be flagged", got)
	}
}

// TestHintsFor_SeeThroughADomain covers the three PG-sourced hint entries.
// One representative would not do: each is a separate predicate with its
// own type assertion, which is exactly the shape that let two of these go
// unnoticed (the Bug 74 "pin the class" rule applied to a rule registry).
func TestHintsFor_SeeThroughADomain(t *testing.T) {
	cases := []struct {
		name      string
		base      ir.Type
		wantAlias string
	}{
		{"uuid", ir.UUID{}, "binary_uuid"},
		{"unbounded-text", ir.Text{Size: ir.TextLong}, "mediumtext"},
		{"unconstrained-numeric", ir.Decimal{Unconstrained: true}, "decimal(N,M)"},
	}
	for _, tc := range cases {
		bare := &ir.Column{Name: "c", Type: tc.base}
		wrapped := &ir.Column{Name: "c", Type: domainOver("d_"+tc.name, tc.base)}
		// The target column is what the MySQL side lands; its exact shape
		// is irrelevant to these hints (they gate on the source), so the
		// same one serves both halves of the pair.
		tgt := &ir.Column{Name: "c", Type: ir.Text{Size: ir.TextLong}}

		bareHints := HintsFor("t", bare, tgt, "postgres", "mysql")
		wrappedHints := HintsFor("t", wrapped, tgt, "postgres", "mysql")

		if !hintsCarryAlias(bareHints, tc.wantAlias) {
			t.Fatalf("%s: the BARE column produced no %q hint (%+v); this pin is measuring the wrong thing",
				tc.name, tc.wantAlias, bareHints)
		}
		if !hintsCarryAlias(wrappedHints, tc.wantAlias) {
			t.Errorf("%s: a DOMAIN over the same base type produced no %q hint (%+v); the wrapper must be "+
				"transparent to the advisory, since it is transparent to the storage the column lands in",
				tc.name, tc.wantAlias, wrappedHints)
		}
	}
}

func hintsCarryAlias(hints []Hint, alias string) bool {
	for _, h := range hints {
		if strings.Contains(h.SuggestedOverride, alias) || strings.Contains(h.Message, alias) {
			return true
		}
	}
	return false
}
