// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package rowpredicate

import (
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestPushdownTerms pins the flattened leaf-comparison surface the
// ADR-0176 push-down classifier consumes: one term per leaf in AST walk
// order, columns lower-cased, and the bool-vs-0/1-literal flag set for
// exactly the comparisons Postgres SQL would reject.
func TestPushdownTerms(t *testing.T) {
	infos := map[string]ColumnInfo{
		"id":      {Family: FamilyNumeric},
		"name":    {Family: FamilyString, Faithful: true},
		"active":  {Family: FamilyBool},
		"deleted": {Family: FamilyBool},
		"d":       {Family: FamilyTemporal},
	}

	tests := []struct {
		name      string
		predicate string
		want      []PushdownTerm
	}{
		{
			name:      "compound walk order, IS NULL and IN included",
			predicate: "NOT (id = 5 OR name IN ('a', 'b')) AND id IS NULL",
			want: []PushdownTerm{
				{Column: "id"},
				{Column: "name"},
				{Column: "id"},
			},
		},
		{
			name:      "bool vs TRUE literal is NOT flagged",
			predicate: "active = TRUE",
			want:      []PushdownTerm{{Column: "active"}},
		},
		{
			name:      "bool vs 0/1 numeric literal IS flagged (invalid PG SQL)",
			predicate: "active = 1",
			want:      []PushdownTerm{{Column: "active", BoolNumericLiteral: true}},
		},
		{
			name:      "bool IN with a numeric member IS flagged",
			predicate: "active IN (1)",
			want:      []PushdownTerm{{Column: "active", BoolNumericLiteral: true}},
		},
		{
			name:      "bool IN with only keyword literals is NOT flagged",
			predicate: "active IN (TRUE, FALSE)",
			want:      []PushdownTerm{{Column: "active"}},
		},
		{
			name:      "column casing is normalized like Compile's",
			predicate: `"Active" = FALSE AND ID < 3`,
			want: []PushdownTerm{
				{Column: "active"},
				{Column: "id"},
			},
		},

		// ---- Temporal literal-granularity flags (audit 2026-07-23 D0-5) ----
		{
			name:      "pure-date temporal literal carries no granularity flag",
			predicate: "d = '2026-01-15'",
			want:      []PushdownTerm{{Column: "d"}},
		},
		{
			name:      "time-bearing literal (space form, no seconds) is flagged",
			predicate: "d < '2026-01-15 08:30'",
			want:      []PushdownTerm{{Column: "d", TemporalLiteralTimeBearing: true}},
		},
		{
			name:      "time-bearing literal (T form) is flagged",
			predicate: "d != '2026-01-15T08:30:00'",
			want:      []PushdownTerm{{Column: "d", TemporalLiteralTimeBearing: true}},
		},
		{
			name:      "midnight is still time-bearing (the flag keys on the TEXT, conservatively)",
			predicate: "d = '2026-01-15 00:00:00'",
			want:      []PushdownTerm{{Column: "d", TemporalLiteralTimeBearing: true}},
		},
		{
			name:      "exactly 6 fractional digits is time-bearing but NOT sub-microsecond",
			predicate: "d >= '2026-01-15 08:30:00.123456'",
			want:      []PushdownTerm{{Column: "d", TemporalLiteralTimeBearing: true}},
		},
		{
			name:      "7 fractional digits is sub-microsecond (PG rounds to µs, the client keeps ns)",
			predicate: "d >= '2026-01-15 08:30:00.1234567'",
			want:      []PushdownTerm{{Column: "d", TemporalLiteralTimeBearing: true, TemporalLiteralSubMicrosecond: true}},
		},
		{
			name:      "IN list flags fold across members (one time-bearing member flags the term)",
			predicate: "d IN ('2026-01-15', '2026-01-16 08:30:00.1234567')",
			want:      []PushdownTerm{{Column: "d", TemporalLiteralTimeBearing: true, TemporalLiteralSubMicrosecond: true}},
		},
		{
			name:      "IN list of pure dates carries no granularity flag",
			predicate: "d NOT IN ('2026-01-15', '2026-01-16')",
			want:      []PushdownTerm{{Column: "d"}},
		},
		{
			name:      "granularity flags survive NOT/AND composition",
			predicate: "NOT (d >= '2026-01-15 12:00:00') AND id < 3",
			want: []PushdownTerm{
				{Column: "d", TemporalLiteralTimeBearing: true},
				{Column: "id"},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Compile("t", tc.predicate, infos)
			if err != nil {
				t.Fatalf("Compile(%q): %v", tc.predicate, err)
			}
			if got := p.PushdownTerms(); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("PushdownTerms(%q) = %+v, want %+v", tc.predicate, got, tc.want)
			}
		})
	}

	var nilPred *Predicate
	if got := nilPred.PushdownTerms(); got != nil {
		t.Errorf("nil predicate PushdownTerms() = %v, want nil", got)
	}
}

// TestPushdownTerms_NormalizedLiteralCarriesNoFlags pins the Q1 interlock
// (audit 2026-07-23 D0-5): a temporal literal compiled through an engine
// temporal-literal lens is NORMALIZED to the engine's granularity, so the
// term flags — computed from the (rewritten) literal text — no longer fire
// and the push-down classifier admits the term. The flags stay reachable
// only for a ClientExact compile (the classifier's fail-closed belt), which
// the granularity cases in TestPushdownTerms pin.
func TestPushdownTerms_NormalizedLiteralCarriesNoFlags(t *testing.T) {
	pgLens := map[string]ColumnInfo{
		"d":  {Family: FamilyTemporal, TemporalSemantics: ir.TemporalLiteralCastToColumn, TemporalDateOnly: true},
		"ts": {Family: FamilyTemporal, TemporalSemantics: ir.TemporalLiteralCastToColumn},
	}
	for _, pred := range []string{
		"d < '2026-01-15 08:30'",                          // time-bearing → truncated to the date
		"d = '2026-01-15 00:00:00'",                       // midnight is still rewritten (text-keyed)
		"d IN ('2026-01-15', '2026-01-16 08:30:00')",      // per-member truncation
		"ts >= '2026-01-15 08:30:00.1234567'",             // 7 digits → rounded to µs
		"ts IN ('2026-01-15 08:30:00.1234565')",           // half-even boundary member
		"NOT (d >= '2026-01-15 12:00:00') AND ts IS NULL", // composition
	} {
		t.Run(pred, func(t *testing.T) {
			p, err := Compile("t", pred, pgLens)
			if err != nil {
				t.Fatalf("Compile(%q): %v", pred, err)
			}
			for _, term := range p.PushdownTerms() {
				if term.Column == "d" && term.TemporalLiteralTimeBearing {
					t.Errorf("term %+v still time-bearing after CastToColumn normalization", term)
				}
				if term.TemporalLiteralSubMicrosecond {
					t.Errorf("term %+v still sub-microsecond after normalization", term)
				}
				if term.Unrecognized != "" {
					t.Errorf("term %+v unexpectedly unrecognized", term)
				}
			}
		})
	}
}

// fakeGrammarNode is a synthetic AST node the term walker does not know —
// the ARCH-1 pin's stand-in for a future grammar construct (BETWEEN, LIKE, a
// function call) whose collectPushdownTerms case was forgotten.
type fakeGrammarNode struct{}

func (fakeGrammarNode) eval(ir.Row) truth       { return truthUnknown }
func (fakeGrammarNode) columns(map[string]bool) {}

// TestPushdownTerms_UnrecognizedNodeFailsClosed pins the ARCH-1 default arm
// (audit 2026-07-23): an AST node type collectPushdownTerms does not
// recognize emits an Unrecognized term naming the type — never ZERO terms,
// which would let an unproven construct slip into a push-down envelope. The
// classifier-side rejection is pinned by the pipeline's
// TestPGPushdownEligibleTerms_FailClosed.
func TestPushdownTerms_UnrecognizedNodeFailsClosed(t *testing.T) {
	p := &Predicate{root: andNode{kids: []node{
		cmpNode{column: "id", fam: FamilyNumeric, lit: literal{kind: litNumber}},
		fakeGrammarNode{},
	}}}
	got := p.PushdownTerms()
	want := []PushdownTerm{
		{Column: "id"},
		{Unrecognized: "rowpredicate.fakeGrammarNode"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("PushdownTerms() = %+v, want %+v", got, want)
	}
}
