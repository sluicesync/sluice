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
				{Column: "id", Op: "="},
				{Column: "name", Op: "IN"},
				{Column: "id", Op: "IS NULL"},
			},
		},
		{
			name:      "bool vs TRUE literal is NOT flagged",
			predicate: "active = TRUE",
			want:      []PushdownTerm{{Column: "active", Op: "="}},
		},
		{
			name:      "bool vs 0/1 numeric literal IS flagged (invalid PG SQL)",
			predicate: "active = 1",
			want:      []PushdownTerm{{Column: "active", Op: "=", BoolNumericLiteral: true}},
		},
		{
			name:      "bool IN with a numeric member IS flagged",
			predicate: "active IN (1)",
			want:      []PushdownTerm{{Column: "active", Op: "IN", BoolNumericLiteral: true}},
		},
		{
			name:      "bool IN with only keyword literals is NOT flagged",
			predicate: "active IN (TRUE, FALSE)",
			want:      []PushdownTerm{{Column: "active", Op: "IN"}},
		},
		{
			name:      "column casing is normalized like Compile's",
			predicate: `"Active" = FALSE AND ID < 3`,
			want: []PushdownTerm{
				{Column: "active", Op: "="},
				{Column: "id", Op: "<"},
			},
		},

		// ---- Temporal literal-granularity flags (audit 2026-07-23 D0-5) ----
		{
			name:      "pure-date temporal literal carries no granularity flag",
			predicate: "d = '2026-01-15'",
			want:      []PushdownTerm{{Column: "d", Op: "="}},
		},
		{
			name:      "time-bearing literal (space form, no seconds) is flagged",
			predicate: "d < '2026-01-15 08:30'",
			want:      []PushdownTerm{{Column: "d", Op: "<", TemporalLiteralTimeBearing: true}},
		},
		{
			name:      "time-bearing literal (T form) is flagged",
			predicate: "d != '2026-01-15T08:30:00'",
			want:      []PushdownTerm{{Column: "d", Op: "!=", TemporalLiteralTimeBearing: true}},
		},
		{
			name:      "midnight is still time-bearing (the flag keys on the TEXT, conservatively)",
			predicate: "d = '2026-01-15 00:00:00'",
			want:      []PushdownTerm{{Column: "d", Op: "=", TemporalLiteralTimeBearing: true}},
		},
		{
			name:      "exactly 6 fractional digits is time-bearing but NOT sub-microsecond",
			predicate: "d >= '2026-01-15 08:30:00.123456'",
			want:      []PushdownTerm{{Column: "d", Op: ">=", TemporalLiteralTimeBearing: true}},
		},
		{
			name:      "IN list of pure dates carries no granularity flag",
			predicate: "d NOT IN ('2026-01-15', '2026-01-16')",
			want:      []PushdownTerm{{Column: "d", Op: "NOT IN"}},
		},
		{
			name:      "granularity flags survive NOT/AND composition",
			predicate: "NOT (d >= '2026-01-15 12:00:00') AND id < 3",
			want: []PushdownTerm{
				{Column: "d", Op: ">=", TemporalLiteralTimeBearing: true},
				{Column: "id", Op: "<"},
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
		"ts IN ('2026-01-15 08:30:00.1234565')",           // half-boundary member
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

// TestPushdownTerms_SubMicrosecondFlagOnHandBuiltAST pins the
// TemporalLiteralSubMicrosecond flag on hand-built cmp/IN nodes. Compile can
// no longer PRODUCE such a term (an engine lens normalizes the literal to
// µs; the ClientExact zero value refuses it outright — review F4), so the
// flag is purely the classifier's fail-closed belt for terms that reach a
// push-down site without going through Compile's gates — and its
// computation is pinned here on the AST directly, exactly the shape the
// belt defends against.
func TestPushdownTerms_SubMicrosecondFlagOnHandBuiltAST(t *testing.T) {
	subMicroLit := literal{kind: litString, str: "2026-01-15 08:30:00.1234567"}
	pureDateLit := literal{kind: litString, str: "2026-01-15"}

	p := &Predicate{root: andNode{kids: []node{
		cmpNode{column: "d", fam: FamilyTemporal, op: opGe, lit: subMicroLit},
		inNode{column: "d", fam: FamilyTemporal, lits: []literal{pureDateLit, subMicroLit}},
	}}}
	want := []PushdownTerm{
		{Column: "d", Op: ">=", TemporalLiteralTimeBearing: true, TemporalLiteralSubMicrosecond: true},
		{Column: "d", Op: "IN", TemporalLiteralTimeBearing: true, TemporalLiteralSubMicrosecond: true}, // one sub-µs member flags the IN term
	}
	if got := p.PushdownTerms(); !reflect.DeepEqual(got, want) {
		t.Errorf("PushdownTerms() = %+v, want %+v", got, want)
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
		{Column: "id", Op: "="},
		{Unrecognized: "rowpredicate.fakeGrammarNode"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("PushdownTerms() = %+v, want %+v", got, want)
	}
}

// TestGrammarLeafOps_EveryOpCompilesAndEmits keeps [GrammarLeafOps]
// honest against the grammar (audit 2026-07-23 TEST-2 / G-12): every
// inventory entry must be reachable through Compile and come back as a
// PushdownTerm.Op spelled exactly like the inventory, and Compile must
// not emit an Op outside the inventory. With this pin, the inventory is
// safe to use as the enumeration source for op-coverage gates (the
// push-down oracle's op-completeness assert) — a grammar operator added
// without an inventory entry surfaces here as an out-of-inventory Op.
func TestGrammarLeafOps_EveryOpCompilesAndEmits(t *testing.T) {
	infos := map[string]ColumnInfo{"id": {Family: FamilyNumeric}}
	// One predicate per inventory op, canonical spelling.
	preds := map[string]string{
		"=":           "id = 1",
		"!=":          "id != 1",
		"<":           "id < 1",
		"<=":          "id <= 1",
		">":           "id > 1",
		">=":          "id >= 1",
		"IN":          "id IN (1, 2)",
		"NOT IN":      "id NOT IN (1, 2)",
		"IS NULL":     "id IS NULL",
		"IS NOT NULL": "id IS NOT NULL",
	}
	inventory := map[string]bool{}
	for _, op := range GrammarLeafOps() {
		inventory[op] = true
		pred, ok := preds[op]
		if !ok {
			t.Errorf("GrammarLeafOps() lists %q but this pin has no predicate for it — extend the preds map (and the oracle matrix)", op)
			continue
		}
		p, err := Compile("t", pred, infos)
		if err != nil {
			t.Errorf("inventory op %q does not compile (%q): %v — drop the stale inventory entry", op, pred, err)
			continue
		}
		terms := p.PushdownTerms()
		if len(terms) != 1 || terms[0].Op != op {
			t.Errorf("Compile(%q) emitted terms %+v; want exactly one term with Op %q", pred, terms, op)
		}
	}
	// The alternate `<>` spelling must FOLD into the canonical inventory,
	// not extend it.
	p, err := Compile("t", "id <> 1", infos)
	if err != nil {
		t.Fatalf("Compile(id <> 1): %v", err)
	}
	if got := p.PushdownTerms()[0].Op; got != "!=" {
		t.Errorf("`<>` compiled to Op %q; want the canonical \"!=\"", got)
	}
	if len(preds) != len(inventory) {
		t.Errorf("pin has %d predicates for %d inventory ops — keep them 1:1", len(preds), len(inventory))
	}
}
