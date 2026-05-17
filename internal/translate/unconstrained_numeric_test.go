// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// pgNumericSchema models a PG source mixing an unconstrained `numeric`
// (Bug 69), a bounded numeric (must NOT be flagged), and a
// `numeric[]` (lands as MySQL JSON — out of scope, must NOT be flagged
// since the scanner only sees scalar Decimal columns).
func pgNumericSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{
			Name: "ledger",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "amount", Type: ir.Decimal{Unconstrained: true}},
				{Name: "rate", Type: ir.Decimal{Precision: 15, Scale: 2}}, // bounded — not flagged
			},
		},
		{
			Name: "audit",
			Columns: []*ir.Column{
				{Name: "balance", Type: ir.Decimal{Unconstrained: true}},
				{Name: "tags", Type: ir.Array{Element: ir.Decimal{Unconstrained: true}}}, // array → JSON, not flagged
			},
		},
	}}
}

func TestScanUnconstrainedNumericNotices_Shape(t *testing.T) {
	got := ScanUnconstrainedNumericNotices(pgNumericSchema(), "postgres", "mysql")
	// audit.balance, ledger.amount — the two scalar unconstrained
	// numerics; `rate` is bounded, `tags` is an array (→ JSON).
	want := []UnconstrainedNumericNotice{
		{Table: "audit", Column: "balance"},
		{Table: "ledger", Column: "amount"},
	}
	if len(got) != len(want) {
		t.Fatalf("notices = %d (%+v); want %d", len(got), got, len(want))
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("notices[%d] = %+v; want %+v", i, got[i], w)
		}
	}
}

func TestScanUnconstrainedNumericNotices_NonCrossEngineIsNil(t *testing.T) {
	// PG → PG: unconstrained numeric round-trips as bare NUMERIC, no
	// narrowing → no notice.
	if got := ScanUnconstrainedNumericNotices(pgNumericSchema(), "postgres", "postgres"); got != nil {
		t.Errorf("PG→PG notices = %+v; want nil", got)
	}
	// MySQL → PG: reverse direction unaffected.
	if got := ScanUnconstrainedNumericNotices(pgNumericSchema(), "mysql", "postgres"); got != nil {
		t.Errorf("MySQL→PG notices = %+v; want nil", got)
	}
	if got := ScanUnconstrainedNumericNotices(nil, "postgres", "mysql"); got != nil {
		t.Errorf("nil-schema notices = %+v; want nil", got)
	}
}

func TestScanUnconstrainedNumericNotices_BoundedUnaffected(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "t",
		Columns: []*ir.Column{
			{Name: "n0", Type: ir.Decimal{Precision: 0, Scale: 0}},   // Decimal(0,0) — bounded, NOT unconstrained
			{Name: "n15", Type: ir.Decimal{Precision: 15, Scale: 2}}, // bounded
			{Name: "u", Type: ir.Decimal{Unconstrained: true}},       // the only flagged one
		},
	}}}
	got := ScanUnconstrainedNumericNotices(s, "postgres", "mysql")
	if len(got) != 1 || got[0].Column != "u" {
		t.Fatalf("notices = %+v; want exactly [t.u]", got)
	}
}

func TestScanUnconstrainedNumericNotices_PlanetScaleTargetCovered(t *testing.T) {
	if got := ScanUnconstrainedNumericNotices(pgNumericSchema(), "postgres", "planetscale"); len(got) != 2 {
		t.Errorf("PG→planetscale notices = %d; want 2 (PS is MySQL-wire)", len(got))
	}
}

func TestUnconstrainedNumericNoticeError_LoudAndActionable(t *testing.T) {
	err := UnconstrainedNumericNoticeError(pgNumericSchema(), "postgres", "mysql", "migrate")
	if err == nil {
		t.Fatal("UnconstrainedNumericNoticeError = nil; want a non-nil advisory")
	}
	msg := err.Error()
	for _, want := range []string{
		"migrate",            // contextID surfaced
		"numeric",            // the source type named
		"DECIMAL(65,30)",     // the target type named
		"deliberate",         // documented policy, not a bug
		"Migration proceeds", // a NOTICE, not a refusal
		"--type-override",    // the escape hatch
		"ledger.amount",      // names an affected column
		"audit.balance",      // names the other affected column
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("notice message missing %q\nfull message:\n%s", want, msg)
		}
	}
	// Non-cross-engine pair → nil.
	if UnconstrainedNumericNoticeError(pgNumericSchema(), "postgres", "postgres", "migrate") != nil {
		t.Error("PG→PG UnconstrainedNumericNoticeError != nil; want nil")
	}
	// Schema with no unconstrained-numeric column → nil.
	clean := &ir.Schema{Tables: []*ir.Table{{
		Name:    "t",
		Columns: []*ir.Column{{Name: "rate", Type: ir.Decimal{Precision: 15, Scale: 2}}},
	}}}
	if UnconstrainedNumericNoticeError(clean, "postgres", "mysql", "schema preview") != nil {
		t.Error("clean-schema UnconstrainedNumericNoticeError != nil; want nil")
	}
}
