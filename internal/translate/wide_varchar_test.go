// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// pgWideVarcharSchema models a PG source mixing a narrow varchar (must
// NOT be flagged), a boundary-width varchar at the inline cap (must NOT
// be flagged), and two wide varchars over the cap (Bug 72 — flagged).
func pgWideVarcharSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{
			Name: "docs",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "title", Type: ir.Varchar{Length: 255}},                      // narrow — not flagged
				{Name: "slug", Type: ir.Varchar{Length: wideVarcharThresholdChars}}, // boundary — not flagged
				{Name: "body", Type: ir.Varchar{Length: 16384}},                     // wide — flagged
			},
		},
		{
			Name: "blobs",
			Columns: []*ir.Column{
				{Name: "payload", Type: ir.Varchar{Length: 70000}}, // wide — flagged
			},
		},
	}}
}

func TestScanWideVarcharNotices_Shape(t *testing.T) {
	got := ScanWideVarcharNotices(pgWideVarcharSchema(), "postgres", "mysql")
	want := []WideVarcharNotice{
		{Table: "blobs", Column: "payload", Length: 70000},
		{Table: "docs", Column: "body", Length: 16384},
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

func TestScanWideVarcharNotices_NonCrossEngineIsNil(t *testing.T) {
	// PG → PG: varchar round-trips unchanged → no notice.
	if got := ScanWideVarcharNotices(pgWideVarcharSchema(), "postgres", "postgres"); got != nil {
		t.Errorf("PG→PG notices = %+v; want nil", got)
	}
	if got := ScanWideVarcharNotices(pgWideVarcharSchema(), "mysql", "postgres"); got != nil {
		t.Errorf("MySQL→PG notices = %+v; want nil", got)
	}
	if got := ScanWideVarcharNotices(nil, "postgres", "mysql"); got != nil {
		t.Errorf("nil-schema notices = %+v; want nil", got)
	}
}

func TestScanWideVarcharNotices_PlanetScaleTargetCovered(t *testing.T) {
	if got := ScanWideVarcharNotices(pgWideVarcharSchema(), "postgres", "planetscale"); len(got) != 2 {
		t.Errorf("PG→planetscale notices = %d; want 2 (PS is MySQL-wire)", len(got))
	}
}

func TestWideVarcharNoticeError_LoudAndActionable(t *testing.T) {
	err := WideVarcharNoticeError(pgWideVarcharSchema(), "postgres", "mysql", "migrate")
	if err == nil {
		t.Fatal("WideVarcharNoticeError = nil; want a non-nil advisory")
	}
	msg := err.Error()
	for _, want := range []string{
		"migrate",            // contextID surfaced
		"varchar",            // the source type named
		"TEXT",               // the target family named
		"deliberate",         // documented policy, not a bug
		"Migration proceeds", // a NOTICE, not a refusal
		"--type-override",    // the escape hatch
		"docs.body",          // names an affected column
		"blobs.payload",      // names the other affected column
		"varchar(16384)",     // the declared length surfaced
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("notice message missing %q\nfull message:\n%s", want, msg)
		}
	}
	if WideVarcharNoticeError(pgWideVarcharSchema(), "postgres", "postgres", "migrate") != nil {
		t.Error("PG→PG WideVarcharNoticeError != nil; want nil")
	}
	clean := &ir.Schema{Tables: []*ir.Table{{
		Name:    "t",
		Columns: []*ir.Column{{Name: "title", Type: ir.Varchar{Length: 255}}},
	}}}
	if WideVarcharNoticeError(clean, "postgres", "mysql", "schema preview") != nil {
		t.Error("clean-schema WideVarcharNoticeError != nil; want nil")
	}
}
