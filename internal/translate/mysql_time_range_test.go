// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// timeSchema models the Bug 187 repro shape: a TIME(6) duration column
// plus columns that must NOT be flagged (a date, a datetime, and a
// column the operator already overrode to interval — the post-override
// suppression convention).
func timeSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{
		{
			Name: "shifts",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
				{Name: "elapsed", Type: ir.Time{Precision: 6}},
				{Name: "start_day", Type: ir.Date{}},                 // not TIME → not flagged
				{Name: "logged_at", Type: ir.DateTime{Precision: 6}}, // not TIME → not flagged
				{Name: "overridden", Type: ir.Interval{}},            // post-override → not flagged
			},
		},
		{
			Name: "tfam",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "t", Type: ir.Time{Precision: 6}},
			},
		},
	}}
}

func TestScanMySQLTimeRangeNotices_FlagsTimeColumns(t *testing.T) {
	got := ScanMySQLTimeRangeNotices(timeSchema(), "mysql", "postgres")
	// Sorted by (table, column): shifts.elapsed, tfam.t.
	want := []MySQLTimeRangeNotice{
		{Table: "shifts", Column: "elapsed"},
		{Table: "tfam", Column: "t"},
	}
	if len(got) != len(want) {
		t.Fatalf("notices = %+v; want %+v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("notices[%d] = %+v; want %+v", i, got[i], w)
		}
	}
}

// TestScanMySQLTimeRangeNotices_MySQLFamilyMatrix pins the class, not
// the representative (the Bug 186 lesson at the advisory layer): every
// MySQL-family SOURCE name must trigger the scanner toward postgres,
// and no non-family source or non-PG target may.
func TestScanMySQLTimeRangeNotices_MySQLFamilyMatrix(t *testing.T) {
	for _, src := range []string{"mysql", "planetscale", "vitess", "mydumper"} {
		if got := ScanMySQLTimeRangeNotices(timeSchema(), src, "postgres"); len(got) != 2 {
			t.Errorf("%s→postgres notices = %d; want 2 (every MySQL-family source is covered)", src, len(got))
		}
	}
	if got := ScanMySQLTimeRangeNotices(timeSchema(), "mysql", "mysql"); got != nil {
		t.Errorf("mysql→mysql notices = %+v; want nil (same dialect, TIME round-trips)", got)
	}
	if got := ScanMySQLTimeRangeNotices(timeSchema(), "postgres", "postgres"); got != nil {
		t.Errorf("postgres→postgres notices = %+v; want nil (PG time is already a time-of-day)", got)
	}
	if got := ScanMySQLTimeRangeNotices(timeSchema(), "sqlite", "postgres"); got != nil {
		t.Errorf("sqlite→postgres notices = %+v; want nil (not a MySQL-family source)", got)
	}
	if got := ScanMySQLTimeRangeNotices(nil, "mysql", "postgres"); got != nil {
		t.Errorf("nil-schema notices = %+v; want nil", got)
	}
}

func TestMySQLTimeRangeNoticeError_LoudAndActionable(t *testing.T) {
	err := MySQLTimeRangeNoticeError(timeSchema(), "mysql", "postgres", "migrate")
	if err == nil {
		t.Fatal("MySQLTimeRangeNoticeError = nil; want a non-nil advisory")
	}
	msg := err.Error()
	for _, want := range []string{
		"migrate",               // contextID surfaced
		"MySQL TIME",            // the source type named
		"`time`",                // the target type named
		"-838:59:59..838:59:59", // the source range stated
		"24:00:00",              // the target ceiling stated
		"DURATION",              // the semantic mismatch named
		"REFUSE loudly",         // out-of-range fails loud, never clamps
		"Migration proceeds",    // it's a NOTICE, not a refusal
		"--type-override",       // the escape hatch
		"=interval",             // the lossless remediation token
		"shifts.elapsed",        // names an affected column
		"tfam.t",                // names the repro column
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("notice message missing %q\nfull message:\n%s", want, msg)
		}
	}
	// Non-cross-engine pair → nil.
	if MySQLTimeRangeNoticeError(timeSchema(), "mysql", "mysql", "migrate") != nil {
		t.Error("same-engine MySQLTimeRangeNoticeError != nil; want nil")
	}
	// Schema with no TIME column → nil.
	clean := &ir.Schema{Tables: []*ir.Table{{
		Name:    "t",
		Columns: []*ir.Column{{Name: "d", Type: ir.Date{}}},
	}}}
	if MySQLTimeRangeNoticeError(clean, "mysql", "postgres", "sync cold-start") != nil {
		t.Error("clean-schema MySQLTimeRangeNoticeError != nil; want nil")
	}
}
