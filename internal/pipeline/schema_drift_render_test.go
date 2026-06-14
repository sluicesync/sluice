// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
	"testing"

	irdiff "sluicesync.dev/sluice/internal/ir/diff"
)

// TestRenderSchemaDriftReport_Empty verifies the no-changes
// short-circuit — caller short-circuits via HasChanges() in practice
// but the renderer is defensive.
func TestRenderSchemaDriftReport_Empty(t *testing.T) {
	got := RenderSchemaDriftReport(irdiff.SchemaDriftReport{})
	if got != "" {
		t.Errorf("RenderSchemaDriftReport({}) = %q; want \"\"", got)
	}
}

// TestRenderSchemaDriftReport_ColumnAdded verifies that the rendered
// "column-added" line includes the type, nullability, AND the
// operator-action hint. Under ADR-0091 a standalone ADD COLUMN
// auto-forwards, so this line appears only in a multi-shape combo
// refusal; the hint points to drained recovery for the whole combo.
func TestRenderSchemaDriftReport_ColumnAdded(t *testing.T) {
	r := irdiff.SchemaDriftReport{
		ColumnsAdded: []irdiff.ColumnDriftEntry{
			{Name: "created_at", Type: "Timestamp", Nullable: true, Default: "<none>"},
		},
	}
	got := RenderSchemaDriftReport(r)
	for _, want := range []string{
		"[column-added]",
		"created_at",
		"Timestamp",
		"NULL",
		"drained schema migrate",
		"auto-forwards by default",
		"ADR-0091",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered %q missing %q", got, want)
		}
	}
}

// TestRenderSchemaDriftReport_ColumnDropped verifies that drop's
// operator action explicitly mentions "destructive, no auto-forwarding"
// — operators must NOT believe a drop will be auto-forwarded.
func TestRenderSchemaDriftReport_ColumnDropped(t *testing.T) {
	r := irdiff.SchemaDriftReport{
		ColumnsDropped: []irdiff.ColumnDriftEntry{
			{Name: "legacy", Type: "Text", Nullable: true, Default: "<none>"},
		},
	}
	got := RenderSchemaDriftReport(r)
	for _, want := range []string{
		"[column-dropped]",
		"legacy",
		"destructive",
		"no auto-forwarding",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered %q missing %q", got, want)
		}
	}
}

// TestRenderSchemaDriftReport_AlteredMultiKind verifies that a
// multi-kind alter (type + nullability changed together) surfaces
// BOTH changes on the same line — operators see the full picture.
func TestRenderSchemaDriftReport_AlteredMultiKind(t *testing.T) {
	r := irdiff.SchemaDriftReport{
		ColumnsAltered: []irdiff.ColumnAlterEntry{
			{
				Name:   "score",
				Before: irdiff.ColumnDriftEntry{Name: "score", Type: "Integer(32)", Nullable: true},
				After:  irdiff.ColumnDriftEntry{Name: "score", Type: "Integer(64)", Nullable: false},
				AlterKinds: []irdiff.ColumnAlterKind{
					irdiff.ColumnAlterType,
					irdiff.ColumnAlterNullable,
				},
			},
		},
	}
	got := RenderSchemaDriftReport(r)
	for _, want := range []string{
		"[column-altered]",
		"score",
		"Integer(32)",
		"Integer(64)",
		"NULL",
		"NOT NULL",
		"type",
		"nullability",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered %q missing %q", got, want)
		}
	}
}

// TestRenderSchemaDriftReport_Renamed verifies rename rendering and
// the operator hint that rename is NOT auto-forwarded.
func TestRenderSchemaDriftReport_Renamed(t *testing.T) {
	r := irdiff.SchemaDriftReport{
		ColumnsRenamed: []irdiff.ColumnRenameEntry{
			{OldName: "email_old", NewName: "email", Type: "Varchar(255)"},
		},
	}
	got := RenderSchemaDriftReport(r)
	for _, want := range []string{
		"[column-renamed]",
		"email_old",
		"email",
		"Varchar(255)",
		"not auto-forwarded",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered %q missing %q", got, want)
		}
	}
}

// TestRenderSchemaDriftReport_IndexShapes verifies index add and
// drop lines render distinctly (the operator-action hint differs:
// add mentions "concurrent rebuild needs operator scheduling", drop
// just calls for drained migrate).
func TestRenderSchemaDriftReport_IndexShapes(t *testing.T) {
	r := irdiff.SchemaDriftReport{
		IndexesAdded: []irdiff.IndexDriftEntry{
			{Name: "ix_users_email", Columns: "email", Unique: true},
		},
		IndexesDropped: []irdiff.IndexDriftEntry{
			{Name: "ix_users_legacy", Columns: "legacy", Unique: false},
		},
	}
	got := RenderSchemaDriftReport(r)
	for _, want := range []string{
		"[index-added]",
		"ix_users_email",
		"unique index",
		"concurrent rebuild",
		"[index-dropped]",
		"ix_users_legacy",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered %q missing %q", got, want)
		}
	}
}

// TestRenderSchemaDriftReport_GreppablePrefixes verifies the per-
// category bracketed prefix contract — operators paste rendered
// output into ticket triage flows and grep on category names.
// Every drift category must lead with "[<category>]".
func TestRenderSchemaDriftReport_GreppablePrefixes(t *testing.T) {
	r := irdiff.SchemaDriftReport{
		ColumnsAdded:       []irdiff.ColumnDriftEntry{{Name: "a", Type: "T", Default: "<none>"}},
		ColumnsDropped:     []irdiff.ColumnDriftEntry{{Name: "b", Type: "T", Default: "<none>"}},
		ColumnsRenamed:     []irdiff.ColumnRenameEntry{{OldName: "c", NewName: "c2", Type: "T"}},
		ColumnsAltered:     []irdiff.ColumnAlterEntry{{Name: "d", AlterKinds: []irdiff.ColumnAlterKind{irdiff.ColumnAlterType}, Before: irdiff.ColumnDriftEntry{Type: "T1"}, After: irdiff.ColumnDriftEntry{Type: "T2"}}},
		IndexesAdded:       []irdiff.IndexDriftEntry{{Name: "ix1", Columns: "x"}},
		IndexesDropped:     []irdiff.IndexDriftEntry{{Name: "ix2", Columns: "y"}},
		ChecksAdded:        []irdiff.CheckDriftEntry{{Name: "ck1", Expr: "x > 0"}},
		ChecksDropped:      []irdiff.CheckDriftEntry{{Name: "ck2", Expr: "y > 0"}},
		ChecksAltered:      []irdiff.CheckAlterEntry{{Name: "ck3", BeforeExpr: "z > 0", AfterExpr: "z >= 0"}},
		ForeignKeysAdded:   []irdiff.ForeignKeyDriftEntry{{Name: "fk1", Columns: "x", ReferencedTable: "t", ReferencedColumns: "id"}},
		ForeignKeysDropped: []irdiff.ForeignKeyDriftEntry{{Name: "fk2", Columns: "y", ReferencedTable: "u", ReferencedColumns: "id"}},
		ForeignKeysAltered: []irdiff.ForeignKeyAlterEntry{{Name: "fk3", Before: irdiff.ForeignKeyDriftEntry{Columns: "a"}, After: irdiff.ForeignKeyDriftEntry{Columns: "b"}}},
	}
	got := RenderSchemaDriftReport(r)
	wantPrefixes := []string{
		"[column-added]",
		"[column-dropped]",
		"[column-renamed]",
		"[column-altered]",
		"[index-added]",
		"[index-dropped]",
		"[check-added]",
		"[check-dropped]",
		"[check-altered]",
		"[fk-added]",
		"[fk-dropped]",
		"[fk-altered]",
	}
	for _, prefix := range wantPrefixes {
		if !strings.Contains(got, prefix) {
			t.Errorf("rendered output missing grep-prefix %q", prefix)
		}
	}
}

// TestRenderSchemaDriftReport_LineSeparators verifies the multi-
// entry rendering uses "\n  " separators — operators expect one
// entry per line for diff/grep workflows.
func TestRenderSchemaDriftReport_LineSeparators(t *testing.T) {
	r := irdiff.SchemaDriftReport{
		ColumnsAdded: []irdiff.ColumnDriftEntry{
			{Name: "a", Type: "T", Default: "<none>"},
			{Name: "b", Type: "T", Default: "<none>"},
		},
	}
	got := RenderSchemaDriftReport(r)
	if !strings.HasPrefix(got, "\n  ") {
		t.Errorf("rendered output does not start with \\n   indent: %q", got)
	}
	if strings.Count(got, "[column-added]") != 2 {
		t.Errorf("rendered output has %d column-added entries; want 2", strings.Count(got, "[column-added]"))
	}
}
