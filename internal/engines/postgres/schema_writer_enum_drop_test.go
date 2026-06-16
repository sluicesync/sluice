// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Unit tests for the Bug 150 orphaned-enum-type drop guard. The live
// drop-then-readd round-trip is pinned by the integration test; this pins the
// load-bearing DECISION: which enum types are safe to auto-drop on a forwarded
// DROP COLUMN.

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestOrphanedEnumTypeDrop(t *testing.T) {
	w := &SchemaWriter{schema: "public"}
	tbl := &ir.Table{Name: "widgets"}

	// SYNTHESIZED per-column type (MySQL source: no enum type identity →
	// TypeName=="") → safe to drop the dedicated "<table>_<col>_enum".
	t.Run("synthesized enum → drop", func(t *testing.T) {
		col := &ir.Column{Name: "status", Type: ir.Enum{Values: []string{"a", "b"}}}
		stmt, ok := w.orphanedEnumTypeDrop(tbl, col)
		if !ok {
			t.Fatal("expected a DROP TYPE for a synthesized per-column enum")
		}
		if want := `DROP TYPE IF EXISTS "public"."widgets_status_enum"`; stmt != want {
			t.Errorf("stmt = %q; want %q", stmt, want)
		}
	})

	// PRESERVED same-engine-PG type (TypeName set) — may be SHARED across
	// columns/tables (catalog Bug 19c) → NEVER auto-dropped here.
	t.Run("preserved PG type → skip (shared-safety)", func(t *testing.T) {
		col := &ir.Column{Name: "status", Type: ir.Enum{Values: []string{"a", "b"}, TypeName: "mood"}}
		if stmt, ok := w.orphanedEnumTypeDrop(tbl, col); ok {
			t.Errorf("preserved PG enum type must NOT be auto-dropped; got %q", stmt)
		}
	})

	// Non-enum column → no DROP TYPE.
	t.Run("non-enum → skip", func(t *testing.T) {
		col := &ir.Column{Name: "n", Type: ir.Integer{Width: 32}}
		if _, ok := w.orphanedEnumTypeDrop(tbl, col); ok {
			t.Error("non-enum column must not yield a DROP TYPE")
		}
	})
}
