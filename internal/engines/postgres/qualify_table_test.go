// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestQualifyTable_TargetSchemaWins pins the rule every post-create ALTER
// on the Postgres writer rides: an explicit `--target-schema` re-homes the
// table under the writer's schema, so the IR's source schema must NOT win
// the qualification. GC-3's identity restore issued
// `ALTER TABLE "public"."orders"` on a `--target-schema customer_svc`
// migrate and failed 42P01 (CI on 0d24fd53); the helper is shared by the
// ADR-0054 shape-delta and CHECK appliers, so the fix and this pin sit on
// the helper, not on one caller. Without an override the IR's own schema
// still wins, which is what a multi-schema source needs.
func TestQualifyTable_TargetSchemaWins(t *testing.T) {
	table := &ir.Table{Schema: "public", Name: "orders"}

	// No override: the IR's schema qualifies the table.
	w := &SchemaWriter{schema: "public"}
	if got, want := w.qualifyTable(table), `"public"."orders"`; got != want {
		t.Errorf("no override: qualifyTable = %s; want %s", got, want)
	}
	w.schema = "ignored_default"
	if got, want := w.qualifyTable(table), `"public"."orders"`; got != want {
		t.Errorf("no override, non-default writer schema: qualifyTable = %s; want %s (IR schema wins)", got, want)
	}

	// Explicit --target-schema: the writer's schema qualifies the table,
	// whatever the IR says.
	w = &SchemaWriter{}
	w.SetSchema("customer_svc")
	if got, want := w.qualifyTable(table), `"customer_svc"."orders"`; got != want {
		t.Errorf("--target-schema: qualifyTable = %s; want %s (the IR's %q must not win)", got, want, table.Schema)
	}

	// A table with no IR schema falls to the writer's schema either way.
	bare := &ir.Table{Name: "orders"}
	if got, want := w.qualifyTable(bare), `"customer_svc"."orders"`; got != want {
		t.Errorf("bare table under --target-schema: qualifyTable = %s; want %s", got, want)
	}
	w = &SchemaWriter{schema: "public"}
	if got, want := w.qualifyTable(bare), `"public"."orders"`; got != want {
		t.Errorf("bare table, no override: qualifyTable = %s; want %s", got, want)
	}
}
