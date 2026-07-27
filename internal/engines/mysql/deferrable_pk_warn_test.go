// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Pin for Bug 210 — a DEFERRABLE PRIMARY KEY against a MySQL-family target
// must WARN, like its FOREIGN KEY and UNIQUE siblings already do.
//
// InnoDB has no deferred-constraint concept, so the attribute genuinely cannot
// be carried. That is not the finding. The finding is that it vanished in
// SILENCE while the other two constraint kinds — in the same schema, in the
// same migrate run — both warned. The consequence is the one the attribute
// exists for: `UPDATE t SET id = id + 1` commits on the source and fails
// partway through on the target with a duplicate-key error.
//
// This is the half a Postgres-target check structurally cannot see, which is
// why the fix for the primary-key carry shipped looking complete.
package mysql

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// captureWarnings swaps the default slog handler for one writing to a buffer.
func captureWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return &buf
}

func deferrablePKTable(deferrable, initiallyDeferred bool) *ir.Table {
	return &ir.Table{
		Name: "orders",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "note", Type: ir.Varchar{Length: 64}},
		},
		PrimaryKey: &ir.Index{
			Name:                        "orders_pk",
			Unique:                      true,
			Columns:                     []ir.IndexColumn{{Column: "id"}},
			ConstraintBacked:            false, // a PK re-emits as PRIMARY KEY, not ADD CONSTRAINT
			ConstraintDeferrable:        deferrable,
			ConstraintInitiallyDeferred: initiallyDeferred,
		},
	}
}

func TestEmitCreateTable_WarnsOnDeferrablePrimaryKey(t *testing.T) {
	t.Run("deferrable PK warns and names the consequence", func(t *testing.T) {
		logs := captureWarnings(t)
		if _, err := emitTableDef(deferrablePKTable(true, true)); err != nil {
			t.Fatalf("emitCreateTable: %v", err)
		}
		out := logs.String()
		if !strings.Contains(out, "primary-key attribute cannot be represented") {
			t.Fatalf("a DEFERRABLE PRIMARY KEY was dropped with NO warning, while a DEFERRABLE UNIQUE and a "+
				"DEFERRABLE FOREIGN KEY in the same run both warn. The bulk key shift UPDATE t SET id = id + 1 "+
				"commits on the source and fails on the target, and nothing said so (Bug 210).\nlogs:\n%s", out)
		}
		for _, want := range []string{"orders", "orders_pk", "duplicate-key"} {
			if !strings.Contains(out, want) {
				t.Errorf("the WARN must name %q so the operator can act; logs:\n%s", want, out)
			}
		}
	})

	t.Run("plain PK is silent", func(t *testing.T) {
		logs := captureWarnings(t)
		if _, err := emitTableDef(deferrablePKTable(false, false)); err != nil {
			t.Fatalf("emitCreateTable: %v", err)
		}
		if strings.Contains(logs.String(), "primary-key attribute cannot be represented") {
			t.Errorf("an ordinary PRIMARY KEY produced the deferrable warning — a WARN on every table would "+
				"train operators to ignore it; logs:\n%s", logs.String())
		}
	})
}
