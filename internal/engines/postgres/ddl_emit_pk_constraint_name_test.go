// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 84 — the PRIMARY KEY constraint-NAME carry, pinned as a
// matrix of SOURCE ENGINE x PK SHAPE rather than one representative.
//
// The axis that matters is not "does the name round-trip" but "does the
// recorded name carry operator intent". A Postgres PK constraint and its
// backing index are one named catalog object, so the name is real; a
// MySQL PK index is literally named `PRIMARY`, a fixed sentinel shared by
// every table, so re-emitting it would INVENT a name on the target; a
// SQLite PK has no name at all. Those three source shapes reach the same
// emitter and must produce three different results, which is exactly why
// one green cell is not evidence for the others.
//
// These drive PreviewDDL — the same function `sluice schema preview`
// calls and the same emitTableDef the live CreateTablesWithoutConstraints
// path uses — rather than emitTableDef directly, so the writer's own
// dispatch is part of what is pinned.

package postgres

import (
	"context"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// pkNameTable builds a one-table schema whose PK carries the given name /
// ConstraintNamed / column list. Everything else is deliberately plain so
// the assertion is about the PK clause alone.
func pkNameTable(table, pkName string, named bool, cols ...string) *ir.Schema {
	pk := &ir.Index{Name: pkName, Unique: true, ConstraintNamed: named}
	columns := make([]*ir.Column, 0, len(cols))
	for _, c := range cols {
		pk.Columns = append(pk.Columns, ir.IndexColumn{Column: c})
		columns = append(columns, &ir.Column{Name: c, Type: ir.Integer{Width: 64}})
	}
	return &ir.Schema{Tables: []*ir.Table{{
		Name:       table,
		Columns:    columns,
		PrimaryKey: pk,
	}}}
}

// previewCreateTable returns the CREATE TABLE statement PreviewDDL emits
// for the single table in s.
func previewCreateTable(t *testing.T, s *ir.Schema) string {
	t.Helper()
	w := &SchemaWriter{schema: "public"}
	stmts, err := w.PreviewDDL(context.Background(), s)
	if err != nil {
		t.Fatalf("PreviewDDL: %v", err)
	}
	for _, st := range stmts {
		if st.Kind == "CREATE TABLE" {
			return st.SQL
		}
	}
	t.Fatalf("PreviewDDL emitted no CREATE TABLE (got %d statements)", len(stmts))
	return ""
}

func TestPreviewDDL_PrimaryKeyConstraintNameMatrix(t *testing.T) {
	cases := []struct {
		name string
		// source is the source engine whose reader shape this row
		// imitates — recorded so a failure names the cell.
		source    string
		schema    *ir.Schema
		want      string
		notWant   []string
		rationale string
	}{
		{
			name:      "PG source, explicitly-named single-column PK",
			source:    "postgres",
			schema:    pkNameTable("orders", "orders_pk", true, "id"),
			want:      `CONSTRAINT "orders_pk" PRIMARY KEY ("id")`,
			rationale: "the operator wrote CONSTRAINT orders_pk; ON CONFLICT ON CONSTRAINT orders_pk must keep working on the target",
		},
		{
			name:      "PG source, explicitly-named composite PK",
			source:    "postgres",
			schema:    pkNameTable("line_items", "pk_line_items", true, "order_id", "sku"),
			want:      `CONSTRAINT "pk_line_items" PRIMARY KEY ("order_id", "sku")`,
			rationale: "a composite key is a different emit branch (multi-column list) and a prefix-shaped name (pk_ leading) proves the pgIndexName table-prefixing is NOT applied",
		},
		{
			name:      "PG source, auto-named single-column PK",
			source:    "postgres",
			schema:    pkNameTable("orders", "orders_pkey", true, "id"),
			want:      `CONSTRAINT "orders_pkey" PRIMARY KEY ("id")`,
			rationale: "PG's own default name re-emits verbatim: the target lands the identical name it would have chosen, so no heuristic guesses whether the name was chosen",
		},
		{
			name:      "PG source, auto-named composite PK",
			source:    "postgres",
			schema:    pkNameTable("attr_parent", "attr_parent_pkey", true, "a", "b"),
			want:      `CONSTRAINT "attr_parent_pkey" PRIMARY KEY ("a", "b")`,
			rationale: "auto-named x composite is its own cell",
		},
		{
			name:      "MySQL source, single-column PK",
			source:    "mysql",
			schema:    pkNameTable("orders", "PRIMARY", false, "id"),
			want:      `PRIMARY KEY ("id")`,
			notWant:   []string{"CONSTRAINT"},
			rationale: "MySQL's PK index name is the fixed sentinel PRIMARY; carrying it would emit CONSTRAINT \"PRIMARY\" — a name no operator chose",
		},
		{
			name:      "MySQL source, composite PK",
			source:    "mysql",
			schema:    pkNameTable("line_items", "PRIMARY", false, "order_id", "sku"),
			want:      `PRIMARY KEY ("order_id", "sku")`,
			notWant:   []string{"CONSTRAINT"},
			rationale: "the sentinel is shared by every table regardless of key arity",
		},
		{
			name:      "SQLite source, unnamed PK",
			source:    "sqlite",
			schema:    pkNameTable("orders", "", false, "id"),
			want:      `PRIMARY KEY ("id")`,
			notWant:   []string{"CONSTRAINT"},
			rationale: "SQLite's PK has no name at all; the reader leaves both Name and the flag zero",
		},
		{
			name:   "defensive: ConstraintNamed set but Name empty",
			source: "(hand-built IR)",
			schema: pkNameTable("orders", "", true, "id"),
			want:   `PRIMARY KEY ("id")`,
			notWant: []string{
				"CONSTRAINT",
			},
			rationale: "an empty name cannot be emitted as an identifier; fall back to the inline form rather than producing CONSTRAINT \"\"",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := previewCreateTable(t, c.schema)
			if !strings.Contains(got, c.want) {
				t.Errorf("[source=%s] CREATE TABLE missing %q (%s)\ngot:\n%s", c.source, c.want, c.rationale, got)
			}
			for _, nw := range c.notWant {
				if strings.Contains(got, nw) {
					t.Errorf("[source=%s] CREATE TABLE contains %q, which it must not (%s)\ngot:\n%s", c.source, nw, c.rationale, got)
				}
			}
		})
	}
}

// TestPreviewDDL_PrimaryKeyConstraintNameWithDeferrable pins the
// interaction with the Bug 208 / SL-8 attribute carry: the name prefixes
// the clause and the DEFERRABLE suffix still lands after the column list.
// Two independent carries on one clause is exactly the shape that breaks
// when one is bolted on without re-reading the other.
func TestPreviewDDL_PrimaryKeyConstraintNameWithDeferrable(t *testing.T) {
	s := pkNameTable("attr_defpk", "attr_defpk_pk", true, "a", "b")
	s.Tables[0].PrimaryKey.ConstraintDeferrable = true
	s.Tables[0].PrimaryKey.ConstraintInitiallyDeferred = true

	got := previewCreateTable(t, s)
	const want = `CONSTRAINT "attr_defpk_pk" PRIMARY KEY ("a", "b") DEFERRABLE INITIALLY DEFERRED`
	if !strings.Contains(got, want) {
		t.Errorf("CREATE TABLE missing %q\ngot:\n%s", want, got)
	}
}

// TestPreviewDDL_PrimaryKeyConstraintNameOverLength pins the roadmap
// item 43 refusal at this new call site: a carried name past PG's 63-byte
// identifier limit is REFUSED, not silently truncated into a different
// constraint name. Unreachable from a PG source (its catalog cannot hold
// one), so this guards a hand-built or future-engine IR.
func TestPreviewDDL_PrimaryKeyConstraintNameOverLength(t *testing.T) {
	long := strings.Repeat("p", maxPGIdentifierLen+1)
	w := &SchemaWriter{schema: "public"}
	_, err := w.PreviewDDL(context.Background(), pkNameTable("orders", long, true, "id"))
	if err == nil {
		t.Fatal("PreviewDDL accepted a PK constraint name over PG's identifier limit — PG would silently truncate it")
	}
	if !strings.Contains(err.Error(), long) || !strings.Contains(err.Error(), "orders") {
		t.Errorf("refusal names neither the offending name nor the table: %v", err)
	}

	// The over-length guard must not fire on the historical unnamed path:
	// a MySQL-source PK is not emitted as an identifier at all, so its
	// name length is irrelevant.
	if _, err := w.PreviewDDL(context.Background(), pkNameTable("orders", long, false, "id")); err != nil {
		t.Errorf("PreviewDDL refused an over-length name that is never emitted: %v", err)
	}
}
