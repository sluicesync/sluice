//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SLP-2 (audit 2026-09-01): the `ddl_command_end` tier was database-wide
// and tag-only, so DDL on a relation this install never captures recorded
// an op='X' marker and HALTED the stream with the restart-from-scratch
// remedy. A Postgres event trigger cannot be scoped to a schema, so the
// scope has to come from the predicate: the command's relation must carry
// this install's row-capture trigger.
//
// Both directions are pinned here, because a filter is only as good as
// what it still lets through: every shape on a CAPTURED table must keep
// recording (a missed ALTER is the silent class the tier exists for), and
// every shape off it must record nothing.

package pgtrigger

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestCaptureDDLTier_RecordsOnlyCapturedRelations(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mustExec := func(stmt string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("exec %q: %v", stmt, err)
		}
	}

	// One captured table, one uncaptured table beside it, and a whole
	// schema sluice never hears about.
	mustExec(`CREATE TABLE public.captured (id int PRIMARY KEY, v text)`)
	mustExec(`CREATE TABLE public.uncaptured (id int PRIMARY KEY, v text)`)
	mustExec(`CREATE SCHEMA elsewhere`)
	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"captured"}}); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	// Setup's own DDL is suppressed by its marker; start the count at zero
	// so every row below is attributable to the statements in this test.
	mustExec(`DELETE FROM public.sluice_change_log WHERE op = 'X'`)

	for _, tc := range []struct {
		name string
		stmt string
		want bool // want an op='X' marker
		why  string
	}{
		// --- on the captured table: every shape must still record ---
		{"add column", `ALTER TABLE public.captured ADD COLUMN c1 text`, true, "the shape the tier exists for"},
		{"alter column type", `ALTER TABLE public.captured ALTER COLUMN v TYPE varchar(64)`, true, "a value-shape change the applier must not miss"},
		{"add constraint", `ALTER TABLE public.captured ADD CONSTRAINT chk_c1 CHECK (c1 IS NULL OR length(c1) > 0)`, true, "measured: reports object_type table, objid = the table (NOT a constraint oid)"},
		{"rename column", `ALTER TABLE public.captured RENAME COLUMN c1 TO c1b`, true, "measured: object_type 'table column', objid = the table"},
		{"create index", `CREATE INDEX idx_cap ON public.captured (v)`, true, "measured: object_type 'index', resolved through pg_index.indrelid"},
		{"drop column", `ALTER TABLE public.captured DROP COLUMN c1b`, true, "a value-shape change"},

		// --- off it: the SLP-2 complaint, in three flavours ---
		{"create table in another schema", `CREATE TABLE elsewhere.unrelated (id int PRIMARY KEY)`, false, "SLP-2: somebody else's table halted the stream"},
		{"create index in another schema", `CREATE INDEX idx_unrel ON elsewhere.unrelated (id)`, false, "the index resolves to an uncaptured table"},
		{"alter table in another schema", `ALTER TABLE elsewhere.unrelated ADD COLUMN z int`, false, "never carried a capture trigger"},
		{"create table in the captured schema", `CREATE TABLE public.brand_new (id int PRIMARY KEY)`, false, "deliberate: an uncaptured table emits no change rows, so it cannot make the applier write a wrong one; sync add-table is how it joins"},
		{"alter an uncaptured neighbour", `ALTER TABLE public.uncaptured ADD COLUMN z int`, false, "same schema, not captured"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := countXRows(t, ctx, db)
			mustExec(tc.stmt)
			after := countXRows(t, ctx, db)
			got := after > before
			if got != tc.want {
				detail := strings.Join(xRowDetail(t, ctx, db), " | ")
				t.Fatalf("%q recorded=%v, want %v (%s)\n  markers now: %s", tc.stmt, got, tc.want, tc.why, detail)
			}
		})
	}

	// Anti-vacuity: if NOTHING recorded, every "want false" cell above
	// passed for the wrong reason (a broken tier looks identical to a
	// well-scoped one when you only count absences).
	if n := countXRows(t, ctx, db); n < 6 {
		t.Fatalf("only %d op='X' markers across the whole run; the captured-table half recorded almost nothing, so the "+
			"uncaptured half proves nothing", n)
	}
}

// ATTACH PARTITION is deliberately NOT pinned here. The measurement says
// it reports object_type "table" with objid = the PARENT, so a captured
// parent would record — but this engine REFUSES a partitioned table at
// setup ("1 table(s) refused"), so a captured partitioned parent cannot
// exist on the pgtrigger lane and the cell would be unreachable. Written
// down rather than deleted silently, because the resolution logic does
// handle the shape and a future change that lets pgtrigger capture a
// partitioned parent inherits a working predicate.
