//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// DDL applied through a PARENT is recorded when the capture triggers sit on
// the CHILDREN (audit A0909-HIGH-1).
//
// # The defect
//
// The `ddl_command_end` capture matched only commands whose own relation
// carried a capture trigger (`tg.tgrelid = c.objid`). Measured on PG 16, an
// `ALTER TABLE <partitioned parent>` arrives as exactly ONE
// pg_catalog.pg_event_trigger_ddl_commands() row whose objid is the PARENT --
// so with triggers on the partitions the predicate matched nothing, no 'X' row
// was written, and the stream ran on green while the target kept the pre-ALTER
// values. Source 100/200/300, target 1/2/3, exit 0, no warning.
//
// # Why the precondition is the COMMON case, not an exotic one
//
// Installing on the children is the route operators actually take: `sync start`
// and `migrate` refuse a declaratively partitioned parent at preflight, so the
// supported shape is to exclude the parent and copy the partitions. That makes
// "capture on the partitions, DDL on the parent" the ordinary configuration,
// which is what moved this from a curiosity to a HIGH.
//
// # Scope of the fix these cells grade
//
// The predicate now walks `pg_inherits` recursively DOWNWARD from the command's
// relation. That covers declarative partitioning and classic INHERITS with one
// mechanism, and it nests -- a trigger on a sub-partition is as much a reason to
// record the root's ALTER as one on a direct child. Both are cells here, because
// a fix verified only against `PARTITION OF` would leave the `INHERITS` sibling
// to be discovered later, which is the shape this project keeps paying for.
func TestCaptureDDL_ThroughParentWithTriggersOnChildren(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	applyPGSQL(t, dsn, `
		-- Declarative partitioning.
		CREATE TABLE ev (id bigint, region text, val int, PRIMARY KEY (id, region)) PARTITION BY LIST (region);
		CREATE TABLE ev_us PARTITION OF ev FOR VALUES IN ('us');
		CREATE TABLE ev_eu PARTITION OF ev FOR VALUES IN ('eu');

		-- Classic inheritance, the sibling mechanism.
		CREATE TABLE base (id bigint PRIMARY KEY, val int);
		-- INHERITS does NOT carry the parent's PRIMARY KEY in PostgreSQL, and
		-- pgtrigger refuses a keyless table, so the child declares its own.
		CREATE TABLE leaf (PRIMARY KEY (id)) INHERITS (base);

		-- Nesting: a capture trigger on a SUB-partition must still make the
		-- root's ALTER visible.
		CREATE TABLE deep (id bigint, region text, val int, PRIMARY KEY (id, region)) PARTITION BY LIST (region);
		CREATE TABLE deep_eu PARTITION OF deep FOR VALUES IN ('eu') PARTITION BY LIST (region);
		CREATE TABLE deep_eu_de PARTITION OF deep_eu FOR VALUES IN ('eu');

		-- The control: an ordinary captured table with no relatives at all.
		CREATE TABLE plain (id bigint PRIMARY KEY, val int);

		-- The NEGATIVE control: a table nobody captures, whose DDL must stay
		-- invisible. Without this the fix could "pass" by recording everything.
		CREATE TABLE unwatched (id bigint PRIMARY KEY, val int);`)

	// Install on the CHILDREN only -- never on a parent. That is the whole
	// point: a parent install clones the trigger down and would pass even with
	// the old predicate.
	if _, err := Setup(ctx, dsn, SetupOptions{
		Tables: []string{"ev_us", "ev_eu", "leaf", "deep_eu_de", "plain"},
	}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	countX := func(t *testing.T, table string) int {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM public.sluice_change_log WHERE op = 'X' AND table_name = $1`,
			table).Scan(&n); err != nil {
			t.Fatalf("count X rows for %q: %v", table, err)
		}
		return n
	}

	for _, tc := range []struct {
		name  string
		ddl   string
		table string // object_identity, which is SCHEMA-QUALIFIED
		want  int
	}{
		{
			name:  "declarative partitioning: ALTER on the parent is seen from the partitions",
			ddl:   `ALTER TABLE ev ALTER COLUMN val TYPE bigint`,
			table: "public.ev",
			want:  1,
		},
		{
			name:  "classic INHERITS: ALTER on the parent is seen from the child",
			ddl:   `ALTER TABLE base ADD COLUMN note text`,
			table: "public.base",
			want:  1,
		},
		{
			name:  "nesting: a trigger on a SUB-partition sees the root's ALTER",
			ddl:   `ALTER TABLE deep ALTER COLUMN val TYPE bigint`,
			table: "public.deep",
			want:  1,
		},
		{
			name:  "the ordinary case still works",
			ddl:   `ALTER TABLE plain ADD COLUMN note text`,
			table: "public.plain",
			want:  1,
		},
		{
			name:  "NEGATIVE control: an uncaptured table's DDL stays invisible",
			ddl:   `ALTER TABLE unwatched ADD COLUMN note text`,
			table: "public.unwatched",
			want:  0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := countX(t, tc.table)
			applyPGSQL(t, dsn, tc.ddl)
			got := countX(t, tc.table) - before
			if got != tc.want {
				t.Errorf("%q recorded %d 'X' marker(s) for %q, want %d.\n"+
					"  A missing marker here is SILENT: the DDL tier exists so a rewriting ALTER halts the "+
					"stream instead of letting the target drift. With no marker the stream stays green and "+
					"the target keeps its pre-ALTER values at exit 0.\n"+
					"  A SPURIOUS marker is its own failure -- it halts a stream over a table nobody captures.",
					tc.ddl, got, tc.table, tc.want)
			}
		})
	}
}
