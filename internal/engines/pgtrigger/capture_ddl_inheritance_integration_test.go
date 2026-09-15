//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import (
	"context"
	"database/sql"
	"strings"
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

	// # Relation-ADDING shapes (the 2026-09-15 PG audit's F4)
	//
	// Every statement that adds an UNCAPTURED relation to a tree one of
	// whose members is captured. Rows routed into the new member are rows
	// the captured logical table used to receive and now silently does
	// not, so each must record a marker — or be named in the exemption map
	// with a reason. Before the fix, `ATTACH PARTITION` (objid = the
	// parent, walked downward) recorded one and `CREATE TABLE … PARTITION
	// OF` (objid = the NEW partition, descendant set empty) recorded
	// nothing, on PG 16.15 and 18.6 alike: the fix's own class, half
	// closed. The negative controls are the same shapes against a tree
	// nobody captures, so the fix cannot "pass" by recording every CREATE.
	//
	// table is the marker's table_name — object_identity of the command's
	// OWN object, which for a CREATE is the NEW relation and for an ATTACH
	// is the parent.
	relationAddingExempt := map[string]string{}
	applyPGSQL(t, dsn, `
		-- val is BIGINT: the ALTER cells above already widened the parents, and ATTACH requires the same type.
		CREATE TABLE ev_ap (id bigint, region text, val bigint, PRIMARY KEY (id, region));
		CREATE TABLE deep_eu_fr_ap (id bigint, region text, val bigint, PRIMARY KEY (id, region));
		CREATE TABLE unwatched_p (id bigint, region text, val int, PRIMARY KEY (id, region)) PARTITION BY LIST (region);
		CREATE TABLE unwatched_p_us PARTITION OF unwatched_p FOR VALUES IN ('us');
		CREATE TABLE unwatched_base (id bigint PRIMARY KEY, val int);
		CREATE TABLE unwatched_leaf (PRIMARY KEY (id)) INHERITS (unwatched_base);
		CREATE TABLE unwatched_ap (id bigint, region text, val int, PRIMARY KEY (id, region));`)
	for _, tc := range []struct {
		name  string
		ddl   string
		table string
		want  int
	}{
		{
			name:  "ALTER … ATTACH PARTITION adds a partition to a captured tree",
			ddl:   `ALTER TABLE ev ATTACH PARTITION ev_ap FOR VALUES IN ('ap')`,
			table: "public.ev",
			want:  1,
		},
		{
			name:  "CREATE TABLE … PARTITION OF adds a partition to a captured tree",
			ddl:   `CREATE TABLE ev_uk PARTITION OF ev FOR VALUES IN ('uk')`,
			table: "public.ev_uk",
			want:  1,
		},
		{
			name:  "CREATE TABLE … INHERITS adds a child to a captured tree",
			ddl:   `CREATE TABLE leaf2 (PRIMARY KEY (id)) INHERITS (base)`,
			table: "public.leaf2",
			want:  1,
		},
		{
			name:  "nesting: CREATE TABLE … PARTITION OF a SUB-partition whose sibling is captured",
			ddl:   `CREATE TABLE deep_eu_fr PARTITION OF deep_eu FOR VALUES IN ('fr')`,
			table: "public.deep_eu_fr",
			want:  1,
		},
		{
			name:  "nesting: ATTACH PARTITION to a SUB-partition whose child is captured",
			ddl:   `ALTER TABLE deep_eu ATTACH PARTITION deep_eu_fr_ap FOR VALUES IN ('fr-ap')`,
			table: "public.deep_eu",
			want:  1,
		},
		{
			name:  "NEGATIVE control: CREATE TABLE … PARTITION OF an uncaptured tree",
			ddl:   `CREATE TABLE unwatched_p_eu PARTITION OF unwatched_p FOR VALUES IN ('eu')`,
			table: "public.unwatched_p_eu",
			want:  0,
		},
		{
			name:  "NEGATIVE control: ATTACH PARTITION to an uncaptured tree",
			ddl:   `ALTER TABLE unwatched_p ATTACH PARTITION unwatched_ap FOR VALUES IN ('ap')`,
			table: "public.unwatched_p",
			want:  0,
		},
		{
			name:  "NEGATIVE control: CREATE TABLE … INHERITS an uncaptured parent",
			ddl:   `CREATE TABLE unwatched_leaf2 (PRIMARY KEY (id)) INHERITS (unwatched_base)`,
			table: "public.unwatched_leaf2",
			want:  0,
		},
		{
			name:  "NEGATIVE control: a plain CREATE TABLE joins no tree",
			ddl:   `CREATE TABLE loner (id bigint PRIMARY KEY, val int)`,
			table: "public.loner",
			want:  0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if reason, ok := relationAddingExempt[tc.name]; ok {
				t.Logf("exempt: %s", reason)
				return
			}
			before := countX(t, tc.table)
			applyPGSQL(t, dsn, tc.ddl)
			got := countX(t, tc.table) - before
			if got != tc.want {
				t.Errorf("%q recorded %d 'X' marker(s) for %q, want %d.\n"+
					"  A missing marker here is SILENT: the new relation carries no capture trigger, so every "+
					"row routed into it is never captured and never reaches the target, at exit 0 (F4).\n"+
					"  A SPURIOUS marker is its own failure -- it halts a stream over a tree nobody captures.",
					tc.ddl, got, tc.table, tc.want)
			}
		})
	}
	if len(relationAddingExempt) > 0 {
		t.Logf("relation-adding shapes exempt from the marker requirement: %v", relationAddingExempt)
	}
	for name := range relationAddingExempt {
		if strings.TrimSpace(relationAddingExempt[name]) == "" {
			t.Errorf("relation-adding exemption %q has no reason", name)
		}
	}
}
