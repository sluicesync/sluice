//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Two source TABLES that land on one SQLite table name must be refused BEFORE
// any data moves (roadmap item 148) — end to end, on a real Postgres source and
// a real SQLite target, by BOTH routes the item files.
//
// # Why this needs a live source, and why the oracle is the target file
//
// The premise is a fact about PostgreSQL, not about sluice: quoted identifiers
// are case-SENSITIVE there, so `public.orders` and `public."Orders"` are two
// ordinary relations a real server hands sluice in one schema read. A
// hand-built ir.Schema can assert the refusal but cannot assert that a real
// source produces the pair.
//
// The oracle is deliberately the target database's own catalog and row counts —
// read here with `database/sql`, never from anything sluice reports — because
// the defect's entire character is that sluice reports success. A migration
// that merges two tables copies every row, exits 0, and leaves a row count that
// is correct for the surviving name; `verify` compares that name against that
// name and agrees. So "the run failed" is not the assertion. The assertion is
// what is in the file.
//
// # The pre-fix behaviour these tests pin against, measured
//
// With the refusals removed, the same two cases produce (independently
// verified by the same queries below):
//
//	route 1  target holds ONE table `orders` with FOUR rows — two from
//	         `public.orders` and two from `public."Orders"` — at exit 0.
//	route 2  target holds ONE table `widgets` with FIVE rows — two from
//	         `sales.widgets` and three from `billing.widgets` — at exit 0.
//
// Both are recorded in item 148's roadmap entry with the mutation that
// reproduces them.

package pipeline

import (
	"database/sql"
	"path/filepath"
	"testing"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// sqliteUserTables returns every user table on the SQLite target with its row
// count, read from the file's own catalog and data. This is the independent
// oracle for both routes: nothing here derives from anything sluice reports,
// which matters because what sluice reports on the defective path is success.
//
// It returns the WHOLE map rather than one table's count on purpose. The
// surviving name in a fold collision is whichever spelling was created first —
// `Orders` as easily as `orders`, and sqlite_master stores it as written — so a
// probe keyed on one spelling reads "absent" for a table that is very much
// present under the other. The first draft of this helper did exactly that and
// reported -1 for a target holding four merged rows; the map makes the real
// state unmissable in any failure message.
func sqliteUserTables(t *testing.T, dst string) map[string]int {
	t.Helper()
	db, err := sql.Open("sqlite", dst)
	if err != nil {
		t.Fatalf("open sqlite target: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.Query(
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`,
	)
	if err != nil {
		t.Fatalf("list sqlite target tables: %v", err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table name: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table names: %v", err)
	}
	_ = rows.Close()

	out := make(map[string]int, len(names))
	for _, name := range names {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM "` + name + `"`).Scan(&n); err != nil {
			t.Fatalf("count %q on the sqlite target: %v", name, err)
		}
		out[name] = n
	}
	return out
}

// TestMigrate_TableNamespace_PGToSQLite_CaseFoldedTableNames is ROUTE 1: two
// tables in ONE source namespace that SQLite folds onto one name.
func TestMigrate_TableNamespace_PGToSQLite_CaseFoldedTableNames(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}

	t.Run("PG holds both spellings as distinct tables, and the collision is refused", func(t *testing.T) {
		// The premise, established by the server: two relations, two row
		// counts, in one schema.
		//
		// THE KEY RANGES ARE DISJOINT ON PURPOSE, and the reason is a measured
		// correction rather than caution. The first draft of this fixture gave
		// both tables ids 1 and 2, and mutation M1 (both refusals removed)
		// produced a LOUD failure — `UNIQUE constraint failed: Orders.id` —
		// because the merged rows happened to collide on the primary key. That
		// would have pinned the wrong thing: the run failing for an unrelated
		// reason, on a schema whose corruption a PK happened to catch. A source
		// whose two tables carry disjoint keys (or no primary key at all) gets
		// no such luck, and that is the shape item 148 is about. With these ids
		// the pre-fix run exits 0 with four rows in `orders`.
		applyPGDDL(t, pgSource, `
			DROP TABLE IF EXISTS public."Orders";
			DROP TABLE IF EXISTS public.orders;
			CREATE TABLE public.orders   (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
			CREATE TABLE public."Orders" (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
			INSERT INTO public.orders   (id, tag) VALUES (1, 'lower-1'), (2, 'lower-2');
			INSERT INTO public."Orders" (id, tag) VALUES (101, 'upper-1'), (102, 'upper-2');
		`)

		dst := filepath.Join(t.TempDir(), "item148-route1-refused.db")
		mig := &Migrator{
			Source: pgEng, Target: sqliteEng,
			SourceDSN: pgSource, TargetDSN: dst,
			MigrationID: "item148-pg-to-sqlite-table-name-collision",
		}
		err := mig.Run(ctx2min(t))
		if err == nil {
			t.Fatalf("migrate SUCCEEDED with two source tables that fold to one SQLite name. That is not a "+
				"partial failure — the target now holds %v, where the source holds TWO tables of two rows "+
				"each, at exit 0, with a count that verifies clean against the surviving name.",
				sqliteUserTables(t, dst))
		}
		if ce, coded := sluicecode.FromError(err); !coded || ce.Code != sluicecode.CodeSchemaTableNameCollision {
			t.Errorf("refusal must carry %s; got %v", sluicecode.CodeSchemaTableNameCollision, err)
		}
		for _, want := range []string{"orders", "Orders", "SILENTLY no-op"} {
			if !contains(err.Error(), want) {
				t.Errorf("refusal does not mention %q; got: %v", want, err)
			}
		}
		// THE INDEPENDENT ORACLE. Not "did sluice say no" — what is in the
		// file. Pre-fix (mutation M1) this read map[Orders:4].
		if got := sqliteUserTables(t, dst); len(got) != 0 {
			t.Errorf("the target holds %v after the refusal; the check is supposed to run beside the other "+
				"pre-DDL gates, so nothing should have been created and no row copied", got)
		}
	})

	// THE CONTROL, and it is not optional: an over-refusing check would break
	// every PG→SQLite migration whose schema happens to carry two tables at
	// all. Without this, "refused" and "this pair never worked" are the same
	// observation.
	t.Run("distinct table names still migrate, every row in its own table", func(t *testing.T) {
		applyPGDDL(t, pgSource, `
			DROP TABLE IF EXISTS public."Orders";
			DROP TABLE IF EXISTS public.orders;
			DROP TABLE IF EXISTS public.order_items;
			CREATE TABLE public.orders      (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
			CREATE TABLE public.order_items (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
			INSERT INTO public.orders      (id, tag) VALUES (1, 'o-1'), (2, 'o-2');
			INSERT INTO public.order_items (id, tag) VALUES (1, 'i-1'), (2, 'i-2'), (3, 'i-3');
		`)

		dst := filepath.Join(t.TempDir(), "item148-route1-allowed.db")
		mig := &Migrator{
			Source: pgEng, Target: sqliteEng,
			SourceDSN: pgSource, TargetDSN: dst,
			MigrationID: "item148-pg-to-sqlite-distinct-table-names",
		}
		if err := mig.Run(ctx2min(t)); err != nil {
			t.Fatalf("two tables with DISTINCT names must still migrate to SQLite: %v", err)
		}
		got := sqliteUserTables(t, dst)
		if got["orders"] != 2 || got["order_items"] != 3 {
			t.Errorf("target tables = %v; want orders:2 and order_items:3 — every row in its own table", got)
		}
	})
}

// TestMigrate_MultiNamespaceIntoFlatTarget_PGToSQLite is ROUTE 2: the
// multi-namespace fan-out into a target that cannot namespace.
//
// The `else` arm this closes carried a comment asserting it "is unreachable
// with today's engines". It was reachable by every flat target, and it set a
// TargetSchema that [migcore.ApplyTargetSchema] then discarded because SQLite
// implements no ir.SchemaSetter — so every source schema wrote bare names into
// one file. This test is the reachability proof as much as the refusal pin.
func TestMigrate_MultiNamespaceIntoFlatTarget_PGToSQLite(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	sqliteEng, ok := engines.Get("sqlite")
	if !ok {
		t.Fatal("sqlite engine not registered")
	}

	// Two schemas, each with a same-named table and DIFFERENT row counts, so a
	// merge is arithmetically visible rather than plausible — and with DISJOINT
	// key ranges, for the reason measured in route 1 above: overlapping primary
	// keys would make the merge fail loudly on a UNIQUE violation, which is
	// luck rather than a guarantee and would pin the wrong behaviour.
	applyPGDDL(t, pgSource, `
		DROP SCHEMA IF EXISTS sales CASCADE;
		DROP SCHEMA IF EXISTS billing CASCADE;
		CREATE SCHEMA sales;
		CREATE SCHEMA billing;
		CREATE TABLE sales.widgets   (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		CREATE TABLE billing.widgets (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		INSERT INTO sales.widgets   (id, name) VALUES (1, 'a-one'), (2, 'a-two');
		INSERT INTO billing.widgets (id, name) VALUES (101, 'b-one'), (102, 'b-two'), (103, 'b-three');
	`)

	t.Run("two source schemas into one SQLite file is refused before anything is written", func(t *testing.T) {
		dst := filepath.Join(t.TempDir(), "item148-route2-refused.db")
		mig := &Migrator{
			Source: pgEng, Target: sqliteEng,
			SourceDSN: pgSource, TargetDSN: dst,
			DatabaseFilter: DatabaseFilter{Include: []string{"sales", "billing"}},
			MigrationID:    "item148-pg-multischema-to-sqlite",
		}
		err := mig.Run(ctx2min(t))
		if err == nil {
			t.Fatalf("multi-namespace migrate SUCCEEDED into a flat target: the two schemas' `widgets` "+
				"tables merged, at exit 0. Target now holds %v, where the source holds sales.widgets(2) "+
				"and billing.widgets(3).", sqliteUserTables(t, dst))
		}
		if ce, coded := sluicecode.FromError(err); !coded ||
			ce.Code != sluicecode.CodeConfigMultiNamespaceTargetFlat {
			t.Errorf("refusal must carry %s; got %v", sluicecode.CodeConfigMultiNamespaceTargetFlat, err)
		}
		for _, want := range []string{"sales", "billing", "sqlite", "SILENTLY MERGE"} {
			if !contains(err.Error(), want) {
				t.Errorf("refusal does not mention %q; got: %v", want, err)
			}
		}
		// THE INDEPENDENT ORACLE. Pre-fix (mutation M8) this read
		// map[widgets:5] — two schemas' rows in one table, which is exactly the
		// count nothing in the run disputes.
		if got := sqliteUserTables(t, dst); len(got) != 0 {
			t.Errorf("the target holds %v after the refusal; the run was supposed to refuse the "+
				"CONFIGURATION before opening a target writer", got)
		}
	})

	// CONTROL A: the fan-out narrowed to ONE namespace is allowed through — a
	// flat target serves it losslessly, and refusing it would be over-refusal.
	t.Run("a single-namespace fan-out into SQLite still migrates", func(t *testing.T) {
		dst := filepath.Join(t.TempDir(), "item148-route2-single.db")
		mig := &Migrator{
			Source: pgEng, Target: sqliteEng,
			SourceDSN: pgSource, TargetDSN: dst,
			DatabaseFilter: DatabaseFilter{Include: []string{"billing"}},
			MigrationID:    "item148-pg-singleschema-to-sqlite",
		}
		if err := mig.Run(ctx2min(t)); err != nil {
			t.Fatalf("a one-namespace fan-out into a flat target must still migrate: %v", err)
		}
		if got := sqliteUserTables(t, dst); got["widgets"] != 3 || len(got) != 1 {
			t.Errorf("target tables = %v; want exactly widgets:3 (billing only)", got)
		}
	})

	// CONTROL B: the same two-namespace fan-out into a NAMESPACED target is
	// untouched. This is what stops the new refusal from being a blanket ban on
	// multi-namespace runs — the predicate is the target's capability, and this
	// is the other side of it.
	t.Run("the same fan-out into a namespaced target is unaffected", func(t *testing.T) {
		_, pgTarget, cleanup := startPostgres(t)
		defer cleanup()

		mig := &Migrator{
			Source: pgEng, Target: pgEng,
			SourceDSN: pgSource, TargetDSN: pgTarget,
			DatabaseFilter: DatabaseFilter{Include: []string{"sales", "billing"}},
			MigrationID:    "item148-pg-multischema-to-pg",
		}
		if err := mig.Run(ctx2min(t)); err != nil {
			t.Fatalf("multi-schema PG→PG must still migrate: %v", err)
		}
		db, err := sql.Open("pgx", pgTarget)
		if err != nil {
			t.Fatalf("open pg target: %v", err)
		}
		defer func() { _ = db.Close() }()
		for _, tc := range []struct {
			table string
			want  int
		}{{"sales.widgets", 2}, {"billing.widgets", 3}} {
			var n int
			if err := db.QueryRow(`SELECT COUNT(*) FROM ` + tc.table).Scan(&n); err != nil {
				t.Fatalf("count %s: %v", tc.table, err)
			}
			if n != tc.want {
				t.Errorf("%s = %d; want %d", tc.table, n, tc.want)
			}
		}
	})
}
