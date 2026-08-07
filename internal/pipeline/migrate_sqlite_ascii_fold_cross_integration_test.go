//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SQLite folds identifiers ASCII-ONLY, so a source pair differing only in
// NON-ASCII case is two objects on the target and must migrate (roadmap item
// 150) — end to end, on a real Postgres source and a real SQLite target, for
// all three folding walks: tables, views and indexes.
//
// # What went wrong, and why this file is an ACCEPT test
//
// The three walks keyed on Go's [strings.ToLower], which is Unicode-aware and
// therefore folds a strict SUPERSET of what SQLite folds. `idx_é` / `idx_É`
// migrated cleanly through v0.113.0 and were REFUSED from v0.114.0 on. The
// direction was safe — a loud refusal naming a rename, never a silent loss —
// but it broke a schema shape that worked, and the rename it demanded was
// advice the operator did not need.
//
// So the assertion here is the mirror image of items 147/148's: not "the run
// refuses" but "the run succeeds AND the target holds TWO of each object". The
// refusal half is still pinned, in the control subtest and in
// migrate_table_namespace_cross_integration_test.go, because an accept-only
// change to a refusal is one edit away from accepting everything.
//
// # Why this needs a live Postgres source
//
// Same premise as its item-148 neighbour: quoted identifiers are
// case-SENSITIVE on PostgreSQL, so `public."é"` and `public."É"` are two
// ordinary relations a real server hands sluice in one schema read. A
// hand-built ir.Schema can assert the walks accept the pair but cannot assert
// that a real source produces it.
//
// # The independent expected value
//
// The SQLite file's own catalog and row counts, read here with database/sql.
// Not "mig.Run returned nil" — that is what the pre-fix binary would have
// failed at, so it cannot distinguish "correctly accepted" from "accepted and
// merged", and the merge is the silent direction.

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

// sqliteObjectNames returns every object of kind on the SQLite target, read
// from the file's own catalog. Like [sqliteUserTables] next door it returns
// everything rather than probing one name: a fold collision keeps whichever
// spelling was created first, so a probe keyed on one spelling reads "absent"
// for an object that is very much present under the other.
func sqliteObjectNames(t *testing.T, dst, kind string) []string {
	t.Helper()
	db, err := sql.Open("sqlite", dst)
	if err != nil {
		t.Fatalf("open sqlite target: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.Query(
		`SELECT name FROM sqlite_schema WHERE type = ? AND name NOT LIKE 'sqlite_%' ORDER BY name`, kind,
	)
	if err != nil {
		t.Fatalf("list sqlite target %ss: %v", kind, err)
	}
	defer func() { _ = rows.Close() }()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan %s name: %v", kind, err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s names: %v", kind, err)
	}
	return names
}

func TestMigrate_SQLiteASCIIFold_PGToSQLite_NonASCIICasePairsSurvive(t *testing.T) {
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

	t.Run("tables, views and indexes differing only in non-ASCII case all arrive", func(t *testing.T) {
		// Two families in one schema: a purely non-ASCII pair, and the
		// partly-ASCII pair where an off-by-one fold would show up (the ASCII
		// letters fold, the É does not, so the two names stay distinct).
		// Disjoint key ranges, for the reason item 148 measured: overlapping
		// primary keys would make a merge fail loudly on a UNIQUE violation,
		// which is luck rather than evidence.
		applyPGDDL(t, pgSource, `
			DROP VIEW IF EXISTS public."v_é";
			DROP VIEW IF EXISTS public."v_É";
			DROP TABLE IF EXISTS public."é";
			DROP TABLE IF EXISTS public."É";
			DROP TABLE IF EXISTS public."Café_Order";
			DROP TABLE IF EXISTS public."CAFÉ_ORDER";
			CREATE TABLE public."é"          (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
			CREATE TABLE public."É"          (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
			CREATE TABLE public."Café_Order" (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
			CREATE TABLE public."CAFÉ_ORDER" (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
			CREATE INDEX "idx_é" ON public."é" (tag);
			CREATE INDEX "idx_É" ON public."É" (tag);
			CREATE VIEW public."v_é" AS SELECT id FROM public."é";
			CREATE VIEW public."v_É" AS SELECT id FROM public."É";
			INSERT INTO public."é"          (id, tag) VALUES (1, 'lower-1'), (2, 'lower-2');
			INSERT INTO public."É"          (id, tag) VALUES (101, 'upper-1');
			INSERT INTO public."Café_Order" (id, tag) VALUES (201, 'mixed-1');
			INSERT INTO public."CAFÉ_ORDER" (id, tag) VALUES (301, 'shout-1'), (302, 'shout-2');
		`)

		dst := filepath.Join(t.TempDir(), "item150-non-ascii-fold.db")
		mig := &Migrator{
			Source: pgEng, Target: sqliteEng,
			SourceDSN: pgSource, TargetDSN: dst,
			MigrationID: "item150-pg-to-sqlite-non-ascii-case",
		}
		if err := mig.Run(ctx2min(t)); err != nil {
			t.Fatalf("migrate REFUSED a schema whose names a real SQLite target keeps apart — SQLite "+
				"folds ASCII only, so every pair here is two objects. This is the over-refusal roadmap "+
				"item 150 closed: %v", err)
		}

		// THE INDEPENDENT ORACLE: the target's own catalog and rows.
		wantTables := map[string]int{"é": 2, "É": 1, "Café_Order": 1, "CAFÉ_ORDER": 2}
		got := sqliteUserTables(t, dst)
		for name, want := range wantTables {
			if got[name] != want {
				t.Errorf("target table %q holds %d row(s), want %d; whole target = %v. A count that is "+
					"the SUM of two source tables is the silent merge — the target lost a table and "+
					"kept every row.", name, got[name], want, got)
			}
		}
		if len(got) != len(wantTables) {
			t.Errorf("target holds %d table(s) (%v); want exactly %d", len(got), got, len(wantTables))
		}

		for _, tc := range []struct {
			kind string
			want []string
		}{
			{"index", []string{"idx_é", "idx_É"}},
			{"view", []string{"v_é", "v_É"}},
		} {
			names := sqliteObjectNames(t, dst, tc.kind)
			found := map[string]bool{}
			for _, n := range names {
				found[n] = true
			}
			for _, w := range tc.want {
				if !found[w] {
					t.Errorf("target is missing %s %q; it holds %v. `CREATE ... IF NOT EXISTS` makes a "+
						"collision silent, so a missing object here is the loss, not an error.",
						tc.kind, w, names)
				}
			}
		}
	})

	// THE CONTROL, and it is the half that keeps this change from being a
	// relaxation: the ASCII fold must still fire. `orders` / `Orders` really
	// do become one table on SQLite, and that refusal is item 148.
	t.Run("the ASCII case pair is still refused", func(t *testing.T) {
		applyPGDDL(t, pgSource, `
			DROP TABLE IF EXISTS public."Orders";
			DROP TABLE IF EXISTS public.orders;
			CREATE TABLE public.orders   (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
			CREATE TABLE public."Orders" (id BIGINT PRIMARY KEY, tag TEXT NOT NULL);
			INSERT INTO public.orders   (id, tag) VALUES (1, 'lower-1');
			INSERT INTO public."Orders" (id, tag) VALUES (101, 'upper-1');
		`)

		dst := filepath.Join(t.TempDir(), "item150-ascii-control.db")
		mig := &Migrator{
			Source: pgEng, Target: sqliteEng,
			SourceDSN: pgSource, TargetDSN: dst,
			MigrationID: "item150-pg-to-sqlite-ascii-control",
		}
		err := mig.Run(ctx2min(t))
		if err == nil {
			t.Fatalf("migrate SUCCEEDED with two source tables SQLite folds onto one name. The ASCII "+
				"half of the fold is the whole point of the check; target now holds %v.",
				sqliteUserTables(t, dst))
		}
		if ce, coded := sluicecode.FromError(err); !coded || ce.Code != sluicecode.CodeSchemaTableNameCollision {
			t.Errorf("refusal must carry %s; got %v", sluicecode.CodeSchemaTableNameCollision, err)
		}
	})
}
