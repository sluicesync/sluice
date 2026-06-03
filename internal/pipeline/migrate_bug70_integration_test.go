//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for Bug 70 (a NULL *element* inside any typed PG
// array, AND any multi-dimensional array, hard-fails the PG→PG
// COPY-protocol writer):
//
//   - Pre-fix: convertArray (postgres/row_writer.go) type-asserted each
//     element to a concrete Go type (e.(int64)/e.(string)/…) and built
//     non-pointer slices ([]int64/[]string/…). A nil element →
//     "expected int64, got <nil>"; a nested []any (multi-dim, the Bug 68
//     decoder shape) → "expected int64, got []interface {}". Either way
//     SQLSTATE 57014, exit 1, 0 rows.
//
//   - Post-fix: convertArray builds pointer-element slices ([]*int64,
//     …) so a nil slot is a typed nil pointer (SQL NULL), and recurses
//     on nested []any so int[][] becomes [][]*int64 — the shape pgx's
//     multi-dim array encode plan requires. Faithful round-trip: NULL
//     element → NULL element, dimensionality preserved.
//
// This is the verbatim BUG-CATALOG section 70 minimal repro: int[],
// text[], numeric[] each with a NULL element, plus a multi-dimensional
// int[][], ground-truthed EXACT src==dst on the PG target. A non-null
// 1-D array regression guard (#18 surface) is included.

package pipeline

import (
	"database/sql"
	"testing"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// bug70SeedDDL is the canonical BUG-CATALOG section 70 repro: typed
// arrays with NULL elements (int/text/numeric), a multi-dimensional
// int[][], and a non-null 1-D array regression guard.
const bug70SeedDDL = `
	CREATE TABLE arr_shapes (
	  id      int PRIMARY KEY,
	  ai      int[],          -- NULL element
	  at      text[],         -- NULL element
	  an      numeric[],      -- NULL element
	  md      int[][],        -- multi-dimensional
	  guard   int[]           -- non-null 1-D regression guard (#18)
	);
	INSERT INTO arr_shapes (id, ai, at, an, md, guard) VALUES
	  (1, ARRAY[1,NULL,3]::int[],
	      ARRAY['x',NULL,'z']::text[],
	      ARRAY[1.5,NULL,3.5]::numeric[],
	      ARRAY[ARRAY[1,2],ARRAY[3,4]]::int[][],
	      ARRAY[10,20,30]::int[]),
	  (2, ARRAY[NULL,NULL]::int[],
	      ARRAY[NULL]::text[],
	      NULL,
	      ARRAY[ARRAY[5,NULL],ARRAY[NULL,8]]::int[][],
	      ARRAY[]::int[]);
`

// TestMigrate_PostgresToPostgres_Bug70NullableMultiDimArrays pins the
// PG→PG closure: pre-fix the COPY writer aborted with SQLSTATE 57014 on
// the first NULL array element / multi-dim array. Post-fix migrate
// exits 0 and every array — including NULL elements and the 2-D shape —
// is ground-truthed EXACT on the PG target via ::text rendering (which
// makes NULL elements and dimensionality observable).
func TestMigrate_PostgresToPostgres_Bug70NullableMultiDimArrays(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, sourceDSN, bug70SeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: sourceDSN,
		TargetDSN: targetDSN,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		// Pre-fix this failed with SQLSTATE 57014 (got <nil> / got
		// []interface {}).
		t.Fatalf("Migrator.Run (PG→PG nullable/multi-dim arrays must migrate, no 57014): %v", err)
	}

	pgDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open pg target: %v", err)
	}
	defer func() { _ = pgDB.Close() }()

	type row struct {
		id    int
		ai    sql.NullString
		at    sql.NullString
		an    sql.NullString
		md    sql.NullString
		guard sql.NullString
	}
	// PG's canonical ::text array rendering: NULL elements show as the
	// bare NULL keyword; a 2-D array shows nested braces. This is the
	// load-bearing oracle — it makes element-NULL and dimensionality
	// observable in a single string compare.
	want := []row{
		{
			1,
			sql.NullString{String: "{1,NULL,3}", Valid: true},
			sql.NullString{String: `{x,NULL,z}`, Valid: true},
			sql.NullString{String: "{1.5,NULL,3.5}", Valid: true},
			sql.NullString{String: "{{1,2},{3,4}}", Valid: true},
			sql.NullString{String: "{10,20,30}", Valid: true},
		},
		{
			2,
			sql.NullString{String: "{NULL,NULL}", Valid: true},
			sql.NullString{String: "{NULL}", Valid: true},
			sql.NullString{Valid: false},
			sql.NullString{String: "{{5,NULL},{NULL,8}}", Valid: true},
			sql.NullString{String: "{}", Valid: true},
		},
	}
	rows, err := pgDB.Query(`SELECT id, ai::text, at::text, an::text, md::text, guard::text
		FROM arr_shapes ORDER BY id`)
	if err != nil {
		t.Fatalf("query pg target: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.ai, &r.at, &r.an, &r.md, &r.guard); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d", len(got), len(want))
	}
	for i, g := range got {
		w := want[i]
		if g.ai != w.ai {
			t.Errorf("row[%d] ai: got %+v; want %+v (int[] NULL element must survive)", i, g.ai, w.ai)
		}
		if g.at != w.at {
			t.Errorf("row[%d] at: got %+v; want %+v (text[] NULL element must survive)", i, g.at, w.at)
		}
		if g.an != w.an {
			t.Errorf("row[%d] an: got %+v; want %+v (numeric[] NULL element must survive)", i, g.an, w.an)
		}
		if g.md != w.md {
			t.Errorf("row[%d] md: got %+v; want %+v (int[][] dimensionality must survive)", i, g.md, w.md)
		}
		if g.guard != w.guard {
			t.Errorf("row[%d] guard: got %+v; want %+v (#18 non-null 1-D regression)", i, g.guard, w.guard)
		}
	}
}

// TestMigrate_PostgresToMySQL_Bug70ArraysUnaffected guards that the
// Bug 70 array-encoder change does not regress the cross-engine
// PG→MySQL path (Bug 68 surface): nullable + multi-dim arrays land as
// MySQL JSON with NULL elements correct and dimensionality preserved,
// exit 0 (this path was already correct pre-fix; the guard ensures the
// convertArray rewrite didn't break it).
func TestMigrate_PostgresToMySQL_Bug70ArraysUnaffected(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyPGDDL(t, pgSource, bug70SeedDDL)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	mig := &Migrator{
		Source:    pgEng,
		Target:    mysqlEng,
		SourceDSN: pgSource,
		TargetDSN: mysqlTarget,
	}
	if err := mig.Run(ctx2min(t)); err != nil {
		t.Fatalf("Migrator.Run (PG→MySQL arrays, Bug 68 regression guard): %v", err)
	}

	mysqlDB, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open mysql target: %v", err)
	}
	defer func() { _ = mysqlDB.Close() }()

	type row struct {
		id int
		ai string
		at string
		md string
	}
	// MySQL JSON renders NULL elements as JSON null and nested arrays as
	// nested JSON — the Bug 68 faithful cross-engine shape.
	want := []row{
		{1, "[1, null, 3]", `["x", null, "z"]`, "[[1, 2], [3, 4]]"},
		{2, "[null, null]", "[null]", "[[5, null], [null, 8]]"},
	}
	rows, err := mysqlDB.Query(`SELECT id, CAST(ai AS CHAR), CAST(at AS CHAR), CAST(md AS CHAR)
		FROM arr_shapes ORDER BY id`)
	if err != nil {
		t.Fatalf("query mysql target: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.ai, &r.at, &r.md); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d", len(got), len(want))
	}
	for i, g := range got {
		w := want[i]
		if g.ai != w.ai || g.at != w.at || g.md != w.md {
			t.Errorf("row[%d]: got {ai:%q at:%q md:%q}; want {ai:%q at:%q md:%q} (Bug 68 PG→MySQL must not regress)",
				i, g.ai, g.at, g.md, w.ai, w.at, w.md)
		}
	}
}
