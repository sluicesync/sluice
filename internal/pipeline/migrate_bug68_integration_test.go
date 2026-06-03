//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for Bug 68 (PG multi-dimensional array column
// `int[][]` / any `T[][]` silently loses ALL rows of the table on
// cross-engine PG→MySQL migrate: exit 0, no error, no WARN at any log
// level; target table created correctly but ZERO rows copied).
//
// Pre-fix shape: pgx stdlib mode hands an `int[][]` value back as the
// PG text form `{{1,2},{3,4}}`. The postgres reader's parsePGArrayText
// refused nested braces ("nested arrays not supported"), so decodeValue
// returned an error. The RowReader stored that error on its sticky
// `Err()` field and closed the row channel — but `ir.RowReader` did not
// expose `Err()` and the bulk-copy orchestrator never observed it.
// Channel-close was overloaded as "table fully read", so WriteRows
// wrote 0 rows, copyTable returned nil, and migrate exited 0 with the
// entire table silently empty — the single worst failure class under
// the project's loud-failure tenet.
//
// The fix has two independent parts:
//
//  1. The postgres array text parser now decodes multi-dimensional
//     arrays into nested []any, so `int[][]` round-trips faithfully to
//     a nested MySQL JSON array (`[[1,2],[3,4]]`) — the natural analogue
//     of the 1-D int[]→JSON path that already worked (Bug 18).
//  2. (independent, loud-failure-tenet) `ir.RowReader` now exposes
//     `Err()` and every bulk-copy path checks it after the row channel
//     drains. A decode/scan error on ANY row now fails the migrate
//     loudly instead of ending the table's stream with exit 0.
//
// This is the verbatim canonical BUG-CATALOG section 68 minimal repro
// (3 rows, single-reader path).

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	// Both engines must be registered for engines.Get to find them.
	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_PostgresToMySQL_MultiDimArrayBulkCopy pins the Bug 68
// closure: a PG source with an `int[][]` column carrying populated and
// NULL values migrates PG→MySQL with every row copied, and each MySQL
// `json` target cell round-trips to the expected nested JSON array
// (ground-truthed by reading the MySQL target, not just "no error").
func TestMigrate_PostgresToMySQL_MultiDimArrayBulkCopy(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	// The verbatim BUG-CATALOG section 68 minimal repro: an int[][]
	// column, a plain text column, three rows exercising a rectangular
	// matrix / a single-row matrix / a NULL whole-array.
	const seedDDL = `
		CREATE TABLE md (
		  id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		  matrix int[][],
		  notes  text
		);
		INSERT INTO md (matrix,notes) VALUES
		  (ARRAY[ARRAY[1,2],ARRAY[3,4]],'r1'),
		  (ARRAY[ARRAY[9,8]],'r2'),
		  (NULL,'r3');
	`
	applyPGDDL(t, pgSource, seedDDL)

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

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		// Pre-fix this DID NOT error (exit 0, 0 rows) — the silent loss.
		// A successful Run plus the row assertions below is the
		// load-bearing proof the faithful multi-dim decode engaged.
		t.Fatalf("Migrator.Run: %v", err)
	}

	type row struct {
		id     int
		matrix sql.NullString
		mType  sql.NullString
		notes  sql.NullString
	}
	want := []row{
		{
			id:     1,
			matrix: sql.NullString{String: `[[1, 2], [3, 4]]`, Valid: true},
			mType:  sql.NullString{String: "ARRAY", Valid: true},
			notes:  sql.NullString{String: "r1", Valid: true},
		},
		{
			id:     2,
			matrix: sql.NullString{String: `[[9, 8]]`, Valid: true},
			mType:  sql.NullString{String: "ARRAY", Valid: true},
			notes:  sql.NullString{String: "r2", Valid: true},
		},
		{
			id: 3,
			// NULL whole-array column → SQL NULL, not `[]` / `[[]]`.
			matrix: sql.NullString{Valid: false},
			mType:  sql.NullString{Valid: false},
			notes:  sql.NullString{String: "r3", Valid: true},
		},
	}

	db, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx,
		"SELECT id, matrix, JSON_TYPE(matrix), notes FROM md ORDER BY id")
	if err != nil {
		t.Fatalf("query target: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.matrix, &r.mType, &r.notes); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d (pre-fix Bug 68: 0 rows copied, exit 0, silent)", len(got), len(want))
	}
	for i, g := range got {
		w := want[i]
		if g.id != w.id {
			t.Errorf("row[%d] id: got %d; want %d", i, g.id, w.id)
		}
		// matrix is a JSON cell; reuse the Bug 18 helper for the
		// NULL-vs-array distinction + whitespace-tolerant JSON compare.
		assertJSONCell(t, i, "matrix", g.matrix, g.mType, w.matrix, w.mType)
		if g.notes.String != w.notes.String || g.notes.Valid != w.notes.Valid {
			t.Errorf("row[%d] notes: got %q (valid=%v); want %q (valid=%v)",
				i, g.notes.String, g.notes.Valid, w.notes.String, w.notes.Valid)
		}
	}
}
