//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for Bug 18 (PG→MySQL bulk copy of a PG array column
// crashes the MySQL LOAD DATA row serializer).
//
// Pre-fix shape: a PG source table with an array column (text[], int[],
// …) containing rows migrated PG→MySQL. The schema phase succeeded
// (the array column maps to MySQL `JSON`), but the data-copy phase died
// with `mysql: LOAD DATA: unsupported value type []interface {}`: the
// PG RowReader yields the array as a Go []any, and prepareValue only
// routed the array→JSON conversion when the IR column type was
// ir.JSON. For a PG array source the IR type stays ir.Array (only
// ddl_emit renders it as MySQL `JSON`), so the []any fell through to
// tsvEncode and crashed. Tables were created but ZERO rows copied;
// migrate exited 1.
//
// The fix: prepareValue treats an ir.Array-typed column exactly like
// ir.JSON for value conversion (consistent with ddl_emit rendering
// ir.Array as MySQL `JSON`), serializing the []any to its JSON text
// form. Cross-ref Bug 14/47 (the same convertArrayLikeToJSON surface).
//
// This is the exact canonical BUG-CATALOG section 18 repro.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	// Both engines must be registered for engines.Get to find them.
	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestMigrate_PostgresToMySQL_ArrayColumnBulkCopy pins the Bug 18
// closure: a PG source with `text[]` and `int[]` columns carrying
// populated, empty, and NULL values migrates PG→MySQL with every row
// copied, and each MySQL `json` target cell round-trips to the
// expected JSON text (ground-truthed by reading the MySQL target,
// not just "no error").
func TestMigrate_PostgresToMySQL_ArrayColumnBulkCopy(t *testing.T) {
	pgSource, _, pgCleanup := startPostgres(t)
	defer pgCleanup()

	_, mysqlTarget, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	// The verbatim BUG-CATALOG section 18 repro: a text[] column, an
	// int[] column, three rows exercising populated / empty / NULL.
	const seedDDL = `
		CREATE TABLE a (id int primary key, t text[], n int[]);
		INSERT INTO a VALUES
			(1, '{x,y}', '{1,2}'),
			(2, '{}',    '{}'),
			(3, NULL,    NULL);
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
		// Pre-fix this surfaced as
		// `pipeline: copy table "a": write rows: mysql: LOAD DATA:
		// serialize rows for "a": mysql: LOAD DATA: unsupported value
		// type []interface {}`. A successful Run is the load-bearing
		// assertion that the array→JSON serialization engaged.
		t.Fatalf("Migrator.Run: %v", err)
	}

	// Ground-truth the target: read each row's t/n value, the SQL-NULL
	// marker, and JSON_TYPE so the array-vs-null distinction is
	// observable. A NULL whole-array column must land as SQL NULL (not
	// `[]`); an empty array as `[]`; a populated array as the JSON
	// array faithfully representing the source.
	type row struct {
		id    int
		t     sql.NullString
		tType sql.NullString
		n     sql.NullString
		nType sql.NullString
	}
	want := []row{
		{
			id:    1,
			t:     sql.NullString{String: `["x", "y"]`, Valid: true},
			tType: sql.NullString{String: "ARRAY", Valid: true},
			n:     sql.NullString{String: `[1, 2]`, Valid: true},
			nType: sql.NullString{String: "ARRAY", Valid: true},
		},
		{
			id:    2,
			t:     sql.NullString{String: `[]`, Valid: true},
			tType: sql.NullString{String: "ARRAY", Valid: true},
			n:     sql.NullString{String: `[]`, Valid: true},
			nType: sql.NullString{String: "ARRAY", Valid: true},
		},
		{
			id: 3,
			// NULL whole-array column → SQL NULL, NOT `[]`.
			t:     sql.NullString{Valid: false},
			tType: sql.NullString{Valid: false},
			n:     sql.NullString{Valid: false},
			nType: sql.NullString{Valid: false},
		},
	}

	db, err := sql.Open("mysql", mysqlTarget)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx,
		"SELECT id, t, JSON_TYPE(t), n, JSON_TYPE(n) FROM a ORDER BY id")
	if err != nil {
		t.Fatalf("query target: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.t, &r.tType, &r.n, &r.nType); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("got %d rows; want %d (pre-fix: 0 rows copied)", len(got), len(want))
	}
	for i, g := range got {
		w := want[i]
		if g.id != w.id {
			t.Errorf("row[%d] id: got %d; want %d", i, g.id, w.id)
		}
		assertJSONCell(t, i, "t", g.t, g.tType, w.t, w.tType)
		assertJSONCell(t, i, "n", g.n, g.nType, w.n, w.nType)
	}
}

// assertJSONCell compares a MySQL JSON cell's value and JSON_TYPE
// against the expected shape. NULL (SQL null) and a JSON array are the
// two load-bearing distinctions for Bug 18 — a NULL array column must
// not silently become `[]`. JSON text is compared with whitespace
// tolerance (normaliseJSON, defined in migrate_bug14_integration_test.go)
// so MySQL's version-to-version JSON formatting doesn't trip the test.
func assertJSONCell(t *testing.T, rowIdx int, col string, gotVal, gotType, wantVal, wantType sql.NullString) {
	t.Helper()
	if gotVal.Valid != wantVal.Valid {
		t.Errorf("row[%d].%s NULL-ness: got valid=%v (%q); want valid=%v (%q) (Bug 18: NULL array column must stay SQL NULL, not [])",
			rowIdx, col, gotVal.Valid, gotVal.String, wantVal.Valid, wantVal.String)
		return
	}
	if !wantVal.Valid {
		return // both SQL NULL — nothing more to compare.
	}
	if gotType.String != wantType.String {
		t.Errorf("row[%d].%s JSON_TYPE: got %q; want %q",
			rowIdx, col, gotType.String, wantType.String)
	}
	if normaliseJSON(gotVal.String) != normaliseJSON(wantVal.String) {
		t.Errorf("row[%d].%s value: got %q; want %q",
			rowIdx, col, gotVal.String, wantVal.String)
	}
}
