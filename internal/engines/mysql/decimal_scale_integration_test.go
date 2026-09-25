//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRowWriter_BatchedInsert_DecimalScale pins GC-37 (c) on the batched
// INSERT path — PlanetScale's cold copy and the LOAD DATA fallback. That path
// was already LOUD before the fix (MEASURED under the guard-off mutant: the
// server's Note 1265 made reportBulkWriteWarnings refuse the strict-mode
// write); it now refuses client-side with DECIMAL-SCALE-EXCEEDED before the
// wire, naming the column, and leaves no row. An at-scale value and one whose
// only excess digits are trailing zeros must land exact. Independent expected
// value: the literal written, read back through the server's own
// CAST(v AS CHAR).
func TestRowWriter_BatchedInsert_DecimalScale(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	applyDDL(t, dsn, "CREATE TABLE ds (id INT PRIMARY KEY, v DECIMAL(65,30))")

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	table := findTable(schema, "ds")
	if table == nil {
		t.Fatalf("ds table not found; have %v", tableNames(schema))
	}

	write := func(rows ...ir.Row) error {
		rwGeneric, err := Engine{Flavor: FlavorPlanetScale}.OpenRowWriter(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenRowWriter: %v", err)
		}
		defer closeIf(rwGeneric)
		rw := rwGeneric.(*RowWriter)
		if rw.bulkLoad != ir.BulkLoadBatchedInsert {
			t.Fatalf("writer bulk-load method = %v; this pin must exercise the batched INSERT path", rw.bulkLoad)
		}
		in := make(chan ir.Row, len(rows))
		for _, r := range rows {
			in <- r
		}
		close(in)
		return rw.WriteRows(ctx, table, in)
	}

	if err := write(
		ir.Row{"id": int64(1), "v": "0.123456789012345678901234567890"},
		ir.Row{"id": int64(2), "v": "1.50000000000000000000000000000000000"},
	); err != nil {
		t.Fatalf("exact values refused: %v", err)
	}
	err = write(ir.Row{"id": int64(3), "v": "0.1234567890123456789012345678901"})
	if err == nil || !strings.Contains(err.Error(), decimalScaleExceededMarker) {
		t.Fatalf("over-scale value: WriteRows returned %v; want the %s refusal", err, decimalScaleExceededMarker)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	got := map[int]string{}
	rows, err := db.QueryContext(ctx, "SELECT id, CAST(v AS CHAR) FROM ds ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id int
		var v string
		if err := rows.Scan(&id, &v); err != nil {
			t.Fatal(err)
		}
		got[id] = v
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := map[int]string{1: "0.123456789012345678901234567890", 2: "1.500000000000000000000000000000"}
	if len(got) != len(want) {
		t.Fatalf("target holds %v; want exactly %v (the refused row must not land)", got, want)
	}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("id=%d: target holds %s, want %s", id, got[id], w)
		}
	}
}
