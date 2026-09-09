//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-09-09 A0909-MYSQL-MEDIUM-2 on a real MySQL: LOAD DATA LOCAL
// turns a CHECK constraint violation into warning 3819 and SKIPS the row,
// in strict and relaxed sql_mode alike (the worker measured `{2}` landing
// out of `(1,-5),(2,7),(3,-1)` with two warnings and no error in both).
// The writer must refuse that as a lost row in both modes; before the fix
// relaxed mode WARNed "clamped or truncated" and returned nil.

package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestRowWriter_LoadData_CheckViolationIsASkippedRowNotACoercion(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	enableLocalInfile(t, dsn)

	relaxed, strict := "", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"
	for _, cell := range []struct {
		name string
		mode *string
		// cappedList empties SHOW WARNINGS server-side (max_error_count=0)
		// so the 3819 sample count cannot fire and ONLY the first-attempt
		// inserted-vs-sent witness can catch the skip — the cell that
		// isolates that gate (@@warning_count stays accurate; COLD-1).
		cappedList bool
	}{{"strict", &strict, false}, {"relaxed", &relaxed, false}, {"relaxed_capped_warning_list", &relaxed, true}} {
		t.Run(cell.name, func(t *testing.T) {
			if cell.cappedList {
				applyDDL(t, dsn, "SET GLOBAL max_error_count = 0;")
				defer applyDDL(t, dsn, "SET GLOBAL max_error_count = DEFAULT;")
			}
			tableName := "ck_" + cell.name
			applyDDL(t, dsn, "CREATE TABLE "+tableName+
				" (id INT NOT NULL, q INT NULL, PRIMARY KEY (id), CONSTRAINT "+tableName+"_nonneg CHECK (q >= 0)) ENGINE=InnoDB;")
			sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenSchemaReader: %v", err)
			}
			defer closeIf(sr)
			schema, err := sr.ReadSchema(ctx)
			if err != nil {
				t.Fatalf("ReadSchema: %v", err)
			}
			table := findTable(schema, tableName)
			if table == nil {
				t.Fatalf("%s not found", tableName)
			}

			db, err := openDB(ctx, mustParseDSN(t, dsn), cell.mode)
			if err != nil {
				t.Fatalf("openDB: %v", err)
			}
			defer db.Close()
			w := &RowWriter{db: db, sqlMode: cell.mode, bulkLoad: ir.BulkLoadLoadDataInfile}
			in := make(chan ir.Row, 3)
			for _, r := range []ir.Row{{"id": int64(1), "q": int64(-5)}, {"id": int64(2), "q": int64(7)}, {"id": int64(3), "q": int64(-1)}} {
				in <- r
			}
			close(in)
			err = w.WriteRows(ctx, table, in)
			if err == nil {
				t.Fatalf("%s: WriteRows returned nil with two CHECK-violating rows — the rows were silently skipped "+
					"(target holds %d of 3)", cell.name, countRowsIn(t, ctx, dsn, tableName))
			}
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s: WriteRows timed out: %v", cell.name, err)
			}
			for _, want := range []string{loadDataRowsSkippedMarker, "SKIPPED"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s: refusal does not say %q: %v", cell.name, want, err)
				}
			}
			// The independent number: the source had 3 rows; the target
			// cannot hold all 3, whatever the refusal says.
			if n := countRowsIn(t, ctx, dsn, tableName); n >= 3 {
				t.Fatalf("%s: target holds %d rows; the CHECK cannot have admitted the negative ones", cell.name, n)
			}
			t.Logf("%s: refused as documented; target holds %d of 3: %v", cell.name, countRowsIn(t, ctx, dsn, tableName), err)
		})
	}
}

func countRowsIn(t *testing.T, ctx context.Context, dsn, table string) int {
	t.Helper()
	db, err := openDB(ctx, mustParseDSN(t, dsn), nil)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}
