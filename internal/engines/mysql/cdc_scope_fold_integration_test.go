//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Audit 2026-09-09 RC-1b — on a FOLDING server the reader's scope must
// match the name the server stores, not the name the DSN spelled.
//
// Measured before the fix on real mysql:8.0 at lower_case_table_names=1:
// a DSN spelled `/SOURCE_DB` connects, counts and inserts into the same
// table the lowercase DSN does (the server folds every identifier), and
// the CDC reader opened on it emitted ZERO inserts for the same write the
// lowercase-DSN reader emitted — only commit markers, at a green stream.
// The reader compared the Table_map's stored `source_db` against the
// DSN's `SOURCE_DB` byte-exactly.
//
// Both DSN spellings are driven against one folding server. The
// lowercase cell is the control that proves the harness sees inserts at
// all; without it a broken harness would green the uppercase cell for
// the wrong reason.

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestCDCReader_ScopeFollowsTheServersFold(t *testing.T) {
	// Upstream image: the pre-baked one is initialised at
	// lower_case_table_names=0 and MySQL 8 refuses to boot it under 1.
	dsn, cleanup := startMySQLM2PreflightImage(t, "mysql:8.0", "--lower-case-table-names=1")
	defer cleanup()
	applyMySQL(t, dsn, `CREATE TABLE t (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;`)

	for _, spelling := range []string{"source_db", "SOURCE_DB"} {
		t.Run("dsn_"+spelling, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			rdsn := dsn
			if spelling != "source_db" {
				rdsn = dsnForDatabase(t, dsn, "source_db", spelling)
			}
			rdr, err := (Engine{Flavor: FlavorVanilla}).OpenCDCReader(ctx, rdsn)
			if err != nil {
				t.Fatalf("OpenCDCReader: %v", err)
			}
			defer func() { _ = rdr.(*CDCReader).Close() }()
			ch, err := rdr.(*CDCReader).StreamChanges(ctx, ir.Position{})
			if err != nil {
				t.Fatalf("StreamChanges: %v", err)
			}
			if !rdr.(*CDCReader).foldScopeNames {
				t.Fatal("reader did not learn the server folds (lower_case_table_names=1) — the cell below would measure the wrong regime")
			}

			// Give the pump its startup window, then write through THIS
			// spelling: the server resolving it is part of the premise.
			time.Sleep(3 * time.Second)
			db, err := sql.Open("mysql", rdsn)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			var n int
			if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&n); err != nil {
				t.Fatalf("count via %q DSN: %v", spelling, err)
			}
			if _, err := db.ExecContext(ctx, "INSERT INTO t (id) VALUES (?)", n+1); err != nil {
				t.Fatalf("insert via %q DSN: %v", spelling, err)
			}

			deadline := time.After(30 * time.Second)
			for {
				select {
				case c, ok := <-ch:
					if !ok {
						t.Fatalf("stream closed before the insert arrived; reader err=%v", rdr.(*CDCReader).Err())
					}
					if ins, isIns := c.(ir.Insert); isIns {
						if ins.Table != "t" || ins.Row["id"] == nil {
							t.Fatalf("unexpected insert %+v", ins)
						}
						return // the write reached the stream under this spelling
					}
				case <-deadline:
					t.Fatalf("DSN %q: no insert reached the stream within 30s — the reader's scope compare dropped a write the server folded into scope (RC-1b)", spelling)
				}
			}
		})
	}
}
