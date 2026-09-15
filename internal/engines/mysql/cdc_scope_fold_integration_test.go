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
	"strings"
	"sync/atomic"
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
			if rdr.(*CDCReader).lowerCaseTableNames != 1 {
				t.Fatalf("reader learned lower_case_table_names=%d; want 1 — the cell below would measure the wrong regime", rdr.(*CDCReader).lowerCaseTableNames)
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

// TestCDCReader_EmittedTruncateNameFollowsTheServersFold is the payload
// half of RC-1b, which the test above does not reach: it drives INSERT
// only, and its file comment claimed the whole scope (audit 2026-09-15
// A0915-MYSQL-HIGH-1 — a gate narrower than its name).
//
// The TRUNCATE arm is the one event this lane builds from QUERY TEXT
// rather than from the Table_map, so on a folding server it carried the
// operator's spelling (`T`) while every row event carried the stored one
// (`t`); the pipeline's byte-exact filter and a case-sensitive target
// then treated them as two tables.
//
// The independent expected value is the ir.Insert's name for the SAME
// table, written through the same spelling: it comes from the Table_map,
// which the code under test never touches. Both server regimes are
// driven, because each alone can pass for the wrong reason — a
// fold-always mutant greens the lct=1 cell and reds the lct=0 one, where
// the mixed-case stored name must come back byte-exact.
//
// Two resolution sources are driven on the real server (the unit test
// grades the third, the lct=2 regime, which no Linux container can run):
// a table whose rows the reader has already MAPPED (each TRUNCATE is
// preceded by an INSERT, so the stored spelling comes off the Table_map
// record) and a table the reader has never seen a row for (the catalog
// lookup — the information_schema query is what this cell proves valid
// on a real 8.0; its answer is checked against the CREATE spelling). The
// lookup is counted through the reader's seam, set before the pump
// starts: on lct=1 it must be asked exactly once (the unmapped table,
// never the mapped ones), on lct=0 never — a resolution that consulted
// the catalog on a case-sensitive server, or ahead of the reader's own
// record, shows here.
func TestCDCReader_EmittedTruncateNameFollowsTheServersFold(t *testing.T) {
	cells := []struct {
		name            string
		args            []string
		wantLCT         int
		table           string   // the CREATE TABLE spelling (= the stored name on both regimes)
		truncates       []string // the TRUNCATE statements, in the operator's spelling
		unmappedTable   string   // a second table the reader never sees a row event for
		unmappedTrunc   string   // its TRUNCATE, in the operator's spelling
		wantLookupCalls int64
	}{
		{
			name:    "lct1_folding_server",
			args:    []string{"--lower-case-table-names=1"},
			wantLCT: 1,
			table:   "t",
			// Unqualified with the table upper-cased, then qualified with
			// BOTH halves upper-cased: the schema fold and the table fold
			// are each exercised against the real server.
			truncates:       []string{"TRUNCATE TABLE T", "TRUNCATE TABLE SOURCE_DB.T"},
			unmappedTable:   "u",
			unmappedTrunc:   "TRUNCATE TABLE SOURCE_DB.U",
			wantLookupCalls: 1,
		},
		{
			name:            "lct0_case_sensitive_server_stays_byte_exact",
			args:            nil, // the upstream image's Linux default, lct=0
			wantLCT:         0,
			table:           "Tt",
			truncates:       []string{"TRUNCATE TABLE Tt", "TRUNCATE TABLE source_db.Tt"},
			unmappedTable:   "Uu",
			unmappedTrunc:   "TRUNCATE TABLE source_db.Uu",
			wantLookupCalls: 0,
		},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			dsn, cleanup := startMySQLM2PreflightImage(t, "mysql:8.0", cell.args...)
			defer cleanup()
			applyMySQL(t, dsn, "CREATE TABLE "+cell.table+" (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;")
			applyMySQL(t, dsn, "CREATE TABLE "+cell.unmappedTable+" (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;")

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			rdr, err := (Engine{Flavor: FlavorVanilla}).OpenCDCReader(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenCDCReader: %v", err)
			}
			defer func() { _ = rdr.(*CDCReader).Close() }()
			// Count the catalog lookups through the seam, delegating to
			// the REAL query so the server still answers it. Written
			// before the pump starts, read after the stream is drained.
			var lookupCalls atomic.Int64
			rdr.(*CDCReader).storedTableNameLookup = func(ctx context.Context, q rowQuerier, schema, table string) (string, string, bool, error) {
				lookupCalls.Add(1)
				return lookupStoredTableName(ctx, q, schema, table)
			}
			ch, err := rdr.(*CDCReader).StreamChanges(ctx, ir.Position{})
			if err != nil {
				t.Fatalf("StreamChanges: %v", err)
			}
			if got := rdr.(*CDCReader).lowerCaseTableNames; got != cell.wantLCT {
				t.Fatalf("reader learned lower_case_table_names=%d; want %d — the cell would measure the wrong regime", got, cell.wantLCT)
			}

			time.Sleep(3 * time.Second)
			db, err := sql.Open("mysql", dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			// One INSERT through the operator's spelling (the server
			// resolves it; that is the premise), then the TRUNCATEs. Each
			// TRUNCATE is preceded by an INSERT so the stream carries a
			// Table_map-sourced name right beside every text-sourced one.
			for i, stmt := range cell.truncates {
				spelled := strings.TrimPrefix(strings.TrimPrefix(stmt, "TRUNCATE TABLE "), "SOURCE_DB.")
				spelled = strings.TrimPrefix(spelled, "source_db.")
				if _, err := db.ExecContext(ctx, "INSERT INTO "+spelled+" (id) VALUES (?)", i+1); err != nil {
					t.Fatalf("insert through %q: %v", spelled, err)
				}
				if _, err := db.ExecContext(ctx, stmt); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}
			// The unmapped table: a TRUNCATE with no row event before it,
			// so the reader holds no Table_map record to resolve from.
			if _, err := db.ExecContext(ctx, cell.unmappedTrunc); err != nil {
				t.Fatalf("%s: %v", cell.unmappedTrunc, err)
			}

			var inserts []ir.Insert
			var truncates []ir.Truncate
			wantTruncates := len(cell.truncates) + 1
			deadline := time.After(45 * time.Second)
			for len(truncates) < wantTruncates {
				select {
				case c, ok := <-ch:
					if !ok {
						t.Fatalf("stream closed early; reader err=%v", rdr.(*CDCReader).Err())
					}
					switch e := c.(type) {
					case ir.Insert:
						inserts = append(inserts, e)
					case ir.Truncate:
						truncates = append(truncates, e)
					}
				case <-deadline:
					t.Fatalf("saw %d insert(s) and %d truncate(s) within 45s; want %d truncates", len(inserts), len(truncates), wantTruncates)
				}
			}
			if len(inserts) != len(cell.truncates) {
				t.Fatalf("saw %d inserts; want %d (one per mapped TRUNCATE) — the harness is not measuring what it claims", len(inserts), len(cell.truncates))
			}
			// The Table_map's spelling is the stored name: the premise
			// that makes the insert an INDEPENDENT expected value.
			for _, ins := range inserts {
				if ins.Table != cell.table || ins.Schema != "source_db" {
					t.Fatalf("Table_map-sourced insert carries %q.%q; want source_db.%s (the stored spelling)", ins.Schema, ins.Table, cell.table)
				}
			}
			for i, tr := range truncates[:len(cell.truncates)] {
				ins := inserts[i]
				if tr.Schema != ins.Schema || tr.Table != ins.Table {
					t.Fatalf("%s: emitted ir.Truncate names %q.%q but the Table_map-sourced ir.Insert for the same table names "+
						"%q.%q — the pipeline's byte-exact filter and a case-sensitive target see two tables (A0915-MYSQL-HIGH-1)",
						cell.truncates[i], tr.Schema, tr.Table, ins.Schema, ins.Table)
				}
			}
			// The unmapped table's expected value is its CREATE spelling
			// (the stored name on both regimes: lowercase on lct=1 where
			// it was created lowercase, byte-exact on lct=0).
			if last := truncates[len(cell.truncates)]; last.Schema != "source_db" || last.Table != cell.unmappedTable {
				t.Fatalf("%s: emitted ir.Truncate names %q.%q; want source_db.%s (the stored spelling of a table the reader never mapped)",
					cell.unmappedTrunc, last.Schema, last.Table, cell.unmappedTable)
			}
			if got := lookupCalls.Load(); got != cell.wantLookupCalls {
				t.Fatalf("the catalog lookup ran %d time(s); want %d — lct=%d resolves the mapped tables from the reader's own "+
					"Table_map record and only the unmapped one from information_schema, and lct=0 resolves nothing", got, cell.wantLookupCalls, cell.wantLCT)
			}
		})
	}
}
