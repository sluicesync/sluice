// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"

	_ "modernc.org/sqlite"
)

// execD1Handler returns a mock-D1 handler that EXECUTES the incoming SQL against
// a real modernc SQLite db and serialises the result rows into a D1 envelope. It
// turns the httptest mock into a faithful SQLite executor, so the staging
// materializer runs its real read/projection SQL against real data — the only
// way to prove the staged copy is byte-faithful to the source.
func execD1Handler(db *sql.DB) d1Handler {
	return func(sqlStr string, params []string) (int, []byte) {
		args := make([]any, len(params))
		for i, p := range params {
			args[i] = p
		}
		rows, err := db.QueryContext(context.Background(), sqlStr, args...)
		if err != nil {
			return http.StatusOK, d1Err(1, err.Error())
		}
		defer func() { _ = rows.Close() }()
		cols, err := rows.Columns()
		if err != nil {
			return http.StatusOK, d1Err(1, err.Error())
		}
		var results []map[string]any
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return http.StatusOK, d1Err(1, err.Error())
			}
			m := make(map[string]any, len(cols))
			for i, c := range cols {
				v := vals[i]
				if b, ok := v.([]byte); ok {
					v = string(b) // modernc returns TEXT as []byte; D1 sends a JSON string
				}
				m[c] = v
			}
			results = append(results, m)
		}
		if err := rows.Err(); err != nil {
			return http.StatusOK, d1Err(1, err.Error())
		}
		return http.StatusOK, d1OK(results)
	}
}

// TestStageD1_ByteFaithful pins the Strategy-A staging materializer: replicating
// a (mock-backed) live D1 into a local SQLite file must be BYTE-FAITHFUL — every
// cell's exact storage class and value preserved (integers > 2^53 included),
// generated columns recomputed from the verbatim DDL, explicit indexes
// recreated, and WITHOUT ROWID / composite-PK tables handled. Faithfulness is
// asserted with SQLite's quote(): it renders each value with its storage class
// (text quoted, blob as X'…', numbers bare, NULL), so a per-row quote()-dump
// equal between source and staged copy proves an exact replica.
func TestStageD1_ByteFaithful(t *testing.T) {
	srcPath := seedDB(
		t,
		// Every storage class incl. a big integer > 2^53 (the JS-double trap),
		// a REAL, a BLOB, and NULLs.
		`CREATE TABLE t_basic (id INTEGER PRIMARY KEY, big INTEGER, r REAL, txt TEXT, b BLOB, n TEXT)`,
		`INSERT INTO t_basic (id,big,r,txt,b,n) VALUES
			(1, 9007199254740993, 3.5, 'hello', X'48656C6C6F', NULL),
			(5, -9007199254740995, 0.0, '', X'00FF', 'x'),
			(9, 9223372036854775807, 1.25, 'tab	end', NULL, NULL)`,
		// Generated column: must be SKIPPED on insert and recomputed from the DDL.
		`CREATE TABLE t_gen (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER,
			c INTEGER GENERATED ALWAYS AS (a + b) STORED)`,
		`INSERT INTO t_gen (id,a,b) VALUES (1,2,3),(2,10,20),(3,100,NULL)`,
		// Explicit unique index: must be recreated in the staged file.
		`CREATE TABLE t_unique (id INTEGER PRIMARY KEY, code TEXT)`,
		`INSERT INTO t_unique (id,code) VALUES (1,'a'),(2,'b'),(3,'c')`,
		`CREATE UNIQUE INDEX idx_code ON t_unique(code)`,
		// WITHOUT ROWID (PK keyset) and composite PK.
		`CREATE TABLE t_wr (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID`,
		`INSERT INTO t_wr (k,v) VALUES ('a',1),('b',2),('c',3)`,
		`CREATE TABLE t_comp (a INTEGER, b TEXT, v INTEGER, PRIMARY KEY(a,b))`,
		`INSERT INTO t_comp (a,b,v) VALUES (1,'x',10),(1,'y',20),(2,'x',30)`,
	)

	srcDB, err := sql.Open("sqlite", srcPath)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	t.Cleanup(func() { _ = srcDB.Close() })

	mock := startMockD1(t, execD1Handler(srcDB))
	dest := filepath.Join(t.TempDir(), "stage.db")
	if err := stageD1ClientToLocalFile(context.Background(), mock, dest, nil); err != nil {
		t.Fatalf("stage: %v", err)
	}

	dstDB, err := sql.Open("sqlite", dest)
	if err != nil {
		t.Fatalf("open staged: %v", err)
	}
	t.Cleanup(func() { _ = dstDB.Close() })

	for _, tc := range []struct{ table, orderBy string }{
		{"t_basic", "id"},
		{"t_gen", "id"},
		{"t_unique", "id"},
		{"t_wr", "k"},
		{"t_comp", "a, b"},
	} {
		t.Run(tc.table, func(t *testing.T) {
			src := dumpTableQuoted(t, srcDB, tc.table, tc.orderBy)
			dst := dumpTableQuoted(t, dstDB, tc.table, tc.orderBy)
			if !reflect.DeepEqual(src, dst) {
				t.Errorf("staged copy of %s differs from source:\n src=%v\n dst=%v", tc.table, src, dst)
			}
			if len(src) == 0 {
				t.Errorf("%s: no rows compared (test bug)", tc.table)
			}
		})
	}

	// The explicit index must exist in the staged file.
	var n int
	if err := dstDB.QueryRowContext(context.Background(),
		"SELECT count(*) FROM sqlite_master WHERE type='index' AND name='idx_code'").Scan(&n); err != nil {
		t.Fatalf("index check: %v", err)
	}
	if n != 1 {
		t.Errorf("explicit index idx_code missing from staged file (got %d)", n)
	}
}

// dumpTableQuoted returns one string per row: each column rendered via SQLite
// quote() (storage-class-faithful) joined by '|', ordered deterministically.
func dumpTableQuoted(t *testing.T, db *sql.DB, table, orderBy string) []string {
	t.Helper()
	ctx := context.Background()

	cols := quotedColumnExprs(t, db, table)
	sel := "SELECT "
	for i, c := range cols {
		if i > 0 {
			sel += " || '|' || "
		}
		sel += c
	}
	sel += " FROM " + quoteSQLiteIdent(table) + " ORDER BY " + orderBy

	rows, err := db.QueryContext(ctx, sel)
	if err != nil {
		t.Fatalf("dump %s: %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		out = append(out, line)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("dump rows: %v", err)
	}
	return out
}

// quotedColumnExprs returns `quote("col")` expressions for every column of table.
func quotedColumnExprs(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		"SELECT name FROM pragma_table_info("+quoteSQLiteIdent(table)+")")
	if err != nil {
		t.Fatalf("table_info(%s): %v", table, err)
	}
	defer func() { _ = rows.Close() }()
	var cols []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan col: %v", err)
		}
		cols = append(cols, "quote("+quoteSQLiteIdent(name)+")")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("col rows: %v", err)
	}
	return cols
}
