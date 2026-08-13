// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

// TestModerncExecRunsMultiStatementStrings pins the PREMISE behind the
// C1-1 SQLite door (audit backlog 2026-08-13): modernc's driver
// executes EVERY statement in an Exec string, so an emitted DDL string
// that is structurally more than one statement runs the extra SQL —
// unlike go-sql-driver/mysql, which refuses multi-statement strings by
// default and makes the MySQL lane structurally immune. If this pin
// ever fails, modernc changed behaviour and the door's rationale (and
// the backlog filing) need re-deriving.
func TestModerncExecRunsMultiStatementStrings(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE a (x int); CREATE TABLE b (y int); INSERT INTO a VALUES (1)`); err != nil {
		t.Fatalf("multi-statement Exec errored — modernc no longer runs multi-statement strings, "+
			"and the C1-1 SQLite door premise is gone: %v", err)
	}
	var n int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM b`).Scan(&n); err != nil {
		t.Fatalf("second statement did not execute — premise changed: %v", err)
	}
	var m int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM a`).Scan(&m); err != nil || m != 1 {
		t.Fatalf("third statement did not execute (rows=%d, err=%v) — premise changed", m, err)
	}
}
