//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRowReader_ReadRows_SurvivesALowServerStatementTimeout is the source-side
// counterpart to the copy lanes' statement_timeout pin, and it is a real
// server test because the thing under test is a server GUC.
//
// The COPY-lane pin landed on the write side and on the raw lane's byte-pipe.
// The typed IR lane's own SELECT was never pinned, so copying OUT of a server
// with a low statement_timeout still died — the read is killed with 57014
// having produced nothing, and the re-run hits the same wall at the same
// place. PlanetScale Neki ships statement_timeout = 30s as a platform default,
// which is what surfaced this, but nothing about it is Neki-specific: any
// operator who set a timeout for interactive work has it govern their
// migration.
//
// The table is deliberately small and the SLOWNESS is in the predicate. What
// has to be exercised is wall-clock time inside one statement, and 20ms per
// row over 40 rows crosses the 300ms limit by a wide, deterministic margin
// without making the test slow or the fixture large.
func TestRowReader_ReadRows_SurvivesALowServerStatementTimeout(t *testing.T) {
	const dbName = "read_stmt_timeout_db"
	dsn, cleanup := newSharedPGDB(t, dbName)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = admin.Close() }()

	if _, err := admin.ExecContext(ctx, `CREATE TABLE slow_rows (id INT PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO slow_rows SELECT g, 'v' || g FROM pg_catalog.generate_series(1, 40) g`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// The GUC is set on the DATABASE so it reaches every connection the
	// reader's own pool opens — the shape an operator actually produces
	// (ALTER DATABASE / ALTER ROLE), and the shape a managed platform ships.
	if _, err := admin.ExecContext(ctx,
		fmt.Sprintf(`ALTER DATABASE %s SET statement_timeout = '300ms'`, quoteIdent(dbName))); err != nil {
		t.Fatalf("set database statement_timeout: %v", err)
	}

	// Premise check: the limit is real on a FRESH connection. Without this
	// the test could pass because the timeout never applied at all — which
	// is precisely how the defect stayed invisible.
	fresh, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open fresh: %v", err)
	}
	defer func() { _ = fresh.Close() }()
	var shown string
	if err := fresh.QueryRowContext(ctx, `SHOW statement_timeout`).Scan(&shown); err != nil {
		t.Fatalf("show statement_timeout: %v", err)
	}
	if shown != "300ms" {
		t.Fatalf("statement_timeout reads back as %q on a fresh connection, want \"300ms\" — "+
			"the limit this test exists to cross is not in effect, so the assertions below would be vacuous", shown)
	}
	var sink int
	err = fresh.QueryRowContext(ctx, `SELECT 1 FROM pg_catalog.pg_sleep(1)`).Scan(&sink)
	if err == nil {
		t.Fatal("a 1s statement completed under a 300ms statement_timeout — the limit is not enforced here, " +
			"so this test cannot distinguish a pinned read from an unpinned one")
	}

	rr, err := (Engine{}).OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer func() { _ = rr.(*RowReader).Close() }()

	// 40 rows x 20ms = ~800ms in one statement, against a 300ms limit. The
	// predicate must be TRUE for every row as well as slow: pg_sleep returns
	// void, whose ::text rendering is the empty string, so this keeps all 40
	// rows. ("pg_sleep(...) IS NULL" is equally slow and silently selects
	// NOTHING — which reads exactly like a killed statement.)
	setter, ok := rr.(ir.RowFilterSetter)
	if !ok {
		t.Fatalf("reader %T does not accept row filters; this test needs one to make the read slow", rr)
	}
	setter.SetRowFilters(map[string]string{
		"slow_rows": "pg_catalog.pg_sleep(0.02)::text = ''",
	})

	table := &ir.Table{
		Name:   "slow_rows",
		Schema: "public",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 32}},
			{Name: "v", Type: ir.Text{}},
		},
	}

	start := time.Now()
	rows, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v (an unpinned read fails HERE, at the first statement)", err)
	}
	var got int
	for range rows {
		got++
	}
	elapsed := time.Since(start)

	errReader, ok := rr.(interface{ Err() error })
	if !ok {
		t.Fatal("reader does not expose Err(); a mid-stream 57014 would be indistinguishable from a clean end")
	}
	if rerr := errReader.Err(); rerr != nil {
		t.Fatalf("read failed after %v: %v — with statement_timeout pinned to 0 this read must complete, "+
			"however long it takes", elapsed, rerr)
	}
	if got != 40 {
		t.Fatalf("read %d rows, want 40 (after %v) — a truncated stream is the shape a killed statement takes",
			got, elapsed)
	}
	// The read MUST have outlived the limit, or it never tested the pin.
	if elapsed < 400*time.Millisecond {
		t.Fatalf("the read finished in %v, comfortably inside the 300ms limit — the predicate did not slow it "+
			"down, so a pinned and an unpinned read would both have passed", elapsed)
	}
}

// TestRowReader_ReadRows_PinDoesNotLeakIntoThePool is the other half, and the
// reason the pin is a SET LOCAL inside a transaction rather than a session SET
// or an after-connect hook.
//
// pgx's ResetSession issues no DISCARD ALL, so a session-scoped GUC set on a
// pooled connection stays set for whatever runs on it next. A leak here would
// silently disable the operator's statement_timeout for every later statement
// on that connection — the read would work and the protection would be gone,
// which is the worst of the available failure modes because nothing reports it.
//
// WHAT THIS GATE ACTUALLY DISTINGUISHES, measured by mutation rather than
// assumed: transaction-scoped from UNSCOPED. Replacing SET LOCAL with a plain
// SET inside the same transaction does NOT fail this test, and should not —
// PostgreSQL rolls a plain SET back with the transaction too, so that variant
// is equally leak-free. What fails it is dropping the transaction and setting
// the GUC on the bare pooled connection, which is the shape the leak actually
// takes. Read the name accordingly.
func TestRowReader_ReadRows_PinDoesNotLeakIntoThePool(t *testing.T) {
	const dbName = "read_stmt_timeout_leak_db"
	dsn, cleanup := newSharedPGDB(t, dbName)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = admin.Close() }()
	if _, err := admin.ExecContext(ctx, `CREATE TABLE leak_rows (id INT PRIMARY KEY)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		`INSERT INTO leak_rows SELECT g FROM pg_catalog.generate_series(1, 5) g`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := admin.ExecContext(ctx,
		fmt.Sprintf(`ALTER DATABASE %s SET statement_timeout = '7s'`, quoteIdent(dbName))); err != nil {
		t.Fatalf("set database statement_timeout: %v", err)
	}

	rr, err := (Engine{}).OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer func() { _ = rr.(*RowReader).Close() }()

	table := &ir.Table{
		Name:    "leak_rows",
		Schema:  "public",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}},
	}
	rows, err := rr.ReadRows(ctx, table)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	n := 0
	for range rows {
		n++
	}
	if n != 5 {
		t.Fatalf("read %d rows, want 5", n)
	}

	// The pinned connection has now gone back to the pool. Ask the reader's
	// OWN pool — several times, because which connection answers is not
	// ours to choose — whether the operator's timeout survived.
	counter, ok := rr.(ir.RowCounter)
	if !ok {
		t.Fatal("reader does not implement RowCounter; this test needs a second read on the same pool")
	}
	if _, err := counter.CountRows(ctx, table); err != nil {
		t.Fatalf("CountRows on the same pool: %v", err)
	}

	pool, ok := rr.(*RowReader).q.(*sql.DB)
	if !ok {
		t.Fatalf("reader's querier is %T, want *sql.DB — this test reads the pool directly", rr.(*RowReader).q)
	}
	for i := range 8 {
		var v string
		if err := pool.QueryRowContext(ctx, `SHOW statement_timeout`).Scan(&v); err != nil {
			t.Fatalf("show statement_timeout (probe %d): %v", i, err)
		}
		if v != "7s" {
			t.Fatalf("statement_timeout is %q on probe %d of the reader's own pool, want \"7s\" — the read's "+
				"pin LEAKED, and the operator's timeout is now disabled for every later statement on that "+
				"connection", v, i)
		}
	}
}
