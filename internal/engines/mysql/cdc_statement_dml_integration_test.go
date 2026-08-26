//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// M2 critic P2 — the STATEMENT-DML dispatch belt on a real mysqld. The
// global stays ROW (so every preflight passes); a SUPER writer session
// then flips SESSION binlog_format=STATEMENT — the exact residue the
// 68e preflight documents slipping past it — and the belt must stop
// the stream loudly instead of the pre-belt behaviour (the statement
// fell into the generic-DDL arm: cache cleared, nothing applied, no
// error, position frozen). Both directions: an OUT-of-scope session's
// statement DML must NOT kill the stream (Bug 246 discipline), proven
// by a later in-scope row-format write arriving on the same stream.

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestCDCReader_StatementDMLTripwire boots its own container (session
// overrides leak nothing globally, but the shared TestMain container's
// concurrent tests must not observe this test's writes).
func TestCDCReader_StatementDMLTripwire(t *testing.T) {
	dsn, cleanup := startMySQLM2Preflight(t)
	defer cleanup()

	applyMySQL(t, dsn, `
		CREATE TABLE orders (id BIGINT NOT NULL, v VARCHAR(32) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
		INSERT INTO orders (id, v) VALUES (1, 'seed');
		CREATE DATABASE other_db;
		CREATE TABLE other_db.t (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;
	`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// execOnSessionConn pins ONE connection so the SESSION override and
	// the DML run in the same session (the same vehicle as the row-image
	// belt test).
	execOnSessionConn := func(t *testing.T, connDSN string, stmts ...string) {
		t.Helper()
		db, err := sql.Open("mysql", connDSN)
		if err != nil {
			t.Fatalf("open writer: %v", err)
		}
		defer func() { _ = db.Close() }()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("pin writer conn: %v", err)
		}
		defer func() { _ = conn.Close() }()
		for _, stmt := range stmts {
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				t.Fatalf("writer session %q: %v", stmt, err)
			}
		}
	}

	openStream := func(t *testing.T) (*CDCReader, <-chan ir.Change) {
		t.Helper()
		rdr, err := eng.OpenCDCReader(ctx, dsn)
		if err != nil {
			t.Fatalf("OpenCDCReader: %v", err)
		}
		t.Cleanup(func() { _ = rdr.(*CDCReader).Close() })
		changes, err := rdr.(*CDCReader).StreamChanges(ctx, ir.Position{})
		if err != nil {
			t.Fatalf("StreamChanges: %v", err)
		}
		time.Sleep(200 * time.Millisecond) // syncer registration boundary
		return rdr.(*CDCReader), changes
	}

	// --- Out-of-scope survival + DDL control first (same stream): an
	// out-of-scope session's statement DML and an in-scope DDL both keep
	// the stream alive, proven by an in-scope row-format write arriving
	// afterwards.
	t.Run("out_of_scope_statement_dml_survives", func(t *testing.T) {
		rdr, changes := openStream(t)

		// A STATEMENT-format writer whose session default database is
		// other_db (out of the reader's source_db scope).
		execOnSessionConn(
			t,
			dsnForDatabase(t, dsn, "source_db", "other_db"),
			"SET SESSION binlog_format = 'STATEMENT'",
			"INSERT INTO t (id) VALUES (100)",
		)
		// In-scope DDL as query text: first token CREATE, must not trip.
		applyMySQL(t, dsn, "CREATE TABLE ddl_ok (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB;")
		// In-scope ROW-format write: must arrive, proving the stream
		// survived both query events above.
		applyMySQL(t, dsn, "INSERT INTO orders (id, v) VALUES (2, 'alive');")

		got := drainChanges(t, ctx, changes, 1, 30*time.Second)
		if len(got) != 1 {
			if streamErr := rdr.Err(); streamErr != nil {
				t.Fatalf("stream died on out-of-scope statement DML / in-scope DDL (false kill): %v", streamErr)
			}
			t.Fatalf("got %d changes; want 1", len(got))
		}
		if _, ok := got[0].(ir.Insert); !ok {
			t.Fatalf("change[0] = %T; want ir.Insert", got[0])
		}
		_ = rdr.Close()
	})

	// --- The belt: an IN-scope session-STATEMENT DML must stop the
	// stream with the coded refusal — never a silent drop.
	t.Run("in_scope_statement_dml_stops_stream", func(t *testing.T) {
		rdr, changes := openStream(t)

		execOnSessionConn(
			t, dsn,
			"SET SESSION binlog_format = 'STATEMENT'",
			"INSERT INTO orders (id, v) VALUES (3, 'stealthy')",
		)

		deadline := time.After(30 * time.Second)
		for {
			select {
			case _, open := <-changes:
				if !open {
					err := rdr.Err()
					if err == nil {
						t.Fatal("stream closed with nil Err after an in-scope statement DML; want the coded refusal")
					}
					ce, ok := sluicecode.FromError(err)
					if !ok || ce.Code != sluicecode.CodeCDCStatementDML {
						t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCStatementDML, err, err)
					}
					return
				}
				// Non-terminal changes (schema snapshots, tx boundaries)
				// may precede the poisoned event; keep draining.
			case <-deadline:
				t.Fatal("stream did not stop within 30s of the in-scope statement DML")
			}
		}
	})
}
