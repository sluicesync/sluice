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
	"io"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"

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

	// awaitRefusal drains the stream until it closes and asserts the
	// coded refusal, naming verb and withholding secret.
	awaitRefusal := func(t *testing.T, rdr *CDCReader, changes <-chan ir.Change, verb, secret string) {
		t.Helper()
		deadline := time.After(30 * time.Second)
		for {
			select {
			case _, open := <-changes:
				if !open {
					err := rdr.Err()
					if err == nil {
						t.Fatalf("stream closed with nil Err after an in-scope statement %s; want the coded refusal", verb)
					}
					ce, ok := sluicecode.FromError(err)
					if !ok || ce.Code != sluicecode.CodeCDCStatementDML {
						t.Fatalf("want %s; got %T: %v", sluicecode.CodeCDCStatementDML, err, err)
					}
					if !strings.Contains(err.Error(), "a "+verb+" statement") {
						t.Fatalf("refusal does not name the verb %q: %v", verb, err)
					}
					// Audit 2026-08-27 A4: the refusal must not echo the
					// statement's payload — the row values would bypass
					// --redact by riding the error into logs.
					if strings.Contains(err.Error(), secret) || strings.Contains(ce.Hint, secret) {
						t.Fatalf("refusal leaked the statement's row values: %v", err)
					}
					return
				}
				// Non-terminal changes (schema snapshots, tx boundaries)
				// may precede the poisoned event; keep draining.
			case <-deadline:
				t.Fatalf("stream did not stop within 30s of the in-scope statement %s", verb)
			}
		}
	}

	// --- The belt: an IN-scope session-STATEMENT DML must stop the
	// stream with the coded refusal — never a silent drop.
	t.Run("in_scope_statement_dml_stops_stream", func(t *testing.T) {
		rdr, changes := openStream(t)
		execOnSessionConn(
			t, dsn,
			"SET SESSION binlog_format = 'STATEMENT'",
			"INSERT INTO orders (id, v) VALUES (3, 'stealthy')",
		)
		awaitRefusal(t, rdr, changes, "INSERT", "stealthy")
	})

	// --- Audit 2026-09-01 SLM-3: the four shapes that bypassed the belt
	// silently, each observed on 8.0.46 as rows missing from the target
	// with the stream alive. Same container, one stream per shape.

	// Shape 2: DML wrapped entirely in a versioned comment. The server
	// EXECUTES the contents and binlogs the statement verbatim.
	t.Run("slm3_shape2_versioned_comment_wrapped_dml_stops_stream", func(t *testing.T) {
		rdr, changes := openStream(t)
		execOnSessionConn(
			t, dsn,
			"SET SESSION binlog_format = 'STATEMENT'",
			"/*!50000 INSERT INTO orders (id, v) VALUES (10, 'versioned') */",
		)
		awaitRefusal(t, rdr, changes, "INSERT", "versioned")
	})

	// Shape 3: `--` followed by a newline. The go driver sends the text
	// raw (the `mysql` CLI strips a bare `--` line client-side, which is
	// why hand testing misses this); the server treats `--\n` as a
	// comment, executes the INSERT and binlogs the text as sent.
	t.Run("slm3_shape3_dash_newline_dml_stops_stream", func(t *testing.T) {
		rdr, changes := openStream(t)
		execOnSessionConn(
			t, dsn,
			"SET SESSION binlog_format = 'STATEMENT'",
			"--\nINSERT INTO orders (id, v) VALUES (13, 'dashnl')",
		)
		awaitRefusal(t, rdr, changes, "INSERT", "dashnl")
	})

	// Shape 4: cross-database DML from an OUT-of-scope session database.
	// The session's default database is other_db; the statement writes
	// into source_db by qualifying the table.
	t.Run("slm3_shape4_cross_database_dml_from_out_of_scope_session_stops_stream", func(t *testing.T) {
		rdr, changes := openStream(t)
		execOnSessionConn(
			t,
			dsnForDatabase(t, dsn, "source_db", "other_db"),
			"SET SESSION binlog_format = 'STATEMENT'",
			"INSERT INTO source_db.orders (id, v) VALUES (600, 'crossdb'), (601, 'crossdb2')",
		)
		awaitRefusal(t, rdr, changes, "INSERT", "crossdb")
	})

	// Shape 1: statement-format LOAD DATA, which the server writes as
	// BEGIN_LOAD_QUERY + EXECUTE_LOAD_QUERY rather than a QueryEvent. The
	// go driver's Reader-handler protocol supplies the file from memory;
	// local_infile must be ON server-side and allowAllFiles on the DSN.
	t.Run("slm3_shape1_statement_load_data_stops_stream", func(t *testing.T) {
		setSessionVar(t, dsn, "SET GLOBAL local_infile = 1")
		t.Cleanup(func() { setSessionVar(t, dsn, "SET GLOBAL local_infile = 0") })
		const handler = "slm3_shape1"
		driver.RegisterReaderHandler(handler, func() io.Reader {
			return strings.NewReader("20\tloaded20\n21\tloaded21\n")
		})
		t.Cleanup(func() { driver.DeregisterReaderHandler(handler) })

		rdr, changes := openStream(t)
		execOnSessionConn(
			t, dsn+"&allowAllFiles=true",
			"SET SESSION binlog_format = 'STATEMENT'",
			"LOAD DATA LOCAL INFILE 'Reader::"+handler+"' INTO TABLE orders FIELDS TERMINATED BY '\t' (id, v)",
		)
		awaitRefusal(t, rdr, changes, "LOAD DATA", "loaded20")
	})

	// Out-of-scope LOAD DATA must NOT kill the stream (Bug 246 discipline
	// applied to the new arm), proven the same way as the QueryEvent case.
	t.Run("slm3_shape1_out_of_scope_load_data_survives", func(t *testing.T) {
		setSessionVar(t, dsn, "SET GLOBAL local_infile = 1")
		t.Cleanup(func() { setSessionVar(t, dsn, "SET GLOBAL local_infile = 0") })
		const handler = "slm3_shape1_other"
		driver.RegisterReaderHandler(handler, func() io.Reader { return strings.NewReader("300\n") })
		t.Cleanup(func() { driver.DeregisterReaderHandler(handler) })

		rdr, changes := openStream(t)
		execOnSessionConn(
			t, dsnForDatabase(t, dsn, "source_db", "other_db")+"&allowAllFiles=true",
			"SET SESSION binlog_format = 'STATEMENT'",
			"LOAD DATA LOCAL INFILE 'Reader::"+handler+"' INTO TABLE t (id)",
		)
		applyMySQL(t, dsn, "INSERT INTO orders (id, v) VALUES (4, 'alive_after_load');")
		got := drainChanges(t, ctx, changes, 1, 30*time.Second)
		if len(got) != 1 {
			if streamErr := rdr.Err(); streamErr != nil {
				t.Fatalf("stream died on an out-of-scope LOAD DATA (false kill): %v", streamErr)
			}
			t.Fatalf("got %d changes; want 1", len(got))
		}
		_ = rdr.Close()
	})

	// The comment grammar, ground-truthed rather than transcribed (AQP-2):
	// every prefix the lexer treats as a comment must be one to the
	// server too — each prefix + `SELECT 1` executes — and the one shape
	// the lexer refuses to treat as a comment (`--x`) must be a syntax
	// error. A lexer that were LOOSER than the server would refuse a
	// statement the server never ran; one that were STRICTER is the
	// silent-drop direction this audit found.
	t.Run("aqp2_comment_grammar_matches_the_server", func(t *testing.T) {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer func() { _ = db.Close() }()
		for name, prefix := range statementDMLCommentPrefixes {
			q := prefix.wrap("SELECT 1")
			if strings.HasPrefix(prefix.text, "/*M!") {
				// MariaDB-only executable comment: MySQL treats it as a
				// plain comment, so the SELECT inside never runs and the
				// statement is empty — not a grammar disagreement, a
				// flavor one. Skipped on MySQL; stated here.
				continue
			}
			var one int
			if err := db.QueryRowContext(ctx, q).Scan(&one); err != nil || one != 1 {
				t.Errorf("%s: the server rejected %q (%v) — the lexer treats this prefix as a comment and the server does not", name, q, err)
			}
		}
		if _, err := db.ExecContext(ctx, "--x\nSELECT 1"); err == nil {
			t.Error("the server accepted `--x` as a comment; the lexer's bare-`--` arm is now looser than the server")
		}
	})
}
