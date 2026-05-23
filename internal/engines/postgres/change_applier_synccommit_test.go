// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

// # F7 unit pin: SET LOCAL synchronous_commit = on emits on every apply tx
//
// Severity-A finding F7 from the 2026-05-22 PG-internals research run
// (see sluice-pg-internals-research-chapters-9-10-11-2026-05-22.md and
// the doc-comment on [ChangeApplier.forceSynchronousCommitOn]): a
// role- or database-level default of `synchronous_commit = off` will
// be silently inherited by the sluice apply session, allowing PG to
// ACK a COMMIT before the WAL is durably written. A target-side crash
// between the ACK and the WAL flush then breaks ADR-0007's "position
// + data lands durably together" guarantee.
//
// The fix is to force `SET LOCAL synchronous_commit = on` as the
// first statement on every apply transaction; SET LOCAL reverts at
// tx end so non-sluice sessions on the same role are unaffected.
// The integration-level proof against a real PG role lives in
// change_applier_synccommit_integration_test.go; this file pins the
// SQL emission itself via a recording driver so the literal can't
// regress under a refactor of the helper.

// recordingConn / recordingDriver / recordingStmt / recordingTx /
// recordingResult: a deliberately minimal database/sql/driver
// implementation that records every statement passed through Exec /
// Query (we only Exec in this test). All it needs to do is let
// database/sql open a tx and run a SET LOCAL inside it; nothing
// downstream needs real semantics. Returning io.EOF on Query is fine
// because the F7 helper only Execs.
type recordingConn struct {
	mu      *sync.Mutex
	queries *[]string
}

type recordingDriver struct {
	mu      *sync.Mutex
	queries *[]string
}

func (d recordingDriver) Open(_ string) (driver.Conn, error) {
	return recordingConn(d), nil
}

type recordingStmt struct {
	mu      *sync.Mutex
	queries *[]string
	query   string
}

func (recordingStmt) Close() error  { return nil }
func (recordingStmt) NumInput() int { return -1 }
func (s recordingStmt) Exec(_ []driver.Value) (driver.Result, error) {
	s.mu.Lock()
	*s.queries = append(*s.queries, s.query)
	s.mu.Unlock()
	return recordingResult{}, nil
}

func (s recordingStmt) Query(_ []driver.Value) (driver.Rows, error) {
	return nil, io.EOF
}

type recordingResult struct{}

func (recordingResult) LastInsertId() (int64, error) { return 0, nil }
func (recordingResult) RowsAffected() (int64, error) { return 0, nil }

type recordingTx struct {
	mu      *sync.Mutex
	queries *[]string
}

func (t recordingTx) Commit() error {
	t.mu.Lock()
	*t.queries = append(*t.queries, "<COMMIT>")
	t.mu.Unlock()
	return nil
}

func (t recordingTx) Rollback() error {
	t.mu.Lock()
	*t.queries = append(*t.queries, "<ROLLBACK>")
	t.mu.Unlock()
	return nil
}

func (c recordingConn) Prepare(query string) (driver.Stmt, error) {
	return recordingStmt{mu: c.mu, queries: c.queries, query: query}, nil
}

func (recordingConn) Close() error { return nil }

func (c recordingConn) Begin() (driver.Tx, error) {
	c.mu.Lock()
	*c.queries = append(*c.queries, "<BEGIN>")
	c.mu.Unlock()
	return recordingTx(c), nil
}

func newRecordingDB(t *testing.T) (*sql.DB, *[]string, *sync.Mutex) {
	t.Helper()
	var mu sync.Mutex
	queries := make([]string, 0, 8)
	d := recordingDriver{mu: &mu, queries: &queries}
	// database/sql panics if the same name is registered twice in a
	// process; use a unique name per call so parallel test runs and
	// repeated invocations within one process don't collide.
	name := fmt.Sprintf("recording-pg-f7-%p", &mu)
	sql.Register(name, d)
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("sql.Open(%s): %v", name, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, &queries, &mu
}

// TestForceSynchronousCommitOn_EmitsSetLocal is the F7 pin: the
// helper must emit exactly `SET LOCAL synchronous_commit = on` on the
// tx it's handed. A refactor that drops the statement (or changes
// scope to plain SET) silently un-hardens ADR-0007 against the
// role/db-level inheritance hazard; this test fails loud the moment
// the literal drifts.
func TestForceSynchronousCommitOn_EmitsSetLocal(t *testing.T) {
	db, queries, mu := newRecordingDB(t)
	a := &ChangeApplier{db: db}

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := a.forceSynchronousCommitOn(context.Background(), tx); err != nil {
		t.Fatalf("forceSynchronousCommitOn: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	mu.Lock()
	got := append([]string(nil), *queries...)
	mu.Unlock()

	// Expected sequence on the recorded connection:
	//   <BEGIN> -> SET LOCAL synchronous_commit = on -> <ROLLBACK>
	if len(got) < 2 {
		t.Fatalf("expected at least BEGIN + SET LOCAL emissions, got %v", got)
	}
	if got[0] != "<BEGIN>" {
		t.Errorf("first recorded op = %q; want <BEGIN>", got[0])
	}
	// The SET LOCAL must be the FIRST statement on the tx so every
	// subsequent applier statement runs under synchronous_commit = on.
	// "First statement after BEGIN" is the load-bearing property; a
	// later position would leave a window where a position write
	// could land under the inherited `off` value.
	if !strings.EqualFold(strings.TrimSpace(got[1]), "SET LOCAL synchronous_commit = on") {
		t.Errorf("first statement on tx = %q; want %q (F7 hardening regressed?)",
			got[1], "SET LOCAL synchronous_commit = on")
	}
}
