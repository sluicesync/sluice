// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0108 cold-copy reparent-retry pins. These exercise the bounded
// retry around the per-batch flush (both the idempotent and the plain
// path), the FRESH-conn re-acquire requirement, the plain-path
// tolerate-1062-on-retry wart (and that a first-attempt 1062 stays
// terminal), exhaustion, and prompt ctx-cancel during backoff — all via
// a scriptable fake driver, no testcontainers.
//
// The retry bounds (coldCopyReparentBackoffBase = 100ms) make a full
// real backoff slow for a test; every test below shrinks the bounds via
// withFastReparentBackoff so the suite stays fast while still exercising
// the real loop, classifier, and conn re-acquire.

// scriptDriver is a fake database/sql driver whose ExecContext outcome is
// scripted per-INSERT-call by a shared atomic counter, so it survives the
// pool handing out a fresh *sql.Conn on each retry (the load-bearing
// re-acquire path). SHOW WARNINGS always returns zero rows (a clean
// flush) so the post-flush Vector-B probe passes. The driver records how
// many DISTINCT underlying connections were opened so a test can assert
// the retry actually re-acquired rather than reusing the dead conn.
type scriptDriver struct{ script *flushScript }

// flushScript holds the per-call exec outcomes plus instrumentation.
type flushScript struct {
	// execErrs[i] is returned by the (i+1)-th INSERT ExecContext call;
	// calls past the end return nil (success). A nil entry is success.
	execErrs []error

	execCalls atomic.Int64 // total INSERT ExecContext calls
	opens     atomic.Int64 // total driver.Open calls (distinct conns)
}

func (s *flushScript) nextExecErr() error {
	n := s.execCalls.Add(1) // 1-based
	idx := int(n - 1)
	if idx < len(s.execErrs) {
		return s.execErrs[idx]
	}
	return nil
}

type scriptConn struct{ script *flushScript }

func (d scriptDriver) Open(string) (driver.Conn, error) {
	d.script.opens.Add(1)
	return scriptConn(d), nil // identical single-field shape; staticcheck S1016 prefers the conversion
}

func (scriptConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (scriptConn) Close() error                        { return nil }
func (scriptConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }

func (c scriptConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(strings.TrimSpace(query), "INSERT") {
		if err := c.script.nextExecErr(); err != nil {
			return nil, err
		}
		return driver.RowsAffected(0), nil
	}
	return driver.RowsAffected(0), nil
}

// QueryContext serves SHOW WARNINGS as an empty result set (clean flush).
func (scriptConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(query, "SHOW WARNINGS") {
		return &emptyWarningsRows{}, nil
	}
	return &emptyWarningsRows{}, nil
}

// emptyWarningsRows is a 3-column (Level, Code, Message) empty result.
type emptyWarningsRows struct{}

func (*emptyWarningsRows) Columns() []string           { return []string{"Level", "Code", "Message"} }
func (*emptyWarningsRows) Close() error                { return nil }
func (*emptyWarningsRows) Next(_ []driver.Value) error { return io.EOF }

// newScriptDB registers a driver instance bound to this test's script and
// returns a *sql.DB over it. sql.Register is global and panics on a
// duplicate name; t.Name() is unique per test (subtests include the
// parent path) so the name is safe within a single test run / process.
func newScriptDB(t *testing.T, script *flushScript) *sql.DB {
	t.Helper()
	name := "sluice-script-test-" + t.Name()
	sql.Register(name, scriptDriver{script: script})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("open script db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// withFastReparentBackoff shrinks the package-level reparent backoff
// bounds for the duration of a test so the retry loop runs fast, and
// restores them after. The attempt cap is also lowered so the exhaustion
// test doesn't run the full 12 attempts.
func withFastReparentBackoff(t *testing.T, attempts int) {
	t.Helper()
	origAttempts := coldCopyReparentRetryAttemptsVar
	origBase := coldCopyReparentBackoffBaseVar
	origCap := coldCopyReparentBackoffCapVar
	coldCopyReparentRetryAttemptsVar = attempts
	coldCopyReparentBackoffBaseVar = time.Millisecond
	coldCopyReparentBackoffCapVar = 2 * time.Millisecond
	t.Cleanup(func() {
		coldCopyReparentRetryAttemptsVar = origAttempts
		coldCopyReparentBackoffBaseVar = origBase
		coldCopyReparentBackoffCapVar = origCap
	})
}

// pinReparentTable is a minimal single-PK table for the flush tests.
func pinReparentTable() *ir.Table {
	return &ir.Table{
		Name: "t_pin",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 8}, Nullable: false},
			{Name: "v", Type: ir.Text{}, Nullable: true},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
}

func feedReparentRows(n int) <-chan ir.Row {
	ch := make(chan ir.Row, n)
	for i := 0; i < n; i++ {
		ch <- ir.Row{"id": int64(i), "v": "x"}
	}
	close(ch)
	return ch
}

// vttabletUnavailable is the framed reparent shape (Error 1105
// code = Unavailable), the way "tablet not serving" normally surfaces.
func vttabletUnavailable() error {
	return &gomysql.MySQLError{Number: 1105, Message: "target: ks.-.primary: vttablet: rpc error: code = Unavailable desc = tablet not serving"}
}

// TestColdCopyReparentRetry_IdempotentConverges pins: idempotent flush
// hits a retriable (reparent) error N times then succeeds → the batch
// lands once and the copy converges, having re-acquired a fresh conn.
func TestColdCopyReparentRetry_IdempotentConverges(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{execErrs: []error{
		vttabletUnavailable(),
		vttabletUnavailable(),
		nil, // 3rd INSERT succeeds
	}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}
	table := pinReparentTable()

	if err := w.WriteRowsIdempotent(context.Background(), table, feedReparentRows(3)); err != nil {
		t.Fatalf("WriteRowsIdempotent: unexpected error: %v", err)
	}
	if got := script.execCalls.Load(); got != 3 {
		t.Errorf("INSERT exec calls = %d; want 3 (2 transient + 1 success)", got)
	}
	// At least 2 distinct conns opened: the pinned one + ≥1 fresh
	// re-acquire (the dead conn must never be reused).
	if got := script.opens.Load(); got < 2 {
		t.Errorf("driver Open calls = %d; want >= 2 (fresh conn re-acquired on retry)", got)
	}
}

// TestColdCopyReparentRetry_PlainConverges pins: plain flush hits a
// retriable error then succeeds → lands once.
func TestColdCopyReparentRetry_PlainConverges(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{execErrs: []error{
		errors.New("write tcp 10.0.0.1:3306: connection reset by peer"),
		nil,
	}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}
	table := pinReparentTable()

	if err := w.WriteRows(context.Background(), table, feedReparentRows(2)); err != nil {
		t.Fatalf("WriteRows: unexpected error: %v", err)
	}
	if got := script.execCalls.Load(); got != 2 {
		t.Errorf("INSERT exec calls = %d; want 2 (1 transient + 1 success)", got)
	}
	if got := script.opens.Load(); got < 2 {
		t.Errorf("driver Open calls = %d; want >= 2 (fresh conn re-acquired)", got)
	}
}

// TestColdCopyReparentRetry_PlainTolerates1062OnRetry is THE wart pin:
// committed-but-unacked → the retry of the byte-identical batch sees
// Error 1062 → it is TOLERATED (the rows already landed) and the copy
// continues to success. The transient comes FIRST (so the 1062 is a
// genuine retry-after-transient), then the retry collides.
func TestColdCopyReparentRetry_PlainTolerates1062OnRetry(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{execErrs: []error{
		// 1st attempt: target committed the batch but the ack was lost to a
		// reparent (we model the lost-ack as the transient that triggers
		// the retry).
		vttabletUnavailable(),
		// 2nd attempt (retry): the byte-identical batch collides with the
		// rows the committed-but-unacked first attempt already landed.
		&gomysql.MySQLError{Number: 1062, Message: "Duplicate entry '0' for key 't_pin.PRIMARY'"},
	}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}
	table := pinReparentTable()

	if err := w.WriteRows(context.Background(), table, feedReparentRows(3)); err != nil {
		t.Fatalf("WriteRows: tolerate-1062-on-retry should make this succeed; got error: %v", err)
	}
	if got := script.execCalls.Load(); got != 2 {
		t.Errorf("INSERT exec calls = %d; want 2 (transient then tolerated 1062)", got)
	}
}

// TestColdCopyReparentRetry_PlainFirstAttempt1062IsTerminal pins the
// tolerance scope: a FIRST-attempt 1062 (no preceding transient) is a
// real dup-key / dirty-target failure and MUST stay terminal — the
// tolerance must not leak to the first attempt.
func TestColdCopyReparentRetry_PlainFirstAttempt1062IsTerminal(t *testing.T) {
	withFastReparentBackoff(t, 12)
	dup := &gomysql.MySQLError{Number: 1062, Message: "Duplicate entry '0' for key 't_pin.PRIMARY'"}
	script := &flushScript{execErrs: []error{dup}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}
	table := pinReparentTable()

	err := w.WriteRows(context.Background(), table, feedReparentRows(3))
	if err == nil {
		t.Fatal("first-attempt 1062 must be TERMINAL; got nil error (tolerance leaked to the first attempt)")
	}
	var myErr *gomysql.MySQLError
	if !errors.As(err, &myErr) || myErr.Number != 1062 {
		t.Fatalf("first-attempt 1062 should surface as the underlying 1062; got %v", err)
	}
	// Exactly one exec: a first-attempt 1062 is non-retriable, so the loop
	// must not retry it.
	if got := script.execCalls.Load(); got != 1 {
		t.Errorf("INSERT exec calls = %d; want 1 (first-attempt 1062 not retried)", got)
	}
}

// TestColdCopyReparentRetry_Exhaustion pins: a persistent retriable error
// is retried up to the bound, then fails LOUDLY with a terminal error
// (not silent, not infinite). The terminal error wraps the underlying
// transient and does NOT satisfy ir.RetriableError.
func TestColdCopyReparentRetry_Exhaustion(t *testing.T) {
	withFastReparentBackoff(t, 4) // small bound for a fast test
	errs := make([]error, 100)
	for i := range errs {
		errs[i] = vttabletUnavailable()
	}
	script := &flushScript{execErrs: errs}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}
	table := pinReparentTable()

	err := w.WriteRows(context.Background(), table, feedReparentRows(2))
	if err == nil {
		t.Fatal("persistent transient must exhaust the budget LOUDLY; got nil")
	}
	if !strings.Contains(err.Error(), "reparent-retry window") {
		t.Errorf("exhaustion error should name the reparent-retry window; got %v", err)
	}
	// The terminal error must NOT be classified retriable (the stream/copy
	// gives up, it doesn't loop forever).
	var re ir.RetriableError
	if errors.As(err, &re) {
		t.Error("exhaustion error must be TERMINAL (not ir.RetriableError)")
	}
	// Underlying transient still reachable for diagnosis.
	var myErr *gomysql.MySQLError
	if !errors.As(err, &myErr) || myErr.Number != 1105 {
		t.Errorf("exhaustion error should Unwrap to the underlying transient 1105; got %v", err)
	}
	// Bounded: exactly `attempts` exec calls (1 first + (attempts-1)
	// retries), never infinite.
	if got := script.execCalls.Load(); got != 4 {
		t.Errorf("INSERT exec calls = %d; want 4 (bounded by the attempt cap)", got)
	}
}

// TestColdCopyReparentRetry_WallClockBound pins the v0.99.103 change: the
// REAL terminal bound is wall-clock, not attempt count. With a tiny
// max-wall and a high attempt backstop, a persistent transient retries
// MANY times (far past the old 24-attempt cap) but still terminates LOUDLY
// once the wall-clock deadline passes — proving a single batch rides a
// prolonged grow regardless of how fast the (gate-driven) probe cycles burn
// attempts.
func TestColdCopyReparentRetry_WallClockBound(t *testing.T) {
	origWall := coldCopyReparentMaxWallVar
	origAttempts := coldCopyReparentRetryAttemptsVar
	origBase := coldCopyReparentBackoffBaseVar
	origCap := coldCopyReparentBackoffCapVar
	coldCopyReparentMaxWallVar = 40 * time.Millisecond
	coldCopyReparentRetryAttemptsVar = 100000 // high backstop: wall-clock must be what fires
	coldCopyReparentBackoffBaseVar = time.Millisecond
	coldCopyReparentBackoffCapVar = time.Millisecond
	t.Cleanup(func() {
		coldCopyReparentMaxWallVar = origWall
		coldCopyReparentRetryAttemptsVar = origAttempts
		coldCopyReparentBackoffBaseVar = origBase
		coldCopyReparentBackoffCapVar = origCap
	})

	errs := make([]error, 100000)
	for i := range errs {
		errs[i] = vttabletUnavailable()
	}
	script := &flushScript{execErrs: errs}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}

	err := w.WriteRows(context.Background(), pinReparentTable(), feedReparentRows(2))
	if err == nil {
		t.Fatal("persistent transient must terminate LOUDLY on the wall-clock bound; got nil")
	}
	if !strings.Contains(err.Error(), "wall-clock") {
		t.Errorf("terminal error should name the wall-clock window; got %v", err)
	}
	var re ir.RetriableError
	if errors.As(err, &re) {
		t.Error("wall-clock terminal error must be TERMINAL (not ir.RetriableError)")
	}
	// The wall-clock bound — not the attempt count — is what fired: with a
	// ~1ms backoff and a 40ms window the loop ran well past the old 24-attempt
	// cap but FAR below the 100000 backstop, so it is bounded by time.
	got := script.execCalls.Load()
	if got <= 24 {
		t.Errorf("exec calls = %d; want > 24 (wall-clock bound rides past the old attempt cap)", got)
	}
	if got >= 100000 {
		t.Errorf("exec calls = %d; the attempt backstop fired instead of the wall-clock bound", got)
	}
}

// TestColdCopyReparentRetry_CtxCancelDuringBackoff pins prompt unwind:
// cancelling ctx while the loop is backing off returns ctx.Err() quickly
// rather than waiting out the backoff or the budget.
func TestColdCopyReparentRetry_CtxCancelDuringBackoff(t *testing.T) {
	// Make the backoff long so the cancel clearly pre-empts it.
	origBase := coldCopyReparentBackoffBaseVar
	origCap := coldCopyReparentBackoffCapVar
	coldCopyReparentBackoffBaseVar = 10 * time.Second
	coldCopyReparentBackoffCapVar = 10 * time.Second
	t.Cleanup(func() {
		coldCopyReparentBackoffBaseVar = origBase
		coldCopyReparentBackoffCapVar = origCap
	})

	errs := make([]error, 100)
	for i := range errs {
		errs[i] = vttabletUnavailable()
	}
	script := &flushScript{execErrs: errs}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}
	table := pinReparentTable()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	done := make(chan error, 1)
	go func() { done <- w.WriteRows(ctx, table, feedReparentRows(2)) }()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ctx-cancel during backoff should return context.Canceled; got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ctx-cancel during backoff did not unwind promptly (waited out the 10s backoff)")
	}
}
