// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// Roadmap item 139 pins — connection ACQUISITION rides the transient
// classifier.
//
// These drive the REAL write cores (WriteRows / WriteRowsIdempotent /
// WriteRowsParallel / the LOAD DATA core) through the scriptable fake
// driver in row_writer_reparent_retry_test.go, with the failures scripted
// on driver.Open rather than on the statement — so what is under test is
// the acquire, on every core that performs one.
//
// The failure they pin is the FIELD REPORT's, verbatim: the run absorbed
// 78 of this exact error at the flush and then died on the same error at
// the acquire.

// vtgateAcquireDrop is the field report's acquire-time error (2026-08-05,
// run 2) — the same wording item 122 taught the classifier, arriving one
// line earlier than the flush that already absorbed 78 of them.
func vtgateAcquireDrop() error {
	return &gomysql.MySQLError{
		Number:  1105,
		Message: "internal: vtgate connection error: read tcp 10.0.0.5:15999: read: connection reset by peer",
	}
}

// accessDenied is a TERMINAL acquire failure — a misconfiguration, not a
// transient. It must be returned unchanged and NOT retried, or a wrong
// password would take 30 minutes to report.
func accessDenied() error {
	return &gomysql.MySQLError{Number: 1045, Message: "Access denied for user 'nope'@'%' (using password: YES)"}
}

// TestColdCopyConnAcquire_PlainConverges: the plain batched core's pinned
// acquire eats two vtgate drops and then succeeds. Pre-item-139 the first
// drop returned raw as `pin connection: …` and killed the run.
func TestColdCopyConnAcquire_PlainConverges(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{openErrs: []error{vtgateAcquireDrop(), vtgateAcquireDrop()}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}

	if err := w.WriteRows(context.Background(), pinReparentTable(), feedReparentRows(2)); err != nil {
		t.Fatalf("WriteRows: unexpected error: %v", err)
	}
	if got := script.opens.Load(); got < 3 {
		t.Errorf("driver Open calls = %d; want >= 3 (2 refused acquires + 1 success)", got)
	}
}

// TestColdCopyConnAcquire_IdempotentConverges: the upsert core's acquire.
// The two batched cores are separate call sites, and pinning one says
// nothing about the other.
func TestColdCopyConnAcquire_IdempotentConverges(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{openErrs: []error{vtgateAcquireDrop()}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}

	if err := w.WriteRowsIdempotent(context.Background(), pinReparentTable(), feedReparentRows(2)); err != nil {
		t.Fatalf("WriteRowsIdempotent: unexpected error: %v", err)
	}
	if got := script.opens.Load(); got < 2 {
		t.Errorf("driver Open calls = %d; want >= 2 (1 refused acquire + 1 success)", got)
	}
}

// TestColdCopyConnAcquire_LoadDataConverges: item 114's LOAD DATA core is
// the third bulk-write core and pins its own connection for the
// session-scoped warning probe. Its acquire is the roadmap filing's
// load_data_writer.go site.
func TestColdCopyConnAcquire_LoadDataConverges(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{openErrs: []error{vtgateAcquireDrop()}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadLoadDataInfile}

	if err := w.WriteRows(context.Background(), pinReparentTable(), feedReparentRows(2)); err != nil {
		t.Fatalf("WriteRows (LOAD DATA): unexpected error: %v", err)
	}
	if got := script.opens.Load(); got < 2 {
		t.Errorf("driver Open calls = %d; want >= 2 (1 refused acquire + 1 success)", got)
	}
}

// TestColdCopyConnAcquire_FanOutWorkersConverge: the two fan-out lanes
// acquire ONCE PER WORKER GOROUTINE, and those two sites were missing
// from the roadmap filing's hand-written roster. Two workers, two drops:
// both must be absorbed, or a single drop kills a fan-out copy.
func TestColdCopyConnAcquire_FanOutWorkersConverge(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{openErrs: []error{vtgateAcquireDrop(), vtgateAcquireDrop()}}
	db := newScriptDB(t, script)
	// MaxIdleConns=0 keeps every acquire a fresh driver.Open, so the
	// scripted refusals reach both workers instead of one of them being
	// handed a pooled conn.
	db.SetMaxIdleConns(0)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}

	if err := w.WriteRowsParallel(context.Background(), pinReparentTable(),
		[]<-chan ir.Row{feedReparentRows(2), feedReparentRows(2)}); err != nil {
		t.Fatalf("WriteRowsParallel: unexpected error: %v", err)
	}
	if got := script.opens.Load(); got < 4 {
		t.Errorf("driver Open calls = %d; want >= 4 (2 refused acquires + 2 successes)", got)
	}
}

// TestColdCopyConnAcquire_IdempotentFanOutWorkersConverge is the same pin
// on the upsert fan-out — the sibling of the lane above, which is exactly
// the pair a per-representative pin would have covered by half.
func TestColdCopyConnAcquire_IdempotentFanOutWorkersConverge(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{openErrs: []error{vtgateAcquireDrop(), vtgateAcquireDrop()}}
	db := newScriptDB(t, script)
	db.SetMaxIdleConns(0)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}

	if err := w.WriteRowsIdempotentParallel(context.Background(), pinReparentTable(),
		[]<-chan ir.Row{feedReparentRows(2), feedReparentRows(2)}); err != nil {
		t.Fatalf("WriteRowsIdempotentParallel: unexpected error: %v", err)
	}
	if got := script.opens.Load(); got < 4 {
		t.Errorf("driver Open calls = %d; want >= 4 (2 refused acquires + 2 successes)", got)
	}
}

// TestColdCopyConnAcquire_TerminalIsNotRetried: a wrong password is not a
// reparent. It must surface on the FIRST attempt with the unchanged
// `pin connection` wording — the retry must not turn a config error into
// a 30-minute stall.
func TestColdCopyConnAcquire_TerminalIsNotRetried(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{openErrs: []error{accessDenied(), accessDenied(), accessDenied()}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}

	err := w.WriteRows(context.Background(), pinReparentTable(), feedReparentRows(2))
	if err == nil {
		t.Fatal("WriteRows: want a terminal error, got nil")
	}
	if !strings.Contains(err.Error(), "pin connection") || !strings.Contains(err.Error(), "Access denied") {
		t.Errorf("terminal acquire error lost its wording: %v", err)
	}
	if got := script.opens.Load(); got != 1 {
		t.Errorf("driver Open calls = %d; want exactly 1 (a terminal acquire error must not be retried)", got)
	}
}

// TestColdCopyConnAcquire_ExhaustionIsLoud: a target that never hands out
// a connection must fail LOUDLY, naming the table, rather than retrying
// forever.
func TestColdCopyConnAcquire_ExhaustionIsLoud(t *testing.T) {
	withFastReparentBackoff(t, 3)
	script := &flushScript{openErrs: []error{
		vtgateAcquireDrop(), vtgateAcquireDrop(), vtgateAcquireDrop(), vtgateAcquireDrop(), vtgateAcquireDrop(),
	}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}

	err := w.WriteRows(context.Background(), pinReparentTable(), feedReparentRows(2))
	if err == nil {
		t.Fatal("WriteRows: want exhaustion error, got nil")
	}
	for _, want := range []string{"t_pin", "still could not acquire a connection", "vtgate connection error"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("exhaustion error missing %q: %v", want, err)
		}
	}
}

// TestColdCopyConnAcquire_CtxCancelIsPrompt: cancelling during the
// acquire backoff unwinds promptly rather than sleeping out the ladder.
func TestColdCopyConnAcquire_CtxCancelIsPrompt(t *testing.T) {
	origBase := coldCopyReparentBackoffBaseVar
	origCap := coldCopyReparentBackoffCapVar
	coldCopyReparentBackoffBaseVar = 10 * time.Second
	coldCopyReparentBackoffCapVar = 10 * time.Second
	t.Cleanup(func() {
		coldCopyReparentBackoffBaseVar = origBase
		coldCopyReparentBackoffCapVar = origCap
	})

	script := &flushScript{openErrs: []error{vtgateAcquireDrop(), vtgateAcquireDrop()}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	err := w.WriteRows(ctx, pinReparentTable(), feedReparentRows(2))
	if err == nil {
		t.Fatal("WriteRows: want a cancellation error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("want context.Canceled, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("cancel took %s; the acquire backoff is not selecting on ctx.Done()", elapsed)
	}
}

// TestColdCopyConnAcquire_KeylessTableIsStillRetried is the pin for the
// ONE place the acquire loop deliberately DIVERGES from the flush loop.
//
// The flush refuses to replay a batch on a table with no PRIMARY KEY and
// no NOT NULL UNIQUE index (audit B-9 / errKeylessAmbiguousReplay),
// because a committed-but-unacked attempt is indistinguishable from a
// rolled-back one. An ACQUIRE writes nothing, so there is no prior
// attempt to be ambiguous about — importing the carve-out here would
// refuse a keyless table's copy for a hazard it cannot have. This pins
// that the acquire converges on exactly such a table.
func TestColdCopyConnAcquire_KeylessTableIsStillRetried(t *testing.T) {
	withFastReparentBackoff(t, 12)
	script := &flushScript{openErrs: []error{vtgateAcquireDrop()}}
	db := newScriptDB(t, script)
	w := &RowWriter{db: db, bulkLoad: ir.BulkLoadBatchedInsert}

	keyless := &ir.Table{
		Name: "t_keyless",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 8}, Nullable: true},
			{Name: "v", Type: ir.Text{}, Nullable: true},
		},
	}
	if err := w.WriteRows(context.Background(), keyless, feedReparentRows(2)); err != nil {
		t.Fatalf("WriteRows on a keyless table: acquire must still be retried: %v", err)
	}
	if got := script.opens.Load(); got < 2 {
		t.Errorf("driver Open calls = %d; want >= 2 (the acquire retry is replay-safe by construction)", got)
	}
}
