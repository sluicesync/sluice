// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// pumpReader emits rows on a goroutine that selects on ctx.Done(). It
// records when the goroutine exits so the test can assert the reader
// unwinds when copyTable cancels its child context. Mirrors the shape
// of the real Postgres row reader (which holds a snapshot tx until
// its goroutine exits — Bug 9 was that this never happened on the
// writer-error path).
type pumpReader struct {
	rowsToSend int
	exited     chan struct{} // closed when the streaming goroutine returns
	exitOnce   sync.Once

	// streamErr, when non-nil, simulates a mid-stream scan/decode
	// failure: the reader closes its channel after rowsToSend rows
	// (exactly as the real PG/MySQL readers do on a decode error —
	// they setErr and return, closing the channel) and surfaces the
	// error only via Err. Bug 68: the orchestrator MUST observe this
	// and fail the migrate rather than treat the early channel-close
	// as "table fully read".
	streamErr error
}

func newPumpReader(rowsToSend int) *pumpReader {
	return &pumpReader{rowsToSend: rowsToSend, exited: make(chan struct{})}
}

func (p *pumpReader) ReadRows(ctx context.Context, _ *ir.Table) (<-chan ir.Row, error) {
	out := make(chan ir.Row)
	go func() {
		defer close(out)
		defer p.exitOnce.Do(func() { close(p.exited) })
		for i := 0; i < p.rowsToSend; i++ {
			row := ir.Row{"id": int64(i)}
			select {
			case <-ctx.Done():
				return
			case out <- row:
			}
		}
	}()
	return out, nil
}

func (p *pumpReader) Err() error { return p.streamErr }

// erroringWriter consumes a fixed number of rows then returns an
// error without draining the rest. Mirrors the shape of MySQL's
// writeBatched returning a duplicate-key error mid-flush — Bug 9's
// trigger.
type erroringWriter struct {
	consume int
	err     error
}

func (w *erroringWriter) WriteRows(_ context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	taken := 0
	for range rows {
		taken++
		if taken >= w.consume {
			return w.err
		}
	}
	return nil
}

// TestCopyTable_CancelsReaderOnWriterError pins the Bug 9 fix: when
// WriteRows returns an error, copyTable must cancel the child ctx so
// the reader's streaming goroutine unwinds. Before the fix, the
// reader blocked indefinitely on `out <- row` (the consumer was
// gone), holding its source-side transaction open and producing the
// "idle in transaction" symptom in the bug report.
func TestCopyTable_CancelsReaderOnWriterError(t *testing.T) {
	captureSlog(t)
	rr := newPumpReader(10000) // way more than the writer will consume
	rw := &erroringWriter{consume: 5, err: errors.New("duplicate key on PRIMARY")}
	table := &ir.Table{Name: "comments", Columns: []*ir.Column{{Name: "id"}}}

	ctx := context.Background()
	err := copyTable(ctx, rr, rw, table, nil, ShardColumnSpec{})
	if err == nil {
		t.Fatal("expected copyTable to surface the writer error; got nil")
	}
	if !strings.Contains(err.Error(), "duplicate key") {
		t.Errorf("expected wrapped writer error; got %v", err)
	}

	// The reader's goroutine must exit promptly. Without the fix it
	// would block forever on `out <- row`. Allow a generous deadline
	// so a slow CI box doesn't false-fail.
	select {
	case <-rr.exited:
	case <-time.After(2 * time.Second):
		t.Fatal("reader goroutine did not exit after copyTable returned an error")
	}
}

// TestCopyTable_LogsAbortedNotComplete verifies the failure-path
// summary log says "bulk copy aborted", not "bulk copy complete" — Bug
// 9's misleading log line was the operator-facing red flag that this
// fix removes.
func TestCopyTable_LogsAbortedNotComplete(t *testing.T) {
	logs := captureSlog(t)
	rr := newPumpReader(100)
	rw := &erroringWriter{consume: 5, err: errors.New("collision")}
	table := &ir.Table{Name: "comments", Columns: []*ir.Column{{Name: "id"}}}

	if err := copyTable(context.Background(), rr, rw, table, nil, ShardColumnSpec{}); err == nil {
		t.Fatal("expected error; got nil")
	}

	out := logs.String()
	if !strings.Contains(out, "bulk copy aborted") {
		t.Errorf("expected aborted line; got %q", out)
	}
	if strings.Contains(out, "bulk copy complete") {
		t.Errorf("expected NO complete line on error path; got %q", out)
	}
}

// TestCopyTable_SuccessLogsComplete confirms the happy-path log line
// is unchanged: rows are forwarded, writer succeeds, summary reads
// "bulk copy complete".
func TestCopyTable_SuccessLogsComplete(t *testing.T) {
	logs := captureSlog(t)
	rr := newPumpReader(3)
	// drainingWriter that drains the channel cleanly.
	rw := &drainingWriter{}
	table := &ir.Table{Name: "users", Columns: []*ir.Column{{Name: "id"}}}

	if err := copyTable(context.Background(), rr, rw, table, nil, ShardColumnSpec{}); err != nil {
		t.Fatalf("copyTable: %v", err)
	}

	out := logs.String()
	if !strings.Contains(out, "bulk copy complete") {
		t.Errorf("expected complete line; got %q", out)
	}
	if strings.Contains(out, "bulk copy aborted") {
		t.Errorf("did NOT expect aborted line on success path; got %q", out)
	}
	if !strings.Contains(out, "rows=3") {
		t.Errorf("expected rows=3; got %q", out)
	}
}

// TestCopyTable_SurfacesReaderStreamError pins the Bug 68 silent-
// swallow elimination, independent of multi-dim arrays. The reader
// emits some rows then closes its channel and reports a non-nil
// Err() — exactly the shape the real PG/MySQL readers produce when a
// per-row scan/decode fails mid-stream (setErr + return, channel
// closes). The writer drains cleanly and returns nil. Before the
// fix, copyTable returned nil here and the migrate exited 0 with a
// silently-truncated table. After the fix, copyTable MUST surface
// the reader's sticky error as a hard failure.
func TestCopyTable_SurfacesReaderStreamError(t *testing.T) {
	captureSlog(t)
	rr := newPumpReader(2)
	rr.streamErr = errors.New("postgres: column \"matrix\": postgres: array text parse: nested arrays not supported")
	rw := &drainingWriter{} // drains everything, returns nil — the dangerous case
	table := &ir.Table{Name: "md", Columns: []*ir.Column{{Name: "id"}}}

	err := copyTable(context.Background(), rr, rw, table, nil, ShardColumnSpec{})
	if err == nil {
		t.Fatal("Bug 68: copyTable returned nil despite a mid-stream reader error; this is the silent total-row-loss class")
	}
	if !strings.Contains(err.Error(), "nested arrays not supported") {
		t.Errorf("expected the wrapped reader stream error; got %v", err)
	}
	if !strings.Contains(err.Error(), `table "md"`) {
		t.Errorf("expected the failing table name in the message; got %v", err)
	}
}

type drainingWriter struct{}

func (drainingWriter) WriteRows(_ context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	for range rows {
	}
	return nil
}

// idempotentCountingWriter records which write path the orchestrator
// chose. It implements ir.IdempotentRowWriter so copyTableColdStart-
// Idempotent can route to it; plainCalls / idemCalls distinguish the
// two paths for the Bug 125 routing pin.
type idempotentCountingWriter struct {
	plainCalls int
	idemCalls  int
}

func (w *idempotentCountingWriter) WriteRows(_ context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	w.plainCalls++
	for range rows {
	}
	return nil
}

func (w *idempotentCountingWriter) WriteRowsIdempotent(_ context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	w.idemCalls++
	for range rows {
	}
	return nil
}

// TestCopyTableColdStartIdempotent_RoutesThroughUpsert pins Bug 125's
// orchestration half: copyTableColdStartIdempotent must use the
// writer's idempotent (upsert) path, NOT plain WriteRows.
func TestCopyTableColdStartIdempotent_RoutesThroughUpsert(t *testing.T) {
	rr := newPumpReader(50)
	rw := &idempotentCountingWriter{}
	table := &ir.Table{Name: "connections", Columns: []*ir.Column{{Name: "id"}}}

	if err := copyTableColdStartIdempotent(context.Background(), rr, rw, table, nil, ShardColumnSpec{}); err != nil {
		t.Fatalf("copyTableColdStartIdempotent: %v", err)
	}
	if rw.idemCalls != 1 || rw.plainCalls != 0 {
		t.Errorf("idemCalls=%d plainCalls=%d; want idem=1 plain=0 (Bug 125 must upsert)", rw.idemCalls, rw.plainCalls)
	}
}

// TestCopyTableColdStartIdempotent_RefusesNonIdempotentWriter pins the
// loud refusal: when the reader declares its rows need idempotent
// writes (VStream COPY re-emits, Bug 125) but the target writer can't
// upsert, the orchestrator must refuse rather than silently fall back
// to plain INSERT (which would re-introduce the duplicate-key collision).
func TestCopyTableColdStartIdempotent_RefusesNonIdempotentWriter(t *testing.T) {
	rr := newPumpReader(10)
	rw := drainingWriter{} // implements only WriteRows
	table := &ir.Table{Name: "connections", Columns: []*ir.Column{{Name: "id"}}}

	err := copyTableColdStartIdempotent(context.Background(), rr, rw, table, nil, ShardColumnSpec{})
	if err == nil {
		t.Fatal("expected refusal when target writer is not idempotent; got nil")
	}
	if !strings.Contains(err.Error(), "connections") || !strings.Contains(err.Error(), "Bug 125") {
		t.Errorf("error %q; want it to name the table and Bug 125", err.Error())
	}
}
