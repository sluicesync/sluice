package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
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
	err := copyTable(ctx, rr, rw, table)
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

	if err := copyTable(context.Background(), rr, rw, table); err == nil {
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

	if err := copyTable(context.Background(), rr, rw, table); err != nil {
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

type drainingWriter struct{}

func (drainingWriter) WriteRows(_ context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	for range rows {
	}
	return nil
}
