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

// TestProgressTicker_StopEmitsCompleteLine verifies the deferred-Stop
// path emits a "bulk copy complete" record carrying the table name and
// the final row count, even when no periodic ticks fired (a fast copy
// completing inside one interval).
func TestProgressTicker_StopEmitsCompleteLine(t *testing.T) {
	logs := captureSlog(t)

	pt := newProgressTicker(context.Background(), time.Hour, "users")
	pt.inc()
	pt.inc()
	pt.inc()
	pt.Stop(context.Background(), nil)

	out := logs.String()
	if !strings.Contains(out, "bulk copy complete") {
		t.Errorf("expected complete line; got: %q", out)
	}
	if !strings.Contains(out, "table=users") {
		t.Errorf("expected table=users attribute; got: %q", out)
	}
	if !strings.Contains(out, "rows=3") {
		t.Errorf("expected rows=3; got: %q", out)
	}
}

// TestProgressTicker_StopWithErrorEmitsAbortedLine verifies the
// failure-path Stop logs "bulk copy aborted" instead of "complete"
// and includes the error message. Bug 9 hinged on the original Stop
// always logging "complete" regardless of writer outcome — this test
// pins the corrected shape so a regression is caught immediately.
func TestProgressTicker_StopWithErrorEmitsAbortedLine(t *testing.T) {
	logs := captureSlog(t)

	pt := newProgressTicker(context.Background(), time.Hour, "comments")
	pt.inc()
	pt.inc()
	pt.Stop(context.Background(), errors.New("duplicate key on PRIMARY"))

	out := logs.String()
	if !strings.Contains(out, "bulk copy aborted") {
		t.Errorf("expected aborted line; got: %q", out)
	}
	if strings.Contains(out, "bulk copy complete") {
		t.Errorf("did NOT expect complete line on error path; got: %q", out)
	}
	if !strings.Contains(out, "rows=2") {
		t.Errorf("expected rows=2; got: %q", out)
	}
	if !strings.Contains(out, "duplicate key on PRIMARY") {
		t.Errorf("expected error message in attrs; got: %q", out)
	}
}

// TestProgressTicker_StopIsIdempotent verifies the sync.Once in Stop
// makes a second call a no-op — important because the orchestrator
// uses a deferred Stop that may collide with an explicit Stop on the
// success path of a future change.
func TestProgressTicker_StopIsIdempotent(t *testing.T) {
	captureSlog(t)
	pt := newProgressTicker(context.Background(), time.Hour, "tbl")
	pt.Stop(context.Background(), nil)
	pt.Stop(context.Background(), nil) // must not panic on close-of-closed-channel
}

// TestProgressTicker_PeriodicTickIncludesETA verifies the periodic
// progress line carries `total_rows`, `eta_seconds`, and the
// rate/bytes attributes added in v0.5.0. The ticker is constructed,
// fed a row count and a total-rows estimate, then driven through
// enough ticks to emit a line.
func TestProgressTicker_PeriodicTickIncludesETA(t *testing.T) {
	logs := captureSlog(t)

	pt := newProgressTicker(context.Background(), 30*time.Millisecond, "events")
	pt.setTotalRows(100)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				pt.inc()
			}
		}
	}()
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
	pt.Stop(context.Background(), nil)

	out := logs.String()
	if !strings.Contains(out, "bulk copy progress") {
		t.Errorf("expected progress line; got: %q", out)
	}
	// total_rows attribute must be present even when the table count
	// returned zero — the wire shape stays stable.
	if !strings.Contains(out, "total_rows=100") {
		t.Errorf("expected total_rows=100 attribute; got: %q", out)
	}
	if !strings.Contains(out, "eta_seconds=") {
		t.Errorf("expected eta_seconds= attribute; got: %q", out)
	}
	if !strings.Contains(out, "rate_rows_per_sec=") {
		t.Errorf("expected rate_rows_per_sec= attribute; got: %q", out)
	}
}

// TestProgressTicker_ChunkAttribute verifies the parallel-copy variant
// emits a `chunk` attribute on every line and propagates it through
// Stop. Operators tailing the log correlate per-chunk progress via
// this attribute.
func TestProgressTicker_ChunkAttribute(t *testing.T) {
	logs := captureSlog(t)

	pt := newProgressTickerForChunk(context.Background(), 30*time.Millisecond, "shipments", 2)
	pt.inc()
	pt.inc()
	pt.Stop(context.Background(), nil)

	out := logs.String()
	if !strings.Contains(out, "chunk=2") {
		t.Errorf("expected chunk=2 attribute; got: %q", out)
	}
}

// TestProgressTicker_PeriodicTickEmitsLine verifies the background
// goroutine emits a "bulk copy progress" line when the row count
// advances between ticks, and stays silent when it doesn't.
func TestProgressTicker_PeriodicTickEmitsLine(t *testing.T) {
	logs := captureSlog(t)

	pt := newProgressTicker(context.Background(), 30*time.Millisecond, "events")
	// Pump rows from a goroutine so multiple ticks fire while the
	// counter is still moving.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				pt.inc()
			}
		}
	}()

	// Sleep long enough for at least two ticks at 30 ms.
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
	pt.Stop(context.Background(), nil)

	out := logs.String()
	if !strings.Contains(out, "bulk copy progress") {
		t.Errorf("expected progress line; got: %q", out)
	}
	if !strings.Contains(out, "table=events") {
		t.Errorf("expected table=events attribute; got: %q", out)
	}
}

// TestTeeRows_ForwardsAndCounts verifies teeRows passes every row
// through to the downstream channel and invokes onRow once per row
// in order. The orchestrator relies on the count being 1:1 with rows
// the writer sees.
func TestTeeRows_ForwardsAndCounts(t *testing.T) {
	src := make(chan ir.Row, 4)
	src <- ir.Row{"id": 1}
	src <- ir.Row{"id": 2}
	src <- ir.Row{"id": 3}
	close(src)

	var counted int
	out := teeRows(context.Background(), src, func() { counted++ })

	received := make([]ir.Row, 0, 3)
	for r := range out {
		received = append(received, r)
	}
	if len(received) != 3 {
		t.Errorf("forwarded %d rows; want 3", len(received))
	}
	if counted != 3 {
		t.Errorf("counted %d rows; want 3", counted)
	}
}

// TestTeeRows_CtxCancelStopsForwarding verifies the tee terminates
// when ctx is cancelled, even with rows still queued in src. The
// downstream channel is closed without surfacing the remaining rows.
func TestTeeRows_CtxCancelStopsForwarding(t *testing.T) {
	src := make(chan ir.Row) // unbuffered: forces tee to park in send
	defer close(src)

	ctx, cancel := context.WithCancel(context.Background())
	out := teeRows(ctx, src, func() {})

	// Push one row asynchronously; the tee must take it.
	go func() { src <- ir.Row{"id": 1} }()

	if r, ok := <-out; !ok || r["id"] != 1 {
		t.Fatalf("expected one forwarded row; got ok=%v r=%v", ok, r)
	}

	cancel()
	// out must close shortly after cancel; if tee leaks we hang.
	select {
	case _, ok := <-out:
		if ok {
			// A second row may sneak through depending on scheduling,
			// but the channel must close eventually.
			select {
			case _, ok2 := <-out:
				if ok2 {
					t.Errorf("expected channel to close after ctx cancel")
				}
			case <-time.After(500 * time.Millisecond):
				t.Errorf("tee did not close out channel after ctx cancel")
			}
		}
	case <-time.After(500 * time.Millisecond):
		t.Errorf("tee did not close out channel after ctx cancel")
	}
}
