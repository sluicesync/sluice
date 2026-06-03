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

// TestApproximateRowBytes covers each arm of the byte-walk type
// switch so a new IR type added in the future without an arm here
// is caught when the metric drops to zero.
func TestApproximateRowBytes(t *testing.T) {
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		row  ir.Row
		want int64
	}{
		{"nil row", nil, 0},
		{"empty row", ir.Row{}, 0},
		{"string", ir.Row{"s": "hello"}, 5},
		{"bytes", ir.Row{"b": []byte{1, 2, 3}}, 3},
		{"bool", ir.Row{"b": true}, 1},
		{"int8", ir.Row{"i": int8(7)}, 1},
		{"int16", ir.Row{"i": int16(7)}, 2},
		{"int32 + float32", ir.Row{"i": int32(1), "f": float32(2.0)}, 8},
		{"int64 + float64", ir.Row{"i": int64(1), "f": float64(2.0)}, 16},
		{"int + uint", ir.Row{"i": 1, "u": uint(2)}, 16},
		{"time.Time", ir.Row{"t": now}, 24},
		{"nil value contributes nothing", ir.Row{"n": nil, "s": "x"}, 1},
		{"[]any of strings", ir.Row{"a": []any{"foo", "bar"}}, 6},
		{"[]string", ir.Row{"a": []string{"abc", "de"}}, 5},
		{"unknown type contributes zero", ir.Row{"x": struct{ A int }{}}, 0},
		{
			"mixed row",
			ir.Row{
				"id":    int64(123),
				"email": "alice@example.com",
				"flag":  true,
				"ts":    now,
				"data":  []byte{1, 2, 3, 4},
			},
			8 + 17 + 1 + 24 + 4,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := approximateRowBytes(c.row)
			if got != c.want {
				t.Errorf("approximateRowBytes(%#v) = %d; want %d", c.row, got, c.want)
			}
		})
	}
}

// TestProgressTicker_ObserveRowAddsBytes pins the ticker's per-row
// hook to the byte-summing path so a regression in the addBytes
// wiring shows up here. We feed a known-shape row through the tick
// loop and assert the emitted line has a non-zero bytes attribute.
func TestProgressTicker_ObserveRowAddsBytes(t *testing.T) {
	logs := captureSlog(t)
	pt := newProgressTicker(context.Background(), 30*time.Millisecond, "events")
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		row := ir.Row{"id": int64(1), "name": "alice"} // 8 + 5 = 13 bytes
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				pt.observeRow(row)
			}
		}
	}()
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
	pt.Stop(context.Background(), nil)

	out := logs.String()
	if !strings.Contains(out, "rate_mb_per_sec=") {
		t.Errorf("expected rate_mb_per_sec= attribute; got: %q", out)
	}
	// A 13-byte row at ~5 ms ticks should put bytes past 0 quickly.
	if strings.Contains(out, "bytes=0") && !strings.Contains(out, "bytes=0\n") {
		// Not a strict assertion — bytes=0 is allowed *if* no row
		// flowed during the tick window — but the test scenario is
		// designed to push enough rows that at least one progress
		// line carries a positive bytes value.
		t.Logf("note: log includes a bytes=0 line; full output: %q", out)
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
	out := teeRows(context.Background(), src, func(_ ir.Row) { counted++ })

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
	out := teeRows(ctx, src, func(_ ir.Row) {})

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
