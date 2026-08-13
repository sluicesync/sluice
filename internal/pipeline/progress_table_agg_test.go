// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/progress"
)

// The PROG-BAR-1 pins (real-user report 2026-08-13: "millions of rows,
// hovered around 0-1%, jumping from 20k to 40k"): every chunk ticker of
// one table shares a tableProgressAgg, and the presentation sink + ETA
// use its TABLE-LEVEL (rows, total) pair — pre-fix each chunk reported
// its own rows against the whole-table total (the 0–1%), and concurrent
// tickers clobbered the table-keyed bar (the jumping).
//
// Stated boundary: the fan-out wiring (withTableProgressAgg in
// migrate_parallel.go's chunk loop) is one line pinned here only via
// the context-resolution test — a dropped wiring degrades to the
// pre-fix per-chunk rendering, which no automated check catches; the
// pair SEMANTICS are what these pins hold.

// tableBarRecordingSink records TableProgress calls; every other Sink
// method is the embedded LogSink no-op.
type tableBarRecordingSink struct {
	progress.LogSink
	mu    sync.Mutex
	calls []struct{ done, total int64 }
}

func (s *tableBarRecordingSink) TableProgress(_ string, done, total int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, struct{ done, total int64 }{done, total})
}

func (s *tableBarRecordingSink) snapshot() []struct{ done, total int64 } {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]struct{ done, total int64 }(nil), s.calls...)
}

// TestChunkTickers_ShareTheTableAggregate pins the aggregation
// arithmetic: two chunk tickers resolved from one agg-carrying context
// sum rows (inc AND the resume seed) into one pair, the whole-table
// count lands once on the aggregate total, and a single-reader ticker
// without the aggregate keeps its own counters.
func TestChunkTickers_ShareTheTableAggregate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	agg := &tableProgressAgg{}
	actx := withTableProgressAgg(ctx, agg)

	a := newProgressTickerForChunk(actx, time.Hour, "t", 0)
	defer a.Stop(ctx, nil)
	b := newProgressTickerForChunk(actx, time.Hour, "t", 1)
	defer b.Stop(ctx, nil)

	a.inc()
	a.inc()
	a.inc()
	b.primeRows(10) // chunk 1 resumes with 10 rows already copied
	b.inc()
	a.setTotalRows(100) // the whole-table count, via any chunk's ticker

	if rows, total := a.tableLevelPair(a.rows.Load(), a.totalRows.Load()); rows != 14 || total != 100 {
		t.Fatalf("chunk 0 table-level pair = (%d, %d); want (14, 100) — 3+1 incs + the 10-row resume seed", rows, total)
	}
	if rows, total := b.tableLevelPair(b.rows.Load(), b.totalRows.Load()); rows != 14 || total != 100 {
		t.Fatalf("chunk 1 table-level pair = (%d, %d); want (14, 100) — both tickers must render ONE pair", rows, total)
	}

	// Single-reader path: no aggregate in the context → own counters.
	solo := newProgressTicker(ctx, time.Hour, "t2")
	defer solo.Stop(ctx, nil)
	solo.inc()
	solo.setTotalRows(7)
	if rows, total := solo.tableLevelPair(solo.rows.Load(), solo.totalRows.Load()); rows != 1 || total != 7 {
		t.Fatalf("single-reader pair = (%d, %d); want the ticker's own (1, 7)", rows, total)
	}
}

// TestChunkTicker_SinkSeesTheAggregatePair drives a real ticker loop
// and asserts the presentation sink receives the AGGREGATE pair — made
// distinguishable from the chunk-local one by advancing a sibling
// ticker's rows first. Pre-fix this recorded (1, 100) — the 0–1% bar.
func TestChunkTicker_SinkSeesTheAggregatePair(t *testing.T) {
	sink := &tableBarRecordingSink{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	actx := withTableProgressAgg(progress.NewContext(ctx, sink), &tableProgressAgg{})

	sibling := newProgressTickerForChunk(actx, time.Hour, "t", 1)
	defer sibling.Stop(ctx, nil)
	for i := 0; i < 40; i++ {
		sibling.inc()
	}

	pt := newProgressTickerForChunk(actx, 5*time.Millisecond, "t", 0)
	defer pt.Stop(ctx, nil)
	pt.setTotalRows(100)
	pt.inc()

	deadline := time.Now().Add(5 * time.Second)
	for {
		if calls := sink.snapshot(); len(calls) > 0 {
			got := calls[len(calls)-1]
			if got.done != 41 || got.total != 100 {
				t.Fatalf("sink saw (%d, %d); want the table-level (41, 100), not chunk 0's own single row", got.done, got.total)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("ticker never reported to the sink")
		}
		time.Sleep(2 * time.Millisecond)
	}
}
