package pipeline

// Bulk-copy progress reporting for the simple-mode orchestrator and
// the streamer's cold-start branch.
//
// A bulk copy of a large table can take tens of minutes. Without a
// periodic signal the operator can't tell whether the migration is
// progressing or hung — process inspection only shows two open
// database connections that are both, technically, busy.
//
// The progressTicker addresses this by sitting in the row pipe between
// [ir.RowReader] and [ir.RowWriter]: the orchestrator increments an
// atomic counter for every row that flows through, and a goroutine
// emits a structured slog line every interval until [progressTicker.Stop]
// is called.
//
// v0.5.0 extends the ticker with throughput metrics — bytes/sec and
// ETA — that pair naturally with the parallel within-table copy. The
// rate-and-ETA math is most meaningful when there's parallel work to
// measure; on a single-reader copy the existing rows-per-second line
// remains accurate. Bytes are observed best-effort by the engine via
// the optional [byteCounter] hook (engines that don't track scan-buf
// sizes pass 0 and the ETA falls back to row-rate alone).
//
// The counting happens at the pipeline layer rather than inside engine
// implementations to keep engines simple and uniform — every engine
// reports progress the same way without having to plumb a callback
// through its bulk-load fast path. The trade-off is that COPY-protocol
// writes (Postgres) batch internally; the row count emitted here is the
// number the orchestrator has *handed to* the writer, not the number
// the writer has fsynced. For long-running migrations the difference is
// in the noise.

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// progressTicker counts rows passing through the bulk-copy pipe and
// emits a periodic slog line summarising progress. One ticker per
// table; call [progressTicker.Stop] when the table's copy completes
// (or fails) to flush a final summary line and stop the goroutine.
//
// The summary line's verbiage depends on the err passed to Stop:
// nil means "bulk copy complete" (success); non-nil means "bulk
// copy aborted" (the writer or context errored mid-stream). Bug 9
// uncovered that the previous "always log complete" shape was
// actively misleading — a duplicate-key error mid-stream would log
// "complete table=comments rows=501" while the reader's goroutine
// silently leaked, holding a snapshot tx open. The status-aware Stop
// makes the failure visible in the operator's tail -f.
type progressTicker struct {
	rows     atomic.Int64
	bytes    atomic.Int64
	table    string
	chunk    int  // 0..N-1 on parallel runs; -1 on the single-reader path
	hasChunk bool // distinguishes "chunk 0 of a parallel run" from "single-reader"
	interval time.Duration
	logger   *slog.Logger

	// total is the source-side row-count estimate for ETA calculation.
	// Set via setTotalRows once the async COUNT(*) query (or
	// pg_class.reltuples estimate) returns. Zero means "not yet
	// available"; the ETA stays unknown in that case.
	totalRows atomic.Int64

	// startedAt is the wall-clock time the first tick fires after
	// rows started flowing. Used as the denominator for the
	// running-average rate that drives ETA — instantaneous rate is
	// noisy at small sample sizes.
	startedAt atomic.Pointer[time.Time]

	stopOnce sync.Once
	done     chan struct{}
	wg       sync.WaitGroup
}

// newProgressTicker starts a goroutine that emits a slog.Info line
// every interval until Stop is called. The logger is captured from
// slog.Default at construction time; pass ctx so the lines carry any
// trace/request attributes the caller has attached via a slog handler.
func newProgressTicker(ctx context.Context, interval time.Duration, table string) *progressTicker {
	p := &progressTicker{
		table:    table,
		chunk:    -1,
		interval: interval,
		logger:   slog.Default(),
		done:     make(chan struct{}),
	}
	p.wg.Add(1)
	go p.loop(ctx)
	return p
}

// newProgressTickerForChunk is the parallel-copy variant. The emitted
// log lines include a `chunk` attribute so operators tailing the log
// can correlate per-chunk progress; the structured shape stays
// identical otherwise so log aggregators don't need a special case.
func newProgressTickerForChunk(ctx context.Context, interval time.Duration, table string, chunk int) *progressTicker {
	p := &progressTicker{
		table:    table,
		chunk:    chunk,
		hasChunk: true,
		interval: interval,
		logger:   slog.Default(),
		done:     make(chan struct{}),
	}
	p.wg.Add(1)
	go p.loop(ctx)
	return p
}

// inc is the per-row hook the row pipe calls. Cheap (a single atomic
// add) so the orchestrator can call it on every row without measurable
// overhead.
func (p *progressTicker) inc() {
	p.rows.Add(1)
}

// addBytes records bytes seen for this row. Optional — engines that
// don't track scan-buffer sizes pass zero and the ticker reports
// rate without bytes-per-second. Cheap (atomic add).
//
// v0.5.0 wires the hook from the pipeline tee point via
// [observeRow], where the row content is available to every engine
// without any per-engine plumbing. The hook stays a method on the
// ticker so future engines that have a tighter byte count (e.g.
// COPY-protocol writers that know exact wire-frame sizes) can call
// it directly without going through observeRow's heuristic.
func (p *progressTicker) addBytes(n int64) {
	if n <= 0 {
		return
	}
	p.bytes.Add(n)
}

// observeRow is the per-row hook the row pipe calls. Combines [inc]
// with a best-effort byte-count of the row's values via
// [approximateRowBytes]. Cheap: no allocation, walks the row map
// once, sums into a local int64.
func (p *progressTicker) observeRow(row ir.Row) {
	p.inc()
	p.addBytes(approximateRowBytes(row))
}

// approximateRowBytes estimates the wire-size of a row by walking
// its values once. The estimate is intentionally rough: the goal is
// to drive the rate_mb_per_sec gauge in progress logs, not to
// reconcile against MySQL's max_allowed_packet or PG's COPY
// statistics. Fixed-width types use their natural byte width;
// strings and []byte use their length; time.Time uses a typical
// wire-format width (24 bytes covers TIMESTAMPTZ with sub-second
// precision and a timezone suffix); nil contributes nothing.
//
// Unknown types contribute zero rather than guessing. The
// progressTicker's emitted rate is a lower bound on real wire
// throughput in such cases — reasonable behaviour for a metric
// whose only consumer is human eyeballs comparing tail -f output
// to expected throughput.
func approximateRowBytes(row ir.Row) int64 {
	if row == nil {
		return 0
	}
	var total int64
	for _, v := range row {
		total += approximateValueBytes(v)
	}
	return total
}

// approximateValueBytes returns the rough byte cost of a single
// IR-canonical value. See [approximateRowBytes] for the policy
// rationale.
func approximateValueBytes(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case string:
		return int64(len(x))
	case []byte:
		return int64(len(x))
	case bool:
		return 1
	case int8, uint8:
		return 1
	case int16, uint16:
		return 2
	case int32, uint32, float32:
		return 4
	case int, uint, int64, uint64, float64:
		return 8
	case time.Time:
		// 24 bytes covers TIMESTAMPTZ-with-tz at microsecond
		// precision in the canonical PG text form.
		return 24
	case []any:
		var n int64
		for _, e := range x {
			n += approximateValueBytes(e)
		}
		return n
	case []string:
		var n int64
		for _, s := range x {
			n += int64(len(s))
		}
		return n
	}
	// Approximation falls back to zero rather than guessing a value
	// that might be wildly wrong for engine-specific shapes (e.g.
	// pgtype.Numeric, geometry WKB). The rate_mb_per_sec gauge is
	// then a lower bound — accurate-enough for human inspection.
	return 0
}

// setTotalRows feeds the row-count estimate into the ticker. Safe to
// call from a separate goroutine (the COUNT(*) query typically runs
// on its own connection and may take seconds on a huge table). Zero
// is a no-op so callers that bail out of the count don't need a
// special path.
func (p *progressTicker) setTotalRows(total int64) {
	if total <= 0 {
		return
	}
	p.totalRows.Store(total)
}

// loop is the background goroutine. It wakes every interval, snapshots
// the counter, and logs a line when the count has advanced since the
// previous tick. When the count hasn't moved, no line is emitted —
// repeated "rows=0" lines on a stuck copy add noise without value.
func (p *progressTicker) loop(ctx context.Context) {
	defer p.wg.Done()
	t := time.NewTicker(p.interval)
	defer t.Stop()

	var lastRows int64
	var lastBytes int64
	lastTime := time.Now()
	for {
		select {
		case <-p.done:
			return
		case <-ctx.Done():
			return
		case now := <-t.C:
			rows := p.rows.Load()
			if rows == lastRows {
				continue
			}
			// Mark the wall-clock start of streaming on the first
			// non-zero tick so ETA uses a stable denominator that
			// matches the operator's perception of "when did this
			// table start moving?".
			if p.startedAt.Load() == nil {
				start := lastTime
				p.startedAt.Store(&start)
			}
			bytes := p.bytes.Load()
			elapsed := now.Sub(lastTime).Seconds()
			rate := float64(0)
			mbps := float64(0)
			if elapsed > 0 {
				rate = float64(rows-lastRows) / elapsed
				mbps = float64(bytes-lastBytes) / (elapsed * 1024 * 1024)
			}

			total := p.totalRows.Load()
			etaSecs := int64(-1)
			if total > 0 && total > rows && p.startedAt.Load() != nil {
				started := *p.startedAt.Load()
				totalElapsed := now.Sub(started).Seconds()
				if totalElapsed > 0 {
					avgRate := float64(rows) / totalElapsed
					if avgRate > 0 {
						etaSecs = int64(float64(total-rows) / avgRate)
					}
				}
			}

			attrs := []slog.Attr{
				slog.String("table", p.table),
				slog.Int64("rows", rows),
				slog.Int64("total_rows", total),
				slog.Int64("bytes", bytes),
				slog.Float64("rate_rows_per_sec", rate),
				slog.Float64("rate_mb_per_sec", mbps),
				slog.Int64("eta_seconds", etaSecs),
			}
			if p.hasChunk {
				attrs = append(attrs, slog.Int("chunk", p.chunk))
			}
			p.logger.LogAttrs(ctx, slog.LevelInfo, "bulk copy progress", attrs...)
			lastRows = rows
			lastBytes = bytes
			lastTime = now
		}
	}
}

// Stop tells the background goroutine to exit and emits a final
// summary line carrying the total row count for the table. Pass nil
// for err on the success path; pass the writer/context error on the
// failure path so the line reads "bulk copy aborted" instead of
// "complete". Idempotent — safe to call from a deferred cleanup as
// well as from the success path.
//
// The row count is "rows handed to the writer", not "rows the writer
// committed". On the failure path the writer may have flushed only a
// subset of those rows before erroring; the count is therefore an
// upper bound on what landed on the dest, not a confirmation that
// every counted row is durable. See progress.go's file-level note
// for the same caveat on the periodic progress line.
func (p *progressTicker) Stop(ctx context.Context, err error) {
	p.stopOnce.Do(func() {
		close(p.done)
		p.wg.Wait()
		msg := "bulk copy complete"
		if err != nil {
			msg = "bulk copy aborted"
		}
		attrs := []slog.Attr{
			slog.String("table", p.table),
			slog.Int64("rows", p.rows.Load()),
		}
		if p.hasChunk {
			attrs = append(attrs, slog.Int("chunk", p.chunk))
		}
		if err != nil {
			attrs = append(attrs, slog.String("err", err.Error()))
		}
		p.logger.LogAttrs(ctx, slog.LevelInfo, msg, attrs...)
	})
}

// kickOffRowCount runs an asynchronous CountRows query on the given
// reader and feeds the result into the ticker once it returns.
// Returns immediately so the bulk-copy phase doesn't pay the
// (potentially multi-second) COUNT(*) cost on the critical path.
//
// On count error or when the reader doesn't implement RowCounter,
// the ticker keeps reporting zero total / unknown ETA — non-fatal.
// The caller passes a context separate from the bulk-copy context
// so a slow COUNT(*) doesn't block bulk-copy abort.
func kickOffRowCount(ctx context.Context, rr ir.RowReader, table *ir.Table, pt *progressTicker) {
	rc, ok := rr.(ir.RowCounter)
	if !ok {
		return
	}
	go func() {
		count, err := rc.CountRows(ctx, table)
		if err != nil {
			slog.WarnContext(ctx, "migration: row-count probe failed; ETA will be unknown",
				slog.String("table", table.Name),
				slog.String("err", err.Error()))
			return
		}
		pt.setTotalRows(count)
	}()
}

// teeRows wraps an [ir.Row] channel and returns a new channel that
// forwards every row downstream while invoking onRow. onRow gets
// the row itself so call-sites can run any per-row bookkeeping
// (count, bytes, PK-tracking, etc.) without the tee plumbing
// caring about the bookkeeping shape. The tee runs in its own
// goroutine and propagates ctx cancellation; downstream is closed
// when src closes (normal completion) or when ctx ends (early
// abort).
//
// The function is unexported and returns an unbuffered channel; the
// tee goroutine is the only writer, and the [ir.RowWriter] consumer
// is the only reader. Buffering wouldn't help here — the writer's
// throughput is the bottleneck, and an unbounded buffer would just
// hide back-pressure.
func teeRows(ctx context.Context, src <-chan ir.Row, onRow func(ir.Row)) <-chan ir.Row {
	out := make(chan ir.Row)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case row, ok := <-src:
				if !ok {
					return
				}
				onRow(row)
				select {
				case out <- row:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
