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
// (or fails) to flush a final "completed" line and stop the goroutine.
type progressTicker struct {
	rows     atomic.Int64
	table    string
	interval time.Duration
	logger   *slog.Logger

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

// loop is the background goroutine. It wakes every interval, snapshots
// the counter, and logs a line when the count has advanced since the
// previous tick. When the count hasn't moved, no line is emitted —
// repeated "rows=0" lines on a stuck copy add noise without value.
func (p *progressTicker) loop(ctx context.Context) {
	defer p.wg.Done()
	t := time.NewTicker(p.interval)
	defer t.Stop()

	var lastRows int64
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
			elapsed := now.Sub(lastTime).Seconds()
			rate := float64(0)
			if elapsed > 0 {
				rate = float64(rows-lastRows) / elapsed
			}
			p.logger.LogAttrs(ctx, slog.LevelInfo, "bulk copy progress",
				slog.String("table", p.table),
				slog.Int64("rows", rows),
				slog.Float64("rate", rate),
			)
			lastRows = rows
			lastTime = now
		}
	}
}

// Stop tells the background goroutine to exit and emits a final
// "bulk copy complete" line carrying the total row count for the
// table. Idempotent — safe to call from a deferred cleanup as well as
// from the success path.
func (p *progressTicker) Stop(ctx context.Context) {
	p.stopOnce.Do(func() {
		close(p.done)
		p.wg.Wait()
		p.logger.LogAttrs(ctx, slog.LevelInfo, "bulk copy complete",
			slog.String("table", p.table),
			slog.Int64("rows", p.rows.Load()),
		)
	})
}

// teeRows wraps an [ir.Row] channel and returns a new channel that
// forwards every row downstream while invoking onRow. The tee runs in
// its own goroutine and propagates ctx cancellation; downstream is
// closed when src closes (normal completion) or when ctx ends (early
// abort).
//
// The function is unexported and returns an unbuffered channel; the
// tee goroutine is the only writer, and the [ir.RowWriter] consumer
// is the only reader. Buffering wouldn't help here — the writer's
// throughput is the bottleneck, and an unbounded buffer would just
// hide back-pressure.
func teeRows(ctx context.Context, src <-chan ir.Row, onRow func()) <-chan ir.Row {
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
				onRow()
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
