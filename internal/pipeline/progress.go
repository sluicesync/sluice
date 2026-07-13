// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

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

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/progress"
)

// migratePhaseLabels maps each migrate phase to its checklist display
// label (ADR-0155). Kept beside the Spec so the label and the emit-site
// can't drift.
var migratePhaseLabels = map[ir.MigrationPhase]string{
	ir.MigrationPhaseTables:       "Tables",
	ir.MigrationPhaseBulkCopy:     "Bulk copy",
	ir.MigrationPhaseIndexes:      "Indexes",
	ir.MigrationPhaseIdentitySync: "Identity",
	ir.MigrationPhaseConstraints:  "Constraints",
	ir.MigrationPhaseViews:        "Views",
}

// migPhase adapts an [ir.MigrationPhase] onto the command-agnostic
// [progress.Phase] the sink now speaks (ADR-0155 phase 2). The Key is the
// phase string — unchanged from phase 1 — so migrate's LogSink lines stay
// byte-identical; the Label drives the TTY checklist.
func migPhase(p ir.MigrationPhase) progress.Phase {
	return progress.Phase{Key: string(p), Label: migratePhaseLabels[p]}
}

// MigrateProgressSpec is the pretty-view [progress.Spec] for `sluice
// migrate` — the checklist order the operator reads (which differs
// slightly from the internal completion order: identity-sync completes
// before indexes on the MySQL fallback path; a row is marked done whenever
// its PhaseCompleted arrives, so an out-of-display-order completion still
// fills in correctly). The CLI hands this to [progress.NewTTYSink].
var MigrateProgressSpec = progress.Spec{
	Title: "sluice migrate",
	Phases: []progress.Phase{
		migPhase(ir.MigrationPhaseTables),
		migPhase(ir.MigrationPhaseBulkCopy),
		migPhase(ir.MigrationPhaseIndexes),
		migPhase(ir.MigrationPhaseIdentitySync),
		migPhase(ir.MigrationPhaseConstraints),
		migPhase(ir.MigrationPhaseViews),
	},
	ProgressKey: string(ir.MigrationPhaseBulkCopy),
	LabelWidth:  11,
}

// sinkOrNop defaults a nil presentation sink to the no-op sink so the
// incremental-backup emit call-sites stay nil-free (migrate uses
// [progress.FromContext] instead; the pipeline package hosts both).
func sinkOrNop(s progress.Sink) progress.Sink {
	if s == nil {
		return progress.Nop{}
	}
	return s
}

// Backup-incremental phases (ADR-0155 phase 2). Incremental backup keeps
// its historical direct-slog output on the non-TTY path (the sink is
// [progress.Nop] there); the checklist drives only the interactive view.
var (
	incrPhaseConnect  = progress.Phase{Key: "connect", Label: "Connect"}
	incrPhaseStream   = progress.Phase{Key: "stream", Label: "Stream"}
	incrPhaseFinalize = progress.Phase{Key: "finalize", Label: "Finalize"}
)

// IncrementalProgressSpec is the pretty-view spec for `sluice backup
// incremental`.
var IncrementalProgressSpec = progress.Spec{
	Title:      "sluice backup incremental",
	Phases:     []progress.Phase{incrPhaseConnect, incrPhaseStream, incrPhaseFinalize},
	LabelWidth: 12,
}

// runRowTotalKey carries a run-scoped rows-copied accumulator through the
// context so every per-table / per-chunk [progressTicker] can add its
// final count without threading a parameter through the concurrent copy
// graph — the same context-carrier pattern the presentation sink uses.
type runRowTotalKey struct{}

// withRunRowTotal attaches a fresh rows-copied accumulator to ctx and
// returns both. The migrate orchestrator reads the accumulator after the
// copy phases to populate the summary's total row count (best-effort:
// "rows handed to the writer" summed across every ticker — see the
// file-level note on that count's exactness). Callers that don't attach
// one (sync cold-start, tests) leave every ticker's add a nil-safe no-op.
func withRunRowTotal(ctx context.Context) (context.Context, *atomic.Int64) {
	c := new(atomic.Int64)
	return context.WithValue(ctx, runRowTotalKey{}, c), c
}

// runRowTotalFromContext returns the run's rows-copied accumulator, or nil
// when none is attached.
func runRowTotalFromContext(ctx context.Context) *atomic.Int64 {
	c, _ := ctx.Value(runRowTotalKey{}).(*atomic.Int64)
	return c
}

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

	// sink is the run's presentation sink (ADR-0155), resolved from the
	// context at construction. On the structured-log path it is the
	// [progress.LogSink], whose TableProgress is a no-op — the rich
	// "bulk copy progress" slog line below is unchanged and byte-identical.
	// On the interactive path it is the [progress.TTYSink], which renders
	// the per-table bar from the (done,total) pair; the slog line is then
	// harmlessly emitted into the CLI's silenced handler.
	sink progress.Sink

	// runRows is the run-scoped rows-copied accumulator (nil when the
	// caller attached none). On a clean Stop this ticker adds its final
	// row count so the orchestrator can report an accurate migration total.
	runRows *atomic.Int64

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
		sink:     progress.FromContext(ctx),
		runRows:  runRowTotalFromContext(ctx),
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
		sink:     progress.FromContext(ctx),
		runRows:  runRowTotalFromContext(ctx),
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
// [ir.ApproximateRowBytes]. Cheap: no allocation, walks the row map
// once, sums into a local int64.
func (p *progressTicker) observeRow(row ir.Row) {
	p.inc()
	p.addBytes(ir.ApproximateRowBytes(row))
}

// approximateRowBytes is retained as a thin pass-through for the
// existing in-package tests; v0.7.0 moved the implementation to
// [ir.ApproximateRowBytes] so the engine packages' batched-write
// and CDC-apply paths can reuse it without importing pipeline
// (which would invert the layering). See ADR-0028.
func approximateRowBytes(row ir.Row) int64 {
	return ir.ApproximateRowBytes(row)
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
			// table start moving?". CompareAndSwap (vs check-then-
			// store) keeps the contract correct if loop ever runs
			// from multiple goroutines — single-goroutine today,
			// but the future-proofing is one line.
			start := lastTime
			p.startedAt.CompareAndSwap(nil, &start)
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
			// ADR-0155: feed the interactive per-table bar. No-op on the
			// LogSink; the rich slog line below stays byte-identical.
			p.sink.TableProgress(p.table, rows, total)
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
		// ADR-0155: fill the interactive per-table bar to 100% on a clean
		// finish (done==total). No-op on the LogSink. On abort we leave the
		// bar where it stopped — the failure surfaces via the summary panel.
		// Also fold this ticker's final row count into the run-scoped total
		// so the orchestrator can report an accurate migration-wide sum
		// (summing per ticker avoids the model-side undercount where a
		// chunked table's per-chunk TableProgress calls overwrite by name).
		if err == nil {
			done := p.rows.Load()
			p.sink.TableProgress(p.table, done, done)
			if p.runRows != nil {
				p.runRows.Add(done)
			}
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
//
// If ctx is cancelled (or the ticker is stopped) before CountRows
// returns, the goroutine exits silently — the count would be
// useless after the parent has already torn down, and a warn line
// at that point is just noise interleaving with the cleanup logs.
func kickOffRowCount(ctx context.Context, rr ir.RowReader, table *ir.Table, pt *progressTicker) {
	rc, ok := rr.(ir.RowCounter)
	if !ok {
		return
	}
	go func() {
		count, err := rc.CountRows(ctx, table)
		if err != nil {
			if ctx.Err() != nil {
				// Parent gave up; the count isn't actionable. Don't
				// log — it's interleaved teardown noise.
				return
			}
			slog.WarnContext(ctx, "migration: row-count probe failed; ETA will be unknown",
				slog.String("table", table.Name),
				slog.String("err", err.Error()))
			return
		}
		// Skip the store if the ticker is already stopped. The
		// atomic.Store is harmless in itself, but skipping keeps
		// the contract clean (a stopped ticker doesn't mutate).
		select {
		case <-pt.done:
			return
		default:
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
// The tee goroutine is the only writer, and the [ir.RowWriter]
// consumer is the only reader. The downstream channel carries a small
// bounded buffer (see [migcore.RowChanBuffer]) so source decode and target
// write overlap instead of rendezvous-alternating; back-pressure is
// preserved because the buffer is bounded.
func teeRows(ctx context.Context, src <-chan ir.Row, onRow func(ir.Row)) <-chan ir.Row {
	out := make(chan ir.Row, migcore.RowChanBuffer)
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
