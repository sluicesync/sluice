// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/progress"
)

// The IDLE-PROGRESS COPY WATCHDOG (roadmap item 146).
//
// # Why a watchdog rather than a fourth point fix
//
// Three silent hangs in three release cycles, three different mechanisms:
// Bug 228 (a cleared transient deadlocking the copy), item 138 (81% of a
// 91-minute run quiesced while every layer reported success), and Bug 229 (a
// dead target socket with no write deadline). Each was found by a different
// route and NONE was caught by a gate, for one shared reason: from the
// outside all three look exactly like healthy slow progress. Silence is not a
// symptom sluice ever reported.
//
// A watchdog closes that class without knowing the mechanism. It cannot fix
// any of the three — the write deadline in [netdeadline] fixes Bug 229's
// instance — but it is the only thing here that would have SPOKEN in all
// three, including the fourth one nobody has hit yet.
//
// # The three design decisions, and what they cost
//
//  1. PROGRESS IS ROWS HANDED TO THE WRITER (bytes on the raw lane, which has
//     no row granularity). Not chunks completed, not durable checkpoints:
//     those advance at whole-chunk granularity, so a stall inside one chunk —
//     which is every one of the three incidents — would be invisible for as
//     long as the chunk was meant to take. The row counter is the finest
//     signal that already exists, it is already atomic, and it freezes for
//     the right reason: rows reach it through the tee that feeds the writer,
//     so a writer that stops draining stops the count.
//
//  2. IT WARNS, IT DOES NOT ABORT. A copy that has genuinely stalled and one
//     that is merely slower than this threshold are indistinguishable from
//     here — that is the whole premise — so aborting would eventually kill a
//     legitimate copy on the strength of a guess. The loud-failure tenet is
//     served by the layers that CAN tell (the write deadline, the per-lane
//     retry budgets, the source-read resume ladder); this layer's job is to
//     end the silence, and a run that is stalled and reported is already
//     categorically better than one that is stalled and mute.
//
//  3. IT IS SILENT DURING A DELIBERATE QUIESCE. An ADR-0110 grow-gate window
//     parks every lane on purpose for up to GrowGateMaxHold (20 minutes).
//     That is not a stall, and a watchdog that reported it would be noise on
//     every storage grow — noise gets suppressed, and a suppressed watchdog
//     catches nothing, which is strictly worse than not having one. The gate
//     is asked directly ([ir.GrowGateQuiesceObserver]); quiesced time does
//     not accumulate toward the threshold.
//
// # What this does NOT reach, stated rather than implied
//
// It observes the COPY. A stall in a phase with no row flow — index build,
// constraint add, schema apply, the CDC apply path — is outside it. Those
// phases have their own bounded-retry ladders and are not the class the three
// incidents came from, but the gap is real and this is where to record it.

// copyStallWarnAfter is how long a copy lane may make NO forward progress
// before the watchdog says so.
//
// Five minutes, chosen against the FALSE-POSITIVE side rather than the
// detection side. The things that legitimately produce no rows for a while
// and are not stalls: a single large `LOAD DATA` segment draining to a slow
// target, a chunk-boundary query on a huge table, a source read that has not
// yielded its first row yet. Minutes, not tens of minutes — but the cost of
// being wrong is asymmetric, so the threshold sits above the plausible range
// and the ESCALATION (below) is what keeps a genuine stall from being
// under-reported. Quiesce time is excluded before this is ever consulted, so
// the 20-minute grow-gate ceiling does not have to be accommodated here.
//
// Deliberately NOT an operator flag in this chunk, and the reason is that it
// cannot break anything: the watchdog only warns, so there is no failure mode
// to tune away from, and a knob whose only effect is when a log line appears
// is a knob nobody can be given good advice about. The same call the
// [netkeepalive] constants make. If a real operator need appears, this lifts
// into the copy-phase flag surface — and then it owes the migrate/sync parity
// gate an entry, like every other copy-phase knob.
//
// A var rather than a const only so the tests can drive it in seconds; it is
// read ONCE per lane at construction, in the constructing goroutine.
var copyStallWarnAfter = 5 * time.Minute

// copyStallRewarnCap bounds the escalating re-warn interval. Without an
// escalation a genuinely wedged overnight run would emit one line every five
// minutes — a flood that trains an operator to filter the message, which is
// the same suppression failure decision (3) exists to avoid. With it, a stall
// is reported at 5m, 15m, 35m, 75m, then hourly.
const copyStallRewarnCap = time.Hour

// copyStallPollDivisor derives the poll interval from the threshold: often
// enough that the reported idle duration is accurate to a tenth of the
// threshold, rarely enough that a wide fan-out's worth of watchdogs costs
// nothing measurable.
const copyStallPollDivisor = 10

// copyQuiesceKey carries the run's grow-gate quiesce source through the
// context, so a watchdog created deep in the concurrent copy graph can ask
// "was that pause deliberate?" without threading a parameter through every
// copy function — the same context-carrier pattern as the presentation sink
// and the run-scoped row total.
type copyQuiesceKey struct{}

// withCopyQuiesceSource attaches gate's quiesce observation to ctx. A nil
// gate, or one that does not implement [ir.GrowGateQuiesceObserver], leaves
// the context unchanged: the watchdog then reads "never quiesced", which
// over-reports rather than under-reports and is the safe direction for a
// warning.
func withCopyQuiesceSource(ctx context.Context, gate ir.GrowGate) context.Context {
	obs, ok := gate.(ir.GrowGateQuiesceObserver)
	if !ok || obs == nil {
		return ctx
	}
	return context.WithValue(ctx, copyQuiesceKey{}, obs.QuiescedSince)
}

// copyQuiesceSourceFromContext returns the run's quiesce predicate, or nil
// when none is attached.
func copyQuiesceSourceFromContext(ctx context.Context) func(since time.Time) bool {
	fn, _ := ctx.Value(copyQuiesceKey{}).(func(since time.Time) bool)
	return fn
}

// copyStallWatchdog watches one copy lane's progress counter and warns when
// it stops moving. One per lane; call [copyStallWatchdog.Stop] when the lane
// finishes (or fails) to end the goroutine.
type copyStallWatchdog struct {
	table    string
	chunk    int
	hasChunk bool
	// units names what progress() counts, so the warning says "rows" on the
	// row lanes and "bytes" on the ADR-0078 raw byte-pipe lane.
	units     string
	progress  func() int64
	quiesced  func(since time.Time) bool
	warnAfter time.Duration
	poll      time.Duration
	sink      progress.Sink

	// now is the clock, a test seam. nil ⇒ time.Now.
	now func() time.Time

	// warns counts the warnings emitted — the observable the wiring tests
	// assert on, and cheap enough to keep in production.
	warns atomic.Int64

	stopOnce sync.Once
	done     chan struct{}
	wg       sync.WaitGroup
}

// newCopyStallWatchdog starts a watchdog over progress for the named lane.
// chunk < 0 means the single-reader lane (no chunk attribute).
func newCopyStallWatchdog(ctx context.Context, table string, chunk int, units string, progressFn func() int64) *copyStallWatchdog {
	return startCopyStallWatchdog(ctx, &copyStallWatchdog{
		table:     table,
		chunk:     chunk,
		hasChunk:  chunk >= 0,
		units:     units,
		progress:  progressFn,
		warnAfter: copyStallWarnAfter,
	})
}

// startCopyStallWatchdog fills in the derived fields and launches the loop.
// Split out so the tests can build a watchdog with an explicit threshold and
// clock without going through the context.
func startCopyStallWatchdog(ctx context.Context, w *copyStallWatchdog) *copyStallWatchdog {
	if w.warnAfter <= 0 {
		w.warnAfter = copyStallWarnAfter
	}
	if w.poll <= 0 {
		w.poll = w.warnAfter / copyStallPollDivisor
	}
	if w.poll <= 0 {
		w.poll = time.Millisecond
	}
	if w.sink == nil {
		w.sink = progress.FromContext(ctx)
	}
	if w.quiesced == nil {
		w.quiesced = copyQuiesceSourceFromContext(ctx)
	}
	w.done = make(chan struct{})
	w.wg.Add(1)
	go w.loop(ctx)
	return w
}

func (w *copyStallWatchdog) clock() time.Time {
	if w.now != nil {
		return w.now()
	}
	return time.Now()
}

// Stop ends the goroutine. Idempotent, so it is safe from a deferred cleanup
// as well as the success path.
func (w *copyStallWatchdog) Stop() {
	w.stopOnce.Do(func() {
		close(w.done)
		w.wg.Wait()
	})
}

func (w *copyStallWatchdog) loop(ctx context.Context) {
	defer w.wg.Done()
	t := time.NewTicker(w.poll)
	defer t.Stop()

	last := w.progress()
	since := w.clock()
	step := w.warnAfter
	nextWarn := w.warnAfter
	for {
		select {
		case <-w.done:
			return
		case <-ctx.Done():
			return
		case <-t.C:
			now := w.clock()
			if cur := w.progress(); cur != last {
				// Forward progress. Any amount resets the clock — the
				// watchdog reports STOPPED, never SLOW, which is what keeps
				// a legitimately slow-but-advancing copy silent.
				last, since, step, nextWarn = cur, now, w.warnAfter, w.warnAfter
				continue
			}
			if w.quiesced != nil && w.quiesced(since) {
				// Some of this idle window was an ADR-0110 coordinated pause,
				// which is a deliberate stop and not a stall. Restart the
				// clock rather than trying to subtract the window: the gate
				// reports THAT it paused, and an approximation that
				// under-counted quiesce time would reintroduce exactly the
				// false positive this branch exists to prevent.
				since, step, nextWarn = now, w.warnAfter, w.warnAfter
				continue
			}
			idle := now.Sub(since)
			if idle < nextWarn {
				continue
			}
			w.warn(idle, last)
			if step *= 2; step > copyStallRewarnCap {
				step = copyStallRewarnCap
			}
			nextWarn = idle + step
		}
	}
}

func (w *copyStallWatchdog) warn(idle time.Duration, at int64) {
	n := w.warns.Add(1)
	attrs := []any{
		slog.String("table", w.table),
		slog.Duration("idle", idle.Round(time.Second)),
		slog.Int64("progress_"+w.units, at),
		slog.Int64("warning", n),
	}
	if w.hasChunk {
		attrs = append(attrs, slog.Int("chunk", w.chunk))
	}
	// Emitted through the SINK, not slog directly: [progress.LogSink] (the
	// default, and every non-TTY run) routes it to slog.Warn unchanged, while
	// the interactive path — where the CLI silences slog for the render's
	// duration — would otherwise swallow the one line the operator most needs.
	w.sink.Warn(
		"pipeline: cold-copy has made NO forward progress — the copy is still running, but nothing has moved for a "+
			"while. This is NOT a deliberate ADR-0110 grow-gate pause (those are excluded) and NOT a merely slow copy "+
			"(any forward progress resets this clock), so something between the source read and the target write is not "+
			"returning: a wedged connection, a lock wait, or a read that never yields. Check the target's active "+
			"queries/locks; sluice will keep waiting and will not abort on this warning",
		attrs...,
	)
}
