//go:build integration

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

	"sluicesync.dev/sluice/internal/progress"
)

// progressWatchdog bounds a migration by IDLE TIME rather than by total wall
// clock: it cancels only when no progress event has arrived for `idle`.
//
// # TESTFRAGILE-3, which is why this exists
//
// `TestMigrate_FastLoader_CrashMidFastChunk_ResumeIsIdempotent/postgres` blew a
// fixed `3*time.Minute` budget on two consecutive RELEASE TAGS — v0.152.1 and
// v0.152.2 — both times at ~181s, both times passing on rerun, while the MySQL
// leg of the same test finishes in about six seconds. Neither failure was a
// regression: the two tags carried unrelated runtime deltas and produced an
// identical `create indexes: context deadline exceeded`.
//
// It fires on tags specifically because routine pushes are Linux-only while the
// Windows matrix and the full job set join on TAG pushes — so a tag CI is the
// most contended run the project ever does, and a fixed budget is most likely
// to blow exactly when it is most expensive to blow, on the check that gates
// publish.
//
// # Why a bigger number is the wrong fix
//
// A fixed deadline cannot distinguish the two things it is asked to
// distinguish. "Hung" and "slow because forty other jobs are running" look
// identical to it, so any constant is either too small on a loaded runner or
// too large to catch the hang the test exists to catch. Raising it trades a
// flake for a weaker test and leaves the ambiguity intact.
//
// An IDLE bound removes the ambiguity instead. A migration that is merely
// contended still emits progress — phases start and complete, tables report
// rows — so it is never idle for long. A migration that is genuinely wedged
// emits nothing, and is caught in `idle` rather than in the remaining budget.
// The signal is what the run is DOING rather than how long it has been
// running.
//
// # The interface is implemented explicitly, not by embedding progress.Nop
//
// Embedding would make this compile against a future [progress.Sink] method
// and silently NOT observe it — a new event class would stop counting as
// progress, and the watchdog would start firing on runs that are working. An
// explicit implementation turns that into a build failure and a decision.
type progressWatchdog struct {
	mu   sync.Mutex
	last time.Time
	idle time.Duration

	cancel context.CancelCauseFunc
	stop   chan struct{}
	once   sync.Once
}

// newProgressWatchdog returns a context that survives as long as the migration
// keeps reporting progress, plus the sink to hand the Migrator and a stop
// function the caller must defer.
//
// `hard` is a backstop, not the working bound: a run that emits progress
// forever without finishing would otherwise never be cut off, and a test that
// can hang indefinitely is worse than one with a budget that is too tight. Set
// it generously — it should never be the thing that fires.
func newProgressWatchdog(parent context.Context, idle, hard time.Duration) (
	context.Context, *progressWatchdog, func(),
) {
	ctx, cancel := context.WithCancelCause(parent)
	hardCtx, hardCancel := context.WithTimeout(ctx, hard)

	w := &progressWatchdog{
		last:   time.Now(),
		idle:   idle,
		cancel: cancel,
		stop:   make(chan struct{}),
	}

	go w.run()

	return hardCtx, w, func() {
		w.once.Do(func() { close(w.stop) })
		hardCancel()
		cancel(nil)
	}
}

func (w *progressWatchdog) run() {
	// Tick well inside the idle window so the cancel lands promptly once the
	// run really has stopped, rather than up to a full window late.
	//
	// The floor is 50ms rather than the 1s it started at. 1s was fine for the
	// real 90s bound (tick 22.5s) and made the watchdog UNTESTABLE at short
	// idles: a mutation run with a 1ns bound did not fire, because the resume
	// it guards finishes in ~2s against a warm container and the watchdog had
	// not ticked once. A floor that prevents the thing from being exercised is
	// a floor that hides whether it works.
	tick := w.idle / 4
	if tick < 50*time.Millisecond {
		tick = 50 * time.Millisecond
	}
	t := time.NewTicker(tick)
	defer t.Stop()

	for {
		select {
		case <-w.stop:
			return
		case <-t.C:
			w.mu.Lock()
			since := time.Since(w.last)
			w.mu.Unlock()
			if since >= w.idle {
				w.cancel(&watchdogIdleError{idle: w.idle, since: since})
				return
			}
		}
	}
}

// mark records that something happened. Every Sink method routes here — the
// watchdog does not care WHICH event arrived, only that the run is alive.
func (w *progressWatchdog) mark() {
	w.mu.Lock()
	w.last = time.Now()
	w.mu.Unlock()
}

func (w *progressWatchdog) PhaseStarted(progress.Phase)        { w.mark() }
func (w *progressWatchdog) PhaseCompleted(progress.Phase)      { w.mark() }
func (w *progressWatchdog) PhaseCompletedEarly(progress.Phase) { w.mark() }
func (w *progressWatchdog) TableProgress(string, int64, int64) { w.mark() }
func (w *progressWatchdog) Warn(string, ...any)                { w.mark() }
func (w *progressWatchdog) Summary(progress.Result)            { w.mark() }

// watchdogIdleError is the cancel cause, so a test that trips the watchdog
// says "nothing happened for N" rather than the bare `context canceled` a
// plain cancel would produce — which reads as a test bug rather than as the
// stall it is reporting.
type watchdogIdleError struct {
	idle  time.Duration
	since time.Duration
}

func (e *watchdogIdleError) Error() string {
	return "progress watchdog: the migration reported no progress for " + e.since.String() +
		" (idle bound " + e.idle.String() + ") — this is a STALL, not a slow runner: a contended " +
		"migration still emits phase and table events, so silence for this long means the run is " +
		"wedged rather than merely behind"
}

var _ progress.Sink = (*progressWatchdog)(nil)

// TestProgressWatchdog_FiresOnlyOnSilence is the watchdog's own gate, and it
// exists because the integration test it serves cannot be one.
//
// A mutation run that set the idle bound to 1ns did NOT fail that test: the
// resume leg finishes in about two seconds against a warm container, and the
// watchdog's tick floor meant it had not looked even once. The mutation proved
// nothing in either direction — the shape CLAUDE.md warns about, where a green
// result reads like evidence and carries none.
//
// So the machinery is graded here, where both directions are cheap and fast.
// The margins are deliberately wide (an order of magnitude between the idle
// bound and the marking interval) because this is the one test in the file
// that legitimately depends on real time — bounding a run by idleness means
// something has to measure idleness.
func TestProgressWatchdog_FiresOnlyOnSilence(t *testing.T) {
	t.Parallel()

	t.Run("fires when nothing reports progress", func(t *testing.T) {
		t.Parallel()

		ctx, _, stop := newProgressWatchdog(context.Background(), 150*time.Millisecond, time.Minute)
		defer stop()

		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Fatal("the watchdog did NOT fire after 5s of total silence with a 150ms idle bound — " +
				"a stall would run to the hard backstop instead of being caught, which is the whole " +
				"failure this was built to catch")
		}

		var idleErr *watchdogIdleError
		if !errors.As(context.Cause(ctx), &idleErr) {
			t.Fatalf("the watchdog cancelled with %v, not a watchdogIdleError — a bare `context "+
				"canceled` reads as a test bug rather than as the stall it is reporting, which is "+
				"most of this type's value", context.Cause(ctx))
		}
		if !strings.Contains(idleErr.Error(), "STALL") {
			t.Errorf("the cause does not distinguish a stall from a slow runner: %q", idleErr.Error())
		}
	})

	t.Run("does NOT fire while progress keeps arriving", func(t *testing.T) {
		t.Parallel()

		ctx, w, stop := newProgressWatchdog(context.Background(), 150*time.Millisecond, time.Minute)
		defer stop()

		// Mark an order of magnitude faster than the idle bound, for several
		// multiples of it. If this fires, the watchdog would cancel healthy
		// contended runs — strictly worse than the fixed budget it replaced,
		// because it would fail runs that are working.
		deadline := time.After(900 * time.Millisecond)
		tick := time.NewTicker(15 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-deadline:
				if ctx.Err() != nil {
					t.Fatalf("the watchdog fired (%v) while progress arrived every 15ms against a "+
						"150ms idle bound — it would cancel healthy runs", context.Cause(ctx))
				}
				return
			case <-tick.C:
				w.TableProgress("t", 1, 2)
			case <-ctx.Done():
				t.Fatalf("the watchdog fired (%v) while progress was still arriving",
					context.Cause(ctx))
			}
		}
	})

	t.Run("every Sink method counts as progress", func(t *testing.T) {
		t.Parallel()

		// The methods route to one mark(), so this is cheap — but it is the
		// anti-vacuity arm for the explicit-implementation decision: if a
		// future method were added and wired to nothing, its events would stop
		// counting and the watchdog would fire on runs emitting only those.
		for name, call := range map[string]func(*progressWatchdog){
			"PhaseStarted":        func(w *progressWatchdog) { w.PhaseStarted(progress.Phase{}) },
			"PhaseCompleted":      func(w *progressWatchdog) { w.PhaseCompleted(progress.Phase{}) },
			"PhaseCompletedEarly": func(w *progressWatchdog) { w.PhaseCompletedEarly(progress.Phase{}) },
			"TableProgress":       func(w *progressWatchdog) { w.TableProgress("t", 1, 2) },
			"Warn":                func(w *progressWatchdog) { w.Warn("x") },
			"Summary":             func(w *progressWatchdog) { w.Summary(progress.Result{}) },
		} {
			w := &progressWatchdog{idle: time.Hour}
			w.last = time.Now().Add(-time.Hour)
			before := w.last
			call(w)
			if !w.last.After(before) {
				t.Errorf("%s did not count as progress — a run reporting only this event class would "+
					"look idle to the watchdog", name)
			}
		}
	})
}
