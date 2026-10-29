// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// stopObservedAttempt is one attempt's settle with a `sync stop` observed:
// the real phaseSettleDispatch, wrapped by the real stopTerminal, exactly as
// applyWithReshardFollow returns it. srcErr is what the source pump reports.
func stopObservedAttempt(t *testing.T, srcErr error) error {
	t.Helper()
	store := &fakeLivenessStore{value: 500}
	app := livenessOnlyApplier{store}
	s := &Streamer{Applier: app, sourceErrFn: func() error { return srcErr }}
	s.liveness = &runLiveness{ctx: context.Background()}
	s.armStreamLiveness("st")
	var stop stopSignal
	stop.observed.Store(true)
	return stopTerminal(&stop, s.phaseSettleDispatch(context.Background(), app, "st", nil, &stop))
}

// TestStopObservedRunIsTerminal_RetryLoop is the third fix-aimed review of
// d2bbcad8 (MEDIUM): with a `sync stop` observed, an attempt ending in an
// error wrapping ir.ErrPositionInvalid reached runWithRetry's ADR-0093
// reactive re-snapshot, which set RestartFromScratch and started a new
// attempt — one that clears the stop request, streams on, and on a native
// binlog source drops and recreates the in-scope target tables. A
// stop-observed Run is now terminal: one attempt, no re-snapshot, and the
// error keeps its identity. The control cell (no stop) still re-snapshots.
func TestStopObservedRunIsTerminal_RetryLoop(t *testing.T) {
	invalid := fmt.Errorf("vstream: %w", ir.ErrPositionInvalid)
	t.Run("stop observed: terminal, no re-snapshot", func(t *testing.T) {
		s := fastRetryStreamer(t, true, 8)
		calls := 0
		s.runOnceFn = func(context.Context) error {
			calls++
			return stopObservedAttempt(t, invalid)
		}
		err := s.Run(context.Background())
		if calls != 1 || s.RestartFromScratch {
			t.Fatalf("attempts %d, RestartFromScratch %v; want one attempt and no re-snapshot", calls, s.RestartFromScratch)
		}
		if !errors.Is(err, ir.ErrPositionInvalid) || !errors.Is(err, ErrStopRequestedRun) || errors.Is(err, ErrStopDrainHardCancelled) {
			t.Fatalf("Run = %v; want the invalid position kept, marked stop-requested, not an undrained stop", err)
		}
	})
	// The single-attempt path Run falls back to when the retry loop cannot
	// open its position reader has its own reactive re-snapshot loop.
	t.Run("stop observed, single-attempt path: no re-snapshot", func(t *testing.T) {
		s := &Streamer{StreamID: "test-stream"}
		calls := 0
		s.runOnceFn = func(context.Context) error {
			calls++
			return stopObservedAttempt(t, invalid)
		}
		err := s.runOnceWithReactiveResnapshot(context.Background())
		if calls != 1 || s.RestartFromScratch || !errors.Is(err, ir.ErrPositionInvalid) {
			t.Fatalf("attempts %d, RestartFromScratch %v, err %v; want one attempt, no re-snapshot, the invalid position kept",
				calls, s.RestartFromScratch, err)
		}
	})
	t.Run("no stop: the re-snapshot still runs (control)", func(t *testing.T) {
		s := fastRetryStreamer(t, true, 8)
		calls := 0
		s.runOnceFn = func(context.Context) error {
			calls++
			if calls == 1 {
				return invalid
			}
			return nil
		}
		if err := s.Run(context.Background()); err != nil || calls != 2 || !s.RestartFromScratch {
			t.Fatalf("Run = %v after %d attempts, RestartFromScratch %v; want the re-snapshot recovery", err, calls, s.RestartFromScratch)
		}
	})
}

// TestApplyWithReshardFollowMarksAStopObservedRun pins the wrap where it is
// applied in production (applyWithReshardFollow), not a test helper's own
// stopTerminal call: with a stop observed, both the settle exit and the
// reshard-reopen exit return an error carrying ErrStopRequestedRun beside
// its own identity; with no stop, neither is marked.
func TestApplyWithReshardFollowMarksAStopObservedRun(t *testing.T) {
	invalid := fmt.Errorf("vstream: %w", ir.ErrPositionInvalid)
	ctx := context.Background()
	for _, observed := range []bool{true, false} {
		var stop stopSignal
		stop.observed.Store(observed)
		settled := (&Streamer{ApplyBatchSize: 1, sourceErrFn: func() error { return invalid }}).
			applyWithReshardFollow(ctx, ctx, &reshardRecordingApplier{}, "sid", chanOf(), &liveAddedFilter{}, &stop)
		reopenErr := errors.New("vtgate: journal unreadable")
		reopened := newReshardStreamer(&fakeReshardReader{err: reopenErr}).
			applyWithReshardFollow(ctx, ctx, &reshardRecordingApplier{}, "sid", chanOf(), &liveAddedFilter{}, &stop)
		for name, c := range map[string]struct {
			err  error
			keep error
		}{"settle exit": {settled, ir.ErrPositionInvalid}, "reshard-reopen exit": {reopened, reopenErr}} {
			if !errors.Is(c.err, c.keep) {
				t.Errorf("observed=%v, %s: %v lost %v", observed, name, c.err, c.keep)
			}
			if errors.Is(c.err, ErrStopRequestedRun) != observed {
				t.Errorf("observed=%v, %s: marked stop-requested = %v", observed, name, errors.Is(c.err, ErrStopRequestedRun))
			}
		}
	}
}

// TestStopObservedRunIsTerminal_Supervisor is the fleet half: a leg that
// fails with a non-retriable error after its stop was observed — one not on
// the supervisor's refusals-a-restart-repeats list — used to be restarted on
// backoff, and the restart cleared the stop request. It now ends failed, not
// restarted, keeping its own exit status (a coded refusal's 3).
func TestStopObservedRunIsTerminal_Supervisor(t *testing.T) {
	refusal := sluicecode.Wrap(sluicecode.CodeColdStartTargetNotEmpty, "empty the target", errors.New("target not empty"))
	var stop stopSignal
	stop.observed.Store(true)
	runs := 0
	sup := NewSupervisor([]SupervisedSync{{
		ID: "leg",
		Runner: runnerFunc(func(context.Context) error {
			runs++
			return stopTerminal(&stop, refusal)
		}),
	}}, fastTestPolicy(0))
	err := sup.Run(context.Background())
	if runs != 1 {
		t.Fatalf("the leg ran %d times; want 1 (not restarted after its stop)", runs)
	}
	if snap, _ := snapshotFor(sup, "leg"); snap.State != SyncFailed {
		t.Fatalf("leg state = %q; want %q", snap.State, SyncFailed)
	}
	var ec interface{ ExitCode() int }
	if !errors.As(err, &ec) || ec.ExitCode() != sluicecode.ExitRefusal {
		t.Fatalf("the fleet's error does not keep the refusal's exit status: %v", err)
	}
}

// TestStopObservedSourceErrorCells pins both arms of the source-pump branch
// under a stop (the third review, F4 gaps): a connect-phase-marked
// transient (the isRetriableConnectFailure arm) is an undrained stop, exit
// 4; a generic non-retriable error keeps its identity and exit status 1,
// leaves the stop request set (settle does not clear it), and the Run's exit
// still records the stop (the run streamed, and it ended).
func TestStopObservedSourceErrorCells(t *testing.T) {
	t.Run("connect-phase transient: undrained stop", func(t *testing.T) {
		err := stopObservedAttempt(t, connectHint(errors.New("dial tcp 10.0.0.1:3306: connect: connection refused")))
		if !errors.Is(err, ErrStopDrainHardCancelled) {
			t.Fatalf("settle = %v; want the %s failure", err, StopDrainHardCancelledMarker)
		}
	})
	t.Run("generic non-retriable: identity kept, flag kept, stop recorded at exit", func(t *testing.T) {
		generic := errors.New("vstream: unknown keyspace")
		var order []string
		store := &fakeLivenessStore{value: 500}
		app := settleOrderApplier{livenessOnlyApplier: livenessOnlyApplier{store}, order: &order}
		s := &Streamer{Applier: app, sourceErrFn: func() error { return generic }, livenessOpen: func(context.Context) (ir.StreamLivenessStore, func(), error) {
			return store, func() {}, nil
		}}
		s.liveness = &runLiveness{ctx: context.Background()}
		s.armStreamLiveness("st")
		var stop stopSignal
		stop.observed.Store(true)
		err := stopTerminal(&stop, s.phaseSettleDispatch(context.Background(), app, "st", nil, &stop))
		if !errors.Is(err, generic) || errors.Is(err, ErrStopDrainHardCancelled) || !errors.Is(err, ErrStopRequestedRun) {
			t.Fatalf("settle = %v; want the source error kept and marked stop-requested", err)
		}
		var ec interface{ ExitCode() int }
		if errors.As(err, &ec) {
			t.Fatalf("a generic error must exit 1, but it carries exit status %d", ec.ExitCode())
		}
		if len(order) != 0 {
			t.Fatalf("settle wrote %v; want the stop request left set and no stop record from settle", order)
		}
		s.stopStreamLiveness()
		if !store.stopped() {
			t.Fatal("the Run's exit did not record the stop")
		}
	})
}
