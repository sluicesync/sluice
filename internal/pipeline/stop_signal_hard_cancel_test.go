// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/sluicecode"
)

// TestPollStopSignal_HardCancelIsRecordedBeforeCancelApply: the watchdog
// marks the stop hard-cancelled before it cancels the apply, so a settle
// that runs because of that cancel always sees the mark.
func TestPollStopSignal_HardCancelIsRecordedBeforeCancelApply(t *testing.T) {
	withFastPollInterval(t)
	withFastDrainTimeout(t, 50*time.Millisecond)

	reader := &fakeStopFlagReader{}
	reader.setStopRequested(true)
	pollCtx, cancelPoll := context.WithCancel(context.Background())
	defer cancelPoll()

	var state stopSignal
	markedAtCancel := make(chan bool, 1)
	cancelApply := func() { markedAtCancel <- state.hardCancelled.Load() }
	go pollStopSignal(pollCtx, reader, "stream-1", func() {}, cancelApply, &state)

	select {
	case marked := <-markedAtCancel:
		if !marked {
			t.Fatal("cancelApply fired before the stop was marked hard-cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the watchdog never hard-cancelled the apply")
	}
	if !state.observed.Load() {
		t.Error("a hard-cancelled stop was not marked observed")
	}
}

// TestSupervisor_HardCancelledStopNotRestarted: a fleet leg whose stop was
// hard-cancelled is neither restarted (the restart would clear the stop
// request and keep streaming) nor reported as a clean stop; with the policy
// set to restart forever, one run proves the first half.
func TestSupervisor_HardCancelledStopNotRestarted(t *testing.T) {
	var calls atomic.Int64
	leg := SupervisedSync{
		ID: "leg",
		Runner: runnerFunc(func(context.Context) error {
			calls.Add(1)
			return fmt.Errorf("pipeline: %w: stream %q was asked to stop", ErrStopDrainHardCancelled, "leg")
		}),
	}
	sup := NewSupervisor([]SupervisedSync{leg}, fastTestPolicy(0))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sup.Run(ctx); !errors.Is(err, ErrStopDrainHardCancelled) {
		t.Fatalf("Supervisor.Run = %v; want the leg's %s failure", err, StopDrainHardCancelledMarker)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("the hard-cancelled leg ran %d times; want 1 (not restarted)", got)
	}
	if snap, _ := snapshotFor(sup, "leg"); snap.State != SyncFailed {
		t.Fatalf("leg state = %q; want %q (it did not drain)", snap.State, SyncFailed)
	}
}

// TestSettle_StopEndingRetriableIsNotGraceful is the watchdog case's sibling
// (the fix-aimed review of c421369a, MEDIUM): a `sync stop` drain that ends
// in an error the retry loop would retry — an --apply-exec-timeout shorter
// than the watchdog, a transient fault at the last batch, a source pump
// error — used to be returned for the retry, whose start cleared the stop
// flag and kept streaming. With the stop observed it now settles like a
// hard cancel; without a stop it is still returned for the retry (control).
func TestSettle_StopEndingRetriableIsNotGraceful(t *testing.T) {
	retriable := fakeRetriableErr{msg: "context deadline exceeded (--apply-exec-timeout)"}
	cases := []struct {
		name        string
		observed    bool
		dispatchErr error
		sourceErr   error
		undrained   bool
	}{
		{name: "stop observed, apply ends retriable", observed: true, dispatchErr: retriable, undrained: true},
		{name: "stop observed, source pump error", observed: true, sourceErr: errors.New("read: connection reset"), undrained: true},
		{name: "no stop, apply ends retriable (retried as before)", dispatchErr: retriable},
		{name: "no stop, source pump error (retried as before)", sourceErr: errors.New("read: connection reset")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var order []string
			store := &fakeLivenessStore{}
			app := settleOrderApplier{livenessOnlyApplier: livenessOnlyApplier{store}, order: &order}
			s := &Streamer{Applier: app}
			if tc.sourceErr != nil {
				s.sourceErrFn = func() error { return tc.sourceErr }
			}
			s.liveness = &runLiveness{ctx: context.Background()}
			s.armStreamLiveness("st")
			var stop stopSignal
			stop.observed.Store(tc.observed)
			err := s.phaseSettleDispatch(context.Background(), app, "st", tc.dispatchErr, &stop)
			s.stopStreamLiveness()
			if !tc.undrained {
				if errors.Is(err, ErrStopDrainHardCancelled) || err == nil {
					t.Fatalf("without a stop the error must go back to the retry loop unchanged, got %v", err)
				}
				return
			}
			if !errors.Is(err, ErrStopDrainHardCancelled) || !strings.Contains(err.Error(), "did not drain") {
				t.Fatalf("settle = %v; want the %s failure", err, StopDrainHardCancelledMarker)
			}
			if _, r := classifyRetriable(err); r || isRetriableConnectFailure(err) {
				t.Fatalf("the undrained-stop failure is retriable: %v", err)
			}
			if len(order) != 0 {
				t.Fatalf("settle or Run's exit wrote %v; want nothing", order)
			}
		})
	}
}

// TestStopNotDrainedExitStatus: a stop that did not drain exits
// sluicecode.ExitStopNotDrained (4), so a service manager can be told not to
// restart it (RestartPreventExitStatus=4), including when an owed
// added-column backfill joins it at the attempt's end (the review's LOW: the
// backfill settle used to replace the error and lose its identity).
func TestStopNotDrainedExitStatus(t *testing.T) {
	s := &Streamer{}
	err := s.settleUndrainedStop(context.Background(), "st", nil, stopCutOffByWatchdog)
	type exitCoder interface{ ExitCode() int }
	for name, e := range map[string]error{
		"alone":             err,
		"wrapped":           fmt.Errorf("pipeline: run: %w", err),
		"joined with a gap": errors.Join(&addedColumnBackfillIncompleteError{runErr: err}, err),
	} {
		var ec exitCoder
		if !errors.As(e, &ec) || ec.ExitCode() != sluicecode.ExitStopNotDrained {
			t.Errorf("%s: exit status not %d: %v", name, sluicecode.ExitStopNotDrained, e)
		}
		if !errors.Is(e, ErrStopDrainHardCancelled) {
			t.Errorf("%s: lost ErrStopDrainHardCancelled: %v", name, e)
		}
	}
}

// TestSettle_HardCancelledDrainIsNotAGracefulStop drives the real watchdog
// into the real settle path: the stop is observed, the apply ignores the
// drain until the watchdog cancels its context, and settle must then keep
// stop_requested_at, record no stop (neither itself nor Run on its way out),
// and fail the attempt with ErrStopDrainHardCancelled — non-retriable even
// when the cut-off apply returned a retriable error, since a retry would
// clear the stop flag at its start and keep streaming. The graceful cell is
// the control: the same rig with an apply that drains records the stop, then
// clears the flag.
func TestSettle_HardCancelledDrainIsNotAGracefulStop(t *testing.T) {
	cases := []struct {
		name     string
		drains   bool
		applyErr func(ctxErr error) error
	}{
		{name: "bare cancel", applyErr: func(ctxErr error) error { return ctxErr }},
		{name: "retriable error from the cut-off statement", applyErr: func(ctxErr error) error {
			return fakeRetriableErr{msg: "write: connection reset after " + ctxErr.Error()}
		}},
		{name: "graceful drain (control)", drains: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			withFastPollInterval(t)
			withFastDrainTimeout(t, 50*time.Millisecond)

			reader := &fakeStopFlagReader{}
			reader.setStopRequested(true)
			streamCtx, cancelStream := context.WithCancel(context.Background())
			defer cancelStream()
			applyCtx, cancelApply := context.WithCancel(context.Background())
			defer cancelApply()

			var state stopSignal
			go pollStopSignal(applyCtx, reader, "st", cancelStream, cancelApply, &state)
			// The apply: a wedged one notices only its own context's cancel;
			// a draining one returns as soon as the stream is cancelled.
			var dispatchErr error
			if tc.drains {
				<-streamCtx.Done()
			} else {
				select {
				case <-applyCtx.Done():
					dispatchErr = tc.applyErr(applyCtx.Err())
				case <-time.After(5 * time.Second):
					t.Fatal("the watchdog never cancelled the wedged apply")
				}
			}

			var order []string
			store := &fakeLivenessStore{}
			app := settleOrderApplier{livenessOnlyApplier: livenessOnlyApplier{store}, order: &order}
			s := &Streamer{Applier: app}
			s.liveness = &runLiveness{ctx: context.Background()}
			s.armStreamLiveness("st")
			err := s.phaseSettleDispatch(context.Background(), app, "st", dispatchErr, &state)
			cancelApply() // the attempt's deferred cancel, after settle
			s.stopStreamLiveness()

			if tc.drains {
				if err != nil || strings.Join(order, ",") != "stopped,clear" {
					t.Fatalf("graceful drain: err %v, order %v; want nil and [stopped clear]", err, order)
				}
				return
			}
			if !errors.Is(err, ErrStopDrainHardCancelled) || !strings.Contains(err.Error(), "was not graceful") {
				t.Fatalf("settle = %v; want the %s failure", err, StopDrainHardCancelledMarker)
			}
			if _, retriable := classifyRetriable(err); retriable || isRetriableConnectFailure(err) {
				t.Fatalf("the hard-cancel failure is retriable, so the retry loop would restart the stream: %v", err)
			}
			// order logs the applier's stop record and flag clear, from settle
			// and from Run's exit record alike (both go through app).
			if len(order) != 0 {
				t.Fatalf("settle or Run's exit wrote %v; want nothing (no stop recorded, stop_requested_at kept)", order)
			}
			if calls := store.snapshot(); len(calls) != 0 {
				t.Fatalf("liveness writes %v; want none, at settle or at Run's exit", calls)
			}
		})
	}
}
