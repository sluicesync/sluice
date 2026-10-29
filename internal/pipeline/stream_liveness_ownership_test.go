// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRunLivenessOwnership is the fix-aimed review's HIGH (c421369a): a Run
// records a stop only for a stream it ran. One that never reached its apply
// phase — refused at a door, failed against its source, a fleet duplicate —
// writes nothing over a running stream's record, and puts back only a stop
// its own stamps replaced, by compare-and-set.
func TestRunLivenessOwnership(t *testing.T) {
	const running = 500 // another process's running stamp
	cases := []struct {
		name string
		// start is the row's value before this Run (0 = a recorded stop).
		start int64
		// attempts is how many attempts this Run stamps; between is a stamp
		// or position write another run makes after the first of them.
		attempts int
		between  bool
		armed    bool
		want     string // the exit's write: "", "restore", "restore-refused", "stop"
		stopped  bool   // the row holds the stopped marker at the end
	}{
		{name: "refused start over a running stream writes nothing", start: running, attempts: 1, want: ""},
		{name: "refused start over a running stream, retried, writes nothing", start: running, attempts: 2, want: ""},
		{name: "refused start over a stop puts the stop back", start: 0, attempts: 1, want: "restore", stopped: true},
		{name: "refused start over a stop, retried with only its own stamps, puts it back", start: 0, attempts: 2, want: "restore", stopped: true},
		{name: "refused start over a stop that another run stamped over since leaves it running", start: 0, attempts: 1, between: true, want: "restore-refused"},
		{name: "a retry that stamped over another run's stamp owns nothing", start: 0, attempts: 2, between: true, want: ""},
		{name: "a Run that streamed records the stop", start: running, attempts: 1, armed: true, want: "stop", stopped: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeLivenessStore{value: tc.start}
			app := livenessOnlyApplier{store}
			s := &Streamer{livenessOpen: func(context.Context) (ir.StreamLivenessStore, func(), error) {
				return store, func() {}, nil
			}}
			s.liveness = &runLiveness{ctx: context.Background()}
			for i := 0; i < tc.attempts; i++ {
				if err := s.stampAttemptStart(context.Background(), app, "st"); err != nil {
					t.Fatalf("attempt %d stamp: %v", i+1, err)
				}
				if i == 0 && tc.between {
					store.otherRunStamps()
				}
			}
			if tc.armed {
				s.armStreamLiveness("st")
			}
			before := len(store.snapshot())
			s.stopStreamLiveness()
			exit := strings.Join(store.snapshot()[before:], ",")
			if exit != tc.want {
				t.Fatalf("the exit wrote %q; want %q", exit, tc.want)
			}
			if store.stopped() != tc.stopped {
				t.Fatalf("row stopped = %v; want %v", store.stopped(), tc.stopped)
			}
		})
	}
}

// withPostCopyStopGrace shortens the post-copy check's wait for a test.
// Not for parallel tests: the values are package variables.
func withPostCopyStopGrace(t *testing.T, grace, poll time.Duration) {
	t.Helper()
	prevGrace, prevPoll := postCopyStopGrace, postCopyStopPoll
	postCopyStopGrace, postCopyStopPoll = grace, poll
	t.Cleanup(func() { postCopyStopGrace, postCopyStopPoll = prevGrace, prevPoll })
}

// TestPostCopyCheckWaitsOutARestartLoop is the review's LOW-MEDIUM: a
// supervisor that restarts the stream again and again while a drained add
// copies — each start stamping running, deferring, and putting the stop back
// on its way out — must not make the post-copy check refuse a good copy. Run
// against real Run-exit code: a goroutine plays the restart loop through
// stampAttemptStart and stopStreamLiveness on one shared row, while the check
// reads it. A stream that keeps running (its stamp stays) still refuses when
// the grace runs out.
func TestPostCopyCheckWaitsOutARestartLoop(t *testing.T) {
	withPostCopyStopGrace(t, 2*time.Second, 5*time.Millisecond)
	start := streamPositionMark{token: "pos-1", found: true}
	restartOnce := func(store *fakeLivenessStore) {
		s := &Streamer{livenessOpen: func(context.Context) (ir.StreamLivenessStore, func(), error) {
			return store, func() {}, nil
		}}
		s.liveness = &runLiveness{ctx: context.Background()}
		if err := s.stampAttemptStart(context.Background(), livenessOnlyApplier{store}, "live"); err != nil {
			t.Errorf("restart stamp: %v", err)
		}
		time.Sleep(20 * time.Millisecond) // deferring: INCLUDE-TABLE-ADD-IN-PROGRESS
		s.stopStreamLiveness()
	}

	t.Run("a restart loop that ends leaves the copy good", func(t *testing.T) {
		store := &fakeLivenessStore{}
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < 10; i++ {
				restartOnce(store)
			}
		}()
		time.Sleep(10 * time.Millisecond) // the check lands inside a restart's window
		a := &AddTable{Source: newAddTableSourceEngine("source"), StreamID: "live", TableName: "nt", positionAtStart: start}
		err := a.refuseStreamRanDuringAdd(context.Background(), storePositionApplier{livenessOnlyApplier{store}, "pos-1"})
		<-done
		if err != nil {
			t.Fatalf("a restart loop that put the stop back each time made the check refuse: %v", err)
		}
	})
	t.Run("a stream that keeps running still refuses", func(t *testing.T) {
		store := &fakeLivenessStore{value: 500}
		a := &AddTable{Source: newAddTableSourceEngine("source"), StreamID: "live", TableName: "nt", positionAtStart: start}
		err := a.refuseStreamRanDuringAdd(context.Background(), storePositionApplier{livenessOnlyApplier{store}, "pos-1"})
		if !errors.Is(err, errStreamNotStopped) || !strings.Contains(err.Error(), "recorded that it was running") {
			t.Fatalf("want the running-stream refusal after the grace, got %v", err)
		}
	})
}

// storePositionApplier is a liveness-only applier with a fixed position.
type storePositionApplier struct {
	livenessOnlyApplier
	token string
}

func (a storePositionApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return ir.Position{Token: a.token}, true, nil
}

// seqDriftApplier answers liveness reads from a sequence (the last value
// repeats), over the scope-drift test applier.
type seqDriftApplier struct {
	livenessDriftApplier
	seq   []ir.StreamLiveness
	reads *int
}

func (a seqDriftApplier) ReadStreamLiveness(context.Context, string) (ir.StreamLiveness, error) {
	i := *a.reads
	*a.reads++
	if i >= len(a.seq) {
		i = len(a.seq) - 1
	}
	return a.seq[i], nil
}

// TestRecheckAfterPendingWaitsOutADeferringStart (the second fix-aimed review
// of 6b5898dc, F8): a start that a supervisor makes right after the add
// writes its pending entry stamps running, defers and puts the stop back; the
// post-pending recheck waits that out, as the post-copy check does, instead
// of refusing a good add. A record that stays running still refuses.
func TestRecheckAfterPendingWaitsOutADeferringStart(t *testing.T) {
	withPostCopyStopGrace(t, 300*time.Millisecond, 5*time.Millisecond)
	now := time.Now()
	running := ir.StreamLiveness{Found: true, Recorded: true, AliveAt: now, Now: now}
	stopped := ir.StreamLiveness{Found: true, Recorded: true, Stopped: true, Now: now}
	for _, tc := range []struct {
		name   string
		seq    []ir.StreamLiveness
		refuse bool
	}{
		{name: "running for a few reads, then the stop is back: passes", seq: []ir.StreamLiveness{running, running, running, stopped}},
		{name: "running throughout: refuses after the grace", seq: []ir.StreamLiveness{running}, refuse: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := seqDriftApplier{livenessDriftApplier: livenessDriftApplier{scopeDriftApplier: &scopeDriftApplier{}}, seq: tc.seq, reads: new(int)}
			a := &AddTable{StreamID: "live", TableName: "nt"}
			err := a.recheckStreamStoppedAfterPending(context.Background(), app)
			if tc.refuse != errors.Is(err, errStreamNotStopped) {
				t.Fatalf("recheck = %v; want refused=%v", err, tc.refuse)
			}
		})
	}
}

// TestDrainedStopRecordRetriedAtRunExit (the second fix-aimed review of
// 6b5898dc, F3): when a graceful drain's stop record fails, settle still
// clears the flag (the drain itself completed), and Run's exit tries the
// record once more through a fresh applier; `sync stop --wait` waits a
// bounded moment for exactly that attempt (TestWaitForStopComplete_*).
func TestDrainedStopRecordRetriedAtRunExit(t *testing.T) {
	store := &fakeLivenessStore{value: 500, recordFails: 1}
	app := livenessOnlyApplier{store}
	s := &Streamer{Applier: app, livenessOpen: func(context.Context) (ir.StreamLivenessStore, func(), error) {
		return store, func() {}, nil
	}}
	s.liveness = &runLiveness{ctx: context.Background()}
	s.armStreamLiveness("st")
	var stop stopSignal
	stop.observed.Store(true)
	if err := s.phaseSettleDispatch(context.Background(), app, "st", nil, &stop); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if store.stopped() {
		t.Fatal("RIG: the first stop record was meant to fail")
	}
	s.stopStreamLiveness()
	if !store.stopped() || strings.Join(store.snapshot(), ",") != "stop-failed,stop" {
		t.Fatalf("calls %v, stopped=%v; want the failed record retried once at Run's exit", store.snapshot(), store.stopped())
	}
}

// TestStampAttemptStartFailsClosedOverAStop is the review's MEDIUM: a stamp
// that fails while the record still reads the stopped marker (or cannot be
// read) fails the attempt, rather than streaming with its position writes
// advancing under a record that says stopped. Over a running record, or a
// column a role cannot add, it WARNs and streams, as before.
func TestStampAttemptStartFailsClosedOverAStop(t *testing.T) {
	stampErr := errors.New("connection reset by peer")
	cases := []struct {
		name    string
		store   *fakeLivenessStore
		wantErr bool
	}{
		{name: "over a recorded stop: refused", store: &fakeLivenessStore{value: 0, stampErr: stampErr}, wantErr: true},
		{name: "record unreadable too: refused", store: &fakeLivenessStore{value: 0, stampErr: stampErr, readErr: errors.New("gone")}, wantErr: true},
		{name: "over a running record: streams", store: &fakeLivenessStore{value: 500, stampErr: stampErr}},
		{name: "over a never-stamped record: streams", store: &fakeLivenessStore{null: true, stampErr: stampErr}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Streamer{}
			s.liveness = &runLiveness{ctx: context.Background()}
			err := s.stampAttemptStart(context.Background(), livenessOnlyApplier{tc.store}, "st")
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("want the attempt to stream on, got %v", err)
				}
				return
			}
			if err == nil || !errors.Is(err, stampErr) || !strings.Contains(err.Error(), StreamLivenessUnrecordedMarker) {
				t.Fatalf("want the attempt refused with %s wrapping the stamp error, got %v", StreamLivenessUnrecordedMarker, err)
			}
		})
	}
}
