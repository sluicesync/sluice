// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestJudgeStreamStopped is DRAINED-ADD-TABLE-RUNNING-STREAM-UNCHECKED's
// decision table: a drained add passes only on a recorded stop or on no
// control row. A running stamp of any age refuses — an idle running stream
// records nothing new, so its stamp ages exactly like a crashed one's.
func TestJudgeStreamStopped(t *testing.T) {
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	alive := func(age time.Duration) ir.StreamLiveness {
		return ir.StreamLiveness{Found: true, Recorded: true, AliveAt: now.Add(-age), Now: now}
	}
	cases := []struct {
		name    string
		l       ir.StreamLiveness
		readErr error
		refuse  string // "" = passes
	}{
		{name: "no control row: a new stream passes", l: ir.StreamLiveness{}},
		{name: "a recorded stop passes", l: ir.StreamLiveness{Found: true, Recorded: true, Stopped: true, Now: now}},
		{name: "a fresh stamp refuses", l: alive(3 * time.Second), refuse: "has not recorded that it stopped"},
		{name: "an hour-old stamp refuses (a crash, or an idle stream)", l: alive(time.Hour), refuse: "has not recorded that it stopped"},
		{name: "a stamp ahead of the clock refuses", l: alive(-time.Hour), refuse: "has not recorded that it stopped"},
		{name: "a row no recording release stamped refuses", l: ir.StreamLiveness{Found: true, Now: now}, refuse: "no liveness record"},
		{name: "an unreadable record refuses", readErr: errors.New("connection reset by peer"), refuse: "could not read whether"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := judgeStreamStopped("live", tc.l, tc.readErr)
			if tc.refuse == "" {
				if err != nil {
					t.Fatalf("want a pass, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("want a refusal containing %q, got a pass", tc.refuse)
			}
			for _, want := range []string{
				tc.refuse, DrainedAddTableStreamNotStoppedMarker, "sluice sync stop --wait --stream-id live",
				"must not run while the stream applies changes",
			} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("refusal %q does not contain %q", err, want)
				}
			}
			if tc.readErr == nil && !strings.Contains(err.Error(), "`sluice sync start` with the same --stream-id") {
				t.Errorf("refusal %q does not give the crashed stream's remedy (start, then stop)", err)
			}
			if !errors.Is(err, errStreamNotStopped) {
				t.Errorf("refusal does not wrap errStreamNotStopped: %v", err)
			}
			if tc.readErr != nil && !errors.Is(err, tc.readErr) {
				t.Errorf("refusal dropped the read error from its chain: %v", err)
			}
		})
	}
}

// fakeLivenessStore records the liveness calls a Streamer makes.
type fakeLivenessStore struct {
	mu    sync.Mutex
	calls []string // "stamp" / "stop"
}

func (f *fakeLivenessStore) StampStreamAlive(context.Context, string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "stamp")
	return true, nil
}

func (f *fakeLivenessStore) RecordStreamStopped(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "stop")
	return nil
}

func (f *fakeLivenessStore) ReadStreamLiveness(context.Context, string) (ir.StreamLiveness, error) {
	panic("a stream does not read its own liveness")
}

func (f *fakeLivenessStore) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// livenessOnlyApplier is a ChangeApplier whose only real surface is the
// liveness store.
type livenessOnlyApplier struct{ *fakeLivenessStore }

func (livenessOnlyApplier) EnsureControlTable(context.Context) error { return nil }
func (livenessOnlyApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return ir.Position{}, false, nil
}

func (livenessOnlyApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) { return nil, nil }
func (livenessOnlyApplier) Apply(context.Context, string, <-chan ir.Change) error  { return nil }
func (livenessOnlyApplier) RequestStop(context.Context, string) error              { return nil }
func (livenessOnlyApplier) ClearStopRequested(context.Context, string) error       { return nil }

// TestStreamerRecordsTheStopOnItsWayOut: a Run that reached the target records
// the stream's stop as it ends, once; a Run that never reached it, and a dry
// run, record nothing.
func TestStreamerRecordsTheStopOnItsWayOut(t *testing.T) {
	store := &fakeLivenessStore{}
	s := &Streamer{Applier: livenessOnlyApplier{store}}
	s.liveness = &runLiveness{ctx: context.Background()}
	s.stopStreamLiveness() // not armed: never reached the target
	if got := store.snapshot(); len(got) != 0 {
		t.Fatalf("calls %v from a Run that never reached the target; want none", got)
	}
	s.armStreamLiveness("st")
	s.armStreamLiveness("st") // a later attempt
	s.stopStreamLiveness()
	s.stopStreamLiveness()
	if got := store.snapshot(); strings.Join(got, ",") != "stop" {
		t.Fatalf("calls %v; want one stop", got)
	}

	dry := &fakeLivenessStore{}
	s = &Streamer{Applier: livenessOnlyApplier{dry}, DryRun: true}
	s.liveness = &runLiveness{ctx: context.Background()}
	s.armStreamLiveness("st")
	s.stopStreamLiveness()
	if got := dry.snapshot(); len(got) != 0 {
		t.Fatalf("a dry run recorded %v; want nothing", got)
	}
}

// TestStreamerStopRecordingFailureWarns: a stopping Run that cannot open the
// store WARNs with the marker (the add will refuse it until a later start and
// stop records the stop) and does not fail.
func TestStreamerStopRecordingFailureWarns(t *testing.T) {
	logs := captureSlog(t)
	var opens atomic.Int32
	s := &Streamer{livenessOpen: func(context.Context) (ir.StreamLivenessStore, func(), error) {
		opens.Add(1)
		return nil, func() {}, errors.New("too many connections")
	}}
	s.liveness = &runLiveness{ctx: context.Background()}
	s.armStreamLiveness("st")
	s.stopStreamLiveness()
	if opens.Load() != 1 || !strings.Contains(logs.String(), StreamLivenessUnrecordedMarker) || !strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("want one open and a WARN carrying %s, got %d opens:\n%s", StreamLivenessUnrecordedMarker, opens.Load(), logs.String())
	}
}

// settleOrderApplier logs the order of the stopped stamp and the stop-flag
// clear on a graceful drain.
type settleOrderApplier struct {
	livenessOnlyApplier
	order *[]string
}

func (a settleOrderApplier) RecordStreamStopped(context.Context, string) error {
	*a.order = append(*a.order, "stopped")
	return nil
}

func (a settleOrderApplier) ClearStopRequested(context.Context, string) error {
	*a.order = append(*a.order, "clear")
	return nil
}

// TestSettleRecordsTheStopBeforeClearingTheFlag: `sync stop --wait` returns
// on the flag's clear, so the stopped marker must already be down — an add
// run right after it would otherwise read no stop. Run's own record on the
// way out then adds nothing.
func TestSettleRecordsTheStopBeforeClearingTheFlag(t *testing.T) {
	var order []string
	app := settleOrderApplier{livenessOnlyApplier: livenessOnlyApplier{&fakeLivenessStore{}}, order: &order}
	var stop stopSignal
	stop.observed.Store(true)
	s := &Streamer{Applier: app}
	s.liveness = &runLiveness{ctx: context.Background()}
	s.armStreamLiveness("st")
	if err := s.phaseSettleDispatch(context.Background(), app, "st", nil, &stop); err != nil {
		t.Fatalf("settle: %v", err)
	}
	s.stopStreamLiveness()
	if strings.Join(order, ",") != "stopped,clear" {
		t.Fatalf("order %v; want [stopped clear] (one marker, before the clear)", order)
	}
}

// livenessDriftApplier is the scope-door test applier with a liveness record.
type livenessDriftApplier struct {
	*scopeDriftApplier
	liveness ir.StreamLiveness
}

func (a livenessDriftApplier) ReadStreamLiveness(context.Context, string) (ir.StreamLiveness, error) {
	return a.liveness, nil
}

func (livenessDriftApplier) StampStreamAlive(context.Context, string) (bool, error) {
	return true, nil
}
func (livenessDriftApplier) RecordStreamStopped(context.Context, string) error { return nil }

// TestAddTableRecheckAfterPending is the race half: a stream that started
// between the add's first read and its pending entry has stamped by the
// recheck, which refuses and takes the entry back out; a stream still
// stopped passes and the entry stays.
func TestAddTableRecheckAfterPending(t *testing.T) {
	now := time.Now()
	t.Run("a stream that started meanwhile refuses and the entry is removed", func(t *testing.T) {
		app := livenessDriftApplier{
			scopeDriftApplier: &scopeDriftApplier{},
			liveness:          ir.StreamLiveness{Found: true, Recorded: true, AliveAt: now, Now: now},
		}
		a := &AddTable{StreamID: "live", TableName: "nt"}
		err := a.recheckStreamStoppedAfterPending(context.Background(), app)
		if !errors.Is(err, errStreamNotStopped) {
			t.Fatalf("want the running-stream refusal, got %v", err)
		}
		if strings.Join(app.writes, " ") != "clear=nt" {
			t.Fatalf("writes %v; want the pending entry cleared", app.writes)
		}
	})
	t.Run("a stream still stopped passes and the entry stays", func(t *testing.T) {
		app := livenessDriftApplier{
			scopeDriftApplier: &scopeDriftApplier{},
			liveness:          ir.StreamLiveness{Found: true, Recorded: true, Stopped: true, Now: now},
		}
		a := &AddTable{StreamID: "live", TableName: "nt"}
		if err := a.recheckStreamStoppedAfterPending(context.Background(), app); err != nil {
			t.Fatalf("got %v", err)
		}
		if len(app.writes) != 0 {
			t.Fatalf("writes %v; want none", app.writes)
		}
	})
	// F2: over an earlier entry (a dead add's, the evidence of a partial
	// copy) the refused add's entry is aged out under its own token, never
	// removed by name.
	t.Run("over an earlier entry, the entry is aged, not removed", func(t *testing.T) {
		app := livenessDriftApplier{
			scopeDriftApplier: &scopeDriftApplier{},
			liveness:          ir.StreamLiveness{Found: true, Recorded: true, AliveAt: now, Now: now},
		}
		a := &AddTable{StreamID: "live", TableName: "nt", pendingToken: "tok", pendingPredecessor: true}
		if err := a.recheckStreamStoppedAfterPending(context.Background(), app); !errors.Is(err, errStreamNotStopped) {
			t.Fatalf("want the running-stream refusal, got %v", err)
		}
		if strings.Join(app.writes, " ") != "touch=nt stopped" {
			t.Fatalf("writes %v; want the entry aged out (touch=nt stopped), not cleared", app.writes)
		}
	})
}

// pendingRecordingApplier is the scope-door test applier whose pending
// record write succeeds.
type pendingRecordingApplier struct{ *scopeDriftApplier }

func (pendingRecordingApplier) RecordPendingAddedTable(context.Context, string, string, string) error {
	return nil
}

// TestAddTableNotesAPendingPredecessor (F2): the add notes, before writing
// its pending entry, whether one for the table was already there — or could
// not tell, which counts as there — so a refusal ages rather than removes it.
func TestAddTableNotesAPendingPredecessor(t *testing.T) {
	for name, tc := range map[string]struct {
		pending []ir.PendingAddedTable
		err     error
		want    bool
	}{
		"an entry for the table":          {pending: []ir.PendingAddedTable{{Table: "nt"}}, want: true},
		"entries for other tables only":   {pending: []ir.PendingAddedTable{{Table: "other"}}},
		"nothing pending":                 {},
		"unreadable: counts as one there": {err: errors.New("connection reset"), want: true},
	} {
		t.Run(name, func(t *testing.T) {
			app := pendingRecordingApplier{&scopeDriftApplier{pending: tc.pending, pendErr: tc.err}}
			a := &AddTable{StreamID: "live", TableName: "nt", Target: nonScoperEngine{}}
			if err := a.recordPendingAdd(context.Background(), app); err != nil {
				t.Fatal(err)
			}
			if a.pendingPredecessor != tc.want {
				t.Fatalf("pendingPredecessor = %v; want %v", a.pendingPredecessor, tc.want)
			}
		})
	}
}

// positionApplier is a fakeApplier whose persisted position the test sets.
type positionApplier struct {
	*fakeApplier
	pos    ir.Position
	found  bool
	posErr error
}

func (a positionApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	return a.pos, a.found, a.posErr
}

// TestAddTableRefusesAStreamThatRanDuringTheAdd is the drained add's last
// check, after the copy: the record must still show the stop and the stream's
// position must be the one the add started from. Only a position write moves
// the position, so a stream that applied during the add and then stopped
// cleanly — leaving the stopped marker — is caught by the position.
func TestAddTableRefusesAStreamThatRanDuringTheAdd(t *testing.T) {
	now := time.Now()
	stopped := ir.StreamLiveness{Found: true, Recorded: true, Stopped: true, Now: now}
	running := ir.StreamLiveness{Found: true, Recorded: true, AliveAt: now, Now: now}
	start := streamPositionMark{token: "pos-1", found: true}
	cases := []struct {
		name   string
		end    ir.StreamLiveness
		pos    ir.Position
		found  bool
		endErr error
		posErr error
		refuse string // "" = passes
	}{
		{name: "stopped throughout, position unchanged", end: stopped, pos: ir.Position{Token: "pos-1"}, found: true},
		{name: "applied, then stopped cleanly: position moved", end: stopped, pos: ir.Position{Token: "pos-2"}, found: true, refuse: "position moved"},
		{name: "running at the end, position unchanged", end: running, pos: ir.Position{Token: "pos-1"}, found: true, refuse: "recorded that it was running"},
		{name: "running and moved", end: running, pos: ir.Position{Token: "pos-2"}, found: true, refuse: "position moved"},
		{name: "position gone", end: stopped, refuse: "position moved"},
		{name: "liveness unreadable", endErr: errors.New("connection reset"), refuse: "could not read whether"},
		{name: "position unreadable", end: stopped, posErr: errors.New("connection reset"), refuse: "could not read whether"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := positionApplier{fakeApplier: &fakeApplier{liveness: tc.end, livenessErr: tc.endErr}, pos: tc.pos, found: tc.found, posErr: tc.posErr}
			a := &AddTable{Source: newAddTableSourceEngine("source"), StreamID: "live", TableName: "nt", positionAtStart: start}
			err := a.refuseStreamRanDuringAdd(context.Background(), app)
			if tc.refuse == "" {
				if err != nil {
					t.Fatalf("want a pass, got %v", err)
				}
				return
			}
			if !errors.Is(err, errStreamNotStopped) || !strings.Contains(err.Error(), tc.refuse) ||
				!strings.Contains(err.Error(), "NOT handed to the stream") {
				t.Fatalf("want a refusal containing %q, got %v", tc.refuse, err)
			}
		})
	}
	// On a publication source the table joined the stream's scope before the
	// copy: the remedy says so and names --abandon instead.
	a := &AddTable{Source: newAddTableSourceEngineWithPub("source"), StreamID: "live", TableName: "nt", positionAtStart: start}
	err := a.refuseStreamRanDuringAdd(context.Background(), positionApplier{fakeApplier: &fakeApplier{liveness: running}, pos: ir.Position{Token: "pos-1"}, found: true})
	if err == nil || strings.Contains(err.Error(), "NOT handed") || !strings.Contains(err.Error(), "--abandon nt") {
		t.Fatalf("publication-source refusal %v; want the already-in-scope remedy naming --abandon", err)
	}
}

// TestScopeDriftDoor_OutOfScopeAddInProgress is the revival half: a stream
// restarting while a drained add of a table OUTSIDE its scope still runs
// (a supervisor restart, a fleet reload) is deferred like an in-scope add in
// progress — the table joins its scope only when the add finishes, so it
// would drop every change to the table until then. A dead add's stale entry
// does not wedge it, and an abandoned one is still cleared.
func TestScopeDriftDoor_OutOfScopeAddInProgress(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	src, tgt := binlogPair([]string{"kd", "nt"}, []string{"kd"})
	withPending := func(p ir.PendingAddedTable) *scopeDriftApplier {
		app := recorded([]string{"kd"})
		app.pending = []ir.PendingAddedTable{p}
		return app
	}
	t.Run("fresh, out of scope: deferred, restartably", func(t *testing.T) {
		p := ir.PendingAddedTable{Table: "nt", Token: "t", StartedAt: now.Add(-time.Minute), HeartbeatAt: now.Add(-5 * time.Second)}
		err := driftStreamer(t, src, tgt, "kd").phaseRefuseIncludeTableScopeDrift(ctx, withPending(p), "st")
		if !errors.Is(err, ir.ErrIncludeTableAddInProgress) || errors.Is(err, ir.ErrIncludeTableScopeDrift) {
			t.Fatalf("want only INCLUDE-TABLE-ADD-IN-PROGRESS, got %v", err)
		}
		if !strings.Contains(err.Error(), "nt (heartbeat") {
			t.Fatalf("the deferral does not name the table: %v", err)
		}
		if refusalARestartRepeats(err) != nil {
			t.Fatal("the deferral is in the do-not-restart roster; the stream would stay down after the add")
		}
	})
	t.Run("stale, out of scope: not judged (the dead add does not wedge the stream)", func(t *testing.T) {
		p := ir.PendingAddedTable{Table: "nt", Token: "t", StartedAt: now.Add(-time.Hour), HeartbeatAt: now.Add(-ir.PendingAddStaleAfter - time.Second)}
		requireNoDrift(t, driftStreamer(t, src, tgt, "kd").phaseRefuseIncludeTableScopeDrift(ctx, withPending(p), "st"))
	})
	t.Run("abandoned, out of scope: cleared", func(t *testing.T) {
		app := withPending(ir.PendingAddedTable{Table: "nt", Token: ir.PendingAddAbandonedToken, StartedAt: now.Add(-time.Hour), HeartbeatAt: time.Unix(0, 0)})
		requireNoDrift(t, driftStreamer(t, src, tgt, "kd").phaseRefuseIncludeTableScopeDrift(ctx, app, "st"))
		if strings.Join(app.writes, " ") != "clear=nt" {
			t.Fatalf("writes %v; want the abandoned entry cleared", app.writes)
		}
	})
}
