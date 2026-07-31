// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// fakeApplierRaiseRecorder is a ChangeApplier (via the embedded fakeApplier)
// that ALSO implements ir.QueryTimeoutRaiseRecorder, keyed by stream_id — the
// sync-side shape the MySQL ChangeApplier provides. A plain *fakeApplier does
// NOT implement the recorder, so it stands in for a non-MySQL/Vitess target.
type fakeApplierRaiseRecorder struct {
	*fakeApplier
	mu    sync.Mutex
	raise map[string]string // streamID → previous (present ⇒ dangling)
}

func newFakeApplierRaiseRecorder() *fakeApplierRaiseRecorder {
	return &fakeApplierRaiseRecorder{fakeApplier: &fakeApplier{}, raise: map[string]string{}}
}

func (f *fakeApplierRaiseRecorder) ReadQueryTimeoutRaise(_ context.Context, streamID string) (previous string, ok bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.raise[streamID]
	return v, ok, nil
}

func (f *fakeApplierRaiseRecorder) RecordQueryTimeoutRaise(_ context.Context, streamID, previous string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.raise[streamID] = previous
	return nil
}

func (f *fakeApplierRaiseRecorder) ClearQueryTimeoutRaise(_ context.Context, streamID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.raise, streamID)
	return nil
}

// compile-time proof the fake really satisfies the recorder (so the
// type-assert in phasePrepareQueryTimeoutRaise resolves to a non-nil recorder).
var _ ir.QueryTimeoutRaiseRecorder = (*fakeApplierRaiseRecorder)(nil)

// TestPhasePrepareQueryTimeoutRaise_DanglingRevertedAndCleared: a dangling raise
// recorded under the stream_id, with a controller present, is reverted and the
// record cleared — the sync-side crash recovery, run on EVERY attempt.
func TestPhasePrepareQueryTimeoutRaise_DanglingRevertedAndCleared(t *testing.T) {
	applier := newFakeApplierRaiseRecorder()
	applier.raise["stream-1"] = "1200"
	ctrl := &fakeController{mu: &sync.Mutex{}, order: new([]string)}
	s := &Streamer{QueryTimeoutController: ctrl}

	if err := s.phasePrepareQueryTimeoutRaise(context.Background(), applier, "stream-1"); err != nil {
		t.Fatalf("phasePrepareQueryTimeoutRaise: %v", err)
	}
	if ctrl.reverts != 1 {
		t.Errorf("reverts = %d; want 1 (the dangling raise reverted)", ctrl.reverts)
	}
	if _, ok := applier.raise["stream-1"]; ok {
		t.Error("record must be cleared after a successful auto-revert")
	}
}

// TestPhasePrepareQueryTimeoutRaise_DanglingNoControllerRefuses: a dangling
// raise with NO controller to revert it is a loud refusal naming the
// credentials, and the record survives so a later credentialled run can revert.
func TestPhasePrepareQueryTimeoutRaise_DanglingNoControllerRefuses(t *testing.T) {
	applier := newFakeApplierRaiseRecorder()
	applier.raise["stream-1"] = "900"
	s := &Streamer{QueryTimeoutController: nil} // credentials absent on this run

	err := s.phasePrepareQueryTimeoutRaise(context.Background(), applier, "stream-1")
	if err == nil || !strings.Contains(err.Error(), "left it raised") {
		t.Fatalf("err = %v; want a loud refusal that the keyspace is still raised", err)
	}
	if _, ok := applier.raise["stream-1"]; !ok {
		t.Error("a refused auto-revert must NOT clear the record")
	}
}

// TestPhasePrepareQueryTimeoutRaise_ArmedNonRecorderRefuses: the raise armed
// against a target whose applier can't record it crash-safely (a non-recorder)
// refuses loudly up front, on warm AND cold attempts alike.
func TestPhasePrepareQueryTimeoutRaise_ArmedNonRecorderRefuses(t *testing.T) {
	s := &Streamer{
		RaiseQueryTimeout:      true,
		QueryTimeoutController: &fakeController{mu: &sync.Mutex{}, order: new([]string)},
	}
	// A plain fakeApplier does NOT implement ir.QueryTimeoutRaiseRecorder.
	err := s.phasePrepareQueryTimeoutRaise(context.Background(), &fakeApplier{}, "stream-1")
	if err == nil || !strings.Contains(err.Error(), "crash-safely") {
		t.Fatalf("err = %v; want a loud refusal that the target cannot record the raise crash-safely", err)
	}
}

// TestPhasePrepareQueryTimeoutRaise_ArmedRecorderNoDanglingIsNoOp: armed against
// a recorder-capable target with nothing dangling is a clean no-op (the actual
// raise happens later, scoped to the cold-start copy).
func TestPhasePrepareQueryTimeoutRaise_ArmedRecorderNoDanglingIsNoOp(t *testing.T) {
	applier := newFakeApplierRaiseRecorder()
	ctrl := &fakeController{mu: &sync.Mutex{}, order: new([]string)}
	s := &Streamer{RaiseQueryTimeout: true, QueryTimeoutController: ctrl}

	if err := s.phasePrepareQueryTimeoutRaise(context.Background(), applier, "stream-1"); err != nil {
		t.Fatalf("phasePrepareQueryTimeoutRaise: %v", err)
	}
	if ctrl.reverts != 0 {
		t.Errorf("reverts = %d; want 0 (nothing dangling)", ctrl.reverts)
	}
}

// TestPhasePrepareQueryTimeoutRaise_UnarmedNonRecorderIsNoOp: an unarmed run
// against a non-recorder target (the common case) does nothing and never
// refuses — the arm check only fires when the operator asked for the raise.
func TestPhasePrepareQueryTimeoutRaise_UnarmedNonRecorderIsNoOp(t *testing.T) {
	s := &Streamer{RaiseQueryTimeout: false, QueryTimeoutController: nil}
	if err := s.phasePrepareQueryTimeoutRaise(context.Background(), &fakeApplier{}, "stream-1"); err != nil {
		t.Fatalf("phasePrepareQueryTimeoutRaise (unarmed) must be a no-op; got %v", err)
	}
}
