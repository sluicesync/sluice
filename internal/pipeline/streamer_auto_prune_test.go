// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// --- test doubles -----------------------------------------------------------

// autoPruneFakeApplier is a minimal scriptable [ir.ChangeApplier] whose only
// live method is ReadPosition; the rest panic if the sidecar ever touches them
// (it must not).
type autoPruneFakeApplier struct {
	mu sync.Mutex

	pos       ir.Position
	found     bool
	readErr   error
	readCalls int
}

func (a *autoPruneFakeApplier) ReadPosition(context.Context, string) (ir.Position, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.readCalls++
	return a.pos, a.found, a.readErr
}

func (a *autoPruneFakeApplier) reads() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.readCalls
}

func (a *autoPruneFakeApplier) EnsureControlTable(context.Context) error { return nil }
func (a *autoPruneFakeApplier) ListStreams(context.Context) ([]ir.StreamStatus, error) {
	return nil, nil
}

func (a *autoPruneFakeApplier) Apply(context.Context, string, <-chan ir.Change) error {
	panic("autoPruneFakeApplier.Apply called — the auto-prune sidecar must not stream")
}
func (a *autoPruneFakeApplier) RequestStop(context.Context, string) error { return nil }
func (a *autoPruneFakeApplier) ClearStopRequested(context.Context, string) error {
	return nil
}

// autoPruneFakePruner is a scriptable [ir.ChangeLogPruner] that records every
// call's token + keep and returns configurable (deleted, err).
type autoPruneFakePruner struct {
	mu sync.Mutex

	deleted int64
	err     error

	calls     int
	lastToken string
	lastKeep  int64
	tokenSeen []string
}

func (p *autoPruneFakePruner) PruneConsumedChangeLog(_ context.Context, token string, keep int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.lastToken = token
	p.lastKeep = keep
	p.tokenSeen = append(p.tokenSeen, token)
	return p.deleted, p.err
}

func (p *autoPruneFakePruner) numCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// --- autoPruneGate cadence -------------------------------------------------

// TestAutoPruneGate_AtMostOncePerInterval pins the cadence contract with an
// injected clock: the first call is due, calls WITHIN the interval are skipped,
// and the next call at/after the interval boundary is due again.
func TestAutoPruneGate_AtMostOncePerInterval(t *testing.T) {
	base := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	g := &autoPruneGate{interval: time.Minute}

	if !g.due(base) {
		t.Fatal("first due() must be true (prune on the first tick, not after two intervals)")
	}
	if g.due(base.Add(30 * time.Second)) {
		t.Error("due() within the interval must be false (at most once per interval)")
	}
	if g.due(base.Add(59 * time.Second)) {
		t.Error("due() still within the interval must be false")
	}
	if !g.due(base.Add(60 * time.Second)) {
		t.Error("due() at the interval boundary must be true again")
	}
	// After the boundary re-arm, the very next within-window call skips again.
	if g.due(base.Add(61 * time.Second)) {
		t.Error("due() within the second interval must be false")
	}
	if !g.due(base.Add(120 * time.Second)) {
		t.Error("due() at the second boundary must be true")
	}
}

// --- runAutoPruneTick ------------------------------------------------------

func newDueGate() *autoPruneGate {
	// Zero last ⇒ the first due() is always true.
	return &autoPruneGate{interval: time.Minute}
}

// TestRunAutoPruneTick_PrunesWithDurableToken asserts the tick reads the
// target's durable position and hands its TOKEN (+ keep) to the source pruner.
func TestRunAutoPruneTick_PrunesWithDurableToken(t *testing.T) {
	applier := &autoPruneFakeApplier{
		pos:   ir.Position{Engine: "sqlite-trigger", Token: `{"last_id":42}`},
		found: true,
	}
	pruner := &autoPruneFakePruner{deleted: 7}

	runAutoPruneTick(context.Background(), newDiscardLogger(), applier, pruner, "s1", 1000, newDueGate(), time.Now())

	if pruner.numCalls() != 1 {
		t.Fatalf("pruner calls = %d; want 1", pruner.numCalls())
	}
	if pruner.lastToken != `{"last_id":42}` {
		t.Errorf("pruner token = %q; want the durable position token", pruner.lastToken)
	}
	if pruner.lastKeep != 1000 {
		t.Errorf("pruner keep = %d; want 1000", pruner.lastKeep)
	}
}

// TestRunAutoPruneTick_NoDurablePosition_SkipsPrune asserts that with no durable
// frontier persisted yet (found=false), the tick never prunes — there is no safe
// lower bound.
func TestRunAutoPruneTick_NoDurablePosition_SkipsPrune(t *testing.T) {
	applier := &autoPruneFakeApplier{found: false}
	pruner := &autoPruneFakePruner{}

	runAutoPruneTick(context.Background(), newDiscardLogger(), applier, pruner, "s1", 100, newDueGate(), time.Now())

	if pruner.numCalls() != 0 {
		t.Errorf("pruner calls = %d; want 0 (no durable frontier ⇒ nothing to prune)", pruner.numCalls())
	}
}

// TestRunAutoPruneTick_ReadPositionError_Swallowed asserts a ReadPosition error
// is logged-and-swallowed (no panic, no prune, tick returns normally).
func TestRunAutoPruneTick_ReadPositionError_Swallowed(t *testing.T) {
	applier := &autoPruneFakeApplier{readErr: errors.New("target down")}
	pruner := &autoPruneFakePruner{}

	runAutoPruneTick(context.Background(), newDiscardLogger(), applier, pruner, "s1", 100, newDueGate(), time.Now())

	if pruner.numCalls() != 0 {
		t.Errorf("pruner calls = %d; want 0 (position read failed)", pruner.numCalls())
	}
}

// TestRunAutoPruneTick_PruneError_Swallowed asserts a prune error is swallowed:
// the tick returns normally so the sync is never affected.
func TestRunAutoPruneTick_PruneError_Swallowed(t *testing.T) {
	applier := &autoPruneFakeApplier{
		pos:   ir.Position{Engine: "sqlite-trigger", Token: `{"last_id":5}`},
		found: true,
	}
	pruner := &autoPruneFakePruner{err: errors.New("change-log locked")}

	// Must not panic; the swallow is the assertion.
	runAutoPruneTick(context.Background(), newDiscardLogger(), applier, pruner, "s1", 0, newDueGate(), time.Now())

	if pruner.numCalls() != 1 {
		t.Errorf("pruner calls = %d; want 1 (attempted, then swallowed)", pruner.numCalls())
	}
}

// TestRunAutoPruneTick_GateNotDue_SkipsEntirely asserts that when the gate is
// not due, the tick short-circuits BEFORE touching the target (no ReadPosition,
// no prune) — the cadence bound holds even against a faster driver.
func TestRunAutoPruneTick_GateNotDue_SkipsEntirely(t *testing.T) {
	applier := &autoPruneFakeApplier{
		pos:   ir.Position{Engine: "sqlite-trigger", Token: `{"last_id":42}`},
		found: true,
	}
	pruner := &autoPruneFakePruner{}

	now := time.Now()
	g := &autoPruneGate{interval: time.Minute, last: now} // already pruned "now"
	runAutoPruneTick(context.Background(), newDiscardLogger(), applier, pruner, "s1", 100, g, now.Add(30*time.Second))

	if applier.reads() != 0 {
		t.Errorf("applier reads = %d; want 0 (gate not due ⇒ skip before any target I/O)", applier.reads())
	}
	if pruner.numCalls() != 0 {
		t.Errorf("pruner calls = %d; want 0 (gate not due)", pruner.numCalls())
	}
}

// --- startAutoPruneChangeLog no-op preconditions ---------------------------

// TestStartAutoPruneChangeLog_NoOpWhenDisabled asserts the sidecar does NOT
// spawn (never prunes) when the opt-in flag is off — the safe zero-value
// default for every non-CLI construction.
func TestStartAutoPruneChangeLog_NoOpWhenDisabled(t *testing.T) {
	pruner := &autoPruneFakePruner{}
	s := &Streamer{
		AutoPruneChangeLog: false, // the zero value / default
		AutoPruneInterval:  time.Millisecond,
		changeLogPruner:    pruner,
	}
	applier := &autoPruneFakeApplier{found: true, pos: ir.Position{Token: `{"last_id":1}`}}

	ctx, cancel := context.WithCancel(context.Background())
	s.startAutoPruneChangeLog(ctx, "s1", applier)
	time.Sleep(20 * time.Millisecond)
	cancel()

	if pruner.numCalls() != 0 {
		t.Errorf("pruner calls = %d; want 0 (auto-prune disabled ⇒ no goroutine)", pruner.numCalls())
	}
}

// TestStartAutoPruneChangeLog_NoOpWhenNoPruner asserts the sidecar does NOT
// spawn when the source is not a trigger-CDC engine (nil pruner) even with the
// flag on — a set flag on a vanilla source is a no-op.
func TestStartAutoPruneChangeLog_NoOpWhenNoPruner(t *testing.T) {
	s := &Streamer{
		AutoPruneChangeLog: true,
		AutoPruneInterval:  time.Millisecond,
		changeLogPruner:    nil, // non-trigger source
	}
	applier := &autoPruneFakeApplier{found: true, pos: ir.Position{Token: `{"last_id":1}`}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Must not panic / deref the nil pruner.
	s.startAutoPruneChangeLog(ctx, "s1", applier)
	time.Sleep(10 * time.Millisecond)
	if applier.reads() != 0 {
		t.Errorf("applier reads = %d; want 0 (nil pruner ⇒ no goroutine)", applier.reads())
	}
}

// TestStartAutoPruneChangeLog_PrunesOnCadence asserts the wired sidecar actually
// prunes on its ticker cadence (the opt-in path end-to-end, with fakes).
func TestStartAutoPruneChangeLog_PrunesOnCadence(t *testing.T) {
	pruner := &autoPruneFakePruner{deleted: 3}
	s := &Streamer{
		AutoPruneChangeLog: true,
		AutoPruneInterval:  5 * time.Millisecond,
		AutoPruneKeep:      10,
		changeLogPruner:    pruner,
	}
	applier := &autoPruneFakeApplier{found: true, pos: ir.Position{Token: `{"last_id":99}`}}

	ctx, cancel := context.WithCancel(context.Background())
	s.startAutoPruneChangeLog(ctx, "s1", applier)
	// A few ticks worth of wall-clock; the exact count isn't asserted (timing),
	// only that the cadence fired at least once with the right token/keep.
	time.Sleep(40 * time.Millisecond)
	cancel()

	if pruner.numCalls() == 0 {
		t.Fatal("pruner never called; want at least one prune on the cadence")
	}
	if pruner.lastToken != `{"last_id":99}` || pruner.lastKeep != 10 {
		t.Errorf("pruner called with token=%q keep=%d; want token={last_id:99} keep=10", pruner.lastToken, pruner.lastKeep)
	}
}
