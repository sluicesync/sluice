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

// autoPruneFakePruner is a scriptable trigger-CDC source reader: it implements
// BOTH the base [ir.ChangeLogPruner] and the item-115
// [ir.ChangeLogConsumerRegistry] companion, exactly as the three real trigger
// engines do, and records every call so the tests can assert which surface the
// sidecar reached.
type autoPruneFakePruner struct {
	mu sync.Mutex

	deleted     int64
	err         error
	registerErr error

	// unscopedCalls counts the BASE (single-consumer) prune. It must stay at 0
	// on every sidecar path — the item-115 sidecar prunes only through the
	// registry.
	unscopedCalls int

	calls          int
	lastToken      string
	lastKeep       int64
	lastConsumerID string

	registrations   int
	registeredToken []string
	registeredIDs   []string
}

func (p *autoPruneFakePruner) PruneConsumedChangeLog(_ context.Context, token string, keep int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unscopedCalls++
	p.lastToken = token
	p.lastKeep = keep
	return p.deleted, p.err
}

func (p *autoPruneFakePruner) RegisterChangeLogConsumer(_ context.Context, consumerID, token string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.registrations++
	p.registeredIDs = append(p.registeredIDs, consumerID)
	p.registeredToken = append(p.registeredToken, token)
	return p.registerErr
}

func (p *autoPruneFakePruner) PruneConsumedChangeLogToRegisteredMin(
	_ context.Context, consumerID, token string, keep int64,
) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.lastToken = token
	p.lastKeep = keep
	p.lastConsumerID = consumerID
	return p.deleted, p.err
}

func (p *autoPruneFakePruner) numCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *autoPruneFakePruner) numUnscopedCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.unscopedCalls
}

func (p *autoPruneFakePruner) numRegistrations() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.registrations
}

func (p *autoPruneFakePruner) registrationTokens() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.registeredToken...)
}

// lastCall returns the most recent (consumerID, token, keep) under the lock. The
// sidecar goroutine writes them, so assertions must read them through this
// accessor — a bare field read races the still-running sidecar (ctx cancel does
// not wait for it to exit).
func (p *autoPruneFakePruner) lastCall() (consumerID, token string, keep int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastConsumerID, p.lastToken, p.lastKeep
}

// autoPruneBasePruner implements ONLY the base [ir.ChangeLogPruner] — the shape
// of a trigger engine that has not been migrated to the item-115 companion. It
// panics if it is ever pruned: the sidecar must fail CLOSED, not fall back.
type autoPruneBasePruner struct{}

func (autoPruneBasePruner) PruneConsumedChangeLog(context.Context, string, int64) (int64, error) {
	panic("PruneConsumedChangeLog called — an engine without the consumer registry must NOT be auto-pruned")
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

// TestRunAutoPruneTick_PrunesThroughTheConsumerRegistry asserts the tick reads
// the target's durable position and hands its TOKEN (+ this stream's consumer id
// + keep) to the source's REGISTRY surface — never to the base single-consumer
// pruner, whose cut is the item-115 defect.
func TestRunAutoPruneTick_PrunesThroughTheConsumerRegistry(t *testing.T) {
	applier := &autoPruneFakeApplier{
		pos:   ir.Position{Engine: "sqlite-trigger", Token: `{"last_id":42}`},
		found: true,
	}
	pruner := &autoPruneFakePruner{deleted: 7}

	runAutoPruneTick(context.Background(), newDiscardLogger(), applier, pruner, "s1", "s1 -> pg://h/db", 1000, newDueGate(), time.Now())

	if pruner.numCalls() != 1 {
		t.Fatalf("registry prune calls = %d; want 1", pruner.numCalls())
	}
	if pruner.numUnscopedCalls() != 0 {
		t.Errorf("base PruneConsumedChangeLog calls = %d; want 0 — the sidecar must never take the "+
			"single-consumer cut (roadmap item 115)", pruner.numUnscopedCalls())
	}
	gotID, gotToken, gotKeep := pruner.lastCall()
	if gotToken != `{"last_id":42}` {
		t.Errorf("prune token = %q; want the durable position token", gotToken)
	}
	if gotKeep != 1000 {
		t.Errorf("prune keep = %d; want 1000", gotKeep)
	}
	if gotID != "s1 -> pg://h/db" {
		t.Errorf("prune consumer id = %q; want the composed consumer id", gotID)
	}
}

// TestRunAutoPruneTick_NoDurablePosition_SkipsPrune asserts that with no durable
// frontier persisted yet (found=false), the tick never prunes — there is no safe
// lower bound.
func TestRunAutoPruneTick_NoDurablePosition_SkipsPrune(t *testing.T) {
	applier := &autoPruneFakeApplier{found: false}
	pruner := &autoPruneFakePruner{}

	runAutoPruneTick(context.Background(), newDiscardLogger(), applier, pruner, "s1", "c1", 100, newDueGate(), time.Now())

	if pruner.numCalls() != 0 {
		t.Errorf("pruner calls = %d; want 0 (no durable frontier ⇒ nothing to prune)", pruner.numCalls())
	}
}

// TestRunAutoPruneTick_ReadPositionError_Swallowed asserts a ReadPosition error
// is logged-and-swallowed (no panic, no prune, tick returns normally).
func TestRunAutoPruneTick_ReadPositionError_Swallowed(t *testing.T) {
	applier := &autoPruneFakeApplier{readErr: errors.New("target down")}
	pruner := &autoPruneFakePruner{}

	runAutoPruneTick(context.Background(), newDiscardLogger(), applier, pruner, "s1", "c1", 100, newDueGate(), time.Now())

	if pruner.numCalls() != 0 {
		t.Errorf("pruner calls = %d; want 0 (position read failed)", pruner.numCalls())
	}
}

// TestRunAutoPruneTick_PruneError_Swallowed asserts a prune error — including
// the item-115 REFUSALS (empty registry, unregistered stream, un-migrated
// change log) — is swallowed: the tick returns normally so the sync is never
// affected.
func TestRunAutoPruneTick_PruneError_Swallowed(t *testing.T) {
	applier := &autoPruneFakeApplier{
		pos:   ir.Position{Engine: "sqlite-trigger", Token: `{"last_id":5}`},
		found: true,
	}
	pruner := &autoPruneFakePruner{err: errors.New("consumer registry is EMPTY — refusing to prune")}

	// Must not panic; the swallow is the assertion.
	runAutoPruneTick(context.Background(), newDiscardLogger(), applier, pruner, "s1", "c1", 0, newDueGate(), time.Now())

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
	runAutoPruneTick(context.Background(), newDiscardLogger(), applier, pruner, "s1", "c1", 100, g, now.Add(30*time.Second))

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
		Target:             stubEngine{},
		AutoPruneChangeLog: false, // the zero value / default
		AutoPruneInterval:  time.Millisecond,
		changeLogPruner:    pruner,
		changeLogConsumers: pruner,
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
		Target:             stubEngine{},
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

// TestStartAutoPruneChangeLog_FailsClosedWithoutTheConsumerRegistry is the
// item-115 FAIL-CLOSED gate, and the one that would fail if someone "fixed" a
// missing companion by falling back to the base pruner.
//
// The source here implements [ir.ChangeLogPruner] and NOT
// [ir.ChangeLogConsumerRegistry] — a trigger engine that has not been migrated.
// With the flag on, the sidecar must spawn NOTHING: no goroutine, no position
// read, and above all no unscoped prune (the fake panics if pruned).
func TestStartAutoPruneChangeLog_FailsClosedWithoutTheConsumerRegistry(t *testing.T) {
	s := &Streamer{
		Target:             stubEngine{},
		AutoPruneChangeLog: true,
		AutoPruneInterval:  time.Millisecond,
		changeLogPruner:    autoPruneBasePruner{},
		changeLogConsumers: nil, // the companion this engine never implemented
	}
	applier := &autoPruneFakeApplier{found: true, pos: ir.Position{Token: `{"last_id":1}`}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startAutoPruneChangeLog(ctx, "s1", applier)
	time.Sleep(20 * time.Millisecond)

	if applier.reads() != 0 {
		t.Errorf("applier reads = %d; want 0 — an engine without the consumer registry must not be pruned "+
			"at all (fail closed), not pruned unscoped", applier.reads())
	}
}

// TestStartAutoPruneChangeLog_PrunesOnCadence asserts the wired sidecar actually
// prunes on its ticker cadence (the opt-in path end-to-end, with fakes).
func TestStartAutoPruneChangeLog_PrunesOnCadence(t *testing.T) {
	pruner := &autoPruneFakePruner{deleted: 3}
	s := &Streamer{
		Target:             stubEngine{},
		TargetDSN:          "postgres://u:p@host:5432/db",
		AutoPruneChangeLog: true,
		AutoPruneInterval:  5 * time.Millisecond,
		AutoPruneKeep:      10,
		changeLogPruner:    pruner,
		changeLogConsumers: pruner,
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
	gotID, lastToken, lastKeep := pruner.lastCall()
	if lastToken != `{"last_id":99}` || lastKeep != 10 {
		t.Errorf("pruner called with token=%q keep=%d; want token={last_id:99} keep=10", lastToken, lastKeep)
	}
	if !strings.HasPrefix(gotID, "s1 -> stub://host:5432/db") {
		t.Errorf("consumer id = %q; want it to carry the stream id and the redacted target locator", gotID)
	}
}

// --- the consumer registry (roadmap item 115) -------------------------------

// TestChangeLogConsumerID_IsUniquePerTargetAndCredentialFree pins the identity
// the registry keys on. Two syncs off ONE source into two databases on the SAME
// server share the default stream id (the auto-generated locator drops the
// database name), so keying the registry on the stream id alone would merge them
// into one row and let the faster one prune past the slower — the exact defect.
func TestChangeLogConsumerID_IsUniquePerTargetAndCredentialFree(t *testing.T) {
	a := ChangeLogConsumerID("sync", "postgres", "postgres://u:secret@db.host:5432/alpha?sslmode=require")
	b := ChangeLogConsumerID("sync", "postgres", "postgres://u:secret@db.host:5432/beta?sslmode=require")

	if a == b {
		t.Fatalf("two targets on one server produced the SAME consumer id %q — they would share one registry "+
			"row and prune past each other", a)
	}
	for _, id := range []string{a, b} {
		if strings.Contains(id, "secret") {
			t.Errorf("consumer id %q carries the DSN password; the registry is written to the SOURCE database", id)
		}
		if !strings.HasPrefix(id, "sync -> postgres://db.host:5432/") {
			t.Errorf("consumer id = %q; want the stream id + redacted target locator", id)
		}
	}
	// Same stream, same target ⇒ the SAME row across restarts (a new id per
	// restart would leave a stale row holding the prune back forever).
	again := ChangeLogConsumerID("sync", "postgres", "postgres://u:secret@db.host:5432/alpha?sslmode=require")
	if again != a {
		t.Errorf("consumer id is not stable across restarts: %q vs %q", again, a)
	}
}

// TestStartChangeLogConsumerRegistration_RunsWithoutTheAutoPruneFlag is the
// asymmetry gate. The sync that gets its rows deleted is typically the one
// WITHOUT --auto-prune-change-log, so registration must not be gated on it: a
// peer that never registers is a peer the pruner cannot see.
func TestStartChangeLogConsumerRegistration_RunsWithoutTheAutoPruneFlag(t *testing.T) {
	pruner := &autoPruneFakePruner{}
	s := &Streamer{
		Target:             stubEngine{},
		TargetDSN:          "postgres://u:p@host:5432/db",
		AutoPruneChangeLog: false, // this stream never prunes anything
		changeLogPruner:    pruner,
		changeLogConsumers: pruner,
	}
	applier := &autoPruneFakeApplier{found: true, pos: ir.Position{Token: `{"last_id":12}`}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startChangeLogConsumerRegistration(ctx, "slow", applier)

	if pruner.numRegistrations() != 1 {
		t.Fatalf("registrations = %d; want 1 immediately at start (not one cadence later — a cold copy can "+
			"run for hours)", pruner.numRegistrations())
	}
	if got := pruner.registrationTokens()[0]; got != `{"last_id":12}` {
		t.Errorf("registered token = %q; want the durable position token", got)
	}
	if pruner.numCalls() != 0 || pruner.numUnscopedCalls() != 0 {
		t.Error("registration must never prune")
	}
}

// TestStartChangeLogConsumerRegistration_ColdStartRegistersZeroFrontier pins the
// cold-copy window: a stream that has applied nothing registers an EMPTY token,
// which the engines record as frontier 0 and which blocks every peer's prune for
// the duration of the copy. The alternative — not registering until the first
// apply — is the silent-loss window a multi-hour copy would open.
func TestStartChangeLogConsumerRegistration_ColdStartRegistersZeroFrontier(t *testing.T) {
	pruner := &autoPruneFakePruner{}
	s := &Streamer{
		Target:             stubEngine{},
		changeLogConsumers: pruner,
	}
	applier := &autoPruneFakeApplier{found: false} // nothing durably applied yet

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startChangeLogConsumerRegistration(ctx, "cold", applier)

	tokens := pruner.registrationTokens()
	if len(tokens) != 1 || tokens[0] != "" {
		t.Fatalf("registered tokens = %q; want exactly one EMPTY token (frontier 0)", tokens)
	}
}

// TestStartChangeLogConsumerRegistration_OncePerAttempt asserts the guard the
// two entry points rely on: cold start registers before the copy, the apply
// sidecars register on warm resume, and a stream that takes both paths must not
// end up with two goroutines refreshing the same row.
func TestStartChangeLogConsumerRegistration_OncePerAttempt(t *testing.T) {
	pruner := &autoPruneFakePruner{}
	s := &Streamer{Target: stubEngine{}, changeLogConsumers: pruner}
	applier := &autoPruneFakeApplier{found: true, pos: ir.Position{Token: `{"last_id":1}`}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startChangeLogConsumerRegistration(ctx, "s1", applier)
	s.startChangeLogConsumerRegistration(ctx, "s1", applier)

	if got := pruner.numRegistrations(); got != 1 {
		t.Errorf("registrations = %d after two starts; want 1 (the once-guard)", got)
	}
}

// TestStartChangeLogConsumerRegistration_NoOpForNonTriggerSource asserts a
// source with no registry surface never spawns the sidecar — the zero-value-safe
// default for every non-trigger engine and every bare test Streamer.
func TestStartChangeLogConsumerRegistration_NoOpForNonTriggerSource(t *testing.T) {
	s := &Streamer{Target: stubEngine{}}
	applier := &autoPruneFakeApplier{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.startChangeLogConsumerRegistration(ctx, "s1", applier)

	if applier.reads() != 0 {
		t.Errorf("applier reads = %d; want 0 (no registry surface ⇒ no sidecar)", applier.reads())
	}
}

// TestRunChangeLogConsumerTick_RegisterErrorSwallowed asserts a failed
// registration cannot stall the sync. It is loud in the log (an unregistered
// stream is invisible to a peer's pruner) but never fatal.
func TestRunChangeLogConsumerTick_RegisterErrorSwallowed(t *testing.T) {
	pruner := &autoPruneFakePruner{registerErr: errors.New("source read-only")}
	applier := &autoPruneFakeApplier{found: true, pos: ir.Position{Token: `{"last_id":3}`}}

	runChangeLogConsumerTick(context.Background(), newDiscardLogger(), applier, pruner, "s1", "c1")

	if pruner.numRegistrations() != 1 {
		t.Errorf("registrations = %d; want 1 (attempted, then swallowed)", pruner.numRegistrations())
	}
}

// TestRunChangeLogConsumerTick_PositionReadErrorRegistersZero asserts the
// conservative direction on a target read failure: register frontier 0 rather
// than skipping the refresh. A skipped refresh eventually looks like a stale
// consumer; a 0 frontier holds a peer's prune back, which is never wrong.
func TestRunChangeLogConsumerTick_PositionReadErrorRegistersZero(t *testing.T) {
	pruner := &autoPruneFakePruner{}
	applier := &autoPruneFakeApplier{readErr: errors.New("target down")}

	runChangeLogConsumerTick(context.Background(), newDiscardLogger(), applier, pruner, "s1", "c1")

	tokens := pruner.registrationTokens()
	if len(tokens) != 1 || tokens[0] != "" {
		t.Fatalf("registered tokens = %q; want one EMPTY token (frontier 0) after a position-read failure", tokens)
	}
}
