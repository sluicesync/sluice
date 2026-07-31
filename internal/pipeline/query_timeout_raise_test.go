// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// ---- fakes ----

// fakeRecorderStore is a fakeStateStore that also records ADR-0182
// query-timeout raises (ir.QueryTimeoutRaiseRecorder).
type fakeRecorderStore struct {
	*fakeStateStore
	mu    sync.Mutex
	raise map[string]string // migrationID → previous (present ⇒ dangling)
}

func newFakeRecorderStore() *fakeRecorderStore {
	return &fakeRecorderStore{fakeStateStore: newFakeStateStore(), raise: map[string]string{}}
}

func (s *fakeRecorderStore) ReadQueryTimeoutRaise(_ context.Context, id string) (previous string, ok bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.raise[id]
	return v, ok, nil
}

func (s *fakeRecorderStore) RecordQueryTimeoutRaise(_ context.Context, id, previous string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.raise[id] = previous
	return nil
}

func (s *fakeRecorderStore) ClearQueryTimeoutRaise(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.raise, id)
	return nil
}

// fakeController is an ir.QueryTimeoutController that logs its raise/revert
// calls (relative to the copy phase) into a shared order slice.
type fakeController struct {
	mu    *sync.Mutex
	order *[]string

	recordPrevious string // value handed to record (default "900")
	skipRaise      bool   // simulate "already at max": record NOT called, no raise logged
	raiseErr       error
	revertErr      error

	reverts int
}

func (c *fakeController) Raise(_ context.Context, record func(string) error) error {
	if c.raiseErr != nil {
		return c.raiseErr
	}
	if c.skipRaise {
		return nil
	}
	if err := record(c.recordPrevious); err != nil {
		return err
	}
	c.mu.Lock()
	*c.order = append(*c.order, "raise")
	c.mu.Unlock()
	return nil
}

func (c *fakeController) Revert(_ context.Context, previous string) error {
	c.mu.Lock()
	c.reverts++
	*c.order = append(*c.order, "revert:"+previous)
	c.mu.Unlock()
	return c.revertErr
}

// copyOrderEngine is a target engine whose store records raises and whose row
// writer logs "copy" into a shared order slice, so a Run-level test can assert
// the raise precedes the copy and the revert follows it.
type copyOrderEngine struct {
	*recordingEngine
	store    *fakeRecorderStore
	mu       *sync.Mutex
	order    *[]string
	writeErr error
}

func (e *copyOrderEngine) OpenMigrationStateStore(context.Context, string) (ir.MigrationStateStore, error) {
	return e.store, nil
}

func (e *copyOrderEngine) OpenRowWriter(context.Context, string) (ir.RowWriter, error) {
	return &copyOrderRowWriter{mu: e.mu, order: e.order, writeErr: e.writeErr}, nil
}

type copyOrderRowWriter struct {
	mu       *sync.Mutex
	order    *[]string
	writeErr error
}

func (w *copyOrderRowWriter) WriteRows(_ context.Context, _ *ir.Table, rows <-chan ir.Row) error {
	// Drain the channel so the reader goroutine doesn't block.
	for range rows { //nolint:revive // intentional drain
	}
	w.mu.Lock()
	*w.order = append(*w.order, "copy")
	w.mu.Unlock()
	return w.writeErr
}

// ---- method-level unit tests ----

func rcFor(store ir.MigrationStateStore) resumeContext {
	return resumeContext{store: store, migrationID: "mig-1", enabled: true}
}

func TestAutoRevert_DanglingReverted(t *testing.T) {
	var order []string
	store := newFakeRecorderStore()
	store.raise["mig-1"] = "1200"
	ctrl := &fakeController{mu: &sync.Mutex{}, order: &order}
	m := &Migrator{QueryTimeoutController: ctrl}

	if err := m.autoRevertDanglingQueryTimeoutRaise(context.Background(), rcFor(store)); err != nil {
		t.Fatalf("autoRevert: %v", err)
	}
	if ctrl.reverts != 1 {
		t.Errorf("reverts = %d; want 1", ctrl.reverts)
	}
	if len(order) != 1 || order[0] != "revert:1200" {
		t.Errorf("order = %v; want [revert:1200] (restores the recorded previous)", order)
	}
	if _, ok := store.raise["mig-1"]; ok {
		t.Error("record must be cleared after a successful auto-revert")
	}
}

func TestAutoRevert_NoRecordIsNoOp(t *testing.T) {
	var order []string
	ctrl := &fakeController{mu: &sync.Mutex{}, order: &order}
	m := &Migrator{QueryTimeoutController: ctrl}
	if err := m.autoRevertDanglingQueryTimeoutRaise(context.Background(), rcFor(newFakeRecorderStore())); err != nil {
		t.Fatalf("autoRevert: %v", err)
	}
	if ctrl.reverts != 0 {
		t.Errorf("reverts = %d; want 0 (nothing recorded)", ctrl.reverts)
	}
}

func TestAutoRevert_DanglingWithNoControllerRefuses(t *testing.T) {
	store := newFakeRecorderStore()
	store.raise["mig-1"] = "900"
	m := &Migrator{QueryTimeoutController: nil} // credentials absent on this run
	err := m.autoRevertDanglingQueryTimeoutRaise(context.Background(), rcFor(store))
	if err == nil || !strings.Contains(err.Error(), "left it raised") {
		t.Fatalf("err = %v; want a loud refusal that the keyspace is still raised", err)
	}
	// The record must survive so a later run WITH credentials can still revert.
	if _, ok := store.raise["mig-1"]; !ok {
		t.Error("a refused auto-revert must NOT clear the record")
	}
}

func TestMaybeRaise_UnarmedIsNoOp(t *testing.T) {
	m := &Migrator{RaiseQueryTimeout: false, QueryTimeoutController: &fakeController{mu: &sync.Mutex{}, order: new([]string)}}
	revert, err := m.maybeRaiseQueryTimeout(context.Background(), rcFor(newFakeRecorderStore()), sampleSchema(), &recordingRowReader{})
	if err != nil || revert != nil {
		t.Fatalf("unarmed maybeRaise = (revert!=nil:%v, %v); want (false, nil)", revert != nil, err)
	}
}

func TestMaybeRaise_ArmedWithoutControllerRefuses(t *testing.T) {
	m := &Migrator{RaiseQueryTimeout: true, QueryTimeoutController: nil}
	_, err := m.maybeRaiseQueryTimeout(context.Background(), rcFor(newFakeRecorderStore()), sampleSchema(), &recordingRowReader{})
	if err == nil {
		t.Fatal("armed raise with no controller must refuse loudly")
	}
}

func TestMaybeRaise_SizeGateSkipsSmallMigration(t *testing.T) {
	var order []string
	ctrl := &fakeController{mu: &sync.Mutex{}, order: &order}
	m := &Migrator{RaiseQueryTimeout: true, QueryTimeoutController: ctrl}
	// Reader estimates a tiny table (below the floor) → skip, no raise, no record.
	rr := &estimatingReader{rows: 10}
	store := newFakeRecorderStore()
	revert, err := m.maybeRaiseQueryTimeout(context.Background(), rcFor(store), sampleSchema(), rr)
	if err != nil {
		t.Fatalf("maybeRaise: %v", err)
	}
	if revert != nil {
		t.Error("size-gated skip must return a nil revert (nothing to undo)")
	}
	if len(order) != 0 {
		t.Errorf("order = %v; want empty (no raise on a small migration)", order)
	}
	if len(store.raise) != 0 {
		t.Error("a size-gated skip must not record a raise")
	}
}

func TestMaybeRaise_LargeMigrationRaisesAndRecordsBeforeApply(t *testing.T) {
	var order []string
	ctrl := &fakeController{mu: &sync.Mutex{}, order: &order, recordPrevious: "900"}
	m := &Migrator{RaiseQueryTimeout: true, QueryTimeoutController: ctrl}
	rr := &estimatingReader{rows: queryTimeoutRaiseMinRows + 1}
	store := newFakeRecorderStore()

	revert, err := m.maybeRaiseQueryTimeout(context.Background(), rcFor(store), sampleSchema(), rr)
	if err != nil {
		t.Fatalf("maybeRaise: %v", err)
	}
	if revert == nil {
		t.Fatal("a raised migration must return a revert closure")
	}
	if got, ok := store.raise["mig-1"]; !ok || got != "900" {
		t.Errorf("recorded previous = (%q,%v); want (900,true) — recorded before applying", got, ok)
	}
	// The revert closure restores the recorded value and clears it.
	revert()
	if ctrl.reverts != 1 {
		t.Errorf("reverts = %d; want 1 after the deferred revert ran", ctrl.reverts)
	}
	if _, ok := store.raise["mig-1"]; ok {
		t.Error("revert must clear the record")
	}
}

func TestMaybeRaise_AlreadyAtMaxReturnsNilRevert(t *testing.T) {
	var order []string
	ctrl := &fakeController{mu: &sync.Mutex{}, order: &order, skipRaise: true}
	m := &Migrator{RaiseQueryTimeout: true, QueryTimeoutController: ctrl}
	store := newFakeRecorderStore()
	revert, err := m.maybeRaiseQueryTimeout(context.Background(), rcFor(store), sampleSchema(), &recordingRowReader{})
	if err != nil {
		t.Fatalf("maybeRaise: %v", err)
	}
	if revert != nil {
		t.Error("an already-at-max keyspace records nothing, so the revert closure must be nil")
	}
	if len(store.raise) != 0 {
		t.Error("nothing should be recorded when the raise was a no-op")
	}
}

func TestMaybeRaise_RaiseFailureAfterRecordReverts(t *testing.T) {
	var order []string
	ctrl := &fakeController{mu: &sync.Mutex{}, order: &order, recordPrevious: "900"}
	// The controller records (via the callback) then fails — models a rollout
	// that errored after the record landed. The record IS written before the
	// failure in this fake, so maybeRaise must revert it.
	ctrl.raiseErr = nil
	failing := &recordThenFailController{fakeController: ctrl}
	m := &Migrator{RaiseQueryTimeout: true, QueryTimeoutController: failing}
	store := newFakeRecorderStore()

	_, err := m.maybeRaiseQueryTimeout(context.Background(), rcFor(store), sampleSchema(), &estimatingReader{rows: queryTimeoutRaiseMinRows + 1})
	if err == nil {
		t.Fatal("a raise failure must surface as an error")
	}
	if ctrl.reverts != 1 {
		t.Errorf("reverts = %d; want 1 — a partial raise must be reverted", ctrl.reverts)
	}
	if _, ok := store.raise["mig-1"]; ok {
		t.Error("record must be cleared after the compensating revert")
	}
}

// recordThenFailController records the previous value then fails the raise.
type recordThenFailController struct {
	*fakeController
}

func (c *recordThenFailController) Raise(_ context.Context, record func(string) error) error {
	if err := record(c.recordPrevious); err != nil {
		return err
	}
	return errors.New("rollout wedged after the change applied")
}

// ---- Run-level sequencing ----

func newSequencingMigrator(t *testing.T, ctrl ir.QueryTimeoutController, order *[]string, mu *sync.Mutex, writeErr error) *Migrator {
	t.Helper()
	src := newRecordingEngine("source")
	src.schema = sampleSchema()
	tgt := &copyOrderEngine{
		recordingEngine: newRecordingEngine("target"),
		store:           newFakeRecorderStore(),
		mu:              mu,
		order:           order,
		writeErr:        writeErr,
	}
	return &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		RaiseQueryTimeout:      true,
		QueryTimeoutController: ctrl,
	}
}

func TestRun_RaiseBeforeCopy_RevertAfter(t *testing.T) {
	var order []string
	mu := &sync.Mutex{}
	ctrl := &fakeController{mu: mu, order: &order, recordPrevious: "900"}
	m := newSequencingMigrator(t, ctrl, &order, mu, nil)

	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertOrder(t, order, "raise", "copy", "revert:900")
}

func TestRun_RevertRunsOnAbort(t *testing.T) {
	var order []string
	mu := &sync.Mutex{}
	ctrl := &fakeController{mu: mu, order: &order, recordPrevious: "900"}
	m := newSequencingMigrator(t, ctrl, &order, mu, errors.New("copy blew up"))

	if err := m.Run(context.Background()); err == nil {
		t.Fatal("Run must surface the copy failure")
	}
	// Even on the abort path the revert must still run, AFTER the copy attempt.
	raiseIdx := indexOf(order, "raise")
	copyIdx := indexOf(order, "copy")
	revertIdx := indexOfPrefix(order, "revert:")
	if raiseIdx == -1 || copyIdx == -1 || revertIdx == -1 {
		t.Fatalf("missing a phase in order=%v (raise=%d copy=%d revert=%d)", order, raiseIdx, copyIdx, revertIdx)
	}
	if raiseIdx >= copyIdx || copyIdx >= revertIdx {
		t.Errorf("order=%v; want raise < copy < revert even on abort", order)
	}
}

// estimatingReader is a recordingRowReader with a row-count estimate for the
// size gate.
type estimatingReader struct {
	recordingRowReader
	rows int64
}

func (r *estimatingReader) EstimateRowCount(context.Context, *ir.Table) (int64, error) {
	return r.rows, nil
}

func assertOrder(t *testing.T, order []string, want ...string) {
	t.Helper()
	last := -1
	for _, w := range want {
		i := indexOf(order, w)
		if i == -1 {
			t.Fatalf("order %v missing %q", order, w)
		}
		if i <= last {
			t.Fatalf("order %v: %q out of sequence (want %v)", order, w, want)
		}
		last = i
	}
}

func indexOfPrefix(s []string, prefix string) int {
	for i, x := range s {
		if strings.HasPrefix(x, prefix) {
			return i
		}
	}
	return -1
}
