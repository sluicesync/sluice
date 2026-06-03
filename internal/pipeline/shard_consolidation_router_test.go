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

// fakeShapeApplier records calls and (optionally) injects an error.
// Implements ir.ShapeDeltaApplier (via the embedded
// SchemaDeltaApplier-compatible AlterAddColumn).
type fakeShapeApplier struct {
	mu          sync.Mutex
	calls       []string
	injectErr   error
	addColCalls int
}

func (f *fakeShapeApplier) record(name string) error {
	f.mu.Lock()
	f.calls = append(f.calls, name)
	f.mu.Unlock()
	return f.injectErr
}

func (f *fakeShapeApplier) AlterAddColumn(_ context.Context, _ *ir.Table, _ []*ir.Column) error {
	f.mu.Lock()
	f.addColCalls++
	f.mu.Unlock()
	return f.record("AlterAddColumn")
}

func (f *fakeShapeApplier) AlterDropColumn(_ context.Context, _ *ir.Table, _ []*ir.Column) error {
	return f.record("AlterDropColumn")
}

func (f *fakeShapeApplier) CreateShapeIndex(_ context.Context, _ *ir.Table, _ []*ir.Index) error {
	return f.record("CreateShapeIndex")
}

func (f *fakeShapeApplier) DropShapeIndex(_ context.Context, _ *ir.Table, _ []*ir.Index) error {
	return f.record("DropShapeIndex")
}

func (f *fakeShapeApplier) AlterColumnType(_ context.Context, _ *ir.Table, _ *ir.Column) error {
	return f.record("AlterColumnType")
}

func (f *fakeShapeApplier) AlterColumnNullability(_ context.Context, _ *ir.Table, _ *ir.Column) error {
	return f.record("AlterColumnNullability")
}

func (f *fakeShapeApplier) AlterRenameColumn(_ context.Context, _ *ir.Table, _ /*oldName*/, _ /*newName*/ string) error {
	return f.record("AlterRenameColumn")
}

func (f *fakeShapeApplier) AlterAddCheck(_ context.Context, _ *ir.Table, _ []*ir.CheckConstraint) error {
	return f.record("AlterAddCheck")
}

func (f *fakeShapeApplier) AlterDropCheck(_ context.Context, _ *ir.Table, _ []*ir.CheckConstraint) error {
	return f.record("AlterDropCheck")
}

func (f *fakeShapeApplier) AlterModifyCheck(_ context.Context, _ *ir.Table, _, _ *ir.CheckConstraint) error {
	return f.record("AlterModifyCheck")
}

func newTestRouter(t *testing.T, store *fakeLeaseStore, streamID string, prober ShardConsolidationProber, applier ir.ShapeDeltaApplier, clock *mockClock) *BoundaryRouter {
	t.Helper()
	cfg := LeaseConfig{LeaseDuration: time.Minute, RenewDeadline: 30 * time.Second, RetryPeriod: 5 * time.Second}
	mgr := newTestLeaseManager(t, store, streamID, cfg, clock)
	router, err := NewBoundaryRouter(mgr, applier, prober)
	if err != nil {
		t.Fatalf("NewBoundaryRouter: %v", err)
	}
	// Shrink poll interval for tests so the observer-wait paths don't
	// add real seconds of latency.
	router.observePollInterval = 5 * time.Millisecond
	router.observeTimeout = 200 * time.Millisecond
	return router
}

func TestRouteBoundary_NoneShape_NoOp(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	prober := &fakeProber{}
	applier := &fakeShapeApplier{}
	router := newTestRouter(t, store, "stream-a", prober, applier, clock)

	pre := fixtureTable("users", "id", "email")
	post := fixtureTable("users", "id", "email")
	if err := router.RouteBoundary(context.Background(), "users", pre, post, "no-op", 1, ir.Position{}); err != nil {
		t.Fatalf("RouteBoundary: %v", err)
	}
	if applier.addColCalls != 0 {
		t.Errorf("expected no AlterAddColumn calls on no-op delta")
	}
	if _, ok := store.snapshot("users"); ok {
		t.Errorf("expected no lease row for no-op delta")
	}
}

func TestRouteBoundary_HappyPath_ApplyAndFinalize(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	prober := &fakeProber{}
	applier := &fakeShapeApplier{}
	router := newTestRouter(t, store, "stream-a", prober, applier, clock)

	pre := fixtureTable("users", "id")
	post := fixtureTable("users", "id", "added_at")
	if err := router.RouteBoundary(context.Background(), "users", pre, post, "ALTER TABLE users ADD COLUMN added_at INT", 1, ir.Position{}); err != nil {
		t.Fatalf("RouteBoundary: %v", err)
	}
	if applier.addColCalls != 1 {
		t.Errorf("AlterAddColumn calls = %d, want 1", applier.addColCalls)
	}
	row, ok := store.snapshot("users")
	if !ok {
		t.Fatal("expected lease row to be created")
	}
	if !row.HasAppliedAt {
		t.Error("expected applied_at to be set after Apply")
	}
	if row.AppliedSchemaVersion != 1 {
		t.Errorf("AppliedSchemaVersion = %d, want 1", row.AppliedSchemaVersion)
	}
}

// TestRouteBoundary_RenameColumn_HappyPath: rename-shaped delta
// fires AlterRenameColumn on the applier (task #22).
func TestRouteBoundary_RenameColumn_HappyPath(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	prober := &fakeProber{}
	applier := &fakeShapeApplier{}
	router := newTestRouter(t, store, "stream-a", prober, applier, clock)

	pre := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "email", Type: ir.Text{}},
	}}
	post := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "email_address", Type: ir.Text{}},
	}}
	if err := router.RouteBoundary(context.Background(), "users", pre, post,
		"ALTER TABLE users RENAME COLUMN email TO email_address", 1, ir.Position{}); err != nil {
		t.Fatalf("RouteBoundary rename: %v", err)
	}
	applier.mu.Lock()
	calls := append([]string(nil), applier.calls...)
	applier.mu.Unlock()
	if len(calls) != 1 || calls[0] != "AlterRenameColumn" {
		t.Errorf("applier calls = %v, want [AlterRenameColumn]", calls)
	}
	row, ok := store.snapshot("users")
	if !ok {
		t.Fatal("expected lease row to be created")
	}
	if !row.HasAppliedAt {
		t.Error("expected applied_at to be set after rename apply")
	}
}

func TestRouteBoundary_UnrecognizedShape_RefuseLoudly(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	prober := &fakeProber{}
	applier := &fakeShapeApplier{}
	router := newTestRouter(t, store, "stream-a", prober, applier, clock)

	// True combo delta: DROP column + CREATE INDEX. (The v0.78.0
	// task #22 RENAME classifier treats a same-attribute drop+add
	// as a rename, so we mix a column delta with an index delta to
	// exercise the genuine combo refusal.)
	pre := fixtureTable("users", "id", "deprecated")
	post := fixtureTable("users", "id")
	post.Indexes = append(post.Indexes, &ir.Index{
		Name:    "ix_users_id_secondary",
		Columns: []ir.IndexColumn{{Column: "id"}},
	})
	err := router.RouteBoundary(context.Background(), "users", pre, post, "combo", 1, ir.Position{})
	if err == nil {
		t.Fatal("expected refusal on unrecognized combo shape")
	}
	if !strings.Contains(err.Error(), "drained model") {
		t.Errorf("refusal should include drained-model recovery hint; got %v", err)
	}
}

func TestRouteBoundary_TakeoverNotApplied_ReApply(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	prober := &fakeProber{addCol: ProbeOutcomeNotApplied}
	applier := &fakeShapeApplier{}

	// Stream A acquires lease and crashes (release without finalize).
	mgrA := newTestLeaseManager(t, store, "stream-a", LeaseConfig{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 10 * time.Second}, clock)
	leaseA, err := mgrA.Acquire(context.Background(), "users", "ALTER TABLE users ADD COLUMN added_at INT")
	if err != nil {
		t.Fatalf("mgrA.Acquire: %v", err)
	}
	mgrA.Release(context.Background(), leaseA)
	clock.Advance(31 * time.Second) // expire lease A

	// Stream B's router takes over via probe → re-apply path.
	router := newTestRouter(t, store, "stream-b", prober, applier, clock)
	pre := fixtureTable("users", "id")
	post := fixtureTable("users", "id", "added_at")
	if err := router.RouteBoundary(context.Background(), "users", pre, post, "ALTER TABLE users ADD COLUMN added_at INT", 1, ir.Position{}); err != nil {
		t.Fatalf("RouteBoundary takeover: %v", err)
	}
	if applier.addColCalls != 1 {
		t.Errorf("expected exactly 1 AlterAddColumn call on takeover NotApplied; got %d", applier.addColCalls)
	}
}

func TestRouteBoundary_TakeoverApplied_RecordOnly(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	prober := &fakeProber{addCol: ProbeOutcomeApplied}
	applier := &fakeShapeApplier{}

	mgrA := newTestLeaseManager(t, store, "stream-a", LeaseConfig{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 10 * time.Second}, clock)
	leaseA, err := mgrA.Acquire(context.Background(), "users", "ALTER TABLE users ADD COLUMN added_at INT")
	if err != nil {
		t.Fatalf("mgrA.Acquire: %v", err)
	}
	mgrA.Release(context.Background(), leaseA)
	clock.Advance(31 * time.Second)

	router := newTestRouter(t, store, "stream-b", prober, applier, clock)
	pre := fixtureTable("users", "id")
	post := fixtureTable("users", "id", "added_at")
	if err := router.RouteBoundary(context.Background(), "users", pre, post, "ALTER TABLE users ADD COLUMN added_at INT", 1, ir.Position{}); err != nil {
		t.Fatalf("RouteBoundary takeover: %v", err)
	}
	if applier.addColCalls != 0 {
		t.Errorf("expected 0 AlterAddColumn calls on takeover Applied (record-only); got %d", applier.addColCalls)
	}
}

func TestRouteBoundary_TakeoverInconsistent_RefuseLoudly(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	prober := &fakeProber{addCol: ProbeOutcomeInconsistent}
	applier := &fakeShapeApplier{}

	mgrA := newTestLeaseManager(t, store, "stream-a", LeaseConfig{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 10 * time.Second}, clock)
	leaseA, err := mgrA.Acquire(context.Background(), "users", "ALTER TABLE users ADD COLUMN added_at INT")
	if err != nil {
		t.Fatalf("mgrA.Acquire: %v", err)
	}
	mgrA.Release(context.Background(), leaseA)
	clock.Advance(31 * time.Second)

	router := newTestRouter(t, store, "stream-b", prober, applier, clock)
	pre := fixtureTable("users", "id")
	post := fixtureTable("users", "id", "added_at")
	err = router.RouteBoundary(context.Background(), "users", pre, post, "ALTER", 1, ir.Position{})
	if err == nil {
		t.Fatal("expected loud refusal on takeover Inconsistent")
	}
	if !strings.Contains(err.Error(), "Inconsistent") && !strings.Contains(err.Error(), "drained model") {
		t.Errorf("refusal should mention Inconsistent + drained model; got %v", err)
	}
}

func TestRouteBoundary_PeerApplied_ChecksumMatch(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	prober := &fakeProber{}
	applier := &fakeShapeApplier{}

	// Peer (stream-a) acquires + applies first.
	const ddl = "ALTER TABLE users ADD COLUMN added_at INT"
	mgrA := newTestLeaseManager(t, store, "stream-a", LeaseConfig{}, clock)
	leaseA, err := mgrA.Acquire(context.Background(), "users", ddl)
	if err != nil {
		t.Fatalf("mgrA.Acquire: %v", err)
	}
	if err := mgrA.Apply(context.Background(), leaseA, 1, ddl, ChecksumDDLText(ddl), ir.Position{}); err != nil {
		t.Fatalf("mgrA.Apply: %v", err)
	}

	// Stream-b's router observes the applied state and verifies
	// checksum match.
	router := newTestRouter(t, store, "stream-b", prober, applier, clock)
	pre := fixtureTable("users", "id")
	post := fixtureTable("users", "id", "added_at")
	if err := router.RouteBoundary(context.Background(), "users", pre, post, ddl, 1, ir.Position{}); err != nil {
		t.Fatalf("RouteBoundary peer-applied: %v", err)
	}
	if applier.addColCalls != 0 {
		t.Errorf("expected 0 AlterAddColumn calls on peer-applied; got %d", applier.addColCalls)
	}
}

func TestRouteBoundary_PeerApplied_ChecksumMismatch(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	prober := &fakeProber{}
	applier := &fakeShapeApplier{}

	// Peer applies DDL #1; this stream observes DDL #2 (different
	// checksum). Per ADR-0054 §3 step 6: refuse loudly.
	const peerDDL = "ALTER TABLE users ADD COLUMN added_at INT"
	mgrA := newTestLeaseManager(t, store, "stream-a", LeaseConfig{}, clock)
	leaseA, err := mgrA.Acquire(context.Background(), "users", peerDDL)
	if err != nil {
		t.Fatalf("mgrA.Acquire: %v", err)
	}
	if err := mgrA.Apply(context.Background(), leaseA, 1, peerDDL, ChecksumDDLText(peerDDL), ir.Position{}); err != nil {
		t.Fatalf("mgrA.Apply: %v", err)
	}

	router := newTestRouter(t, store, "stream-b", prober, applier, clock)
	pre := fixtureTable("users", "id")
	post := fixtureTable("users", "id", "added_at")
	// This stream's "ddl_text" intentionally differs from the peer's.
	err = router.RouteBoundary(context.Background(), "users", pre, post, "ALTER TABLE users ADD COLUMN different_col INT", 1, ir.Position{})
	if err == nil {
		t.Fatal("expected ErrLeaseChecksumMismatch on peer-applied divergent DDL")
	}
	if !errors.Is(err, ErrLeaseChecksumMismatch) {
		t.Errorf("expected ErrLeaseChecksumMismatch; got %v", err)
	}
}

func TestNewBoundaryRouter_NilGuards(t *testing.T) {
	t.Parallel()
	store := newFakeLeaseStore(testClockNow)
	mgr, err := NewLeaseManager(store, "stream-a", LeaseConfig{})
	if err != nil {
		t.Fatalf("NewLeaseManager: %v", err)
	}
	if _, err := NewBoundaryRouter(nil, &fakeShapeApplier{}, &fakeProber{}); err == nil {
		t.Error("expected error on nil manager")
	}
	if _, err := NewBoundaryRouter(mgr, nil, &fakeProber{}); err == nil {
		t.Error("expected error on nil applier")
	}
	if _, err := NewBoundaryRouter(mgr, &fakeShapeApplier{}, nil); err == nil {
		t.Error("expected error on nil prober")
	}
}
