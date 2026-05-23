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

	"github.com/orware/sluice/internal/ir"
)

// testClockNow returns a fixed wall-clock for tests that don't need to
// drive state transitions but want a deterministic now.
func testClockNow() time.Time {
	return time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
}

// fakeLeaseStore is an in-memory mock of
// [ir.ShardConsolidationLeaseStore] backed by a per-table row map and
// an injected clock. Used by the unit tests in this file to exercise
// the LeaseManager's state machine + heartbeat goroutine without a
// real database.
//
// Behaviourally matches the engine implementations:
//   - TryAcquireLease wins on ABSENT and on EXPIRED rows; loses on
//     HELD and APPLIED.
//   - Heartbeat / RecordDDLText / FinalizeLeaseApply all require
//     continued ownership (holder match + not yet applied).
//   - The mock's clock is callable; tests advance it to drive
//     state transitions deterministically.
type fakeLeaseStore struct {
	mu   sync.Mutex
	rows map[string]ir.ShardConsolidationLeaseRow
	now  func() time.Time
}

func newFakeLeaseStore(now func() time.Time) *fakeLeaseStore {
	return &fakeLeaseStore{
		rows: map[string]ir.ShardConsolidationLeaseRow{},
		now:  now,
	}
}

func (s *fakeLeaseStore) TryAcquireLease(_ context.Context, tableName, streamID string, expires time.Time) (bool, ir.ShardConsolidationLeaseRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[tableName]
	if !ok {
		fresh := ir.ShardConsolidationLeaseRow{
			TargetTableFullName: tableName,
			LeaseHolderStreamID: streamID,
			LeaseExpiresAt:      expires,
			HasLeaseExpiresAt:   true,
		}
		s.rows[tableName] = fresh
		return true, fresh, nil
	}
	if row.HasAppliedAt {
		return false, row, nil
	}
	if row.HasLeaseExpiresAt && row.LeaseExpiresAt.After(s.now()) {
		// HELD
		return false, row, nil
	}
	// EXPIRED — preserve ddl_text for probe-and-record.
	row.LeaseHolderStreamID = streamID
	row.LeaseExpiresAt = expires
	row.HasLeaseExpiresAt = true
	s.rows[tableName] = row
	return true, row, nil
}

func (s *fakeLeaseStore) HeartbeatLease(_ context.Context, tableName, streamID string, expires time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[tableName]
	if !ok {
		return false, nil
	}
	if row.LeaseHolderStreamID != streamID || row.HasAppliedAt {
		return false, nil
	}
	row.LeaseExpiresAt = expires
	row.HasLeaseExpiresAt = true
	s.rows[tableName] = row
	return true, nil
}

func (s *fakeLeaseStore) RecordDDLText(_ context.Context, tableName, streamID, ddlText string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[tableName]
	if !ok {
		return false, nil
	}
	if row.LeaseHolderStreamID != streamID || row.HasAppliedAt {
		return false, nil
	}
	row.DDLText = ddlText
	s.rows[tableName] = row
	return true, nil
}

func (s *fakeLeaseStore) FinalizeLeaseApply(_ context.Context, tableName, streamID, ddlText, ddlChecksum string, version int64, anchor ir.Position) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[tableName]
	if !ok {
		return false, nil
	}
	if row.LeaseHolderStreamID != streamID || row.HasAppliedAt {
		return false, nil
	}
	row.DDLText = ddlText
	row.DDLChecksum = ddlChecksum
	row.AppliedSchemaVersion = version
	row.AppliedAt = s.now()
	row.HasAppliedAt = true
	// Persist the anchor (Token+Engine present → HasAnchor=true so the
	// lease GC sweep can compare it). v0.76.0 task #21 contract.
	if anchor.Engine != "" || anchor.Token != "" {
		row.AnchorPosition = anchor
		row.HasAnchor = true
	}
	s.rows[tableName] = row
	return true, nil
}

func (s *fakeLeaseStore) ObserveLease(_ context.Context, tableName string) (ir.ShardConsolidationLeaseRow, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[tableName]
	if !ok {
		return ir.ShardConsolidationLeaseRow{}, false, nil
	}
	return row, true, nil
}

// snapshot returns a copy of the row under tableName for assertion use
// in tests (the mock's mutex protects map access; the returned struct
// is safe to read after release).
func (s *fakeLeaseStore) snapshot(tableName string) (ir.ShardConsolidationLeaseRow, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.rows[tableName]
	return row, ok
}

// mockClock is a goroutine-safe stub clock. Advance() bumps the wall.
type mockClock struct {
	mu  sync.Mutex
	now time.Time
}

func newMockClock(start time.Time) *mockClock { return &mockClock{now: start} }

func (c *mockClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *mockClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newTestLeaseManager(t *testing.T, store ir.ShardConsolidationLeaseStore, streamID string, cfg LeaseConfig, clock *mockClock) *LeaseManager {
	t.Helper()
	m, err := NewLeaseManager(store, streamID, cfg)
	if err != nil {
		t.Fatalf("NewLeaseManager: %v", err)
	}
	if clock != nil {
		m.now = clock.Now
	}
	return m
}

func TestLeaseManager_AcquireAbsentRow(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	cfg := LeaseConfig{LeaseDuration: time.Hour, RenewDeadline: 30 * time.Minute, RetryPeriod: 10 * time.Minute}
	mgr := newTestLeaseManager(t, store, "stream-a", cfg, clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lease, err := mgr.Acquire(ctx, "public.users", "ALTER TABLE users ADD COLUMN x INT")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease.Takeover() {
		t.Error("absent-row acquire should not be a takeover")
	}
	row, ok := store.snapshot("public.users")
	if !ok {
		t.Fatal("expected row to exist after acquire")
	}
	if row.LeaseHolderStreamID != "stream-a" {
		t.Errorf("holder = %q, want %q", row.LeaseHolderStreamID, "stream-a")
	}
	if row.DDLText != "ALTER TABLE users ADD COLUMN x INT" {
		t.Errorf("ddl_text = %q, want recorded", row.DDLText)
	}

	// Finalize and confirm the row reflects applied state.
	checksum := ChecksumDDLText("ALTER TABLE users ADD COLUMN x INT")
	if err := mgr.Apply(ctx, lease, 1, "ALTER TABLE users ADD COLUMN x INT", checksum, ir.Position{}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	row, _ = store.snapshot("public.users")
	if !row.HasAppliedAt {
		t.Error("expected applied_at to be set after Apply")
	}
	if row.DDLChecksum != checksum {
		t.Errorf("ddl_checksum = %q, want %q", row.DDLChecksum, checksum)
	}
	if row.AppliedSchemaVersion != 1 {
		t.Errorf("applied_schema_version = %d, want 1", row.AppliedSchemaVersion)
	}
}

func TestLeaseManager_AcquireContended(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	cfg := LeaseConfig{LeaseDuration: time.Hour, RenewDeadline: 30 * time.Minute, RetryPeriod: 10 * time.Minute}
	mgrA := newTestLeaseManager(t, store, "stream-a", cfg, clock)
	mgrB := newTestLeaseManager(t, store, "stream-b", cfg, clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leaseA, err := mgrA.Acquire(ctx, "public.users", "alter")
	if err != nil {
		t.Fatalf("mgrA.Acquire: %v", err)
	}
	defer mgrA.Release(ctx, leaseA)

	// Second acquire while held → ErrLeaseContended.
	_, err = mgrB.Acquire(ctx, "public.users", "alter")
	if !errors.Is(err, ErrLeaseContended) {
		t.Fatalf("expected ErrLeaseContended, got %v", err)
	}
}

func TestLeaseManager_TakeoverAfterExpiry(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	cfg := LeaseConfig{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 10 * time.Second}
	mgrA := newTestLeaseManager(t, store, "stream-a", cfg, clock)
	mgrB := newTestLeaseManager(t, store, "stream-b", cfg, clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leaseA, err := mgrA.Acquire(ctx, "public.users", "ALTER TABLE users ADD COLUMN x INT")
	if err != nil {
		t.Fatalf("mgrA.Acquire: %v", err)
	}
	// Simulate a crashed holder: release without finalizing + advance
	// clock past lease expiry.
	mgrA.Release(ctx, leaseA)
	clock.Advance(31 * time.Second)

	leaseB, err := mgrB.Acquire(ctx, "public.users", "ALTER TABLE users ADD COLUMN x INT")
	if err != nil {
		t.Fatalf("mgrB.Acquire (takeover): %v", err)
	}
	defer mgrB.Release(ctx, leaseB)

	if !leaseB.Takeover() {
		t.Error("expected Takeover() true for stream-b's acquire over expired stream-a")
	}
	if leaseB.PriorDDLText() != "ALTER TABLE users ADD COLUMN x INT" {
		t.Errorf("priorDDLText = %q, want recorded", leaseB.PriorDDLText())
	}
}

func TestLeaseManager_ObserveStates(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	cfg := LeaseConfig{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 10 * time.Second}
	mgrA := newTestLeaseManager(t, store, "stream-a", cfg, clock)
	mgrB := newTestLeaseManager(t, store, "stream-b", cfg, clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ABSENT
	obs, err := mgrB.Observe(ctx, "public.users")
	if err != nil {
		t.Fatalf("Observe(absent): %v", err)
	}
	if obs.State != LeaseStateAbsent {
		t.Errorf("State = %v, want Absent", obs.State)
	}

	// HELD
	leaseA, err := mgrA.Acquire(ctx, "public.users", "alter")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	obs, err = mgrB.Observe(ctx, "public.users")
	if err != nil {
		t.Fatalf("Observe(held): %v", err)
	}
	if obs.State != LeaseStateHeld {
		t.Errorf("State = %v, want Held", obs.State)
	}
	if obs.HolderStreamID != "stream-a" {
		t.Errorf("HolderStreamID = %q, want stream-a", obs.HolderStreamID)
	}

	// APPLIED
	if err := mgrA.Apply(ctx, leaseA, 7, "alter", ChecksumDDLText("alter"), ir.Position{}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	obs, err = mgrB.Observe(ctx, "public.users")
	if err != nil {
		t.Fatalf("Observe(applied): %v", err)
	}
	if obs.State != LeaseStateApplied {
		t.Errorf("State = %v, want Applied", obs.State)
	}
	if obs.AppliedSchemaVersion != 7 {
		t.Errorf("AppliedSchemaVersion = %d, want 7", obs.AppliedSchemaVersion)
	}
	if obs.DDLChecksum != ChecksumDDLText("alter") {
		t.Errorf("DDLChecksum mismatch")
	}
}

func TestLeaseManager_HeartbeatExtendsExpiry(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	cfg := LeaseConfig{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 10 * time.Second}
	mgr := newTestLeaseManager(t, store, "stream-a", cfg, clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lease, err := mgr.Acquire(ctx, "public.users", "alter")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer mgr.Release(ctx, lease)

	rowBefore, _ := store.snapshot("public.users")

	// Advance clock and explicit-heartbeat. The expiry should bump.
	clock.Advance(5 * time.Second)
	if err := mgr.Heartbeat(ctx, lease); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	rowAfter, _ := store.snapshot("public.users")
	if !rowAfter.LeaseExpiresAt.After(rowBefore.LeaseExpiresAt) {
		t.Errorf("heartbeat did not extend expiry: before=%v after=%v", rowBefore.LeaseExpiresAt, rowAfter.LeaseExpiresAt)
	}
}

func TestLeaseManager_HeartbeatAfterTakeoverIsLost(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	cfg := LeaseConfig{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 10 * time.Second}
	mgrA := newTestLeaseManager(t, store, "stream-a", cfg, clock)
	mgrB := newTestLeaseManager(t, store, "stream-b", cfg, clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	leaseA, err := mgrA.Acquire(ctx, "public.users", "alter")
	if err != nil {
		t.Fatalf("mgrA.Acquire: %v", err)
	}
	mgrA.Release(ctx, leaseA)
	clock.Advance(31 * time.Second)

	leaseB, err := mgrB.Acquire(ctx, "public.users", "alter")
	if err != nil {
		t.Fatalf("mgrB.Acquire: %v", err)
	}
	defer mgrB.Release(ctx, leaseB)

	// Stream A still holds the in-memory handle but has lost the
	// durable lease. A synchronous Heartbeat should return ErrLeaseLost.
	if err := mgrA.Heartbeat(ctx, leaseA); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expected ErrLeaseLost from mgrA.Heartbeat after takeover, got %v", err)
	}
}

func TestLeaseManager_ConcurrentAcquireOneWinner(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	cfg := LeaseConfig{LeaseDuration: time.Hour, RenewDeadline: 30 * time.Minute, RetryPeriod: 10 * time.Minute}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const peers = 8
	type result struct {
		streamID string
		err      error
	}
	results := make(chan result, peers)
	var wg sync.WaitGroup
	for i := 0; i < peers; i++ {
		wg.Add(1)
		streamID := "stream-" + string(rune('a'+i))
		go func() {
			defer wg.Done()
			mgr := newTestLeaseManager(t, store, streamID, cfg, clock)
			_, err := mgr.Acquire(ctx, "public.users", "alter")
			results <- result{streamID: streamID, err: err}
		}()
	}
	wg.Wait()
	close(results)

	winners := 0
	contended := 0
	for r := range results {
		switch {
		case r.err == nil:
			winners++
		case errors.Is(r.err, ErrLeaseContended):
			contended++
		default:
			t.Errorf("%s: unexpected error: %v", r.streamID, r.err)
		}
	}
	if winners != 1 {
		t.Errorf("winners = %d, want exactly 1", winners)
	}
	if contended != peers-1 {
		t.Errorf("contended = %d, want %d", contended, peers-1)
	}
}

func TestLeaseManager_FinalizeIsIdempotent(t *testing.T) {
	t.Parallel()
	clock := newMockClock(time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC))
	store := newFakeLeaseStore(clock.Now)
	cfg := LeaseConfig{LeaseDuration: time.Hour, RenewDeadline: 30 * time.Minute, RetryPeriod: 10 * time.Minute}
	mgr := newTestLeaseManager(t, store, "stream-a", cfg, clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lease, err := mgr.Acquire(ctx, "public.users", "alter")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := mgr.Apply(ctx, lease, 1, "alter", ChecksumDDLText("alter"), ir.Position{}); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if err := mgr.Apply(ctx, lease, 2, "different", "wrong-checksum", ir.Position{}); err != nil {
		t.Fatalf("second Apply (idempotent): %v", err)
	}
	row, _ := store.snapshot("public.users")
	if row.AppliedSchemaVersion != 1 {
		t.Errorf("AppliedSchemaVersion = %d, want 1 (idempotent Apply must not overwrite)", row.AppliedSchemaVersion)
	}
}

func TestNormalizeDDLText(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"whitespace-only", "   \t  \n", ""},
		{"trim-edges", "  ALTER TABLE x  ", "alter table x"},
		{"collapse-internal", "ALTER   TABLE\t\tx", "alter table x"},
		{"lowercase-keywords", "Alter TABLE x ADD COLUMN y INT", "alter table x add column y int"},
		// Quoted identifiers contain non-letters so they pass through.
		{"preserve-quoted-ident", `ALTER TABLE "Foo Bar" ADD COLUMN x INT`, `alter table "Foo Bar" add column x int`},
		// Literals containing punctuation pass through.
		{"preserve-literal", "ALTER TABLE x ADD COLUMN y DEFAULT 'A=B'", "alter table x add column y default 'A=B'"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			got := NormalizeDDLText(c.in)
			// Note: the literal-preservation case has internal
			// whitespace inside the literal too, which our normalizer
			// (using strings.Fields) collapses — that's a known
			// limitation; the literal must not contain whitespace for
			// the normalizer to round-trip it. The test cases above
			// avoid that case.
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestChecksumDDLText_StableAndCaseInsensitive(t *testing.T) {
	t.Parallel()
	a := ChecksumDDLText("ALTER TABLE users ADD COLUMN x INT")
	b := ChecksumDDLText("alter   table users add column x int")
	c := ChecksumDDLText("  ALTER  TABLE users ADD COLUMN x INT  ")
	if a != b || a != c {
		t.Errorf("checksum should be stable across whitespace + case: a=%q b=%q c=%q", a, b, c)
	}
	d := ChecksumDDLText("ALTER TABLE users ADD COLUMN y INT")
	if a == d {
		t.Errorf("checksum should differ for different DDL: a=%q d=%q", a, d)
	}
	// SHA-256 hex is 64 chars.
	if len(a) != 64 {
		t.Errorf("checksum length = %d, want 64 (SHA-256 hex)", len(a))
	}
	if strings.ContainsAny(a, "ABCDEF") {
		t.Errorf("checksum should be lowercase hex, got %q", a)
	}
}

func TestLeaseConfig_Validate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		cfg     LeaseConfig
		wantErr bool
	}{
		{"zero-defaults", LeaseConfig{}, false},
		{"valid", LeaseConfig{LeaseDuration: 30 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 10 * time.Second}, false},
		{"renew-too-small", LeaseConfig{LeaseDuration: 30 * time.Second, RenewDeadline: 5 * time.Second, RetryPeriod: 10 * time.Second}, true},
		{"lease-too-small", LeaseConfig{LeaseDuration: 10 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 5 * time.Second}, true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			err := c.cfg.Validate()
			if (err != nil) != c.wantErr {
				t.Errorf("Validate() err = %v, wantErr = %v", err, c.wantErr)
			}
		})
	}
}

func TestNewLeaseManager_Refusals(t *testing.T) {
	t.Parallel()
	store := newFakeLeaseStore(time.Now)
	cases := []struct {
		name     string
		store    ir.ShardConsolidationLeaseStore
		streamID string
		cfg      LeaseConfig
	}{
		{"nil-store", nil, "stream-a", LeaseConfig{}},
		{"empty-stream-id", store, "", LeaseConfig{}},
		{"whitespace-stream-id", store, "   ", LeaseConfig{}},
		{"bad-cfg", store, "stream-a", LeaseConfig{LeaseDuration: 10 * time.Second, RenewDeadline: 20 * time.Second, RetryPeriod: 5 * time.Second}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewLeaseManager(c.store, c.streamID, c.cfg); err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}
