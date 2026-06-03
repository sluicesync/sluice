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

// fakeStateStore is an in-memory ir.MigrationStateStore for unit
// testing the resume orchestration. It records every Read / Write
// call and lets tests pre-seed a state row to simulate prior runs.
//
// The errors-on-write hook lets tests exercise the "state-write
// failure joined with primary error" branch.
type fakeStateStore struct {
	mu       sync.Mutex
	rows     map[string]ir.MigrationState
	reads    int
	writes   int
	writeErr error
	closed   bool
}

func newFakeStateStore() *fakeStateStore {
	return &fakeStateStore{rows: map[string]ir.MigrationState{}}
}

func (f *fakeStateStore) EnsureControlTable(context.Context) error { return nil }

func (f *fakeStateStore) Read(_ context.Context, id string) (ir.MigrationState, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	s, ok := f.rows[id]
	if !ok {
		return ir.MigrationState{}, false, nil
	}
	return s, true, nil
}

func (f *fakeStateStore) Write(_ context.Context, s ir.MigrationState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes++
	if f.writeErr != nil {
		return f.writeErr
	}
	// Preserve started_at across upserts the way the real
	// implementations do — first write captures it, later writes
	// keep the original.
	if existing, ok := f.rows[s.MigrationID]; ok && !existing.StartedAt.IsZero() {
		s.StartedAt = existing.StartedAt
	}
	f.rows[s.MigrationID] = s
	return nil
}

func (f *fakeStateStore) ClearMigration(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.rows, id)
	return nil
}

func (f *fakeStateStore) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeStateStore) get(id string) (ir.MigrationState, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.rows[id]
	return s, ok
}

// TestMigrationStateRoundTrip covers the JSON encoding / decoding of
// the table_progress map plus the basic Write+Read shape. The fake
// store mirrors the real implementations' state-preserving semantics,
// so this exercise also acts as a contract test for the engine-side
// helpers.
func TestMigrationStateRoundTrip(t *testing.T) {
	store := newFakeStateStore()
	rc := resumeContext{store: store, migrationID: "m1", enabled: true}

	state := ir.MigrationState{
		MigrationID: "m1",
		Phase:       ir.MigrationPhaseBulkCopy,
		TableProgress: map[string]ir.TableProgress{
			"users":  {State: ir.TableProgressComplete},
			"orders": {State: ir.TableProgressInProgress, LastPK: []any{int64(42)}, RowsCopied: 42},
		},
	}
	if err := writeState(context.Background(), rc, state); err != nil {
		t.Fatalf("writeState: %v", err)
	}

	got, ok := store.get("m1")
	if !ok {
		t.Fatal("row not persisted")
	}
	if got.Phase != ir.MigrationPhaseBulkCopy {
		t.Errorf("phase = %q; want %q", got.Phase, ir.MigrationPhaseBulkCopy)
	}
	if got.TableProgress["users"].State != ir.TableProgressComplete {
		t.Errorf("TableProgress[users].State = %q; want complete", got.TableProgress["users"].State)
	}
	if got.TableProgress["orders"].State != ir.TableProgressInProgress {
		t.Errorf("TableProgress[orders].State = %q; want in_progress", got.TableProgress["orders"].State)
	}
	if got.TableProgress["orders"].RowsCopied != 42 {
		t.Errorf("TableProgress[orders].RowsCopied = %d; want 42", got.TableProgress["orders"].RowsCopied)
	}
}

// TestLoadOrInitState_FreshMigration confirms a brand-new run with no
// state row writes a pending row and continues without short-
// circuiting.
func TestLoadOrInitState_FreshMigration(t *testing.T) {
	store := newFakeStateStore()
	rc := resumeContext{store: store, migrationID: "fresh", enabled: true}

	state, exitClean, err := loadOrInitState(context.Background(), rc, false /*resume*/, false)
	if err != nil {
		t.Fatalf("loadOrInitState: %v", err)
	}
	if exitClean {
		t.Error("exitClean = true on fresh run; want false")
	}
	if state.Phase != ir.MigrationPhasePending {
		t.Errorf("phase = %q; want pending", state.Phase)
	}
	if got, ok := store.get("fresh"); !ok || got.Phase != ir.MigrationPhasePending {
		t.Errorf("persisted row missing or wrong phase; got %+v ok=%v", got, ok)
	}
}

// TestLoadOrInitState_RefusePartialWithoutResume covers the
// "row exists, --resume not passed" guard. Operators must explicitly
// opt into resume rather than silently overwriting.
func TestLoadOrInitState_RefusePartialWithoutResume(t *testing.T) {
	store := newFakeStateStore()
	store.rows["m1"] = ir.MigrationState{MigrationID: "m1", Phase: ir.MigrationPhaseBulkCopy}
	rc := resumeContext{store: store, migrationID: "m1", enabled: true}

	_, _, err := loadOrInitState(context.Background(), rc, false /*resume*/, false)
	if err == nil {
		t.Fatal("loadOrInitState succeeded; want refusal error")
	}
	if !strings.Contains(err.Error(), "partial migration") {
		t.Errorf("err = %v; want 'partial migration' wording", err)
	}
}

// TestLoadOrInitState_RefuseCompleteWithoutResume covers
// "row exists, phase=complete, no --resume". The operator gets a
// distinct message pointing at the right remediation.
func TestLoadOrInitState_RefuseCompleteWithoutResume(t *testing.T) {
	store := newFakeStateStore()
	store.rows["m1"] = ir.MigrationState{MigrationID: "m1", Phase: ir.MigrationPhaseComplete}
	rc := resumeContext{store: store, migrationID: "m1", enabled: true}

	_, _, err := loadOrInitState(context.Background(), rc, false /*resume*/, false)
	if err == nil {
		t.Fatal("loadOrInitState succeeded; want refusal error")
	}
	if !strings.Contains(err.Error(), "already complete") {
		t.Errorf("err = %v; want 'already complete' wording", err)
	}
}

// TestLoadOrInitState_ResumeNoRow covers "--resume but no row". The
// expected outcome is an error pointing the operator at the fresh-
// run path, not a silent fall-through to fresh state.
func TestLoadOrInitState_ResumeNoRow(t *testing.T) {
	store := newFakeStateStore()
	rc := resumeContext{store: store, migrationID: "missing", enabled: true}

	_, _, err := loadOrInitState(context.Background(), rc, true /*resume*/, false)
	if err == nil {
		t.Fatal("loadOrInitState succeeded; want refusal error")
	}
	if !strings.Contains(err.Error(), "no migration found") {
		t.Errorf("err = %v; want 'no migration found' wording", err)
	}
}

// TestLoadOrInitState_ResettingBypassesRefusal covers
// --reset-target-data: an existing complete or partial row does NOT
// refuse the run, because the reset path will DELETE it shortly.
// loadOrInitState returns a fresh pending state.
func TestLoadOrInitState_ResettingBypassesRefusal(t *testing.T) {
	cases := []struct {
		name  string
		phase ir.MigrationPhase
	}{
		{"complete row", ir.MigrationPhaseComplete},
		{"partial row", ir.MigrationPhaseBulkCopy},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			store := newFakeStateStore()
			store.rows["m1"] = ir.MigrationState{MigrationID: "m1", Phase: c.phase}
			rc := resumeContext{store: store, migrationID: "m1", enabled: true}

			state, exitClean, err := loadOrInitState(context.Background(), rc, false, true /*resetting*/)
			if err != nil {
				t.Fatalf("loadOrInitState: %v", err)
			}
			if exitClean {
				t.Error("exitClean = true on reset; want false")
			}
			if state.Phase != ir.MigrationPhasePending {
				t.Errorf("phase = %q; want pending", state.Phase)
			}
		})
	}
}

// TestLoadOrInitState_ResumeAlreadyComplete covers the clean exit
// path: an already-complete migration with --resume logs and exits
// with no further work.
func TestLoadOrInitState_ResumeAlreadyComplete(t *testing.T) {
	store := newFakeStateStore()
	store.rows["m1"] = ir.MigrationState{MigrationID: "m1", Phase: ir.MigrationPhaseComplete}
	rc := resumeContext{store: store, migrationID: "m1", enabled: true}

	state, exitClean, err := loadOrInitState(context.Background(), rc, true, false)
	if err != nil {
		t.Fatalf("loadOrInitState: %v", err)
	}
	if !exitClean {
		t.Error("exitClean = false on resume-of-complete; want true")
	}
	if state.Phase != ir.MigrationPhaseComplete {
		t.Errorf("phase = %q; want complete", state.Phase)
	}
}

// TestResumePhaseSkipping confirms classifyTableForResume returns
// the right action for each (state, resuming) input. This is the
// load-bearing decision in the bulk-copy phase, so unit-testing the
// classifier keeps the orchestrator's switch honest.
func TestResumePhaseSkipping(t *testing.T) {
	state := ir.MigrationState{
		TableProgress: map[string]ir.TableProgress{
			"users":  {State: ir.TableProgressComplete},
			"orders": {State: ir.TableProgressInProgress, LastPK: []any{int64(100)}, RowsCopied: 100},
			"legacy": {State: ir.TableProgressInProgress}, // v0.3.0-shape: no cursor
			"events": {State: ir.TableProgressNoPKTruncateAndRedo},
			"chunked": {
				State: ir.TableProgressInProgress,
				Chunks: []ir.TableChunkProgress{
					{ChunkIndex: 0, UpperPK: []any{int64(50)}, State: ir.TableProgressComplete},
					{ChunkIndex: 1, LowerPK: []any{int64(50)}, State: ir.TableProgressInProgress, LastPK: []any{int64(75)}, RowsCopied: 25},
				},
			},
		},
	}
	cases := []struct {
		name     string
		table    string
		resuming bool
		want     resumeBulkCopyAction
	}{
		{"complete + resuming", "users", true, resumeActionSkip},
		{"in_progress with cursor + resuming", "orders", true, resumeActionResumeFromCursor},
		{"v0.3.0 in_progress without cursor + resuming", "legacy", true, resumeActionTruncate},
		{"no_pk_truncate_and_redo + resuming", "events", true, resumeActionTruncate},
		{"missing + resuming", "audit_log", true, resumeActionFresh},
		{"complete but not resuming", "users", false, resumeActionFresh},
		{"in_progress but not resuming", "orders", false, resumeActionFresh},
		{"chunked + resuming", "chunked", true, resumeActionResumeChunked},
		{"chunked but not resuming", "chunked", false, resumeActionFresh},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := classifyTableForResume(state, c.table, c.resuming)
			if got != c.want {
				t.Errorf("classifyTableForResume(%q, resuming=%v) = %d; want %d", c.table, c.resuming, got, c.want)
			}
		})
	}
}

// TestPartialBulkCopyTruncates exercises the per-table truncate-and-
// redo behaviour through the orchestrator. Two tables are seeded as
// `complete` and `in_progress`; on resume, the in-progress one
// must invoke TruncateTable (via the optional ir.TableTruncator
// surface), and the completed one must be skipped entirely.
func TestPartialBulkCopyTruncates(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = &ir.Schema{
		Tables: []*ir.Table{
			{Name: "orders", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
			{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}},
		},
	}
	tgt := newRecordingEngineWithStore("target")
	tgt.store.rows["m1"] = ir.MigrationState{
		MigrationID: "m1",
		Phase:       ir.MigrationPhaseBulkCopy,
		TableProgress: map[string]ir.TableProgress{
			// "users" already complete; "orders" was an in-progress
			// row from a v0.3.0 binary (no cursor) → resume falls back
			// to truncate-and-redo.
			"users":  {State: ir.TableProgressComplete},
			"orders": {State: ir.TableProgressInProgress},
		},
	}

	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		Resume:      true,
		MigrationID: "m1",
	}
	if err := m.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// users (complete) → no WriteRows, no truncate.
	// orders (in_progress) → truncate then WriteRows.
	wantPhases := []string{
		"CreateTablesWithoutConstraints",
		"TruncateTable:orders",
		"WriteRows:orders",
		"SyncIdentitySequences",
		"CreateIndexes",
		"CreateConstraints",
	}
	if len(tgt.phaseLog) != len(wantPhases) {
		t.Fatalf("phaseLog = %v; want %v", tgt.phaseLog, wantPhases)
	}
	for i, want := range wantPhases {
		if tgt.phaseLog[i] != want {
			t.Errorf("phase[%d] = %q; want %q (full: %v)", i, tgt.phaseLog[i], want, tgt.phaseLog)
		}
	}

	// State row should be marked complete after a successful resume.
	got, ok := tgt.store.get("m1")
	if !ok {
		t.Fatal("state row missing after resume run")
	}
	if got.Phase != ir.MigrationPhaseComplete {
		t.Errorf("post-run phase = %q; want complete", got.Phase)
	}
}

// TestNoResumeRefusesPartialState is the orchestrator-level shape of
// the loadOrInitState refusal: a Migrator without --resume
// configured against a target with a partial state row exits with a
// clear error rather than overwriting silently.
func TestNoResumeRefusesPartialState(t *testing.T) {
	src := newRecordingEngine("source")
	src.schema = sampleSchema()
	tgt := newRecordingEngineWithStore("target")
	tgt.store.rows["m1"] = ir.MigrationState{MigrationID: "m1", Phase: ir.MigrationPhaseBulkCopy}

	m := &Migrator{
		Source: src, Target: tgt,
		SourceDSN: "src", TargetDSN: "tgt",
		Resume:      false,
		MigrationID: "m1",
	}
	err := m.Run(context.Background())
	if err == nil {
		t.Fatal("Run succeeded; want refusal error")
	}
	if !strings.Contains(err.Error(), "partial migration") {
		t.Errorf("err = %v; want 'partial migration' wording", err)
	}
	// No phases should have run on the target.
	for _, p := range tgt.phaseLog {
		if strings.HasPrefix(p, "WriteRows") || p == "CreateTablesWithoutConstraints" {
			t.Errorf("phaseLog contains %q; want no work performed", p)
		}
	}
}

// TestMarkFailedJoinsStateError covers the rare path where the state
// write itself fails after a primary phase error. The original error
// stays the head of the chain (so [errors.Is] traversal works), and
// the joined state-write failure surfaces too so an operator
// inspecting the error sees both.
func TestMarkFailedJoinsStateError(t *testing.T) {
	store := newFakeStateStore()
	store.writeErr = errors.New("simulated state-write failure")
	rc := resumeContext{store: store, migrationID: "m1", enabled: true}
	state := ir.MigrationState{MigrationID: "m1", Phase: ir.MigrationPhaseTables}

	primary := errors.New("primary phase error")
	got := markFailed(context.Background(), rc, state, ir.MigrationPhaseTables, primary)
	if got == nil {
		t.Fatal("markFailed returned nil; want joined error")
	}
	if !errors.Is(got, primary) {
		t.Errorf("errors.Is(got, primary) = false; want true")
	}
	if !strings.Contains(got.Error(), "simulated state-write failure") {
		t.Errorf("err = %v; want state-write failure surfaced", got)
	}
}

// TestDeriveMigrationID confirms the auto-derivation produces a
// stable, length-bounded ID for the same source/target host pair
// and a different ID for a different pair.
func TestDeriveMigrationID(t *testing.T) {
	a := deriveMigrationID("mysql", "user:pw@tcp(prod-1:3306)/db", "postgres", "postgres://u:p@warehouse:5432/db", "")
	b := deriveMigrationID("mysql", "user:pw@tcp(prod-1:3306)/db", "postgres", "postgres://u:p@warehouse:5432/db", "")
	c := deriveMigrationID("mysql", "user:pw@tcp(prod-2:3306)/db", "postgres", "postgres://u:p@warehouse:5432/db", "")
	// v0.25.0 added targetSchema as a discriminator: same hosts +
	// different --target-schema must produce distinct IDs so the
	// multi-source-aggregation pattern (one operator runs N
	// migrations against the same target with different
	// --target-schema values) doesn't collide on auto-derived IDs.
	d := deriveMigrationID("mysql", "user:pw@tcp(prod-1:3306)/db", "postgres", "postgres://u:p@warehouse:5432/db", "customer_svc")
	e := deriveMigrationID("mysql", "user:pw@tcp(prod-1:3306)/db", "postgres", "postgres://u:p@warehouse:5432/db", "billing_svc")
	if a != b {
		t.Errorf("same input produced different IDs: %q vs %q", a, b)
	}
	if a == c {
		t.Errorf("different host pair produced identical IDs: %q", a)
	}
	if a == d {
		t.Errorf("--target-schema discriminator failed: %q matched no-schema baseline", d)
	}
	if d == e {
		t.Errorf("different --target-schema produced identical IDs: %q", d)
	}
	if !strings.HasPrefix(a, "auto-") {
		t.Errorf("derived ID does not start with 'auto-': %q", a)
	}
	if len(a) > 255 {
		t.Errorf("derived ID exceeds VARCHAR(255) bound: len=%d", len(a))
	}
}

// TestTruncateLastError clamps overlong messages to the 1KB limit.
func TestTruncateLastError(t *testing.T) {
	short := "short error"
	if got := truncateLastError(short); got != short {
		t.Errorf("short input mutated: %q -> %q", short, got)
	}
	long := strings.Repeat("x", lastErrorMaxLen+200)
	got := truncateLastError(long)
	if len(got) > lastErrorMaxLen {
		t.Errorf("truncated len = %d; want <= %d", len(got), lastErrorMaxLen)
	}
	if !strings.HasSuffix(got, "…") {
		t.Errorf("truncated value missing ellipsis: tail=%q", got[len(got)-3:])
	}
}
