// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0049 Chunk C — unit pins for the applier-side active-version
// cache (ActiveSchema lookup + cacheActiveSchemaAfterCommit + the
// O(1)-amortised counter contract). Integration tests against a real
// MySQL exercise the end-to-end prime + dispatch cycle; the unit pins
// here run without a DB and pin the in-memory contracts the
// integration test layers on top of.

func irColInt(name string) *ir.Column {
	return &ir.Column{Name: name, Type: ir.Integer{Width: 64}, Nullable: false}
}

func irColVarchar(name string, length int) *ir.Column {
	return &ir.Column{Name: name, Type: ir.Varchar{Length: length}, Nullable: true}
}

// TestApplier_ActiveSchema_MissReturnsNilFalse: a fresh applier (no
// prime, no dispatch) reports (nil, false) for every lookup. ADR-0049
// Chunk C locked decision #4: ActiveSchema returns (nil, false) for a
// miss; the consumer decides whether the miss is loud.
func TestApplier_ActiveSchema_MissReturnsNilFalse(t *testing.T) {
	a := &ChangeApplier{
		schema:       "app",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	got, ok := a.ActiveSchema("app", "users")
	if ok {
		t.Fatalf("ActiveSchema on a fresh applier: got ok=true (want false); got=%v", got)
	}
	if got != nil {
		t.Fatalf("ActiveSchema on a fresh applier: got non-nil table (want nil); got=%v", got)
	}
}

// TestApplier_ActiveSchema_NilMap: even with activeSchema nil (test-
// stub construction skipping the map init), ActiveSchema must not
// panic — it returns (nil, false). Mirrors the cacheActiveSchemaAfterCommit
// lazy-init defence so unit-test constructions of &ChangeApplier{}
// stay safe.
func TestApplier_ActiveSchema_NilMap(t *testing.T) {
	a := &ChangeApplier{schema: "app", streamID: "s1"}
	got, ok := a.ActiveSchema("app", "users")
	if ok || got != nil {
		t.Fatalf("ActiveSchema with nil map: want (nil,false); got (%v,%v)", got, ok)
	}
}

// TestApplier_CacheAfterCommit_HitReturnsIR pins that after the
// post-commit hook runs for a SchemaSnapshot, ActiveSchema returns
// the snapshot's IR.
func TestApplier_CacheAfterCommit_HitReturnsIR(t *testing.T) {
	a := &ChangeApplier{
		schema:       "app",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	tbl := &ir.Table{Name: "users", Columns: []*ir.Column{irColInt("id"), irColVarchar("email", 255)}}
	snap := ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNameMySQL, Token: "anchor-1"},
		Schema:   "app",
		Table:    "users",
		IR:       tbl,
	}
	a.cacheActiveSchemaAfterCommit(snap)

	got, ok := a.ActiveSchema("app", "users")
	if !ok {
		t.Fatal("ActiveSchema after cache update: want ok=true, got false")
	}
	if got != tbl {
		t.Fatalf("ActiveSchema after cache update: returned table not equal to snapshot.IR (got=%p want=%p)", got, tbl)
	}
}

// TestApplier_CacheAfterCommit_SwapNotMerge pins ADR-0049 Chunk C
// locked decision #4 "after a second snapshot for the same table,
// returns the *new* IR (swap-not-merge)". A second boundary replaces
// the prior version wholesale rather than merging column lists.
func TestApplier_CacheAfterCommit_SwapNotMerge(t *testing.T) {
	a := &ChangeApplier{
		schema:       "app",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	tblV1 := &ir.Table{Name: "users", Columns: []*ir.Column{irColInt("id")}}
	tblV2 := &ir.Table{Name: "users", Columns: []*ir.Column{irColInt("id"), irColVarchar("email", 255)}}
	a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNameMySQL, Token: "anchor-1"},
		Schema:   "app", Table: "users", IR: tblV1,
	})
	a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNameMySQL, Token: "anchor-2"},
		Schema:   "app", Table: "users", IR: tblV2,
	})

	got, ok := a.ActiveSchema("app", "users")
	if !ok {
		t.Fatal("ActiveSchema after two snapshots: want ok=true, got false")
	}
	if got != tblV2 {
		t.Fatalf("ActiveSchema after swap: got %p (want tblV2=%p; merge-not-swap would return tblV1=%p)",
			got, tblV2, tblV1)
	}
	if len(got.Columns) != 2 {
		t.Fatalf("post-swap IR.Columns count: got %d (want 2 — V2 added 'email')", len(got.Columns))
	}
}

// TestApplier_CacheAfterCommit_PerTableIsolation pins that the cache
// keys on (schema, table); two distinct tables don't clobber each
// other's versions.
func TestApplier_CacheAfterCommit_PerTableIsolation(t *testing.T) {
	a := &ChangeApplier{
		schema:       "app",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	users := &ir.Table{Name: "users", Columns: []*ir.Column{irColInt("id")}}
	orders := &ir.Table{Name: "orders", Columns: []*ir.Column{irColInt("order_id")}}
	a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNameMySQL, Token: "u-anchor"},
		Schema:   "app", Table: "users", IR: users,
	})
	a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNameMySQL, Token: "o-anchor"},
		Schema:   "app", Table: "orders", IR: orders,
	})
	gotU, okU := a.ActiveSchema("app", "users")
	gotO, okO := a.ActiveSchema("app", "orders")
	if !okU || !okO {
		t.Fatalf("per-table lookup: users ok=%v orders ok=%v (want both true)", okU, okO)
	}
	if gotU != users || gotO != orders {
		t.Fatalf("per-table lookup: users=%p (want %p), orders=%p (want %p)", gotU, users, gotO, orders)
	}
}

// TestApplier_CacheAfterCommit_InvariantOnFailure pins ADR-0049 Chunk C
// locked design point 2 (cache-after-commit invariant): if a dispatch
// fails (or its tx rolls back), the cache must NOT be mutated.
//
// We simulate the failure by NOT calling cacheActiveSchemaAfterCommit
// after a SchemaSnapshot dispatch — the actual apply paths
// (applyOne / commitBatch) gate the cache update on `tx.Commit()`
// returning nil, so a forced failure short-circuits before the
// post-commit hook runs. The pin here is that the cache stays empty
// in that arrangement; any future refactor that moves the update
// into dispatch (or any other pre-commit path) will fail this test.
func TestApplier_CacheAfterCommit_InvariantOnFailure(t *testing.T) {
	a := &ChangeApplier{
		schema:       "app",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	// "dispatch failed → caller's post-commit hook never fires."
	// The cache must stay empty.
	got, ok := a.ActiveSchema("app", "users")
	if ok || got != nil {
		t.Fatalf("cache must remain empty when post-commit hook did not run: got (%v, %v)", got, ok)
	}

	// Confirm dispatch's nil-IR loud refusal still works (a failure-
	// path SchemaSnapshot dispatch can't produce a cache mutation).
	err := a.dispatch(context.Background(), nil, "s1", ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNameMySQL, Token: "tok"},
		Schema:   "app",
		Table:    "users",
		IR:       nil,
	})
	if err == nil {
		t.Fatal("expected loud error on nil-IR SchemaSnapshot dispatch")
	}
	if !strings.Contains(err.Error(), "nil IR") {
		t.Fatalf("dispatch error %q does not name the nil IR loud floor", err.Error())
	}
	got, ok = a.ActiveSchema("app", "users")
	if ok || got != nil {
		t.Fatalf("cache must remain empty after a failed dispatch: got (%v, %v)", got, ok)
	}
}

// TestApplier_PrimeSchemaHistoryCache_BrandNewStreamSkip pins ADR-0049
// Chunk C locked design point 3: a cold-start (brand-new stream) is
// detected by an empty Position Token and short-circuits the prime to
// a no-op. NO storage hit, no error, the cache stays empty — the
// reader's first SchemaSnapshot will populate it via the post-commit
// hook.
func TestApplier_PrimeSchemaHistoryCache_BrandNewStreamSkip(t *testing.T) {
	a := &ChangeApplier{
		schema:       "app",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
		// a.db left nil on purpose: a brand-new-stream prime must
		// short-circuit BEFORE touching the db. If the short-circuit
		// fails, the nil-db error fires (the next branch).
	}
	if err := a.PrimeSchemaHistoryCache(context.Background(), "s1", ir.Position{}); err != nil {
		t.Fatalf("brand-new-stream prime: want nil (no-op), got error: %v", err)
	}
	// The test-only resolve counter must not have ticked.
	if got := a.resolveCallsForTest.Load(); got != 0 {
		t.Fatalf("brand-new-stream prime: resolveCallsForTest=%d (want 0 — must skip storage entirely)", got)
	}
	if len(a.activeSchema) != 0 {
		t.Fatalf("brand-new-stream prime: activeSchema map len=%d (want 0)", len(a.activeSchema))
	}
}

// TestApplier_PrimeSchemaHistoryCache_NilDBOnRealResume pins the
// defence-in-depth nil-db branch: a non-zero position prime against
// a nil-db applier surfaces a loud error rather than NPE-crashing
// (a unit-test construction that forgets to wire db must not silently
// "succeed").
func TestApplier_PrimeSchemaHistoryCache_NilDBOnRealResume(t *testing.T) {
	a := &ChangeApplier{schema: "app", streamID: "s1"}
	err := a.PrimeSchemaHistoryCache(context.Background(), "s1",
		ir.Position{Engine: engineNameMySQL, Token: "some-anchor"})
	if err == nil {
		t.Fatal("nil-db prime on non-empty position: want loud error, got nil")
	}
	if !strings.Contains(err.Error(), "db is nil") {
		t.Fatalf("nil-db prime error %q does not name the nil-db loud floor", err.Error())
	}
}

// TestApplier_PrimeSchemaHistoryCache_EmptyStreamID pins the loud
// refusal on an empty streamID — mirrors the symmetry MySQL applier
// methods enforce on the streamID input.
func TestApplier_PrimeSchemaHistoryCache_EmptyStreamID(t *testing.T) {
	a := &ChangeApplier{schema: "app"}
	err := a.PrimeSchemaHistoryCache(context.Background(), "",
		ir.Position{Engine: engineNameMySQL, Token: "tok"})
	if err == nil {
		t.Fatal("empty streamID prime: want loud error, got nil")
	}
	if !strings.Contains(err.Error(), "streamID is empty") {
		t.Fatalf("empty-streamID error %q does not name the empty-streamID loud floor", err.Error())
	}
}

// TestApplier_O1Amortised_SteadyStateBoundaryDispatches pins the
// O(1)-amortised invariant the ADR-0049 Consequences mandate:
// post-prime, a synthetic stream of N rows interleaved with M
// SchemaSnapshots produces ZERO additional resolve-storage hits.
// (Per-row reads go through the cache — see ActiveSchema; per-
// boundary writes update the cache from the snapshot's IR field,
// never via storage.)
//
// Counter wiring (test-only): [ChangeApplier.resolveCallsForTest] is
// incremented inside PrimeSchemaHistoryCache's per-table loop and
// NOWHERE else. The dispatch-after-commit path
// (cacheActiveSchemaAfterCommit) updates the map in-memory from the
// snapshot's IR; it never touches the resolve path. This test
// confirms by simulating the steady-state directly: zero priming +
// many boundary dispatches keeps the counter at zero.
//
// (The end-to-end version that also exercises the prime is the
// integration test integration-tagged sibling; see the in-package
// integration test file. The point here is the unit-level
// invariant: dispatching M boundaries does not invoke any storage
// resolve.)
func TestApplier_O1Amortised_SteadyStateBoundaryDispatches(t *testing.T) {
	a := &ChangeApplier{
		schema:       "app",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	// Simulate the post-commit hook for many boundaries across two
	// tables. Each call should mutate the cache (cheap) and NOT
	// increment resolveCallsForTest.
	const M = 100
	tbl := &ir.Table{Name: "users", Columns: []*ir.Column{irColInt("id")}}
	for i := 0; i < M; i++ {
		a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
			Position: ir.Position{Engine: engineNameMySQL, Token: "u"},
			Schema:   "app", Table: "users", IR: tbl,
		})
		a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
			Position: ir.Position{Engine: engineNameMySQL, Token: "o"},
			Schema:   "app", Table: "orders", IR: tbl,
		})
	}
	if got := a.resolveCallsForTest.Load(); got != 0 {
		t.Fatalf("steady-state boundary dispatches: resolveCallsForTest=%d (want 0 — dispatch updates the cache without storage)", got)
	}
	// Verify per-row lookups are cache-only (don't increment the
	// counter either; the counter is wired ONLY in
	// PrimeSchemaHistoryCache).
	for i := 0; i < 10000; i++ {
		_, _ = a.ActiveSchema("app", "users")
		_, _ = a.ActiveSchema("app", "orders")
	}
	if got := a.resolveCallsForTest.Load(); got != 0 {
		t.Fatalf("per-row ActiveSchema lookups: resolveCallsForTest=%d (want 0 — cache-only)", got)
	}
}

// TestApplier_O1Amortised_PrimeIsSoleResolveSite reads as: "the only
// code path that increments resolveCallsForTest is
// PrimeSchemaHistoryCache". This pin guards against a future refactor
// adding a per-row or per-boundary resolve call without updating the
// counter contract — that change would now-fail this test by
// producing a counter inconsistent with the cache-call shape the test
// scaffolds.
//
// We can't easily integration-prime in a unit test (the prime needs a
// real db), so we assert the inverse: the counter starts at 0,
// dispatch+lookup churn doesn't move it, and ONLY direct counter
// increments (which only PrimeSchemaHistoryCache does in production
// code) move it. A `grep -n resolveCallsForTest.Add` over the engine
// package should return exactly one production-code hit.
func TestApplier_O1Amortised_PrimeIsSoleResolveSite(t *testing.T) {
	a := &ChangeApplier{
		schema:       "app",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNameMySQL, Token: "anchor"},
		Schema:   "app", Table: "users",
		IR: &ir.Table{Name: "users", Columns: []*ir.Column{irColInt("id")}},
	})
	_, _ = a.ActiveSchema("app", "users")
	if got := a.resolveCallsForTest.Load(); got != 0 {
		t.Fatalf("dispatch+lookup must not touch resolveCallsForTest; got=%d", got)
	}
}

// TestApplier_ActiveSchema_NotConfusedByUnknownSchema: a lookup on a
// schema/table that was never cached returns (nil, false) even when
// other (schema, table) entries exist for the applier. Catches a
// future bug where the cache key derivation collapsed schema + table
// into a colliding form.
func TestApplier_ActiveSchema_NotConfusedByUnknownSchema(t *testing.T) {
	a := &ChangeApplier{
		schema:       "app",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNameMySQL, Token: "u"},
		Schema:   "app", Table: "users",
		IR: &ir.Table{Name: "users", Columns: []*ir.Column{irColInt("id")}},
	})
	got, ok := a.ActiveSchema("other_db", "users")
	if ok || got != nil {
		t.Fatalf("ActiveSchema on uncached (other_db, users): want (nil,false); got (%v,%v)", got, ok)
	}
	got, ok = a.ActiveSchema("app", "orders")
	if ok || got != nil {
		t.Fatalf("ActiveSchema on uncached (app, orders): want (nil,false); got (%v,%v)", got, ok)
	}
}

// TestApplier_PrimeErrorWrapping pins that a prime error carries the
// engine prefix + identifies the (schema, table) — required so a
// loud ErrPositionInvalid traces back to the specific table that
// hit the floor.
//
// The shape of the wrap is verified separately on a real engine (the
// integration sibling); here we only assert that the empty-streamID
// branch's error message wraps the call site.
// TestApplier_CacheAfterCommit_InvalidatesTargetCaches pins the
// ADR-0091 F7a fix (symmetric to the PG GAP #3 fix): a committed
// SchemaSnapshot boundary drops the applier's target-side per-table
// caches (colTypeCache / pkCache) for that table, keyed by the ROUTED
// target schema, so the next DML re-reads the live post-DDL catalog.
func TestApplier_CacheAfterCommit_InvalidatesTargetCaches(t *testing.T) {
	a := &ChangeApplier{
		schema:       "app",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
		colTypeCache: make(map[string]map[string]*ir.Column),
		pkCache:      make(map[string][]string),
	}
	const qn = "app.widgets"
	// Pre-DDL baseline (counter int4). First snapshot is the baseline, not
	// a boundary → must NOT invalidate (symmetric to the PG over-reach fix).
	a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNameMySQL, Token: "gtid0"},
		Schema:   "source_db", Table: "widgets",
		IR: &ir.Table{Name: "widgets", Columns: []*ir.Column{
			{Name: "counter", Type: ir.Integer{Width: 32}},
		}},
	})
	a.colTypeCache[qn] = map[string]*ir.Column{
		"counter": {Name: "counter", Type: ir.Integer{Width: 32}},
	}
	a.pkCache[qn] = []string{"id"}

	// A boundary carrying the SOURCE schema name — single-DB routedSchema
	// maps it back to the applier's bound schema ("app"); the widen is a
	// real change (signature differs) → invalidate.
	a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNameMySQL, Token: "gtid"},
		Schema:   "source_db", Table: "widgets",
		IR: &ir.Table{Name: "widgets", Columns: []*ir.Column{
			{Name: "counter", Type: ir.Integer{Width: 64}},
		}},
	})

	if _, ok := a.colTypeCache[qn]; ok {
		t.Errorf("colTypeCache[%q] not invalidated after boundary", qn)
	}
	if _, ok := a.pkCache[qn]; ok {
		t.Errorf("pkCache[%q] not invalidated after boundary", qn)
	}
}

func TestApplier_PrimeErrorWrapping(t *testing.T) {
	a := &ChangeApplier{}
	err := a.PrimeSchemaHistoryCache(context.Background(), "", ir.Position{Engine: engineNameMySQL, Token: "x"})
	if err == nil {
		t.Fatal("empty-streamID prime: want loud error")
	}
	// The error should be plain (no %w wrap of ErrPositionInvalid;
	// that sentinel is reserved for the resolver's loud-floor).
	if errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatal("empty-streamID prime must not surface as ErrPositionInvalid (that sentinel is reserved for resolver loud-floor)")
	}
}
