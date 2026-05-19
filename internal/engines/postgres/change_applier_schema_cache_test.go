// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// ADR-0049 Chunk C — unit pins for the PG applier-side active-version
// cache. Mirrors the MySQL sibling
// (engines/mysql/change_applier_schema_cache_test.go). End-to-end
// prime + dispatch is exercised by the integration sibling against a
// real PG; the unit pins here run without a DB and pin the in-memory
// contracts.

func pgColInt(name string) *ir.Column {
	return &ir.Column{Name: name, Type: ir.Integer{Width: 64}, Nullable: false}
}

func pgColVarchar(name string, length int) *ir.Column {
	return &ir.Column{Name: name, Type: ir.Varchar{Length: length}, Nullable: true}
}

// TestApplier_ActiveSchema_MissReturnsNilFalse: a fresh applier (no
// prime, no dispatch) reports (nil, false) for every lookup.
func TestApplier_ActiveSchema_MissReturnsNilFalse(t *testing.T) {
	a := &ChangeApplier{
		schema:       "public",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	got, ok := a.ActiveSchema("public", "users")
	if ok || got != nil {
		t.Fatalf("ActiveSchema on a fresh applier: want (nil,false); got (%v,%v)", got, ok)
	}
}

// TestApplier_ActiveSchema_NilMap: ActiveSchema must not panic when
// the map field is nil (test-stub construction).
func TestApplier_ActiveSchema_NilMap(t *testing.T) {
	a := &ChangeApplier{schema: "public", streamID: "s1"}
	got, ok := a.ActiveSchema("public", "users")
	if ok || got != nil {
		t.Fatalf("ActiveSchema with nil map: want (nil,false); got (%v,%v)", got, ok)
	}
}

func TestApplier_CacheAfterCommit_HitReturnsIR(t *testing.T) {
	a := &ChangeApplier{
		schema:       "public",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	tbl := &ir.Table{Name: "users", Columns: []*ir.Column{pgColInt("id"), pgColVarchar("email", 255)}}
	a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNamePostgres, Token: "0/100"},
		Schema:   "public", Table: "users", IR: tbl,
	})
	got, ok := a.ActiveSchema("public", "users")
	if !ok {
		t.Fatal("ActiveSchema after cache update: want ok=true, got false")
	}
	if got != tbl {
		t.Fatalf("ActiveSchema after cache update: returned table not equal to snapshot.IR (got=%p want=%p)", got, tbl)
	}
}

// TestApplier_CacheAfterCommit_SwapNotMerge: a second snapshot for
// the same table replaces the first wholesale (ADR-0049 Chunk C
// locked decision #4 — "returns the *new* IR (swap-not-merge)").
func TestApplier_CacheAfterCommit_SwapNotMerge(t *testing.T) {
	a := &ChangeApplier{
		schema:       "public",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	tblV1 := &ir.Table{Name: "users", Columns: []*ir.Column{pgColInt("id")}}
	tblV2 := &ir.Table{Name: "users", Columns: []*ir.Column{pgColInt("id"), pgColVarchar("email", 255)}}
	a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNamePostgres, Token: "0/100"},
		Schema:   "public", Table: "users", IR: tblV1,
	})
	a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNamePostgres, Token: "0/200"},
		Schema:   "public", Table: "users", IR: tblV2,
	})
	got, ok := a.ActiveSchema("public", "users")
	if !ok || got != tblV2 {
		t.Fatalf("ActiveSchema after swap: got (%p, %v); want tblV2=%p ok=true", got, ok, tblV2)
	}
	if len(got.Columns) != 2 {
		t.Fatalf("post-swap IR.Columns count: got %d (want 2 — V2 added 'email')", len(got.Columns))
	}
}

func TestApplier_CacheAfterCommit_PerTableIsolation(t *testing.T) {
	a := &ChangeApplier{
		schema:       "public",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	users := &ir.Table{Name: "users", Columns: []*ir.Column{pgColInt("id")}}
	orders := &ir.Table{Name: "orders", Columns: []*ir.Column{pgColInt("order_id")}}
	a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNamePostgres, Token: "0/100"},
		Schema:   "public", Table: "users", IR: users,
	})
	a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNamePostgres, Token: "0/200"},
		Schema:   "public", Table: "orders", IR: orders,
	})
	gotU, okU := a.ActiveSchema("public", "users")
	gotO, okO := a.ActiveSchema("public", "orders")
	if !okU || !okO || gotU != users || gotO != orders {
		t.Fatalf("per-table lookup: users=(%p,%v) want (%p,true); orders=(%p,%v) want (%p,true)",
			gotU, okU, users, gotO, okO, orders)
	}
}

// TestApplier_CacheAfterCommit_InvariantOnFailure pins the cache-
// after-commit invariant: a failed dispatch (here, the nil-IR loud
// refusal — ADR-0049 Chunk B locked decision #4b) MUST NOT mutate
// the cache.
func TestApplier_CacheAfterCommit_InvariantOnFailure(t *testing.T) {
	a := &ChangeApplier{
		schema:       "public",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	err := a.dispatch(context.Background(), nil, ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNamePostgres, Token: "0/100"},
		Schema:   "public", Table: "users", IR: nil,
	})
	if err == nil {
		t.Fatal("expected loud error on nil-IR SchemaSnapshot dispatch")
	}
	if !strings.Contains(err.Error(), "nil IR") {
		t.Fatalf("dispatch error %q does not name the nil IR loud floor", err.Error())
	}
	got, ok := a.ActiveSchema("public", "users")
	if ok || got != nil {
		t.Fatalf("cache must remain empty after a failed dispatch: got (%v, %v)", got, ok)
	}
}

// TestApplier_PrimeSchemaHistoryCache_BrandNewStreamSkip: empty Position
// Token short-circuits the prime to a no-op without touching the db.
func TestApplier_PrimeSchemaHistoryCache_BrandNewStreamSkip(t *testing.T) {
	a := &ChangeApplier{
		schema:        "public",
		controlSchema: "public",
		streamID:      "s1",
		activeSchema:  make(map[string]activeSchemaVersion),
		// a.db left nil on purpose — a brand-new-stream prime must
		// short-circuit BEFORE touching the db.
	}
	if err := a.PrimeSchemaHistoryCache(context.Background(), "s1", ir.Position{}); err != nil {
		t.Fatalf("brand-new-stream prime: want nil (no-op), got error: %v", err)
	}
	if got := a.resolveCallsForTest.Load(); got != 0 {
		t.Fatalf("brand-new-stream prime: resolveCallsForTest=%d (want 0)", got)
	}
	if len(a.activeSchema) != 0 {
		t.Fatalf("brand-new-stream prime: activeSchema map len=%d (want 0)", len(a.activeSchema))
	}
}

// TestApplier_PrimeSchemaHistoryCache_NilDBOnRealResume: a non-zero
// position prime against a nil-db applier surfaces a loud error.
func TestApplier_PrimeSchemaHistoryCache_NilDBOnRealResume(t *testing.T) {
	a := &ChangeApplier{schema: "public", controlSchema: "public", streamID: "s1"}
	err := a.PrimeSchemaHistoryCache(context.Background(), "s1",
		ir.Position{Engine: engineNamePostgres, Token: "0/100"})
	if err == nil {
		t.Fatal("nil-db prime on non-empty position: want loud error, got nil")
	}
	if !strings.Contains(err.Error(), "db is nil") {
		t.Fatalf("nil-db prime error %q does not name the nil-db loud floor", err.Error())
	}
}

func TestApplier_PrimeSchemaHistoryCache_EmptyStreamID(t *testing.T) {
	a := &ChangeApplier{schema: "public", controlSchema: "public"}
	err := a.PrimeSchemaHistoryCache(context.Background(), "",
		ir.Position{Engine: engineNamePostgres, Token: "0/100"})
	if err == nil {
		t.Fatal("empty streamID prime: want loud error, got nil")
	}
	if !strings.Contains(err.Error(), "streamID is empty") {
		t.Fatalf("empty-streamID error %q does not name the empty-streamID loud floor", err.Error())
	}
}

// TestApplier_O1Amortised_SteadyStateBoundaryDispatches: post-prime, a
// synthetic stream of N rows interleaved with M SchemaSnapshots
// produces ZERO additional resolve-storage hits. ADR-0049 Consequences
// "must be O(1) amortised — cache the active version, swap on
// boundary."
func TestApplier_O1Amortised_SteadyStateBoundaryDispatches(t *testing.T) {
	a := &ChangeApplier{
		schema:       "public",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	const M = 100
	tbl := &ir.Table{Name: "users", Columns: []*ir.Column{pgColInt("id")}}
	for i := 0; i < M; i++ {
		a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
			Position: ir.Position{Engine: engineNamePostgres, Token: "u"},
			Schema:   "public", Table: "users", IR: tbl,
		})
		a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
			Position: ir.Position{Engine: engineNamePostgres, Token: "o"},
			Schema:   "public", Table: "orders", IR: tbl,
		})
	}
	if got := a.resolveCallsForTest.Load(); got != 0 {
		t.Fatalf("steady-state boundary dispatches: resolveCallsForTest=%d (want 0)", got)
	}
	for i := 0; i < 10000; i++ {
		_, _ = a.ActiveSchema("public", "users")
		_, _ = a.ActiveSchema("public", "orders")
	}
	if got := a.resolveCallsForTest.Load(); got != 0 {
		t.Fatalf("per-row ActiveSchema lookups: resolveCallsForTest=%d (want 0)", got)
	}
}

func TestApplier_O1Amortised_PrimeIsSoleResolveSite(t *testing.T) {
	a := &ChangeApplier{
		schema:       "public",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNamePostgres, Token: "0/100"},
		Schema:   "public", Table: "users",
		IR: &ir.Table{Name: "users", Columns: []*ir.Column{pgColInt("id")}},
	})
	_, _ = a.ActiveSchema("public", "users")
	if got := a.resolveCallsForTest.Load(); got != 0 {
		t.Fatalf("dispatch+lookup must not touch resolveCallsForTest; got=%d", got)
	}
}

func TestApplier_ActiveSchema_NotConfusedByUnknownSchema(t *testing.T) {
	a := &ChangeApplier{
		schema:       "public",
		streamID:     "s1",
		activeSchema: make(map[string]activeSchemaVersion),
	}
	a.cacheActiveSchemaAfterCommit(ir.SchemaSnapshot{
		Position: ir.Position{Engine: engineNamePostgres, Token: "0/100"},
		Schema:   "public", Table: "users",
		IR: &ir.Table{Name: "users", Columns: []*ir.Column{pgColInt("id")}},
	})
	got, ok := a.ActiveSchema("other", "users")
	if ok || got != nil {
		t.Fatalf("ActiveSchema on uncached (other, users): want (nil,false); got (%v,%v)", got, ok)
	}
	got, ok = a.ActiveSchema("public", "orders")
	if ok || got != nil {
		t.Fatalf("ActiveSchema on uncached (public, orders): want (nil,false); got (%v,%v)", got, ok)
	}
}

func TestApplier_PrimeErrorWrapping(t *testing.T) {
	a := &ChangeApplier{}
	err := a.PrimeSchemaHistoryCache(context.Background(), "", ir.Position{Engine: engineNamePostgres, Token: "0/100"})
	if err == nil {
		t.Fatal("empty-streamID prime: want loud error")
	}
	if errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatal("empty-streamID prime must not surface as ErrPositionInvalid")
	}
}
