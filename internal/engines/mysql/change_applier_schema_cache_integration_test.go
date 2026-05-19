//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0049 Chunk C — integration pins for the applier-side
// active-version cache + boundary swap. Boots a MySQL container,
// seeds the schema-history control table directly via the Chunk A
// store, then exercises:
//
//   - PrimeSchemaHistoryCache on a non-zero position seeds the cache
//     from storage; the test-only resolve counter increments by
//     exactly #primed-tables (NOT by row count).
//   - A brand-new-stream prime (empty Position) is a no-op (no
//     storage hit; counter stays at 0).
//   - Resume against a non-zero position with no retained versions
//     for the stream → cache stays empty, counter stays at 0 (the
//     loud floor will fire on the FIRST per-row dispatch via the
//     reader's SchemaSnapshot — not here).
//   - Resume against a non-zero position that is BELOW the retention
//     floor for a known table → loud ir.ErrPositionInvalid (DP-2
//     floor → ADR-0022 cold-start).
//
// The O(1)-amortised steady-state pin lives in the unit-tests
// alongside cacheActiveSchemaAfterCommit — it's a structural pin
// (boundary dispatch never touches storage); the integration here is
// the end-to-end "prime hits storage exactly once per table".

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

func TestPrimeSchemaHistoryCache_Integration_SeedsFromStorage(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := ensureSchemaHistoryTable(ctx, db); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	// Seed two tables with one version each.
	const u = "11111111-1111-1111-1111-111111111111"
	anchorUsers := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-10"}`}
	anchorOrders := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-20"}`}

	tblUsers := &ir.Table{Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: true},
	}}
	tblOrders := &ir.Table{Name: "orders", Columns: []*ir.Column{
		{Name: "order_id", Type: ir.Integer{Width: 64}},
	}}

	if err := writeSchemaVersion(ctx, db, "stream-1", "", "users", anchorUsers, tblUsers); err != nil {
		t.Fatalf("seed users: %v", err)
	}
	if err := writeSchemaVersion(ctx, db, "stream-1", "", "orders", anchorOrders, tblOrders); err != nil {
		t.Fatalf("seed orders: %v", err)
	}

	eng := Engine{Flavor: FlavorVanilla}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	a := applier.(*ChangeApplier)

	// Resume from a position at/after both anchors → both versions
	// resolve to the seeded tables.
	resumePos := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-30"}`}
	if err := a.PrimeSchemaHistoryCache(ctx, "stream-1", resumePos); err != nil {
		t.Fatalf("PrimeSchemaHistoryCache: %v", err)
	}

	// One storage hit per primed table = exactly 2. Boundary
	// dispatches will NOT add to this counter (cache-only).
	if got := a.resolveCallsForTest.Load(); got != 2 {
		t.Errorf("resolveCallsForTest after prime: got %d, want 2 (one per primed table)", got)
	}

	gotU, okU := a.ActiveSchema("", "users")
	gotO, okO := a.ActiveSchema("", "orders")
	if !okU || !okO {
		t.Fatalf("ActiveSchema lookup after prime: users ok=%v, orders ok=%v (want both true)", okU, okO)
	}
	if gotU == nil || gotO == nil {
		t.Fatalf("ActiveSchema returned nil tables: users=%v orders=%v", gotU, gotO)
	}
	if len(gotU.Columns) != 2 || gotU.Columns[1].Name != "email" {
		t.Errorf("users IR after prime: got %d cols, col[1]=%q (want 2 cols, email)", len(gotU.Columns), gotU.Columns[1].Name)
	}
	if len(gotO.Columns) != 1 || gotO.Columns[0].Name != "order_id" {
		t.Errorf("orders IR after prime: got cols=%+v (want 1 col, order_id)", gotO.Columns)
	}
}

func TestPrimeSchemaHistoryCache_Integration_BrandNewStreamIsNoOp(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	eng := Engine{Flavor: FlavorVanilla}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	a := applier.(*ChangeApplier)

	// Brand-new-stream sentinel: empty Position. Must short-circuit
	// before touching storage.
	if err := a.PrimeSchemaHistoryCache(ctx, "fresh-stream", ir.Position{}); err != nil {
		t.Fatalf("brand-new-stream prime: %v", err)
	}
	if got := a.resolveCallsForTest.Load(); got != 0 {
		t.Errorf("brand-new-stream resolveCallsForTest: got %d, want 0", got)
	}
	if _, ok := a.ActiveSchema("", "anything"); ok {
		t.Error("ActiveSchema after brand-new-stream prime: expected miss (cache empty)")
	}
}

func TestPrimeSchemaHistoryCache_Integration_ResumeWithNoHistory(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	eng := Engine{Flavor: FlavorVanilla}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	a := applier.(*ChangeApplier)

	// Resume with a non-zero position but no retained versions on the
	// stream — the cache stays empty (no tables to prime). The
	// reader's first SchemaSnapshot will populate the cache via the
	// post-commit hook on the live apply path.
	resumePos := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"00000000-0000-0000-0000-000000000000:1-100"}`}
	if err := a.PrimeSchemaHistoryCache(ctx, "no-history-stream", resumePos); err != nil {
		t.Fatalf("resume-with-no-history prime: %v", err)
	}
	if got := a.resolveCallsForTest.Load(); got != 0 {
		t.Errorf("resume-with-no-history resolveCallsForTest: got %d, want 0", got)
	}
	if _, ok := a.ActiveSchema("", "users"); ok {
		t.Error("ActiveSchema after no-history prime: expected miss (cache empty)")
	}
}

func TestPrimeSchemaHistoryCache_Integration_BelowFloorIsLoud(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := ensureSchemaHistoryTable(ctx, db); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	// Seed a single version anchored at 1-10. A resume position at 1-5
	// (BELOW the floor) must surface ir.ErrPositionInvalid → ADR-0022
	// cold-start path. NEVER silent.
	const u = "11111111-1111-1111-1111-111111111111"
	anchor := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-10"}`}
	tbl := &ir.Table{Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	if err := writeSchemaVersion(ctx, db, "stream-below", "", "users", anchor, tbl); err != nil {
		t.Fatalf("seed: %v", err)
	}

	eng := Engine{Flavor: FlavorVanilla}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	a := applier.(*ChangeApplier)

	// 1-5 is BELOW the seeded floor (1-10).
	pBelow := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-5"}`}
	err = a.PrimeSchemaHistoryCache(ctx, "stream-below", pBelow)
	if err == nil {
		t.Fatal("below-floor prime must surface a loud error (got nil)")
	}
	if !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("below-floor prime error must wrap ir.ErrPositionInvalid; got: %v", err)
	}
}
