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
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"

	// Bug 79 (v0.70.2): the cross-engine pin and the unregistered-
	// source-engine pin exercise PrimeSchemaHistoryCache's
	// engines.Get(currentPos.Engine) dispatch. The MySQL test binary
	// must therefore have the Postgres engine registered so the
	// "postgres"-source-orderer path resolves to a real
	// ir.PositionOrderer (and so the "made-up-engine" path's loud
	// fail is a genuine miss, not a side-effect of the test binary
	// happening to register only MySQL). This blank import mirrors
	// the pattern the pipeline package's cross-engine integration
	// tests use (e.g. internal/pipeline/migrate_cross_integration_test.go).
	_ "github.com/orware/sluice/internal/engines/postgres"
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

// TestPrimeSchemaHistoryCache_CrossEngine_UsesSourceOrderer is the
// headline Bug 79 regression pin (v0.70.2 hotfix), MySQL-applier
// mirror of the PG-side test.
//
// Pin shape: seed MySQL's sluice_cdc_schema_history with a row whose
// source_engine is "postgres" (the cross-engine chain-restore shape:
// a PG LSN token persisted under a MySQL applier — the PG→MySQL
// direction of cross-engine chain-restore-then-resume). Call
// PrimeSchemaHistoryCache with currentPos.Engine="postgres" — the
// shape retagPositionForSource produces on cross-engine warm-resume.
// Pre-fix this would crash with the engine-strict decode reject from
// MySQL's orderer (`decode p: engine = "postgres"; want "mysql"`)
// because the prime hardcoded the MySQL orderer; the v0.70.2 fix
// routes orderer selection off currentPos.Engine via the engines
// registry, so the PG orderer compares PG-shape positions correctly.
//
// PASS = prime returns nil; ActiveSchema(schema, table) returns the
// seeded IR. FAIL = the v0.70.1 crash class recurs. This exercises
// the full PrimeSchemaHistoryCache → ResolveSchemaVersion →
// orderer.PositionAtOrAfter path with cross-engine inputs — the path
// the v0.70.1 storage-only pin
// (TestSchemaHistory_LoadPreservesSourceEngine_CrossEngine_MySQL)
// does NOT cover.
func TestPrimeSchemaHistoryCache_CrossEngine_UsesSourceOrderer(t *testing.T) {
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

	// A cross-engine chain-restore (PG src → MySQL dst → resume) lands
	// a PG LSN token in MySQL's schema-history. The token shape is
	// opaque to the MySQL applier — what matters is the prime path
	// dispatches the PG orderer (which can decode the LSN token)
	// rather than the MySQL orderer (which rejects it loudly).
	anchorTok := `{"slot":"s1","lsn":"0/1000000"}`
	anchor := ir.Position{Engine: "postgres", Token: anchorTok}
	tbl := &ir.Table{Name: "widgets", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "sku", Type: ir.Varchar{Length: 64}, Nullable: true},
	}}
	if err := writeSchemaVersion(ctx, db, "sluice_chain_restore", "src_app", "widgets", anchor, tbl); err != nil {
		t.Fatalf("seed cross-engine row: %v", err)
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

	// resumePos is the streamer's retagPositionForSource(persisted,
	// sourceEngineName) output: a PG LSN position with Engine
	// re-tagged to "postgres" so the source CDC reader can decode it.
	// The PG orderer's LSN comparison makes 0/2000000 >= 0/1000000,
	// so the anchor is at-or-before resumePos.
	resumePos := ir.Position{
		Engine: "postgres",
		Token:  `{"slot":"s1","lsn":"0/2000000"}`,
	}
	if err := a.PrimeSchemaHistoryCache(ctx, "sluice_chain_restore", resumePos); err != nil {
		t.Fatalf("Bug 79 regression: cross-engine prime failed: %v", err)
	}

	got, ok := a.ActiveSchema("src_app", "widgets")
	if !ok {
		t.Fatal("ActiveSchema(src_app, widgets) miss after cross-engine prime (want hit)")
	}
	if got == nil {
		t.Fatal("ActiveSchema returned (nil, true) after cross-engine prime")
	}
	if len(got.Columns) != 2 || got.Columns[1].Name != "sku" {
		t.Errorf("cross-engine prime IR: got %d cols, col[1]=%q (want 2 cols, sku)",
			len(got.Columns), got.Columns[1].Name)
	}
}

// TestPrimeSchemaHistoryCache_UnregisteredSourceEngine_IsLoud pins
// the loud-fail behaviour for an unknown source engine name (mirror
// of the PG-side test). A currentPos.Engine that doesn't match any
// registered engine MUST surface a named error (NOT
// ir.ErrPositionInvalid, which is the below-floor / cold-start
// signal) — this is a config-bug class, not a recoverable resume
// event.
func TestPrimeSchemaHistoryCache_UnregisteredSourceEngine_IsLoud(t *testing.T) {
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

	// Seed one row so distinctSchemaTablesForStream returns >0 — the
	// engine-lookup happens AFTER that early return and must still
	// fire for a populated stream with a bogus currentPos.Engine.
	const u = "11111111-1111-1111-1111-111111111111"
	anchor := ir.Position{Engine: engineNameMySQL, Token: `{"mode":"gtid","gtid_set":"` + u + `:1-10"}`}
	tbl := &ir.Table{Name: "widgets", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
	}}
	if err := writeSchemaVersion(ctx, db, "bogus-stream", "", "widgets", anchor, tbl); err != nil {
		t.Fatalf("seed row: %v", err)
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

	bogus := ir.Position{Engine: "made-up-engine", Token: `{"foo":"bar"}`}
	err = a.PrimeSchemaHistoryCache(ctx, "bogus-stream", bogus)
	if err == nil {
		t.Fatal("unregistered source engine prime must surface a loud error (got nil)")
	}
	if errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("unregistered source engine error must NOT wrap ir.ErrPositionInvalid "+
			"(that's the cold-start signal, not the config-bug signal); got: %v", err)
	}
	if !strings.Contains(err.Error(), "made-up-engine") {
		t.Errorf("error message must name the unknown engine; got: %v", err)
	}
	if !strings.Contains(err.Error(), "not registered") {
		t.Errorf("error message must say `not registered`; got: %v", err)
	}
}

// TestApplier_SchemaSnapshotDispatch_UsesApplyArgStreamID is the
// ADR-0049 follow-up task #27 regression pin: the applier's dispatch
// `case ir.SchemaSnapshot` MUST source streamID from the Apply
// method's arg, NOT from `a.streamID` (the field set only via the
// optional [ir.StreamIDSetter]).
//
// Repro shape: open the applier directly and DELIBERATELY do NOT call
// SetStreamID — that simulates a future non-migrate Apply caller (a
// new sync flow, a test stub, a chain-restore variant) that doesn't
// know about the optional StreamIDSetter contract. Push a
// SchemaSnapshot through Apply with an explicit streamID arg, then
// assert the schema-history row landed under that streamID — NOT
// under "" (which is what `a.streamID` would default to without
// SetStreamID).
//
// Bug 78 surfaced the symptom (chain_restore.go was missing the
// SetStreamID call → schema-history rows landed under ""). The
// 411a71c hotfix patched the call site; task #27 closes the latent
// footgun at the source by making the dispatch arg-driven so any
// future non-migrate Apply path is correct by construction.
func TestApplier_SchemaSnapshotDispatch_UsesApplyArgStreamID(t *testing.T) {
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

	// DELIBERATELY DO NOT CALL SetStreamID. This is the whole point
	// of the pin: simulate a future non-migrate Apply caller that
	// hasn't learned about the optional StreamIDSetter contract.
	// a.streamID stays "" — pre-task-27 the dispatch would have keyed
	// the schema-history row under "", silently breaking resume.
	const argStreamID = "custom-non-migrate-stream"

	const u = "11111111-1111-1111-1111-111111111111"
	snap := ir.SchemaSnapshot{
		Position: ir.Position{
			Engine: engineNameMySQL,
			Token:  `{"mode":"gtid","gtid_set":"` + u + `:1-10"}`,
		},
		Schema: "",
		Table:  "users",
		IR: &ir.Table{Name: "users", Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
		}},
	}

	ch := make(chan ir.Change, 1)
	ch <- snap
	close(ch)
	if err := applier.Apply(ctx, argStreamID, ch); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// The applier's a.streamID field MUST still be empty (no
	// SetStreamID was called) — guards against a future "fix" that
	// silently routes the Apply arg through SetStreamID and defeats
	// the test.
	if got := applier.(*ChangeApplier).streamID; got != "" {
		t.Errorf("a.streamID = %q after Apply (without SetStreamID); want \"\" — "+
			"the pin's premise (dispatch must source from arg, NOT field) is invalidated otherwise", got)
	}

	// Assert: schema-history row landed under the Apply arg's
	// streamID. Pre-task-27 it would land under "" (a.streamID).
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	got, err := loadRetainedSchemaVersions(ctx, db, argStreamID, "", "users")
	if err != nil {
		t.Fatalf("loadRetainedSchemaVersions(arg streamID): %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("schema-history rows under arg streamID %q: got %d, want 1 "+
			"(pre-task-27 the dispatch keyed off a.streamID=\"\" so this would be 0)", argStreamID, len(got))
	}

	// Cross-check: the empty-streamID key MUST be empty. If the
	// dispatch had read a.streamID="", the row would be under "" and
	// this would return 1 row.
	gotEmpty, err := loadRetainedSchemaVersions(ctx, db, "", "", "users")
	if err != nil {
		t.Fatalf("loadRetainedSchemaVersions(empty streamID): %v", err)
	}
	if len(gotEmpty) != 0 {
		t.Fatalf("schema-history rows under empty streamID: got %d, want 0 — "+
			"dispatch is keying off a.streamID (\"\") instead of the Apply arg (task #27 regression)", len(gotEmpty))
	}
}
