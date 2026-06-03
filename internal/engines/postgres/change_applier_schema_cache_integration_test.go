//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0049 Chunk C — integration pins for the PG applier-side
// active-version cache + boundary swap. Mirrors the MySQL sibling
// (change_applier_schema_cache_integration_test.go).

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"

	// Bug 79 (v0.70.2): the cross-engine pin and the unregistered-
	// source-engine pin exercise PrimeSchemaHistoryCache's
	// engines.Get(currentPos.Engine) dispatch. The PG test binary
	// must therefore have the MySQL engine registered so the
	// "mysql"-source-orderer path resolves to a real
	// ir.PositionOrderer (and so the "made-up-engine" path's loud
	// fail is a genuine miss, not a side-effect of the test binary
	// happening to register only PG). This blank import mirrors the
	// pattern the pipeline package's cross-engine integration tests
	// use (e.g. internal/pipeline/migrate_cross_integration_test.go).
	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

func TestPrimeSchemaHistoryCache_Integration_SeedsFromStorage(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const schema = "public"
	if err := ensureSchemaHistoryTable(ctx, db, schema); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	mkPos := func(lsn string) ir.Position {
		p, err := encodePGPos(pgPos{Slot: "s1", LSN: lsn})
		if err != nil {
			t.Fatalf("encodePGPos: %v", err)
		}
		return p
	}

	anchorUsers := mkPos("0/1000000")
	anchorOrders := mkPos("0/2000000")
	tblUsers := &ir.Table{Schema: schema, Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: true},
	}}
	tblOrders := &ir.Table{Schema: schema, Name: "orders", Columns: []*ir.Column{
		{Name: "order_id", Type: ir.Integer{Width: 64}},
	}}
	if err := writeSchemaVersion(ctx, db, schema, "stream-1", schema, "users", anchorUsers, tblUsers); err != nil {
		t.Fatalf("seed users: %v", err)
	}
	if err := writeSchemaVersion(ctx, db, schema, "stream-1", schema, "orders", anchorOrders, tblOrders); err != nil {
		t.Fatalf("seed orders: %v", err)
	}

	eng := Engine{}
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

	resumePos := mkPos("0/3000000")
	if err := a.PrimeSchemaHistoryCache(ctx, "stream-1", resumePos); err != nil {
		t.Fatalf("PrimeSchemaHistoryCache: %v", err)
	}

	if got := a.resolveCallsForTest.Load(); got != 2 {
		t.Errorf("resolveCallsForTest after prime: got %d, want 2 (one per primed table)", got)
	}

	gotU, okU := a.ActiveSchema(schema, "users")
	gotO, okO := a.ActiveSchema(schema, "orders")
	if !okU || !okO {
		t.Fatalf("ActiveSchema lookup after prime: users ok=%v, orders ok=%v (want both true)", okU, okO)
	}
	if len(gotU.Columns) != 2 || gotU.Columns[1].Name != "email" {
		t.Errorf("users IR after prime: got %d cols (want 2)", len(gotU.Columns))
	}
	if len(gotO.Columns) != 1 || gotO.Columns[0].Name != "order_id" {
		t.Errorf("orders IR after prime: cols=%+v (want 1 col, order_id)", gotO.Columns)
	}
}

func TestPrimeSchemaHistoryCache_Integration_BrandNewStreamIsNoOp(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	eng := Engine{}
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

	if err := a.PrimeSchemaHistoryCache(ctx, "fresh", ir.Position{}); err != nil {
		t.Fatalf("brand-new-stream prime: %v", err)
	}
	if got := a.resolveCallsForTest.Load(); got != 0 {
		t.Errorf("brand-new-stream resolveCallsForTest: got %d, want 0", got)
	}
	if _, ok := a.ActiveSchema("public", "anything"); ok {
		t.Error("ActiveSchema after brand-new-stream prime: expected miss (cache empty)")
	}
}

func TestPrimeSchemaHistoryCache_Integration_ResumeWithNoHistory(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	eng := Engine{}
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

	resumePos, err := encodePGPos(pgPos{Slot: "s1", LSN: "0/9000000"})
	if err != nil {
		t.Fatalf("encodePGPos: %v", err)
	}
	if err := a.PrimeSchemaHistoryCache(ctx, "no-history-stream", resumePos); err != nil {
		t.Fatalf("resume-with-no-history prime: %v", err)
	}
	if got := a.resolveCallsForTest.Load(); got != 0 {
		t.Errorf("resume-with-no-history resolveCallsForTest: got %d, want 0", got)
	}
	if _, ok := a.ActiveSchema("public", "users"); ok {
		t.Error("ActiveSchema after no-history prime: expected miss (cache empty)")
	}
}

func TestPrimeSchemaHistoryCache_Integration_BelowFloorIsLoud(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const schema = "public"
	if err := ensureSchemaHistoryTable(ctx, db, schema); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	mkPos := func(lsn string) ir.Position {
		p, err := encodePGPos(pgPos{Slot: "s1", LSN: lsn})
		if err != nil {
			t.Fatalf("encodePGPos: %v", err)
		}
		return p
	}

	anchor := mkPos("0/2000000")
	tbl := &ir.Table{Schema: schema, Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	if err := writeSchemaVersion(ctx, db, schema, "stream-below", schema, "users", anchor, tbl); err != nil {
		t.Fatalf("seed: %v", err)
	}

	eng := Engine{}
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

	pBelow := mkPos("0/1000000")
	err = a.PrimeSchemaHistoryCache(ctx, "stream-below", pBelow)
	if err == nil {
		t.Fatal("below-floor prime must surface a loud error (got nil)")
	}
	if !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("below-floor prime error must wrap ir.ErrPositionInvalid; got: %v", err)
	}
}

// TestPrimeSchemaHistoryCache_CrossEngine_UsesSourceOrderer is the
// headline Bug 79 regression pin (v0.70.2 hotfix).
//
// Pin shape: seed PG's sluice_cdc_schema_history with a row whose
// source_engine is "mysql" (the cross-engine chain-restore shape: a
// MySQL GTID token persisted under a PG applier). Call
// PrimeSchemaHistoryCache with currentPos.Engine="mysql" — the shape
// retagPositionForSource produces on the cross-engine warm-resume
// path. Pre-fix this would crash with the engine-strict decode reject
// (`decode p: engine = "mysql"; want "postgres"`) because the prime
// hardcoded the PG orderer; the v0.70.2 fix routes orderer selection
// off currentPos.Engine via the engines registry, so the MySQL
// orderer compares MySQL-shape positions correctly.
//
// PASS = prime returns nil; ActiveSchema(schema, table) returns the
// seeded IR. FAIL = the v0.70.1 crash class recurs. This exercises
// the full PrimeSchemaHistoryCache → ResolveSchemaVersion →
// orderer.PositionAtOrAfter path with cross-engine inputs — the path
// the v0.70.1 storage-only pin (TestSchemaHistory_LoadPreservesSourceEngine_CrossEngine)
// does NOT cover.
func TestPrimeSchemaHistoryCache_CrossEngine_UsesSourceOrderer(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const schema = "public"
	if err := ensureSchemaHistoryTable(ctx, db, schema); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	// A cross-engine chain-restore (MySQL src → PG dst → resume) lands a
	// MySQL GTID token in PG's schema-history. The token shape is
	// opaque to the PG applier — what matters here is that the prime
	// path dispatches the MySQL orderer (which can decode the GTID
	// token) rather than the PG orderer (which rejects it loudly).
	const u = "26d1668c-1234-5678-9abc-def012345678"
	anchor := ir.Position{
		Engine: "mysql",
		Token:  `{"mode":"gtid","gtid_set":"` + u + `:1-10"}`,
	}
	tbl := &ir.Table{Schema: schema, Name: "widgets", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "sku", Type: ir.Varchar{Length: 64}, Nullable: true},
	}}
	if err := writeSchemaVersion(ctx, db, schema, "sluice_chain_restore", "src_app", "widgets", anchor, tbl); err != nil {
		t.Fatalf("seed cross-engine row: %v", err)
	}

	eng := Engine{}
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
	// sourceEngineName) output: a MySQL GTID position with Engine
	// re-tagged to "mysql" so the source CDC reader can decode it.
	// The MySQL orderer's GTID superset check makes 1-20 ⊇ 1-10 = true,
	// so the anchor at 1-10 is at-or-before resumePos at 1-20.
	resumePos := ir.Position{
		Engine: "mysql",
		Token:  `{"mode":"gtid","gtid_set":"` + u + `:1-20"}`,
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

// TestPrimeSchemaHistoryCache_UnregisteredSourceEngine_IsLoud pins the
// loud-fail behaviour for an unknown source engine name. A
// currentPos.Engine that doesn't match any registered engine MUST
// surface a named error (NOT ir.ErrPositionInvalid, which is the
// below-floor / cold-start signal) — this is a config-bug class, not
// a recoverable resume event.
//
// Per the loud-failure tenet: a silently-ignored unknown engine name
// would fall back to whatever default the prime might pick and either
// crash later with a misleading error or, worse, silently succeed
// with the wrong orderer. Surface it at the dispatch site.
func TestPrimeSchemaHistoryCache_UnregisteredSourceEngine_IsLoud(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const schema = "public"
	if err := ensureSchemaHistoryTable(ctx, db, schema); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	// Seed one row so distinctSchemaTablesForStream returns >0 — the
	// engine-lookup happens AFTER that early return and must still
	// fire for a populated stream with a bogus currentPos.Engine.
	const u = "26d1668c-1234-5678-9abc-def012345678"
	anchor := ir.Position{
		Engine: "mysql",
		Token:  `{"mode":"gtid","gtid_set":"` + u + `:1-10"}`,
	}
	tbl := &ir.Table{Schema: schema, Name: "widgets", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
	}}
	if err := writeSchemaVersion(ctx, db, schema, "bogus-stream", "src_app", "widgets", anchor, tbl); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	eng := Engine{}
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
// ADR-0049 follow-up task #27 regression pin (PG-side mirror of the
// MySQL sibling).
//
// The applier's dispatch `case ir.SchemaSnapshot` MUST source streamID
// from the Apply method's arg, NOT from `a.streamID` (the field set
// only via the optional [ir.StreamIDSetter]).
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
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	eng := Engine{}
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

	snapPos, err := encodePGPos(pgPos{Slot: "s1", LSN: "0/1000000"})
	if err != nil {
		t.Fatalf("encodePGPos: %v", err)
	}
	snap := ir.SchemaSnapshot{
		Position: snapPos,
		Schema:   "public",
		Table:    "users",
		IR: &ir.Table{Schema: "public", Name: "users", Columns: []*ir.Column{
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
	a := applier.(*ChangeApplier)
	if got := a.streamID; got != "" {
		t.Errorf("a.streamID = %q after Apply (without SetStreamID); want \"\" — "+
			"the pin's premise (dispatch must source from arg, NOT field) is invalidated otherwise", got)
	}

	// Assert: schema-history row landed under the Apply arg's
	// streamID. Pre-task-27 it would land under "" (a.streamID).
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	got, err := loadRetainedSchemaVersions(ctx, db, a.controlSchema, argStreamID, "public", "users")
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
	gotEmpty, err := loadRetainedSchemaVersions(ctx, db, a.controlSchema, "", "public", "users")
	if err != nil {
		t.Fatalf("loadRetainedSchemaVersions(empty streamID): %v", err)
	}
	if len(gotEmpty) != 0 {
		t.Fatalf("schema-history rows under empty streamID: got %d, want 0 — "+
			"dispatch is keying off a.streamID (\"\") instead of the Apply arg (task #27 regression)", len(gotEmpty))
	}
}
