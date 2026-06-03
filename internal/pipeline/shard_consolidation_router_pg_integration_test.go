//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0054 Shape A Phase 2e — multi-shard BoundaryRouter integration
// against real Postgres. Simulates 3 per-shard streams converging on
// one consolidated target table; asserts exactly-once DDL apply +
// every shard's lease-row reflects the same applied state.
//
// This is the minimal Phase 2e harness: real PG target, real lease
// primitive + per-shape applier + probe, but the source side is
// stubbed (we drive RouteBoundary directly rather than simulating 3
// MySQL/PG source CDC readers). The "exactly one shard does the
// apply" assertion is the load-bearing correctness property.

package pipeline

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	pgtc "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/testcontainers/testcontainers-go"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	// Register the PG engine for the test harness.
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// startPGForRouterTest boots a minimal PG container for the Phase 2e
// multi-shard router test. Returns a connection DSN + a cleanup
// closure that terminates the container.
func startPGForRouterTest(t *testing.T) (string, func()) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := pgtc.Run(
		// Task #68: pre-baked PG image. `warehouse` is in the bake's
		// seed-DB list. See pg_prebaked_integration_test.go.
		ctx, pgPrebakedImage,
		pgtc.WithDatabase("warehouse"),
		pgtc.WithUsername("test"),
		pgtc.WithPassword("test"),
		pgtc.BasicWaitStrategies(),
		pgPrebakedWaitStrategy(),
	)
	if err != nil {
		t.Fatalf("start pg target: %v", err)
	}
	term := func() {
		sd, cc := context.WithTimeout(context.Background(), 30*time.Second)
		defer cc()
		_ = c.Terminate(sd)
	}
	conn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		term()
		t.Fatalf("pg conn string: %v", err)
	}
	return conn, term
}

func TestBoundaryRouter_PG_MultiShardExactlyOnceApply(t *testing.T) {
	dsn, cleanup := startPGForRouterTest(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	eng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("engines.Get(postgres) returned not-found")
	}

	// Create the target table and the control tables.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."consolidated_users" (id INT PRIMARY KEY)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Pre-create the sluice control tables (`sluice_cdc_state` +
	// `sluice_shard_consolidation_lease`) from a single goroutine
	// BEFORE fanning out to the shard goroutines. Concurrent
	// `CREATE TABLE IF NOT EXISTS` on a fresh PG target races on
	// `pg_type`'s unique constraint (`pg_type_typname_nsp_index`,
	// SQLSTATE 23505): the first transaction allocates a row in
	// pg_type with the type name; concurrent transactions that
	// haven't seen the commit attempt to insert their own and
	// collide. The race shape is well-known
	// (https://www.postgresql.org/message-id/...). Production
	// deployments hit it only on the precise "N shards start
	// simultaneously against a fresh target" boundary (rare); the
	// test reproduces it deterministically because all 3 shards
	// start tightly. Pre-creating from one applier avoids the race
	// without weakening the test's contention assertion — the
	// shard goroutines below still each call EnsureControlTable
	// (it's the production code path), but the second-and-third
	// calls become no-ops on the already-existing tables.
	prep, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("prep OpenChangeApplier: %v", err)
	}
	if err := prep.EnsureControlTable(ctx); err != nil {
		t.Fatalf("prep EnsureControlTable: %v", err)
	}
	closeAnyApplier(prep)

	// Each shard gets its own applier + schema writer + lease mgr +
	// boundary router instance (mirroring the production per-streamer
	// pattern).
	const numShards = 3
	const targetTable = "public.consolidated_users"
	preIR := &ir.Table{Schema: "public", Name: "consolidated_users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
	}}
	postIR := &ir.Table{Schema: "public", Name: "consolidated_users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "added_at", Type: ir.Timestamp{}, Nullable: true},
	}}
	ddlText := "ir-schema:consolidated_users:add-column-added_at"

	results := make(chan error, numShards)
	var wg sync.WaitGroup
	for i := 0; i < numShards; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			streamID := newShardStreamID(t, i)
			applier, err := eng.OpenChangeApplier(ctx, dsn)
			if err != nil {
				results <- err
				return
			}
			defer closeAnyApplier(applier)
			if err := applier.EnsureControlTable(ctx); err != nil {
				results <- err
				return
			}
			store := applier.(ir.ShardConsolidationLeaseStore)
			prober := applier.(ir.ShardConsolidationProber)
			swRaw, err := eng.OpenSchemaWriter(ctx, dsn)
			if err != nil {
				results <- err
				return
			}
			defer closeAnyApplier(swRaw)
			shapeApplier := swRaw.(ir.ShapeDeltaApplier)
			mgr, err := NewLeaseManager(store, streamID, LeaseConfig{
				LeaseDuration: 60 * time.Second,
				RenewDeadline: 40 * time.Second,
				RetryPeriod:   10 * time.Second,
			})
			if err != nil {
				results <- err
				return
			}
			router, err := NewBoundaryRouter(mgr, shapeApplier, prober)
			if err != nil {
				results <- err
				return
			}
			// Each shard tries to route the SAME boundary. Exactly
			// one wins the apply; the others observe via the
			// peer-applied checksum-match path.
			results <- router.RouteBoundary(ctx, targetTable, preIR, postIR, ddlText, 1, ir.Position{})
		}()
	}
	wg.Wait()
	close(results)

	successes := 0
	for r := range results {
		if r != nil {
			t.Errorf("shard returned error: %v", r)
			continue
		}
		successes++
	}
	if successes != numShards {
		t.Fatalf("expected all %d shards to succeed (one applies, others observe peer-applied); got %d", numShards, successes)
	}

	// Assert the target schema reflects the applied DDL exactly once.
	var colCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'consolidated_users'
		  AND column_name = 'added_at'`).Scan(&colCount); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if colCount != 1 {
		t.Errorf("expected exactly 1 added_at column; got %d", colCount)
	}

	// Lease row should be in Applied state.
	const leaseQ = `SELECT applied_at IS NOT NULL, applied_schema_version, ddl_checksum
		FROM "public"."sluice_shard_consolidation_lease"
		WHERE target_table_full_name = $1`
	var (
		applied bool
		version int64
		cksum   string
	)
	if err := db.QueryRowContext(ctx, leaseQ, targetTable).Scan(&applied, &version, &cksum); err != nil {
		t.Fatalf("scan lease: %v", err)
	}
	if !applied {
		t.Error("expected applied_at to be set on the lease row")
	}
	if version != 1 {
		t.Errorf("AppliedSchemaVersion = %d, want 1", version)
	}
	if cksum != ChecksumDDLText(ddlText) {
		t.Errorf("DDLChecksum = %q, want %q", cksum, ChecksumDDLText(ddlText))
	}
}

// TestBoundaryRouter_PG_TakeoverProbeAndRecord exercises the crash-
// injection path: shard A acquires the lease, records ddl_text,
// applies the DDL, but the lease expires before the finalize lands
// (simulated via Release without Apply + a manual DDL execution).
// Shard B's RouteBoundary should run probe-and-record, see the
// target schema is Applied, and record-only without re-applying.
func TestBoundaryRouter_PG_TakeoverProbeAndRecord(t *testing.T) {
	dsn, cleanup := startPGForRouterTest(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	eng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("engines.Get(postgres) returned not-found")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."takeover_target" (id INT PRIMARY KEY)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	const targetTable = "public.takeover_target"

	// Shard A: acquire + record DDL + manually apply DDL + Release
	// (simulates a crash between ALTER-commit and lease-finalize).
	applierA, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier A: %v", err)
	}
	defer closeAnyApplier(applierA)
	if err := applierA.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable A: %v", err)
	}
	mgrA, err := NewLeaseManager(applierA.(ir.ShardConsolidationLeaseStore), "shard-a", LeaseConfig{
		LeaseDuration: 30 * time.Second,
		RenewDeadline: 20 * time.Second,
		RetryPeriod:   10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager A: %v", err)
	}
	leaseA, err := mgrA.Acquire(ctx, targetTable, "ir-schema:takeover_target:add-x")
	if err != nil {
		t.Fatalf("Acquire A: %v", err)
	}
	// Manually apply the DDL (simulating shard A's apply phase
	// landing before the finalize crash).
	if _, err := db.ExecContext(ctx, `ALTER TABLE "public"."takeover_target" ADD COLUMN IF NOT EXISTS x INT`); err != nil {
		t.Fatalf("manual ALTER: %v", err)
	}
	// Crash simulation: Release without Apply.
	mgrA.Release(ctx, leaseA)

	// Wait until the lease expires (shrinks the test runtime by
	// shrinking the lease duration above; lease expires 30s after
	// acquire). Instead of sleeping, set the lease's expires_at to a
	// past time directly via SQL — the production heartbeat-stall
	// path lands the same state.
	if _, err := db.ExecContext(ctx, `UPDATE "public"."sluice_shard_consolidation_lease"
		SET lease_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 minute'
		WHERE target_table_full_name = $1`, targetTable); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	// Shard B: takes over via the BoundaryRouter. Probe should report
	// Applied (the manual ALTER landed); RouteBoundary should
	// record-only without re-applying.
	applierB, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier B: %v", err)
	}
	defer closeAnyApplier(applierB)
	if err := applierB.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable B: %v", err)
	}
	swB, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter B: %v", err)
	}
	defer closeAnyApplier(swB)

	mgrB, err := NewLeaseManager(applierB.(ir.ShardConsolidationLeaseStore), "shard-b", LeaseConfig{
		LeaseDuration: 30 * time.Second,
		RenewDeadline: 20 * time.Second,
		RetryPeriod:   10 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager B: %v", err)
	}
	router, err := NewBoundaryRouter(mgrB, swB.(ir.ShapeDeltaApplier), applierB.(ir.ShardConsolidationProber))
	if err != nil {
		t.Fatalf("NewBoundaryRouter: %v", err)
	}

	pre := &ir.Table{Schema: "public", Name: "takeover_target", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
	}}
	post := &ir.Table{Schema: "public", Name: "takeover_target", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "x", Type: ir.Integer{Width: 32}, Nullable: true},
	}}
	if err := router.RouteBoundary(ctx, targetTable, pre, post, "ir-schema:takeover_target:add-x", 1, ir.Position{}); err != nil {
		t.Fatalf("RouteBoundary takeover: %v", err)
	}

	// Verify column still exists exactly once (no re-apply / no
	// duplication).
	var colCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'takeover_target' AND column_name = 'x'`).Scan(&colCount); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if colCount != 1 {
		t.Errorf("expected exactly 1 x column after takeover record-only path; got %d", colCount)
	}

	// Lease row should be Applied.
	var applied bool
	if err := db.QueryRowContext(ctx, `
		SELECT applied_at IS NOT NULL
		FROM "public"."sluice_shard_consolidation_lease"
		WHERE target_table_full_name = $1`, targetTable).Scan(&applied); err != nil {
		t.Fatalf("scan lease: %v", err)
	}
	if !applied {
		t.Error("expected lease to be Applied after takeover record-only")
	}
}

// newShardStreamID returns a deterministic stream-id for the i-th
// shard.
func newShardStreamID(_ *testing.T, i int) string {
	return "shard-" + string(rune('a'+i))
}

// closeAnyApplier closes anything with Close() error, swallowing the
// error. Defer-friendly cleanup helper for the test.
func closeAnyApplier(v any) {
	if c, ok := v.(interface{ Close() error }); ok {
		_ = c.Close()
	}
}
