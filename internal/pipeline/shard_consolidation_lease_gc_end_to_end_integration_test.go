//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Task #39 — end-to-end ChangeApplier integration test for the v0.76.0
// lease GC sweep's heartbeat-fires-sweep path.
//
// This is the structural regression guard the Bug 85 saga (v0.76.0 →
// v0.77.0 → v0.77.1, three releases of broken-or-wrong-fix) exposed
// the need for. Every prior test bypassed the production wire-up:
//
//   - v0.76.0's [pipeline.shard_consolidation_lease_gc_test.go] unit
//     pins exercise [pipeline.SweepConsolidationLeases] directly with
//     fakes — they would have passed even though [WithGC] was never
//     called from [engageShardCoordination].
//   - v0.76.0's [engines/postgres/shard_consolidation_lease_gc_integration_test.go]
//     drives [SweepConsolidationLeases] DIRECTLY against a real PG
//     applier — same gap: never engages the streamer's wire-up, never
//     observes a heartbeat fire the sweep on its own.
//   - v0.77.0's [TestEngage_WiresGCDepsWhenAllSurfacesPresent] unit pin
//     asserts `gcDeps != nil` post-engagement but used a stub applier
//     that implemented `PositionAtOrAfter` on itself (the test/production
//     surface mismatch that silently passed v0.77.0 while production
//     was still broken). v0.77.1 removed the misleading stub and the
//     pin now correctly fails when the orderer is on the wrong surface,
//     but the test still uses stubs — a real-engine variant is the only
//     way to guarantee future regressions of this class surface loudly.
//
// What this test does that the prior ones don't:
//
//  1. Drives [Streamer.engageShardCoordination] through its production
//     code path with a REAL [postgres.Engine] (the source-side
//     type-assertion target — Bug 85.b's load-bearing surface) and a
//     REAL [postgres.ChangeApplier] (the lease-store / lister /
//     deleter surface). No stubs of any kind on the lease-coordination
//     path.
//  2. Compile-pins that `gcDeps` is fully populated post-engagement,
//     with each field non-nil. Would have caught Bug 85 AND Bug 85.b
//     immediately — Bug 85 because `gcDeps` would have been nil; Bug
//     85.b because the orderer would have been nil even though the
//     other three surfaces wired up.
//  3. Exercises the heartbeat-fires-sweep path by:
//     - Calling [LeaseManager.Acquire] + [LeaseManager.Apply] on
//       `table_eligible` to land an APPLIED row with a populated
//       `anchor_position`. Apply stops THAT lease's heartbeat goroutine.
//     - Acquiring a SECOND lease on `table_keepalive` (never Applied)
//       whose heartbeat goroutine remains alive and drives the periodic
//       GC sweep.
//     - Writing a `sluice_cdc_state` row whose `source_position` LSN
//       is past `table_eligible`'s anchor LSN.
//     - Polling the lease table until `table_eligible`'s row is deleted
//       by the heartbeat-driven sweep, OR the deadline fires (in which
//       case the regression has returned).
//
// Polling vs blocking on a channel signal: the GC sweep doesn't emit a
// caller-observable signal — the WARN/INFO slog lines are best-effort
// (loud-failure tenet: GC errors stay non-fatal). Polling the lease
// table is the production-shape observation surface (an operator
// inspecting the table sees the same thing). With RetryPeriod=100ms
// and gcEveryNTicks=3 (overridden via the private field, package-
// accessible since the test lives in `pipeline`), the sweep fires
// every ~300ms, so a 30s deadline gives generous slack for container
// startup jitter and PG's per-statement latency under testcontainers.

package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"

	// Register the PG engine for the test harness.
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// e2ePGPos encodes a Postgres-engine position token without importing
// the postgres package's internal helpers. The on-wire shape is the
// JSON {slot, lsn} envelope the PG engine's [decodePGPos] expects.
// Mirrors [internal/engines/postgres/cdc_position.go]'s [pgPos] struct
// — kept here so the test stays a pure pipeline-package consumer.
func e2ePGPos(t *testing.T, slot, lsn string) ir.Position {
	t.Helper()
	type pgPos struct {
		Slot string `json:"slot"`
		LSN  string `json:"lsn"`
	}
	b, err := json.Marshal(pgPos{Slot: slot, LSN: lsn})
	if err != nil {
		t.Fatalf("marshal pgPos: %v", err)
	}
	return ir.Position{Engine: "postgres", Token: string(b)}
}

// TestStreamer_SweepFiresEndToEnd_OnRealPGEngagement is the Bug 85 / 85.b
// regression guard for the streamer-side wire-up of the lease GC
// sweep. Drives [engageShardCoordination] on a Streamer wired with a
// REAL postgres engine + REAL ChangeApplier; asserts that the
// heartbeat-driven sweep actually deletes an eligible APPLIED lease
// row WITHOUT any direct call to [SweepConsolidationLeases] from test
// code.
func TestStreamer_SweepFiresEndToEnd_OnRealPGEngagement(t *testing.T) {
	dsn, cleanup := startPGForRouterTest(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("engines.Get(postgres) returned not-found")
	}

	// Real PG ChangeApplier — the production lease store / lister /
	// deleter / position reader surface. No stubs.
	applier, err := pgEng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer closeAnyApplier(applier)
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	const (
		streamID         = "test-gc-end-to-end"
		eligibleTable    = "public.gc_eligible"
		keepaliveTable   = "public.gc_keepalive"
		slot             = "sluice_slot"
		anchorLSN        = "0/1000000"
		streamPosLSN     = "0/2000000" // strictly past anchorLSN
		ddlText          = "ALTER TABLE gc_eligible ADD COLUMN x INT"
		ddlChecksumDummy = "deadbeef"
	)

	// --- Drive engageShardCoordination through the PRODUCTION code path ---
	//
	// Source AND Target are the real PG Engine value: Source provides
	// the PositionOrderer (Bug 85.b — the surface where the v0.77.0
	// fix went wrong); Target provides the SchemaWriter / ShapeDeltaApplier.
	s := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: dsn,
		TargetDSN: dsn,
		StreamID:  streamID,
		InjectShardColumn: ShardColumnSpec{
			Name:  "source_shard_id",
			Value: "shard_a",
		},
		CoordinateLiveDDL: true,
		ShardCoordinationLease: LeaseConfig{
			// Aggressive cadence for the test: heartbeat every 100ms,
			// renew window 5s, lease TTL 30s. The TTL > RenewDeadline >
			// RetryPeriod invariant ([LeaseConfig.Validate]) holds.
			LeaseDuration: 30 * time.Second,
			RenewDeadline: 5 * time.Second,
			RetryPeriod:   100 * time.Millisecond,
		},
		Applier: applier,
	}

	if err := s.engageShardCoordination(ctx, applier); err != nil {
		t.Fatalf("engageShardCoordination: %v", err)
	}
	defer s.closeShardCoordination()

	// --- Compile-pin the wire-up (would have caught Bug 85 + Bug 85.b) ---
	mgr := s.ShardConsolidationLeaseManager()
	if mgr == nil {
		t.Fatal("ShardConsolidationLeaseManager() = nil; engagement did not construct a manager")
	}
	if mgr.gcDeps == nil {
		t.Fatal("Bug 85 / 85.b regression: mgr.gcDeps is nil after engagement; the heartbeat-loop's GC trigger will never fire")
	}
	if mgr.gcDeps.Lister == nil {
		t.Error("gcDeps.Lister is nil; engagement failed to wire the applier as lister")
	}
	if mgr.gcDeps.Deleter == nil {
		t.Error("gcDeps.Deleter is nil; engagement failed to wire the applier as deleter")
	}
	if mgr.gcDeps.PosReader == nil {
		t.Error("gcDeps.PosReader is nil; engagement failed to wire the applier as position reader")
	}
	if mgr.gcDeps.Orderer == nil {
		t.Fatal("Bug 85.b regression: gcDeps.Orderer is nil; the orderer must come from s.Source (the engine), not from the applier")
	}
	// Sanity: the orderer that wired up must be the source engine's
	// PositionOrderer (i.e. the postgres.Engine, not the applier).
	if _, ok := mgr.gcDeps.Orderer.(ir.PositionOrderer); !ok {
		t.Errorf("gcDeps.Orderer does not satisfy ir.PositionOrderer (type %T)", mgr.gcDeps.Orderer)
	}

	// --- Override gcEveryNTicks for fast-fire test cadence ---
	//
	// The production default is 30 ticks (× 10s default RetryPeriod =
	// every ~5 min). At RetryPeriod=100ms with the production default,
	// the sweep would fire every 3s — workable, but tighter. Setting
	// gcEveryNTicks=3 gives a 300ms sweep cadence; the 30s polling
	// deadline below covers ~100 sweeps' worth of slack.
	mgr.gcEveryNTicks = 3

	// --- Open a direct SQL connection for test-side writes / reads ---
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// --- Step 1: write a CDC stream-position row whose LSN is past the
	// anchor LSN we're about to record. The sweep's safety condition
	// (every stream's position at-or-after the row's anchor) must hold
	// for the row to be eligible.
	streamPos := e2ePGPos(t, slot, streamPosLSN)
	if _, err := db.ExecContext(
		ctx,
		`INSERT INTO "public"."sluice_cdc_state" (stream_id, source_position, updated_at) VALUES ($1, $2, CURRENT_TIMESTAMP)`,
		streamID, streamPos.Token,
	); err != nil {
		t.Fatalf("insert cdc state row: %v", err)
	}

	// --- Step 2: acquire + Apply a lease on the eligible table.
	// Acquire records the ddl_text + starts the heartbeat goroutine;
	// Apply records applied_at + ddl_checksum + applied_schema_version
	// + anchor_position, then STOPS that lease's heartbeat. After Apply,
	// the row is APPLIED with a populated anchor — the GC sweep's
	// only-eligible-shape.
	anchorPos := e2ePGPos(t, slot, anchorLSN)
	eligibleLease, err := mgr.Acquire(ctx, eligibleTable, ddlText)
	if err != nil {
		t.Fatalf("Acquire(eligible): %v", err)
	}
	if err := mgr.Apply(ctx, eligibleLease, 1, ddlText, ddlChecksumDummy, anchorPos); err != nil {
		t.Fatalf("Apply(eligible): %v", err)
	}

	// Sanity: confirm the row landed APPLIED with anchor.
	var (
		preApplied bool
		preAnchor  sql.NullString
	)
	if err := db.QueryRowContext(
		ctx,
		`SELECT applied_at IS NOT NULL, anchor_position
		   FROM "public"."sluice_shard_consolidation_lease"
		   WHERE target_table_full_name = $1`,
		eligibleTable,
	).Scan(&preApplied, &preAnchor); err != nil {
		t.Fatalf("scan pre-sweep eligible row: %v", err)
	}
	if !preApplied {
		t.Fatal("pre-sweep: expected eligible row to be APPLIED")
	}
	if !preAnchor.Valid || preAnchor.String == "" {
		t.Fatal("pre-sweep: expected eligible row to have anchor_position populated")
	}

	// --- Step 3: acquire a second lease (NOT Applied) whose
	// heartbeat goroutine keeps running and drives the periodic GC
	// sweep. Apply on the first lease stopped its own heartbeat; this
	// second one is the live driver.
	keepaliveLease, err := mgr.Acquire(ctx, keepaliveTable, "ALTER TABLE gc_keepalive ADD COLUMN y INT")
	if err != nil {
		t.Fatalf("Acquire(keepalive): %v", err)
	}
	// Release the keepalive lease at test end — its heartbeat goroutine
	// stops via Release (without recording an Apply).
	defer mgr.Release(ctx, keepaliveLease)

	// --- Step 4: poll until the eligible row is gone, or fail loud.
	//
	// Polling shape rationale: the GC sweep doesn't emit a caller-
	// observable signal (slog WARN/INFO lines are best-effort, not
	// channels). Polling the lease table mirrors the production
	// observation surface — an operator inspecting the table sees the
	// same thing. With gcEveryNTicks=3 and RetryPeriod=100ms the sweep
	// fires every ~300ms; 30s of polling = ~100 sweep opportunities,
	// generous slack for container/PG jitter.
	deadline := time.Now().Add(30 * time.Second)
	var lastCount int
	for time.Now().Before(deadline) {
		if err := db.QueryRowContext(
			ctx,
			`SELECT COUNT(*) FROM "public"."sluice_shard_consolidation_lease"
			   WHERE target_table_full_name = $1`,
			eligibleTable,
		).Scan(&lastCount); err != nil {
			t.Fatalf("scan lease count: %v", err)
		}
		if lastCount == 0 {
			// Sweep fired and deleted the eligible row.
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("ctx done while waiting for sweep: %v", ctx.Err())
		case <-time.After(150 * time.Millisecond):
		}
	}
	t.Fatalf("Bug 85 / 85.b regression: heartbeat-driven GC sweep did NOT delete the eligible lease row within 30s "+
		"(last observed count for %q = %d). The wire-up between engageShardCoordination and the heartbeat-loop's "+
		"GC trigger is broken — every prior version of this bug shipped because no end-to-end test exercised this path.",
		eligibleTable, lastCount)
}
