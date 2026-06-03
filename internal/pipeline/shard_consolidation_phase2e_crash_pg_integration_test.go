//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0054 Shape A Phase 2e — remaining crash-injection boundaries
// (task #23). Sibling to shard_consolidation_router_pg_integration_test.go
// (Phase 2e v1, which covered the Applied takeover path); this file
// covers the two remaining state-machine boundaries:
//
//   * NotApplied — lease holder acquires + records ddl_text but
//     crashes BEFORE the ALTER fires. Lease expires; takeover stream
//     probes → NotApplied → re-applies + records the boundary.
//
//   * Inconsistent — lease holder applies a partial / wrong-shape DDL
//     out-of-band (e.g. ALTER COLUMN TYPE landed with the wrong target
//     type). Takeover stream's probe finds the column with the wrong
//     IR type → ProbeOutcomeInconsistent → router refuses loudly with
//     an operator-actionable message naming table + column +
//     expected-vs-observed type + the drained-model recovery hint per
//     ADR-0054 §DP-E.
//
// "Crash injection" mechanism: rather than embed a feature-flag in
// production code, the tests synthesise the crash state by driving the
// LeaseManager directly (Acquire → record ddl_text → Release without
// Apply) and manipulating lease_expires_at via SQL to fast-forward
// expiry. The combination produces a lease row identical to what a
// real holder process exit would leave behind — applied_at NULL,
// ddl_text populated, lease_expires_at in the past — which the
// takeover stream's BoundaryRouter then processes through the
// production code path. No production hook needed; the crash boundary
// is observable purely from the persisted control-table state.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	// Register the PG engine for the test harness.
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestPhase2e_PG_Takeover_ProbeAndRecord_NotApplied exercises the
// "crash before ALTER" boundary: shard A acquires the lease, records
// ddl_text, then exits before the ALTER reaches the target. Lease
// expires via SQL fast-forward (production: TTL elapses naturally).
// Shard B's RouteBoundary takes over, probes the target → NotApplied,
// re-applies the shape, records the boundary.
//
// Bug 74 class-pin: subtests exercise ADD COLUMN across multiple IR
// type families to confirm no MySQL-style family-dispatched
// asymmetry leaks into the takeover apply path. Each subtest is a
// fresh container so lease + control state is isolated.
func TestPhase2e_PG_Takeover_ProbeAndRecord_NotApplied(t *testing.T) {
	cases := []struct {
		name      string
		addColumn *ir.Column
		seedDDL   string
		tableName string
	}{
		{
			name:      "add_column_integer_family",
			addColumn: &ir.Column{Name: "tally", Type: ir.Integer{Width: 64}, Nullable: true},
			seedDDL:   `CREATE TABLE "public"."na_int" (id INT PRIMARY KEY)`,
			tableName: "public.na_int",
		},
		{
			name:      "add_column_varchar_family",
			addColumn: &ir.Column{Name: "tagline", Type: ir.Varchar{Length: 128}, Nullable: true},
			seedDDL:   `CREATE TABLE "public"."na_str" (id INT PRIMARY KEY)`,
			tableName: "public.na_str",
		},
		{
			name:      "add_column_temporal_family",
			addColumn: &ir.Column{Name: "noted_at", Type: ir.Timestamp{}, Nullable: true},
			seedDDL:   `CREATE TABLE "public"."na_ts" (id INT PRIMARY KEY)`,
			tableName: "public.na_ts",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runPGNotAppliedTakeover(t, tc.tableName, tc.seedDDL, tc.addColumn)
		})
	}
}

// runPGNotAppliedTakeover is the shared crash-before-ALTER scaffold:
// shard A acquires + records ddl_text + Releases (never applies),
// the lease is force-expired, shard B's RouteBoundary takes over and
// re-applies. Asserts: column landed, applied_at set on the lease,
// no duplicate apply.
func runPGNotAppliedTakeover(t *testing.T, targetTable, seedDDL string, addCol *ir.Column) {
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
	if _, err := db.ExecContext(ctx, seedDDL); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Shard A: acquire + record DDL + Release WITHOUT applying. The
	// production crash analog: holder process exits between
	// RecordDDLText and the ShapeDeltaApplier.AlterAddColumn call.
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
	ddlText := "ir-schema:" + targetTable + ":add-" + addCol.Name
	leaseA, err := mgrA.Acquire(ctx, targetTable, ddlText)
	if err != nil {
		t.Fatalf("Acquire A: %v", err)
	}
	// Crash simulation: Release without Apply AND without firing the
	// ALTER. The lease row now has ddl_text populated, applied_at
	// NULL — the exact post-crash state a holder exit between
	// RecordDDLText and the actual ALTER would leave.
	mgrA.Release(ctx, leaseA)

	// Confirm the column does NOT exist on the target — pin the
	// "crash before ALTER" invariant before letting the takeover
	// stream observe it. If something landed the column out of band
	// (e.g. a stray applier in another goroutine), the NotApplied
	// path's assertion would silently be wrong.
	var preCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2 AND column_name = $3`,
		schemaPart(targetTable), tablePart(targetTable), addCol.Name).Scan(&preCount); err != nil {
		t.Fatalf("scan pre count: %v", err)
	}
	if preCount != 0 {
		t.Fatalf("pre-takeover invariant violated: %q column already exists (count=%d)", addCol.Name, preCount)
	}

	// Force-expire the lease via SQL (fast-forward TTL).
	if _, err := db.ExecContext(ctx, `UPDATE "public"."sluice_shard_consolidation_lease"
		SET lease_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 minute'
		WHERE target_table_full_name = $1`, targetTable); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	// Shard B: takes over via the BoundaryRouter. Probe should report
	// NotApplied (the ALTER never fired); RouteBoundary should
	// re-apply and finalize the lease.
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

	pre := &ir.Table{Schema: schemaPart(targetTable), Name: tablePart(targetTable), Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
	}}
	post := &ir.Table{Schema: schemaPart(targetTable), Name: tablePart(targetTable), Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		addCol,
	}}
	if err := router.RouteBoundary(ctx, targetTable, pre, post, ddlText, 1, ir.Position{}); err != nil {
		t.Fatalf("RouteBoundary takeover: %v", err)
	}

	// Column should now exist exactly once — the takeover stream
	// applied the deferred ALTER.
	var postCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2 AND column_name = $3`,
		schemaPart(targetTable), tablePart(targetTable), addCol.Name).Scan(&postCount); err != nil {
		t.Fatalf("scan post count: %v", err)
	}
	if postCount != 1 {
		t.Errorf("expected exactly 1 %q column after takeover re-apply; got %d", addCol.Name, postCount)
	}

	// Lease row should be Applied.
	var applied bool
	var cksum string
	if err := db.QueryRowContext(ctx, `
		SELECT applied_at IS NOT NULL, ddl_checksum
		FROM "public"."sluice_shard_consolidation_lease"
		WHERE target_table_full_name = $1`, targetTable).Scan(&applied, &cksum); err != nil {
		t.Fatalf("scan lease: %v", err)
	}
	if !applied {
		t.Error("expected lease applied_at to be set after NotApplied takeover re-apply")
	}
	if cksum != ChecksumDDLText(ddlText) {
		t.Errorf("lease ddl_checksum = %q, want %q", cksum, ChecksumDDLText(ddlText))
	}
}

// TestPhase2e_PG_Takeover_ProbeAndRecord_Inconsistent exercises the
// loud-refusal boundary: shard A applies the boundary's ALTER but the
// out-of-band session lands the WRONG target type (BIGINT requested,
// TEXT materialised). When shard B's takeover stream probes, the
// column exists but with the wrong IR type → ProbeOutcomeInconsistent
// + the engine probe's expected-vs-observed-type error. The router
// surfaces both the probe-error context AND the drained-model
// recovery hint per ADR-0054 §DP-E.
//
// We use ShapeKindAlterColumnType for this path because the v0.76.0
// ProbeAlterColumnType v2 is the probe that catches wrong-type
// divergence with a rich, operator-actionable error message (the
// ProbeAddColumn / ProbeDropColumn probes are existence-only by
// design and would not surface the wrong-type case as a refusal).
func TestPhase2e_PG_Takeover_ProbeAndRecord_Inconsistent(t *testing.T) {
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
	// Seed: table has an INT amount column; the intended ALTER widens
	// it to BIGINT. Shard A's "crashed" out-of-band session lands a
	// TEXT type instead — the takeover probe should refuse.
	if _, err := db.ExecContext(ctx, `CREATE TABLE "public"."inc_target" (id INT PRIMARY KEY, amount INTEGER)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	const targetTable = "public.inc_target"

	// Shard A: acquire + record DDL + simulate the out-of-band wrong-
	// type ALTER landing (the production crash analog: a runaway
	// administrator-issued ALTER applied to the target while the
	// lease holder was mid-flight; the holder then exits and the
	// next stream takes over).
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
	const ddlText = "ir-schema:public.inc_target:alter-column-amount-bigint"
	leaseA, err := mgrA.Acquire(ctx, targetTable, ddlText)
	if err != nil {
		t.Fatalf("Acquire A: %v", err)
	}
	// Wrong-type ALTER applied out-of-band: INT → TEXT instead of
	// INT → BIGINT. The takeover probe should detect the divergence.
	// USING clause needed because PG can't implicitly cast INTEGER → TEXT.
	if _, err := db.ExecContext(ctx, `ALTER TABLE "public"."inc_target" ALTER COLUMN amount TYPE TEXT USING amount::TEXT`); err != nil {
		t.Fatalf("wrong-type ALTER (out-of-band): %v", err)
	}
	mgrA.Release(ctx, leaseA)

	// Force-expire the lease.
	if _, err := db.ExecContext(ctx, `UPDATE "public"."sluice_shard_consolidation_lease"
		SET lease_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 minute'
		WHERE target_table_full_name = $1`, targetTable); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	// Shard B: takeover. Probe should report Inconsistent + a rich
	// error naming observed-vs-want. Router wraps with the drained-
	// model recovery hint.
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

	pre := &ir.Table{Schema: "public", Name: "inc_target", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "amount", Type: ir.Integer{Width: 32}},
	}}
	// post intends BIGINT (Width:64) — the boundary the lease was
	// recording. The out-of-band ALTER landed TEXT, so the probe
	// should refuse.
	post := &ir.Table{Schema: "public", Name: "inc_target", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "amount", Type: ir.Integer{Width: 64}},
	}}

	err = router.RouteBoundary(ctx, targetTable, pre, post, ddlText, 1, ir.Position{})
	if err == nil {
		t.Fatal("RouteBoundary takeover with wrong-type out-of-band: expected refusal, got nil")
	}

	// Verify the refusal message carries the required operator-
	// actionable context per ADR-0054 §DP-E:
	//   1. The table is named (so the operator can scope the fix).
	//   2. The column is named (so the operator can target the ALTER).
	//   3. The expected vs observed types are surfaced (so the
	//      operator can choose between fixing the target type or
	//      reshaping the source's intent).
	//   4. The drained-model recovery hint is present (the canonical
	//      ADR-0054 §4 escape).
	msg := err.Error()
	for _, expected := range []string{
		"inc_target",    // table name
		"amount",        // column name
		"observed",      // observed-vs-want type framing
		"want",          // observed-vs-want type framing
		"drained model", // recovery hint preamble
		"sluice sync",   // recovery commands
	} {
		if !strings.Contains(msg, expected) {
			t.Errorf("refusal message missing %q; full message:\n%s", expected, msg)
		}
	}

	// Defensive: the probe should NOT have re-applied (the column on
	// the target should still be TEXT, the wrong-type state we
	// induced — the router's refusal is on the inconsistent path,
	// which intentionally does NOT mutate).
	var dataType string
	if err := db.QueryRowContext(ctx, `
		SELECT data_type FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'inc_target' AND column_name = 'amount'`).Scan(&dataType); err != nil {
		t.Fatalf("scan post-refusal type: %v", err)
	}
	if dataType != "text" {
		t.Errorf("post-refusal data_type = %q, want %q (router must not silently fix the divergence)", dataType, "text")
	}

	// Sanity: the lease row should still be unapplied (the router
	// refused; it must not have finalized).
	var applied bool
	if err := db.QueryRowContext(ctx, `
		SELECT applied_at IS NOT NULL
		FROM "public"."sluice_shard_consolidation_lease"
		WHERE target_table_full_name = $1`, targetTable).Scan(&applied); err != nil {
		t.Fatalf("scan lease: %v", err)
	}
	if applied {
		t.Error("lease applied_at should remain NULL after Inconsistent refusal")
	}

	// Defensive: ensure the refusal isn't a wrapped ErrLeaseChecksumMismatch
	// (that's a different loud-failure shape for divergent peer DDL on
	// the contention path; the Inconsistent path here is a takeover
	// refusal, not a peer divergence).
	if errors.Is(err, ErrLeaseChecksumMismatch) {
		t.Errorf("refusal incorrectly classified as ErrLeaseChecksumMismatch: %v", err)
	}
}

// schemaPart returns the schema portion of a `schema.table` qualified
// name. Returns "public" if no dot (matches the lease primitives'
// default).
func schemaPart(qualified string) string {
	idx := strings.IndexByte(qualified, '.')
	if idx < 0 {
		return "public"
	}
	return qualified[:idx]
}

// tablePart returns the table portion of a `schema.table` qualified
// name (the full string if no dot).
func tablePart(qualified string) string {
	idx := strings.IndexByte(qualified, '.')
	if idx < 0 {
		return qualified
	}
	return qualified[idx+1:]
}
