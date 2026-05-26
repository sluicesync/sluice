//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0054 Shape A Phase 2e — MySQL counterpart of the multi-shard
// BoundaryRouter integration suite (task #23).
//
// Mirrors shard_consolidation_router_pg_integration_test.go (PG) against
// real MySQL: real lease primitive + per-shape applier + probe wired
// over real `mysql:8.0` containers. The source side is stubbed (we
// drive RouteBoundary directly rather than spinning up N CDC streams);
// the load-bearing correctness property exercised here is "exactly one
// shard applies; the others observe peer-applied" against the
// ChangeApplier + SchemaWriter impls in internal/engines/mysql.
//
// Why the MySQL counterpart matters: ShapeDeltaApplier + Prober on
// MySQL use detect-then-ALTER (no IF EXISTS on older 8.0.x), and
// information_schema queries scope via DATABASE() rather than schema-
// qualified identifiers. The PG-side integration alone doesn't pin
// either of those engine-specific paths under multi-stream contention.

package pipeline

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"

	// Register the MySQL engine for the test harness.
	_ "github.com/orware/sluice/internal/engines/mysql"
)

// installPhase2eDebugLogger installs a DEBUG-level slog handler on
// os.Stderr for the duration of the test, restoring the prior default
// in Cleanup. Phase A instrumentation for task #65; remove once the
// fix lands. Stderr is what `go test -v` captures into the test log.
func installPhase2eDebugLogger(t *testing.T) {
	t.Helper()
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
}

// TestPhase2e_MySQL_3ShardContention_ExactlyOnceApply boots a single
// mysql:8.0 container (the lease + shape applier + probe all run on
// the same DB; the "shards" are 3 concurrent ChangeApplier + writer
// pairs racing for the same lease row), creates the consolidated
// target table, and fires 3 concurrent RouteBoundary calls. Exactly
// one shard wins the lease and applies the DDL; the other two observe
// peer-applied via checksum-match and return without re-issuing the
// ALTER.
//
// Bug 74 class-pin: the parametric subtest matrix exercises ADD COLUMN
// across multiple IR Type families (Integer, Varchar, Timestamp) +
// CREATE INDEX. A single representative would not catch a future MySQL
// engine-side asymmetry in any of those families (Bug 74's lesson: pin
// the family, not the representative).
func TestPhase2e_MySQL_3ShardContention_ExactlyOnceApply(t *testing.T) {
	cases := []struct {
		name      string
		tableName string
		seedDDL   string
		preIR     *ir.Table
		postIR    *ir.Table
		ddlText   string
		assertion func(t *testing.T, ctx context.Context, db *sql.DB)
	}{
		{
			name:      "add_column_integer_family",
			tableName: "consolidated_int",
			seedDDL:   "CREATE TABLE `consolidated_int` (id INT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			preIR: &ir.Table{Name: "consolidated_int", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
			}},
			postIR: &ir.Table{Name: "consolidated_int", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "tally", Type: ir.Integer{Width: 64}, Nullable: true},
			}},
			ddlText: "ir-schema:consolidated_int:add-column-tally",
			assertion: func(t *testing.T, ctx context.Context, db *sql.DB) {
				assertMySQLColumnExactlyOnce(t, ctx, db, "consolidated_int", "tally")
			},
		},
		{
			name:      "add_column_varchar_family",
			tableName: "consolidated_str",
			seedDDL:   "CREATE TABLE `consolidated_str` (id INT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			preIR: &ir.Table{Name: "consolidated_str", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
			}},
			postIR: &ir.Table{Name: "consolidated_str", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "tagline", Type: ir.Varchar{Length: 128}, Nullable: true},
			}},
			ddlText: "ir-schema:consolidated_str:add-column-tagline",
			assertion: func(t *testing.T, ctx context.Context, db *sql.DB) {
				assertMySQLColumnExactlyOnce(t, ctx, db, "consolidated_str", "tagline")
			},
		},
		{
			name:      "add_column_temporal_family",
			tableName: "consolidated_ts",
			seedDDL:   "CREATE TABLE `consolidated_ts` (id INT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			preIR: &ir.Table{Name: "consolidated_ts", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
			}},
			postIR: &ir.Table{Name: "consolidated_ts", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "noted_at", Type: ir.Timestamp{}, Nullable: true},
			}},
			ddlText: "ir-schema:consolidated_ts:add-column-noted_at",
			assertion: func(t *testing.T, ctx context.Context, db *sql.DB) {
				assertMySQLColumnExactlyOnce(t, ctx, db, "consolidated_ts", "noted_at")
			},
		},
		{
			name:      "create_index_shape",
			tableName: "consolidated_idx",
			seedDDL:   "CREATE TABLE `consolidated_idx` (id INT PRIMARY KEY, sku VARCHAR(64) NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
			preIR: &ir.Table{Name: "consolidated_idx", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "sku", Type: ir.Varchar{Length: 64}},
			}},
			postIR: &ir.Table{Name: "consolidated_idx", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "sku", Type: ir.Varchar{Length: 64}},
			}, Indexes: []*ir.Index{
				{Name: "ix_sku", Columns: []ir.IndexColumn{{Column: "sku"}}},
			}},
			ddlText: "ir-schema:consolidated_idx:create-index-ix_sku",
			assertion: func(t *testing.T, ctx context.Context, db *sql.DB) {
				var n int
				if err := db.QueryRowContext(ctx, `
					SELECT COUNT(DISTINCT index_name) FROM information_schema.statistics
					WHERE table_schema = DATABASE() AND table_name = 'consolidated_idx' AND index_name = 'ix_sku'`).Scan(&n); err != nil {
					t.Fatalf("scan index count: %v", err)
				}
				if n != 1 {
					t.Errorf("ix_sku index count = %d, want 1", n)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runMySQL3ShardContention(t, tc.tableName, tc.seedDDL, tc.preIR, tc.postIR, tc.ddlText, tc.assertion)
		})
	}
}

// runMySQL3ShardContention is the shared scaffolding for the Phase 2e
// MySQL contention shape: bring up the target table, fan out 3 shard
// goroutines that each construct their own applier + writer + lease
// manager + router, each calls RouteBoundary on the same (pre, post)
// IR delta, assert exactly-once apply via the test-case-supplied
// assertion closure.
func runMySQL3ShardContention(
	t *testing.T,
	tableName, seedDDL string,
	preIR, postIR *ir.Table,
	ddlText string,
	assertObservable func(t *testing.T, ctx context.Context, db *sql.DB),
) {
	sourceDSN, _, cleanup := startMySQLBinlog(t)
	defer cleanup()

	// MySQL Phase 2e uses source_db as the consolidated target (the
	// `mysql:8.0` startMySQLBinlog helper already creates source_db +
	// target_db on the container; we re-use source_db as the
	// consolidated target so the test runs against the same DSN every
	// shard goroutine connects to). The choice is purely test-side —
	// production deploys point all 3 shards at a single target DSN.
	dsn := sourceDSN

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	eng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("engines.Get(mysql) returned not-found")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, seedDDL); err != nil {
		t.Fatalf("create target table: %v", err)
	}

	// Pre-create the sluice control tables from a single applier BEFORE
	// fanning out to the shard goroutines. MySQL's CREATE TABLE IF NOT
	// EXISTS does not race the same way PG's pg_type_typname_nsp_index
	// does, but a single pre-create still avoids interleaving the
	// initial ALTER-for-additive-columns migrations (anchor_position /
	// source_engine) the shard goroutines would otherwise each attempt.
	prep, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("prep OpenChangeApplier: %v", err)
	}
	if err := prep.EnsureControlTable(ctx); err != nil {
		t.Fatalf("prep EnsureControlTable: %v", err)
	}
	closeAnyApplier(prep)

	const numShards = 3
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
			results <- router.RouteBoundary(ctx, tableName, preIR, postIR, ddlText, 1, ir.Position{})
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
		t.Fatalf("expected all %d shards to succeed; got %d", numShards, successes)
	}

	assertObservable(t, ctx, db)

	// Lease row should reflect the recorded apply: applied_at set,
	// applied_schema_version = 1, ddl_checksum matches the routed text.
	const leaseQ = `SELECT applied_at IS NOT NULL, applied_schema_version, ddl_checksum
		FROM sluice_shard_consolidation_lease
		WHERE target_table_full_name = ?`
	var (
		applied bool
		version int64
		cksum   string
	)
	if err := db.QueryRowContext(ctx, leaseQ, tableName).Scan(&applied, &version, &cksum); err != nil {
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

// dumpMySQLLeaseRow logs the full row state of the lease control table
// for tableName, so CI logs can correlate test-side state with the
// BoundaryRouter / LeaseManager decision tree. Phase A instrumentation
// for task #65; remove once Phase B fix lands.
func dumpMySQLLeaseRow(t *testing.T, ctx context.Context, db *sql.DB, tableName, label string) {
	t.Helper()
	rows, err := db.QueryContext(ctx, `
		SELECT target_table_full_name,
			COALESCE(lease_holder_stream_id, ''),
			lease_expires_at,
			COALESCE(ddl_text, ''),
			COALESCE(ddl_checksum, ''),
			applied_schema_version,
			applied_at,
			NOW(),
			TIMESTAMPDIFF(SECOND, NOW(), lease_expires_at)
		FROM sluice_shard_consolidation_lease
		WHERE target_table_full_name = ?`, tableName)
	if err != nil {
		t.Logf("phase-a dump (%s): query err: %v", label, err)
		return
	}
	defer func() { _ = rows.Close() }()
	sawRow := false
	for rows.Next() {
		sawRow = true
		var (
			tn, holder, ddlText, cksum string
			expires                    sql.NullTime
			version                    int64
			appliedAt                  sql.NullTime
			now                        sql.NullTime
			secondsToExpiry            sql.NullInt64
		)
		if err := rows.Scan(&tn, &holder, &expires, &ddlText, &cksum, &version, &appliedAt, &now, &secondsToExpiry); err != nil {
			t.Logf("phase-a dump (%s): scan err: %v", label, err)
			return
		}
		t.Logf("phase-a dump (%s) row: table=%q holder=%q expires_at=%v applied_at=%v ddl_text=%q ddl_checksum=%q version=%d now=%v seconds_to_expiry=%v",
			label, tn, holder, expires, appliedAt, ddlText, cksum, version, now, secondsToExpiry)
	}
	if err := rows.Err(); err != nil {
		t.Logf("phase-a dump (%s): rows iter err: %v", label, err)
	}
	if !sawRow {
		t.Logf("phase-a dump (%s): no row for table=%q", label, tableName)
	}
}

// assertMySQLColumnExactlyOnce verifies the named column appears
// exactly once on the named table in the test's MySQL database. Used
// by the Phase 2e contention assertion closures.
func assertMySQLColumnExactlyOnce(t *testing.T, ctx context.Context, db *sql.DB, table, column string) {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
		table, column).Scan(&n); err != nil {
		t.Fatalf("scan column count: %v", err)
	}
	if n != 1 {
		t.Errorf("expected exactly 1 %q column on %q; got %d", column, table, n)
	}
}

// TestPhase2e_MySQL_Takeover_ProbeAndRecord_Applied exercises the
// MySQL counterpart of the PG takeover-Applied integration test:
// shard A applies the ALTER and Releases the lease (simulating a
// crash between ALTER-commit and lease finalize). Shard B's
// RouteBoundary runs probe-and-record, the probe reports Applied (the
// manual ALTER landed), the takeover record-only finalizes the lease
// without re-applying.
func TestPhase2e_MySQL_Takeover_ProbeAndRecord_Applied(t *testing.T) {
	installPhase2eDebugLogger(t) // task #65 Phase A
	sourceDSN, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	dsn := sourceDSN

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	eng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("engines.Get(mysql) returned not-found")
	}

	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE `takeover_target` (id INT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	const targetTable = "takeover_target"

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
	// Manually apply the DDL (simulating shard A's apply phase landing
	// before the finalize crash).
	if _, err := db.ExecContext(ctx, "ALTER TABLE `takeover_target` ADD COLUMN x INT NULL"); err != nil {
		t.Fatalf("manual ALTER: %v", err)
	}
	// Crash simulation: Release without Apply.
	mgrA.Release(ctx, leaseA)

	// Expire the lease via SQL — MySQL TIMESTAMP arithmetic via
	// DATE_SUB to mirror the PG counterpart's "set lease to past"
	// fast-forward (skip the wait for natural TTL expiry).
	if _, err := db.ExecContext(ctx, `UPDATE sluice_shard_consolidation_lease
		SET lease_expires_at = DATE_SUB(NOW(), INTERVAL 1 MINUTE)
		WHERE target_table_full_name = ?`, targetTable); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	// Shard B: takeover via the BoundaryRouter. Probe should report
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

	pre := &ir.Table{Name: "takeover_target", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
	}}
	post := &ir.Table{Name: "takeover_target", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 32}},
		{Name: "x", Type: ir.Integer{Width: 32}, Nullable: true},
	}}
	// Phase A instrumentation (task #65): dump the lease row state
	// right before shard-b's RouteBoundary so CI logs correlate
	// test-side state with the router/lease-manager decision tree.
	dumpMySQLLeaseRow(t, ctx, db, targetTable, "pre-shard-b-RouteBoundary")
	if err := router.RouteBoundary(ctx, targetTable, pre, post, "ir-schema:takeover_target:add-x", 1, ir.Position{}); err != nil {
		dumpMySQLLeaseRow(t, ctx, db, targetTable, "post-shard-b-RouteBoundary-err")
		t.Fatalf("RouteBoundary takeover: %v", err)
	}

	// Column still exists exactly once (no duplicate apply).
	assertMySQLColumnExactlyOnce(t, ctx, db, "takeover_target", "x")

	// Lease row should be Applied.
	var applied bool
	if err := db.QueryRowContext(ctx, `
		SELECT applied_at IS NOT NULL
		FROM sluice_shard_consolidation_lease
		WHERE target_table_full_name = ?`, targetTable).Scan(&applied); err != nil {
		t.Fatalf("scan lease: %v", err)
	}
	if !applied {
		t.Error("expected lease to be Applied after MySQL takeover record-only")
	}
}
