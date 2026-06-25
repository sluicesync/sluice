//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the ADR-0107 item 35 sluice_target_metrics_history
// rolling-history table (Postgres store). Boots a Postgres container and
// asserts the ensure → record → list → prune round-trip, with particular
// care on the NULL ⇄ *Known honesty contract: an unobserved metric
// persists as NULL and reconstructs as Known=false on read (never a 0).

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestTargetMetricsHistory_RoundTripPruneNULLs(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	store, ok := applier.(ir.TargetMetricsHistoryStore)
	if !ok {
		t.Fatal("postgres ChangeApplier does not implement ir.TargetMetricsHistoryStore")
	}

	if err := store.EnsureTargetMetricsHistory(ctx); err != nil {
		t.Fatalf("EnsureTargetMetricsHistory: %v", err)
	}
	// Idempotent.
	if err := store.EnsureTargetMetricsHistory(ctx); err != nil {
		t.Fatalf("EnsureTargetMetricsHistory (idempotent): %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)

	recent := ir.TargetMetricsSample{
		StreamID: "s1", Database: "appdb", Branch: "main",
		SampledAt: now,
		CPUUtil:   0.42, CPUKnown: true,
		MemUtil: 0.70, MemKnown: true,
		StorageUtil: 0.55, StorageAvailableBytes: 5 << 30, StorageCapacityBytes: 10 << 30, StorageKnown: true,
		ReplicaLagSeconds: 1.5, LagKnown: true,
		ActiveConnections: 12, MaxConnections: 100, ConnKnown: true,
	}
	partial := ir.TargetMetricsSample{
		StreamID:  "s1",
		SampledAt: now.Add(-30 * time.Second),
		CPUUtil:   0.99, CPUKnown: true,
		// mem/storage/lag/conns *Known=false → NULL
	}
	old := ir.TargetMetricsSample{
		StreamID:  "s1",
		SampledAt: now.Add(-48 * time.Hour),
		CPUUtil:   0.10, CPUKnown: true,
	}
	other := ir.TargetMetricsSample{StreamID: "s2", SampledAt: now, CPUUtil: 0.5, CPUKnown: true}

	for _, s := range []ir.TargetMetricsSample{recent, partial, old, other} {
		if err := store.RecordTargetMetricsSample(ctx, s); err != nil {
			t.Fatalf("RecordTargetMetricsSample: %v", err)
		}
	}

	rows, err := store.ListTargetMetricsHistory(ctx, "s1", 100)
	if err != nil {
		t.Fatalf("ListTargetMetricsHistory: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("want 3 rows for s1, got %d", len(rows))
	}
	if !rows[0].SampledAt.Equal(now) {
		t.Errorf("rows not ordered DESC; rows[0].SampledAt=%v want %v", rows[0].SampledAt, now)
	}

	r0 := rows[0]
	if !r0.CPUKnown || r0.CPUUtil != 0.42 {
		t.Errorf("CPU: %+v", r0)
	}
	if !r0.MemKnown || r0.MemUtil != 0.70 {
		t.Errorf("Mem: %+v", r0)
	}
	if !r0.StorageKnown || r0.StorageUtil != 0.55 || r0.StorageAvailableBytes != 5<<30 || r0.StorageCapacityBytes != 10<<30 {
		t.Errorf("Storage: %+v", r0)
	}
	if !r0.LagKnown || r0.ReplicaLagSeconds != 1.5 {
		t.Errorf("Lag: %+v", r0)
	}
	if !r0.ConnKnown || r0.ActiveConnections != 12 || r0.MaxConnections != 100 {
		t.Errorf("Conn: %+v", r0)
	}
	if r0.Database != "appdb" || r0.Branch != "main" {
		t.Errorf("db/branch: %+v", r0)
	}

	r1 := rows[1]
	if !r1.CPUKnown || r1.CPUUtil != 0.99 {
		t.Errorf("partial CPU: %+v", r1)
	}
	if r1.MemKnown || r1.StorageKnown || r1.LagKnown || r1.ConnKnown {
		t.Errorf("partial sample should reconstruct all-but-CPU as Known=false (NULL); got %+v", r1)
	}

	if got, err := store.ListTargetMetricsHistory(ctx, "s1", 1); err != nil {
		t.Fatalf("list limit: %v", err)
	} else if len(got) != 1 {
		t.Errorf("limit=1 returned %d rows", len(got))
	}

	if err := store.PruneTargetMetricsHistory(ctx, 24*time.Hour); err != nil {
		t.Fatalf("PruneTargetMetricsHistory: %v", err)
	}
	rows, err = store.ListTargetMetricsHistory(ctx, "s1", 100)
	if err != nil {
		t.Fatalf("ListTargetMetricsHistory after prune: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("after prune want 2 rows, got %d", len(rows))
	}

	// The NULL columns are genuinely NULL in storage — assert via raw SQL.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var memNull sql.NullFloat64
	if err := db.QueryRowContext(ctx,
		`SELECT mem_util FROM "public"."`+targetMetricsHistoryTableName+`" WHERE stream_id='s1' AND cpu_util=0.99`).
		Scan(&memNull); err != nil {
		t.Fatalf("raw mem_util select: %v", err)
	}
	if memNull.Valid {
		t.Errorf("unobserved mem_util stored as %v, want SQL NULL", memNull.Float64)
	}
}

// TestTargetMetricsHistory_ToleratesMissingTable pins that List/Prune
// against a target that never recorded (table absent) degrade to
// empty/no-op rather than erroring.
func TestTargetMetricsHistory_ToleratesMissingTable(t *testing.T) {
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
	store := applier.(ir.TargetMetricsHistoryStore)

	rows, err := store.ListTargetMetricsHistory(ctx, "s1", 100)
	if err != nil {
		t.Fatalf("List on absent table should not error: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("List on absent table should be empty, got %d", len(rows))
	}
	if err := store.PruneTargetMetricsHistory(ctx, time.Hour); err != nil {
		t.Fatalf("Prune on absent table should not error: %v", err)
	}
}
