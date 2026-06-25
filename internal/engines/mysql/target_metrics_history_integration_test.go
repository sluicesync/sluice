//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the ADR-0107 item 35 sluice_target_metrics_history
// rolling-history table (MySQL store). Boots a MySQL container and asserts
// the ensure → record → list → prune round-trip, with particular care on
// the NULL ⇄ *Known honesty contract: an unobserved metric persists as
// NULL and reconstructs as Known=false on read (never a misleading 0).

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestTargetMetricsHistory_RoundTripPruneNULLs(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	store, ok := applier.(ir.TargetMetricsHistoryStore)
	if !ok {
		t.Fatal("mysql ChangeApplier does not implement ir.TargetMetricsHistoryStore")
	}

	if err := store.EnsureTargetMetricsHistory(ctx); err != nil {
		t.Fatalf("EnsureTargetMetricsHistory: %v", err)
	}
	// Idempotent.
	if err := store.EnsureTargetMetricsHistory(ctx); err != nil {
		t.Fatalf("EnsureTargetMetricsHistory (idempotent): %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)

	// recent: all metrics observed.
	recent := ir.TargetMetricsSample{
		StreamID: "s1", Database: "appdb", Branch: "main",
		SampledAt: now,
		CPUUtil:   0.42, CPUKnown: true,
		MemUtil: 0.70, MemKnown: true,
		StorageUtil: 0.55, StorageAvailableBytes: 5 << 30, StorageCapacityBytes: 10 << 30, StorageKnown: true,
		ReplicaLagSeconds: 1.5, LagKnown: true,
		ActiveConnections: 12, MaxConnections: 100, ConnKnown: true,
	}
	// partial: only CPU observed — mem/storage/lag/conns UNOBSERVED → NULL.
	partial := ir.TargetMetricsSample{
		StreamID:  "s1",
		SampledAt: now.Add(-30 * time.Second),
		CPUUtil:   0.99, CPUKnown: true,
		// everything else *Known=false
	}
	// old: outside the prune window.
	old := ir.TargetMetricsSample{
		StreamID:  "s1",
		SampledAt: now.Add(-48 * time.Hour),
		CPUUtil:   0.10, CPUKnown: true,
	}
	// other stream — must NOT appear in s1's list.
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
		t.Fatalf("want 3 rows for s1 (recent, partial, old), got %d", len(rows))
	}
	// Ordered sampled_at DESC: [recent, partial, old].
	if !rows[0].SampledAt.Equal(now) {
		t.Errorf("rows not ordered DESC; rows[0].SampledAt=%v want %v", rows[0].SampledAt, now)
	}

	// rows[0] = the fully-observed sample: every *Known true, values exact.
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

	// rows[1] = the partial sample: CPU known, EVERYTHING ELSE reconstructed
	// Known=false from NULL (the honesty contract — not a fabricated 0).
	r1 := rows[1]
	if !r1.CPUKnown || r1.CPUUtil != 0.99 {
		t.Errorf("partial CPU: %+v", r1)
	}
	if r1.MemKnown {
		t.Errorf("partial MemKnown should be false (NULL); got %+v", r1)
	}
	if r1.StorageKnown {
		t.Errorf("partial StorageKnown should be false (NULL); got %+v", r1)
	}
	if r1.LagKnown {
		t.Errorf("partial LagKnown should be false (NULL); got %+v", r1)
	}
	if r1.ConnKnown {
		t.Errorf("partial ConnKnown should be false (NULL); got %+v", r1)
	}

	// limit honored.
	if got, err := store.ListTargetMetricsHistory(ctx, "s1", 1); err != nil {
		t.Fatalf("list limit: %v", err)
	} else if len(got) != 1 {
		t.Errorf("limit=1 returned %d rows", len(got))
	}

	// Prune drops the 48h-old row; recent + partial remain.
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
	for _, r := range rows {
		if r.SampledAt.Before(now.Add(-24 * time.Hour)) {
			t.Errorf("pruned-window row survived: %v", r.SampledAt)
		}
	}

	// The NULL columns are genuinely NULL in storage (not 0) — assert via
	// raw SQL so a future scan-side bug can't mask it.
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	var memNull sql.NullFloat64
	if err := db.QueryRowContext(ctx,
		"SELECT mem_util FROM `"+targetMetricsHistoryTableName+"` WHERE stream_id='s1' AND cpu_util=0.99").
		Scan(&memNull); err != nil {
		t.Fatalf("raw mem_util select: %v", err)
	}
	if memNull.Valid {
		t.Errorf("unobserved mem_util stored as %v, want SQL NULL", memNull.Float64)
	}
}

// TestTargetMetricsHistory_ToleratesMissingTable pins that List/Prune
// against a target that never recorded (table absent) degrade to
// empty/no-op rather than erroring — diagnose must assemble a bundle even
// when telemetry was never wired.
func TestTargetMetricsHistory_ToleratesMissingTable(t *testing.T) {
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
