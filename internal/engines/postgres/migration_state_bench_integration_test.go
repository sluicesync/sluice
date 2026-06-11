//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0082 checkpoint-cost measurement against real Postgres: what
// ONE per-batch checkpoint costs end-to-end (encode + upsert
// round-trip through MVCC/TOAST) at a 10k-table schema, before vs
// after the per-table-rows split. Written as a scripted-measurement
// TEST rather than a testing.B benchmark because the shared-container
// helpers take *testing.T; the in-process (clone+encode) half of the
// same comparison lives in internal/migratestate's unit benchmarks.
// Measured numbers are recorded in ADR-0082.
//
// The assertion at the end is deliberately loose (per-table must
// simply be faster than legacy) — wall-clock numbers vary by host;
// the t.Logf lines are the measurement output:
//
//	go test -tags=integration -run TestMigrationStateCheckpointCost \
//	    -v ./internal/engines/postgres

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func benchProgressMap(n int) map[string]ir.TableProgress {
	m := make(map[string]ir.TableProgress, n)
	for i := 0; i < n; i++ {
		m[fmt.Sprintf("app_table_%05d", i)] = ir.TableProgress{
			State:      ir.TableProgressInProgress,
			LastPK:     []any{int64(i * 5000)},
			RowsCopied: int64(i * 5000),
		}
	}
	return m
}

func TestMigrationStateCheckpointCost_Measure10k(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg, err := Engine{}.parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer db.Close()

	progress := benchProgressMap(10_000)

	// ---- Legacy path: the ≤v0.99.x per-checkpoint write, verbatim —
	// marshal the WHOLE 10k-entry map, upsert the blob into ONE row
	// (the hand-rolled SQL below is the v0.99.x Write statement).
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE bench_migrate_state_legacy (
			migration_id    VARCHAR(255) NOT NULL,
			phase           VARCHAR(32)  NOT NULL,
			table_progress  TEXT         NULL,
			started_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			last_error      TEXT         NULL,
			PRIMARY KEY (migration_id)
		)`); err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	const legacyUpsert = "INSERT INTO bench_migrate_state_legacy " +
		"(migration_id, phase, table_progress, started_at, updated_at, last_error) " +
		"VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $4) " +
		"ON CONFLICT (migration_id) DO UPDATE SET " +
		"phase = EXCLUDED.phase, table_progress = EXCLUDED.table_progress, " +
		"updated_at = CURRENT_TIMESTAMP, last_error = EXCLUDED.last_error"

	const legacyIters = 50
	var blobBytes int
	legacyStart := time.Now()
	for i := 0; i < legacyIters; i++ {
		blob, err := json.Marshal(progress)
		if err != nil {
			t.Fatal(err)
		}
		blobBytes = len(blob)
		if _, err := db.ExecContext(ctx, legacyUpsert, "bench", "bulk_copy", string(blob), nil); err != nil {
			t.Fatal(err)
		}
	}
	legacyPer := time.Since(legacyStart) / legacyIters

	// ---- New path: one WriteTableProgress upsert per checkpoint,
	// cycling through the same 10k tables so the progress table
	// carries the full 10k-row working set.
	eng := Engine{}
	store, err := eng.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenMigrationStateStore: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	if err := store.Write(ctx, ir.MigrationState{
		MigrationID:   "bench",
		Phase:         ir.MigrationPhaseBulkCopy,
		TableProgress: progress,
	}); err != nil {
		t.Fatalf("seed full snapshot: %v", err)
	}

	const perTableIters = 1000
	perTableStart := time.Now()
	for i := 0; i < perTableIters; i++ {
		name := fmt.Sprintf("app_table_%05d", i%10_000)
		entry := ir.TableProgress{
			State:      ir.TableProgressInProgress,
			LastPK:     []any{int64(i * 5000)},
			RowsCopied: int64(i * 5000),
		}
		if err := store.WriteTableProgress(ctx, "bench", name, entry); err != nil {
			t.Fatal(err)
		}
	}
	perTablePer := time.Since(perTableStart) / perTableIters

	t.Logf("ADR-0082 per-checkpoint cost at 10k tables (real PG, %d/%d iters):", legacyIters, perTableIters)
	t.Logf("  legacy full-blob upsert: %v/checkpoint (%d JSON bytes/checkpoint)", legacyPer, blobBytes)
	t.Logf("  per-table row upsert:    %v/checkpoint (~67 JSON bytes/checkpoint)", perTablePer)
	t.Logf("  speedup: %.1fx, payload reduction: %.0fx", float64(legacyPer)/float64(perTablePer), float64(blobBytes)/67)

	if perTablePer >= legacyPer {
		t.Errorf("per-table checkpoint (%v) not faster than legacy blob checkpoint (%v)", perTablePer, legacyPer)
	}
}
