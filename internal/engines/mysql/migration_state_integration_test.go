//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the per-target sluice_migrate_state table on
// MySQL. Same shape as the postgres-side test plus a small idempotency
// check on EnsureControlTable.

package mysql

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestMigrationStateStoreMySQL_RoundTrip writes a state row, reads
// it back, and confirms the JSON map plus phase/error fields land
// untouched.
func TestMigrationStateStoreMySQL_RoundTrip(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := eng.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenMigrationStateStore: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	// Idempotency: second call is a no-op.
	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("second EnsureControlTable: %v", err)
	}

	want := ir.MigrationState{
		MigrationID: "round-trip",
		Phase:       ir.MigrationPhaseBulkCopy,
		TableProgress: map[string]ir.TableProgress{
			"users":  {State: ir.TableProgressComplete},
			"orders": {State: ir.TableProgressInProgress, LastPK: []any{int64(7)}, RowsCopied: 7},
		},
		LastError: "phase failed: connection reset",
	}
	if err := store.Write(ctx, want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, ok, err := store.Read(ctx, "round-trip")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !ok {
		t.Fatal("Read ok=false after Write")
	}
	if got.Phase != want.Phase {
		t.Errorf("phase = %q; want %q", got.Phase, want.Phase)
	}
	if got.TableProgress["users"].State != ir.TableProgressComplete {
		t.Errorf("TableProgress[users].State = %q; want complete", got.TableProgress["users"].State)
	}
	if got.TableProgress["orders"].State != ir.TableProgressInProgress {
		t.Errorf("TableProgress[orders].State = %q; want in_progress", got.TableProgress["orders"].State)
	}
	if got.TableProgress["orders"].RowsCopied != 7 {
		t.Errorf("TableProgress[orders].RowsCopied = %d; want 7", got.TableProgress["orders"].RowsCopied)
	}
	if got.LastError != want.LastError {
		t.Errorf("LastError = %q; want %q", got.LastError, want.LastError)
	}
}

// TestMigrationStateStoreMySQL_ReadMissingTolerated confirms a read
// against a missing table returns ok=false rather than erroring —
// matches the dry-run / pre-EnsureControlTable inspection shape.
func TestMigrationStateStoreMySQL_ReadMissingTolerated(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := eng.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenMigrationStateStore: %v", err)
	}
	defer func() { _ = store.Close() }()

	// Note: EnsureControlTable deliberately NOT called.
	_, ok, err := store.Read(ctx, "absent")
	if err != nil {
		t.Errorf("Read on missing table errored: %v", err)
	}
	if ok {
		t.Error("Read ok=true on missing table")
	}
}
