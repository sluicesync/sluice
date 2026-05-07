//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the per-target sluice_cdc_state control
// table. Two flavors:
//
//   - Schema migration: a v0.2.x-shape table without the new
//     stop_requested_at column should pick up the column on the
//     next EnsureControlTable call, without losing existing rows.
//   - RequestStop / ReadStopRequested: the column round-trips an
//     UPDATE through ReadStopRequested, and an unknown stream id
//     surfaces errStreamNotFound.

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// TestEnsureControlTable_AddsStopRequestedColumn verifies the
// migration path for v0.2.x deployments. We hand-create a control
// table with the pre-v0.3 column set, insert a row, then call
// EnsureControlTable and confirm:
//
//   - The new stop_requested_at column exists.
//   - The previously inserted row is still there with its old
//     position intact.
//
// The detect-then-ALTER path is exercised here (vs. the simpler
// CREATE TABLE path) — that's the codepath we ship for MySQL 8.x
// versions older than 8.0.29 that lack ADD COLUMN IF NOT EXISTS.
func TestEnsureControlTable_AddsStopRequestedColumn(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	// Pre-v0.3 shape: no stop_requested_at column.
	applyMySQLApplier(t, dsn, "CREATE TABLE `sluice_cdc_state` ("+
		"  stream_id       VARCHAR(255) NOT NULL,"+
		"  source_position TEXT         NOT NULL,"+
		"  updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"+
		"  PRIMARY KEY (stream_id)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"+
		"INSERT INTO `sluice_cdc_state` (stream_id, source_position) VALUES ('legacy-stream', 'legacy-token');")

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM   information_schema.COLUMNS
		WHERE  TABLE_SCHEMA = DATABASE()
		  AND  TABLE_NAME   = 'sluice_cdc_state'
		  AND  COLUMN_NAME  = 'stop_requested_at'
	`).Scan(&n); err != nil {
		t.Fatalf("column lookup: %v", err)
	}
	if n != 1 {
		t.Fatalf("stop_requested_at column missing after EnsureControlTable; got count %d", n)
	}

	// Existing row should be preserved with its old token.
	var token string
	var stopReq sql.NullTime
	if err := db.QueryRowContext(ctx,
		"SELECT source_position, stop_requested_at FROM `sluice_cdc_state` WHERE stream_id = ?",
		"legacy-stream",
	).Scan(&token, &stopReq); err != nil {
		t.Fatalf("legacy row select: %v", err)
	}
	if token != "legacy-token" {
		t.Errorf("legacy token = %q; want %q", token, "legacy-token")
	}
	if stopReq.Valid {
		t.Errorf("stop_requested_at on freshly-migrated row should be NULL; got %v", stopReq.Time)
	}

	// Calling EnsureControlTable again is still a no-op.
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("second EnsureControlTable: %v", err)
	}
}

// TestRequestStop_RoundTrips covers the happy path: write a stream
// position via writePositionTx, RequestStop, then observe the flag
// via ReadStopRequested.
func TestRequestStop_RoundTrips(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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

	// Seed a row by writing a position. Reuses the engine's
	// writePositionTx via a manual tx — the applier's normal Apply
	// path goes through this too.
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := writePositionTx(ctx, tx, "test-stream", "tok"); err != nil {
		t.Fatalf("writePositionTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Initially: no stop request.
	myApplier := applier.(*ChangeApplier)
	stop, err := myApplier.ReadStopRequested(ctx, "test-stream")
	if err != nil {
		t.Fatalf("ReadStopRequested(initial): %v", err)
	}
	if stop {
		t.Fatal("ReadStopRequested(initial) = true; want false")
	}

	if err := applier.RequestStop(ctx, "test-stream"); err != nil {
		t.Fatalf("RequestStop: %v", err)
	}

	stop, err = myApplier.ReadStopRequested(ctx, "test-stream")
	if err != nil {
		t.Fatalf("ReadStopRequested(after stop): %v", err)
	}
	if !stop {
		t.Fatal("ReadStopRequested(after stop) = false; want true")
	}

	// Idempotent: second RequestStop should not error.
	if err := applier.RequestStop(ctx, "test-stream"); err != nil {
		t.Errorf("second RequestStop: %v", err)
	}
}

// TestRequestStop_UnknownStreamReturnsSentinel verifies the
// errStreamNotFound branch — operators sometimes typo the stream
// ID; the CLI surfaces a friendly message based on this sentinel.
func TestRequestStop_UnknownStreamReturnsSentinel(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

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

	err = applier.RequestStop(ctx, "no-such-stream")
	if err == nil {
		t.Fatal("RequestStop on unknown stream returned nil; want errStreamNotFound")
	}
	if !errors.Is(err, errStreamNotFound) {
		t.Errorf("RequestStop unknown: err = %v; want errors.Is errStreamNotFound", err)
	}
}
