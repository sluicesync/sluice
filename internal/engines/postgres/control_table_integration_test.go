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

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// TestEnsureControlTable_AddsStopRequestedColumn verifies the
// migration path for v0.2.x deployments. We hand-create a control
// table with the pre-v0.3 column set, insert a row, then call
// EnsureControlTable and confirm:
//
//   - The new stop_requested_at column exists.
//   - The previously inserted row is still there with its old
//     position intact.
func TestEnsureControlTable_AddsStopRequestedColumn(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	// Pre-v0.3 shape: no stop_requested_at column.
	applyPGApplier(t, dsn, `
		CREATE TABLE "public"."sluice_cdc_state" (
			stream_id       VARCHAR(255) NOT NULL,
			source_position TEXT         NOT NULL,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (stream_id)
		);
		INSERT INTO "public"."sluice_cdc_state" (stream_id, source_position)
		VALUES ('legacy-stream', 'legacy-token');
	`)

	eng := Engine{}
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

	// Column should now exist.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM   information_schema.columns
		WHERE  table_schema = 'public'
		  AND  table_name   = 'sluice_cdc_state'
		  AND  column_name  = 'stop_requested_at'
	`).Scan(&n); err != nil {
		t.Fatalf("column lookup: %v", err)
	}
	if n != 1 {
		t.Fatalf("stop_requested_at column missing after EnsureControlTable; got count %d", n)
	}

	// Existing row should be preserved with its old token.
	var token string
	var stopReq sql.NullTime
	if err := db.QueryRowContext(
		ctx,
		`SELECT source_position, stop_requested_at FROM "public"."sluice_cdc_state" WHERE stream_id = $1`,
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
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	eng := Engine{}
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
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := writePositionTx(ctx, tx, "public", "test-stream", "tok", "", "", ""); err != nil {
		t.Fatalf("writePositionTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Initially: no stop request.
	pgApplier := applier.(*ChangeApplier)
	stop, err := pgApplier.ReadStopRequested(ctx, "test-stream")
	if err != nil {
		t.Fatalf("ReadStopRequested(initial): %v", err)
	}
	if stop {
		t.Fatal("ReadStopRequested(initial) = true; want false")
	}

	if err := applier.RequestStop(ctx, "test-stream"); err != nil {
		t.Fatalf("RequestStop: %v", err)
	}

	stop, err = pgApplier.ReadStopRequested(ctx, "test-stream")
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
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	eng := Engine{}
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

// TestEnsureControlTable_AddsTargetSchemaColumn verifies the Bug 46
// migration path: a v0.25.0-shape control table without target_schema
// picks up the column on the next EnsureControlTable call without
// losing existing rows. Mirrors the v0.24.0 slot_name and v0.25.0
// source_dsn_fingerprint migrations.
func TestEnsureControlTable_AddsTargetSchemaColumn(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	// Pre-Bug-46 shape: no target_schema column. Includes all columns
	// added prior to v0.25.1 so the migration arm-by-arm test pins the
	// new column ADD without colliding with the older alters.
	applyPGApplier(t, dsn, `
		CREATE TABLE "public"."sluice_cdc_state" (
			stream_id              VARCHAR(255) NOT NULL,
			source_position        TEXT         NOT NULL,
			updated_at             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			stop_requested_at      TIMESTAMP    NULL,
			slot_name              TEXT         NULL,
			source_dsn_fingerprint TEXT         NULL,
			PRIMARY KEY (stream_id)
		);
		INSERT INTO "public"."sluice_cdc_state"
			(stream_id, source_position, slot_name, source_dsn_fingerprint)
		VALUES ('legacy-stream', 'legacy-token', 'sluice_slot', 'abcd1234ef56');
	`)

	eng := Engine{}
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

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM   information_schema.columns
		WHERE  table_schema = 'public'
		  AND  table_name   = 'sluice_cdc_state'
		  AND  column_name  = 'target_schema'
	`).Scan(&n); err != nil {
		t.Fatalf("column lookup: %v", err)
	}
	if n != 1 {
		t.Fatalf("target_schema column missing after EnsureControlTable; count = %d", n)
	}

	// Existing row still there with its prior values intact;
	// target_schema starts NULL on the freshly-migrated row.
	var (
		token       string
		slot        sql.NullString
		fingerprint sql.NullString
		ts          sql.NullString
	)
	if err := db.QueryRowContext(
		ctx,
		`SELECT source_position, slot_name, source_dsn_fingerprint, target_schema
		 FROM   "public"."sluice_cdc_state" WHERE stream_id = $1`,
		"legacy-stream",
	).Scan(&token, &slot, &fingerprint, &ts); err != nil {
		t.Fatalf("legacy row select: %v", err)
	}
	if token != "legacy-token" {
		t.Errorf("legacy token = %q; want %q", token, "legacy-token")
	}
	if !slot.Valid || slot.String != "sluice_slot" {
		t.Errorf("slot = %v; want sluice_slot", slot)
	}
	if !fingerprint.Valid || fingerprint.String != "abcd1234ef56" {
		t.Errorf("fingerprint = %v; want abcd1234ef56", fingerprint)
	}
	if ts.Valid {
		t.Errorf("target_schema on freshly-migrated row should be NULL; got %q", ts.String)
	}

	// Calling EnsureControlTable again is still a no-op (idempotent).
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("second EnsureControlTable: %v", err)
	}
}

// TestWritePositionTx_TargetSchemaRoundTrip verifies the Bug 46
// round-trip: writePositionTx upserts target_schema; listStreams
// returns it via StreamStatus; SetTargetSchema controls the persisted
// value. Empty inputs preserve the existing recorded value (the
// chain-handoff / pre-Bug-46 case) via the COALESCE-on-conflict
// pattern.
func TestWritePositionTx_TargetSchemaRoundTrip(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	eng := Engine{}
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

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// First write: includes target_schema=customer_svc.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := writePositionTx(ctx, tx, "public", "stream-a", "tok-1", "sluice_a", "fp-a", "customer_svc"); err != nil {
		t.Fatalf("first writePositionTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit 1: %v", err)
	}

	streams, err := applier.ListStreams(ctx)
	if err != nil {
		t.Fatalf("ListStreams: %v", err)
	}
	if len(streams) != 1 {
		t.Fatalf("ListStreams returned %d rows; want 1", len(streams))
	}
	if streams[0].TargetSchema != "customer_svc" {
		t.Errorf("StreamStatus.TargetSchema = %q; want customer_svc", streams[0].TargetSchema)
	}

	// Second write with empty target_schema (chain-handoff /
	// pre-streamer WritePosition): COALESCE preserves the prior
	// recorded value rather than clobbering it to NULL.
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin 2: %v", err)
	}
	if err := writePositionTx(ctx, tx, "public", "stream-a", "tok-2", "", "", ""); err != nil {
		t.Fatalf("second writePositionTx: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit 2: %v", err)
	}
	streams, err = applier.ListStreams(ctx)
	if err != nil {
		t.Fatalf("second ListStreams: %v", err)
	}
	if streams[0].TargetSchema != "customer_svc" {
		t.Errorf("after empty-update, TargetSchema = %q; want preserved customer_svc", streams[0].TargetSchema)
	}

	// SetTargetSchema on the applier propagates through Apply-path
	// position writes (here exercised via WritePosition, the same
	// helper).
	pgApplier := applier.(*ChangeApplier)
	pgApplier.SetTargetSchema("billing_svc")
	if err := pgApplier.WritePosition(ctx, "stream-b", ir.Position{Token: "tok-b"}); err != nil {
		t.Fatalf("WritePosition: %v", err)
	}
	streams, err = applier.ListStreams(ctx)
	if err != nil {
		t.Fatalf("ListStreams after SetTargetSchema: %v", err)
	}
	var bSchema string
	for _, s := range streams {
		if s.StreamID == "stream-b" {
			bSchema = s.TargetSchema
		}
	}
	if bSchema != "billing_svc" {
		t.Errorf("stream-b TargetSchema = %q; want billing_svc", bSchema)
	}
}
