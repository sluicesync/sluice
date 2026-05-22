//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the ADR-0054 Shape A Phase 2 lease primitive
// against a real MySQL target. Exercises the SELECT-FOR-UPDATE acquire
// path (MySQL has no INSERT ... ON CONFLICT WHERE form), the heartbeat
// extend, and the takeover flow.

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestShardConsolidationLease_EnsureCreatesTable(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
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

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var n int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = DATABASE()
		AND   table_name   = 'sluice_shard_consolidation_lease'`).Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("sluice_shard_consolidation_lease missing after EnsureControlTable; count=%d", n)
	}

	// Idempotent.
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("second EnsureControlTable: %v", err)
	}
}

func TestShardConsolidationLease_AcquireHeartbeatApply(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := a.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := a.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	applier := a.(*ChangeApplier)

	const tableName = "users"
	const streamID = "stream-a"
	expires := time.Now().Add(30 * time.Second)

	acquired, row, err := applier.TryAcquireLease(ctx, tableName, streamID, expires)
	if err != nil {
		t.Fatalf("TryAcquireLease: %v", err)
	}
	if !acquired {
		t.Fatalf("expected acquired=true; got row=%+v", row)
	}

	const ddlText = "ALTER TABLE users ADD COLUMN added_at TIMESTAMP NULL"
	recorded, err := applier.RecordDDLText(ctx, tableName, streamID, ddlText)
	if err != nil {
		t.Fatalf("RecordDDLText: %v", err)
	}
	if !recorded {
		t.Error("expected recorded=true")
	}

	newExpires := time.Now().Add(60 * time.Second)
	extended, err := applier.HeartbeatLease(ctx, tableName, streamID, newExpires)
	if err != nil {
		t.Fatalf("HeartbeatLease: %v", err)
	}
	if !extended {
		t.Error("expected extended=true")
	}

	const ddlChecksum = "deadbeef"
	finalized, err := applier.FinalizeLeaseApply(ctx, tableName, streamID, ddlText, ddlChecksum, 1)
	if err != nil {
		t.Fatalf("FinalizeLeaseApply: %v", err)
	}
	if !finalized {
		t.Error("expected finalized=true")
	}

	observed, ok, err := applier.ObserveLease(ctx, tableName)
	if err != nil {
		t.Fatalf("ObserveLease: %v", err)
	}
	if !ok {
		t.Fatal("expected row to exist")
	}
	if !observed.HasAppliedAt {
		t.Error("expected applied_at to be set")
	}
	if observed.DDLChecksum != ddlChecksum {
		t.Errorf("DDLChecksum = %q, want %q", observed.DDLChecksum, ddlChecksum)
	}
	if observed.AppliedSchemaVersion != 1 {
		t.Errorf("AppliedSchemaVersion = %d, want 1", observed.AppliedSchemaVersion)
	}
}

func TestShardConsolidationLease_ContendedAcquire(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := a.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := a.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	applier := a.(*ChangeApplier)

	const tableName = "users"
	expires := time.Now().Add(60 * time.Second)
	if _, _, err := applier.TryAcquireLease(ctx, tableName, "stream-a", expires); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	acquired, row, err := applier.TryAcquireLease(ctx, tableName, "stream-b", expires)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if acquired {
		t.Fatal("expected acquired=false on contended row")
	}
	if row.LeaseHolderStreamID != "stream-a" {
		t.Errorf("current holder = %q, want stream-a", row.LeaseHolderStreamID)
	}
}

func TestShardConsolidationLease_TakeoverExpiredRow(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := a.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := a.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	applier := a.(*ChangeApplier)

	const tableName = "users"
	pastExpires := time.Now().Add(-1 * time.Second)
	if _, _, err := applier.TryAcquireLease(ctx, tableName, "stream-a", pastExpires); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	const priorDDL = "ALTER TABLE users ADD COLUMN x INT"
	if _, err := applier.RecordDDLText(ctx, tableName, "stream-a", priorDDL); err != nil {
		t.Fatalf("RecordDDLText: %v", err)
	}

	expires := time.Now().Add(60 * time.Second)
	acquired, row, err := applier.TryAcquireLease(ctx, tableName, "stream-b", expires)
	if err != nil {
		t.Fatalf("takeover acquire: %v", err)
	}
	if !acquired {
		t.Fatal("expected takeover acquired=true on expired row")
	}
	if row.LeaseHolderStreamID != "stream-b" {
		t.Errorf("new holder = %q, want stream-b", row.LeaseHolderStreamID)
	}
	if row.DDLText != priorDDL {
		t.Errorf("prior ddl_text not preserved: got %q, want %q", row.DDLText, priorDDL)
	}
}
