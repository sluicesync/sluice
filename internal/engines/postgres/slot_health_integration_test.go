//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for [SchemaReader.SlotHealth] — severity-A finding
// F13 (ADR-0059) of the 2026-05-22 Reddit-research run.
//
// The pin confirms the SQL query wires up correctly: against a live
// Postgres instance, sluice's SlotHealth surface returns the expected
// fields for a freshly-created slot, including the
// `max_slot_wal_keep_size` GUC translated to bytes (with -1 → unlimited
// passing through cleanly). The threshold-evaluator's percentage math
// is unit-tested in the pipeline package; this file just confirms the
// query against real PG returns what the evaluator expects.

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/orware/sluice/internal/ir"
)

// TestSlotHealth_NoSlot pins the "unavailable" surface: querying for a
// slot that doesn't exist returns ok=false (no row in
// pg_replication_slots) cleanly rather than as an error. Operators
// running health probes before CDC has started should see "no signal
// yet" rather than spurious errors in their logs.
func TestSlotHealth_NoSlot(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeReader(t, sr)

	pgsr, ok := sr.(*SchemaReader)
	if !ok {
		t.Fatalf("expected *SchemaReader; got %T", sr)
	}

	_, healthOK, err := pgsr.SlotHealth(ctx, "nonexistent_slot")
	if err != nil {
		t.Fatalf("SlotHealth: unexpected error: %v", err)
	}
	if healthOK {
		t.Errorf("expected ok=false for nonexistent slot; got ok=true")
	}
}

// TestSlotHealth_EmptySlotName pins the validation surface — same
// shape as SlotSpillStats.
func TestSlotHealth_EmptySlotName(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeReader(t, sr)

	pgsr := sr.(*SchemaReader)
	_, _, err = pgsr.SlotHealth(ctx, "")
	if err == nil {
		t.Fatal("expected error on empty slot name")
	}
	if !strings.Contains(err.Error(), "slotName") {
		t.Errorf("error should mention slotName; got %v", err)
	}
}

// TestSlotHealth_DefaultGUCUnlimited pins the production-default GUC
// path: a vanilla PG 16 container has `max_slot_wal_keep_size = -1`
// (unlimited). The SchemaReader must translate that to
// MaxKeepSizeBytes = -1 cleanly so the threshold evaluator's "no warn
// when unlimited" branch is reachable end-to-end.
//
// Also pins the basic populated-row shape: slot_name, active=false
// (the slot is created but no one is consuming from it), wal_status,
// lag_bytes (zero — the slot's restart_lsn == pg_current_wal_lsn just
// after creation).
func TestSlotHealth_DefaultGUCUnlimited(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const slotName = "f13_pin_slot"

	replConn, err := openReplicationConn(ctx, dsn)
	if err != nil {
		t.Fatalf("openReplicationConn: %v", err)
	}
	if _, err := pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication}); err != nil {
		_ = replConn.Close(ctx)
		t.Fatalf("CreateReplicationSlot: %v", err)
	}
	if err := replConn.Close(ctx); err != nil {
		t.Fatalf("close repl conn: %v", err)
	}

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeReader(t, sr)

	pgsr := sr.(*SchemaReader)
	health, ok, err := pgsr.SlotHealth(ctx, slotName)
	if err != nil {
		t.Fatalf("SlotHealth: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true after slot creation")
	}
	if health.SlotName != slotName {
		t.Errorf("SlotName: got %q; want %q", health.SlotName, slotName)
	}
	if health.MaxKeepSizeBytes != -1 {
		t.Errorf("MaxKeepSizeBytes: PG default is unlimited (-1); got %d", health.MaxKeepSizeBytes)
	}
	if health.Active {
		t.Errorf("Active: a freshly-created slot with no consumer should be inactive; got true")
	}
	if health.WALStatus == "" {
		t.Errorf("WALStatus: expected non-empty (one of reserved/extended/unreserved/lost); got empty")
	}
	if health.LagBytes < 0 {
		t.Errorf("LagBytes: expected >=0; got %d", health.LagBytes)
	}
}

// TestSlotHealth_ExplicitGUCBytesConversion pins the GUC-to-bytes
// conversion: when `max_slot_wal_keep_size` is set to a finite value,
// SlotHealth must translate the GUC's MB units to bytes for the
// threshold-evaluator path. A 64 MB cap should surface as 64 * 1024 *
// 1024 = 67108864 bytes.
//
// We don't try to induce actual retention pressure here — that would
// require generating WAL exceeding the cap, which is slow and brittle.
// The unit tests already pin the percentage math; this test pins that
// the bytes value reaching the evaluator is correct.
func TestSlotHealth_ExplicitGUCBytesConversion(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	applyPGSQL(t, dsn, "ALTER SYSTEM SET max_slot_wal_keep_size = '64MB'")
	applyPGSQL(t, dsn, "SELECT pg_reload_conf()")

	const slotName = "f13_explicit_guc_slot"
	replConn, err := openReplicationConn(ctx, dsn)
	if err != nil {
		t.Fatalf("openReplicationConn: %v", err)
	}
	if _, err := pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication}); err != nil {
		_ = replConn.Close(ctx)
		t.Fatalf("CreateReplicationSlot: %v", err)
	}
	if err := replConn.Close(ctx); err != nil {
		t.Fatalf("close repl conn: %v", err)
	}

	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeReader(t, sr)

	pgsr := sr.(*SchemaReader)

	// Poll briefly — pg_reload_conf is asynchronous; the GUC may not
	// have propagated to a fresh backend immediately.
	var health ir.SlotHealth
	var ok bool
	const want = int64(64) * 1024 * 1024
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		health, ok, err = pgsr.SlotHealth(ctx, slotName)
		if err != nil {
			t.Fatalf("SlotHealth: %v", err)
		}
		if ok && health.MaxKeepSizeBytes == want {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ok {
		t.Fatal("expected ok=true")
	}
	if health.MaxKeepSizeBytes != want {
		t.Errorf("MaxKeepSizeBytes: got %d; want %d (64MB in bytes)", health.MaxKeepSizeBytes, want)
	}
}
