//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for [SchemaReader.SlotSpillStats] — severity-B
// finding F2 of the 2026-05-22 PG-internals research run.
//
// The pin confirms end-to-end wire-up: PG's `pg_stat_replication_slots`
// view returns non-zero `spill_bytes` after a deliberately large
// transaction is decoded through a slot whose `logical_decoding_work_mem`
// has been tuned down, and sluice's SchemaReader.SlotSpillStats reads
// the same values. The job here is to characterise the wire-up, not the
// PG spill-threshold behaviour itself; we use a small work_mem (64 kB)
// and an undersized transaction so the test is fast and deterministic.

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSlotSpillStats_NoSlot pins the "unavailable" surface: querying
// for a slot that doesn't exist returns ok=false (no row in the view)
// rather than an error. Operators running sync-health probes before
// CDC has started should see "spill stats unavailable" cleanly.
func TestSlotSpillStats_NoSlot(t *testing.T) {
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

	_, statsOK, err := pgsr.SlotSpillStats(ctx, "nonexistent_slot")
	if err != nil {
		t.Fatalf("SlotSpillStats: unexpected error: %v", err)
	}
	if statsOK {
		t.Errorf("expected ok=false for nonexistent slot; got ok=true")
	}
}

// TestSlotSpillStats_EmptySlotName pins the validation: an empty slot
// name returns an error (the wiring layer didn't supply enough info to
// scope the query). Surfacing this as ok=false would mask a real bug.
func TestSlotSpillStats_EmptySlotName(t *testing.T) {
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

	_, statsOK, err := pgsr.SlotSpillStats(ctx, "")
	if err == nil {
		t.Fatal("expected error on empty slot name")
	}
	if statsOK {
		t.Errorf("ok should be false on error path; got ok=true")
	}
	if !strings.Contains(err.Error(), "slotName") {
		t.Errorf("error should mention slotName; got %v", err)
	}
}

// TestSlotSpillStats_DecodeProducesNonZeroBytes is the end-to-end pin:
// a slot whose logical_decoding_work_mem is tuned down to 64 kB
// produces non-zero spill_bytes after a transaction with enough rows
// to exceed the threshold is decoded. SchemaReader.SlotSpillStats reads
// the same values out via pg_stat_replication_slots.
//
// The test deliberately doesn't characterize the spill thresholds —
// PG's spill behaviour is well-tested by PG itself. Our job is to
// confirm the sluice wire-up surfaces what PG reports.
func TestSlotSpillStats_DecodeProducesNonZeroBytes(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Tune work_mem down so the smallest realistic transaction spills.
	// 64 kB is the documented PG minimum for logical_decoding_work_mem.
	//
	// ALTER SYSTEM cannot run inside a transaction block (SQLSTATE
	// 25001); pgx's simple-query path wraps multi-statement scripts
	// implicitly, so split into individual statements.
	applyPGSQL(t, dsn, "ALTER SYSTEM SET logical_decoding_work_mem = '64kB'")
	applyPGSQL(t, dsn, "SELECT pg_reload_conf()")
	applyPGSQL(t, dsn, "CREATE TABLE bulk (id INT PRIMARY KEY, payload TEXT)")

	const slotName = "spill_pin_slot"

	// Create the slot via the replication protocol — same code path
	// the CDC reader takes during cold-start.
	replConn, err := openReplicationConn(ctx, dsn, "-")
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

	// Create publication so pgoutput has something to emit.
	applyPGSQL(t, dsn, `CREATE PUBLICATION spill_pub FOR TABLE bulk;`)

	// Generate a transaction with enough wide rows to exceed 64 kB of
	// decoded change buffer. Each row carries ~512 bytes of payload;
	// 2000 rows * ~600 bytes per change record = ~1.2 MB of decode
	// buffer — comfortably above the 64 kB threshold.
	applyPGSQL(t, dsn, `
		BEGIN;
		INSERT INTO bulk
		SELECT g, repeat('x', 512)
		FROM generate_series(1, 2000) g;
		COMMIT;
	`)

	// Trigger decoding so PG actually emits change records through the
	// slot and the spill counters advance. pg_logical_slot_get_binary_changes
	// runs the decoder against the slot's accumulated WAL. We pass
	// publication_names to drive pgoutput; the output bytes themselves
	// aren't what we measure — we just need decode to run.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, `
		SELECT lsn FROM pg_logical_slot_get_binary_changes(
			$1, NULL, NULL,
			'proto_version', '1',
			'publication_names', 'spill_pub'
		)`, slotName)
	if err != nil {
		t.Fatalf("pg_logical_slot_get_binary_changes: %v", err)
	}
	// Drain rows so the decode actually completes.
	for rows.Next() {
		var lsn string
		if err := rows.Scan(&lsn); err != nil {
			t.Fatalf("scan: %v", err)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	_ = rows.Close()

	// Now read spill stats via sluice's SchemaReader surface.
	sr, err := Engine{}.OpenSchemaReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer closeReader(t, sr)

	pgsr, ok := sr.(*SchemaReader)
	if !ok {
		t.Fatalf("expected *SchemaReader; got %T", sr)
	}

	// Poll briefly — pg_stat_replication_slots is updated by the
	// decoder, and the update may lag the get_changes call by a tick.
	var stats ir.SpillStats
	var statsOK bool
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		stats, statsOK, err = pgsr.SlotSpillStats(ctx, slotName)
		if err != nil {
			t.Fatalf("SlotSpillStats: %v", err)
		}
		if statsOK && stats.SpillBytes > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !statsOK {
		t.Fatal("expected stats to be available (ok=true); pg_stat_replication_slots had no row for the slot after decoding")
	}
	if stats.SpillBytes == 0 {
		t.Errorf("expected SpillBytes > 0 after a deliberately oversized transaction with logical_decoding_work_mem=64kB; got 0 (txns=%d). The wire-up appears to be reading the wrong column or the slot's decode didn't actually spill.", stats.SpillTxns)
	}
	// SpillTxns may be 0 even when SpillBytes > 0 in some PG versions
	// (the counter increments per-spill-event, not per-tx). We pin on
	// bytes as the load-bearing signal; txns is informational.
	t.Logf("spill stats: txns=%d bytes=%d", stats.SpillTxns, stats.SpillBytes)
}

// closeReader is a typed wrapper around io.Closer's nil-handling for
// the SchemaReader returned by Engine{}.OpenSchemaReader. The reader
// type-asserts to io.Closer cleanly; this helper just avoids repeating
// the closure boilerplate in every test.
func closeReader(t *testing.T, sr ir.SchemaReader) {
	t.Helper()
	type closer interface {
		Close() error
	}
	if c, ok := sr.(closer); ok {
		if err := c.Close(); err != nil && !errors.Is(err, sql.ErrConnDone) {
			t.Logf("close reader: %v (non-fatal)", err)
		}
	}
}
