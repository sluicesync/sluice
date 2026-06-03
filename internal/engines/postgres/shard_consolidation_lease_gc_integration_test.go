//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the v0.76.0 lease GC sweep (task #21) against
// a real Postgres target. Exercises: (a) the additive anchor_position +
// source_engine column migration; (b) FinalizeLeaseApply persisting
// the anchor; (c) listShardLeases reading it back; (d) deleteShardLease
// removing the row; (e) the pipeline.SweepConsolidationLeases pipeline
// against a populated CDC-state table.

package postgres

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline"
)

// gcPos builds a Postgres-engine ir.Position for the test. The PG
// engine's PositionOrderer expects a JSON-encoded pgPos{Slot,LSN} in
// the Token field — gcPos wraps encodePGPos so the integration tests
// can produce positions the engine actually accepts.
func gcPos(t *testing.T, slot, lsn string) ir.Position {
	t.Helper()
	p, err := encodePGPos(pgPos{Slot: slot, LSN: lsn})
	if err != nil {
		t.Fatalf("encodePGPos(%q,%q): %v", slot, lsn, err)
	}
	return p
}

func TestLeaseGC_AnchorPersistedAndSweepDeletesEligibleRow(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	eng := Engine{}
	a, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := a.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	applier := a.(*ChangeApplier)
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	// Step 1: write a CDC stream position so the sweeper has something
	// to compare anchors against. The PG position token is a JSON
	// pgPos{Slot,LSN}; we write that JSON into source_position via a
	// direct INSERT (the existing writePositionTx helper is tx-scoped
	// — for an isolated test fixture an INSERT is simpler).
	streamPos := gcPos(t, "slot-a", "0/2000000")
	if _, err := applier.db.ExecContext(
		ctx,
		`INSERT INTO "public"."sluice_cdc_state" (stream_id, source_position, updated_at) VALUES ($1, $2, CURRENT_TIMESTAMP)`,
		"shard-a", streamPos.Token,
	); err != nil {
		t.Fatalf("insert stream row: %v", err)
	}

	// Step 2: acquire + finalize a lease with an anchor below the
	// stream's position. Anchor LSN 0/1000000 is before the stream's
	// 0/2000000, so the row should be GC-eligible.
	tableName := "public.gc_target"
	streamID := "shard-a"
	expires := time.Now().Add(time.Hour)
	acquired, _, err := applier.TryAcquireLease(ctx, tableName, streamID, expires)
	if err != nil || !acquired {
		t.Fatalf("TryAcquireLease: acquired=%v err=%v", acquired, err)
	}
	if _, err := applier.RecordDDLText(ctx, tableName, streamID, "ALTER TABLE gc_target ADD COLUMN x INT"); err != nil {
		t.Fatalf("RecordDDLText: %v", err)
	}
	anchor := gcPos(t, "slot-a", "0/1000000")
	finalized, err := applier.FinalizeLeaseApply(
		ctx, tableName, streamID,
		"ALTER TABLE gc_target ADD COLUMN x INT", "deadbeef",
		1,
		anchor,
	)
	if err != nil || !finalized {
		t.Fatalf("FinalizeLeaseApply: finalized=%v err=%v", finalized, err)
	}

	// Step 3: verify the row materialized with anchor.
	rows, err := applier.ListLeases(ctx)
	if err != nil {
		t.Fatalf("ListLeases: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("ListLeases len = %d, want 1", len(rows))
	}
	if !rows[0].HasAnchor {
		t.Error("HasAnchor = false, want true (anchor was persisted)")
	}
	if rows[0].AnchorPosition.Token == "" {
		t.Error("AnchorPosition.Token = empty, want JSON pgPos token")
	}
	if rows[0].AnchorPosition.Engine != "postgres" {
		t.Errorf("AnchorPosition.Engine = %q, want %q", rows[0].AnchorPosition.Engine, "postgres")
	}

	// Step 4: run the sweep. The stream's position (0/2000000) is
	// past the anchor (0/1000000) → row should be GC'd.
	deleted, err := pipeline.SweepConsolidationLeases(ctx, pipeline.LeaseGCDeps{
		Lister:    applier,
		Deleter:   applier,
		PosReader: applier,
		Orderer:   Engine{}, // PG engine implements PositionOrderer
	})
	if err != nil {
		t.Fatalf("SweepConsolidationLeases: %v", err)
	}
	if deleted != 1 {
		t.Errorf("deleted = %d, want 1", deleted)
	}

	// Step 5: verify the row is gone.
	rows, err = applier.ListLeases(ctx)
	if err != nil {
		t.Fatalf("ListLeases (post-sweep): %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("ListLeases post-sweep len = %d, want 0", len(rows))
	}
}

// TestLeaseGC_StreamBehindAnchorRetainsRow pins the per-row safety
// condition: if any stream's persisted position is older than the
// anchor, the row stays.
func TestLeaseGC_StreamBehindAnchorRetainsRow(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	eng := Engine{}
	a, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := a.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	applier := a.(*ChangeApplier)
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	// Two streams, one behind the anchor.
	posA := gcPos(t, "slot-a", "0/3000000") // past anchor
	posB := gcPos(t, "slot-a", "0/1500000") // BEHIND anchor (0/2000000)
	if _, err := applier.db.ExecContext(
		ctx,
		`INSERT INTO "public"."sluice_cdc_state" (stream_id, source_position, updated_at) VALUES ($1, $2, CURRENT_TIMESTAMP)`,
		"shard-a", posA.Token,
	); err != nil {
		t.Fatalf("insert stream-a: %v", err)
	}
	if _, err := applier.db.ExecContext(
		ctx,
		`INSERT INTO "public"."sluice_cdc_state" (stream_id, source_position, updated_at) VALUES ($1, $2, CURRENT_TIMESTAMP)`,
		"shard-b", posB.Token,
	); err != nil {
		t.Fatalf("insert stream-b: %v", err)
	}

	tableName := "public.gc_behind"
	expires := time.Now().Add(time.Hour)
	if _, _, err := applier.TryAcquireLease(ctx, tableName, "shard-a", expires); err != nil {
		t.Fatalf("TryAcquireLease: %v", err)
	}
	if _, err := applier.RecordDDLText(ctx, tableName, "shard-a", "ALTER"); err != nil {
		t.Fatalf("RecordDDLText: %v", err)
	}
	anchor := gcPos(t, "slot-a", "0/2000000")
	finalized, err := applier.FinalizeLeaseApply(ctx, tableName, "shard-a", "ALTER", "checksum", 1, anchor)
	if err != nil || !finalized {
		t.Fatalf("FinalizeLeaseApply: finalized=%v err=%v", finalized, err)
	}

	// Sweep: shard-b is behind → row retained.
	deleted, err := pipeline.SweepConsolidationLeases(ctx, pipeline.LeaseGCDeps{
		Lister:    applier,
		Deleter:   applier,
		PosReader: applier,
		Orderer:   Engine{},
	})
	if err != nil {
		t.Fatalf("SweepConsolidationLeases: %v", err)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0 (shard-b is behind anchor)", deleted)
	}
}
