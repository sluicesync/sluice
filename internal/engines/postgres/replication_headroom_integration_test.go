//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the Postgres replication-headroom prober
// (roadmap item 68d). Ground-truths the census SQL against a real PG:
// the ceilings match current_setting, and creating a logical slot is
// reflected in SlotsInUse + the named Slots inventory. The
// orchestrator-side gate + refusal (capability gating, the advisory
// probe-failure degrade, the message shape) is unit-tested in
// internal/pipeline/replication_headroom_preflight_test.go; this file
// pins the engine-side probe the gate rides on.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestReplicationHeadroom_ProbeCensus(t *testing.T) {
	// The CDC container boots wal_level=logical so a logical slot can be
	// created for the census delta below.
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	before, err := probeReplicationHeadroom(ctx, db)
	if err != nil {
		t.Fatalf("probe (before): %v", err)
	}

	// Ceilings must mirror the server's own settings verbatim.
	var wantSlots, wantSenders int
	if err := db.QueryRowContext(
		ctx,
		`SELECT current_setting('max_replication_slots')::int, current_setting('max_wal_senders')::int`,
	).Scan(&wantSlots, &wantSenders); err != nil {
		t.Fatalf("read settings: %v", err)
	}
	if before.MaxReplicationSlots != wantSlots || before.MaxWALSenders != wantSenders {
		t.Errorf("ceilings = %d/%d; want %d/%d",
			before.MaxReplicationSlots, before.MaxWALSenders, wantSlots, wantSenders)
	}

	// Creating a slot must show up in the census: count +1 and the name
	// in the inventory, inactive (no consumer attached).
	if _, err := db.ExecContext(ctx,
		`SELECT pg_create_logical_replication_slot('headroom_occupier', 'pgoutput')`); err != nil {
		t.Fatalf("create occupier slot: %v", err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(),
			`SELECT pg_drop_replication_slot('headroom_occupier')`)
	}()

	after, err := probeReplicationHeadroom(ctx, db)
	if err != nil {
		t.Fatalf("probe (after): %v", err)
	}
	if after.SlotsInUse != before.SlotsInUse+1 {
		t.Errorf("SlotsInUse = %d; want %d (before %d + the occupier)",
			after.SlotsInUse, before.SlotsInUse+1, before.SlotsInUse)
	}
	found := false
	for _, s := range after.Slots {
		if s.Name == "headroom_occupier" {
			found = true
			if s.Active {
				t.Errorf("occupier slot reported active; no consumer is attached")
			}
		}
	}
	if !found {
		t.Errorf("slot inventory %v missing the just-created occupier", after.Slots)
	}
}
