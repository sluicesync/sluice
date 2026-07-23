//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pin for the target-side vacuum-health probe (the ADR-0107
// item-36 vacuum rule family): against a REAL Postgres, a healthy database
// reads as ok=true with a zero-value (not unobserved) reading; a genuinely
// bloated table surfaces as the worst table with a faithful ratio; and a
// tiny all-dead table below the noise floor can never displace it (nor
// page on its own).

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestTargetVacuumHealth_ProbeAgainstRealPG(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "vacuum_health_db")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	eng := Engine{}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	reporter, ok := applier.(ir.TargetVacuumHealthReporter)
	if !ok {
		t.Fatal("postgres ChangeApplier does not implement ir.TargetVacuumHealthReporter")
	}

	// Fresh database: a HEALTHY reading, not an unobserved one — ok=true,
	// no worst table, zero ratio, but a real XID age + datname (age can
	// legitimately be small on a fresh cluster, so pin presence, not size).
	h, ok, err := reporter.TargetVacuumHealth(ctx)
	if err != nil || !ok {
		t.Fatalf("probe on fresh db: ok=%v err=%v", ok, err)
	}
	if h.WorstTable != "" || h.DeadTupleRatio != 0 {
		t.Fatalf("fresh db reported bloat: worst=%q ratio=%v", h.WorstTable, h.DeadTupleRatio)
	}
	if h.Datname == "" {
		t.Fatal("fresh db probe returned empty datname")
	}
	if h.XIDAge <= 0 {
		t.Fatalf("fresh db XIDAge = %d, want > 0 (age(datfrozenxid) is never 0 on a running cluster)", h.XIDAge)
	}

	// Manufacture real bloat: a table with autovacuum disabled, 3000 rows
	// inserted, 2500 deleted ⇒ ~0.83 dead ratio, well above the 1000-dead
	// noise floor. Plus a DECOY: 50 rows all deleted (ratio 1.0!) but far
	// below the floor — it must neither win the worst-table slot nor page.
	applyPGApplier(t, dsn, `
		CREATE TABLE bloated (id int primary key, pad text) WITH (autovacuum_enabled = off);
		INSERT INTO bloated SELECT g, repeat('x', 100) FROM generate_series(1, 3000) g;
		DELETE FROM bloated WHERE id <= 2500;
		CREATE TABLE tiny_decoy (id int primary key) WITH (autovacuum_enabled = off);
		INSERT INTO tiny_decoy SELECT g FROM generate_series(1, 50) g;
		DELETE FROM tiny_decoy;
	`)

	// The stats views lag the writes slightly (shared-memory flush at
	// transaction end, plus reader-side caching) — poll until the dead
	// tuples are visible rather than sleeping a guessed interval.
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	deadline := time.Now().Add(60 * time.Second)
	for {
		var dead int64
		if err := db.QueryRowContext(ctx,
			`SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'bloated'`).Scan(&dead); err == nil && dead >= vacuumDeadTupleNoiseFloor {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pg_stat_user_tables never surfaced the dead tuples for 'bloated' within 60s")
		}
		time.Sleep(500 * time.Millisecond)
	}

	h, ok, err = reporter.TargetVacuumHealth(ctx)
	if err != nil || !ok {
		t.Fatalf("probe on bloated db: ok=%v err=%v", ok, err)
	}
	if h.WorstTable != "public.bloated" {
		t.Fatalf("WorstTable = %q, want public.bloated (the sub-floor tiny_decoy must not win despite its 1.0 ratio)", h.WorstTable)
	}
	// Stats are estimates; pin the shape, not exact counts.
	if h.DeadTupleRatio < 0.5 {
		t.Errorf("DeadTupleRatio = %v, want >= 0.5 (2500 of 3000 rows deleted)", h.DeadTupleRatio)
	}
	if h.DeadTuples < vacuumDeadTupleNoiseFloor {
		t.Errorf("DeadTuples = %d, want >= the %d floor", h.DeadTuples, vacuumDeadTupleNoiseFloor)
	}
	if h.LiveTuples <= 0 {
		t.Errorf("LiveTuples = %d, want > 0 (500 rows remain)", h.LiveTuples)
	}
	if !h.LastAutovacuum.IsZero() {
		t.Errorf("LastAutovacuum = %v, want zero (autovacuum disabled on the table)", h.LastAutovacuum)
	}
	if h.XIDAge <= 0 {
		t.Errorf("XIDAge = %d, want > 0", h.XIDAge)
	}
}
