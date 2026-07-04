//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end Bug 177 pin: a PG-source `sync start` cold-start that is
// REFUSED on a populated target (Bug 9's SLUICE-E-COLDSTART-TARGET-
// NOT-EMPTY) must not orphan the replication slot it just created —
// pre-fix, the refusal exited with the slot alive (pinning source WAL)
// and the refusal hint's own preferred recovery (`--reset-target-data`)
// then failed on "slot already exists" until an un-hinted manual drop.
// The pin asserts both halves: zero slots on the source after the
// refusal, and the preferred recovery succeeding FIRST TRY.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
)

func pgSlotCount(t *testing.T, dsn string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_replication_slots`).Scan(&n); err != nil {
		t.Fatalf("count slots: %v", err)
	}
	return n
}

func TestStreamer_ColdStart_PG_RefusalDropsSlot(t *testing.T) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, src, `CREATE TABLE narrow (id BIGINT PRIMARY KEY, v INT);
		INSERT INTO narrow (id, v) SELECT g, g FROM generate_series(1, 500) g;`)
	// Pre-populate the TARGET table — the accidental-re-point shape that
	// triggers the Bug 9 refusal.
	applyDDL(t, tgt, `CREATE TABLE narrow (id BIGINT PRIMARY KEY, v INT);
		INSERT INTO narrow (id, v) VALUES (1, 1);`)

	streamer := &Streamer{
		Source:    pgEng,
		Target:    pgEng,
		SourceDSN: src,
		TargetDSN: tgt,
		StreamID:  "bug177-refusal",
	}
	err := streamer.Run(context.Background())
	if err == nil {
		t.Fatal("expected the populated-target cold-start refusal; Run returned nil")
	}
	if !errors.Is(err, errColdStartRefused) {
		t.Fatalf("Run error = %v; want errColdStartRefused", err)
	}

	// THE Bug 177 pin: the refusal must not leave the slot it created.
	if got := pgSlotCount(t, src); got != 0 {
		t.Fatalf("source replication slots after refusal = %d; want 0 (Bug 177: orphaned slot pins WAL)", got)
	}

	// And the hint's PREFERRED recovery works first try — pre-fix this
	// failed on `replication slot "sluice_slot" already exists`.
	recovery := &Streamer{
		Source:          pgEng,
		Target:          pgEng,
		SourceDSN:       src,
		TargetDSN:       tgt,
		StreamID:        "bug177-refusal",
		ResetTargetData: true,
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- recovery.Run(streamCtx) }()

	if !waitForExactRowCount(tgt, "narrow", 500, 2*time.Minute) {
		select {
		case err := <-runErr:
			t.Fatalf("recovery run exited before copying (rows=%d): %v", pollRowCount(tgt, "narrow"), err)
		default:
			t.Fatalf("recovery cold-start never delivered 500 rows (got %d)", pollRowCount(tgt, "narrow"))
		}
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("recovery Run did not return after ctx cancel")
	}
}
