//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0173 Phase 2 — continuous filtered sync, CROSS-engine cell.
//
// Postgres source → MySQL target. The row-move dispatch is engine-neutral
// pipeline code, but the move-IN → INSERT and move-OUT → DELETE cells
// synthesize target ops the CROSS-engine applier must handle — this pins
// those two cells non-vacuously across the engine boundary (the PG source
// carries the full before-image under REPLICA IDENTITY FULL).

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestStreamer_WhereFilter_PostgresToMySQLRowMove pins the move-IN and
// move-OUT cells across the PG→MySQL boundary.
func TestStreamer_WhereFilter_PostgresToMySQLRowMove(t *testing.T) {
	pgSourceDSN, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	_, mysqlTargetDSN, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	applyDDL(t, pgSourceDSN, `
		CREATE TABLE users (
			id      BIGINT      NOT NULL PRIMARY KEY,
			country VARCHAR(8)  NOT NULL
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		INSERT INTO users (id, country) VALUES (1, 'US'), (2, 'CA');
	`)

	pgEng, _ := engines.Get("postgres")
	mysqlEng, _ := engines.Get("mysql")
	streamer := &Streamer{
		Source:     pgEng,
		Target:     mysqlEng,
		SourceDSN:  pgSourceDSN,
		TargetDSN:  mysqlTargetDSN,
		StreamID:   "where-cross-rowmove",
		RowFilters: map[string]string{"users": "country = 'US'"},
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	// Filtered cold-start: only id=1 (US).
	if !waitForExactRowCountMySQL(mysqlTargetDSN, "users", 1, 90*time.Second) {
		t.Fatalf("filtered cold-start delivered %d rows; want 1", pollRowCountMySQL(mysqlTargetDSN, "users"))
	}

	// move-IN: id=2 CA→US → target gains 2 (INSERT the after-image).
	applyDDL(t, pgSourceDSN, "UPDATE users SET country = 'US' WHERE id = 2;")
	if !waitForMySQLRow(t, mysqlTargetDSN, streamCtx, 2, true, 45*time.Second) {
		t.Fatalf("move-IN across engines did not INSERT id=2 on the MySQL target")
	}
	// move-OUT: id=1 US→MX → target loses 1 (DELETE by key).
	applyDDL(t, pgSourceDSN, "UPDATE users SET country = 'MX' WHERE id = 1;")
	if !waitForMySQLRow(t, mysqlTargetDSN, streamCtx, 1, false, 45*time.Second) {
		t.Fatalf("move-OUT across engines did not DELETE id=1 on the MySQL target")
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(15 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}
}
