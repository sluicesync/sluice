//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Serial-fallback pin for the FAST sync cold-start (ADR-0079). The fast
// parallel cold-start is PG-source-only — it requires a SHAREABLE exported
// snapshot ([ir.SnapshotStream.SnapshotName]) AND an
// [ir.SnapshotImporterOpener]. MySQL's REPEATABLE READ snapshot is
// per-session and not shareable, so a MySQL→MySQL `sync start` cold-start
// MUST stay on the serial `runBulkCopyWithOpts` path. This boots a real
// MySQL container, runs a cold-start sync, and asserts:
//
//   - the dispatch took the SERIAL path (the coldStartDispatchObserver seam
//     fired with fast == false) — a green zero-loss test alone can't tell
//     the serial path from the parallel one; AND
//   - the cold-start is still zero-loss + CDC continues after.
//
// (A VStream/PlanetScale source likewise leaves SnapshotName empty and
// hits the same serial branch; the vttestserver-tagged suites own the
// VStream-source coverage. The capability gate is engine-name-free, so
// MySQL here is a representative of the whole "no shareable snapshot"
// class — see coldStartFastEligible.)

package pipeline

import (
	"context"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// setColdStartDispatchObserverForTest installs the dispatch seam and
// returns a restore func (same-package; mirrors
// setOnTableCopiedObserverForTest).
func setColdStartDispatchObserverForTest(fn func(fast bool)) func() {
	prev := coldStartDispatchObserver
	coldStartDispatchObserver = fn
	return func() { coldStartDispatchObserver = prev }
}

// TestStreamer_ColdStart_SerialFallback_MySQL asserts a MySQL→MySQL sync
// cold-start takes the SERIAL path (no shareable snapshot) and stays
// zero-loss + CDC-correct.
func TestStreamer_ColdStart_SerialFallback_MySQL(t *testing.T) {
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	src, tgt, cleanup := startMySQL(t)
	defer cleanup()

	const rows = 3
	applyMySQLDDL(t, src, `
		CREATE TABLE widgets (id BIGINT NOT NULL, name VARCHAR(64) NOT NULL, PRIMARY KEY (id));
		INSERT INTO widgets (id, name) VALUES (1,'one'),(2,'two'),(3,'three');
	`)

	var sawFast, sawSerial bool
	restore := setColdStartDispatchObserverForTest(func(fast bool) {
		if fast {
			sawFast = true
		} else {
			sawSerial = true
		}
	})
	defer restore()

	streamer := &Streamer{
		Source:    mysqlEng,
		Target:    mysqlEng,
		SourceDSN: src,
		TargetDSN: tgt,
		StreamID:  "coldstart-serial-fallback-mysql",
		// These would drive the fast path on a PG source; on MySQL they are
		// inert (the gate refuses the fast path), which is exactly what this
		// pins.
		TableParallelism: 4,
		BulkParallelism:  4,
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()

	if !waitForExactRowCountMySQL(tgt, "widgets", rows, 90*time.Second) {
		t.Fatalf("cold-start copy never delivered %d rows (got %d)", rows, pollRowCountMySQL(tgt, "widgets"))
	}

	// CDC continues after the serial cold-start.
	applyMySQLDDL(t, src, "INSERT INTO widgets (id, name) VALUES (4, 'four');")
	if !waitForExactRowCountMySQL(tgt, "widgets", rows+1, 60*time.Second) {
		t.Fatalf("post-cold-start CDC INSERT never propagated; rows = %d, want %d",
			pollRowCountMySQL(tgt, "widgets"), rows+1)
	}

	streamCancel()
	select {
	case <-runErr:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run did not return after ctx cancel")
	}

	if sawFast {
		t.Error("MySQL cold-start took the FAST parallel path — MySQL has no shareable snapshot; it MUST stay serial (gate regressed)")
	}
	if !sawSerial {
		t.Error("dispatch observer never fired with fast==false; the cold-start serial branch was not reached")
	}
}
