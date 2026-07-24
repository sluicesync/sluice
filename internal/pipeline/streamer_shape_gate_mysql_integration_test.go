//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end pins for the ADR-0166 shape gate on the SYNC cold-start
// leg (roadmap item 25 residual), on real MySQL — the engine whose
// relaxed sql_mode made the pre-fix hole a SILENT-coercion vector (an
// empty-but-drifted pre-existing target passed the Bug-9 populated
// check and the IF-NOT-EXISTS create, then cold-copied into the
// drifted schema):
//
//   - drifted empty pre-existing table → the coded
//     SLUICE-E-TARGET-TABLE-SHAPE-MISMATCH refusal BEFORE any row moves;
//   - matching pre-existing table → skipped with INFO, the sync
//     proceeds and converges (cold copy + a live CDC change);
//   - --reset-target-data with the same drifted table → proceeds (the
//     in-scope tables are dropped + recreated first, so the gate has
//     nothing pre-existing to compare).

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/sluicecode"
)

const shapeGateSourceDDL = `CREATE TABLE drift_t (id BIGINT PRIMARY KEY, v VARCHAR(255));
	INSERT INTO drift_t (id, v) VALUES (1, 'one'), (2, 'two'), (3, 'three');`

func TestStreamer_SyncShapeGate_DriftedEmptyTargetRefuses(t *testing.T) {
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	src, tgt, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyMySQLDDL(t, src, shapeGateSourceDDL)
	// The empty-but-DRIFTED pre-existing target table: passes the Bug-9
	// populated check (empty), differs in column shape.
	applyMySQLDDL(t, tgt, `CREATE TABLE drift_t (id BIGINT PRIMARY KEY, only_col VARCHAR(10));`)

	streamer := &Streamer{
		Source:    myEng,
		Target:    myEng,
		SourceDSN: src,
		TargetDSN: tgt,
		StreamID:  "shape-gate-drift",
	}
	err := streamer.Run(context.Background())
	if err == nil {
		t.Fatal("expected the coded shape-mismatch refusal; Run returned nil")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeTargetTableShapeMismatch {
		t.Fatalf("expected coded %s; got %v (err=%v)", sluicecode.CodeTargetTableShapeMismatch, ce, err)
	}
	// Refused BEFORE any row moved: the drifted table stays empty.
	if got := pollRowCountMySQL(tgt, "drift_t"); got != 0 {
		t.Fatalf("target drift_t rows after refusal = %d; want 0 (refusal must precede the copy)", got)
	}
}

func TestStreamer_SyncShapeGate_MatchingTargetSkipsAndProceeds(t *testing.T) {
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	src, tgt, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyMySQLDDL(t, src, shapeGateSourceDDL)
	// A matching-shape pre-existing table (a bootstrapped/pre-created
	// target): skipped with INFO; the copy and CDC still cover it.
	applyMySQLDDL(t, tgt, `CREATE TABLE drift_t (id BIGINT PRIMARY KEY, v VARCHAR(255));`)

	streamer := &Streamer{
		Source:    myEng,
		Target:    myEng,
		SourceDSN: src,
		TargetDSN: tgt,
		StreamID:  "shape-gate-match",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- streamer.Run(ctx) }()

	if !waitForExactRowCountMySQL(tgt, "drift_t", 3, 2*time.Minute) {
		select {
		case err := <-errCh:
			t.Fatalf("matching-shape sync exited before copying: %v", err)
		default:
			t.Fatalf("cold copy never delivered 3 rows (got %d)", pollRowCountMySQL(tgt, "drift_t"))
		}
	}
	// The CDC leg still covers the skipped table.
	applyMySQLDDL(t, src, `INSERT INTO drift_t (id, v) VALUES (4, 'four');`)
	if !waitForExactRowCountMySQL(tgt, "drift_t", 4, 2*time.Minute) {
		t.Fatalf("CDC never applied the live insert (got %d rows)", pollRowCountMySQL(tgt, "drift_t"))
	}
	cancel()
	select {
	case <-errCh:
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestStreamer_SyncShapeGate_ResetTargetDataBypassesGate(t *testing.T) {
	myEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	src, tgt, cleanup := startMySQLBinlog(t)
	defer cleanup()

	applyMySQLDDL(t, src, shapeGateSourceDDL)
	applyMySQLDDL(t, tgt, `CREATE TABLE drift_t (id BIGINT PRIMARY KEY, only_col VARCHAR(10));`)

	streamer := &Streamer{
		Source:          myEng,
		Target:          myEng,
		SourceDSN:       src,
		TargetDSN:       tgt,
		StreamID:        "shape-gate-reset",
		ResetTargetData: true,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- streamer.Run(ctx) }()

	if !waitForExactRowCountMySQL(tgt, "drift_t", 3, 2*time.Minute) {
		select {
		case err := <-errCh:
			t.Fatalf("--reset-target-data run exited before copying (the gate must not fire on that path): %v", err)
		default:
			t.Fatalf("reset run never delivered 3 rows (got %d)", pollRowCountMySQL(tgt, "drift_t"))
		}
	}
	// The recreated table carries the SOURCE shape (the drifted column
	// is gone) — proof the drop-first path ran rather than the gate
	// merely being skipped over a still-drifted table.
	db, err := sql.Open("mysql", tgt)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'drift_t' AND column_name = 'v'`,
	).Scan(&n); err != nil {
		t.Fatalf("probe recreated columns: %v", err)
	}
	if n != 1 {
		t.Fatalf("recreated drift_t lacks the source column 'v' — reset did not recreate from the source shape")
	}
	cancel()
	select {
	case <-errCh:
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
