//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The MVCC-invisible in-flight-txn anchor hole (epoch-INDEPENDENT;
// live-confirmed 2026-07-08 during the N-1 xid8 fix).
//
// A change-log row INSERTed by a transaction still uncommitted when the
// snapshot's anchor query runs is INVISIBLE to that query (MVCC), so the
// `MIN(id)-1` hold-down arm cannot anchor below an in-flight txn's
// already-allocated id when that id is lower than every VISIBLE
// not-yet-settled row. Repro at any epoch: txn A inserts change-log
// id=N+1 (open), txn B inserts id=N+2 (committed) → the anchor query
// computes MIN(N+2)-1 = N+1, but gap-free needs N — A's change is in
// neither the bulk-copy snapshot (A uncommitted at snapshot time) nor
// the `id > N+1` CDC tail. Same silent-gap class as Bug-94.
//
// The fix (ADR-0066 impl-notes): OpenSnapshotStream exports the
// snapshot's visibility horizon from the SAME pinned conn the anchor
// query uses, assigns a txid upper bound on the pool, waits — on a
// fresh snapshot each poll — until every pre-bound transaction settles,
// then clamps the anchor below the now-visible change-log ids that are
// NOT in the copy's snapshot. These tests pin the closed hole through
// the REAL OpenSnapshotStream path.

package pgtrigger

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestSnapshotStream_InFlightTxnAnchor_NoGap is the live repro of the
// invisible in-flight low-id window, driven through the actual
// Engine.OpenSnapshotStream handoff:
//
//	txn A INSERTs (allocating change-log id maxID+1) and stays OPEN;
//	txn B INSERTs (id maxID+2) and commits;
//	OpenSnapshotStream runs while A is still in flight;
//	A commits shortly after (once the settle wait is observed running,
//	with a bounded fallback so a pre-fix binary can't hang the test).
//
// The gap-free anchor is maxID: A's id maxID+1 is in neither the
// bulk-copy snapshot nor an `id > maxID+1` CDC tail. Pre-fix the anchor
// lands at maxID+1 (MIN(visible unsettled)-1) and A's change is
// silently lost — the exact assertion below.
func TestSnapshotStream_InFlightTxnAnchor_NoGap(t *testing.T) {
	dsn, cleanup := startPGForTrigger(t)
	defer cleanup()

	applyPGSQL(t, dsn, `CREATE TABLE items (id BIGINT PRIMARY KEY, label TEXT);`)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	if _, err := Setup(ctx, dsn, SetupOptions{Tables: []string{"items"}, Schema: "public"}); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var maxID int64
	if err := db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(id), 0) FROM public.sluice_change_log`).Scan(&maxID); err != nil {
		t.Fatalf("read MAX(id): %v", err)
	}

	// txn A: allocates change-log id maxID+1 via the trigger, stays open.
	dbA, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	defer func() { _ = dbA.Close() }()
	txA, err := dbA.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	defer func() { _ = txA.Rollback() }()
	if _, err := txA.ExecContext(ctx, `INSERT INTO items (id, label) VALUES (1, 'A')`); err != nil {
		t.Fatalf("insert A: %v", err)
	}
	// txn B: allocates id maxID+2 and commits — the visible row the
	// pre-fix MIN(id)-1 arm anchors off, masking A's lower invisible id.
	if _, err := db.ExecContext(ctx, `INSERT INTO items (id, label) VALUES (2, 'B')`); err != nil {
		t.Fatalf("insert+commit B: %v", err)
	}

	// Commit A once OpenSnapshotStream's settle wait is observed running
	// (its poll query carries the sluice-anchor-settle-wait marker; the
	// LIKE pattern is concatenated so this monitor's own query text
	// doesn't match itself on a pooled conn). The 20 s fallback keeps a
	// pre-fix binary — which never runs the wait — from wedging the test.
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			var n int
			qErr := db.QueryRowContext(
				ctx,
				`SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%sluice-anchor-settle' || '-wait%' AND pid <> pg_backend_pid()`,
			).Scan(&n)
			if qErr == nil && n > 0 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err := txA.Commit(); err != nil {
			t.Errorf("commit A: %v", err)
		}
	}()

	e := Engine{}
	stream, err := e.OpenSnapshotStream(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSnapshotStream: %v", err)
	}
	defer func() { _ = stream.Close() }()
	<-monitorDone

	pos, ok, err := decodePos(stream.Position)
	if err != nil || !ok {
		t.Fatalf("decode stream position (ok=%v): %v", ok, err)
	}
	if pos.LastID != maxID {
		t.Errorf("anchor = %d; want %d — an anchor of %d silently gaps txn A's in-flight change-log id %d (MVCC-invisible in-flight low-id window)",
			pos.LastID, maxID, maxID+1, maxID+1)
	}

	// User-visible half of the contract: CDC from the stream's anchor
	// must replay BOTH rows (A's change is in neither the bulk copy nor
	// a too-high tail; over-replay of B is a safe idempotent re-apply).
	if err := stream.ReleaseRows(); err != nil {
		t.Fatalf("ReleaseRows: %v", err)
	}
	out, err := stream.Changes.StreamChanges(ctx, stream.Position)
	if err != nil {
		t.Fatalf("StreamChanges from anchor: %v", err)
	}
	got := drainEvents(t, out, 2, 10*time.Second)
	labels := map[string]bool{}
	for _, ev := range got {
		if ins, isInsert := ev.(ir.Insert); isInsert {
			labels[fmt.Sprint(ins.Row["label"])] = true
		}
	}
	if !labels["A"] {
		t.Errorf("CDC replay from anchor missed txn A's row — silent gap (events: %d, labels: %v)", len(got), labels)
	}
	if !labels["B"] {
		t.Errorf("CDC replay from anchor missed txn B's row (events: %d, labels: %v)", len(got), labels)
	}
}

// The settle-timeout loud-refusal pin lives in
// cdc_anchor_settle_integration_test.go alongside the wait helpers it
// exercises.
