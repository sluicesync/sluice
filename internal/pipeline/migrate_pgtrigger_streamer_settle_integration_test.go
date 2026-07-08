//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The user-visible pin for the pgtrigger cold-start anchor's MVCC
// blind spot (the invisible in-flight low-id window; live-confirmed
// 2026-07-08): a source transaction OPEN ACROSS the snapshot→CDC
// handoff whose change-log id undercuts a later committed row was in
// neither the bulk copy (uncommitted at snapshot time) nor the CDC
// tail (anchor landed above its id) — the row silently never reached
// the target, with exit 0. The engine-level anchor pin lives in
// pgtrigger/cdc_snapshot_settle_integration_test.go; this test proves
// the loss-shape end to end through the REAL Streamer cold start.
//
// Container/exec helpers are shared with
// migrate_pgtrigger_streamer_integration_test.go.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"
)

// TestPGTriggerStreamer_InFlightTxnSpansColdStart_NoLoss drives the
// Streamer cold start while a source write transaction is deliberately
// held open across the handoff:
//
//	txn A INSERTs id=900 and stays OPEN;
//	id=901 is INSERTed and committed;
//	Streamer.Run cold-starts (snapshot + anchor while A is in flight);
//	A commits once the anchor settle-wait is observed running
//	(bounded fallback so a regressed binary can't wedge the test).
//
// EVERY committed row — the 50 seeds, 900, and 901 — must land on the
// target. Without the settle-wait + clamp the anchor lands above A's
// change-log id and id=900 is silently lost (the exact live-confirmed
// failure).
func TestPGTriggerStreamer_InFlightTxnSpansColdStart_NoLoss(t *testing.T) {
	src, tgt, cleanup := startPGTrigStreamerPGPair(t)
	defer cleanup()

	pgTrigStreamerExec(t, src, pgTrigStreamerSeedDDL)

	if _, err := pgtrigger.Setup(context.Background(), src, pgtrigger.SetupOptions{
		Tables: []string{pgTrigStreamerTable},
		Schema: "public",
	}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}

	db, err := sql.Open("pgx", src)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	defer func() { _ = db.Close() }()

	// txn A: captured by the trigger (allocating the LOWER change-log
	// id), held open across the cold-start handoff.
	dbA, err := sql.Open("pgx", src)
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	defer func() { _ = dbA.Close() }()
	ctxA, cancelA := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancelA()
	txA, err := dbA.BeginTx(ctxA, nil)
	if err != nil {
		t.Fatalf("begin A: %v", err)
	}
	defer func() { _ = txA.Rollback() }()
	if _, err := txA.ExecContext(ctxA,
		"INSERT INTO "+pgTrigStreamerTable+" (id, n, note) VALUES (900, 1800, 'in-flight')"); err != nil {
		t.Fatalf("insert A: %v", err)
	}
	// The committed higher-id row the broken anchor hides A behind.
	pgTrigStreamerExec(t, src, "INSERT INTO "+pgTrigStreamerTable+" (id, n, note) VALUES (901, 1802, 'committed')")

	// Commit A once the cold start's anchor settle-wait is observed in
	// pg_stat_activity (its poll query carries the marker; the LIKE
	// pattern is concatenated so this monitor's own query text doesn't
	// match itself on a pooled conn). The 30 s fallback keeps a
	// regressed binary — which never runs the wait — from wedging the
	// test: it commits A anyway, long after the too-high anchor was
	// computed, and the drain below then fails on the missing id=900.
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			var n int
			qErr := db.QueryRowContext(
				ctxA,
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

	trigEng, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	streamer := &Streamer{
		Source:    trigEng,
		Target:    trigEng,
		SourceDSN: src,
		TargetDSN: tgt,
		StreamID:  "pgtrig-streamer-settle",
		Filter: mustNewFilter(t, nil, []string{
			pgtrigger.ChangeLogTable,
			pgtrigger.ChangeLogMetaTable,
		}),
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(streamCtx) }()
	defer func() {
		streamCancel()
		select {
		case <-runErr:
		case <-time.After(20 * time.Second):
			t.Error("Streamer.Run did not return after ctx cancel")
		}
	}()
	<-monitorDone

	// Drain: seeds 1..50 plus BOTH boundary rows. id=900 is the
	// loss-shape row — in neither the bulk copy nor the CDC tail when
	// the anchor lands too high.
	want := make([]int64, 0, pgTrigStreamerSeedCount+2)
	for id := int64(1); id <= pgTrigStreamerSeedCount; id++ {
		want = append(want, id)
	}
	want = append(want, 900, 901)
	deadline := time.Now().Add(120 * time.Second)
	var lastMissing int64 = -1
	for time.Now().Before(deadline) {
		missing, allPresent := pgTrigStreamerSettlePresent(tgt, want)
		if allPresent {
			return
		}
		lastMissing = missing
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("target never drained: first missing id=%d (id=900 missing = the in-flight-txn cold-start silent gap)", lastMissing)
}

// pgTrigStreamerSettlePresent reports the first id in want missing from
// the PG target, and ok=true when all are present. Read failures return
// ok=false so the caller's poll keeps trying.
func pgTrigStreamerSettlePresent(dsn string, want []int64) (int64, bool) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return -1, false
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT id FROM "`+pgTrigStreamerTable+`"`)
	if err != nil {
		return -1, false
	}
	defer func() { _ = rows.Close() }()
	present := make(map[int64]bool, len(want))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return -1, false
		}
		present[id] = true
	}
	if err := rows.Err(); err != nil {
		return -1, false
	}
	for _, id := range want {
		if !present[id] {
			return id, false
		}
	}
	return -1, true
}
