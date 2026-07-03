//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration safety pin for ADR-0137 Phase B — AUTOMATIC in-stream pruning of
// the trigger-CDC change-log (Bug 165). The Phase-A pin
// (streamer_trigger_prune_integration_test.go) already proves the prune-BOUND
// safety (rows <= durable-frontier-minus-keep are reaped, the durable position is
// unchanged, warm-resume stays exactly-once). What's NEW here is the automatic
// CADENCE + failure-isolation: the streamer's sidecar prunes the SOURCE change-log
// WHILE the sync runs, without an operator scheduling `sluice trigger prune`.
//
// This proves, against real databases, that with --auto-prune-change-log:
//   - the source sluice_change_log SHRINKS in-stream (its MIN(id) advances past
//     the head) as the target durably applies — bounded growth, automatically;
//   - a prune does NOT advance the target's durable position (pruning source rows
//     is decoupled from the applier's watermark);
//   - warm-resume AFTER the automatic prune still converges exactly-once — the
//     load-bearing proof the cadence didn't reap a row resume needs.

package pipeline

import (
	"context"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
	_ "sluicesync.dev/sluice/internal/engines/sqlite"
)

// TestAutoPruneChangeLog_SQLiteToPostgres exercises the automatic in-stream prune
// end-to-end against a live sqlite-trigger → PG sync.
func TestAutoPruneChangeLog_SQLiteToPostgres(t *testing.T) {
	src := seedSQLiteTriggerSource(t)
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	srcEng, ok := engines.Get(sqlitetrigger.EngineName)
	if !ok {
		t.Fatal("sqlite-trigger engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const (
		streamID = "sqlite-trigger-autoprune-pg"
		keep     = int64(2)
	)
	newStreamer := func() *Streamer {
		return &Streamer{
			Source:    srcEng,
			Target:    pgEng,
			SourceDSN: src,
			TargetDSN: pgTarget,
			StreamID:  streamID,
			// ADR-0137 Phase B: auto-prune on a tight cadence so the test sees the
			// source change-log shrink within a few seconds.
			AutoPruneChangeLog: true,
			AutoPruneInterval:  400 * time.Millisecond,
			AutoPruneKeep:      keep,
		}
	}

	// ---- Run 1: cold-start + a batch of CDC that grows the change-log ----
	ctx1, cancel1 := context.WithCancel(context.Background())
	run1 := make(chan error, 1)
	go func() { run1 <- newStreamer().Run(ctx1) }()

	if !waitForRowCount(t, pgTarget, "events", 2, 90*time.Second) {
		cancel1()
		t.Fatal("cold-start never delivered the 2 seed rows")
	}

	// 12 CDC inserts (change-log ids 1..12; the seed rows pre-date the triggers,
	// so the change-log starts empty and these are its first rows). This pushes
	// the durable frontier well past keep=2, so the prune has rows to reap.
	for i := int64(3); i <= 14; i++ {
		sqliteExec(t, src, `INSERT INTO events (id, big, blb, note) VALUES (?, ?, NULL, 'cdc')`, i, i*100)
	}
	if !waitForEventBig(t, pgTarget, 14, 1400, 60*time.Second) {
		cancel1()
		t.Fatalf("CDC batch never converged: %v", pgEventIDs(t, pgTarget))
	}

	// (a) The source change-log SHRINKS in-stream: wait for MIN(id) to advance
	// past the head (id=1), which only happens once the auto-prune sidecar has
	// reaped durably-applied rows. This is the automatic-cadence proof.
	if !waitForChangeLogMinAdvance(t, src, 1, 30*time.Second) {
		cancel1()
		t.Fatalf("auto-prune never advanced the source change-log MIN(id); ids=%v", sqliteChangeLogIDs(t, src))
	}

	// (b) A prune does NOT move the durable position. With writes stopped, record
	// the frontier, let several prune cadences pass, and assert it is unchanged —
	// pruning source rows is decoupled from the applier's watermark.
	frontier, found := readDurableLastID(t, pgTarget, streamID)
	if !found {
		cancel1()
		t.Fatal("no durable position after the batch converged")
	}
	time.Sleep(2 * time.Second) // several 400ms prune cadences, no new writes
	if postFrontier, found := readDurableLastID(t, pgTarget, streamID); !found || postFrontier != frontier {
		cancel1()
		t.Errorf("auto-prune moved the durable frontier: was %d, now %d (found=%v)", frontier, postFrontier, found)
	}
	// Every surviving change-log row is still above the reaped region: MIN(id) > 1.
	if ids := sqliteChangeLogIDs(t, src); len(ids) == 0 || ids[0] <= 1 {
		cancel1()
		t.Errorf("source change-log not bounded after quiescing: ids=%v (want MIN(id) > 1)", ids)
	}

	// Target is exactly-once so far: seed {1,2} + CDC {3..14}.
	wantAll := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}
	if got := pgEventIDs(t, pgTarget); !containsExactly(got, wantAll) {
		cancel1()
		t.Errorf("pre-resume id set = %v; want %v (exactly-once)", got, wantAll)
	}

	// ---- Hard-stop, then warm-resume AFTER the automatic prune ----
	cancel1()
	select {
	case <-run1:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run (1) did not return after ctx cancel")
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	run2 := make(chan error, 1)
	go func() { run2 <- newStreamer().Run(ctx2) }()

	// Post-resume writes: an INSERT and an UPDATE of an earlier row. The reaped
	// change-log rows were all below the durable frontier, so resume (which reads
	// id > frontier) never needs them — exactly-once must hold.
	sqliteExec(t, src, `INSERT INTO events (id, big, blb, note) VALUES (15, 1500, NULL, 'cdc-15')`)
	sqliteExec(t, src, `UPDATE events SET big = 9999 WHERE id = 3`)

	if !waitForEventBig(t, pgTarget, 15, 1500, 60*time.Second) {
		cancel2()
		t.Fatalf("warm-resume after auto-prune: id=15 never landed: %v", pgEventIDs(t, pgTarget))
	}
	if !waitForEventBig(t, pgTarget, 3, 9999, 30*time.Second) {
		t.Error("warm-resume after auto-prune: UPDATE of id=3 never propagated")
	}
	wantFinal := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	if got := pgEventIDs(t, pgTarget); !containsExactly(got, wantFinal) {
		t.Errorf("warm-resume-after-auto-prune final id set = %v; want %v (exactly-once)", got, wantFinal)
	}

	cancel2()
	select {
	case <-run2:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run (2) did not return after ctx cancel")
	}
}

// TestAutoPruneChangeLog_PgtriggerToPostgres exercises the pgtrigger DELETE code
// path for the automatic in-stream prune (SQL over pgx) end-to-end: the sidecar
// reaps the source PG change-log while the sync runs, the durable position is
// unmoved by the prune, and warm-resume stays exactly-once.
func TestAutoPruneChangeLog_PgtriggerToPostgres(t *testing.T) {
	_, srcDSN, srcCleanup := startPostgres(t)
	defer srcCleanup()
	_, dstDSN, dstCleanup := startPostgres(t)
	defer dstCleanup()

	pgExec(t, srcDSN, `CREATE TABLE events (id BIGINT PRIMARY KEY, big BIGINT NOT NULL, blb BYTEA, note TEXT)`)
	pgExec(t, srcDSN, `INSERT INTO events (id, big, blb, note) VALUES (1, 100, NULL, 'seed-1'), (2, 200, NULL, 'seed-2')`)
	if _, err := pgtrigger.Setup(context.Background(), srcDSN, pgtrigger.SetupOptions{
		Tables: []string{"events"},
		Schema: "public",
	}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}

	srcEng, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	const (
		streamID = "pgtrigger-autoprune"
		keep     = int64(2)
	)
	newStreamer := func() *Streamer {
		return &Streamer{
			Source:             srcEng,
			Target:             pgEng,
			SourceDSN:          srcDSN,
			TargetDSN:          dstDSN,
			StreamID:           streamID,
			AutoPruneChangeLog: true,
			AutoPruneInterval:  400 * time.Millisecond,
			AutoPruneKeep:      keep,
		}
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	run1 := make(chan error, 1)
	go func() { run1 <- newStreamer().Run(ctx1) }()

	if !waitForRowCount(t, dstDSN, "events", 2, 90*time.Second) {
		cancel1()
		t.Fatal("cold-start never delivered the 2 seed rows")
	}
	for i := int64(3); i <= 14; i++ {
		pgExec(t, srcDSN, `INSERT INTO events (id, big, blb, note) VALUES ($1, $2, NULL, 'cdc')`, i, i*100)
	}
	if !waitForEventBig(t, dstDSN, 14, 1400, 60*time.Second) {
		cancel1()
		t.Fatalf("pgtrigger CDC batch never converged: %v", pgEventIDs(t, dstDSN))
	}

	// (a) The source change-log shrinks in-stream (MIN(id) advances past 1).
	if !waitForPgChangeLogMinAdvance(t, srcDSN, 1, 30*time.Second) {
		cancel1()
		t.Fatalf("auto-prune never advanced the pgtrigger change-log MIN(id); ids=%v", pgChangeLogIDs(t, srcDSN))
	}

	// (b) A prune does not move the durable position (quiesce, then compare).
	frontier, found := readDurablePgtriggerLastID(t, dstDSN, streamID)
	if !found {
		cancel1()
		t.Fatal("no durable position after the batch converged")
	}
	time.Sleep(2 * time.Second)
	if postFrontier, found := readDurablePgtriggerLastID(t, dstDSN, streamID); !found || postFrontier != frontier {
		cancel1()
		t.Errorf("auto-prune moved the durable frontier: was %d, now %d", frontier, postFrontier)
	}
	if ids := pgChangeLogIDs(t, srcDSN); len(ids) == 0 || ids[0] <= 1 {
		cancel1()
		t.Errorf("source change-log not bounded after quiescing: ids=%v (want MIN(id) > 1)", ids)
	}

	cancel1()
	select {
	case <-run1:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run (1) did not return")
	}

	// Warm-resume after the automatic prune: exactly-once.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	run2 := make(chan error, 1)
	go func() { run2 <- newStreamer().Run(ctx2) }()

	pgExec(t, srcDSN, `INSERT INTO events (id, big, blb, note) VALUES (15, 1500, NULL, 'cdc-15')`)
	if !waitForEventBig(t, dstDSN, 15, 1500, 60*time.Second) {
		cancel2()
		t.Fatalf("warm-resume after auto-prune: id=15 never landed: %v", pgEventIDs(t, dstDSN))
	}
	wantFinal := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	if got := pgEventIDs(t, dstDSN); !containsExactly(got, wantFinal) {
		t.Errorf("warm-resume-after-auto-prune final id set = %v; want %v (exactly-once)", got, wantFinal)
	}

	cancel2()
	select {
	case <-run2:
	case <-time.After(20 * time.Second):
		t.Fatal("Streamer.Run (2) did not return")
	}
}

// waitForPgChangeLogMinAdvance polls the pgtrigger source change-log until its
// MIN(id) is strictly greater than aboveID, or the timeout elapses.
func waitForPgChangeLogMinAdvance(t *testing.T, dsn string, aboveID int64, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ids := pgChangeLogIDs(t, dsn)
		if len(ids) > 0 && ids[0] > aboveID {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// waitForChangeLogMinAdvance polls the source change-log until its MIN(id) is
// strictly greater than aboveID (i.e. the auto-prune reaped the head), or the
// timeout elapses.
func waitForChangeLogMinAdvance(t *testing.T, path string, aboveID int64, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ids := sqliteChangeLogIDs(t, path)
		if len(ids) > 0 && ids[0] > aboveID {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}
