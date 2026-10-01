//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

// GC-41 (i) end to end: `sync start` stopped in the MIDDLE of its cold
// copy, then started again, ends with every source row on the target.
//
// The unit cells (copy_source_end_test.go) prove the verdict on fakes
// that resolve every writer race the bad way. This is the other half:
// real readers, real writers, a real progress store, and the detached
// COMPLETE write (recordCommittedWorkCtx) in place — the combination
// that was reverted before v0.156.7 because a stop could make a
// partially copied table's COMPLETE row durable. Had that happened here,
// the restart's stopped-cold-start resume would have found every table
// "complete", probed big_t non-empty (it holds the partial rows) and
// small_t exempt (RowsCopied=0), and announced COLD-START-RESUMED over a
// copy that never finished — the tail of big_t and all of small_t gone
// for good. Both target engines, because the writer select race is
// engine code (Postgres COPY / batch, MySQL LOAD DATA / batch).
//
// What a restart after a mid-copy stop DOES today, and this pins: it
// refuses loudly, naming the slot the stopped run left (an unfinished
// copy cannot be resumed — everyTableCopied), and the operator's
// recovery — drop that slot, re-run with --reset-target-data — copies
// everything. The independent expected value is the SOURCE's own row
// count, read from the source, never from anything sluice recorded.
func TestStreamer_StopMidColdCopy_ThenResume_EveryRowArrives(t *testing.T) {
	for _, target := range []string{"postgres", "mysql"} {
		t.Run(target, func(t *testing.T) { runStopMidColdCopyThenResume(t, target) })
	}
}

// stopFixtureBigRows is sized so big_t's copy is still running when the
// stop lands on the first sign of it in flight.
const stopFixtureBigRows = 300000

func runStopMidColdCopyThenResume(t *testing.T, target string) {
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	src, pgTgt, cleanup := startPostgresLogical(t)
	t.Cleanup(cleanup)

	tgtEng, tgtDSN, count := pgEng, pgTgt, pollRowCount
	if target == "mysql" {
		mysqlEng, ok := engines.Get("mysql")
		if !ok {
			t.Fatal("mysql engine not registered")
		}
		_, mysqlTgt, mysqlCleanup := startMySQL(t)
		t.Cleanup(mysqlCleanup)
		tgtEng, tgtDSN, count = mysqlEng, mysqlTgt, pollRowCountMySQL
	}

	applyDDL(t, src, `CREATE TABLE big_t (id BIGINT PRIMARY KEY, v TEXT NOT NULL);
		INSERT INTO big_t (id, v) SELECT g, md5(g::text) FROM generate_series(1, 300000) g;
		CREATE TABLE small_t (id BIGINT PRIMARY KEY);
		INSERT INTO small_t (id) SELECT g FROM generate_series(1, 500) g;`)

	streamID := "gc41i-stop-mid-copy-" + target
	newRun := func() *Streamer {
		return &Streamer{Source: pgEng, Target: tgtEng, SourceDSN: src, TargetDSN: tgtDSN, StreamID: streamID}
	}

	// Run 1: stop the instant big_t's copy is seen in flight ON THE
	// TARGET — the target's own evidence, not sluice's bookkeeping (the
	// sync lane writes no in-progress breadcrumb). MySQL commits the copy
	// in batches, so its first rows are visible mid-copy; a Postgres COPY
	// commits once, so there the evidence is the COPY statement running.
	copyInFlight := func() bool { return count(tgtDSN, "big_t") > 0 }
	if target == "postgres" {
		copyInFlight = func() bool {
			return pgQueryOne[bool](t, tgtDSN, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
				WHERE state = 'active' AND query ILIKE 'COPY%big_t%')`)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- newRun().Run(ctx) }()
	deadline := time.Now().Add(3 * time.Minute)
	for !copyInFlight() {
		if time.Now().After(deadline) {
			t.Fatal("big_t's copy was never seen in flight within 3m")
		}
		select {
		case err := <-errCh:
			t.Fatalf("the cold start returned (%v) before big_t's copy was seen in progress", err)
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(2 * time.Minute):
		t.Fatal("the stopped cold start did not return")
	}
	if runErr == nil {
		t.Fatal("a cold start stopped mid-copy returned nil — its caller reads that as a finished copy")
	}
	mid := count(tgtDSN, "big_t")
	if mid >= stopFixtureBigRows {
		t.Fatalf("the stop landed after big_t finished copying (%d rows on the target); this cell no longer "+
			"stops MID-copy — enlarge stopFixtureBigRows", mid)
	}
	t.Logf("stopped mid-copy: big_t has %d of %d rows on the target; run error = %v", mid, stopFixtureBigRows, runErr)

	// The GC-41 (i) assertion on a real store: every COMPLETE row the
	// stopped run left is TRUE — the target holds the source's whole
	// table. (small_t may legitimately finish before the stop lands on
	// big_t: the copy pool runs tables concurrently.)
	completeTables := 0
	for _, table := range []string{"big_t", "small_t"} {
		e, found := recordedTableProgress(t, tgtEng, tgtDSN, streamID, table)
		if !found || e.State != ir.TableProgressComplete {
			continue
		}
		completeTables++
		if got, want := count(tgtDSN, table), pollRowCount(src, table); got != want {
			t.Fatalf("%s was recorded COMPLETE (RowsCopied=%d) by a cold start stopped mid-copy, but the target holds "+
				"%d of the source's %d rows — the restart would skip the rest (GC-41 (i))", table, e.RowsCopied, got, want)
		}
	}
	t.Logf("tables the stopped run truthfully recorded COMPLETE: %d of 2", completeTables)

	// Run 2: a plain restart. An unfinished copy must not be resumed: it
	// refuses loudly, naming the slot — never COLD-START-RESUMED.
	logs := captureSlog(t)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel2()
	err := newRun().Run(ctx2)
	if err == nil {
		t.Fatal("a restart after a mid-copy stop SUCCEEDED; with the copy unfinished it can only have resumed over it")
	}
	if !strings.Contains(err.Error(), "sluice_slot") {
		t.Errorf("the restart failed without naming the slot the operator has to drop: %v", err)
	}
	if strings.Contains(logs.String(), coldStartResumedMarker) {
		t.Fatalf("the restart announced %s over a copy that never finished", coldStartResumedMarker)
	}

	// Run 3: the operator's recovery — drop the slot, re-run with
	// --reset-target-data. Every source row arrives, and the stream is
	// live afterwards (a row committed now arrives too).
	dropSourceSlot(t, src, "sluice_slot")
	ctx3, cancel3 := context.WithCancel(context.Background())
	defer cancel3()
	errCh3 := make(chan error, 1)
	recovery := newRun()
	recovery.ResetTargetData = true
	go func() { errCh3 <- recovery.Run(ctx3) }()
	waitForCount := func(table string, want int) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Minute)
		for {
			got := count(tgtDSN, table)
			if got == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("after the recovery %s holds %d rows, the source holds %d", table, got, want)
			}
			select {
			case err := <-errCh3:
				t.Fatalf("the recovery stream exited (%v) with %s at %d of %d rows", err, table, got, want)
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	waitForCount("big_t", pollRowCount(src, "big_t"))
	waitForCount("small_t", pollRowCount(src, "small_t"))
	applyDDL(t, src, `INSERT INTO small_t (id) VALUES (100001);`)
	waitForCount("small_t", pollRowCount(src, "small_t"))
	cancel3()
	select {
	case <-errCh3:
	case <-time.After(2 * time.Minute):
		t.Fatal("the recovery stream did not stop")
	}
}

// recordedTableProgress reads one table's progress row from the target's
// state store, keyed by the sync migration id. found is false until the
// run has written its header.
func recordedTableProgress(t *testing.T, eng ir.Engine, dsn, streamID, table string) (ir.TableProgress, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := openMigrationStateStore(ctx, eng, dsn, "")
	if err != nil || store == nil {
		t.Fatalf("open the target's state store: %v", err)
	}
	defer func() { _ = store.Close() }()
	state, found, err := store.Read(ctx, syncMigrationID(streamID))
	if err != nil || !found {
		return ir.TableProgress{}, false
	}
	e, ok := state.TableProgress[table]
	return e, ok
}

// dropSourceSlot is what `sluice slot drop` does on a Postgres source.
func dropSourceSlot(t *testing.T, dsn, slot string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// The stopped run's walsender can hold the slot active for a moment.
	for {
		_, err = db.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", slot)
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("drop slot %s: %v", slot, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
