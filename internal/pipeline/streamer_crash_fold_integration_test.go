//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pglogrepl"

	"sluicesync.dev/sluice/internal/laneapply"
)

// ADR-0190 amendment D's crash cells: with --exactly-once-lanes the mark
// fence's position rides the lane batch carrying the fenced transaction's
// first marked change (a fold), in that batch's own target transaction. The
// fold opens one new window — its batch committed, the transaction's other
// lanes not — and adds one rule — no other lane may commit the
// transaction's marks before the fold does. Each cell kills a stream inside
// one of those states and requires the restart to converge on the source's
// own final tables (the independent expected value), with no
// APPLY-MARK-UNTRUSTED, and the marks garbage-collected.
//
// Every fold cell needs the fenced transaction T to be ANCHORED BY ITS FOLD,
// i.e. T's start not yet persisted when T is fenced. So each runs a small
// unmarked transaction T-1 (an INSERT into the PK-only table) immediately
// before T: T's fence drains it and claims its commit as T's anchor before
// any idle checkpoint could persist it.

// crashFoldPOSeed are the extra PK-only rows the fold cells lock: po routes
// by key, so a harness can pick one whose lane differs from a table-scoped
// secondary-unique table's (crashFoldSiblingKey).
const crashFoldPOSeed = `(20, 's'), (21, 's'), (22, 's'), (23, 's'), (24, 's'), (25, 's'), (26, 's'), (27, 's'), (28, 's'), (29, 's')`

// pgCrashFoldTables are ru and rv, two more secondary-unique tables, on a
// Postgres source; the MySQL source declares the same in its own setup.
// crashFoldSecondTable picks whichever routes to a lane rs does not.
const pgCrashFoldTables = `
			CREATE TABLE ru (id BIGINT PRIMARY KEY, u VARCHAR(32) NULL CONSTRAINT ru_u UNIQUE, v VARCHAR(32) NOT NULL);
			CREATE TABLE rv (id BIGINT PRIMARY KEY, u VARCHAR(32) NULL CONSTRAINT rv_u UNIQUE, v VARCHAR(32) NOT NULL);
			INSERT INTO ru (id, v) VALUES (9, 'seed');
			INSERT INTO rv (id, v) VALUES (9, 'seed');`

// crashFoldCells registers the amendment-D cells on a source × target pair.
func crashFoldCells(t *testing.T, src crashSource, tgt resendTarget) {
	t.Run("fold_landed_siblings_pending", func(t *testing.T) { runCrashFoldLanded(t, src, tgt, 4, nil) })
	t.Run("fold_multi_lane_gated", func(t *testing.T) { runCrashFoldGated(t, src, tgt) })
	t.Run("fold_then_barrier_same_tx", func(t *testing.T) {
		runCrashFoldBarrier(t, src, tgt, `UPDATE rs SET v = 'fb' WHERE id = 7;
UPDATE po SET id = 11 WHERE id = 10;
UPDATE rs SET v = 'blocked' WHERE id = 100;
INSERT INTO po (id, v) VALUES (12, 'after');
`)
	})
	t.Run("barrier_then_fold_same_tx", func(t *testing.T) {
		runCrashFoldBarrier(t, src, tgt, `UPDATE po SET id = 11 WHERE id = 10;
UPDATE rs SET v = 'bf' WHERE id = 7;
UPDATE rs SET v = 'blocked' WHERE id = 100;
INSERT INTO po (id, v) VALUES (12, 'after');
`)
	})
	t.Run("lane_count_changed_after_fold_2_to_4", func(t *testing.T) {
		runCrashFoldLanded(t, src, tgt, 2, func(s *Streamer) { s.ApplyConcurrency = 4 })
	})
	t.Run("lane_count_changed_after_fold_lanes_to_serial", func(t *testing.T) {
		runCrashFoldLanded(t, src, tgt, 4, func(s *Streamer) { s.ApplyConcurrency = 1 })
	})
	if src.engine == "postgres" {
		// Only the Postgres logical-replication reader emits GC-41 (j)
		// keepalive boundaries.
		t.Run("fold_anchored_on_keepalive_boundary", func(t *testing.T) { runCrashFoldAfterKeepalive(t, src, tgt) })
	}
}

// crashTargetSchema is the schema the target's lane router qualifies a table
// with (the applier's routedSchema with no multi-database routing): the
// target DSN's database on MySQL, public on Postgres.
func crashTargetSchema(t *testing.T, tgt resendTarget) string {
	t.Helper()
	if tgt.engine == "postgres" {
		return "public"
	}
	cfg, err := gomysql.ParseDSN(tgt.dsn)
	if err != nil {
		t.Fatalf("parse the target DSN: %v", err)
	}
	return cfg.DBName
}

// crashLaneOf is the lane a change to table routes to with lanes lanes: a
// secondary-unique table by its name alone, a PK-only one by (name, id) —
// laneapply's own router, fed the route the target applier builds.
func crashLaneOf(t *testing.T, tgt resendTarget, lanes int, table string, id int64, keyScoped bool) int {
	t.Helper()
	rt := laneapply.Route{Qualified: crashTargetSchema(t, tgt) + "." + table, PKVals: []any{id}}
	if keyScoped {
		rt.Scope = laneapply.RouteScopeKey
	}
	return laneapply.NewRouter(lanes).LaneForRoute(rt)
}

// crashFoldSiblingKey picks a seeded PK-only po row that routes to a lane rs
// does not, with lanes lanes — so the fenced transaction's po change runs on
// a SIBLING lane of its fold. The cell's behavioural evidence (rs landed, po
// pending) fails loudly should the two ever share a lane anyway.
func crashFoldSiblingKey(t *testing.T, tgt resendTarget, lanes int) int64 {
	t.Helper()
	rs := crashLaneOf(t, tgt, lanes, "rs", 0, false)
	for id := int64(20); id <= 29; id++ {
		if crashLaneOf(t, tgt, lanes, "po", id, true) != rs {
			return id
		}
	}
	t.Fatalf("no seeded po row routes off rs's lane %d of %d", rs, lanes)
	return 0
}

// crashFoldSecondTable picks ru or rv: a second secondary-unique table on a
// lane rs does not use, with lanes lanes.
func crashFoldSecondTable(t *testing.T, tgt resendTarget, lanes int) string {
	t.Helper()
	rs := crashLaneOf(t, tgt, lanes, "rs", 0, false)
	for _, table := range []string{"ru", "rv"} {
		if crashLaneOf(t, tgt, lanes, table, 0, false) != rs {
			return table
		}
	}
	t.Fatalf("neither ru nor rv routes off rs's lane %d of %d; add a third candidate", rs, lanes)
	return ""
}

var foldedFences = regexp.MustCompile(`folded_fences=(\d+)`)

// crashFoldsLogged sums the fold tickets every run in logs issued (the
// orchestrator logs its count when a run ends).
func crashFoldsLogged(logs string) int {
	n := 0
	for _, m := range foldedFences.FindAllStringSubmatch(logs, -1) {
		v, _ := strconv.Atoi(m[1])
		n += v
	}
	return n
}

// lockWaiterHolds reports whether, on the target, a transaction that holds a
// lock on table — so has written its rows — is waiting for a lock. With the
// stream's control row held, that is a fold blocked on its position write,
// never a coordinator checkpoint (which touches only control tables); with a
// row of table held, it is a lane batch blocked on that row.
func lockWaiterHolds(tgt resendTarget, table string) bool {
	if tgt.engine == "postgres" {
		return countTarget(tgt, `SELECT COUNT(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
			WHERE c.relname = '`+table+`' AND l.pid IN (SELECT pid FROM pg_stat_activity WHERE wait_event_type = 'Lock')`) > 0
	}
	return countTarget(tgt, `SELECT COUNT(*) FROM performance_schema.data_locks dl
		JOIN performance_schema.data_lock_waits w ON dl.ENGINE_TRANSACTION_ID = w.REQUESTING_ENGINE_TRANSACTION_ID
		WHERE dl.OBJECT_NAME = '`+table+`'`) > 0
}

// crashFoldWarm runs the first process up to a settled position after the
// cold copy and one CDC row per table (the metadata caches warm, as
// runCrashToKill does), and returns the run's cancel, its result channel and
// the settled position.
func crashFoldWarm(t *testing.T, src crashSource, tgt resendTarget, fx crashStreamFixture) (context.CancelFunc, chan error, string) {
	t.Helper()
	ctx1, cancel1 := context.WithCancel(context.Background())
	run1 := make(chan error, 1)
	go func() { run1 <- fx.newStreamer().Run(ctx1) }()
	waitResend(t, run1, 120*time.Second, "the cold copy", func() bool { return len(tgt.dump(t)) >= 200 })
	src.exec(t, `INSERT INTO po (id, v) VALUES (100, 'live')`)
	src.exec(t, `UPDATE ru SET v = 'live' WHERE id = 9`)
	src.exec(t, `UPDATE rv SET v = 'live' WHERE id = 9`)
	src.exec(t, fmt.Sprintf(`INSERT INTO rs (id, u, v, pad) VALUES (%d, NULL, 'live', 'l')`, crashSentinelLive))
	waitResend(t, run1, 60*time.Second, "the first CDC rows", func() bool {
		return tgt.has(crashSentinelLive) && countTarget(tgt, "SELECT COUNT(*) FROM po WHERE id = 100") == 1 &&
			countTarget(tgt, "SELECT COUNT(*) FROM rv WHERE id = 9 AND v = 'live'") == 1
	})
	return cancel1, run1, settledPosition(t, tgt, fx.streamID)
}

// crashFoldKill cancels the first run and returns the position it left.
func crashFoldKill(t *testing.T, tgt resendTarget, cancel1 context.CancelFunc, run1 chan error, streamID string) string {
	t.Helper()
	cancel1()
	select {
	case <-run1:
	case <-time.After(60 * time.Second):
		t.Fatal("the first stream did not return after cancel")
	}
	return tgt.readPos(streamID)
}

// crashFoldConverge restarts (adjusted) and requires convergence on the
// source's tables, no APPLY-MARK-UNTRUSTED, and no mark left.
func crashFoldConverge(t *testing.T, src crashSource, tgt resendTarget, fx crashStreamFixture, adjust func(*Streamer), posAtKill string) {
	t.Helper()
	caughtUp, restartErr := resumeCrashStreamWith(t, fx.newStreamer, adjust, src, tgt, true, fx.streamID, posAtKill)
	if !caughtUp || restartErr != nil {
		t.Fatalf("the restart did not converge: caught up %v, error %v", caughtUp, restartErr)
	}
	assertCrashConverged(t, src, tgt)
	assertNoUntrustedMark(t, fx.logBuf.String())
	if left := targetMarks(t, tgt, fx.streamID); len(left) > 0 {
		t.Errorf("%d apply marks survived the position passing their transactions (garbage collection): %v", len(left), marksPerTable(left))
	}
	if n := crashFoldsLogged(fx.logBuf.String()); n == 0 {
		t.Error("no run logged a fold: amendment D's path never ran in this cell")
	}
}

// runCrashFoldLanded is fold_landed_siblings_pending: the window the fold
// opens. T updates the secondary-unique rs (its first marked change: the
// fold) and then a PK-only po row on a SIBLING lane, which a target row lock
// holds; so the fold commits — T's start persisted, T's rs mark durable —
// while T's po change cannot. At the kill: the position MOVED to T's start
// (T-1, unmarked, is behind it), the marks name T and only T, T's rs row is
// on the target and its po row is not. The restart re-delivers T first,
// skips its rs change on the mark, applies the rest, and converges.
//
// restart, when set, changes the restarted stream's lane layout
// (lane_count_changed_after_fold_*): marks are per key and the position
// format is unchanged, so any layout reads the fold's state the same way.
func runCrashFoldLanded(t *testing.T, src crashSource, tgt resendTarget, lanes int, restart func(*Streamer)) {
	fx := newCrashStreamFixture(t, src, tgt, crashMidTxnCell{concurrency: lanes, batch: 1000, exactlyOnceLanes: true})
	defer fx.teardown()
	x := crashFoldSiblingKey(t, tgt, lanes)
	cancel1, run1, posBefore := crashFoldWarm(t, src, tgt, fx)
	defer cancel1()
	release := holdRowLock(t, driverOf(tgt), tgt.dsn, fmt.Sprintf(`SELECT id FROM po WHERE id = %d FOR UPDATE`, x))
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	src.exec(t, `INSERT INTO po (id, v) VALUES (50, 'tm1')`) // T-1: unmarked, so T's anchor is not yet persisted
	src.txn(t, fmt.Sprintf("UPDATE rs SET v = 'f1' WHERE id = 7;\nUPDATE po SET v = 'fx' WHERE id = %d;\n", x))
	if !waitResendSoft(run1, 30*time.Second, func() bool { return countTarget(tgt, "SELECT COUNT(*) FROM rs WHERE id = 7 AND v = 'f1'") == 1 }) {
		t.Fatalf("T's rs change never reached the target while its po change (row %d, a sibling lane) was held: the fold "+
			"did not commit on its own, so the window this cell is about was never built", x)
	}
	time.Sleep(2 * time.Second) // let the sibling lane reach the lock
	posAfterKill := crashFoldKill(t, tgt, cancel1, run1, fx.streamID)
	release()
	released = true

	if posAfterKill == posBefore {
		t.Errorf("the position did not move to T's start though T's fold committed (still %q): the fold's position is not "+
			"durable with its marks", posAfterKill)
	}
	if src.markerless {
		if b, a := src.positionOrdinal(t, posBefore), src.positionOrdinal(t, posAfterKill); a <= b {
			t.Errorf("the persisted position did not advance past T-1 (%d → %d)", b, a)
		}
	}
	if n := countTarget(tgt, fmt.Sprintf("SELECT COUNT(*) FROM po WHERE id = %d AND v = 's'", x)); n != 1 {
		t.Errorf("T's po row %d is not at its seed value at the kill (%d rows match): the sibling lane committed through the lock", x, n)
	}
	if countTarget(tgt, "SELECT COUNT(*) FROM po WHERE id = 50") != 1 {
		t.Error("T-1's row is not on the target at the kill, yet the position passed it")
	}
	// On a marker-less source T's rs change is a transaction of its own, so
	// once its fold commits a later checkpoint may legitimately pass it and
	// delete its mark; only a source with transaction boundaries keeps T open
	// (its po change is still blocked) and so must still hold the mark.
	if !src.markerless && len(targetMarks(t, tgt, fx.streamID)) == 0 {
		t.Error("no apply mark at the kill: T's fold wrote none, so the restart's skip went ungraded")
	}
	assertMarksNameTheInterruptedTx(t, src, tgt, fx.streamID, posAfterKill, "")
	crashFoldConverge(t, src, tgt, fx, restart, posAfterKill)
}

// runCrashFoldGated is fold_multi_lane_gated: the anchored rule. T updates
// rs row 7 (its fold) and then a second secondary-unique table on another
// lane. A target row lock on rs row 7 holds the fold batch, so T's second
// marked change must wait for it (the coordinator's step 8) rather than
// commit its mark on its own lane. At the kill: no mark of T on the target at
// all, the second table's row unchanged, the position not moved. The
// restart converges.
func runCrashFoldGated(t *testing.T, src crashSource, tgt resendTarget) {
	const lanes = 4
	fx := newCrashStreamFixture(t, src, tgt, crashMidTxnCell{concurrency: lanes, batch: 1000, exactlyOnceLanes: true})
	defer fx.teardown()
	second := crashFoldSecondTable(t, tgt, lanes)
	cancel1, run1, posBefore := crashFoldWarm(t, src, tgt, fx)
	defer cancel1()
	release := holdRowLock(t, driverOf(tgt), tgt.dsn, `SELECT id FROM rs WHERE id = 7 FOR UPDATE`)
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	src.exec(t, `INSERT INTO po (id, v) VALUES (51, 'tm1')`) // T-1
	src.txn(t, fmt.Sprintf("UPDATE rs SET v = 'g1' WHERE id = 7;\nUPDATE %s SET v = 'g2' WHERE id = 9;\n", second))
	if !waitResendSoft(run1, 30*time.Second, func() bool { return lockWaiterHolds(tgt, "rs") }) {
		t.Fatal("no lane transaction waited on rs row 7: the fold batch never reached the lock, so the gate went ungraded")
	}
	time.Sleep(2 * time.Second) // let the second change reach its lane — or the wait
	posAfterKill := crashFoldKill(t, tgt, cancel1, run1, fx.streamID)
	release()
	released = true

	if marks := targetMarks(t, tgt, fx.streamID); len(marks) > 0 {
		t.Errorf("%d apply marks of the fenced transaction were durable at the kill (%v) though its fold never committed: "+
			"another lane wrote them first (the anchored rule)", len(marks), marksPerTable(marks))
	}
	if n := countTarget(tgt, "SELECT COUNT(*) FROM "+second+" WHERE id = 9 AND v = 'live'"); n != 1 {
		t.Errorf("T's %s row 9 changed at the kill (%d rows still 'live'): its change committed before T's fold", second, n)
	}
	if !src.markerless && posAfterKill != posBefore {
		t.Errorf("the position moved across the kill (%q → %q) though the fold that would move it never committed", posBefore, posAfterKill)
	}
	crashFoldConverge(t, src, tgt, fx, nil, posAfterKill)
}

// runCrashFoldBarrier is fold_then_barrier_same_tx and
// barrier_then_fold_same_tx: a fold and a lane barrier (a primary-key
// change on po) in one transaction, in either order. Fold first, the
// barrier's drain waits for the fold and its own pre-apply checkpoint has
// nothing left to write; barrier first, its pre-apply checkpoint persists
// T's start synchronously and the fence that follows needs no ticket (the
// unit pin TestOrchestrator_BarrierThenFoldIssuesNoTicket holds that). The
// kill is blocked on a later statement of T; the restart converges.
func runCrashFoldBarrier(t *testing.T, src crashSource, tgt resendTarget, body string) {
	fx := newCrashStreamFixture(t, src, tgt, crashMidTxnCell{concurrency: 4, batch: 1000, exactlyOnceLanes: true})
	defer fx.teardown()
	cancel1, run1, _ := crashFoldWarm(t, src, tgt, fx)
	defer cancel1()
	release := holdRowLock(t, driverOf(tgt), tgt.dsn, `SELECT id FROM rs WHERE id = 100 FOR UPDATE`)
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	src.exec(t, `INSERT INTO po (id, v) VALUES (10, 'x')`) // T-1, and the row T's key change moves
	src.txn(t, body)
	if !waitResendSoft(run1, 30*time.Second, func() bool { return countTarget(tgt, "SELECT COUNT(*) FROM po WHERE id = 11") == 1 }) {
		t.Fatal("T's key change never reached the target: the barrier was not reached, so the cell would pass vacuously")
	}
	time.Sleep(2 * time.Second) // let the blocked statement reach its lock
	posAfterKill := crashFoldKill(t, tgt, cancel1, run1, fx.streamID)
	release()
	released = true
	assertMarksNameTheInterruptedTx(t, src, tgt, fx.streamID, posAfterKill, "")
	crashFoldConverge(t, src, tgt, fx, nil, posAfterKill)
}

// runCrashFoldAfterKeepalive is fold_landed_siblings_pending with a GC-41 (j)
// keepalive boundary as the fold's anchor: the Postgres reader turns the
// walsender's keepalive position into an empty TxBegin/TxCommit while no
// source transaction is open, so on an idle-but-busy server the last
// boundary before T is that keepalive, not T-1's commit.
//
// To make the keepalive the anchor of a FOLD (not a position an idle
// checkpoint already persisted, which would anchor T at its fence with no
// ticket), T-1 — an update of a PK-only row — is held on its lane by a target
// row lock, so the frontier cannot reach T-1's commit or the keepalive
// boundary after it. Foreign WAL in another database makes the walsender's
// keepalive position move. Then T arrives, the lock is released, T's fence
// drains, and the highest boundary at or below T's first marked change is
// the keepalive: the fold persists it. T's sibling-lane change stays locked,
// and the kill lands with the fold committed.
//
// Independent evidence the anchor WAS a keepalive boundary: the position
// persisted at the kill lies beyond pg_current_wal_lsn() read on the source
// right after T-1 committed — no source transaction of the stream's tables
// committed between T-1 and T, so nothing but a keepalive boundary can sit
// there. The restart must converge with no APPLY-MARK-UNTRUSTED.
func runCrashFoldAfterKeepalive(t *testing.T, src crashSource, tgt resendTarget) {
	const lanes = 4
	fx := newCrashStreamFixture(t, src, tgt, crashMidTxnCell{concurrency: lanes, batch: 1000, exactlyOnceLanes: true})
	defer fx.teardown()
	x := crashFoldSiblingKey(t, tgt, lanes)
	held := int64(20) // T-1's row, any seeded po row but T's sibling
	if x == held {
		held = 21
	}
	cancel1, run1, posBefore := crashFoldWarm(t, src, tgt, fx)
	defer cancel1()

	noiseDB := fmt.Sprintf("fold_noise_%d", time.Now().UnixNano()%1e9)
	pgExec(t, src.dsn, "CREATE DATABASE "+noiseDB)
	noiseDSN, err := buildPGDSN(src.dsn, noiseDB)
	if err != nil {
		t.Fatal(err)
	}
	pgExec(t, noiseDSN, "CREATE TABLE noise (id BIGSERIAL PRIMARY KEY, v TEXT)")
	stopNoise := make(chan struct{})
	noiseDone := make(chan struct{})
	go func() {
		defer close(noiseDone)
		writeForeignWAL(noiseDSN, stopNoise)
	}()
	defer func() {
		close(stopNoise)
		<-noiseDone
	}()

	releaseT1 := holdRowLock(t, driverOf(tgt), tgt.dsn, fmt.Sprintf(`SELECT id FROM po WHERE id = %d FOR UPDATE`, held))
	releaseX := holdRowLock(t, driverOf(tgt), tgt.dsn, fmt.Sprintf(`SELECT id FROM po WHERE id = %d FOR UPDATE`, x))
	t1Released, xReleased := false, false
	defer func() {
		if !t1Released {
			releaseT1()
		}
		if !xReleased {
			releaseX()
		}
	}()
	src.exec(t, fmt.Sprintf(`UPDATE po SET v = 'tm1' WHERE id = %d`, held)) // T-1, held on its lane
	afterT1 := currentWALLSN(t, src.dsn)
	// A keepalive boundary needs 10 s since the last boundary and a keepalive
	// from the walsender; wait well past two of them.
	time.Sleep(30 * time.Second)
	src.txn(t, fmt.Sprintf("UPDATE rs SET v = 'k1' WHERE id = 7;\nUPDATE po SET v = 'kx' WHERE id = %d;\n", x))
	time.Sleep(time.Second) // let T reach its fence, which waits on T-1
	releaseT1()
	t1Released = true
	if !waitResendSoft(run1, 30*time.Second, func() bool { return countTarget(tgt, "SELECT COUNT(*) FROM rs WHERE id = 7 AND v = 'k1'") == 1 }) {
		t.Fatal("T's fold never committed after T-1 was released: the cell built nothing")
	}
	time.Sleep(2 * time.Second)
	posAfterKill := crashFoldKill(t, tgt, cancel1, run1, fx.streamID)
	releaseX()
	xReleased = true

	var tok struct {
		LSN string `json:"lsn"`
	}
	if err := json.Unmarshal([]byte(posAfterKill), &tok); err != nil {
		t.Fatalf("decode the persisted position %q: %v", posAfterKill, err)
	}
	persisted, err := pglogrepl.ParseLSN(tok.LSN)
	if err != nil {
		t.Fatalf("parse the persisted LSN %q: %v", tok.LSN, err)
	}
	if posAfterKill == posBefore || persisted <= afterT1 {
		t.Fatalf("the position at the kill (%s) is not past the WAL read right after T-1 committed (%s): no keepalive "+
			"boundary anchored the fold, so the cell measured an ordinary commit anchor", persisted, afterT1)
	}
	if n := countTarget(tgt, fmt.Sprintf("SELECT COUNT(*) FROM po WHERE id = %d AND v = 's'", x)); n != 1 {
		t.Errorf("T's po row %d changed at the kill (%d rows at the seed value)", x, n)
	}
	if len(targetMarks(t, tgt, fx.streamID)) == 0 {
		t.Error("no apply mark at the kill: T's fold wrote none")
	}
	assertMarksNameTheInterruptedTx(t, src, tgt, fx.streamID, posAfterKill, "")
	t.Logf("fold anchored at keepalive boundary %s (WAL after T-1: %s)", persisted, afterT1)
	crashFoldConverge(t, src, tgt, fx, nil, posAfterKill)
}
