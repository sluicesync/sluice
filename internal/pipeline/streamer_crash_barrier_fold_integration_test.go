//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"fmt"
	"testing"
	"time"
)

// ADR-0190 amendment E's crash cells: on the default lane path (no flag) a
// lane barrier's pre-apply checkpoint — the barrier's transaction's START —
// rides the barrier's own target transaction instead of a synchronous commit
// before it. Each cell kills a stream on one side of that commit and grades
// the state against the SOURCE (the independent expected value): the
// position against the transaction boundary the source itself records, the
// marks against the interrupted transaction, and, after the restart, every
// table the transactions touch against the source's own final rows.
//
// Both cells run a barrier-marking transaction T−1 immediately before T, so
// T's start (T−1's commit) is NOT yet persisted when T's barrier arrives —
// the case where the barrier has a checkpoint to fold. T−1's barrier is a
// keyless insert, or, on a source that refuses a keyless table
// (postgres-trigger, VStream), a primary-key change.

// crashBarrierFoldCells registers the amendment-E cells on a source × target
// pair.
func crashBarrierFoldCells(t *testing.T, src crashSource, tgt resendTarget) {
	t.Run("barrier_fold_blocked", func(t *testing.T) { runCrashBarrierFoldBlocked(t, src, tgt) })
	t.Run("barrier_fold_landed", func(t *testing.T) { runCrashBarrierFoldLanded(t, src, tgt) })
}

// crashBarrierFoldPrev is T−1: one source transaction carrying a lane barrier
// that writes an apply mark, then an ordinary lane change to po row
// crashBarrierFoldHeldRow, which the cell holds with a target row lock.
//
// The held row is what makes the cell deterministic. Without it, T−1's
// commit is durable the moment its barrier returns, and the coordinator's
// idle tick and T's first change race in one select: when the tick wins it
// persists T−1's commit — T's start — and T's barrier has nothing left to
// fold, so the cell grades nothing (and a mutant that restores the
// pre-amendment order writes the very same position). With T−1's lane
// change held, T−1's commit cannot become durable until the cell releases
// it — after T is queued behind it — and T's barrier then drains and folds
// inside one handle call, with no select in between.
func crashBarrierFoldPrev(src crashSource) string {
	held := fmt.Sprintf("UPDATE po SET v = 'held' WHERE id = %d;\n", crashBarrierFoldHeldRow)
	if src.noKeyless {
		return "UPDATE po SET id = 47 WHERE id = 28;\n" + held
	}
	return "INSERT INTO kl (k, v) VALUES (7, 'prev');\n" + held
}

// crashBarrierFoldHeldRow is the seeded po row T−1's lane change updates.
const crashBarrierFoldHeldRow = 27

// crashBarrierFoldSendPrevAndT runs T−1 with its lane change held on the
// target, then T, then — once T has had time to reach the coordinator and
// wait in its barrier's drain behind T−1 — releases the held row. It
// returns T−1's identity by the source's own record ("" where there is no
// cheap derivation).
func crashBarrierFoldSendPrevAndT(t *testing.T, src crashSource, tgt resendTarget, body string) (prevTx string) {
	t.Helper()
	releaseHeld := holdRowLock(t, driverOf(tgt), tgt.dsn, fmt.Sprintf(`SELECT id FROM po WHERE id = %d FOR UPDATE`, crashBarrierFoldHeldRow))
	src.txn(t, crashBarrierFoldPrev(src)) // T−1
	if src.lastTxID != nil && !src.markerless {
		prevTx = src.lastTxID(t)
	}
	src.txn(t, body) // T
	time.Sleep(2 * time.Second)
	releaseHeld()
	return prevTx
}

// crashBarrierFoldWarm is crashFoldWarm plus a keyless CDC row (the keyless
// table's metadata warm on both sides), returning the settled position.
func crashBarrierFoldWarm(t *testing.T, src crashSource, tgt resendTarget, fx crashStreamFixture) (cancel func(), run1 chan error, posBefore string) {
	t.Helper()
	cancel1, run, _ := crashFoldWarm(t, src, tgt, fx)
	if !src.noKeyless {
		src.exec(t, `INSERT INTO kl (k, v) VALUES (100, 'live')`)
		waitResend(t, run, 60*time.Second, "the keyless CDC row", func() bool {
			return countTarget(tgt, "SELECT COUNT(*) FROM kl WHERE k = 100") == 1
		})
	}
	return cancel1, run, settledPosition(t, tgt, fx.streamID)
}

// barrierUpdateWaiting reports whether, on the target, the barrier's own
// statement — the primary-key UPDATE of po the cell blocks — is waiting on a
// lock: the coordinator's barrier transaction (on the applier's primary
// pool), not a lane, since T sends nothing else to po.
func barrierUpdateWaiting(tgt resendTarget) bool {
	if tgt.engine == "postgres" {
		return countTarget(tgt, `SELECT COUNT(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND query ILIKE 'UPDATE%po%'`) > 0
	}
	return countTarget(tgt, `SELECT COUNT(*) FROM information_schema.innodb_trx
		WHERE trx_state = 'LOCK WAIT' AND trx_query LIKE 'UPDATE%po%'`) > 0
}

// runCrashBarrierFoldBlocked is barrier_fold_blocked: T−1 writes a barrier
// mark; T's FIRST change is a primary-key change on po (a barrier) that a
// target row lock on the row it updates holds — so the barrier's
// transaction, carrying T's start, cannot commit. At the kill: the persisted
// position is still T−1's START (the position settled before T−1, on a
// source with transaction boundaries the commit just before T−1's), the
// marks name T−1 and only T−1 (by the source's own record where it can be
// derived), and T's barrier has not applied. The restart re-delivers T−1
// first, skips its barrier on the mark, and converges on the source.
//
// The position assertion is the discriminator: with the pre-amendment
// order the coordinator persisted T's start synchronously BEFORE the
// barrier's transaction began, so the kill would find the position at T's
// start, and this cell would fail — it grades the fold, not the class.
func runCrashBarrierFoldBlocked(t *testing.T, src crashSource, tgt resendTarget) {
	fx := newCrashStreamFixture(t, src, tgt, crashMidTxnCell{concurrency: 4, batch: 1000})
	defer fx.teardown()
	cancel1, run1, posBefore := crashBarrierFoldWarm(t, src, tgt, fx)
	defer cancel1()
	release := holdRowLock(t, driverOf(tgt), tgt.dsn, `SELECT id FROM po WHERE id = 1 FOR UPDATE`)
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	prevTx := crashBarrierFoldSendPrevAndT(t, src, tgt, "UPDATE po SET id = 43 WHERE id = 1;\nINSERT INTO po (id, v) VALUES (44, 'after');\n")
	if !waitResendSoft(run1, 30*time.Second, func() bool { return barrierUpdateWaiting(tgt) }) {
		t.Fatal("T's barrier never waited on po row 1 on the target: the barrier transaction was not reached, so the cell " +
			"would grade nothing")
	}
	time.Sleep(2 * time.Second) // any checkpoint that could run, has
	posAfterKill := crashFoldKill(t, tgt, cancel1, run1, fx.streamID)
	release()
	released = true

	graded := true
	if src.markerless && posAfterKill != posBefore {
		// On a marker-less source the coordinator's idle tick can win the
		// select against T's first change once T−1's barrier returns, settle
		// T−1 (its own transaction) as a boundary and persist it — T's start,
		// legitimately, leaving T's barrier nothing to fold. That outcome
		// grades nothing here; anything past T−1 is still a defect.
		if b, a := src.positionOrdinal(t, posBefore), src.positionOrdinal(t, posAfterKill); a != b+1 {
			t.Errorf("the persisted position moved from ordinal %d to %d across the kill; only T−1's own (%d) is legal", b, a, b+1)
		}
		graded = false
		t.Logf("marker-less: an idle checkpoint persisted T−1 (%q) before T's barrier, so this run had no checkpoint to fold "+
			"and grades only the bound; the fold is graded on the sources with transaction boundaries", posAfterKill)
	} else if posAfterKill != posBefore {
		t.Errorf("the persisted position moved to %q though T's barrier transaction — the only writer allowed to persist "+
			"T's start — never committed (T−1's start: %q): T's start was persisted ahead of the barrier", posAfterKill, posBefore)
	}
	if n := countTarget(tgt, "SELECT COUNT(*) FROM po WHERE id = 1"); n != 1 {
		t.Errorf("T's barrier applied at the kill (po row 1 present %d times): the lock did not hold it", n)
	}
	if graded && len(targetMarks(t, tgt, fx.streamID)) == 0 {
		t.Error("no apply mark at the kill: T−1's barrier wrote none, so the restart's skip went ungraded")
	}
	want := prevTx
	if src.markerless {
		want = "" // firstTxAfter(posAtKill) derives T−1 from the change log
	}
	assertMarksNameTheInterruptedTx(t, src, tgt, fx.streamID, posAfterKill, want)
	crashBarrierFoldConverge(t, src, tgt, fx, posAfterKill)
}

// runCrashBarrierFoldLanded is barrier_fold_landed: T−1 writes a barrier
// mark; T's barrier (a primary-key change on po) folds T's start and
// COMMITS — deleting T−1's marks and writing its own in the same commit —
// and then a later statement of T, on a lane, is held by a target row lock.
// At the kill: the persisted position is T's start (moved off T−1's), the
// marks name T and only T. The restart re-delivers T first, skips its
// barrier on the mark, and converges with exactly one copy of T−1's change,
// graded against the source.
//
// A fold that wrote the marks and the closed deletes but no position would
// leave T−1's marks deleted with the position still at T−1's start, and the
// restart would re-apply T−1 unmarked: a duplicate keyless row (or a loud
// key collision), which the convergence check catches.
func runCrashBarrierFoldLanded(t *testing.T, src crashSource, tgt resendTarget) {
	fx := newCrashStreamFixture(t, src, tgt, crashMidTxnCell{concurrency: 4, batch: 1000})
	defer fx.teardown()
	cancel1, run1, posBefore := crashBarrierFoldWarm(t, src, tgt, fx)
	defer cancel1()
	release := holdRowLock(t, driverOf(tgt), tgt.dsn, `SELECT id FROM rs WHERE id = 100 FOR UPDATE`)
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	crashBarrierFoldSendPrevAndT(t, src, tgt, "UPDATE po SET id = 45 WHERE id = 2;\nUPDATE rs SET v = 'blocked' WHERE id = 100;\nINSERT INTO po (id, v) VALUES (46, 'after');\n")
	if !waitResendSoft(run1, 30*time.Second, func() bool { return countTarget(tgt, "SELECT COUNT(*) FROM po WHERE id = 45") == 1 }) {
		t.Fatal("T's barrier never committed: the cell built nothing")
	}
	time.Sleep(2 * time.Second) // let the blocked lane statement reach its lock
	posAfterKill := crashFoldKill(t, tgt, cancel1, run1, fx.streamID)
	release()
	released = true

	if posAfterKill == posBefore {
		t.Errorf("the persisted position did not move off T−1's start (%q) though T's barrier — carrying T's start — "+
			"committed: the fold wrote no position", posAfterKill)
	}
	if src.markerless {
		if b, a := src.positionOrdinal(t, posBefore), src.positionOrdinal(t, posAfterKill); a <= b {
			t.Errorf("the persisted position did not advance past T−1 (%d → %d)", b, a)
		}
	}
	if n := countTarget(tgt, "SELECT COUNT(*) FROM rs WHERE id = 100 AND v = 'blocked'"); n != 0 {
		t.Errorf("T's blocked statement applied at the kill (%d rows): the lock did not hold it", n)
	}
	if !src.markerless && len(targetMarks(t, tgt, fx.streamID)) == 0 {
		t.Error("no apply mark at the kill: T's barrier wrote none, so the restart's skip went ungraded")
	}
	assertMarksNameTheInterruptedTx(t, src, tgt, fx.streamID, posAfterKill, "")
	crashBarrierFoldConverge(t, src, tgt, fx, posAfterKill)
}

// crashBarrierFoldConverge restarts and requires convergence on the source's
// tables, no APPLY-MARK-UNTRUSTED, and no mark left.
func crashBarrierFoldConverge(t *testing.T, src crashSource, tgt resendTarget, fx crashStreamFixture, posAtKill string) {
	t.Helper()
	caughtUp, restartErr := resumeCrashStreamWith(t, fx.newStreamer, nil, src, tgt, true, fx.streamID, posAtKill)
	if !caughtUp || restartErr != nil {
		t.Fatalf("the restart did not converge: caught up %v, error %v", caughtUp, restartErr)
	}
	assertCrashConverged(t, src, tgt)
	assertNoUntrustedMark(t, fx.logBuf.String())
	if left := targetMarks(t, tgt, fx.streamID); len(left) > 0 {
		t.Errorf("%d apply marks survived the position passing their transactions (garbage collection): %v", len(left), marksPerTable(left))
	}
	if n := countTarget(tgt, fmt.Sprintf("SELECT COUNT(*) FROM rs WHERE id = %d", crashSentinelPost)); n != 1 {
		t.Errorf("the post-restart sentinel is on the target %d times; want 1", n)
	}
}
