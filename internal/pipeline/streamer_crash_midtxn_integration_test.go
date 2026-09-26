//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/logcapture"
)

// GC-38 (l): a process that dies INSIDE a source transaction, whose re-
// delivery on restart replays the transaction on top of the part of it the
// target already committed.
//
// # What was measured (2026-09-26, MySQL 8.0 GTID + file/pos and MariaDB 11.4
// sources; Postgres 16 and MySQL 8.0 targets)
//
// The per-change paths (--apply-batch-size 1, serial or lanes) commit each
// row of a source transaction as its own target transaction, and the lane
// path — the operator DEFAULT (--apply-concurrency auto, --apply-batch-size
// auto) — flushes a lane mid-transaction at every barrier (a primary-key
// change, a keyless or malformed row, a Truncate) and at its size cap. On
// all of them a kill mid-transaction leaves a PREFIX of the transaction on
// the target while the persisted position still points before it (the
// position is written only at TxCommit / the durable frontier), and the
// restart re-delivers the whole transaction onto that prefix. A transaction
// that frees a unique value and reuses it (INSERT k, UPDATE k→k2, INSERT k
// again; or a swap through a temporary) then collides: the resumed stream
// stops on a unique violation (Postgres 23505, MySQL 1062) and EVERY
// restart re-delivers the same transaction onto the same prefix, so it stops
// again — loud, not silent, but unrecoverable without re-copying the table.
//
// The serial BATCHED path (--apply-concurrency 1) holds a source transaction
// in one target transaction together with the TxCommit position (ADR-0027
// cohesion) ONLY WHILE THE TRANSACTION FITS ONE BATCH: the kill then rolls
// the whole prefix back and the restart converges exactly. Its batch loop
// also flushes mid-transaction at the row cap (which the ADR-0052 controller
// can shrink far below the ceiling), the byte cap, a keyless row, and a
// 100 ms delivery gap — so a larger transaction leaves a prefix there too
// (the serial_batched_over_cap cell), and --apply-concurrency 1 is NOT a
// workaround.
//
// The lane path's window is also WIDER than "mid-transaction", by
// construction rather than measurement: its checkpoint is a separate write
// from the lane commits (ADR-0104 at-least-once), so a crash after a
// transaction fully committed on its lane but before the next checkpoint
// replays that whole transaction onto its own end state — the same
// collision. Not a timing window a test can land a kill in reliably.
//
// No fix here: every fix shape (per-table exactly-once apply marks written
// with the data, a net-effect replay of the first post-resume transaction,
// or source-transaction-aligned target transactions on the lane path) either
// SKIPS or DELETES rows on resume, and getting that wrong converts this loud,
// recoverable stop into silent loss. The options and their hazards are in
// the audit backlog's GC-38 (l) for an operator decision.
//
// # What this gate pins
//
// The CONTRACT that holds today and that any fix must keep: a kill mid-
// transaction never persists a position past the transaction, never damages
// a row the transaction did not touch, and the restart either converges
// exactly or stops LOUDLY on the unique collision (never a silent
// divergence). The per-cell outcome is pinned too — the cohesive cell must
// converge, the others must stop loudly — so a fix flips those cells here,
// on purpose, and a regression that made the stop silent fails.
//
// The kill is a context cancel with the target holding a row lock on a
// row a LATE statement touches, so the applier has committed everything
// before it and is blocked. No exit path persists a position mid-transaction
// — which the gate ASSERTS (the persisted position after the cancel equals
// the one settled before the transaction ran) — so the target is left
// exactly as a SIGKILL at that moment would leave it. The independent
// expected value is a SELECT on the source.

// crashMidTxnSource is the transaction. Every shape the re-delivery can trip
// over precedes the blocking statement (id = 100), so it is in the applied
// prefix and replayed on top of itself: a row updated twice, an INSERT whose
// unique value a later INSERT of the same transaction reuses (900002 frees
// 'k', 900003 takes it), a primary-key change (a lane barrier), and a
// three-step unique swap (5 frees 'x', 6 takes it and frees 'y', 5 takes
// 'y'); after it, an INSERT-then-DELETE of one key.
const crashMidTxnSource = `
UPDATE rs SET v = 'a1' WHERE id = 1;
DELETE FROM rs WHERE id = 2;
INSERT INTO rs (id, u, v, pad) VALUES (900001, NULL, 'tmp', 't');
INSERT INTO rs (id, u, v, pad) VALUES (900002, 'k', 'x', 'p');
UPDATE rs SET u = 'k2' WHERE id = 900002;
INSERT INTO rs (id, u, v, pad) VALUES (900003, 'k', 'y', 'p');
INSERT INTO rs (id, u, v, pad) VALUES (900010, NULL, 'pk', 'p');
UPDATE rs SET id = 900011 WHERE id = 900010;
UPDATE rs SET u = 'tmp5' WHERE id = 5;
UPDATE rs SET u = 'x' WHERE id = 6;
UPDATE rs SET u = 'y' WHERE id = 5;
UPDATE rs SET v = 'blocked' WHERE id = 100;
INSERT INTO rs (id, u, v, pad) VALUES (900020, 'z', 'after', 'p');
DELETE FROM rs WHERE id = 900001;
UPDATE rs SET v = 'a3' WHERE id = 1;
`

// crashMidTxnTouched is every id the transaction touches. A row outside it
// that differs after the restart is damage, never an expected collision.
var crashMidTxnTouched = map[int64]bool{
	1: true, 2: true, 5: true, 6: true, 100: true,
	900001: true, 900002: true, 900003: true, 900010: true, 900011: true, 900020: true,
}

const (
	crashSentinelLive = 4000000 // the first CDC row, before the transaction
	crashSentinelPost = 4000001 // written after the restart; its arrival is "caught up"
)

func TestStreamer_CrashMidTxn_MySQLGTID_ToPostgres(t *testing.T) {
	src, _, cleanup := startMySQLGTID(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	runCrashMidTxnCells(t, "mysql", src, pgResendTarget(pgDSN))
}

func TestStreamer_CrashMidTxn_MySQLGTID_ToMySQL(t *testing.T) {
	src, _, cleanup := startMySQLGTID(t)
	defer cleanup()
	_, tgt, tgtCleanup := startMySQL(t)
	defer tgtCleanup()
	runCrashMidTxnCells(t, "mysql", src, mysqlResendTarget(t, tgt))
}

func TestStreamer_CrashMidTxn_MariaDB_ToPostgres(t *testing.T) {
	src, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	runCrashMidTxnCells(t, "mariadb", src, pgResendTarget(pgDSN))
}

func TestStreamer_CrashMidTxn_MySQLFilePos_ToPostgres(t *testing.T) {
	src, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	runCrashMidTxnCells(t, "mysql", src, pgResendTarget(pgDSN))
}

// crashMidTxnCell is one apply-path configuration and what it must do.
type crashMidTxnCell struct {
	name        string
	concurrency int
	batch       int
	// cohesive: the path holds THIS source transaction (it fits one batch) in
	// one target transaction with its position, so the kill leaves no prefix
	// and the restart converges exactly. Otherwise the kill leaves a prefix and the
	// restart stops loudly on the unique collision (GC-38 (l), KNOWN LOUD —
	// a fix flips this).
	cohesive bool
	// fixedCap turns the ADR-0052 batch-size controller off, so the batch
	// size is exactly the configured one.
	fixedCap bool
}

func runCrashMidTxnCells(t *testing.T, srcEngine, srcDSN string, tgt resendTarget) {
	for _, cell := range []crashMidTxnCell{
		{name: "serial_per_change", concurrency: 1, batch: 0},
		{name: "serial_batched", concurrency: 1, batch: 1000, cohesive: true},
		// The same path with the transaction LARGER than one batch: the row
		// cap flushes mid-transaction, so the cohesion above holds only for
		// a transaction that fits one batch.
		{name: "serial_batched_over_cap", concurrency: 1, batch: 5, fixedCap: true},
		{name: "lanes_per_change", concurrency: 0, batch: 0},
		{name: "lanes_batched", concurrency: 0, batch: 1000},
	} {
		t.Run(cell.name, func(t *testing.T) {
			runCrashMidTxnCell(t, srcEngine, srcDSN, tgt, cell)
		})
	}
}

func runCrashMidTxnCell(t *testing.T, srcEngine, srcDSN string, tgt resendTarget, cell crashMidTxnCell) {
	applyDDLMySQL(t, srcDSN, `DROP TABLE IF EXISTS rs`)
	tgt.drop(t)
	applyDDLMySQL(t, srcDSN, `
		CREATE TABLE rs (
			id  BIGINT NOT NULL PRIMARY KEY,
			u   VARCHAR(32) NULL,
			v   VARCHAR(32) NOT NULL,
			pad MEDIUMTEXT NOT NULL,
			UNIQUE KEY rs_u (u)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO rs (id, u, v, pad)
		  WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 200)
		  SELECT n, NULL, 'seed', 's' FROM g;
		UPDATE rs SET u = 'x' WHERE id = 5;
		UPDATE rs SET u = 'y' WHERE id = 6;
	`)

	logBuf := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)

	srcEng, ok := engines.Get(srcEngine)
	if !ok {
		t.Fatalf("engine %q not registered", srcEngine)
	}
	tgtEng, ok := engines.Get(tgt.engine)
	if !ok {
		t.Fatalf("engine %q not registered", tgt.engine)
	}
	streamID := fmt.Sprintf("crash-%s-%d-%d-%d", srcEngine, cell.concurrency, cell.batch, time.Now().UnixNano()%1e6)
	newStreamer := func() *Streamer {
		return &Streamer{
			Source:           srcEng,
			Target:           tgtEng,
			SourceDSN:        srcDSN,
			TargetDSN:        tgt.dsn,
			StreamID:         streamID,
			ApplyConcurrency: cell.concurrency,
			ApplyBatchSize:   cell.batch,
			AutoTune:         cell.batch > 1 && !cell.fixedCap,
		}
	}

	// ---- First process: cold copy, one CDC row, then the transaction. ----
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	run1 := make(chan error, 1)
	go func() { run1 <- newStreamer().Run(ctx1) }()
	waitResend(t, run1, 120*time.Second, "the cold copy", func() bool { return len(tgt.dump(t)) >= 200 })
	applyDDLMySQL(t, srcDSN, fmt.Sprintf(`INSERT INTO rs (id, u, v, pad) VALUES (%d, NULL, 'live', 'l')`, crashSentinelLive))
	waitResend(t, run1, 60*time.Second, "the first CDC row", func() bool { return tgt.has(crashSentinelLive) })
	// The lane path checkpoints on an idle tick, so the persisted position
	// may lag the live row briefly: wait for it to settle before the
	// transaction, or "unchanged across the kill" would compare a stale read.
	posBefore := settledPosition(t, tgt, streamID)

	release := holdRowLock(t, map[string]string{"postgres": "pgx", "mysql": "mysql"}[tgt.engine], tgt.dsn,
		`SELECT id FROM rs WHERE id = 100 FOR UPDATE`)
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	runSourceTxnPlain(t, srcDSN, crashMidTxnSource)

	// A path that committed a prefix shows the transaction's first statement;
	// the cohesive path shows nothing until the commit it cannot reach.
	prefixApplied := waitResendSoft(run1, 30*time.Second, func() bool { return tgt.dump(t)[1].v == "a1" })
	time.Sleep(2 * time.Second) // let the applier reach the lock

	// ---- The kill. ----
	cancel1()
	select {
	case <-run1:
	case <-time.After(60 * time.Second):
		t.Fatal("the first stream did not return after cancel")
	}
	posAfterKill := tgt.readPos(streamID)
	release()
	released = true

	if posAfterKill != posBefore {
		t.Errorf("the persisted position MOVED across a kill mid-transaction: %q before the transaction, %q after — "+
			"a position past a partly applied transaction skips its remainder on resume (silent loss)", posBefore, posAfterKill)
	}
	if prefixApplied == cell.cohesive {
		t.Errorf("prefix committed before the kill = %v; want %v for this path (cohesive %v)", prefixApplied, !cell.cohesive, cell.cohesive)
	}

	// ---- Resume, twice for the loud cells: every restart must stop the same way. ----
	caughtUp, restartErr := resumeCrashStream(t, newStreamer, srcDSN, tgt, true)
	logs := logBuf.String()
	diff := diffResendRows(dumpSourceResend(t, srcDSN), tgt.dump(t))
	t.Logf("prefix before the kill %v; restart caught up %v, error %v", prefixApplied, caughtUp, restartErr)

	assertCrashDamageConfined(t, dumpSourceResend(t, srcDSN), tgt.dump(t))
	if cell.cohesive {
		if !caughtUp || restartErr != nil {
			t.Errorf("the cohesive path did not resume: caught up %v, error %v", caughtUp, restartErr)
		}
		if diff != "" {
			t.Errorf("the cohesive path diverged after a kill mid-transaction:\n%s", diff)
		}
		return
	}
	// KNOWN LOUD (GC-38 (l)): the restart stops on the unique collision.
	if caughtUp || restartErr == nil {
		t.Errorf("GC-38 (l) cell changed: the restart after a kill mid-transaction resumed (caught up %v, error %v). "+
			"If a fix landed, make this cell converge exactly and update the backlog; if not, find out why", caughtUp, restartErr)
		if diff != "" {
			t.Errorf("…and the target SILENTLY differs from the source:\n%s", diff)
		}
		return
	}
	if !isUniqueCollision(restartErr) {
		t.Errorf("the restart stopped, but not on the unique collision GC-38 (l) describes: %v", restartErr)
	}
	if diff == "" {
		t.Errorf("the restart stopped yet the target equals the source — the stop is no longer explained by the collision")
	}
	_, again := resumeCrashStream(t, newStreamer, srcDSN, tgt, false)
	if !isUniqueCollision(again) {
		t.Errorf("a SECOND restart did not stop on the same collision: %v", again)
	}
	if n := strings.Count(logs, "retrying"); n > 0 {
		t.Logf("the resumed stream logged %d retry line(s)", n)
	}
}

// resumeCrashStream runs one resumed attempt of the stream. When
// writeSentinel it first writes the post-restart sentinel, whose arrival is
// "caught up". A cancel after catching up is the clean stop, reported as a
// nil error.
func resumeCrashStream(t *testing.T, newStreamer func() *Streamer, srcDSN string, tgt resendTarget, writeSentinel bool) (bool, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := make(chan error, 1)
	go func() { run <- newStreamer().Run(ctx) }()
	if writeSentinel {
		applyDDLMySQL(t, srcDSN, fmt.Sprintf(`INSERT INTO rs (id, u, v, pad) VALUES (%d, NULL, 'sentinel', 's')`, crashSentinelPost))
	}
	caughtUp := waitResendSoft(run, 3*time.Minute, func() bool { return tgt.has(crashSentinelPost) })
	cancel()
	var err error
	select {
	case err = <-run:
	case <-time.After(60 * time.Second):
		t.Fatal("the resumed stream did not return after cancel")
	}
	if caughtUp {
		err = nil
	}
	return caughtUp, err
}

// assertCrashDamageConfined fails for any row OUTSIDE the transaction (and
// the post-restart sentinel) that differs between source and target — the
// collision may leave the transaction's own rows at its prefix, never
// anything else.
func assertCrashDamageConfined(t *testing.T, want, got map[int64]resendRow) {
	t.Helper()
	for id, w := range want {
		if crashMidTxnTouched[id] || id == crashSentinelPost {
			continue
		}
		if g, ok := got[id]; !ok || g != w {
			t.Errorf("row %d, which the transaction does not touch, differs after the restart: source %+v, target %+v (present %v)", id, w, g, ok)
		}
	}
	for id := range got {
		if _, ok := want[id]; !ok && !crashMidTxnTouched[id] {
			t.Errorf("row %d exists on the target but not the source, and the transaction does not touch it", id)
		}
	}
}

// isUniqueCollision reports whether err is the unique-key collision GC-38 (l)
// describes, on either target engine.
func isUniqueCollision(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "23505") || strings.Contains(s, "Error 1062")
}

// settledPosition polls the persisted position until two reads 1.5s apart
// agree.
func settledPosition(t *testing.T, tgt resendTarget, streamID string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	prev := tgt.readPos(streamID)
	for time.Now().Before(deadline) {
		time.Sleep(1500 * time.Millisecond)
		cur := tgt.readPos(streamID)
		if cur != "" && cur == prev {
			return cur
		}
		prev = cur
	}
	t.Fatalf("the persisted position never settled (last %q)", prev)
	return ""
}

// runSourceTxnPlain runs body as one source transaction. Unlike
// runSourceTxnMySQL it sets no MySQL-only session variable, so it serves
// MariaDB too.
func runSourceTxnPlain(t *testing.T, dsn, body string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := db.ExecContext(ctx, "START TRANSACTION;"+body+"COMMIT;"); err != nil {
		t.Fatalf("source transaction: %v", err)
	}
}
