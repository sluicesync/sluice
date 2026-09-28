//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/logcapture"
)

// GC-38 (l) / ADR-0190: a process that dies INSIDE a source transaction,
// whose re-delivery on restart replays the transaction on top of the part of
// it the target already committed.
//
// # What was measured before ADR-0190 (2026-09-26)
//
// The per-change paths (--apply-batch-size 1, serial or lanes) commit each
// row of a source transaction as its own target transaction, and the lane
// path — the operator DEFAULT (--apply-concurrency auto, --apply-batch-size
// auto) — flushes a lane mid-transaction at every barrier and at its size
// cap. On all of them a kill mid-transaction leaves a PREFIX of the
// transaction on the target while the persisted position still points before
// it, and the restart re-delivers the whole transaction onto that prefix. A
// transaction that frees a unique value and reuses it, or swaps one through a
// temporary, then collided (23505 / 1062) on every restart. The serial
// BATCHED path held a transaction in one target transaction with its position
// only while it fit one batch.
//
// # What ADR-0190 changes, and what this gate pins now
//
// Every reader that stamps a stable change identity (MySQL GTID, MySQL
// file/pos, MariaDB, Postgres pgoutput) lets the applier write an apply mark
// with the rows of every non-idempotent change, in the same target
// transaction, and skip on restart what a mark proves already landed. So on
// the serial per-change, serial batched and lanes-per-change paths the same
// crash now CONVERGES — target == source, the position advances, no refusal —
// graded against the source's own final table state (the independent
// expected value).
//
// The lane BATCH path (lanes_batched) is deliberately still the pre-ADR-0190
// loud collision: lane batches CHECK marks but write none. ADR-0190 §3's
// lane protocol (each lane writes its marks in its own transaction) can,
// with two transactions in the lane post-commit window sharing a key, skip
// the later transaction's change after the earlier one's re-applied change
// resurrected a row — a loud collision turned silent. The implementation
// note in the ADR carries the analysis and the proposed amendment; until the
// operator decides it, this cell pins the old loud behaviour, so the
// amendment flips it here on purpose.
//
// The contract that holds in every cell: a kill mid-transaction never
// persists a position past the transaction, never damages a row the
// transaction did not touch, and at the kill the target holds apply marks for
// at most ONE transaction (the invariant the restart sweep rests on).
//
// The kill is a context cancel with the target holding a row lock on a row a
// LATE statement touches, so the applier has committed everything before it
// and is blocked. No exit path persists a position mid-transaction — which
// the gate ASSERTS — so the target is left exactly as a SIGKILL at that
// moment would leave it.

// crashMidTxnSource is the transaction. Every shape the re-delivery can trip
// over precedes the blocking statement (id = 100), so it is in the applied
// prefix and replayed on top of itself: a row updated twice, an INSERT whose
// unique value a later INSERT of the same transaction reuses (900002 frees
// 'k', 900003 takes it), a primary-key change (a lane barrier), a three-step
// unique swap (5 frees 'x', 6 takes it and frees 'y', 5 takes 'y'), and three
// changes to a PK-only table (idempotent — they must write no mark); after
// it, an INSERT-then-DELETE of one key.
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
INSERT INTO po (id, v) VALUES (3, 'c');
UPDATE po SET v = 'b2' WHERE id = 2;
DELETE FROM po WHERE id = 1;
UPDATE rs SET v = 'blocked' WHERE id = 100;
INSERT INTO rs (id, u, v, pad) VALUES (900020, 'z', 'after', 'p');
DELETE FROM rs WHERE id = 900001;
UPDATE rs SET v = 'a3' WHERE id = 1;
`

// crashMidTxnKeylessSource is the keyless variant: two identical rows into a
// table with no key at all, then the blocked statement, then one more. Before
// ADR-0190 a restart duplicated the prefix rows (keyless CDC was at-least-once,
// ADR-0089 — silently); every apply path now marks keyless changes (the lane
// path through its barrier), so every cell converges exactly.
const crashMidTxnKeylessSource = `
INSERT INTO kl (k, v) VALUES (1, 'dup');
INSERT INTO kl (k, v) VALUES (1, 'dup');
UPDATE rs SET v = 'blocked' WHERE id = 100;
INSERT INTO kl (k, v) VALUES (2, 'after');
`

// crashMidTxnPKChangeSource is the primary-key-change variant, on the PK-only
// table: the INSERT of key 10 is an ordinary change (a lane change on the lane
// path, which writes no mark), the key change 10 → 11 is a barrier that marks
// both keys. The replay must SKIP the insert of 10 on the barrier's mark — a
// lane that applied it would re-create key 10 (an upsert), and the key change
// would then be skipped on its own mark, leaving 10 on the target the source
// no longer has. Before ADR-0190 the same replay collided on 11 (loud); every
// path now converges, the lane path through its barrier's marks and its
// lanes' CHECK of them.
const crashMidTxnPKChangeSource = `
INSERT INTO po (id, v) VALUES (10, 'x');
UPDATE po SET id = 11 WHERE id = 10;
UPDATE rs SET v = 'blocked' WHERE id = 100;
INSERT INTO po (id, v) VALUES (12, 'after');
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

// crashSource abstracts the source engines.
type crashSource struct {
	engine string
	dsn    string
	driver string
	// setup (re)creates rs, po and kl, seeded.
	setup string
	// exec runs statements outside a transaction; txn runs body as ONE
	// source transaction.
	exec func(t *testing.T, stmts string)
	txn  func(t *testing.T, body string)
	// configure names the stream's per-cell source resources (a Postgres
	// slot and publication; nothing on MySQL) and returns their teardown.
	configure func(t *testing.T, s *Streamer, suffix string) (teardown func())
}

func mysqlCrashSource(engine, dsn string) crashSource {
	return crashSource{
		engine: engine,
		dsn:    dsn,
		driver: "mysql",
		setup: `
			DROP TABLE IF EXISTS rs; DROP TABLE IF EXISTS po; DROP TABLE IF EXISTS kl;
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
			CREATE TABLE po (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(32) NOT NULL) ENGINE=InnoDB;
			INSERT INTO po (id, v) VALUES (1, 'a'), (2, 'b');
			CREATE TABLE kl (k INT NOT NULL, v VARCHAR(32) NOT NULL) ENGINE=InnoDB;
			INSERT INTO kl (k, v) VALUES (0, 'seed');`,
		exec: func(t *testing.T, stmts string) { applyDDLMySQL(t, dsn, stmts) },
		txn:  func(t *testing.T, body string) { runSourceTxnPlain(t, dsn, body) },
		configure: func(*testing.T, *Streamer, string) func() {
			return func() {}
		},
	}
}

func pgCrashSource(dsn string) crashSource {
	return crashSource{
		engine: "postgres",
		dsn:    dsn,
		driver: "pgx",
		setup: `
			DROP TABLE IF EXISTS rs, po, kl;
			CREATE TABLE rs (
				id  BIGINT PRIMARY KEY,
				u   VARCHAR(32) NULL CONSTRAINT rs_u UNIQUE,
				v   VARCHAR(32) NOT NULL,
				pad TEXT NOT NULL
			);
			INSERT INTO rs (id, u, v, pad) SELECT n, NULL, 'seed', 's' FROM generate_series(1, 200) AS n;
			UPDATE rs SET u = 'x' WHERE id = 5;
			UPDATE rs SET u = 'y' WHERE id = 6;
			CREATE TABLE po (id BIGINT PRIMARY KEY, v VARCHAR(32) NOT NULL);
			INSERT INTO po (id, v) VALUES (1, 'a'), (2, 'b');
			CREATE TABLE kl (k INT NOT NULL, v VARCHAR(32) NOT NULL);
			ALTER TABLE kl REPLICA IDENTITY FULL;
			INSERT INTO kl (k, v) VALUES (0, 'seed');`,
		exec: func(t *testing.T, stmts string) { pgExec(t, dsn, stmts) },
		txn:  func(t *testing.T, body string) { pgExec(t, dsn, "BEGIN;"+body+"COMMIT;") },
		configure: func(t *testing.T, s *Streamer, suffix string) func() {
			s.SlotName = "crash_" + suffix
			s.PublicationName = "pub_crash_" + suffix
			return func() {
				// The stream has stopped; release its slot so the next cell's
				// cold start is not refused on max_replication_slots.
				db, err := sql.Open("pgx", dsn)
				if err != nil {
					return
				}
				defer func() { _ = db.Close() }()
				_, _ = db.Exec(`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE $1`, "%crash_"+suffix)
			}
		},
	}
}

func TestStreamer_CrashMidTxn_MySQLGTID_ToPostgres(t *testing.T) {
	src, _, cleanup := startMySQLGTID(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	runCrashMidTxnSuite(t, mysqlCrashSource("mysql", src), pgResendTarget(pgDSN), crashPinsAll)
}

func TestStreamer_CrashMidTxn_MySQLGTID_ToMySQL(t *testing.T) {
	src, _, cleanup := startMySQLGTID(t)
	defer cleanup()
	_, tgt, tgtCleanup := startMySQL(t)
	defer tgtCleanup()
	runCrashMidTxnSuite(t, mysqlCrashSource("mysql", src), mysqlResendTarget(t, tgt), crashPinsRefusals|crashPinsKeyless)
}

func TestStreamer_CrashMidTxn_MariaDB_ToPostgres(t *testing.T) {
	src, cleanup := startMariaDBBinlog(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	runCrashMidTxnSuite(t, mysqlCrashSource("mariadb", src), pgResendTarget(pgDSN), 0)
}

func TestStreamer_CrashMidTxn_MySQLFilePos_ToPostgres(t *testing.T) {
	src, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	runCrashMidTxnSuite(t, mysqlCrashSource("mysql", src), pgResendTarget(pgDSN), 0)
}

func TestStreamer_CrashMidTxn_Postgres_ToPostgres(t *testing.T) {
	src, tgt, cleanup := startPostgresLogical(t)
	defer cleanup()
	runCrashMidTxnSuite(t, pgCrashSource(src), pgResendTarget(tgt), crashPinsKeyless)
}

func TestStreamer_CrashMidTxn_Postgres_ToMySQL(t *testing.T) {
	src, _, cleanup := startPostgresLogical(t)
	defer cleanup()
	_, tgt, tgtCleanup := startMySQL(t)
	defer tgtCleanup()
	runCrashMidTxnSuite(t, pgCrashSource(src), mysqlResendTarget(t, tgt), crashPinsRefusals|crashPinsKeyless)
}

// crashPins selects the optional pin groups a source × target pair runs: the
// main path matrix runs everywhere; the rest are engine-neutral decisions
// graded on enough pairs to reach both target engines and both source
// families without multiplying the suite.
type crashPins uint8

const (
	crashPinsKeyless   crashPins = 1 << iota // keyless exactly-once, every path
	crashPinsRefusals                        // tripwire, scope, unavailable
	crashPinsColdStart                       // a re-snapshot clears the marks
	crashPinsAll       = crashPinsKeyless | crashPinsRefusals | crashPinsColdStart
)

// crashMidTxnCell is one apply-path configuration and what it must do.
type crashMidTxnCell struct {
	name        string
	concurrency int
	batch       int
	// cohesive: the path holds THIS source transaction (it fits one batch) in
	// one target transaction with its position, so the kill leaves no prefix.
	cohesive bool
	// fixedCap turns the ADR-0052 batch-size controller off, so the batch
	// size is exactly the configured one.
	fixedCap bool
	// converges: the restart converges exactly (ADR-0190). False for the one
	// path that writes no marks — see the file comment.
	converges bool

	// body is the source transaction (crashMidTxnSource when empty).
	body string
	// tamper runs on the target between the kill and the restart.
	tamper func(t *testing.T, tgt resendTarget, streamID string)
	// restart adjusts the restarted stream.
	restart func(s *Streamer)
	// wantRefusal: the restart must stop with an error containing it
	// (instead of converging or colliding).
	wantRefusal string
	// wantLog: the run's log must contain it.
	wantLog string
}

func runCrashMidTxnSuite(t *testing.T, src crashSource, tgt resendTarget, pins crashPins) {
	cells := []crashMidTxnCell{
		{name: "serial_per_change", concurrency: 1, batch: 0, converges: true},
		{name: "serial_batched", concurrency: 1, batch: 1000, cohesive: true, converges: true},
		// The same path with the transaction LARGER than one batch: the row
		// cap flushes mid-transaction, and the marks ride each flush.
		{name: "serial_batched_over_cap", concurrency: 1, batch: 5, fixedCap: true, converges: true},
		{name: "lanes_per_change", concurrency: 0, batch: 0, converges: true},
		{name: "lanes_batched", concurrency: 0, batch: 1000},
		{name: "pkchange_serial_per_change", concurrency: 1, batch: 0, body: crashMidTxnPKChangeSource, converges: true},
		{name: "pkchange_lanes_batched", concurrency: 0, batch: 1000, body: crashMidTxnPKChangeSource, converges: true},
	}
	if pins&crashPinsKeyless != 0 {
		for _, c := range []crashMidTxnCell{
			{name: "keyless_serial_per_change", concurrency: 1, batch: 0},
			{name: "keyless_serial_batched", concurrency: 1, batch: 1000},
			{name: "keyless_lanes_batched", concurrency: 0, batch: 1000},
		} {
			c.body, c.converges = crashMidTxnKeylessSource, true
			cells = append(cells, c)
		}
	}
	if pins&crashPinsRefusals != 0 {
		cells = append(
			cells,
			// The tripwire: a mark naming the replayed change's ordinal with a
			// different digest refuses, never skips.
			crashMidTxnCell{
				name: "tripwire_digest_mismatch", concurrency: 1, batch: 0,
				tamper: func(t *testing.T, tgt resendTarget, streamID string) {
					execTarget(t, tgt, "UPDATE sluice_cdc_apply_marks SET change_digest = 'tampered' WHERE stream_id = ?", streamID)
				},
				wantRefusal: applymarks.MismatchMarker,
			},
			// A changed row-filter scope refuses rather than trusting a mark.
			crashMidTxnCell{
				name: "scope_change", concurrency: 1, batch: 0,
				tamper: func(t *testing.T, tgt resendTarget, streamID string) {
					execTarget(t, tgt, "UPDATE sluice_cdc_apply_marks SET scope_digest = 'another-where' WHERE stream_id = ?", streamID)
				},
				wantRefusal: "row-filter scope",
			},
			// A target whose mark table is gone and that sluice may not
			// create it on (--schema-already-applied) WARNs and behaves
			// exactly as before ADR-0190: the loud collision.
			crashMidTxnCell{
				name: "marks_unavailable", concurrency: 1, batch: 0,
				tamper: func(t *testing.T, tgt resendTarget, _ string) {
					execTarget(t, tgt, "DROP TABLE sluice_cdc_apply_marks")
				},
				restart: func(s *Streamer) { s.SchemaAlreadyApplied = true },
				wantLog: applymarks.UnavailableMarker,
			},
		)
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			runCrashMidTxnCell(t, src, tgt, cell)
		})
	}
	if pins&crashPinsColdStart != 0 {
		t.Run("cold_start_clears_marks", func(t *testing.T) {
			runCrashColdStartClearsMarks(t, src, tgt)
		})
	}
}

// crashStreamFixture is one cell's source + target + stream wiring.
type crashStreamFixture struct {
	streamID    string
	newStreamer func() *Streamer
	logBuf      *logcapture.Buffer
	teardown    func()
}

func newCrashStreamFixture(t *testing.T, src crashSource, tgt resendTarget, cell crashMidTxnCell) crashStreamFixture {
	t.Helper()
	src.exec(t, src.setup)
	tgt.drop(t)
	execTargetQuiet(tgt, "DROP TABLE IF EXISTS po")
	execTargetQuiet(tgt, "DROP TABLE IF EXISTS kl")

	logBuf := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	srcEng, ok := engines.Get(src.engine)
	if !ok {
		t.Fatalf("engine %q not registered", src.engine)
	}
	tgtEng, ok := engines.Get(tgt.engine)
	if !ok {
		t.Fatalf("engine %q not registered", tgt.engine)
	}
	suffix := fmt.Sprintf("%d_%d_%d", cell.concurrency, cell.batch, time.Now().UnixNano()%1e6)
	streamID := "crash-" + src.engine + "-" + suffix
	var teardown func()
	newStreamer := func() *Streamer {
		s := &Streamer{
			Source:           srcEng,
			Target:           tgtEng,
			SourceDSN:        src.dsn,
			TargetDSN:        tgt.dsn,
			StreamID:         streamID,
			ApplyConcurrency: cell.concurrency,
			ApplyBatchSize:   cell.batch,
			AutoTune:         cell.batch > 1 && !cell.fixedCap,
		}
		if td := src.configure(t, s, suffix); teardown == nil {
			teardown = td
		}
		return s
	}
	return crashStreamFixture{streamID: streamID, newStreamer: newStreamer, logBuf: logBuf, teardown: func() {
		if teardown != nil {
			teardown()
		}
	}}
}

// runCrashToKill runs the first process — cold copy, one CDC row, then the
// transaction — and kills it with the applier blocked mid-transaction. It
// returns whether a prefix reached the target and asserts the kill contract.
func runCrashToKill(t *testing.T, src crashSource, tgt resendTarget, fx crashStreamFixture, cell crashMidTxnCell, body string) (prefixApplied bool) {
	t.Helper()
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	run1 := make(chan error, 1)
	go func() { run1 <- fx.newStreamer().Run(ctx1) }()
	waitResend(t, run1, 120*time.Second, "the cold copy", func() bool { return len(tgt.dump(t)) >= 200 })
	// One CDC row per table BEFORE the transaction warms every metadata
	// cache on both sides (the reader's source schema load, the applier's
	// key and unique-index probes): a first-touch probe inside the
	// transaction would open a delivery gap the serial batch loop's 100 ms
	// idle flush turns into a mid-transaction commit, and the cohesive cell
	// would no longer measure cohesion.
	src.exec(t, `INSERT INTO po (id, v) VALUES (100, 'live')`)
	src.exec(t, `INSERT INTO kl (k, v) VALUES (100, 'live')`)
	src.exec(t, fmt.Sprintf(`INSERT INTO rs (id, u, v, pad) VALUES (%d, NULL, 'live', 'l')`, crashSentinelLive))
	waitResend(t, run1, 60*time.Second, "the first CDC rows", func() bool {
		return tgt.has(crashSentinelLive) && countTarget(tgt, "SELECT COUNT(*) FROM kl WHERE k = 100") == 1
	})
	// The lane path checkpoints on an idle tick, so the persisted position
	// may lag the live row briefly: wait for it to settle before the
	// transaction, or "unchanged across the kill" would compare a stale read.
	posBefore := settledPosition(t, tgt, fx.streamID)

	release := holdRowLock(t, map[string]string{"postgres": "pgx", "mysql": "mysql"}[tgt.engine], tgt.dsn,
		`SELECT id FROM rs WHERE id = 100 FOR UPDATE`)
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	src.txn(t, body)

	// A path that committed a prefix shows the transaction's first statement;
	// the cohesive path shows nothing until the commit it cannot reach.
	prefixApplied = waitResendSoft(run1, 30*time.Second, func() bool { return crashPrefixVisible(t, tgt, body) })
	time.Sleep(2 * time.Second) // let the applier reach the lock

	cancel1()
	select {
	case <-run1:
	case <-time.After(60 * time.Second):
		t.Fatal("the first stream did not return after cancel")
	}
	posAfterKill := tgt.readPos(fx.streamID)
	release()
	released = true

	if posAfterKill != posBefore {
		t.Errorf("the persisted position MOVED across a kill mid-transaction: %q before the transaction, %q after — "+
			"a position past a partly applied transaction skips its remainder on resume (silent loss)", posBefore, posAfterKill)
	}
	// The invariant the restart sweep rests on (applymarks sweepLocked): the
	// target holds apply marks for at most ONE transaction.
	marks := targetMarks(t, tgt, fx.streamID)
	if txs := distinctTxs(marks); len(txs) > 1 {
		t.Errorf("apply marks exist for %d transactions at the kill (%v); the design allows at most the one in flight", len(txs), txs)
	}
	t.Logf("apply marks at the kill: %d (%v)", len(marks), marksPerTable(marks))
	return prefixApplied
}

func runCrashMidTxnCell(t *testing.T, src crashSource, tgt resendTarget, cell crashMidTxnCell) {
	fx := newCrashStreamFixture(t, src, tgt, cell)
	defer fx.teardown()
	body := cell.body
	if body == "" {
		body = crashMidTxnSource
	}
	prefixApplied := runCrashToKill(t, src, tgt, fx, cell, body)
	if prefixApplied == cell.cohesive {
		t.Errorf("prefix committed before the kill = %v; want %v for this path (cohesive %v)", prefixApplied, !cell.cohesive, cell.cohesive)
	}

	marks := targetMarks(t, tgt, fx.streamID)
	perTable := marksPerTable(marks)
	if n := perTable["po"]; n != 0 && body == crashMidTxnSource {
		t.Errorf("the PK-only table po wrote %d apply marks; its changes are idempotent and must write none (operator decision 1)", n)
	}
	if prefixApplied && cell.converges && cell.tamper == nil && len(marks) == 0 {
		t.Errorf("a prefix of a non-idempotent transaction reached the target with no apply mark — the restart has nothing to skip on")
	}
	if cell.tamper != nil {
		if len(marks) == 0 {
			t.Fatal("nothing to tamper with: the prefix wrote no apply marks, so this cell would pass vacuously")
		}
		cell.tamper(t, tgt, fx.streamID)
	}

	// ---- Resume. ----
	caughtUp, restartErr := resumeCrashStreamWith(t, fx.newStreamer, cell.restart, src, tgt, true, fx.streamID, tgt.readPos(fx.streamID))
	logs := fx.logBuf.String()
	t.Logf("prefix before the kill %v; restart caught up %v, error %v", prefixApplied, caughtUp, restartErr)
	assertCrashDamageConfined(t, dumpCrashSource(t, src), tgt.dump(t))
	if cell.wantLog != "" && !strings.Contains(logs, cell.wantLog) {
		t.Errorf("the run's log does not contain %q", cell.wantLog)
	}

	switch {
	case cell.wantRefusal != "":
		if caughtUp || restartErr == nil || !strings.Contains(restartErr.Error(), cell.wantRefusal) {
			t.Fatalf("the restart must refuse with %q; caught up %v, error %v", cell.wantRefusal, caughtUp, restartErr)
		}
		if !strings.Contains(restartErr.Error(), applymarks.MismatchMarker) {
			t.Errorf("the refusal does not carry the %s marker: %v", applymarks.MismatchMarker, restartErr)
		}
	case cell.converges:
		if !caughtUp || restartErr != nil {
			t.Fatalf("the restart after a kill mid-transaction did not converge: caught up %v, error %v", caughtUp, restartErr)
		}
		assertCrashConverged(t, src, tgt)
		// GC: the position passed the crashed transaction (and the sentinel's),
		// so their marks are gone — deleted with the position that passed them.
		if left := targetMarks(t, tgt, fx.streamID); len(left) > 0 {
			t.Errorf("%d apply marks survived the position passing their transactions (garbage collection): %v", len(left), marksPerTable(left))
		}
	default:
		// KNOWN LOUD: the path writes no marks (lanes_batched — see the file
		// comment), or its marks are unavailable (marks_unavailable).
		if caughtUp || restartErr == nil {
			diff := diffResendRows(dumpCrashSource(t, src), tgt.dump(t))
			t.Fatalf("cell changed: the restart after a kill mid-transaction resumed (caught up %v, error %v). "+
				"If the lane-mark amendment landed, make this cell converge; otherwise find out why. Diff:\n%s", caughtUp, restartErr, diff)
		}
		if !isUniqueCollision(restartErr) {
			t.Errorf("the restart stopped, but not on the unique collision GC-38 (l) describes: %v", restartErr)
		}
		_, again := resumeCrashStreamWith(t, fx.newStreamer, cell.restart, src, tgt, false, "", "")
		if !isUniqueCollision(again) {
			t.Errorf("a SECOND restart did not stop on the same collision: %v", again)
		}
	}
}

// runCrashColdStartClearsMarks: every cold start deletes the stream's apply
// marks before its copy (ADR-0190 §5). The restart is a --restart-from-scratch
// with NO further source traffic, so no transaction close can delete the
// marks by garbage collection: only the cold start's clear can.
func runCrashColdStartClearsMarks(t *testing.T, src crashSource, tgt resendTarget) {
	cell := crashMidTxnCell{concurrency: 1, batch: 0}
	fx := newCrashStreamFixture(t, src, tgt, cell)
	defer fx.teardown()
	runCrashToKill(t, src, tgt, fx, cell, crashMidTxnSource)
	if len(targetMarks(t, tgt, fx.streamID)) == 0 {
		t.Fatal("the kill left no apply marks, so the clear cannot be graded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := make(chan error, 1)
	go func() {
		s := fx.newStreamer()
		s.RestartFromScratch = true
		run <- s.Run(ctx)
	}()
	want := len(dumpCrashSource(t, src))
	waitResend(t, run, 3*time.Minute, "the re-snapshot copy", func() bool { return len(tgt.dump(t)) == want })
	settledPosition(t, tgt, fx.streamID)
	cancel()
	select {
	case <-run:
	case <-time.After(60 * time.Second):
		t.Fatal("the re-snapshot stream did not return after cancel")
	}
	if left := targetMarks(t, tgt, fx.streamID); len(left) > 0 {
		t.Errorf("%d apply marks survived a --restart-from-scratch cold start: %v", len(left), marksPerTable(left))
	}
	if diff := diffResendRows(dumpCrashSource(t, src), tgt.dump(t)); diff != "" {
		t.Errorf("the re-snapshot diverged:\n%s", diff)
	}
}

// resumeCrashStreamWith runs one resumed attempt of the stream. When
// writeSentinel it first writes the post-restart sentinel, whose arrival is
// "caught up". A cancel after catching up is the clean stop, reported as a
// nil error. A non-empty streamID also waits, after catching up, for the
// persisted position to move past posAtKill.
func resumeCrashStreamWith(t *testing.T, newStreamer func() *Streamer, adjust func(*Streamer), src crashSource, tgt resendTarget, writeSentinel bool, streamID, posAtKill string) (bool, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	run := make(chan error, 1)
	go func() {
		s := newStreamer()
		if adjust != nil {
			adjust(s)
		}
		run <- s.Run(ctx)
	}()
	if writeSentinel {
		src.exec(t, fmt.Sprintf(`INSERT INTO rs (id, u, v, pad) VALUES (%d, NULL, 'sentinel', 's')`, crashSentinelPost))
	}
	caughtUp := waitResendSoft(run, 3*time.Minute, func() bool { return tgt.has(crashSentinelPost) })
	if caughtUp && streamID != "" {
		// Caught up is the DATA; the lane path persists the position on an
		// idle tick up to a second later. Stop only once the persisted
		// position has passed the crashed transaction, so what the cell
		// grades next (its marks' garbage collection) is the steady state
		// rather than a race with the checkpoint.
		waitResendSoft(run, 30*time.Second, func() bool {
			pos := tgt.readPos(streamID)
			return pos != "" && pos != posAtKill
		})
		settledPosition(t, tgt, streamID)
	}
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

// crashPrefixVisible reports whether the transaction's first statement
// reached the target.
func crashPrefixVisible(t *testing.T, tgt resendTarget, body string) bool {
	switch body {
	case crashMidTxnKeylessSource:
		return countTarget(tgt, "SELECT COUNT(*) FROM kl WHERE v = 'dup'") > 0
	case crashMidTxnPKChangeSource:
		return countTarget(tgt, "SELECT COUNT(*) FROM po WHERE id IN (10, 11)") > 0
	}
	return tgt.dump(t)[1].v == "a1"
}

// assertCrashConverged grades every table the transactions touch against the
// SOURCE's own final state — the independent expected value.
func assertCrashConverged(t *testing.T, src crashSource, tgt resendTarget) {
	t.Helper()
	if diff := diffResendRows(dumpCrashSource(t, src), tgt.dump(t)); diff != "" {
		t.Errorf("rs diverged after the restart:\n%s", diff)
	}
	for _, q := range []string{
		"SELECT CONCAT(id, '|', v) FROM po",
		"SELECT CONCAT(k, '|', v) FROM kl",
	} {
		want, got := sortedRows(t, src.driver, src.dsn, q), sortedRows(t, driverOf(tgt), tgt.dsn, q)
		if strings.Join(want, ",") != strings.Join(got, ",") {
			t.Errorf("%q diverged after the restart:\n  source %v\n  target %v", q, want, got)
		}
	}
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

// dumpCrashSource is the rs dump of either source engine.
func dumpCrashSource(t *testing.T, src crashSource) map[int64]resendRow {
	t.Helper()
	if src.driver == "pgx" {
		return dumpResend(t, "pgx", src.dsn, `SELECT id, COALESCE(u, '<null>'), v, length(pad), md5(pad) FROM rs`)
	}
	return dumpSourceResend(t, src.dsn)
}

// crashMark is the part of an apply-mark row the gate grades.
type crashMark struct{ table, tx string }

// targetMarks lists the stream's apply marks on the target (none when the
// table is absent).
func targetMarks(t *testing.T, tgt resendTarget, streamID string) []crashMark {
	t.Helper()
	db, err := sql.Open(driverOf(tgt), tgt.dsn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(bindFor(tgt, "SELECT table_name, tx_id FROM sluice_cdc_apply_marks WHERE stream_id = ?"), streamID)
	if err != nil {
		return nil
	}
	defer func() { _ = rows.Close() }()
	var out []crashMark
	for rows.Next() {
		var m crashMark
		if err := rows.Scan(&m.table, &m.tx); err != nil {
			t.Fatalf("scan marks: %v", err)
		}
		out = append(out, m)
	}
	return out
}

func distinctTxs(marks []crashMark) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range marks {
		if !seen[m.tx] {
			seen[m.tx] = true
			out = append(out, m.tx)
		}
	}
	return out
}

// marksPerTable counts marks by unqualified table name.
func marksPerTable(marks []crashMark) map[string]int {
	out := map[string]int{}
	for _, m := range marks {
		name := m.table
		if i := strings.LastIndex(name, "."); i >= 0 {
			name = name[i+1:]
		}
		out[name]++
	}
	return out
}

func driverOf(tgt resendTarget) string {
	if tgt.engine == "postgres" {
		return "pgx"
	}
	return "mysql"
}

// bindFor renders a single-`?` query in the target's placeholder style.
func bindFor(tgt resendTarget, q string) string {
	if tgt.engine == "postgres" {
		return strings.Replace(q, "?", "$1", 1)
	}
	return q
}

func execTarget(t *testing.T, tgt resendTarget, q string, args ...any) {
	t.Helper()
	db, err := sql.Open(driverOf(tgt), tgt.dsn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(bindFor(tgt, q), args...); err != nil {
		t.Fatalf("target exec %q: %v", q, err)
	}
}

func execTargetQuiet(tgt resendTarget, q string) {
	db, err := sql.Open(driverOf(tgt), tgt.dsn)
	if err != nil {
		return
	}
	defer func() { _ = db.Close() }()
	_, _ = db.Exec(q)
}

func countTarget(tgt resendTarget, q string) int {
	db, err := sql.Open(driverOf(tgt), tgt.dsn)
	if err != nil {
		return 0
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow(q).Scan(&n); err != nil {
		return 0
	}
	return n
}

// sortedRows renders q's single text column, sorted — a multiset comparison
// that works for a keyless table.
func sortedRows(t *testing.T, driver, dsn, q string) []string {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
