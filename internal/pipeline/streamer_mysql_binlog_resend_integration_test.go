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
	"sync"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/logcapture"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// The binlog re-send (perf-parity gap 37, MySQL arm), end to end.
//
// A consumer held longer than the source's net_write_timeout — the
// forwarded ADD COLUMN backfill holds it for a whole pass over a table, and
// so does any slow target — lets every buffer between the dump thread and
// the reader fill. The server then drops the dump thread, and go-mysql
// re-dials inside its syncer, invisibly to sluice: in GTID mode from the
// executed set BEFORE the in-flight transaction (so that transaction is
// re-sent from its first event), in file/pos mode from the end of the last
// event it received. These cells hold the consumer with a row lock on the
// target while the source commits one large transaction that mixes every
// DML shape, release it, and grade the target against the source row by
// row — the independent expected value is a SELECT on the source.
//
// A cell is VACUOUS unless the drop actually happened; the anti-vacuity
// evidence is go-mysql's own "begin to re-sync" log line.

// resendRow is one row as both engines render it for comparison.
type resendRow struct {
	u, v   string
	padLen int
	padMD5 string
}

// resendTarget abstracts the two target engines.
type resendTarget struct {
	engine string
	dsn    string
	// lockRow holds a row lock on rs.id = 1 until release is called.
	lockRow func(t *testing.T) (release func())
	dump    func(t *testing.T) map[int64]resendRow
	// has is the cheap catch-up probe: a point lookup. Polling the full
	// dump instead read ~90 MB every 300ms and starved a MySQL target's
	// own commits (measured: the applier parked in COMMIT, ~5 rows/s).
	has     func(id int64) bool
	readPos func(streamID string) string
	drop    func(t *testing.T)
}

// resendSourceTxn is the held transaction. The consumer blocks on its
// first statement (the target row lock on id 1), so every shape before the
// bulk insert is in the prefix the first delivery applies, and the rest is
// only in the re-send.
const resendSourceTxn = `
UPDATE rs SET v = 'a1' WHERE id = 1;
DELETE FROM rs WHERE id = 2;
INSERT INTO rs (id, u, v, pad) VALUES (900001, NULL, 'tmp', 't');
INSERT INTO rs (id, u, v, pad) VALUES (900002, 'k', 'x', 'p');
UPDATE rs SET u = 'k2' WHERE id = 900002;
INSERT INTO rs (id, u, v, pad) VALUES (900003, 'k', 'y', 'p');
INSERT INTO rs (id, u, v, pad) VALUES (900010, NULL, 'pk', 'p');
UPDATE rs SET id = 900011 WHERE id = 900010;
UPDATE rs SET v = 'a2' WHERE id = 1;
INSERT INTO rs (id, u, v, pad)
  WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 25000)
  SELECT 1000000 + n, NULL, 'bulk', REPEAT('x', 5000) FROM g;
DELETE FROM rs WHERE id = 900001;
UPDATE rs SET v = 'late' WHERE id BETWEEN 10 AND 150;
DELETE FROM rs WHERE id = 3;
UPDATE rs SET v = 'a3' WHERE id = 1;
`

func TestStreamer_MySQLBinlogResend_GTID_MySQLToPostgres(t *testing.T) {
	src, _, cleanup := startMySQLGTID(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	runResendCells(t, src, "gtid", pgResendTarget(pgDSN))
}

// The MySQL targets get a server of their own: on the source's server every
// applied row also lands in the binlog the reader tails, and the per-row
// apply of the bulk insert ran at ~20 rows/s (measured) — too slow to catch
// up inside any sane budget, and it re-held the consumer into repeated drops.
func TestStreamer_MySQLBinlogResend_GTID_MySQLToMySQL(t *testing.T) {
	src, _, cleanup := startMySQLGTID(t)
	defer cleanup()
	_, tgt, tgtCleanup := startMySQL(t)
	defer tgtCleanup()
	runResendCells(t, src, "gtid", mysqlResendTarget(t, tgt))
}

func TestStreamer_MySQLBinlogResend_FilePos_MySQLToPostgres(t *testing.T) {
	src, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	_, pgDSN, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	runResendCells(t, src, "file_pos", pgResendTarget(pgDSN))
}

// There is deliberately no FilePos_MySQLToMySQL cell. File/pos re-sends
// nothing, so the guard is inert there and the file/pos arm is fully graded
// by the Postgres target above; on the Windows Docker rig the MySQL-target
// catch-up in file/pos mode ran at ~22 changes/s regardless of lane count
// and could not finish inside any sane budget (measured 2026-09-24: 19.4k of
// 25.2k rows in 15 minutes, no error, no repeated row). That throughput is
// filed separately (audit backlog GC-38 (m)); it is not this gate's subject.

func runResendCells(t *testing.T, srcDSN, wantMode string, tgt resendTarget) {
	for _, cell := range []struct {
		name        string
		concurrency int
	}{
		{"serial", 1},
		{"lanes", 0},
	} {
		t.Run(cell.name, func(t *testing.T) {
			runResendCell(t, srcDSN, wantMode, tgt, cell.concurrency)
		})
	}
}

func runResendCell(t *testing.T, srcDSN, wantMode string, tgt resendTarget, concurrency int) {
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
		SET SESSION cte_max_recursion_depth = 1000;
		INSERT INTO rs (id, u, v, pad)
		  WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM g WHERE n < 200)
		  SELECT n, NULL, 'seed', 's' FROM g;
	`)
	// The dump thread inherits the global at connect, so this must precede
	// the stream.
	applyDDLMySQL(t, srcDSN, `SET GLOBAL net_write_timeout = 2`)
	defer applyDDLMySQL(t, srcDSN, `SET GLOBAL net_write_timeout = 60`)

	logBuf := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)

	srcEng, _ := engines.Get("mysql")
	tgtEng, ok := engines.Get(tgt.engine)
	if !ok {
		t.Fatalf("engine %q not registered", tgt.engine)
	}
	streamID := fmt.Sprintf("resend-%s-%d-%d", wantMode, concurrency, time.Now().UnixNano()%1e6)
	s := &Streamer{
		Source:           srcEng,
		Target:           tgtEng,
		SourceDSN:        srcDSN,
		TargetDSN:        tgt.dsn,
		StreamID:         streamID,
		ApplyConcurrency: concurrency,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()

	waitResend(t, runErr, 120*time.Second, "the cold copy", func() bool { return len(tgt.dump(t)) >= 200 })
	applyDDLMySQL(t, srcDSN, `INSERT INTO rs (id, u, v, pad) VALUES (4000000, NULL, 'live', 'l')`)
	waitResend(t, runErr, 60*time.Second, "the first CDC row", func() bool { return tgt.has(4000000) })

	// Sample the persisted position for the rest of the run.
	var (
		posMu   sync.Mutex
		posSeen []string
	)
	sampleCtx, stopSampling := context.WithCancel(context.Background())
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for sampleCtx.Err() == nil {
			if p := tgt.readPos(streamID); p != "" {
				posMu.Lock()
				if len(posSeen) == 0 || posSeen[len(posSeen)-1] != p {
					posSeen = append(posSeen, p)
				}
				posMu.Unlock()
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	release := tgt.lockRow(t)
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	runSourceTxnMySQL(t, srcDSN, resendSourceTxn)

	// Hold until the server has dropped the dump thread. go-mysql cannot
	// notice (or re-dial) while its pump is blocked handing an event to the
	// full cache, so the re-dial — and its "begin to re-sync" line — only
	// follows the release; that line is graded after catch-up below.
	deadline := time.Now().Add(120 * time.Second)
	dropped := false
	for !dropped && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		dropped = len(binlogDumpThreadIDsPipeline(t, srcDSN)) == 0
	}
	// One drop is the measurement. Restore the timeout before releasing, so
	// the re-dial (which inherits the global at connect) and the catch-up
	// run at the default: left at 2s, a slow target re-held the consumer
	// past it again and again, and each re-dial re-sent the whole 125 MB
	// transaction from its first event (measured on a MySQL target: ~12
	// rows/s).
	applyDDLMySQL(t, srcDSN, `SET GLOBAL net_write_timeout = 60`)
	time.Sleep(time.Second)
	release()
	released = true
	if !dropped {
		t.Fatalf("VACUOUS: the server never dropped the dump thread across a 120s hold; the cell measured nothing")
	}

	applyDDLMySQL(t, srcDSN, `INSERT INTO rs (id, u, v, pad) VALUES (4000001, NULL, 'sentinel', 's')`)
	caughtUp := waitResendSoft(runErr, 15*time.Minute, func() bool { return tgt.has(4000001) })
	if !strings.Contains(logBuf.String(), "begin to re-sync") {
		t.Errorf("VACUOUS: the dump thread was dropped but go-mysql never re-dialled (no \"begin to re-sync\")")
	}

	stopSampling()
	<-sampled
	cancel()
	var streamErr error
	select {
	case streamErr = <-runErr:
	case <-time.After(60 * time.Second):
		t.Fatal("Streamer.Run did not return after cancel")
	}

	logs := logBuf.String()
	t.Logf("re-syncs: %d; stream returned: %v; caught up: %v", strings.Count(logs, "begin to re-sync"), streamErr, caughtUp)
	// GTID mode re-sends the newest transaction on a re-dial, so the reader
	// must have dropped it; file/pos resumes after the last event received,
	// so there is nothing to drop and the guard must not fire.
	const dedupe = "dropped the events this stream had already delivered"
	switch deduped := strings.Contains(logs, dedupe); {
	case wantMode == "gtid" && !deduped:
		t.Errorf("GTID mode: go-mysql re-dialled but the reader never dropped a re-sent transaction (no %q)", dedupe)
	case wantMode != "gtid" && deduped:
		t.Errorf("%s mode: the reader dropped events as re-sent where go-mysql re-sends nothing: %s", wantMode, firstLineContaining(logs, dedupe))
	case deduped:
		t.Logf("reader: %s", firstLineContaining(logs, dedupe))
	}
	for _, marker := range []string{"retrying", "duplicate", "Duplicate", "unique", "error"} {
		if n := strings.Count(logs, marker); n > 0 {
			t.Logf("log lines mentioning %q: %d (first: %s)", marker, n, firstLineContaining(logs, marker))
		}
	}

	want := dumpSourceResend(t, srcDSN)
	got := tgt.dump(t)
	if diff := diffResendRows(want, got); diff != "" {
		t.Errorf("the target differs from the source after a binlog re-send (mode %s, concurrency %d):\n%s", wantMode, concurrency, diff)
	}
	if !caughtUp {
		t.Errorf("the stream never delivered the post-transaction sentinel (stream error: %v)", streamErr)
	}

	posMu.Lock()
	seen := append([]string(nil), posSeen...)
	posMu.Unlock()
	assertResendPositionsMonotonic(t, srcDSN, wantMode, seen)
}

func waitResend(t *testing.T, runErr chan error, d time.Duration, what string, done func() bool) {
	t.Helper()
	if !waitResendSoft(runErr, d, done) {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// waitResendSoft polls done until it holds or d passes; a stream that
// returns early ends the wait (and is re-queued for the caller to read).
func waitResendSoft(runErr chan error, d time.Duration, done func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if done() {
			return true
		}
		select {
		case err := <-runErr:
			runErr <- err
			return done()
		case <-time.After(300 * time.Millisecond):
		}
	}
	return done()
}

func firstLineContaining(logs, s string) string {
	for _, l := range strings.Split(logs, "\n") {
		if strings.Contains(l, s) {
			if len(l) > 400 {
				return l[:400] + "…"
			}
			return l
		}
	}
	return ""
}

func runSourceTxnMySQL(t *testing.T, dsn, body string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn+"&multiStatements=true")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := db.ExecContext(ctx, "SET SESSION cte_max_recursion_depth = 100000; START TRANSACTION;"+body+"COMMIT;"); err != nil {
		t.Fatalf("source transaction: %v", err)
	}
}

func dumpSourceResend(t *testing.T, dsn string) map[int64]resendRow {
	t.Helper()
	return dumpResend(t, "mysql", dsn, "SELECT id, COALESCE(u, '<null>'), v, LENGTH(pad), MD5(pad) FROM rs")
}

func dumpResend(t *testing.T, driver, dsn, q string) map[int64]resendRow {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return map[int64]resendRow{}
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]resendRow{}
	for rows.Next() {
		var (
			id int64
			r  resendRow
		)
		if err := rows.Scan(&id, &r.u, &r.v, &r.padLen, &r.padMD5); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[id] = r
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

func diffResendRows(want, got map[int64]resendRow) string {
	var missing, extra, differ []int64
	for id, w := range want {
		g, ok := got[id]
		switch {
		case !ok:
			missing = append(missing, id)
		case g != w:
			differ = append(differ, id)
		}
	}
	for id := range got {
		if _, ok := want[id]; !ok {
			extra = append(extra, id)
		}
	}
	if len(missing)+len(extra)+len(differ) == 0 {
		return ""
	}
	for _, s := range [][]int64{missing, extra, differ} {
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	}
	var b strings.Builder
	fmt.Fprintf(&b, "  source rows %d, target rows %d; missing %d, extra %d, differing %d\n", len(want), len(got), len(missing), len(extra), len(differ))
	head := func(s []int64) []int64 {
		if len(s) > 10 {
			return s[:10]
		}
		return s
	}
	fmt.Fprintf(&b, "  missing (first 10): %v\n  extra (first 10): %v\n", head(missing), head(extra))
	for _, id := range head(differ) {
		fmt.Fprintf(&b, "  id %d: source %+v, target %+v\n", id, want[id], got[id])
	}
	return b.String()
}

// assertResendPositionsMonotonic grades the persisted positions sampled
// during the run: a re-delivery must never move the checkpoint backwards
// (GTID mode: each sample ⊆ the next, as the source server computes it;
// file/pos: (file, pos) non-decreasing), and the last one must not claim
// more than the source executed.
func assertResendPositionsMonotonic(t *testing.T, srcDSN, wantMode string, seen []string) {
	t.Helper()
	if len(seen) == 0 {
		t.Error("no persisted position was ever sampled")
		return
	}
	toks := make([]persistedBinlogToken, len(seen))
	for i, s := range seen {
		toks[i] = decodePersistedMySQLToken(t, s)
		if toks[i].Mode != wantMode {
			t.Errorf("persisted position %d is %q mode, want %q: %s", i, toks[i].Mode, wantMode, s)
			return
		}
	}
	t.Logf("distinct persisted positions sampled: %d", len(toks))
	for i := 1; i < len(toks); i++ {
		a, b := toks[i-1], toks[i]
		switch wantMode {
		case "gtid":
			if !gtidSubsetMySQL(t, srcDSN, a.GTIDSet, b.GTIDSet) {
				t.Errorf("the persisted position moved BACKWARDS: %q then %q", a.GTIDSet, b.GTIDSet)
			}
		default:
			if b.File < a.File || (b.File == a.File && b.Pos < a.Pos) {
				t.Errorf("the persisted position moved BACKWARDS: %s:%d then %s:%d", a.File, a.Pos, b.File, b.Pos)
			}
		}
	}
	if wantMode == "gtid" {
		last := toks[len(toks)-1].GTIDSet
		if executed := globalVarMySQL(t, srcDSN, "gtid_executed"); !gtidSubsetMySQL(t, srcDSN, last, executed) {
			t.Errorf("the persisted position %q claims more than the source executed (%q)", last, executed)
		}
	}
}

func pgResendTarget(dsn string) resendTarget {
	return resendTarget{
		engine: "postgres",
		dsn:    dsn,
		lockRow: func(t *testing.T) func() {
			return holdRowLock(t, "pgx", dsn, `SELECT id FROM rs WHERE id = 1 FOR UPDATE`)
		},
		dump: func(t *testing.T) map[int64]resendRow {
			return dumpResend(t, "pgx", dsn, `SELECT id, COALESCE(u, '<null>'), v, length(pad), md5(pad) FROM rs`)
		},
		has: func(id int64) bool {
			return readPosQuiet("pgx", dsn, `SELECT id::text FROM rs WHERE id = $1`, id) != ""
		},
		readPos: func(streamID string) string {
			return readPosQuiet("pgx", dsn, `SELECT source_position FROM sluice_cdc_state WHERE stream_id = $1`, streamID)
		},
		drop: func(t *testing.T) { pgExec(t, dsn, `DROP TABLE IF EXISTS rs`) },
	}
}

func mysqlResendTarget(t *testing.T, dsn string) resendTarget {
	// The held row lock outlasts InnoDB's default 50s wait; raise it so the
	// applier waits rather than failing the attempt (a retry would confound
	// the measurement). Global: the applier's sessions open after this.
	applyDDLMySQL(t, dsn, `SET GLOBAL innodb_lock_wait_timeout = 600`)
	// The cell grades rows, not crash durability; per-row commits at full
	// durability are the slowest part of catching up.
	applyDDLMySQL(t, dsn, `SET GLOBAL innodb_flush_log_at_trx_commit = 2; SET GLOBAL sync_binlog = 0`)
	return resendTarget{
		engine: "mysql",
		dsn:    dsn,
		lockRow: func(t *testing.T) func() {
			return holdRowLock(t, "mysql", dsn, `SELECT id FROM rs WHERE id = 1 FOR UPDATE`)
		},
		dump: func(t *testing.T) map[int64]resendRow {
			return dumpResend(t, "mysql", dsn, "SELECT id, COALESCE(u, '<null>'), v, LENGTH(pad), MD5(pad) FROM rs")
		},
		has: func(id int64) bool {
			return readPosQuiet("mysql", dsn, `SELECT CAST(id AS CHAR) FROM rs WHERE id = ?`, id) != ""
		},
		readPos: func(streamID string) string {
			return readPosQuiet("mysql", dsn, `SELECT source_position FROM sluice_cdc_state WHERE stream_id = ?`, streamID)
		},
		drop: func(t *testing.T) { applyDDLMySQL(t, dsn, `DROP TABLE IF EXISTS rs`) },
	}
}

// holdRowLock opens a transaction holding the lock q takes and returns
// its release.
func holdRowLock(t *testing.T, driver, dsn, q string) func() {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	var id int64
	if err := tx.QueryRowContext(ctx, q).Scan(&id); err != nil {
		t.Fatalf("take the row lock: %v", err)
	}
	return func() {
		_ = tx.Rollback()
		_ = db.Close()
	}
}

func readPosQuiet(driver, dsn, q string, arg any) string {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return ""
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var tok string
	if err := db.QueryRowContext(ctx, q, arg).Scan(&tok); err != nil {
		return ""
	}
	return tok
}

// binlogDumpThreadIDsPipeline lists the server's binlog dump threads.
func binlogDumpThreadIDsPipeline(t *testing.T, dsn string) []int64 {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	rows, err := db.Query(`SELECT ID FROM information_schema.PROCESSLIST WHERE COMMAND LIKE 'Binlog Dump%'`)
	if err != nil {
		t.Fatalf("processlist: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("processlist rows: %v", err)
	}
	return ids
}
