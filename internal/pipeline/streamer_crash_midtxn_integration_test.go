//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	"sluicesync.dev/sluice/internal/ir"
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
// every path — serial per-change, serial batched, lanes per-change and lanes
// batched — the same crash now CONVERGES: target == source, the position
// advances, no refusal, graded against the source's own final table state
// (the independent expected value).
//
// EXCEPT, by default, the lane BATCH path's secondary-unique changes
// (amendment C, operator 2026-09-29): the lanes write marks only with
// --exactly-once-lanes, because the fence below measured ~99.9% slower on a
// workload made of them. Without it lanes_batched stops on the collision
// again, as before ADR-0190 — pinned as the EXPECTED default, not a failure —
// while its barrier changes still mark, and the _exactly_once cells converge.
//
// The lane BATCH path writes its marks under ADR-0190 amendment A: before a
// transaction's first marked change reaches a lane, the coordinator drains
// the lanes and checkpoints, so a lane never commits a transaction's marks
// alongside an earlier, un-checkpointed transaction's changes. Without it, T1
// updating a key and T2 deleting it in one lane batch, killed before the
// checkpoint passed T1, replays T1's update (recreating the key) and skips
// T2's delete on its own mark — silent. lane_post_commit_window builds that
// window on purpose: it holds the stream's position row locked, so every
// checkpoint blocks while the lanes keep committing.
//
// The contract that holds in every cell: a kill mid-transaction never MOVES
// the persisted position (past the transaction or behind an earlier one),
// never damages a row the transaction did not touch, and at the kill every
// apply mark names the interrupted transaction — the first a restart
// re-delivers (amendment B; derived from the source where cheap).
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

	// ddlBefore (run before the cold copy, may be empty) and ddlMid (run
	// between two transactions) are a source-dialect DDL on po, so po's next
	// row arrives inside a transaction with a SchemaSnapshot stamped at the
	// DDL's own position. dropBefore / dropMid are the DROP COLUMN variant,
	// empty where it is not run.
	ddlBefore, ddlMid   string
	dropBefore, dropMid string
	// lastTxID derives the ADR-0190 identity of the source's most recently
	// committed transaction from the source's OWN bookkeeping (its GTID
	// state, its binlog, its change log) — the independent expected value for
	// "the marks name the interrupted transaction". nil where there is no
	// cheap derivation (Postgres: the commit LSN is not queryable after the
	// fact).
	lastTxID func(t *testing.T) string

	// streamDSN, when set, is the DSN the Streamer opens (a VStream source's
	// DSN carries gRPC parameters the plain SQL driver would try to SET);
	// dsn stays the one the harness's own SQL uses.
	streamDSN string
	// afterSetup runs after setup on every fixture (installing capture
	// triggers, waiting for a schema tracker).
	afterSetup func(t *testing.T)

	// markerless marks a source that emits no transaction boundaries (the
	// trigger sources, whose every change is its own ADR-0190 transaction).
	// Its persisted position legitimately ADVANCES inside a source
	// transaction — every flush persists it with the data — so the kill
	// contract is "never backwards" (positionOrdinal) rather than
	// "unchanged", and the transaction the marks may name is the first
	// change after the persisted position (firstTxAfter, from the source's
	// change log).
	markerless      bool
	positionOrdinal func(t *testing.T, token string) int64
	firstTxAfter    func(t *testing.T, token string) string
	// blockedOrdinal is the position ordinal (change-log id) of the
	// transaction's blocking statement — `UPDATE rs SET v = 'blocked' WHERE
	// id = 100` — read from the source's own change log: the first change the
	// kill leaves unapplied. Marker-less sources only.
	blockedOrdinal func(t *testing.T) int64
	// noKeyless: the source cannot capture a keyless table (postgres-trigger
	// refuses one loudly by design), so kl is neither warmed nor graded.
	noKeyless bool
}

// streamerDSN is the DSN the stream under test opens.
func (s crashSource) streamerDSN() string {
	if s.streamDSN != "" {
		return s.streamDSN
	}
	return s.dsn
}

func mysqlCrashSource(engine, dsn string) crashSource {
	return crashSource{
		engine: engine,
		dsn:    dsn,
		driver: "mysql",
		setup: `
			DROP TABLE IF EXISTS rs; DROP TABLE IF EXISTS po; DROP TABLE IF EXISTS ru; DROP TABLE IF EXISTS rv; DROP TABLE IF EXISTS kl;
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
			INSERT INTO po (id, v) VALUES (1, 'a'), (2, 'b'), ` + crashFoldPOSeed + `;
			CREATE TABLE ru (id BIGINT NOT NULL PRIMARY KEY, u VARCHAR(32) NULL, v VARCHAR(32) NOT NULL, UNIQUE KEY ru_u (u)) ENGINE=InnoDB;
			CREATE TABLE rv (id BIGINT NOT NULL PRIMARY KEY, u VARCHAR(32) NULL, v VARCHAR(32) NOT NULL, UNIQUE KEY rv_u (u)) ENGINE=InnoDB;
			INSERT INTO ru (id, v) VALUES (9, 'seed');
			INSERT INTO rv (id, v) VALUES (9, 'seed');
			CREATE TABLE kl (k INT NOT NULL, v VARCHAR(32) NOT NULL) ENGINE=InnoDB;
			INSERT INTO kl (k, v) VALUES (0, 'seed');`,
		exec: func(t *testing.T, stmts string) { applyDDLMySQL(t, dsn, stmts) },
		txn:  func(t *testing.T, body string) { runSourceTxnPlain(t, dsn, body) },
		configure: func(*testing.T, *Streamer, string) func() {
			return func() {}
		},
		ddlBefore: `ALTER TABLE po ADD COLUMN extra INT NULL`,
		ddlMid:    `ALTER TABLE po DROP COLUMN extra`,
		lastTxID:  func(t *testing.T) string { return mysqlLastTxID(t, engine, dsn) },
	}
}

func pgCrashSource(dsn string) crashSource {
	return crashSource{
		engine: "postgres",
		dsn:    dsn,
		driver: "pgx",
		setup: `
			DROP TABLE IF EXISTS rs, po, ru, rv, kl;
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
			INSERT INTO po (id, v) VALUES (1, 'a'), (2, 'b'), ` + crashFoldPOSeed + `;` + pgCrashFoldTables + `
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
		ddlMid:     `ALTER TABLE po ALTER COLUMN v TYPE VARCHAR(64)`,
		dropBefore: `ALTER TABLE po ADD COLUMN extra INT NULL`,
		dropMid:    `ALTER TABLE po DROP COLUMN extra`,
	}
}

// mysqlLastTxID derives the ADR-0190 transaction identity the binlog reader
// gives the source's most recently committed transaction, from the source's
// own state: MariaDB's @@gtid_binlog_pos; MySQL GTID mode's last GNO of
// @@server_uuid in @@gtid_executed; MySQL file/pos mode's server_uuid, the
// current binlog file and the end offset of its last BEGIN (the opening
// event the reader names the transaction by).
func mysqlLastTxID(t *testing.T, engine, dsn string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer func() { _ = db.Close() }()
	if engine == "mariadb" {
		var p string
		if err := db.QueryRow(`SELECT @@gtid_binlog_pos`).Scan(&p); err != nil || strings.Contains(p, ",") {
			t.Fatalf("@@gtid_binlog_pos = %q (err %v); want one domain's last GTID", p, err)
		}
		return p
	}
	var uuid, mode string
	if err := db.QueryRow(`SELECT @@server_uuid, @@gtid_mode`).Scan(&uuid, &mode); err != nil {
		t.Fatalf("read server_uuid / gtid_mode: %v", err)
	}
	if mode == "ON" {
		var executed string
		if err := db.QueryRow(`SELECT @@global.gtid_executed`).Scan(&executed); err != nil {
			t.Fatalf("read gtid_executed: %v", err)
		}
		for _, set := range strings.Split(strings.ReplaceAll(executed, "\n", ""), ",") {
			set = strings.TrimSpace(set)
			if !strings.HasPrefix(set, uuid+":") {
				continue
			}
			intervals := strings.Split(strings.TrimPrefix(set, uuid+":"), ":")
			last := intervals[len(intervals)-1]
			return uuid + ":" + last[strings.LastIndex(last, "-")+1:]
		}
		t.Fatalf("gtid_executed %q holds no transaction of %s", executed, uuid)
	}
	logs := queryStringRows(t, db, `SHOW BINARY LOGS`)
	file := logs[len(logs)-1]["Log_name"]
	var begin string
	for _, ev := range queryStringRows(t, db, "SHOW BINLOG EVENTS IN '"+file+"'") {
		if ev["Event_type"] == "Query" && ev["Info"] == "BEGIN" {
			begin = ev["End_log_pos"]
		}
	}
	if begin == "" {
		t.Fatalf("no BEGIN in %s", file)
	}
	return fmt.Sprintf("filepos:%s:%s:%s", uuid, file, begin)
}

// queryStringRows runs q and returns every row as column → text.
func queryStringRows(t *testing.T, db *sql.DB, q string) []map[string]string {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	var out []map[string]string
	for rows.Next() {
		vals := make([]sql.RawBytes, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		row := make(map[string]string, len(cols))
		for i, c := range cols {
			row[c] = string(vals[i])
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return out
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

// The postgres-trigger source (ADR-0190 phase 5). No refusal, keyless or
// cold-start group: the serial paths write no marks on a marker-less source
// (its position is persisted with every flush), so there is nothing to
// tamper with, and postgres-trigger refuses a keyless table by design.
func TestStreamer_CrashMidTxn_PgTrigger_ToPostgres(t *testing.T) {
	src, _, cleanup := startPostgresLogical(t)
	defer cleanup()
	// The target is a SEPARATE instance: the kill holds a target row lock in
	// an open transaction, and on a shared instance that transaction holds
	// back the snapshot xmin postgres-trigger's gap-free poll waits on, so
	// nothing would be delivered before the kill.
	_, tgt, tgtCleanup := startPostgresLogical(t)
	defer tgtCleanup()
	runCrashMidTxnSuite(t, pgTriggerCrashSource(src), pgResendTarget(tgt), 0)
}

func TestStreamer_CrashMidTxn_PgTrigger_ToMySQL(t *testing.T) {
	src, _, cleanup := startPostgresLogical(t)
	defer cleanup()
	_, tgt, tgtCleanup := startMySQL(t)
	defer tgtCleanup()
	runCrashMidTxnSuite(t, pgTriggerCrashSource(src), mysqlResendTarget(t, tgt), 0)
}

// pgTriggerCrashSource is a postgres-trigger source over the same tables
// (kl excepted: a keyless table is refused by the capture trigger), with the
// capture triggers installed on every fixture. Every change is its own
// ADR-0190 transaction, named by its change-log id, so the source-derived
// expected identities come from sluice_change_log itself.
func pgTriggerCrashSource(dsn string) crashSource {
	src := pgCrashSource(dsn)
	src.engine = pgtrigger.EngineName
	src.setup = `
		DROP TABLE IF EXISTS rs, po, ru, rv, kl;
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
		INSERT INTO po (id, v) VALUES (1, 'a'), (2, 'b'), ` + crashFoldPOSeed + `;` + pgCrashFoldTables
	src.configure = func(*testing.T, *Streamer, string) func() { return func() {} }
	src.ddlMid, src.dropBefore, src.dropMid = "", "", ""
	src.noKeyless = true
	src.afterSetup = func(t *testing.T) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if _, err := pgtrigger.Setup(ctx, dsn, pgtrigger.SetupOptions{Tables: []string{"rs", "po", "ru", "rv"}, Schema: "public"}); err != nil {
			t.Fatalf("pgtrigger.Setup: %v", err)
		}
	}
	changeLogID := func(t *testing.T, q string, args ...any) string {
		t.Helper()
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open source: %v", err)
		}
		defer func() { _ = db.Close() }()
		// The identity is `<engine>:<id>:<txid>` (triggercdc.ChangeApplyID):
		// read both from the change log itself, not from the reader.
		var id, txid sql.NullInt64
		if err := db.QueryRow(q, args...).Scan(&id, &txid); err != nil && !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("%s: %v", q, err)
		}
		if !id.Valid {
			return ""
		}
		return fmt.Sprintf("%s:%d:%d", pgtrigger.EngineName, id.Int64, txid.Int64)
	}
	const logRow = `SELECT id, txid FROM public.` + pgtrigger.ChangeLogTable
	src.markerless = true
	src.positionOrdinal = func(t *testing.T, token string) int64 {
		t.Helper()
		var p struct {
			LastID int64 `json:"last_id"`
		}
		if err := json.Unmarshal([]byte(token), &p); err != nil {
			t.Fatalf("decode trigger position %q: %v", token, err)
		}
		return p.LastID
	}
	src.firstTxAfter = func(t *testing.T, token string) string {
		return changeLogID(t, logRow+` WHERE id > $1 ORDER BY id LIMIT 1`, src.positionOrdinal(t, token))
	}
	src.lastTxID = func(t *testing.T) string {
		return changeLogID(t, logRow+` ORDER BY id DESC LIMIT 1`)
	}
	src.blockedOrdinal = func(t *testing.T) int64 {
		t.Helper()
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open source: %v", err)
		}
		defer func() { _ = db.Close() }()
		var id int64
		q := `SELECT id FROM public.` + pgtrigger.ChangeLogTable +
			` WHERE table_name = 'rs' AND after_jsonb->>'v' = 'blocked' ORDER BY id DESC LIMIT 1`
		if err := db.QueryRow(q).Scan(&id); err != nil {
			t.Fatalf("find the blocking statement in the change log: %v", err)
		}
		return id
	}
	return src
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
	// converges: the restart converges exactly (ADR-0190). False for a cell
	// whose marks are refused or unavailable, or — the default since
	// amendment C — a lane cell whose secondary-unique changes the lanes
	// apply unmarked: it stops on the loud GC-38 (l) collision, as before
	// ADR-0190.
	converges bool
	// exactlyOnceLanes is `sync start --exactly-once-lanes` (amendment C).
	exactlyOnceLanes bool
	// markerlessLoudOrConverges: on a marker-less source (the trigger
	// engines) the default lane cell's outcome is EITHER convergence OR the
	// loud unique collision, and both are accepted. Every trigger change is
	// its own transaction, so the lane checkpoint persists a position INSIDE
	// the source transaction; the restart replays, unmarked, only the changes
	// between that position and the blocking statement, and whether that
	// window contains a collision-prone shape depends on where the checkpoint
	// happened to sit and which of those changes a lane had committed —
	// timing, not the product. MEASURED (2026-09-29): the position sat at
	// change-log id 102 / 104 with the blocking statement at 109, and both
	// pairs converged. The convergence was first explained as "the checkpoint
	// had passed every applied change", checked as "position = blocking
	// statement − 1", and that check FAILED: the blocking statement shares the
	// secondary-unique table's single lane batch with the changes just before
	// it, so the window 103..108 was most likely never committed — but that is
	// not asserted per change, so the cell does not rely on it and accepts
	// both outcomes. logMarkerlessReplayWindow records the window every run.
	markerlessLoudOrConverges bool

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
		// An apply batch size of 1 or less takes the per-change Apply path
		// whatever the lane count (Streamer.ApplyBatchSize > 1 gates
		// ApplyBatch, the only entry to the lanes), so this cell is serial by
		// construction and keeps its marks by default.
		{name: "lanes_per_change", concurrency: 0, batch: 0, converges: true},
		// The lane path by DEFAULT (amendment C): the lanes write no marks, so
		// the transaction's secondary-unique changes replay unmarked onto the
		// prefix and stop on the collision — the documented pre-ADR outcome,
		// loud and recoverable, pinned as the expected default. Its barrier
		// change (the primary-key update 900010 → 900011) still marks.
		{name: "lanes_batched", concurrency: 0, batch: 1000, markerlessLoudOrConverges: true},
		// ... and with --exactly-once-lanes it converges.
		{name: "lanes_batched_exactly_once", concurrency: 0, batch: 1000, exactlyOnceLanes: true, converges: true},
		{name: "pkchange_serial_per_change", concurrency: 1, batch: 0, body: crashMidTxnPKChangeSource, converges: true},
		// The barrier keeps its marks by default: the key change converges on
		// the lane path without the flag, its lane changes CHECKING the
		// barrier's marks (the insert of key 10 is skipped on them).
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
			// Stale marks — another transaction's — are never evidence: one on a
			// key the replay applies skips nothing, and one on a key nothing
			// touches is retired by the restart sweep alone.
			crashMidTxnCell{
				name: "stale_mark_swept", concurrency: 1, batch: 0, converges: true,
				tamper: plantStaleMark,
			},
			// ADR-0190 §8: marks are per KEY, so a restart with a different
			// lane count — or none — reads them the same way.
			// (The first two need the lanes to have WRITTEN marks, so they run
			// with --exactly-once-lanes.)
			crashMidTxnCell{
				name: "lane_count_changed_4_to_2", concurrency: 4, batch: 1000, exactlyOnceLanes: true, converges: true,
				restart: func(s *Streamer) { s.ApplyConcurrency = 2 },
			},
			crashMidTxnCell{
				name: "lane_count_changed_lanes_to_serial", concurrency: 4, batch: 1000, exactlyOnceLanes: true, converges: true,
				restart: func(s *Streamer) { s.ApplyConcurrency = 1 },
			},
			// Marks written by the serial path, CHECKED by default lanes that
			// write none (amendment C): the restart still converges.
			crashMidTxnCell{
				name: "lane_count_changed_serial_to_lanes", concurrency: 1, batch: 0, converges: true,
				restart: func(s *Streamer) { s.ApplyConcurrency, s.ApplyBatchSize = 4, 1000 },
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
	t.Run("lane_post_commit_window", func(t *testing.T) {
		runCrashLanePostCommitWindow(t, src, tgt, true)
	})
	t.Run("lane_post_commit_window_default", func(t *testing.T) {
		runCrashLanePostCommitWindow(t, src, tgt, false)
	})
	// ADR-0190 amendment D: the fold's window, its anchored rule, the mixed
	// barrier orders and a lane-count change after a fold.
	crashFoldCells(t, src, tgt)
	// ADR-0190 amendment E: the lane barrier's pre-apply checkpoint folded
	// into its own transaction — blocked before its commit, and landed.
	crashBarrierFoldCells(t, src, tgt)
	// A schema event inside the interrupted transaction (the 2026-09-28
	// CRITICAL): the serial batched path is the one that regressed; lanes and
	// per-change are the controls.
	schemaCells := []crashMidTxnCell{
		{name: "schema_event_mid_txn_serial_batched", concurrency: 1, batch: 1000, fixedCap: true},
		{name: "schema_event_mid_txn_lanes_batched", concurrency: 0, batch: 1000},
		{name: "schema_event_mid_txn_serial_per_change", concurrency: 1, batch: 0},
	}
	for _, cell := range schemaCells {
		if src.ddlMid == "" {
			break // a source without a schema-event path of its own (the trigger sources)
		}
		t.Run(cell.name, func(t *testing.T) {
			runCrashSchemaEventMidTxn(t, src, tgt, cell, src.ddlBefore, src.ddlMid)
		})
	}
	if src.dropMid != "" {
		// The separate LOUD defect the same regression caused (audit backlog
		// GC-38 (l) follow-up): a DROP COLUMN's 0/0 snapshot position replayed
		// rows from before the DDL and crash-looped on the dropped column.
		t.Run("schema_drop_column_mid_txn_serial_batched", func(t *testing.T) {
			runCrashSchemaEventMidTxn(t, src, tgt, schemaCells[0], src.dropBefore, src.dropMid)
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
	if src.afterSetup != nil {
		src.afterSetup(t)
	}
	tgt.drop(t)
	for _, table := range []string{"po", "ru", "rv", "kl"} {
		execTargetQuiet(tgt, "DROP TABLE IF EXISTS "+table)
	}

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
			SourceDSN:        src.streamerDSN(),
			TargetDSN:        tgt.dsn,
			StreamID:         streamID,
			ApplyConcurrency: cell.concurrency,
			ApplyBatchSize:   cell.batch,
			AutoTune:         cell.batch > 1 && !cell.fixedCap,
			ExactlyOnceLanes: cell.exactlyOnceLanes,
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
	if !src.noKeyless {
		src.exec(t, `INSERT INTO kl (k, v) VALUES (100, 'live')`)
	}
	src.exec(t, fmt.Sprintf(`INSERT INTO rs (id, u, v, pad) VALUES (%d, NULL, 'live', 'l')`, crashSentinelLive))
	waitResend(t, run1, 60*time.Second, "the first CDC rows", func() bool {
		return tgt.has(crashSentinelLive) && (src.noKeyless || countTarget(tgt, "SELECT COUNT(*) FROM kl WHERE k = 100") == 1)
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

	assertKillPosition(t, src, posBefore, posAfterKill)
	assertMarksNameTheInterruptedTx(t, src, tgt, fx.streamID, posAfterKill, "")
	return prefixApplied
}

// logMarkerlessReplayWindow records, at the kill of a markerlessLoudOrConverges
// cell, the unmarked window the restart will replay — from the persisted
// position to the blocking statement (the first change the kill leaves
// unapplied, per the source's change log) — and checks the two facts that do
// NOT depend on timing: the default lanes wrote no apply mark, and the
// position never passed the blocking statement (it was never applied).
func logMarkerlessReplayWindow(t *testing.T, src crashSource, tgt resendTarget, streamID string) {
	t.Helper()
	pos := src.positionOrdinal(t, tgt.readPos(streamID))
	blocked := src.blockedOrdinal(t)
	if marks := targetMarks(t, tgt, streamID); len(marks) != 0 {
		t.Errorf("the default lane path wrote %d apply marks (%v); without --exactly-once-lanes the lanes write none",
			len(marks), marksPerTable(marks))
	}
	if pos >= blocked {
		t.Errorf("the persisted position (change-log id %d) is at or past the blocking statement %d, which never applied", pos, blocked)
	}
	t.Logf("unmarked replay window at the kill: change-log ids %d..%d (position %d, blocking statement %d)", pos+1, blocked-1, pos, blocked)
}

// assertKillPosition grades the persisted position across the kill. On a
// source with transaction boundaries it must not move at all: past a partly
// applied transaction skips its remainder on resume, behind an earlier one
// replays it ahead of this one's marks (both silent). A marker-less source
// persists its position WITH the data at every flush, so it legitimately
// advances inside a source transaction — but never backwards.
func assertKillPosition(t *testing.T, src crashSource, before, after string) {
	t.Helper()
	if src.markerless {
		if b, a := src.positionOrdinal(t, before), src.positionOrdinal(t, after); a < b {
			t.Errorf("the persisted position moved BACKWARDS across the kill (%d → %d): an applied change would replay", b, a)
		}
		return
	}
	if after != before {
		t.Errorf("the persisted position MOVED across a kill mid-transaction (either direction is a defect): %q before the "+
			"transaction, %q after — past a partly applied transaction skips its remainder on resume; behind an earlier "+
			"transaction replays it ahead of this one's marks (both silent)", before, after)
	}
}

// assertMarksNameTheInterruptedTx asserts the invariant the restart sweep and
// the APPLY-MARK-UNTRUSTED check rest on (ADR-0190 amendment B): at a kill,
// every durable mark names ONE transaction — the one the restart re-delivers
// first. The expected identity, when want is "", is derived where it can be:
// from the position persisted at the kill (posAtKill — the transaction the
// restart re-delivers first begins exactly there; src.firstTxAfter), else
// from the source's own state (src.lastTxID, which with the position
// unchanged across the kill is the interrupted transaction).
func assertMarksNameTheInterruptedTx(t *testing.T, src crashSource, tgt resendTarget, streamID, posAtKill, want string) {
	t.Helper()
	marks := targetMarks(t, tgt, streamID)
	txs := distinctTxs(marks)
	if len(txs) > 1 {
		t.Errorf("apply marks exist for %d transactions at the kill (%v); only the first a restart re-delivers may have any", len(txs), txs)
	}
	if want == "" && src.firstTxAfter != nil && len(txs) > 0 {
		want = src.firstTxAfter(t, posAtKill)
	}
	if want == "" && src.lastTxID != nil && len(txs) > 0 {
		want = src.lastTxID(t)
	}
	for _, tx := range txs {
		if want != "" && tx != want {
			t.Errorf("apply marks at the kill name transaction %q; the only transaction a mark may name is the interrupted one, "+
				"the first a restart re-delivers — %q per the source", tx, want)
		}
	}
	t.Logf("apply marks at the kill: %d (%v), transactions %v", len(marks), marksPerTable(marks), txs)
}

// assertNoUntrustedMark fails when a restart logged APPLY-MARK-UNTRUSTED:
// the run met a mark of a transaction it did not deliver first, which means
// the position had moved behind the marks — converging only because the
// runtime check caught it is still a defect in whatever moved the position.
func assertNoUntrustedMark(t *testing.T, logs string) {
	t.Helper()
	if strings.Contains(logs, applymarks.UntrustedMarker) {
		t.Errorf("the restart logged %s: a durable mark named a transaction the run did not re-deliver first — the "+
			"persisted position moved behind the marks", applymarks.UntrustedMarker)
	}
}

func runCrashMidTxnCell(t *testing.T, src crashSource, tgt resendTarget, cell crashMidTxnCell) {
	fx := newCrashStreamFixture(t, src, tgt, cell)
	defer fx.teardown()
	body := cell.body
	if body == "" {
		body = crashMidTxnSource
	}
	either := cell.markerlessLoudOrConverges && src.markerless
	prefixApplied := runCrashToKill(t, src, tgt, fx, cell, body)
	if either {
		logMarkerlessReplayWindow(t, src, tgt, fx.streamID)
	}
	if prefixApplied == cell.cohesive {
		t.Errorf("prefix committed before the kill = %v; want %v for this path (cohesive %v)", prefixApplied, !cell.cohesive, cell.cohesive)
	}

	marks := targetMarks(t, tgt, fx.streamID)
	perTable := marksPerTable(marks)
	if n := perTable["po"]; n != 0 && body == crashMidTxnSource {
		t.Errorf("the PK-only table po wrote %d apply marks; its changes are idempotent and must write none (operator decision 1)", n)
	}
	// A marker-less source persists its position with every flush, so a serial
	// path never replays an applied change and writes no mark — by design.
	if prefixApplied && cell.converges && cell.tamper == nil && len(marks) == 0 && !src.markerless {
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
	case either:
		// Pre-ADR at-least-once on a marker-less source: whether the unmarked
		// replay window collides depends on where the checkpoint sat (see
		// markerlessLoudOrConverges). Both outcomes are the documented
		// contract; a SILENT divergence is not, and assertCrashDamageConfined
		// above already graded the untouched rows.
		if caughtUp && restartErr == nil {
			assertCrashConverged(t, src, tgt)
			t.Logf("default lanes on a marker-less source: the unmarked replay converged")
			break
		}
		if !isUniqueCollision(restartErr) {
			t.Fatalf("default lanes on a marker-less source: the restart neither converged nor stopped on the unique "+
				"collision (caught up %v, error %v)", caughtUp, restartErr)
		}
		t.Logf("default lanes on a marker-less source: the unmarked replay stopped on the collision, loudly: %v", restartErr)
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
		assertNoUntrustedMark(t, logs)
		// GC: the position passed the crashed transaction (and the sentinel's),
		// so their marks are gone — deleted with the position that passed them.
		if left := targetMarks(t, tgt, fx.streamID); len(left) > 0 {
			t.Errorf("%d apply marks survived the position passing their transactions (garbage collection): %v", len(left), marksPerTable(left))
		}
	default:
		// KNOWN LOUD: the marks are unavailable (marks_unavailable), so the
		// restart behaves exactly as before ADR-0190.
		if caughtUp || restartErr == nil {
			diff := diffResendRows(dumpCrashSource(t, src), tgt.dump(t))
			t.Fatalf("cell changed: the restart after a kill mid-transaction resumed (caught up %v, error %v); find out why. Diff:\n%s",
				caughtUp, restartErr, diff)
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

// runCrashLanePostCommitWindow is ADR-0190 amendment A's own cell. Two source
// transactions on one row of the secondary-unique table — T1 updates row 7,
// T2 deletes it — reach the lane BATCH path while the stream's position row
// is held locked on the target, so every checkpoint blocks and the lanes
// commit into the window between a lane commit and the checkpoint that would
// pass it. The kill lands in that window.
//
// Without the mark fence a lane can commit T2's delete, with its mark, behind
// T1's update; the restart from before T1 re-applies T1's update (row 7's
// mark is T2's, which proves nothing about T1, so it recreates the row) and
// skips T2's delete on its own mark — row 7 left on the target, at exit 0.
// With the fence, T2's first marked change waits for the checkpoint past T1,
// which the lock holds back, so T2 never commits before the kill; the restart
// skips T1 on its mark and applies T2. Graded against the source's table.
//
// exactlyOnce false is the DEFAULT lane path (amendment C): the lanes write
// no marks and never fence, so T1 and T2 both commit into the window with no
// mark, and the restart replays them unmarked — T1's update re-creates row 7
// (an idempotent upsert) and T2's delete removes it again. Nothing can be
// skipped without a mark, so the window is harmless there; the cell pins
// exactly that: no mark at the kill, and a converged restart.
func runCrashLanePostCommitWindow(t *testing.T, src crashSource, tgt resendTarget, exactlyOnce bool) {
	cell := crashMidTxnCell{concurrency: 0, batch: 1000, exactlyOnceLanes: exactlyOnce}
	fx := newCrashStreamFixture(t, src, tgt, cell)
	defer fx.teardown()
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	run1 := make(chan error, 1)
	go func() { run1 <- fx.newStreamer().Run(ctx1) }()
	waitResend(t, run1, 120*time.Second, "the cold copy", func() bool { return len(tgt.dump(t)) >= 200 })
	src.exec(t, fmt.Sprintf(`INSERT INTO rs (id, u, v, pad) VALUES (%d, NULL, 'live', 'l')`, crashSentinelLive))
	waitResend(t, run1, 60*time.Second, "the first CDC row", func() bool { return tgt.has(crashSentinelLive) })
	posBefore := settledPosition(t, tgt, fx.streamID)

	release := holdRowLock(t, driverOf(tgt), tgt.dsn,
		fmt.Sprintf(`SELECT 1 FROM sluice_cdc_state WHERE stream_id = '%s' FOR UPDATE`, fx.streamID))
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	src.txn(t, `UPDATE rs SET v = 't1' WHERE id = 7;`)
	t1 := ""
	if src.lastTxID != nil && src.firstTxAfter == nil { // else derived from the position at the kill
		t1 = src.lastTxID(t) // the first transaction the restart must re-deliver
	}
	src.txn(t, `DELETE FROM rs WHERE id = 7;`)
	// T1 reaching the target (or, without the fence, T1 and T2 both) proves
	// the lanes committed into the window no checkpoint can close.
	landed := waitResendSoft(run1, 30*time.Second, func() bool {
		return countTarget(tgt, "SELECT COUNT(*) FROM rs WHERE id = 7 AND v = 't1'") == 1 ||
			countTarget(tgt, "SELECT COUNT(*) FROM rs WHERE id = 7") == 0
	})
	if !landed {
		t.Fatal("T1 never reached the target: the window was not built, so this cell would pass vacuously")
	}
	time.Sleep(2 * time.Second) // let T2 reach the fence (or, without one, its lane)
	// Amendment D: T2's position rides its own lane batch (a fold), so the
	// writer the held control row blocks is a LANE transaction that has
	// already deleted row 7 — not a coordinator checkpoint, which touches
	// only control tables. The target's own lock view is the evidence.
	foldBlocked := exactlyOnce && waitResendSoft(run1, 15*time.Second, func() bool { return lockWaiterHolds(tgt, "rs") })
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
		t.Fatalf("the position moved (%q → %q) while its row was held locked: the window closed before the kill, so the cell measured nothing",
			posBefore, posAfterKill)
	}
	if exactlyOnce {
		if !foldBlocked {
			t.Error("no transaction holding a lock on rs waited on the stream's control row: T2's fold was not the blocked " +
				"writer (amendment D folds T2's position into the lane batch that deletes row 7)")
		}
		// No mark lands without its position: T2's delete, its mark and its
		// position were one transaction, and it never committed.
		if n := countTarget(tgt, "SELECT COUNT(*) FROM rs WHERE id = 7 AND v = 't1'"); n != 1 {
			t.Errorf("row 7 with T1's value is %d rows at the kill; want 1 — T2's delete became durable without its position", n)
		}
		// T1's update is a marked lane change the fence admitted (its
		// checkpoint had nothing new to write under the lock), so its mark
		// must be on the target: without it this cell would pass on the
		// idempotent replay alone and grade nothing about the lane marks.
		if len(targetMarks(t, tgt, fx.streamID)) == 0 {
			t.Error("no apply mark at the kill with --exactly-once-lanes: the lanes wrote none, so the fence went ungraded")
		}
		assertMarksNameTheInterruptedTx(t, src, tgt, fx.streamID, posAfterKill, t1)
	} else if marks := targetMarks(t, tgt, fx.streamID); len(marks) > 0 {
		t.Errorf("the default lane path wrote %d apply marks (%v); without --exactly-once-lanes the lanes write none",
			len(marks), marksPerTable(marks))
	}
	t.Logf("row 7 at the kill: %d", countTarget(tgt, "SELECT COUNT(*) FROM rs WHERE id = 7"))

	caughtUp, restartErr := resumeCrashStreamWith(t, fx.newStreamer, nil, src, tgt, true, fx.streamID, posAfterKill)
	if !caughtUp || restartErr != nil {
		t.Fatalf("the restart after a kill in the lane post-commit window did not converge: caught up %v, error %v", caughtUp, restartErr)
	}
	assertCrashConverged(t, src, tgt)
	assertNoUntrustedMark(t, fx.logBuf.String())
	if left := targetMarks(t, tgt, fx.streamID); len(left) > 0 {
		t.Errorf("%d apply marks survived the position passing their transactions (garbage collection): %v", len(left), marksPerTable(left))
	}
}

// runCrashSchemaEventMidTxn is the 2026-09-28 pre-land review's CRITICAL,
// as a cell. A DDL on po (ddlMid) is followed by T-1, one INSERT of row 500
// into the secondary-unique table; then T deletes row 500 (a marked change),
// writes po's first post-DDL row — which reaches the applier as a
// SchemaSnapshot stamped with the DDL's OWN position, from before T-1 — and
// blocks on a target row lock. The kill lands there.
//
// Before the fix the serial batched path persisted the snapshot's position
// mid-transaction, regressing the stream behind T-1 while T's delete and its
// mark were durable; the restart replayed T-1 (re-creating row 500) and then
// skipped T's delete on its own mark: row 500 left on the target at exit 0.
// The pins, each on its own evidence: the position did not move across the
// kill (a regression is a move); the marks name T, per the source; the
// restart converges to the source's table; and it did so without the
// APPLY-MARK-UNTRUSTED runtime check having to fire (which would mean the
// position moved and only the check saved the run).
func runCrashSchemaEventMidTxn(t *testing.T, src crashSource, tgt resendTarget, cell crashMidTxnCell, ddlBefore, ddlMid string) {
	fx := newCrashStreamFixture(t, src, tgt, cell)
	defer fx.teardown()
	if ddlBefore != "" {
		src.exec(t, ddlBefore)
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	run1 := make(chan error, 1)
	go func() { run1 <- fx.newStreamer().Run(ctx1) }()
	waitResend(t, run1, 120*time.Second, "the cold copy", func() bool { return len(tgt.dump(t)) >= 200 })
	src.exec(t, `INSERT INTO po (id, v) VALUES (100, 'live')`)
	src.exec(t, fmt.Sprintf(`INSERT INTO rs (id, u, v, pad) VALUES (%d, NULL, 'live', 'l')`, crashSentinelLive))
	waitResend(t, run1, 60*time.Second, "the first CDC rows", func() bool {
		return tgt.has(crashSentinelLive) && countTarget(tgt, "SELECT COUNT(*) FROM po WHERE id = 100") == 1
	})
	src.exec(t, ddlMid)
	src.txn(t, `INSERT INTO rs (id, u, v, pad) VALUES (500, 'x500', 't1', 'p');`) // T-1
	waitResend(t, run1, 60*time.Second, "T-1", func() bool { return countTarget(tgt, "SELECT COUNT(*) FROM rs WHERE id = 500") == 1 })
	posBefore := settledPosition(t, tgt, fx.streamID)

	release := holdRowLock(t, driverOf(tgt), tgt.dsn, `SELECT id FROM rs WHERE id = 100 FOR UPDATE`)
	released := false
	defer func() {
		if !released {
			release()
		}
	}()
	src.txn(t, `DELETE FROM rs WHERE id = 500;
INSERT INTO po (id, v) VALUES (501, 'n');
UPDATE rs SET v = 'blocked' WHERE id = 100;
`) // T
	if !waitResendSoft(run1, 30*time.Second, func() bool { return countTarget(tgt, "SELECT COUNT(*) FROM rs WHERE id = 500") == 0 }) {
		t.Fatal("T's delete never reached the target: no prefix, so this cell would pass vacuously")
	}
	time.Sleep(3 * time.Second) // let the snapshot's flush and the blocked statement happen
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
		t.Errorf("the persisted position MOVED across a kill mid-transaction (either direction is a defect): %q after T-1, %q "+
			"after the kill — a schema event inside T persisted its DDL-anchored position", posBefore, posAfterKill)
	}
	assertMarksNameTheInterruptedTx(t, src, tgt, fx.streamID, posAfterKill, "")

	caughtUp, restartErr := resumeCrashStreamWith(t, fx.newStreamer, nil, src, tgt, true, fx.streamID, posAfterKill)
	if !caughtUp || restartErr != nil {
		t.Fatalf("the restart after a kill mid-transaction with a schema event did not converge: caught up %v, error %v", caughtUp, restartErr)
	}
	assertCrashConverged(t, src, tgt)
	assertNoUntrustedMark(t, fx.logBuf.String())
	if left := targetMarks(t, tgt, fx.streamID); len(left) > 0 {
		t.Errorf("%d apply marks survived the position passing their transactions (garbage collection): %v", len(left), marksPerTable(left))
	}
}

// plantStaleMark writes two marks of a transaction that is never
// re-delivered. One sits on row 100's key, which the restart's replay DOES
// apply (the blocked statement never reached the target): it is consulted and
// must prove nothing — and the replay's own mark then overwrites it (same
// key), so it cannot grade the sweep. The other sits on row 150's key, which
// nothing touches: nothing overwrites it and no transaction close names it,
// so only the restart sweep can retire it. Their table name and scope are
// copied from a real mark, and the test's key digest is first checked against
// the engine's (row 1's mark): a digest that matched no key would never be
// consulted.
func plantStaleMark(t *testing.T, tgt resendTarget, streamID string) {
	t.Helper()
	db, err := sql.Open(driverOf(tgt), tgt.dsn)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	row1, _ := applymarks.KeyDigest(ir.Row{"id": int64(1)}, []string{"id"})
	row100, _ := applymarks.KeyDigest(ir.Row{"id": int64(100)}, []string{"id"})
	find := "SELECT table_name, scope_digest FROM sluice_cdc_apply_marks WHERE stream_id = ? AND key_digest = ?"
	plant := "INSERT INTO sluice_cdc_apply_marks (stream_id, table_name, key_digest, tx_id, seq, change_digest, scope_digest) VALUES (?, ?, ?, ?, ?, ?, ?)"
	if tgt.engine == "postgres" {
		find = "SELECT table_name, scope_digest FROM sluice_cdc_apply_marks WHERE stream_id = $1 AND key_digest = $2"
		plant = "INSERT INTO sluice_cdc_apply_marks (stream_id, table_name, key_digest, tx_id, seq, change_digest, scope_digest) VALUES ($1, $2, $3, $4, $5, $6, $7)"
	}
	var table, scope string
	if err := db.QueryRow(find, streamID, row1).Scan(&table, &scope); err != nil {
		t.Fatalf("no apply mark on row 1's key (%v): the test's key digest does not match the engine's, so a planted mark would never be consulted", err)
	}
	row150, _ := applymarks.KeyDigest(ir.Row{"id": int64(150)}, []string{"id"})
	for _, key := range []string{row100, row150} {
		if _, err := db.Exec(plant, streamID, table, key, "stale:never-redelivered", 1_000_000, "stale", scope); err != nil {
			t.Fatalf("plant a stale mark: %v", err)
		}
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
	queries := []string{
		"SELECT CONCAT(id, '|', v) FROM po",
		"SELECT CONCAT(id, '|', COALESCE(u, '<null>'), '|', v) FROM ru",
		"SELECT CONCAT(id, '|', COALESCE(u, '<null>'), '|', v) FROM rv",
	}
	if !src.noKeyless {
		queries = append(queries, "SELECT CONCAT(k, '|', v) FROM kl")
	}
	for _, q := range queries {
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
