//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// UNIQUE-KEY-STOPPED-WINDOW-DOOR, round-1 review pins, on real servers and
// graded on the source's rows against the target's (ukdRows: id|k|v):
//
//   - H1: a cross-dialect expression or partial key whose name a Postgres
//     target table-prefixes (`uq_e` → `kd_uq_e`) is still matched — MySQL →
//     Postgres and SQLite (sqlite-trigger) → Postgres — at cold start and
//     on a warm restart;
//   - redefinitions under the same name, refused: a MySQL prefix c(5) →
//     c(10), a Postgres lower(k) → upper(k), a Postgres partial WHERE n > 0
//     → WHERE n > 5; and the KNOWN MISS, a cross-dialect expression
//     redefined under its old name, pinned passing so the hole stays visible;
//   - M1: a Postgres INDEX collation (`(k COLLATE ci)`, nondeterministic
//     ICU) on the target that the source's re-created key lacks;
//   - L3: a source unique index left invalid by a failed CREATE UNIQUE INDEX
//     CONCURRENTLY does not count as enforcing;
//   - L5: a pgoutput DROP COLUMN forwarded to MySQL narrows UNIQUE(a, b) to
//     UNIQUE(a) on the target; the boundary check refuses before a
//     duplicate on a can merge (a keyless table: the apply guard is unarmed
//     there);
//   - a sqlite-trigger source's stopped DROP INDEX, refused at open.
//
// Shard: TestStreamer_ prefix → pipeline-rest-streamer.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
	"sluicesync.dev/sluice/internal/ir"
)

// ukdColdStartStop cold-starts the cell until kd row 2 is on the target and
// the stream is in CDC, then stops it.
func ukdColdStartStop(t *testing.T, cell twfbCell, newStreamer func() *Streamer) {
	t.Helper()
	run := startTWFBRun(newStreamer())
	if !cell.tgt.waitRow(t, "kd", 2, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the cold start stopped with %v", err)
	}
}

// ukdRestartExpect restarts the stream and requires either the door's
// at-open refusal naming key (the target unchanged) or, with key == "", a
// run that converges and carries one more row.
func ukdRestartExpect(t *testing.T, cell twfbCell, newStreamer func() *Streamer, key, nextRow string) {
	t.Helper()
	if key == "" {
		ukdConvergeAndResume(t, newStreamer(), cell.src, cell.tgt, nextRow)
		return
	}
	before := ukdRows(t, cell.tgt)
	run := startTWFBRun(newStreamer())
	ukdAwaitExitOrConverge(t, run, cell.src, cell.tgt, 120*time.Second)
	ukdRequireRefusal(t, run, cell.tgt, before, key, ir.UniqueKeyDoorAtOpen)
}

// TestStreamer_UniqueKeyDoor_ReviewRedefinitions covers H1's MySQL →
// Postgres unprefixed name, the three same-name redefinitions the door
// refuses, and the cross-dialect one it does not.
func TestStreamer_UniqueKeyDoor_ReviewRedefinitions(t *testing.T) {
	myNew, myCleanup := ukcMySQLServer(t)
	defer myCleanup()
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()

	t.Run("mysql to mysql: prefix c(5) redefined as c(10) under its name", func(t *testing.T) {
		src, tgt := myNew("ukdra_s"), myNew("ukdra_t")
		src.exec(t, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) NOT NULL, v VARCHAR(20), UNIQUE KEY kd_c (k(5))); "+
			"INSERT INTO kd VALUES (1,'abcde1','one'),(2,'xyzzy2','two')")
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdra"}
		ukdColdStartStop(t, cell, cell.streamer)
		src.exec(t, "ALTER TABLE kd DROP INDEX kd_c, ADD UNIQUE KEY kd_c (k(10)); INSERT INTO kd VALUES (99,'abcde9','prefix-dup')")
		ukdRestartExpect(t, cell, cell.streamer, "kd.kd_c (k(5))", "")
	})
	t.Run("postgres to postgres: lower(k) redefined as upper(k) under its name", func(t *testing.T) {
		src, tgt := pgNew("ukdrb_s"), pgNew("ukdrb_t")
		src.exec(t, "CREATE TABLE kd (id int NOT NULL PRIMARY KEY, k varchar(20) NOT NULL, v varchar(20)); "+
			"CREATE UNIQUE INDEX kd_e ON kd (lower(k)); INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')")
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdrb", slotName: "ukdrb"}
		ukdColdStartStop(t, cell, cell.streamer)
		src.exec(t, "DROP INDEX kd_e; CREATE UNIQUE INDEX kd_e ON kd (upper(k)); INSERT INTO kd VALUES (99,'z','zz')")
		ukdRestartExpect(t, cell, cell.streamer, "kd.kd_e", "")
	})
	t.Run("postgres to postgres: partial WHERE n > 0 redefined as WHERE n > 5", func(t *testing.T) {
		src, tgt := pgNew("ukdrc_s"), pgNew("ukdrc_t")
		src.exec(t, "CREATE TABLE kd (id int NOT NULL PRIMARY KEY, k varchar(20) NOT NULL, v varchar(20), n int NOT NULL); "+
			"CREATE UNIQUE INDEX kd_p ON kd (k) WHERE n > 0; INSERT INTO kd VALUES (1,'a','one',1),(2,'b','two',2)")
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdrc", slotName: "ukdrc"}
		ukdColdStartStop(t, cell, cell.streamer)
		// (99,'a') with n = 3 is legal under WHERE n > 5, a duplicate under n > 0.
		src.exec(t, "DROP INDEX kd_p; CREATE UNIQUE INDEX kd_p ON kd (k) WHERE n > 5; INSERT INTO kd VALUES (99,'a','dup',3)")
		ukdRestartExpect(t, cell, cell.streamer, "kd.kd_p", "")
	})
	t.Run("mysql to postgres: an unprefixed expression key matches, and its cross-dialect redefinition is the KNOWN MISS", func(t *testing.T) {
		src, tgt := myNew("ukdrd_s"), pgNew("ukdrd_t")
		// `uq_e` is not table-prefixed: the Postgres writer holds it as
		// `kd_uq_e` (H1). Before the fix the first cold start refused here.
		src.exec(t, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) NOT NULL, v VARCHAR(20), UNIQUE KEY uq_e ((lower(k)))); "+
			"INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')")
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdrd"}
		ukdColdStartStop(t, cell, cell.streamer)
		if n := tgt.scalar(t, "SELECT count(*) FROM pg_indexes WHERE tablename = 'kd' AND indexname = 'kd_uq_e'"); n != "1" {
			t.Fatalf("the Postgres writer did not hold uq_e as kd_uq_e (count %s): the premise of this cell moved", n)
		}
		ukdRestartExpect(t, cell, cell.streamer, "", "INSERT INTO kd VALUES (3,'c','three')")
		// KNOWN MISS (stated in irdiff's file comment): a cross-dialect
		// expression is matched by name and shape, so a redefinition under the
		// same name passes the door. Pinned passing so the hole stays visible;
		// a fix flips this cell.
		src.exec(t, "ALTER TABLE kd DROP INDEX uq_e, ADD UNIQUE KEY uq_e ((upper(k)))")
		ukdRestartExpect(t, cell, cell.streamer, "", "INSERT INTO kd VALUES (4,'d','four')")
		t.Log("KNOWN MISS: a cross-dialect expression key redefined under its old name passes the unique-key door")
	})
}

// TestStreamer_UniqueKeyDoor_ReviewPostgresIndexFacts covers M1 (a target
// INDEX collation the source's key lacks) and L3 (an invalid source index is
// not enforcing), Postgres → Postgres on keyless tables, where the ON
// CONFLICT key is the unique index itself and the apply guard is unarmed.
func TestStreamer_UniqueKeyDoor_ReviewPostgresIndexFacts(t *testing.T) {
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()
	const ci = "CREATE COLLATION IF NOT EXISTS ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false); "

	t.Run("M1: a target index collation the source key lacks", func(t *testing.T) {
		src, tgt := pgNew("ukdfa_s"), pgNew("ukdfa_t")
		src.exec(t, ci+"CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, v varchar(20)); "+
			"CREATE UNIQUE INDEX kd_k ON kd (k COLLATE ci); ALTER TABLE kd REPLICA IDENTITY FULL; "+
			"INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')")
		// The operator's target carries the same case-insensitive key (the PG
		// writer would not carry the index collation; it reads no
		// indcollation).
		tgt.exec(t, ci+"CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, v varchar(20)); "+
			"CREATE UNIQUE INDEX kd_k ON kd (k COLLATE ci)")
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdfa", slotName: "ukdfa"}
		ukdColdStartStop(t, cell, cell.streamer)
		// While stopped the source re-creates the key BINARY and writes 'A'
		// beside 'a'; the target's case-insensitive kd_k would merge them.
		src.exec(t, "DROP INDEX kd_k; CREATE UNIQUE INDEX kd_k ON kd (k); INSERT INTO kd VALUES (99,'A','upper')")
		ukdRestartExpect(t, cell, cell.streamer, "kd.kd_k", "")
	})
	t.Run("L3: a source unique index left invalid does not cover the target's", func(t *testing.T) {
		src, tgt := pgNew("ukdfb_s"), pgNew("ukdfb_t")
		src.exec(t, "CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, v varchar(20)); "+
			"CREATE UNIQUE INDEX kd_k ON kd (k); ALTER TABLE kd REPLICA IDENTITY FULL; "+
			"INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')")
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdfb", slotName: "ukdfb"}
		ukdColdStartStop(t, cell, cell.streamer)
		// Drop the key, write a duplicate, then try to re-create it
		// CONCURRENTLY: the build fails on the duplicate and leaves kd_k
		// INVALID — same name, same structure, enforcing nothing.
		src.exec(t, "DROP INDEX kd_k; INSERT INTO kd VALUES (99,'a','dup')")
		ukdExecExpectError(t, src, "CREATE UNIQUE INDEX CONCURRENTLY kd_k ON kd (k)")
		if v := src.scalar(t, "SELECT indisvalid::text FROM pg_index WHERE indexrelid = 'kd_k'::regclass"); v != "false" {
			t.Fatalf("kd_k is not left invalid (indisvalid=%s): the premise of this cell moved", v)
		}
		ukdRestartExpect(t, cell, cell.streamer, "kd.kd_k", "")
	})
}

// ukdExecExpectError runs a statement that must fail.
func ukdExecExpectError(t *testing.T, db twfbDB, stmt string) {
	t.Helper()
	conn, err := sql.Open(db.driver(), db.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Exec(stmt); err == nil {
		t.Fatalf("%q succeeded; the cell needs it to fail", stmt)
	}
}

// TestStreamer_UniqueKeyDoor_ReviewForwardedDropColumn covers L5: pgoutput
// → MySQL, keyless kd with UNIQUE(k, b) as its only key. A live DROP COLUMN
// b is forwarded; MySQL narrows the key to UNIQUE(k) while the Postgres
// source dropped the index outright, so the source now holds no key and the
// target one it never had. The boundary check must refuse before the
// following duplicate on k merges onto row 1 (the apply guard is unarmed on
// a keyless table with one unique key).
func TestStreamer_UniqueKeyDoor_ReviewForwardedDropColumn(t *testing.T) {
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()
	myNew, myCleanup := ukcMySQLServer(t)
	defer myCleanup()
	src, tgt := pgNew("ukdgc_s"), myNew("ukdgc_t")
	src.exec(t, "CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, v varchar(20), b int NOT NULL); "+
		"CREATE UNIQUE INDEX kd_kb ON kd (k, b); ALTER TABLE kd REPLICA IDENTITY FULL; "+
		"INSERT INTO kd VALUES (1,'a','one',1),(2,'b','two',1)")
	cell := twfbCell{src: src, tgt: tgt, streamID: "ukdgc", slotName: "ukdgc"}
	run := startTWFBRun(cell.streamer())
	if !tgt.waitRow(t, "kd", 2, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	src.exec(t, "INSERT INTO kd VALUES (50,'p','fifty',1)")
	if !tgt.waitRow(t, "kd", 50, run, 60*time.Second) {
		t.Fatalf("the priming row never landed (%v)", run.stop(t))
	}
	src.exec(t, "ALTER TABLE kd DROP COLUMN b")
	src.exec(t, "INSERT INTO kd VALUES (99,'a','ninetynine')")
	ukdAwaitExitOrConverge(t, run, src, tgt, 90*time.Second)
	if !run.exited() {
		_ = run.stop(t)
		t.Fatalf("nothing refused (target %s)", ukdRows(t, tgt))
	}
	var refusal *ir.SourceUniqueKeyAbsentError
	if !errors.As(run.err, &refusal) || refusal.When != ir.UniqueKeyDoorAtSchemaChange {
		t.Fatalf("refused with %v; want the boundary SOURCE-UNIQUE-KEY-ABSENT", run.err)
	}
	t.Logf("refusal: %v", run.err)
	g := ukdRows(t, tgt)
	if strings.Contains(g, "99|") || !strings.Contains(g, "1|a|one") {
		t.Fatalf("row 99 merged or row 1 changed: target %s", g)
	}
	// The printed remedy: drop the narrowed key on the target, start again.
	tgt.exec(t, "ALTER TABLE kd DROP INDEX kd_kb")
	ukdConvergeAndResume(t, cell.streamer(), src, tgt, "INSERT INTO kd VALUES (8,'r','eight')")
}

// TestStreamer_UniqueKeyDoor_ReviewSQLiteTrigger covers a sqlite-trigger
// source → Postgres: H1's unprefixed partial key (`uq_live` → `kd_uq_live`)
// passes at cold start and on a restart, and a stopped DROP INDEX refuses at
// open. The SQLite EXPRESSION key beside it is not portable — the Postgres
// writer skips it with a WARN (measured), so no target key exists to judge.
func TestStreamer_UniqueKeyDoor_ReviewSQLiteTrigger(t *testing.T) {
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()
	path := filepath.Join(t.TempDir(), "app.db")
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		"CREATE TABLE kd (id INTEGER PRIMARY KEY, k TEXT NOT NULL, v TEXT, d INTEGER)",
		"CREATE UNIQUE INDEX uq_lower ON kd (lower(k))",
		"CREATE UNIQUE INDEX uq_live ON kd (v) WHERE d IS NULL",
		"INSERT INTO kd VALUES (1,'a','one',NULL),(2,'b','two',NULL)",
	} {
		sqliteExec(t, path, stmt)
	}
	if _, err := sqlitetrigger.Setup(context.Background(), path, sqlitetrigger.SetupOptions{Tables: []string{"kd"}}); err != nil {
		t.Fatalf("sqlitetrigger.Setup: %v", err)
	}
	srcEng, ok := engines.Get(sqlitetrigger.EngineName)
	if !ok {
		t.Fatal("sqlite-trigger engine not registered")
	}
	pgEng, _ := engines.Get("postgres")
	tgt := twfbDB{engine: "postgres", dsn: pgTarget}
	newStreamer := func() *Streamer {
		return &Streamer{
			Source: srcEng, Target: pgEng, SourceDSN: path, TargetDSN: pgTarget,
			StreamID: "ukdsq", Filter: mustNewFilter(t, nil, nil), ApplyRetryAttempts: 8,
		}
	}
	run := startTWFBRun(newStreamer())
	if !tgt.waitRow(t, "kd", 2, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
	}
	time.Sleep(3 * time.Second)
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the cold start stopped with %v", err)
	}
	for _, name := range []string{"kd_uq_live"} {
		if n := tgt.scalar(t, "SELECT count(*) FROM pg_indexes WHERE tablename = 'kd' AND indexname = $1", name); n != "1" {
			t.Fatalf("the Postgres writer did not hold %s (count %s): the premise of this cell moved", name, n)
		}
	}
	// A restart passes (the names map; H1).
	run = startTWFBRun(newStreamer())
	sqliteExec(t, path, "INSERT INTO kd VALUES (3,'c','three',NULL)")
	if !tgt.waitRow(t, "kd", 3, run, 90*time.Second) {
		t.Fatalf("the restart did not stream (%v)", run.stop(t))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the restart stopped with %v", err)
	}
	if s, g := ukdRowsSQLite(t, path), ukdRows(t, tgt); s != g {
		t.Fatalf("the restart did not converge\nsource %s\ntarget %s", s, g)
	}
	// A stopped DROP INDEX refuses at open, the target unchanged.
	before := ukdRows(t, tgt)
	sqliteExec(t, path, "DROP INDEX uq_live")
	sqliteExec(t, path, "INSERT INTO kd VALUES (99,'z','one',NULL)")
	run = startTWFBRun(newStreamer())
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) && !run.exited() {
		time.Sleep(300 * time.Millisecond)
	}
	ukdRequireRefusal(t, run, tgt, before, "kd.kd_uq_live", ir.UniqueKeyDoorAtOpen)
}

// ukdRowsSQLite is ukdRows for a SQLite source file.
func ukdRowsSQLite(t *testing.T, path string) string {
	t.Helper()
	conn, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	rows, err := conn.Query("SELECT COALESCE(CAST(id AS TEXT), '∅'), COALESCE(k, '∅'), COALESCE(v, '∅') FROM kd")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id, k, v string
		if err := rows.Scan(&id, &k, &v); err != nil {
			t.Fatal(err)
		}
		out = append(out, id+"|"+k+"|"+v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}
