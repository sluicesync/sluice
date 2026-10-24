//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// UNIQUE-KEY-STOPPED-WINDOW-DOOR, round-5 review pins, on real servers and
// graded on the source's rows against the target's (ukdRows: id|k|v):
//
//   - MEDIUM-1: an acceptance (--accept-target-only-unique-key) is bound to
//     the key's shape as accepted. A forwarded DROP COLUMN narrowing the
//     accepted MySQL key UNIQUE (k, x) to UNIQUE (k) under its own name used
//     to pass by name, so the next row duplicating k overwrote row 1 at exit
//     0. It must refuse, naming the accepted shape. The same holds for a
//     table added with `schema add-table` while the stream ran (round 6:
//     the acceptance carries the shape as a fingerprint, so it holds there).
//   - MEDIUM-2: a sqlite-trigger source (no collation resolver) into a
//     Postgres target column known to fold — citext, a nondeterministic
//     collation — refuses; the default deterministic text column passes.
//
// Shard: TestStreamer_ prefix → pipeline-rest-streamer.

package pipeline

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	sqlitetrigger "sluicesync.dev/sluice/internal/engines/sqlite-trigger"
	"sluicesync.dev/sluice/internal/ir"
)

func TestStreamer_UniqueKeyDoor_Review5AcceptedKeyNarrowed(t *testing.T) {
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()
	myNew, myCleanup := ukcMySQLServer(t)
	defer myCleanup()
	src, tgt := pgNew("ukdr5a_s"), myNew("ukdr5a_t")
	src.exec(t, "CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, x int NOT NULL DEFAULT 1, v varchar(20)); "+
		"ALTER TABLE kd REPLICA IDENTITY FULL; INSERT INTO kd VALUES (1,'a',1,'one'),(2,'b',1,'two')")
	cell := twfbCell{src: src, tgt: tgt, streamID: "ukdr5a", slotName: "ukdr5a"}
	ukdColdStartStop(t, cell, cell.streamer)
	// The operator's key, accepted by name on every start from here.
	tgt.exec(t, "ALTER TABLE kd ADD UNIQUE KEY uk (k, x)")
	accept := ukdAcceptValue(t, tgt, "kd", "uk") // taken once, as the operator pastes it
	accepted := func() *Streamer {
		s := cell.streamer()
		s.AcceptTargetOnlyUniqueKeys = accept
		// A keyless table cannot be backfilled by cursor; the priming ADD
		// COLUMN below needs none.
		s.SuppressAddedColumnBackfill = true
		return s
	}
	run := startTWFBRun(accepted())
	// A forwarded ADD COLUMN first, so the DROP below is a CDC→CDC boundary
	// the forward applies (the first boundary of a run is compared with the
	// target instead).
	src.exec(t, "ALTER TABLE kd ADD COLUMN p int; INSERT INTO kd (id,k,x,v) VALUES (3,'c',1,'three')")
	if !tgt.waitRow(t, "kd", 3, run, 90*time.Second) {
		t.Fatalf("the accepted stream did not run (%v)", run.stop(t))
	}
	// MySQL narrows uk to (k) when the forward drops x; row 99 duplicates k.
	src.exec(t, "ALTER TABLE kd DROP COLUMN x")
	src.exec(t, "INSERT INTO kd (id,k,v) VALUES (99,'a','ninetynine')")
	ukdAwaitExitOrConverge(t, run, src, tgt, 90*time.Second)
	refusal := ukdR3RequireNoMergeRefusal(t, run, src, tgt, ir.UniqueKeyDoorAtSchemaChange)
	if !strings.HasSuffix(accept[0], "="+refusal.Keys[0].AcceptedFingerprint) ||
		!strings.Contains(run.err.Error(), "accepted with fingerprint "+refusal.Keys[0].AcceptedFingerprint) {
		t.Fatalf("the refusal does not name the accepted fingerprint: %v", run.err)
	}
	if g := ukdRows(t, tgt); strings.Contains(g, "99|") {
		t.Fatalf("the duplicate was applied: target %s", g)
	}
	// The printed remedy: drop the narrowed key, start again.
	tgt.exec(t, "ALTER TABLE kd DROP INDEX uk")
	ukdConvergeAndResume(t, accepted(), src, tgt, "INSERT INTO kd (id,k,v) VALUES (8,'r','eight')")
}

// TestStreamer_UniqueKeyDoor_Review5AddTableAcceptedKey pins the add-table
// path of MEDIUM-1: a table added while the stream runs, onto a pre-created
// target carrying an accepted key, has no shape recorded by the stream's
// start, so a later schema change on it refuses rather than honour the name.
func TestStreamer_UniqueKeyDoor_Review5AddTableAcceptedKey(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	applyDDLMySQL(t, srcDSN, "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL); "+
		"INSERT INTO users VALUES (1, 'alice@example.com');")
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	const streamID = "ukd-r5-add"
	// The operator's pre-created table, with the key they accept: its value
	// is taken from the catalog before anything starts, and passed to the
	// stream and to add-table alike.
	applyDDLMySQL(t, tgtDSN, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) NOT NULL, x INT NULL, v VARCHAR(20), "+
		"UNIQUE KEY uk (k, x));")
	accept := ukdAcceptValue(t, twfbDB{engine: "mysql", dsn: tgtDSN}, "kd", "uk")
	streamer := &Streamer{
		Source: mysqlEng, Target: mysqlEng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID,
		Filter:                     mustNewFilter(t, []string{"users"}, nil),
		AcceptTargetOnlyUniqueKeys: accept,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()
	if !waitForRowCountMySQL(t, tgtDSN, "users", 1, 60*time.Second) {
		t.Fatal("the cold start never delivered the seed row")
	}
	applyDDLMySQL(t, srcDSN, "INSERT INTO users VALUES (2, 'warmup@example.com');")
	if !waitForRowCountMySQL(t, tgtDSN, "users", 2, 60*time.Second) {
		t.Fatal("the stream never delivered the warmup row")
	}
	applyDDLMySQL(t, srcDSN, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) NOT NULL, x INT NULL, v VARCHAR(20)); "+
		"INSERT INTO kd VALUES (1,'a',1,'one'),(2,'b',1,'two');")
	actx, acancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer acancel()
	// The shape under test is a table added while the stream runs. --no-drain
	// on a binlog source refuses now (NO-DRAIN-BINLOG-SOURCE), so the state is
	// reached the one way any stream still can: a drained add the operator
	// ran without stopping the stream, which add-table does not detect
	// (DRAINED-ADD-TABLE-RUNNING-STREAM-UNCHECKED in docs/dev/audit-backlog.md).
	// --no-drain is withdrawn on Postgres too (Bug 332), so there is no live
	// add left to twin this with; when that door closes, this state is only
	// reachable from a pre-v0.163.9 binary's record.
	if err := (&AddTable{
		Source: mysqlEng, Target: mysqlEng, SourceDSN: srcDSN, TargetDSN: tgtDSN,
		StreamID: streamID, TableName: "kd", AcceptTargetOnlyUniqueKeys: accept,
	}).Run(actx); err != nil {
		t.Fatalf("add-table with the key accepted: %v", err)
	}
	src := twfbDB{engine: "mysql", dsn: srcDSN}
	tgt := twfbDB{engine: "mysql", dsn: tgtDSN}
	if !waitForRowCountMySQL(t, tgtDSN, "kd", 2, 60*time.Second) {
		t.Fatal("the add-table copy did not deliver both rows")
	}
	streamFailed := func(what string) {
		t.Helper()
		select {
		case err := <-runErr:
			t.Fatalf("%s: the stream exited: %v", what, err)
		default:
			t.Fatal(what)
		}
	}
	// Changes the stream reads before its poll observes the added table are
	// dropped (ADR-0034); wait for it, as the live add-table pins do.
	waitForLiveAddPollObservation(t)
	applyDDLMySQL(t, srcDSN, "ALTER TABLE kd ADD COLUMN p INT NULL; INSERT INTO kd (id,k,x,v) VALUES (3,'c',1,'three');")
	if !waitForRowCountMySQL(t, tgtDSN, "kd", 3, 60*time.Second) {
		streamFailed("the added table never streamed")
	}
	applyDDLMySQL(t, srcDSN, "ALTER TABLE kd DROP COLUMN x;")
	applyDDLMySQL(t, srcDSN, "INSERT INTO kd (id,k,v) VALUES (99,'a','ninetynine');")
	var err error
	select {
	case err = <-runErr:
	case <-time.After(90 * time.Second):
		t.Fatalf("nothing refused; target %s", ukdRows(t, tgt))
	}
	var refusal *ir.SourceUniqueKeyAbsentError
	if !errors.As(err, &refusal) || refusal.When != ir.UniqueKeyDoorAtSchemaChange || refusal.Keys[0].AcceptedFingerprint == "" {
		t.Fatalf("refused with %v; want the boundary SOURCE-UNIQUE-KEY-ABSENT naming the accepted fingerprint", err)
	}
	t.Logf("refusal: %v", err)
	if g := ukdRows(t, tgt); !strings.Contains(g, "1|a|one") || strings.Contains(g, "99|") {
		t.Fatalf("row 1 changed or the duplicate was applied: target %s (source %s)", g, ukdRows(t, src))
	}
}

func TestStreamer_UniqueKeyDoor_Review5SQLiteSourceFoldingTarget(t *testing.T) {
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()
	tgt := twfbDB{engine: "postgres", dsn: pgTarget}
	tgt.exec(t, "CREATE EXTENSION IF NOT EXISTS citext; "+
		"CREATE COLLATION IF NOT EXISTS ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)")
	srcEng, ok := engines.Get(sqlitetrigger.EngineName)
	if !ok {
		t.Fatal("sqlite-trigger engine not registered")
	}
	pgEng, _ := engines.Get("postgres")
	for i, c := range []struct {
		name, alter string // alter: the operator's change to the target's key column ("" keeps the default)
		refuse      bool
	}{
		{"default text column passes", "", false},
		{"citext column refuses", "ALTER TABLE kd ALTER COLUMN k TYPE citext", true},
		{"nondeterministic column refuses", `ALTER TABLE kd ALTER COLUMN k TYPE text COLLATE ci`, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			tgt.exec(t, "DROP TABLE IF EXISTS kd; DROP TABLE IF EXISTS sluice_cdc_state")
			path := filepath.Join(t.TempDir(), "app.db")
			for _, stmt := range []string{
				"PRAGMA journal_mode=WAL",
				// k is the PRIMARY KEY, so on the Postgres target it is the upsert's
				// ON CONFLICT key and 'A' would merge onto 'a' silently.
				"CREATE TABLE kd (id INTEGER NOT NULL, k TEXT NOT NULL PRIMARY KEY, v TEXT)",
				"INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')",
			} {
				sqliteExec(t, path, stmt)
			}
			if _, err := sqlitetrigger.Setup(context.Background(), path, sqlitetrigger.SetupOptions{Tables: []string{"kd"}}); err != nil {
				t.Fatalf("sqlitetrigger.Setup: %v", err)
			}
			newStreamer := func() *Streamer {
				return &Streamer{
					Source: srcEng, Target: pgEng, SourceDSN: path, TargetDSN: pgTarget,
					StreamID: "ukdr5s" + string(rune('a'+i)), Filter: mustNewFilter(t, nil, nil), ApplyRetryAttempts: 8,
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
			if c.alter != "" {
				tgt.exec(t, c.alter)
			}
			// 'A' is a distinct key on the source.
			sqliteExec(t, path, "INSERT INTO kd VALUES (3,'A','upper')")
			run = startTWFBRun(newStreamer())
			if !c.refuse {
				if !tgt.waitRow(t, "kd", 3, run, 90*time.Second) {
					t.Fatalf("the default column did not stream (%v)", run.stop(t))
				}
				if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
					t.Fatalf("the restart stopped with %v", err)
				}
				if s, g := ukdRowsSQLite(t, path), ukdRows(t, tgt); s != g {
					t.Fatalf("source %s, target %s", s, g)
				}
				return
			}
			deadline := time.Now().Add(120 * time.Second)
			for time.Now().Before(deadline) && !run.exited() {
				time.Sleep(300 * time.Millisecond)
			}
			var refusal *ir.SourceUniqueKeyAbsentError
			if !run.exited() || !errors.As(run.err, &refusal) || refusal.When != ir.UniqueKeyDoorAtOpen {
				_ = run.stop(t)
				t.Fatalf("refused with %v; want the at-open SOURCE-UNIQUE-KEY-ABSENT (target %s)", run.err, ukdRows(t, tgt))
			}
			t.Logf("refusal: %v", run.err)
			if g := ukdRows(t, tgt); g != "1|a|one,2|b|two" {
				t.Fatalf("the target changed: %s", g)
			}
		})
	}
}
