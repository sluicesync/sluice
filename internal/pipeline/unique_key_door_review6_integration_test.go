//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// UNIQUE-KEY-STOPPED-WINDOW-DOOR, round-6 review pins, on real servers and
// graded on the source's rows against the target's (ukdRows: id|k|v):
//
//   - MEDIUM-A: an acceptance carries the key's shape as a fingerprint in the
//     flag value. When the late boundary check fails after a forwarded DROP
//     COLUMN narrowed the accepted key, a restart refuses at open with the
//     old fingerprint (round 5 re-recorded the narrowed shape and passed),
//     again on the next restart, and re-accepting the printed new value
//     converges.
//   - MEDIUM-B: the fingerprint holds each part's full type and effective
//     collation, and a type-only boundary is judged on a table carrying an
//     accepted key: a forwarded MySQL `MODIFY k … COLLATE` refuses at the
//     change, and a Postgres target column made case-insensitive under a key
//     inheriting its collation refuses at the next start, both before
//     'A' merges onto 'a'.
//   - LOW-B: a Postgres array key column on a nondeterministic collation is
//     classified (the door stamps its element from the index's collation).
//
// Shard: TestStreamer_ prefix → pipeline-rest-streamer.

package pipeline

import (
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// ukdR6NarrowingCell builds the round-5 narrowing shape: a PG keyless
// source into MySQL, the operator's UNIQUE (k, x) accepted by its printed
// value, then a forwarded ADD COLUMN so the DROP of x is a CDC→CDC
// boundary MySQL applies, narrowing the key to (k).
func ukdR6NarrowingCell(t *testing.T, tag string) (src, tgt twfbDB, cell twfbCell, accept []string, newStreamer func([]string) *Streamer, cleanup func()) {
	t.Helper()
	pgNew, pgCleanup := ukcPGServer(t)
	myNew, myCleanup := ukcMySQLServer(t)
	src, tgt = pgNew(tag+"_s"), myNew(tag+"_t")
	src.exec(t, "CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, x int NOT NULL DEFAULT 1, v varchar(20)); "+
		"ALTER TABLE kd REPLICA IDENTITY FULL; INSERT INTO kd VALUES (1,'a',1,'one'),(2,'b',1,'two')")
	cell = twfbCell{src: src, tgt: tgt, streamID: tag, slotName: tag}
	ukdColdStartStop(t, cell, cell.streamer)
	tgt.exec(t, "ALTER TABLE kd ADD UNIQUE KEY uk (k, x)")
	accept = ukdAcceptValue(t, tgt, "kd", "uk")
	newStreamer = func(values []string) *Streamer {
		s := cell.streamer()
		s.AcceptTargetOnlyUniqueKeys = values
		s.SuppressAddedColumnBackfill = true // a keyless table cannot be backfilled by cursor
		return s
	}
	return src, tgt, cell, accept, newStreamer, func() { myCleanup(); pgCleanup() }
}

func TestStreamer_UniqueKeyDoor_Review6LateCheckFailsThenRestart(t *testing.T) {
	src, tgt, _, accept, newStreamer, cleanup := ukdR6NarrowingCell(t, "ukdr6a")
	defer cleanup()
	s := newStreamer(accept)
	// The late check of the DROP boundary fails, as a transient catalog
	// read would; the forward has already narrowed uk on the target.
	s.uniqueKeyBoundaryFault = func(snap ir.SchemaSnapshot) error {
		if snap.IR != nil && columnNamed(snap.IR, "x") == nil {
			return errors.New("injected: catalog read failed")
		}
		return nil
	}
	run := startTWFBRun(s)
	src.exec(t, "ALTER TABLE kd ADD COLUMN p int; INSERT INTO kd (id,k,x,v) VALUES (3,'c',1,'three')")
	if !tgt.waitRow(t, "kd", 3, run, 90*time.Second) {
		t.Fatalf("the accepted stream did not run (%v)", run.stop(t))
	}
	src.exec(t, "ALTER TABLE kd DROP COLUMN x")
	src.exec(t, "INSERT INTO kd (id,k,v) VALUES (4,'d','four')")
	ukdAwaitExitOrConverge(t, run, src, tgt, 90*time.Second)
	if !run.exited() || run.err == nil || !strings.Contains(run.err.Error(), SourceUniqueKeyRecheckFailedMarker) {
		_ = run.stop(t)
		t.Fatalf("the injected late-check failure did not stop the stream: %v", run.err)
	}
	if now := ukdAcceptValue(t, tgt, "kd", "uk"); len(now) != 1 || now[0] == accept[0] {
		t.Fatalf("the premise moved: the forward did not narrow uk before the failed check (%v vs %v)", now, accept)
	}
	// The duplicate a narrowed (k) key would merge onto row 1.
	src.exec(t, "INSERT INTO kd (id,k,v) VALUES (99,'a','ninetynine')")
	for i := 0; i < 2; i++ { // a restart, and the next, both with the old value
		run = startTWFBRun(newStreamer(accept))
		ukdAwaitExitOrConverge(t, run, src, tgt, 120*time.Second)
		refusal := ukdR3RequireNoMergeRefusal(t, run, src, tgt, ir.UniqueKeyDoorAtOpen)
		if refusal.Keys[0].AcceptedFingerprint == "" {
			t.Fatalf("restart %d: the refusal does not name the stale fingerprint: %v", i+1, run.err)
		}
		if g := ukdRows(t, tgt); strings.Contains(g, "99|") {
			t.Fatalf("restart %d: the duplicate was applied: %s", i+1, g)
		}
	}
	// The printed remedy for a key that should not be narrower: drop it.
	tgt.exec(t, "ALTER TABLE kd DROP INDEX uk")
	ukdConvergeAndResume(t, newStreamer(accept), src, tgt, "INSERT INTO kd (id,k,v) VALUES (8,'r','eight')")
}

// TestStreamer_UniqueKeyDoor_Review6ReacceptNewFingerprint pins the other
// remedy: the operator, having checked the narrowed key, passes the value
// the refusal prints, and the stream converges.
func TestStreamer_UniqueKeyDoor_Review6ReacceptNewFingerprint(t *testing.T) {
	src, tgt, _, accept, newStreamer, cleanup := ukdR6NarrowingCell(t, "ukdr6b")
	defer cleanup()
	run := startTWFBRun(newStreamer(accept))
	src.exec(t, "ALTER TABLE kd ADD COLUMN p int; INSERT INTO kd (id,k,x,v) VALUES (3,'c',1,'three')")
	if !tgt.waitRow(t, "kd", 3, run, 90*time.Second) {
		t.Fatalf("the accepted stream did not run (%v)", run.stop(t))
	}
	src.exec(t, "ALTER TABLE kd DROP COLUMN x")
	src.exec(t, "INSERT INTO kd (id,k,v) VALUES (4,'d','four')")
	ukdAwaitExitOrConverge(t, run, src, tgt, 90*time.Second)
	ukdR3RequireNoMergeRefusal(t, run, src, tgt, ir.UniqueKeyDoorAtSchemaChange)
	value := ukdAcceptFromRefusal(t, run.err, "uk")
	if value == accept[0] {
		t.Fatalf("the refusal printed the old value %s", value)
	}
	ukdConvergeAndResume(t, newStreamer([]string{value}), src, tgt, "INSERT INTO kd (id,k,v) VALUES (9,'z','nine')")
}

func TestStreamer_UniqueKeyDoor_Review6AcceptedKeyCollationLoosened(t *testing.T) {
	t.Run("mysql MODIFY COLLATE", func(t *testing.T) {
		myNew, myCleanup := ukcMySQLServer(t)
		defer myCleanup()
		src, tgt := myNew("ukdr6c_s"), myNew("ukdr6c_t")
		src.exec(t, "CREATE TABLE kd (id INT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, v VARCHAR(20)); "+
			"INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')")
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdr6c"}
		ukdColdStartStop(t, cell, cell.streamer)
		tgt.exec(t, "ALTER TABLE kd ADD UNIQUE KEY uk (k)")
		ukdR6LoosenAndRefuse(t, cell, ukdAcceptValue(t, tgt, "kd", "uk"),
			"ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL",
			"ALTER TABLE kd DROP INDEX uk")
	})
	t.Run("postgres column collation under an inheriting key", func(t *testing.T) {
		pgNew, pgCleanup := ukcPGServer(t)
		defer pgCleanup()
		const ci = "CREATE COLLATION IF NOT EXISTS ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false); "
		src, tgt := pgNew("ukdr6d_s"), pgNew("ukdr6d_t")
		src.exec(t, ci+"CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, v varchar(20)); "+
			"ALTER TABLE kd REPLICA IDENTITY FULL; INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')")
		tgt.exec(t, ci)
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdr6d", slotName: "ukdr6d"}
		ukdColdStartStop(t, cell, cell.streamer)
		// The key inherits k's collation: no index-level COLLATE.
		tgt.exec(t, "CREATE UNIQUE INDEX uk ON kd (k)")
		accept := ukdAcceptValue(t, tgt, "kd", "uk")
		newStreamer := func() *Streamer {
			s := cell.streamer()
			s.AcceptTargetOnlyUniqueKeys = accept
			return s
		}
		ukdColdStartStop(t, cell, newStreamer) // accepted: runs
		// A Postgres source's collation change is not forwarded (the target
		// stays the stricter side, which merges nothing); the inherited shape
		// moves when the TARGET column's collation does. While the stream is
		// stopped the operator makes it case-insensitive, and the source
		// writes 'A' beside 'a'.
		tgt.exec(t, "ALTER TABLE kd ALTER COLUMN k TYPE varchar(20) COLLATE ci")
		src.exec(t, "INSERT INTO kd VALUES (4,'A','upper')")
		run := startTWFBRun(newStreamer())
		ukdAwaitExitOrConverge(t, run, src, tgt, 120*time.Second)
		refusal := ukdR3RequireNoMergeRefusal(t, run, src, tgt, ir.UniqueKeyDoorAtOpen)
		if refusal.Keys[0].AcceptedFingerprint == "" || !strings.Contains(refusal.Keys[0].Shape, "collate ci") {
			t.Fatalf("the refusal does not name the stale fingerprint and the inherited collation: %v", run.err)
		}
		if g := ukdRows(t, tgt); strings.Contains(g, "4|") {
			t.Fatalf("'A' was applied under the loosened key: %s", g)
		}
		tgt.exec(t, "DROP INDEX uk")
		ukdConvergeAndResume(t, newStreamer(), src, tgt, "INSERT INTO kd VALUES (8,'r','eight')")
	})
}

// ukdR6LoosenAndRefuse runs the accepted stream through a forwarded ADD
// COLUMN (so the next boundary is CDC→CDC) and then a collation change on
// the accepted key's column with a row 'A' beside 'a': the type-only
// boundary must refuse before 'A' merges, and dropping the key converges.
func ukdR6LoosenAndRefuse(t *testing.T, cell twfbCell, accept []string, loosen, dropKey string) {
	t.Helper()
	src, tgt := cell.src, cell.tgt
	newStreamer := func() *Streamer {
		s := cell.streamer()
		s.AcceptTargetOnlyUniqueKeys = accept
		s.SuppressAddedColumnBackfill = true
		return s
	}
	run := startTWFBRun(newStreamer())
	src.exec(t, "ALTER TABLE kd ADD COLUMN p int; INSERT INTO kd (id,k,v) VALUES (3,'c','three')")
	if !tgt.waitRow(t, "kd", 3, run, 90*time.Second) {
		t.Fatalf("the accepted stream did not run (%v)", run.stop(t))
	}
	src.exec(t, loosen)
	src.exec(t, "INSERT INTO kd (id,k,v) VALUES (4,'A','upper')")
	ukdAwaitExitOrConverge(t, run, src, tgt, 90*time.Second)
	refusal := ukdR3RequireNoMergeRefusal(t, run, src, tgt, ir.UniqueKeyDoorAtSchemaChange)
	if refusal.Keys[0].AcceptedFingerprint == "" {
		t.Fatalf("the refusal does not name the stale fingerprint: %v", run.err)
	}
	if g := ukdRows(t, tgt); strings.Contains(g, "4|") {
		t.Fatalf("'A' was applied under the loosened key: %s", g)
	}
	tgt.exec(t, dropKey)
	ukdConvergeAndResume(t, newStreamer(), src, tgt, "INSERT INTO kd (id,k,v) VALUES (8,'r','eight')")
}

// TestStreamer_UniqueKeyDoor_Review6ArrayKeyCollation pins LOW-B: a
// pre-created Postgres target whose array key column is on a
// nondeterministic collation, against a deterministic source one, refuses at
// the cold start's open (it used to pass: the schema read leaves an array
// element's collation unclassified).
func TestStreamer_UniqueKeyDoor_Review6ArrayKeyCollation(t *testing.T) {
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()
	const ci = "CREATE COLLATION IF NOT EXISTS ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false); "
	src, tgt := pgNew("ukdr6e_s"), pgNew("ukdr6e_t")
	src.exec(t, "CREATE TABLE kd (id int NOT NULL, k varchar(20), v varchar(20), tags text[] NOT NULL); "+
		"CREATE UNIQUE INDEX kd_tags ON kd (tags); ALTER TABLE kd REPLICA IDENTITY FULL; "+
		"INSERT INTO kd VALUES (1,'a','one','{a}'),(2,'b','two','{b}')")
	tgt.exec(t, ci+"CREATE TABLE kd (id int NOT NULL, k varchar(20), v varchar(20), tags text[] COLLATE ci NOT NULL); "+
		"CREATE UNIQUE INDEX kd_tags ON kd (tags)")
	msg := ukdColdOpenExpect(t, twfbCell{src: src, tgt: tgt, streamID: "ukdr6e", slotName: "ukdr6e"}, "kd.kd_tags", "")
	if !strings.Contains(msg, "the target's tags compares case-") {
		t.Errorf("the refusal does not name the looser array column: %s", msg)
	}
}
