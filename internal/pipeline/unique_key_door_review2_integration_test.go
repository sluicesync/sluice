//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// UNIQUE-KEY-STOPPED-WINDOW-DOOR, round-2 review pins, on real servers and
// graded on the source's rows against the target's (ukdRows: id|k|v):
//
//   - HIGH-1: a pgoutput DROP COLUMN made before the table's FIRST change of
//     the session arrives as that table's first schema snapshot; the
//     boundary check must judge it against the open door's read rather
//     than adopt it as a baseline. No priming row, b DEFAULT 1 (the
//     reviewer's probe). It is also the first boundary after the cold
//     start, which schema-forward does not forward (ADR-0091 §3), so into
//     MySQL and into Postgres alike the target keeps b and its key on
//     (k, b); both must refuse before row 99 merges onto row 1, and the
//     printed drop-the-key remedy must converge;
//   - L-a: a PARTITIONED Postgres target's NULLS NOT DISTINCT unique index,
//     which the schema read's index read (ordinary tables only) does not
//     list, refuses at open against a source's plain unique key, and the
//     same partitioned target with a plain unique index converges;
//   - MEDIUM-2: key-column collation direction at a cold start's open — a
//     Postgres deterministic key against a pre-created nondeterministic
//     target column refuses; a MySQL 8 NO PAD CHAR key into a Postgres
//     bpchar (both ignore trailing spaces, premise checked on the source)
//     converges; and a same-family MySQL mismatch is refused first by the
//     pre-create shape gate, the premise that keeps the arm's reach on a
//     binlog source to Postgres-family targets. The VStream half is in
//     unique_key_door_vstream_integration_test.go.
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

// ukdColdOpenExpect cold-starts the cell over a pre-created target and
// requires either the door's at-open refusal naming key (no row merged:
// the target holds no row the source lacks) or, with key == "", a run that
// converges and carries one more row.
func ukdColdOpenExpect(t *testing.T, cell twfbCell, key, nextRow string) (refusal string) {
	t.Helper()
	if key == "" {
		ukdConvergeAndResume(t, cell.streamer(), cell.src, cell.tgt, nextRow)
		return ""
	}
	run := startTWFBRun(cell.streamer())
	deadline := time.Now().Add(180 * time.Second)
	for time.Now().Before(deadline) && !run.exited() {
		time.Sleep(300 * time.Millisecond)
	}
	if !run.exited() {
		_ = run.stop(t)
		t.Fatalf("the stream did not refuse; target %s", ukdRows(t, cell.tgt))
	}
	var door *ir.SourceUniqueKeyAbsentError
	if !errors.As(run.err, &door) || door.When != ir.UniqueKeyDoorAtOpen || !strings.Contains(run.err.Error(), key) {
		t.Fatalf("refused with %v; want the at-open SOURCE-UNIQUE-KEY-ABSENT naming %s", run.err, key)
	}
	t.Logf("refusal: %v", run.err)
	src := ukdRows(t, cell.src)
	for _, row := range strings.Split(ukdRows(t, cell.tgt), ",") {
		if row != "" && !strings.Contains(","+src+",", ","+row+",") {
			t.Fatalf("the target holds %q, which the source does not (source %s)", row, src)
		}
	}
	return run.err.Error()
}

func TestStreamer_UniqueKeyDoor_Review2FirstTouchDropColumn(t *testing.T) {
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()
	myNew, myCleanup := ukcMySQLServer(t)
	defer myCleanup()
	const create = "CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, v varchar(20), b int NOT NULL DEFAULT 1); " +
		"CREATE UNIQUE INDEX kd_kb ON kd (k, b); ALTER TABLE kd REPLICA IDENTITY FULL; " +
		"INSERT INTO kd VALUES (1,'a','one',1),(2,'b','two',1)"
	start := func(t *testing.T, cell twfbCell) *twfbRun {
		t.Helper()
		cell.src.exec(t, create)
		run := startTWFBRun(cell.streamer())
		if !cell.tgt.waitRow(t, "kd", 2, run, 180*time.Second) {
			t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
		}
		cell.waitStreaming(t, run)
		// No priming change: the DROP COLUMN is the table's first touch of
		// the session, so its snapshot is the table's first.
		cell.src.exec(t, "ALTER TABLE kd DROP COLUMN b")
		cell.src.exec(t, "INSERT INTO kd VALUES (99,'a','ninetynine')")
		return run
	}

	// Into either target the DROP COLUMN is the first boundary after the
	// cold start, which schema-forward does NOT forward (ADR-0091 §3), so
	// the target keeps b — and the key on (k, b), filled by b's DEFAULT 1 —
	// while the source dropped the index with the column. Row 99 would take
	// b = 1 and upsert onto row 1 on both: MySQL resolves through any unique
	// key, and the Postgres keyless table's ON CONFLICT key is that one.
	cells := []struct {
		name, tag string
		target    func(string) twfbDB
		dropKey   string // the printed remedy: drop the key on the target
	}{
		{"into MySQL", "ukdfta", myNew, "ALTER TABLE kd DROP INDEX kd_kb"},
		{"into Postgres", "ukdftb", pgNew, "ALTER TABLE kd DROP CONSTRAINT kd_kb"},
	}
	for _, c := range cells {
		t.Run(c.name+": refused before row 99 merges onto row 1", func(t *testing.T) {
			src, tgt := pgNew(c.tag+"_s"), c.target(c.tag+"_t")
			cell := twfbCell{src: src, tgt: tgt, streamID: c.tag, slotName: c.tag}
			run := start(t, cell)
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
			if g := ukdRows(t, tgt); strings.Contains(g, "99|") || !strings.Contains(g, "1|a|one") {
				t.Fatalf("row 99 merged or row 1 changed: target %s", g)
			}
			if !strings.Contains(run.err.Error(), "not forwarded") {
				t.Errorf("the refusal does not say the first boundary's DROP COLUMN was not forwarded: %v", run.err)
			}
			// The printed remedy for a change this release was streaming
			// across: drop the key on the target, start again.
			tgt.exec(t, c.dropKey)
			ukdConvergeAndResume(t, cell.streamer(), src, tgt, "INSERT INTO kd VALUES (8,'r','eight')")
		})
	}
}

func TestStreamer_UniqueKeyDoor_Review2PartitionedTarget(t *testing.T) {
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()
	cells := []struct {
		name, tag, index, key string
	}{
		{"NULLS NOT DISTINCT refuses", "ukdpta", "CREATE UNIQUE INDEX kd_k ON kd (k) NULLS NOT DISTINCT", "kd.kd_k"},
		{"plain unique converges", "ukdptb", "CREATE UNIQUE INDEX kd_k ON kd (k)", ""},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			src, tgt := pgNew(c.tag+"_s"), pgNew(c.tag+"_t")
			src.exec(t, "CREATE TABLE kd (id int NOT NULL, k varchar(20), v varchar(20)); "+
				"CREATE UNIQUE INDEX kd_k ON kd (k); ALTER TABLE kd REPLICA IDENTITY FULL; "+
				"INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')")
			tgt.exec(t, "CREATE TABLE kd (id int NOT NULL, k varchar(20), v varchar(20)) PARTITION BY HASH (k); "+
				"CREATE TABLE kd_p0 PARTITION OF kd FOR VALUES WITH (MODULUS 2, REMAINDER 0); "+
				"CREATE TABLE kd_p1 PARTITION OF kd FOR VALUES WITH (MODULUS 2, REMAINDER 1); "+c.index)
			cell := twfbCell{src: src, tgt: tgt, streamID: c.tag, slotName: c.tag}
			// The source's plain key allows two NULL k rows; the target's
			// NULLS NOT DISTINCT key would merge them.
			ukdColdOpenExpect(t, cell, c.key, "INSERT INTO kd VALUES (8,NULL,'eight'),(9,NULL,'nine')")
		})
	}
}

func TestStreamer_UniqueKeyDoor_Review2ColumnCollation(t *testing.T) {
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()
	myNew, myCleanup := ukcMySQLServer(t)
	defer myCleanup()
	const ci = "CREATE COLLATION IF NOT EXISTS ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false); "
	myTable := func(coll string) string {
		return "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE " + coll +
			" NOT NULL, v VARCHAR(20), UNIQUE KEY kd_k (k))"
	}

	// A same-family MySQL target pre-created under another collation never
	// reaches the door's cold-open arm: the pre-create shape gate (ADR-0166)
	// compares the collation and refuses before any data moves, in either
	// direction. Pinned so the claim that the arm is reachable on a
	// Postgres-family target only (on a binlog source) stays checked.
	t.Run("mysql _bin key against a pre-created _ci target column: the shape gate refuses first", func(t *testing.T) {
		src, tgt := myNew("ukdcca_s"), myNew("ukdcca_t")
		src.exec(t, myTable("utf8mb4_0900_bin")+"; INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')")
		tgt.exec(t, myTable("utf8mb4_0900_ai_ci"))
		run := startTWFBRun(twfbCell{src: src, tgt: tgt, streamID: "ukdcca"}.streamer())
		deadline := time.Now().Add(180 * time.Second)
		for time.Now().Before(deadline) && !run.exited() {
			time.Sleep(300 * time.Millisecond)
		}
		if !run.exited() || run.err == nil || !strings.Contains(run.err.Error(), "refusing before any data moves") {
			_ = run.stop(t)
			t.Fatalf("got %v; want the pre-create shape gate's refusal", run.err)
		}
		if g := ukdRows(t, tgt); g != "" {
			t.Fatalf("rows moved: target %s", g)
		}
	})
	t.Run("mysql NO PAD CHAR key into a Postgres bpchar converges", func(t *testing.T) {
		src, tgt := myNew("ukdccd_s"), pgNew("ukdccd_t")
		src.exec(t, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k CHAR(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL, "+
			"v VARCHAR(20), UNIQUE KEY kd_k (k)); INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')")
		// The premise the resolver's CHAR rule rests on: MySQL stores CHAR
		// space-padded, so even under a NO PAD collation 'a ' is 'a'.
		ukdExecExpectError(t, src, "INSERT INTO kd VALUES (3,'a ','three')")
		ukdColdOpenExpect(t, twfbCell{src: src, tgt: tgt, streamID: "ukdccd"}, "", "INSERT INTO kd VALUES (9,'z','nine')")
	})
	t.Run("postgres deterministic key against a pre-created nondeterministic target column refuses", func(t *testing.T) {
		src, tgt := pgNew("ukdcce_s"), pgNew("ukdcce_t")
		src.exec(t, "CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, v varchar(20)); "+
			"CREATE UNIQUE INDEX kd_k ON kd (k); ALTER TABLE kd REPLICA IDENTITY FULL; "+
			"INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')")
		tgt.exec(t, ci+"CREATE TABLE kd (id int NOT NULL, k varchar(20) COLLATE ci NOT NULL, v varchar(20)); "+
			"CREATE UNIQUE INDEX kd_k ON kd (k)")
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdcce", slotName: "ukdcce"}
		msg := ukdColdOpenExpect(t, cell, "kd.kd_k", "")
		if !strings.Contains(msg, "the target's k compares case-, accent- or trailing-space-insensitively") {
			t.Errorf("the refusal does not name the looser column: %s", msg)
		}
		// The printed remedy this refusal adds: ALTER the target column to a
		// collation as strict as the source's, then start again.
		tgt.exec(t, `ALTER TABLE kd ALTER COLUMN k TYPE varchar(20) COLLATE "default"`)
		ukdConvergeAndResume(t, cell.streamer(), src, tgt, "INSERT INTO kd VALUES (9,'A','upper')")
	})
}
