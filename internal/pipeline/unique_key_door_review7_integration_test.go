//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// UNIQUE-KEY-STOPPED-WINDOW-DOOR, round-7 review pins, on real servers and
// graded on the source's rows against the target's (ukdRows: id|k|v):
//
//   - MEDIUM-1: the acceptance fingerprint hashes the target engine's own
//     definition of the key and of every column it depends on, so each
//     dimension the round-6 hand-built shape omitted now changes it: an
//     accepted `text[]` key whose column goes nondeterministic while the
//     stream is stopped (the shape read no array element collation), an
//     accepted expression key `((lower(k)))` whose column's collation loosens
//     (the shape skipped an expression part's collation), on Postgres and on
//     a MySQL functional key, and a partial key re-created with a predicate
//     literal differing in case and a trailing space (the shape folded case
//     and whitespace inside literals). Each restart refuses at open with row
//     1 intact and the duplicate unapplied.
//   - MEDIUM-2: a `--target-schema` stream's refusal prints `tgt.kd.uk=…`;
//     pasted as printed, it arms the boundary gate's type-only judging, so a
//     forwarded `ALTER COLUMN k TYPE char(n)` under the accepted key refuses
//     at the change instead of merging 'a ' onto 'a'.
//   - MEDIUM-3: add-table judges collation direction on a source copy with
//     the unique-key facts folded in, so a pre-created target `text[]
//     COLLATE <nondeterministic>` key against a deterministic source key
//     refuses, naming the column.
//
// Shard: TestStreamer_ prefix → pipeline-rest-streamer.

package pipeline

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

const (
	ukdR7CI = "CREATE COLLATION IF NOT EXISTS ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false); "
	ukdR7AI = "CREATE COLLATION IF NOT EXISTS ai (provider = icu, locale = 'und-u-ks-level1', deterministic = false); "
)

// ukdR7StoppedChange accepts the target's key uk by the value the catalog
// gives, runs the accepted stream once (it passes), then, while it is
// stopped, makes change on the target and writes dup on the source. The
// restart must refuse at open, naming the accepted fingerprint beside a new
// one, with row 1 intact and dup unapplied; dropping the key converges.
func ukdR7StoppedChange(t *testing.T, cell twfbCell, key string, change func(), dup, dupID, dropKey, next string) {
	t.Helper()
	src, tgt := cell.src, cell.tgt
	accept := ukdAcceptValue(t, tgt, "kd", key)
	if len(accept) != 1 {
		t.Fatalf("no key %s on the target", key)
	}
	newStreamer := func() *Streamer {
		s := cell.streamer()
		s.AcceptTargetOnlyUniqueKeys = accept
		return s
	}
	ukdColdStartStop(t, cell, newStreamer) // accepted: runs
	change()
	if now := ukdAcceptValue(t, tgt, "kd", key); len(now) != 1 || now[0] == accept[0] {
		t.Fatalf("the premise moved: the change did not alter the key's definition (%v vs %v)", now, accept)
	}
	src.exec(t, dup)
	run := startTWFBRun(newStreamer())
	ukdAwaitExitOrConverge(t, run, src, tgt, 120*time.Second)
	refusal := ukdR3RequireNoMergeRefusal(t, run, src, tgt, ir.UniqueKeyDoorAtOpen)
	k := refusal.Keys[0]
	if k.AcceptedFingerprint != strings.SplitN(accept[0], "=", 2)[1] || k.Fingerprint == "" || k.Fingerprint == k.AcceptedFingerprint {
		t.Fatalf("the refusal does not name the stale fingerprint beside a new one: %v", run.err)
	}
	if g := ukdRows(t, tgt); strings.Contains(","+g, ","+dupID+"|") {
		t.Fatalf("the duplicate was applied under the changed key: %s", g)
	}
	tgt.exec(t, dropKey)
	ukdConvergeAndResume(t, newStreamer(), src, tgt, next)
}

// ukdR7Next is the converging run's next row for a kd(id, k, v) table.
const ukdR7Next = "INSERT INTO kd (id, k, v) VALUES (8, 'r', 'eight')"

func TestStreamer_UniqueKeyDoor_Review7ArrayKeyStoppedWindow(t *testing.T) {
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()
	src, tgt := pgNew("ukdr7a_s"), pgNew("ukdr7a_t")
	src.exec(t, "CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, v varchar(20), tags text[] NOT NULL); "+
		"ALTER TABLE kd REPLICA IDENTITY FULL; INSERT INTO kd VALUES (1,'a','one','{a}'),(2,'b','two','{b}')")
	tgt.exec(t, ukdR7CI)
	cell := twfbCell{src: src, tgt: tgt, streamID: "ukdr7a", slotName: "ukdr7a"}
	ukdColdStartStop(t, cell, cell.streamer)
	tgt.exec(t, "CREATE UNIQUE INDEX kd_tags ON kd (tags)")
	ukdR7StoppedChange(t, cell, "kd_tags",
		func() { tgt.exec(t, "ALTER TABLE kd ALTER COLUMN tags TYPE text[] COLLATE ci") },
		"INSERT INTO kd VALUES (4,'d','four','{A}')", "4", "DROP INDEX kd_tags",
		"INSERT INTO kd VALUES (8,'r','eight','{r}')")
}

func TestStreamer_UniqueKeyDoor_Review7ExpressionKeyStoppedWindow(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		pgNew, pgCleanup := ukcPGServer(t)
		defer pgCleanup()
		src, tgt := pgNew("ukdr7b_s"), pgNew("ukdr7b_t")
		src.exec(t, "CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, v varchar(20)); "+
			"ALTER TABLE kd REPLICA IDENTITY FULL; INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')")
		tgt.exec(t, ukdR7AI)
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdr7b", slotName: "ukdr7b"}
		ukdColdStartStop(t, cell, cell.streamer)
		tgt.exec(t, "CREATE UNIQUE INDEX uk ON kd ((lower(k)))")
		// lower() folds case, not accents: 'á' stays apart from 'a' until the
		// column's collation ignores accents, which the expression inherits.
		ukdR7StoppedChange(t, cell, "uk",
			func() { tgt.exec(t, "ALTER TABLE kd ALTER COLUMN k TYPE varchar(20) COLLATE ai") },
			"INSERT INTO kd VALUES (4,'á','four')", "4", "DROP INDEX uk", ukdR7Next)
	})
	t.Run("mysql functional", func(t *testing.T) {
		myNew, myCleanup := ukcMySQLServer(t)
		defer myCleanup()
		src, tgt := myNew("ukdr7c_s"), myNew("ukdr7c_t")
		src.exec(t, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, "+
			"k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, v VARCHAR(20)); "+
			"INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')")
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdr7c"}
		ukdColdStartStop(t, cell, cell.streamer)
		tgt.exec(t, "ALTER TABLE kd ADD UNIQUE KEY uk ((lower(k)))")
		ukdR7StoppedChange(t, cell, "uk",
			func() {
				tgt.exec(t, "ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL")
			},
			"INSERT INTO kd VALUES (4,'á','four')", "4", "ALTER TABLE kd DROP INDEX uk", ukdR7Next)
	})
}

func TestStreamer_UniqueKeyDoor_Review7PredicateLiteralCase(t *testing.T) {
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()
	src, tgt := pgNew("ukdr7d_s"), pgNew("ukdr7d_t")
	src.exec(t, "CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, v varchar(20), status varchar(10) NOT NULL); "+
		"ALTER TABLE kd REPLICA IDENTITY FULL; INSERT INTO kd VALUES (1,'a','one','active '),(2,'b','two','x')")
	cell := twfbCell{src: src, tgt: tgt, streamID: "ukdr7d", slotName: "ukdr7d"}
	ukdColdStartStop(t, cell, cell.streamer)
	tgt.exec(t, "CREATE UNIQUE INDEX uk ON kd (k) WHERE status = 'Active'")
	// Re-created under its name, its predicate literal differing only in
	// case and a trailing space: rows with status 'active ' are now keyed.
	ukdR7StoppedChange(t, cell, "uk",
		func() { tgt.exec(t, "DROP INDEX uk; CREATE UNIQUE INDEX uk ON kd (k) WHERE status = 'active '") },
		"INSERT INTO kd VALUES (4,'a','four','active ')", "4", "DROP INDEX uk",
		"INSERT INTO kd VALUES (8,'r','eight','x')")
}

// ukdR7WithSearchPath is db with its session search_path set, so the
// shared helpers' unqualified `kd` reads find a --target-schema table.
func ukdR7WithSearchPath(t *testing.T, db twfbDB, path string) twfbDB {
	t.Helper()
	u, err := url.Parse(db.dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", path)
	u.RawQuery = q.Encode()
	return twfbDB{engine: db.engine, dsn: u.String()}
}

func TestStreamer_UniqueKeyDoor_Review7TargetSchemaAcceptance(t *testing.T) {
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()
	src, tgtRaw := pgNew("ukdr7e_s"), pgNew("ukdr7e_t")
	tgtRaw.exec(t, "CREATE SCHEMA tgt")
	tgt := ukdR7WithSearchPath(t, tgtRaw, "tgt,public")
	src.exec(t, "CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, v varchar(20)); "+
		"ALTER TABLE kd REPLICA IDENTITY FULL; INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')")
	cell := twfbCell{src: src, tgt: tgt, streamID: "ukdr7e", slotName: "ukdr7e"}
	newStreamer := func(accept []string) *Streamer {
		s := cell.streamer()
		s.TargetSchema = "tgt"
		s.AcceptTargetOnlyUniqueKeys = accept
		s.SuppressAddedColumnBackfill = true // a keyless table cannot be backfilled by cursor
		return s
	}
	ukdColdStartStop(t, cell, func() *Streamer { return newStreamer(nil) })
	tgt.exec(t, "CREATE UNIQUE INDEX uk ON tgt.kd (k)")
	src.exec(t, "INSERT INTO kd VALUES (5,'e','five')") // held back by the refusal
	run := startTWFBRun(newStreamer(nil))
	ukdAwaitExitOrConverge(t, run, src, tgt, 120*time.Second)
	ukdR3RequireNoMergeRefusal(t, run, src, tgt, ir.UniqueKeyDoorAtOpen)
	value := ukdAcceptFromRefusal(t, run.err, "uk")
	if !strings.HasPrefix(value, "tgt.kd.uk=") {
		t.Fatalf("the refusal printed %s; want the target-schema-qualified tgt.kd.uk=…", value)
	}

	run = startTWFBRun(newStreamer([]string{value}))
	src.exec(t, "ALTER TABLE kd ADD COLUMN p int; INSERT INTO kd (id,k,v) VALUES (3,'c','three')")
	if !tgt.waitRow(t, "kd", 3, run, 90*time.Second) {
		t.Fatalf("the accepted stream did not run (%v)", run.stop(t))
	}
	// char(n) ignores trailing spaces: 'a ' and 'a' are one value under the
	// forwarded type, which the accepted varchar key kept apart.
	src.exec(t, "ALTER TABLE kd ALTER COLUMN k TYPE char(5)")
	src.exec(t, "INSERT INTO kd (id,k,v) VALUES (4,'a ','four')")
	ukdAwaitExitOrConverge(t, run, src, tgt, 90*time.Second)
	refusal := ukdR3RequireNoMergeRefusal(t, run, src, tgt, ir.UniqueKeyDoorAtSchemaChange)
	if refusal.Keys[0].AcceptedFingerprint != strings.TrimPrefix(value, "tgt.kd.uk=") {
		t.Fatalf("the refusal does not name the accepted fingerprint: %v", run.err)
	}
	if g := ukdRows(t, tgt); strings.Contains(","+g, ",4|") {
		t.Fatalf("'a ' was applied under the loosened key: %s", g)
	}
	tgt.exec(t, "DROP INDEX tgt.uk")
	ukdConvergeAndResume(t, newStreamer([]string{value}), src, tgt, "INSERT INTO kd (id,k,v) VALUES (8,'r','eight')")
}

func TestStreamer_UniqueKeyDoor_Review7AddTableArrayCollation(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	applyDDL(t, sourceDSN, "CREATE TABLE customers (id bigint PRIMARY KEY, email varchar(255) NOT NULL); "+
		"INSERT INTO customers VALUES (1, 'alice@example.com');")
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	const streamID = "ukd-r7-add"
	streamer := &Streamer{Source: pgEng, Target: pgEng, SourceDSN: sourceDSN, TargetDSN: targetDSN, StreamID: streamID}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()
	if !waitForRowCount(t, targetDSN, "customers", 1, 60*time.Second) {
		t.Fatal("the cold start never delivered the seed row")
	}
	applyDDL(t, sourceDSN, "INSERT INTO customers VALUES (2, 'bob@example.com');")
	if !waitForRowCount(t, targetDSN, "customers", 2, 60*time.Second) {
		t.Fatal("the stream never delivered the warmup row")
	}

	// The source keys tags under its deterministic default collation; the
	// operator's pre-created target compares them case-insensitively.
	applyDDL(t, sourceDSN, "CREATE TABLE orders (id bigint PRIMARY KEY, tags text[] NOT NULL, "+
		"CONSTRAINT orders_tags_key UNIQUE (tags)); INSERT INTO orders VALUES (1, '{a}'), (2, '{A}');")
	applyDDL(t, targetDSN, ukdR7CI+"CREATE TABLE orders (id bigint PRIMARY KEY, tags text[] COLLATE ci NOT NULL); "+
		"CREATE UNIQUE INDEX o_tags ON orders (tags);")
	actx, acancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer acancel()
	err := (&AddTable{
		Source: pgEng, Target: pgEng, SourceDSN: sourceDSN, TargetDSN: targetDSN,
		StreamID: streamID, TableName: "orders", LiveMode: true,
	}).Run(actx)
	var refusal *ir.SourceUniqueKeyAbsentError
	if !errors.As(err, &refusal) || refusal.When != ir.UniqueKeyDoorAddTable || !strings.Contains(err.Error(), "orders.o_tags") {
		t.Fatalf("add-table onto the case-insensitive array key returned %v; want the add-table SOURCE-UNIQUE-KEY-ABSENT", err)
	}
	if !strings.Contains(err.Error(), "the target's tags compares case-") {
		t.Errorf("the refusal does not name the looser array column: %v", err)
	}
	t.Logf("refusal: %v", err)
	tgt := twfbDB{engine: "postgres", dsn: targetDSN}
	if n := tgt.scalar(t, "SELECT count(*) FROM orders"); n != "0" {
		t.Fatalf("the refusing add-table copied %s rows", n)
	}
	cancel()
	<-runErr
}
