//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// UNIQUE-KEY-STOPPED-WINDOW-DOOR, round-3 review pins: two pgoutput DROP
// COLUMN shapes that leave the change stream's column-name list unchanged,
// so the boundary check's moved-columns trigger never fired, while the
// pgoutput unique-index door exempted the lost key on the premise that the
// target's own DROP COLUMN removes it too — a DROP nothing forwarded:
//
//   - a dropped GENERATED column carrying a unique key (pgoutput omits
//     generated columns from the RelationMessage, so the column was never
//     in the list): measured before the fix, row 1 overwritten on a MySQL
//     target at exit 0 and a loud 23505 on Postgres;
//   - a column dropped and re-added under its name and type (only the
//     attnum moves): row 1 overwritten on both targets.
//
// Fixed by the reader handing a unique key lost with a column to the
// pipeline's compare (ir.SchemaSnapshot.UniqueKeysMoved) and by attnums in
// the reader's dedupe and the boundary check's column comparison. Each shape
// × each target × {primed (a row of the table applied first), first touch
// (the DDL is the table's first change of the session)}: the stream must
// refuse at the schema change, the target must hold no row the source lacks
// with row 1 intact, and the printed drop-the-key remedy must converge.
// Graded on the source's rows against the target's (ukdRows: id|k|v).
//
// Shard: TestStreamer_ prefix → pipeline-rest-streamer.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
)

func TestStreamer_UniqueKeyDoor_Review3NameStableDropColumn(t *testing.T) {
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()
	myNew, myCleanup := ukcMySQLServer(t)
	defer myCleanup()
	shapes := []struct {
		name, create, ddl, dup string
		dropKey                map[string]string // the printed remedy, per target engine
	}{
		{
			"generated key column dropped",
			"CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, v varchar(20), g varchar(20) GENERATED ALWAYS AS (lower(k)) STORED); " +
				"CREATE UNIQUE INDEX kd_g ON kd (g); ALTER TABLE kd REPLICA IDENTITY FULL; " +
				"INSERT INTO kd (id,k,v) VALUES (1,'a','one'),(2,'b','two')",
			"ALTER TABLE kd DROP COLUMN g",
			"INSERT INTO kd (id,k,v) VALUES (99,'A','ninetynine')",
			map[string]string{"mysql": "ALTER TABLE kd DROP INDEX kd_g", "postgres": "DROP INDEX kd_g"},
		},
		{
			"key column dropped and re-added under its name",
			"CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, v varchar(20), b int NOT NULL DEFAULT 1); " +
				"CREATE UNIQUE INDEX kd_kb ON kd (k, b); ALTER TABLE kd REPLICA IDENTITY FULL; " +
				"INSERT INTO kd VALUES (1,'a','one',1),(2,'b','two',1)",
			"ALTER TABLE kd DROP COLUMN b, ADD COLUMN b int NOT NULL DEFAULT 1",
			"INSERT INTO kd (id,k,v) VALUES (99,'a','ninetynine')",
			map[string]string{"mysql": "ALTER TABLE kd DROP INDEX kd_kb", "postgres": "ALTER TABLE kd DROP CONSTRAINT kd_kb"},
		},
	}
	targets := []struct {
		engine string
		mk     func(string) twfbDB
	}{{"mysql", myNew}, {"postgres", pgNew}}
	n := 0
	for _, sh := range shapes {
		for _, tg := range targets {
			for _, primed := range []bool{true, false} {
				n++
				tag := fmt.Sprintf("ukdr3%c", 'a'+n)
				timing := "first touch"
				if primed {
					timing = "primed"
				}
				t.Run(sh.name+"/into "+tg.engine+"/"+timing, func(t *testing.T) {
					src, tgt := pgNew(tag+"_s"), tg.mk(tag+"_t")
					cell := twfbCell{src: src, tgt: tgt, streamID: tag, slotName: tag}
					src.exec(t, sh.create)
					run := startTWFBRun(cell.streamer())
					if !tgt.waitRow(t, "kd", 2, run, 180*time.Second) {
						t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
					}
					cell.waitStreaming(t, run)
					if primed {
						src.exec(t, "INSERT INTO kd (id,k,v) VALUES (3,'c','three')")
						if !tgt.waitRow(t, "kd", 3, run, 90*time.Second) {
							t.Fatalf("the priming row never landed (%v)", run.stop(t))
						}
					}
					src.exec(t, sh.ddl)
					src.exec(t, sh.dup)
					ukdAwaitExitOrConverge(t, run, src, tgt, 90*time.Second)
					if !run.exited() {
						_ = run.stop(t)
						t.Fatalf("nothing refused (source %s, target %s)", ukdRows(t, src), ukdRows(t, tgt))
					}
					// With a primed table the drop-and-re-add boundary's
					// prior carries attnums, so the schema-forward
					// classifier's own stable-id refusal ("now name a
					// different column") fires first — reachable only since
					// the reader stopped deduping the boundary away (F22).
					// Either is loud and stops before row 99.
					var refusal *ir.SourceUniqueKeyAbsentError
					door := errors.As(run.err, &refusal) && refusal.When == ir.UniqueKeyDoorAtSchemaChange
					identity := primed && run.err != nil && strings.Contains(run.err.Error(), "now name a different column")
					if !door && !identity {
						t.Fatalf("refused with %v; want the boundary SOURCE-UNIQUE-KEY-ABSENT", run.err)
					}
					t.Logf("refusal: %v", run.err)
					s, g := ukdRows(t, src), ukdRows(t, tgt)
					if !strings.Contains(g, "1|a|one") {
						t.Fatalf("row 1 changed: target %s", g)
					}
					for _, row := range strings.Split(g, ",") {
						if row != "" && !strings.Contains(","+s+",", ","+row+",") {
							t.Fatalf("the target holds %q, which the source does not (source %s)", row, s)
						}
					}
					tgt.exec(t, sh.dropKey[tg.engine])
					ukdConvergeAndResume(t, cell.streamer(), src, tgt, "INSERT INTO kd (id,k,v) VALUES (8,'r','eight')")
				})
			}
		}
	}
}

// TestStreamer_UniqueKeyDoor_Review3AddTableFacts is the round-3 add-table
// sibling: add-table judged the shape gate's plain schema read, which
// carries no unique-key facts, so a pre-created Postgres target key that is
// NULLS NOT DISTINCT passed against a source's plain unique key, and the
// source's NULL-email rows merged on the target. It must refuse before
// anything is copied, and the printed remedy (drop the key) must let the
// add copy both rows.
func TestStreamer_UniqueKeyDoor_Review3AddTableFacts(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	applyDDL(t, sourceDSN, "CREATE TABLE customers (id bigint PRIMARY KEY, email varchar(255) NOT NULL); "+
		"INSERT INTO customers VALUES (1, 'alice@example.com');")
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	const streamID = "ukd-r3-add"
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

	applyDDL(t, sourceDSN, "CREATE TABLE orders (id bigint PRIMARY KEY, email varchar(255), CONSTRAINT orders_email_key UNIQUE (email)); "+
		"ALTER TABLE orders REPLICA IDENTITY FULL; INSERT INTO orders VALUES (1, NULL), (2, NULL);")
	applyDDL(t, targetDSN, "CREATE TABLE orders (id bigint PRIMARY KEY, email varchar(255)); "+
		"CREATE UNIQUE INDEX o_email ON orders (email) NULLS NOT DISTINCT;")
	add := func() error {
		actx, acancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer acancel()
		return (&AddTable{
			Source: pgEng, Target: pgEng, SourceDSN: sourceDSN, TargetDSN: targetDSN,
			StreamID: streamID, TableName: "orders", LiveMode: true,
		}).Run(actx)
	}
	err := add()
	var refusal *ir.SourceUniqueKeyAbsentError
	if !errors.As(err, &refusal) || refusal.When != ir.UniqueKeyDoorAddTable || !strings.Contains(err.Error(), "orders.o_email") {
		t.Fatalf("add-table onto the NULLS NOT DISTINCT target returned %v; want the add-table SOURCE-UNIQUE-KEY-ABSENT", err)
	}
	t.Logf("refusal: %v", err)
	tgt := twfbDB{engine: "postgres", dsn: targetDSN}
	if n := tgt.scalar(t, "SELECT count(*) FROM orders"); n != "0" {
		t.Fatalf("the refusing add-table copied %s rows", n)
	}
	applyDDL(t, targetDSN, "DROP INDEX o_email;")
	if err := add(); err != nil {
		t.Fatalf("add-table after the printed remedy: %v", err)
	}
	if !waitForRowCount(t, targetDSN, "orders", 2, 60*time.Second) {
		t.Fatalf("the add copied %s rows; want both NULL-email rows", tgt.scalar(t, "SELECT count(*) FROM orders"))
	}
	cancel()
	<-runErr
}

// TestStreamer_UniqueKeyDoor_Review3CatalogAheadOfPosition pins what the
// boundary check does when the source catalog is already past the boundary
// it judges (it is read when the check runs). A large transaction on
// another table puts the reader behind; behind it come two transactions, so
// at the first one's Relation the catalog already reflects the second:
//
//   - "column dropped ahead": a column is added, then (next transaction) a
//     UNIQUE column is dropped. The target key is lost with a column the
//     stream has not reached yet;
//   - "key dropped, duplicate, then column dropped" (the round-4 reviewer's
//     probe): the source drops the key ON ITS OWN and writes a duplicate of
//     row 1, then drops the column. On a table with no primary key the
//     target's key is the upsert's conflict key on both engines.
//
// The two are indistinguishable to the reader and the gate. The round-3
// cut deferred the first one, and that deferral passed the second, merging
// row 3 onto row 1 at exit 0 on a MySQL target. So both refuse at the
// earlier boundary (loud), with no row merged. A plain restart refuses
// again at open: the stream stops before the DROP COLUMN, so its position
// never passes it, and the open door sees the same two catalogs. The printed
// drop-the-key remedy converges, and the duplicate then lands as its own row.
// Graded on the source's rows against the target's.
func TestStreamer_UniqueKeyDoor_Review3CatalogAheadOfPosition(t *testing.T) {
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()
	myNew, myCleanup := ukcMySQLServer(t)
	defer myCleanup()
	shapes := []struct {
		name, tag, create, txn1, txn2 string
	}{
		{
			"column dropped ahead", "a",
			"CREATE TABLE kd (id int PRIMARY KEY, k varchar(20) NOT NULL, v varchar(20), doomed varchar(20) UNIQUE); " +
				"ALTER TABLE kd REPLICA IDENTITY FULL; INSERT INTO kd VALUES (1,'a','one','x'),(2,'b','two','y')",
			"ALTER TABLE kd ADD COLUMN n int NOT NULL DEFAULT 0; INSERT INTO kd (id,k,v,doomed) VALUES (3,'c','three','z')",
			"ALTER TABLE kd DROP COLUMN doomed; INSERT INTO kd (id,k,v) VALUES (4,'d','four')",
		},
		{
			"key dropped, duplicate, then column dropped", "b",
			"CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, v varchar(20), doomed varchar(20)); " +
				"CREATE UNIQUE INDEX kd_doomed ON kd (doomed); " +
				"ALTER TABLE kd REPLICA IDENTITY FULL; INSERT INTO kd VALUES (1,'a','one','x'),(2,'b','two','y')",
			"DROP INDEX kd_doomed; INSERT INTO kd (id,k,v,doomed) VALUES (3,'c','three','x')",
			"ALTER TABLE kd DROP COLUMN doomed; INSERT INTO kd (id,k,v) VALUES (4,'d','four')",
		},
	}
	for _, sh := range shapes {
		for _, tg := range []struct {
			engine string
			mk     func(string) twfbDB
		}{{"postgres", pgNew}, {"mysql", myNew}} {
			t.Run(sh.name+"/into "+tg.engine, func(t *testing.T) {
				tag := "ukdr3k" + sh.tag + tg.engine[:2]
				src, tgt := pgNew(tag+"_s"), tg.mk(tag+"_t")
				src.exec(t, sh.create+"; CREATE TABLE bulk (id int PRIMARY KEY, pad text)")
				cell := twfbCell{src: src, tgt: tgt, streamID: tag, slotName: tag}
				run := startTWFBRun(cell.streamer())
				if !tgt.waitRow(t, "kd", 2, run, 180*time.Second) {
					t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
				}
				cell.waitStreaming(t, run)
				src.exec(t, "INSERT INTO bulk SELECT g, repeat('x', 200) FROM generate_series(1, 1000) g")
				src.exec(t, sh.txn1)
				src.exec(t, sh.txn2)
				ukdAwaitExitOrConverge(t, run, src, tgt, 240*time.Second)
				refusal := ukdR3RequireNoMergeRefusal(t, run, src, tgt, ir.UniqueKeyDoorAtSchemaChange)

				// A plain restart cannot pass the DROP COLUMN: it refuses at open.
				again := startTWFBRun(cell.streamer())
				ukdAwaitExitOrConverge(t, again, src, tgt, 120*time.Second)
				ukdR3RequireNoMergeRefusal(t, again, src, tgt, ir.UniqueKeyDoorAtOpen)

				// The printed remedy: drop the key on the target, start again.
				k := refusal.Keys[0]
				stmt := "ALTER TABLE kd DROP INDEX " + k.Key
				switch {
				case tg.engine == "postgres" && k.Constraint:
					stmt = "ALTER TABLE kd DROP CONSTRAINT " + k.Key
				case tg.engine == "postgres":
					stmt = "DROP INDEX " + k.Key
				}
				tgt.exec(t, stmt)
				ukdConvergeAndResume(t, cell.streamer(), src, tgt, "INSERT INTO kd (id,k,v) VALUES (8,'r','eight')")
			})
		}
	}
}

// ukdR3RequireNoMergeRefusal requires run to have refused with the
// unique-key door at when, row 1 intact and no target row the source lacks.
func ukdR3RequireNoMergeRefusal(t *testing.T, run *twfbRun, src, tgt twfbDB, when ir.UniqueKeyDoorWhen) *ir.SourceUniqueKeyAbsentError {
	t.Helper()
	if !run.exited() {
		_ = run.stop(t)
		t.Fatalf("nothing refused (source %s, target %s)", ukdRows(t, src), ukdRows(t, tgt))
	}
	var refusal *ir.SourceUniqueKeyAbsentError
	if !errors.As(run.err, &refusal) || refusal.When != when || len(refusal.Keys) == 0 {
		t.Fatalf("refused with %v; want SOURCE-UNIQUE-KEY-ABSENT at %v", run.err, when)
	}
	t.Logf("refusal (%v): %v", when, run.err)
	s, g := ukdRows(t, src), ukdRows(t, tgt)
	if !strings.Contains(g, "1|a|one") {
		t.Fatalf("row 1 changed: target %s", g)
	}
	for _, row := range strings.Split(g, ",") {
		if row != "" && !strings.Contains(","+s+",", ","+row+",") {
			t.Fatalf("the target holds %q, which the source does not (source %s)", row, s)
		}
	}
	return refusal
}
