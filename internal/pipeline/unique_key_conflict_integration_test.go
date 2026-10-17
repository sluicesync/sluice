//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// SOURCE-DROPPED-UNIQUE-KEY-MERGE, end to end on real servers. The source
// table `kd(id, k, v)` carries `CREATE UNIQUE INDEX kd_k ON kd(k)` and rows
// (1,'a','one'), (2,'b','two'); the source then runs `DROP INDEX kd_k` and
// inserts (99,'a','ninetynine'). The target keeps kd_k. Measured on
// v0.161.0, before the fix: a MySQL target upserted row 99 onto row 1
// (`1 a ninetynine`, no row 99; on a keyless table the id itself was
// rewritten), and a Postgres target with no primary key did the same
// through its ON CONFLICT arbiter — exit 0 in every cell below that is not
// marked loud.
//
// Each cell must now END loudly, with the target holding no row the source
// does not hold by id: the apply-time conflict-identity guard
// (UNIQUE-KEY-CONFLICT-OTHER-ROW) wherever the source-side doors cannot see
// the DROP (a postgres-trigger source; any source when the DROP lands while
// the stream is stopped), and the pgoutput door (UNFORWARDED-SCHEMA-CHANGE,
// which now reads plain unique indexes) for a live pgoutput stream. A
// keyless table here also carries a unique key on id (see ukcCreate): the
// single-unique-key keyless shape is the residual the guard cannot see, so
// it has no cell here that could pass for the wrong reason; the stream-open
// unique-key door closes it (unique_key_door_integration_test.go). These
// cells accept kd_k by name so that door stands aside and the guard is
// what they pin (see ukcRun).
//
// The independent expected value is the SOURCE's rows read by id (id, k, v)
// against the target's, never the stream's own logs: every row the target
// holds must equal the source's row of the same id, the seed rows must be
// there, and row 99 must not have merged anywhere.
//
// The controls drop the index the same way and then, in ONE source
// transaction, change row 1's k and reuse the old value for a new row: that
// applies in order and must converge exactly, with no refusal.
//
// Shard: TestStreamer_ prefix → pipeline-rest-streamer.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	"sluicesync.dev/sluice/internal/ir"
)

// ukcRows reads kd as id -> "k:v".
func ukcRows(t *testing.T, db twfbDB) map[string]string {
	t.Helper()
	conn, err := sql.Open(db.driver(), db.dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = conn.Close() }()
	q := "SELECT CAST(id AS CHAR), CONCAT(k, ':', COALESCE(v, 'NULL')) FROM kd"
	if db.engine == "postgres" {
		q = "SELECT id::text, k || ':' || coalesce(v, 'NULL') FROM kd"
	}
	rows, err := conn.Query(q)
	if err != nil {
		t.Fatalf("read kd: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var id, kv string
		if err := rows.Scan(&id, &kv); err != nil {
			t.Fatal(err)
		}
		out[id] = kv
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func ukcRender(m map[string]string) string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = id + "=" + m[id]
	}
	return strings.Join(parts, ",")
}

// ukcScenario is one cell's shape.
type ukcScenario struct {
	keyless bool
	// stopped: the DROP and the colliding insert land while the stream is
	// stopped (a warm resume replays them); otherwise they land live.
	stopped bool
	// control: instead of the colliding insert, one source transaction
	// changes row 1's k and reuses 'a' for row 99 — must converge exactly.
	control bool
	// markers are the refusal texts the cell accepts (any one).
	markers []string
}

const (
	ukcGuard = "UNIQUE-KEY-CONFLICT-OTHER-ROW"
	ukcDoor  = "UNFORWARDED-SCHEMA-CHANGE"
)

// ukcCreate is kd on the source's dialect. The keyless shape carries a
// second unique key, on id: with a single unique key and no primary key the
// merge is indistinguishable from the newer-image re-send the upsert must
// absorb, and is deliberately not guarded at apply time
// (engines/mysql/unique_key_conflict_guard.go); with two, the surviving key
// is the identity the guard checks. The second key is named kd_zid so the
// DROPPED kd_k sorts first and is a Postgres target's ON CONFLICT arbiter
// (fewest columns, then the smallest name): the review's shape, where the
// merge goes through the arbiter itself and no unique_violation fires.
func ukcCreate(dialect string, keyless bool) string {
	pk := " PRIMARY KEY"
	idKey, idKeyMy := "", ""
	if keyless {
		pk = ""
		idKey, idKeyMy = "CREATE UNIQUE INDEX kd_zid ON kd (id); ", ", UNIQUE KEY kd_zid (id)"
	}
	if dialect == "postgres" {
		return "CREATE TABLE kd (id int NOT NULL" + pk + ", k varchar(20) NOT NULL, v varchar(20)); " +
			"CREATE UNIQUE INDEX kd_k ON kd (k); " + idKey + "ALTER TABLE kd REPLICA IDENTITY FULL; " +
			"INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')"
	}
	return "CREATE TABLE kd (id INT NOT NULL" + pk + ", k VARCHAR(20) NOT NULL, v VARCHAR(20), UNIQUE KEY kd_k (k)" + idKeyMy + "); " +
		"INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')"
}

func ukcDropIndex(dialect string) string {
	if dialect == "postgres" {
		return "DROP INDEX kd_k"
	}
	return "ALTER TABLE kd DROP INDEX kd_k"
}

// ukcRun runs one cell: newStreamer builds the stream (fresh each start).
func ukcRun(t *testing.T, sc ukcScenario, src, tgt twfbDB, cell twfbCell, newStreamer func() *Streamer) {
	t.Helper()
	// These cells pin the APPLY guard. Since UNIQUE-KEY-STOPPED-WINDOW-DOOR
	// the stream-open door refuses a target key the source dropped before
	// the guard can see a row (unique_key_door_integration_test.go pins
	// that), and the trigger lane's mid-stream re-check may too; accepting
	// kd_k by name is exactly the configuration where the guard is the last
	// line, so every cell here runs with it.
	build := newStreamer
	newStreamer = func() *Streamer {
		s := build()
		// Accepted as the target holds it, once it exists (a cold start's
		// copied key is covered and needs no acceptance).
		s.AcceptTargetOnlyUniqueKeys = ukdAcceptValue(t, tgt, "kd", "kd_k")
		return s
	}
	srcDialect := "mysql"
	if src.engine == "postgres" {
		srcDialect = "postgres"
	}
	run := startTWFBRun(newStreamer())
	if !tgt.waitRow(t, "kd", 2, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	// One CDC row first, so the stream (and any door's baseline) is live
	// before the DROP.
	src.exec(t, "INSERT INTO kd VALUES (50,'p','fifty')")
	if !tgt.waitRow(t, "kd", 50, run, 60*time.Second) {
		t.Fatalf("the priming row never landed (%v)", run.stop(t))
	}
	if sc.stopped {
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("the clean stop returned %v", err)
		}
	}
	src.exec(t, ukcDropIndex(srcDialect))
	if sc.control {
		src.exec(t, "BEGIN; UPDATE kd SET k = 'x' WHERE id = 1; INSERT INTO kd VALUES (99,'a','ninetynine'); COMMIT")
	} else {
		src.exec(t, "INSERT INTO kd VALUES (99,'a','ninetynine')")
	}
	src.exec(t, "INSERT INTO kd VALUES (100,'z','hundred')")
	if sc.stopped {
		run = startTWFBRun(newStreamer())
	}
	_ = tgt.waitRow(t, "kd", 100, run, 90*time.Second)
	// Let a refusal land, or the target converge.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && !run.exited() {
		if ukcRender(ukcRows(t, src)) == ukcRender(ukcRows(t, tgt)) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	var refusal error
	if run.exited() {
		refusal = run.err
		if refusal == nil || errors.Is(refusal, context.Canceled) {
			t.Fatal("the stream exited without an error")
		}
	} else if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("the stream stopped with %v", err)
	}
	s, g := ukcRows(t, src), ukcRows(t, tgt)
	t.Logf("source %s\ntarget %s\nrefusal: %v", ukcRender(s), ukcRender(g), refusal)

	if sc.control {
		if refusal != nil {
			t.Fatalf("a key moved and reused in one transaction refused: %v", refusal)
		}
		if ukcRender(s) != ukcRender(g) {
			t.Fatalf("the control did not converge exactly\nsource %s\ntarget %s", ukcRender(s), ukcRender(g))
		}
		return
	}
	if refusal == nil {
		t.Fatalf("nothing refused: the target's kd_k cannot hold rows 1 and 99, both 'a' on the source\nsource %s\ntarget %s",
			ukcRender(s), ukcRender(g))
	}
	matched := false
	for _, m := range sc.markers {
		if strings.Contains(refusal.Error(), m) {
			matched = true
		}
	}
	if !matched {
		t.Errorf("refused with %v; want one of %v", refusal, sc.markers)
	}
	if strings.Contains(refusal.Error(), ukcGuard) && !errors.Is(refusal, ir.ErrUniqueKeyConflictOtherRow) {
		t.Errorf("the guard's refusal does not carry %v through the chain", ir.ErrUniqueKeyConflictOtherRow)
	}
	for id, kv := range g {
		if s[id] != kv {
			t.Errorf("target row %s holds %q where the source holds %q (a merge)", id, kv, s[id])
		}
	}
	for _, id := range []string{"1", "2", "50"} {
		if g[id] != s[id] {
			t.Errorf("seed row %s is %q on the target (source %q)", id, g[id], s[id])
		}
	}
	if _, ok := g["99"]; ok {
		t.Errorf("row 99 reached the target, which cannot hold it beside row 1")
	}
}

// ukcPGServer boots a logical Postgres server; newDB makes a database on it.
func ukcPGServer(t *testing.T) (newDB func(name string) twfbDB, cleanup func()) {
	root, _, c := startPostgresLogical(t)
	rootDB := twfbDB{engine: "postgres", dsn: root}
	return func(name string) twfbDB {
		rootDB.exec(t, "CREATE DATABASE "+name)
		dsn, err := buildPGDSN(root, name)
		if err != nil {
			t.Fatal(err)
		}
		return twfbDB{engine: "postgres", dsn: dsn}
	}, c
}

// ukcMySQLServer boots a binlog-enabled MySQL server; newDB makes a
// database on it.
func ukcMySQLServer(t *testing.T) (newDB func(name string) twfbDB, cleanup func()) {
	root, c := rsfBootMySQL(t, "mysql:8.0")
	srv := rsfMySQLFamily(t, "mysql", "mysql", root, buildMySQLDSN)
	return func(name string) twfbDB { return srv.newDB(t, name) }, c
}

// TestStreamer_UniqueKeyConflict_PostgresSource covers pgoutput → MySQL and
// pgoutput → Postgres, live (the door) and stopped (the guard), with and
// without a primary key, and the control.
func TestStreamer_UniqueKeyConflict_PostgresSource(t *testing.T) {
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()
	myNew, myCleanup := ukcMySQLServer(t)
	defer myCleanup()
	cells := []struct {
		name   string
		target string
		sc     ukcScenario
	}{
		{"mysql/pk/live", "mysql", ukcScenario{markers: []string{ukcDoor}}},
		{"mysql/pk/stopped", "mysql", ukcScenario{stopped: true, markers: []string{ukcGuard}}},
		{"mysql/keyless/live", "mysql", ukcScenario{keyless: true, markers: []string{ukcDoor}}},
		{"mysql/keyless/stopped", "mysql", ukcScenario{keyless: true, stopped: true, markers: []string{ukcGuard}}},
		{"mysql/pk/stopped/control", "mysql", ukcScenario{stopped: true, control: true}},
		{"postgres/keyless/live", "postgres", ukcScenario{keyless: true, markers: []string{ukcDoor}}},
		{"postgres/keyless/stopped", "postgres", ukcScenario{keyless: true, stopped: true, markers: []string{ukcGuard}}},
		{"postgres/keyless/stopped/control", "postgres", ukcScenario{keyless: true, stopped: true, control: true}},
	}
	for i, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			tag := "ukcp" + string(rune('a'+i))
			src := pgNew(tag + "_s")
			var tgt twfbDB
			if c.target == "mysql" {
				tgt = myNew(tag + "_t")
			} else {
				tgt = pgNew(tag + "_t")
			}
			src.exec(t, ukcCreate("postgres", c.sc.keyless))
			cell := twfbCell{src: src, tgt: tgt, streamID: tag, slotName: tag}
			ukcRun(t, c.sc, src, tgt, cell, cell.streamer)
		})
	}
}

// TestStreamer_UniqueKeyConflict_MySQLSource covers the binlog lane's
// stopped-window hole (a DROP while the stream is stopped is in the door's
// baseline, mysql/cdc_unforwarded_classes.go): MySQL → MySQL with a primary
// key, MySQL → Postgres keyless, and the control.
func TestStreamer_UniqueKeyConflict_MySQLSource(t *testing.T) {
	myNew, myCleanup := ukcMySQLServer(t)
	defer myCleanup()
	_, pgRoot, pgCleanup := startPostgres(t)
	defer pgCleanup()
	pgRootDB := twfbDB{engine: "postgres", dsn: pgRoot}
	cells := []struct {
		name   string
		target string
		sc     ukcScenario
	}{
		{"mysql/pk/stopped", "mysql", ukcScenario{stopped: true, markers: []string{ukcGuard}}},
		{"mysql/pk/stopped/control", "mysql", ukcScenario{stopped: true, control: true}},
		{"postgres/keyless/stopped", "postgres", ukcScenario{keyless: true, stopped: true, markers: []string{ukcGuard}}},
	}
	for i, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			tag := "ukcm" + string(rune('a'+i))
			src := myNew(tag + "_s")
			var tgt twfbDB
			if c.target == "mysql" {
				tgt = myNew(tag + "_t")
			} else {
				pgRootDB.exec(t, "CREATE DATABASE "+tag+"_t")
				dsn, err := buildPGDSN(pgRoot, tag+"_t")
				if err != nil {
					t.Fatal(err)
				}
				tgt = twfbDB{engine: "postgres", dsn: dsn}
			}
			src.exec(t, ukcCreate("mysql", c.sc.keyless))
			cell := twfbCell{src: src, tgt: tgt, streamID: tag}
			ukcRun(t, c.sc, src, tgt, cell, cell.streamer)
		})
	}
}

// TestStreamer_UniqueKeyConflict_TriggerCDC covers a postgres-trigger
// source, whose change log records no DDL at all: a live DROP INDEX leaves
// no marker, so only the apply-time guard can see it.
func TestStreamer_UniqueKeyConflict_TriggerCDC(t *testing.T) {
	src, tgt, _, cleanup := kttcPairWith(t, "ukct", "mysql",
		"CREATE TABLE kd (id int NOT NULL PRIMARY KEY, k varchar(20) NOT NULL, v varchar(20)); "+
			"CREATE UNIQUE INDEX kd_k ON kd (k); INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')")
	defer cleanup()
	if _, ok := engines.Get(pgtrigger.EngineName); !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	cell := twfbCell{src: src, tgt: tgt, streamID: "ukct"}
	ukcRun(t, ukcScenario{markers: []string{ukcGuard}}, src, tgt, cell, func() *Streamer {
		return kttcStreamer(t, src, tgt, cell.streamID)
	})
}

// TestStreamer_UniqueKeyConflict_TradeRemedy provokes the guard's stated
// false refusal (UNIQUE-KEY-GUARD-OVERLAP-REFUSAL) and measures the remedy
// the refusal prints, from a native MySQL binlog source and a Postgres
// pgoutput source. BOTH take the same drop path: their plain-INSERT copies
// make the restart DROP and recreate the target tables first (only the
// VStream snapshot reader is idempotent — copyReaderIsIdempotent — so the
// upsert re-copy path, which leaves the target's rows in place, is NOT
// measured here; this test called the pgoutput cell "the upsert re-copy
// path" until 2026-10-17, Bug 320). The pgoutput cell adds the slot-drop
// step the remedy names. kd has no primary key and two unique keys (id,
// k). While the stream is stopped the source inserts (3,'c') and then moves
// it to id 4, and the TARGET is made to hold (4,'c') already — the state a
// re-delivery window leaves: the target ahead of the change being replayed.
// The replayed INSERT (3,'c') matches (4,'c') through k and disagrees on id,
// exactly a merge's shape, so it refuses. The source still declares both
// keys, so the remedy that applies is the re-copy for `sync start`,
// `--restart-from-scratch` with the same stream id. It must converge: the
// target equal to the source by id, and the stream live again. Not measured:
// a VStream source — the only upsert re-copy path — whose restart re-runs
// the auto-shard stitch window, so under live writes the refusal may fire
// again, and whose re-copy deletes nothing, so a stale target row the
// source no longer has survives it (`--reset-target-data --yes` is the
// fallback).
func TestStreamer_UniqueKeyConflict_TradeRemedy(t *testing.T) {
	myNew, myCleanup := ukcMySQLServer(t)
	defer myCleanup()
	t.Run("mysql-binlog source, the drop path", func(t *testing.T) {
		src, tgt := myNew("ukctr_s"), myNew("ukctr_t")
		src.exec(t, ukcCreate("mysql", true))
		ukcTradeRemedy(t, src, tgt, twfbCell{src: src, tgt: tgt, streamID: "ukctr"})
	})
	t.Run("postgres-pgoutput source, the drop path plus the slot drop", func(t *testing.T) {
		pgNew, pgCleanup := ukcPGServer(t)
		defer pgCleanup()
		src, tgt := pgNew("ukctrp_s"), myNew("ukctrp_t")
		src.exec(t, ukcCreate("postgres", true))
		ukcTradeRemedy(t, src, tgt, twfbCell{src: src, tgt: tgt, streamID: "ukctrp", slotName: "ukctrp"})
	})
}

func ukcTradeRemedy(t *testing.T, src, tgt twfbDB, cell twfbCell) {
	t.Helper()
	cell.coldStartAndStop(t, "kd", 2)

	src.exec(t, "INSERT INTO kd VALUES (3,'c','three'); UPDATE kd SET id = 4 WHERE id = 3")
	tgt.exec(t, "INSERT INTO kd VALUES (4,'c','three')")

	run := startTWFBRun(cell.streamer())
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) && !run.exited() {
		time.Sleep(300 * time.Millisecond)
	}
	if !run.exited() {
		_ = run.stop(t)
		t.Fatalf("the replay onto a target ahead of it did not refuse (target %s)", ukcRender(ukcRows(t, tgt)))
	}
	refusal := run.err
	if !errors.Is(refusal, ir.ErrUniqueKeyConflictOtherRow) || !strings.Contains(refusal.Error(), "--restart-from-scratch") ||
		!strings.Contains(refusal.Error(), "--reset-target-data --yes") {
		t.Fatalf("refused with %v; want %s naming both re-copy remedies", refusal, ir.ErrUniqueKeyConflictOtherRow)
	}
	t.Logf("refusal: %v", refusal)

	// The printed remedy for `sync start`, as an operator would run it: on a
	// Postgres source, first the slot drop the remedy names (here its SQL
	// equivalent on the resolved, `sluice_`-prefixed slot name).
	if src.engine == "postgres" {
		if !strings.Contains(refusal.Error(), "sluice slot drop") {
			t.Fatalf("the remedy does not name the slot drop a Postgres re-copy needs: %v", refusal)
		}
		src.exec(t, "SELECT pg_drop_replication_slot('sluice_"+cell.slotName+"')")
	}
	s := cell.streamer()
	s.RestartFromScratch = true
	run = startTWFBRun(s)
	deadline = time.Now().Add(180 * time.Second)
	for time.Now().Before(deadline) && !run.exited() {
		if ukcRender(ukcRows(t, src)) == ukcRender(ukcRows(t, tgt)) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if run.exited() && run.err != nil && !errors.Is(run.err, context.Canceled) {
		t.Fatalf("the remedy run failed: %v", run.err)
	}
	cell.waitStreaming(t, run)
	src.exec(t, "INSERT INTO kd VALUES (5,'e','five')")
	if !tgt.waitRow(t, "kd", 5, run, 60*time.Second) {
		t.Fatalf("the stream did not resume after the remedy (%v)", run.stop(t))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the remedy run stopped with %v", err)
	}
	if s, g := ukcRender(ukcRows(t, src)), ukcRender(ukcRows(t, tgt)); s != g {
		t.Fatalf("the remedy did not converge\nsource %s\ntarget %s", s, g)
	}
}
