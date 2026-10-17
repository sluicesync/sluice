//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// UNIQUE-KEY-STOPPED-WINDOW-DOOR, end to end on real servers.
//
// The source drops a unique key the target keeps, and then writes a row
// that duplicates it. The stream does not see the DROP live — it was
// stopped, or the source's change stream carries no DDL — so the target's
// key would turn that row into an overwrite of a different one. Two of
// the shapes are the ones the apply-time guard (UNIQUE-KEY-CONFLICT-OTHER-
// ROW) cannot see, and that v0.161.1 pinned as accepted silent residuals:
//
//   - (a) a table with no primary key and exactly ONE unique key;
//   - (b) a table with no primary key whose rows differ only in a NULL
//     unique-key column (kd(id UNIQUE NULL, k UNIQUE)).
//
// Each refusing cell must end with SOURCE-UNIQUE-KEY-ABSENT at stream open
// (or at the mid-stream re-check), with the target unchanged, and then the
// remedy the refusal prints is run exactly as an operator would and must
// converge. The controls — no DDL, and DDL that keeps the key — must
// converge with no refusal, and the working configurations (functional,
// prefix and composite keys, a renamed multi-database namespace, a key
// accepted by name) must not refuse.
//
// The independent expected value is the SOURCE's rows, read as a multiset
// of (id, k, v) with NULL rendered, against the target's — never the
// stream's own logs.
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
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// ukdRows reads table kd as a sorted multiset of "id|k|v", NULL as "∅".
// ukdAcceptValue reads db's table and returns the
// --accept-target-only-unique-key value accepting its key as it is now
// (`table.key=<fingerprint>`), read the way the door reads a target (the
// engine's schema reader plus its unique-key facts); nil when the table or
// key does not exist yet. A test takes it from the catalog the way an
// operator takes it from the refusal.
func ukdAcceptValue(t *testing.T, db twfbDB, table, key string) []string {
	t.Helper()
	eng, ok := engines.Get(db.engine)
	if !ok {
		t.Fatalf("engine %q not registered", db.engine)
	}
	ctx := context.Background()
	sr, err := eng.OpenSchemaReader(ctx, db.dsn)
	if err != nil {
		t.Fatalf("open schema reader: %v", err)
	}
	defer migcore.CloseIf(sr)
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	if err := applyUniqueKeyFacts(ctx, sr, schema); err != nil {
		t.Fatalf("read unique-key facts: %v", err)
	}
	for _, tbl := range schema.Tables {
		if !strings.EqualFold(tbl.Name, table) {
			continue
		}
		keys := append([]*ir.Index{tbl.PrimaryKey}, tbl.Indexes...)
		for _, idx := range keys {
			if idx != nil && strings.EqualFold(idx.Name, key) {
				def, err := sr.(ir.UniqueKeyDefinitionReader).UniqueKeyDefinition(ctx, tbl.Name, idx.Name)
				if err != nil {
					t.Fatalf("read the definition of %s.%s: %v", tbl.Name, idx.Name, err)
				}
				return []string{table + "." + idx.Name + "=" + definitionFingerprint(def)}
			}
		}
	}
	return nil
}

// ukdAcceptFromRefusal returns the value a SOURCE-UNIQUE-KEY-ABSENT refusal
// prints for key, as an operator would paste it.
func ukdAcceptFromRefusal(t *testing.T, err error, key string) string {
	t.Helper()
	var refusal *ir.SourceUniqueKeyAbsentError
	if !errors.As(err, &refusal) {
		t.Fatalf("not a SOURCE-UNIQUE-KEY-ABSENT refusal: %v", err)
	}
	for _, k := range refusal.Keys {
		if strings.EqualFold(k.Key, key) && k.Fingerprint != "" {
			value := k.Table + "." + k.Key + "=" + k.Fingerprint
			if !strings.Contains(err.Error(), "--accept-target-only-unique-key "+value) {
				t.Fatalf("the refusal does not print the value %s: %v", value, err)
			}
			return value
		}
	}
	t.Fatalf("the refusal names no key %s with a fingerprint: %v", key, err)
	return ""
}

func ukdRows(t *testing.T, db twfbDB) string {
	t.Helper()
	return ukdRowsOf(t, db, "kd")
}

// ukdRowsOf is [ukdRows] over another table.
func ukdRowsOf(t *testing.T, db twfbDB, table string) string {
	t.Helper()
	conn, err := sql.Open(db.driver(), db.dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = conn.Close() }()
	q := "SELECT COALESCE(CAST(id AS CHAR), '∅'), COALESCE(k, '∅'), COALESCE(v, '∅') FROM " + table
	if db.engine == "postgres" {
		// k::text: a bpchar column reads without its padding, as MySQL's CHAR does.
		q = "SELECT COALESCE(id::text, '∅'), COALESCE(k::text, '∅'), COALESCE(v, '∅') FROM " + table
	}
	rows, err := conn.Query(q)
	if err != nil {
		t.Fatalf("read %s: %v", table, err)
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

// ukdShape is one source table layout: its create (dialect-specific) and
// the statements that drop the key the target keeps and write the row
// that duplicates it.
type ukdShape struct {
	name       string
	pgCreate   string
	myCreate   string
	pgDrop     string
	myDrop     string
	duplicate  string
	targetDrop map[string]string // target engine → the remedy's DROP
}

var (
	// ukdShapePK: a primary key and a unique key beside it (the guard sees
	// it too; the door now refuses first).
	ukdShapePK = ukdShape{
		name: "pk",
		pgCreate: "CREATE TABLE kd (id int NOT NULL PRIMARY KEY, k varchar(20) NOT NULL, v varchar(20)); " +
			"CREATE UNIQUE INDEX kd_k ON kd (k); INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')",
		myCreate: "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) NOT NULL, v VARCHAR(20), UNIQUE KEY kd_k (k)); " +
			"INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')",
		pgDrop: "DROP INDEX kd_k", myDrop: "ALTER TABLE kd DROP INDEX kd_k",
		duplicate:  "INSERT INTO kd VALUES (99,'a','ninetynine')",
		targetDrop: map[string]string{"mysql": "ALTER TABLE kd DROP INDEX kd_k", "postgres": "DROP INDEX kd_k"},
	}
	// ukdShapeA: residual (a) — no primary key, exactly one unique key.
	ukdShapeA = ukdShape{
		name: "keyless-one-key",
		pgCreate: "CREATE TABLE kd (id int NOT NULL, k varchar(20) NOT NULL, v varchar(20)); " +
			"CREATE UNIQUE INDEX kd_k ON kd (k); ALTER TABLE kd REPLICA IDENTITY FULL; " +
			"INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')",
		myCreate: "CREATE TABLE kd (id INT NOT NULL, k VARCHAR(20) NOT NULL, v VARCHAR(20), UNIQUE KEY kd_k (k)); " +
			"INSERT INTO kd VALUES (1,'a','one'),(2,'b','two')",
		pgDrop: "DROP INDEX kd_k", myDrop: "ALTER TABLE kd DROP INDEX kd_k",
		duplicate:  "INSERT INTO kd VALUES (99,'a','ninetynine')",
		targetDrop: map[string]string{"mysql": "ALTER TABLE kd DROP INDEX kd_k", "postgres": "DROP INDEX kd_k"},
	}
	// ukdShapeB: residual (b) — no primary key; the surviving unique key is
	// on a NULL column, so two rows differing only there agree on it.
	ukdShapeB = ukdShape{
		name: "keyless-null-key",
		pgCreate: "CREATE TABLE kd (id int, k varchar(20) NOT NULL, v varchar(20)); " +
			"CREATE UNIQUE INDEX kd_id ON kd (id); CREATE UNIQUE INDEX kd_k ON kd (k); ALTER TABLE kd REPLICA IDENTITY FULL; " +
			"INSERT INTO kd VALUES (NULL,'a','one'),(2,'b','two')",
		myCreate: "CREATE TABLE kd (id INT NULL, k VARCHAR(20) NOT NULL, v VARCHAR(20), UNIQUE KEY kd_id (id), UNIQUE KEY kd_k (k)); " +
			"INSERT INTO kd VALUES (NULL,'a','one'),(2,'b','two')",
		pgDrop: "DROP INDEX kd_k", myDrop: "ALTER TABLE kd DROP INDEX kd_k",
		duplicate:  "INSERT INTO kd VALUES (NULL,'a','other')",
		targetDrop: map[string]string{"mysql": "ALTER TABLE kd DROP INDEX kd_k", "postgres": "DROP INDEX kd_k"},
	}
)

func (sh ukdShape) create(dialect string) string {
	if dialect == "postgres" {
		return sh.pgCreate
	}
	return sh.myCreate
}

func (sh ukdShape) drop(dialect string) string {
	if dialect == "postgres" {
		return sh.pgDrop
	}
	return sh.myDrop
}

// ukdDialect is a twfbDB's SQL dialect.
func ukdDialect(db twfbDB) string {
	if db.engine == "postgres" {
		return "postgres"
	}
	return "mysql"
}

// ukdAwaitExitOrConverge waits for the run to exit or the target to equal
// the source, whichever comes first. A target table that does not exist
// yet (a --reset-target-data run before its copy recreates it) is not
// converged.
func ukdAwaitExitOrConverge(t *testing.T, run *twfbRun, src, tgt twfbDB, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && !run.exited() {
		if strings.HasPrefix(tgt.scalar(t, "SELECT count(*) FROM kd"), "<") {
			time.Sleep(300 * time.Millisecond)
			continue
		}
		if ukdRows(t, src) == ukdRows(t, tgt) {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// ukdRequireRefusal asserts the run exited with the door's refusal naming
// the key, and the target is what it was before the run.
func ukdRequireRefusal(t *testing.T, run *twfbRun, tgt twfbDB, before, key string, when ir.UniqueKeyDoorWhen) *ir.SourceUniqueKeyAbsentError {
	t.Helper()
	if !run.exited() {
		_ = run.stop(t)
		t.Fatalf("the stream did not refuse; target %s", ukdRows(t, tgt))
	}
	var refusal *ir.SourceUniqueKeyAbsentError
	if !errors.As(run.err, &refusal) || !errors.Is(run.err, ir.ErrSourceUniqueKeyAbsent) {
		t.Fatalf("refused with %v; want SOURCE-UNIQUE-KEY-ABSENT", run.err)
	}
	if refusal.When != when {
		t.Errorf("refused at %v, want %v: %v", refusal.When, when, refusal)
	}
	if !strings.Contains(run.err.Error(), key) {
		t.Errorf("the refusal does not name %s: %v", key, run.err)
	}
	if got := ukdRows(t, tgt); got != before {
		t.Errorf("the refusing run changed the target\nbefore %s\nafter  %s", before, got)
	}
	t.Logf("refusal: %v", run.err)
	return refusal
}

// ukdConvergeAndResume runs the stream until the target equals the source,
// writes one more source row, and checks the stream carries it — the
// "stream is live again" half of a remedy or control.
func ukdConvergeAndResume(t *testing.T, s *Streamer, src, tgt twfbDB, nextRow string) {
	t.Helper()
	run := startTWFBRun(s)
	ukdAwaitExitOrConverge(t, run, src, tgt, 180*time.Second)
	if run.exited() {
		t.Fatalf("the stream exited: %v (source %s, target %s)", run.err, ukdRows(t, src), ukdRows(t, tgt))
	}
	src.exec(t, nextRow)
	ukdAwaitExitOrConverge(t, run, src, tgt, 90*time.Second)
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the stream stopped with %v", err)
	}
	if s, g := ukdRows(t, src), ukdRows(t, tgt); s != g {
		t.Fatalf("did not converge\nsource %s\ntarget %s", s, g)
	}
}

// ukdStoppedCell is the stopped-window cell: cold start, stop, then on the
// source either drop the key and write the duplicate (refusing) or run the
// control's statements; restart.
func ukdStoppedCell(t *testing.T, cell twfbCell, newStreamer func() *Streamer, sh ukdShape, control string) {
	t.Helper()
	src, tgt := cell.src, cell.tgt
	src.exec(t, sh.create(ukdDialect(src)))
	run := startTWFBRun(newStreamer())
	if !tgt.waitRow(t, "kd", 2, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the clean stop returned %v", err)
	}
	before := ukdRows(t, tgt)

	if control != "" {
		src.exec(t, control)
		ukdConvergeAndResume(t, newStreamer(), src, tgt, "INSERT INTO kd VALUES (7,'q','seven')")
		return
	}

	src.exec(t, sh.drop(ukdDialect(src)))
	src.exec(t, sh.duplicate)
	run = startTWFBRun(newStreamer())
	ukdAwaitExitOrConverge(t, run, src, tgt, 120*time.Second)
	refusal := ukdRequireRefusal(t, run, tgt, before, "kd.kd_k", ir.UniqueKeyDoorAtOpen)

	// The remedy, as printed: drop the key on the target, start again with
	// the same stream id. On a source that sees DDL live the refusal says
	// nothing after the DROP was applied; on one that does not, it says so
	// for a stream stopped across the DROP, which this one was.
	if !strings.Contains(refusal.Error(), "drop each named key on the target") {
		t.Fatalf("the refusal does not print the drop-the-key remedy: %v", refusal)
	}
	tgt.exec(t, ukdTargetDrop(t, refusal, sh, tgt))
	ukdConvergeAndResume(t, newStreamer(), src, tgt, "INSERT INTO kd VALUES (8,'r','eight')")
}

// ukdTargetDrop is the DROP the refusal tells the operator to run: the
// constraint form when it marks the key as a constraint (a keyless table's
// unique key lands on a Postgres target as one, and DROP INDEX refuses it).
func ukdTargetDrop(t *testing.T, refusal *ir.SourceUniqueKeyAbsentError, sh ukdShape, tgt twfbDB) string {
	t.Helper()
	if len(refusal.Keys) == 1 && refusal.Keys[0].Constraint {
		if !strings.Contains(refusal.Error(), "ALTER TABLE kd DROP CONSTRAINT kd_k") {
			t.Fatalf("a constraint-owned key's refusal does not print its DROP CONSTRAINT: %v", refusal)
		}
		return "ALTER TABLE kd DROP CONSTRAINT kd_k"
	}
	return sh.targetDrop[tgt.engine]
}

// TestStreamer_UniqueKeyDoor_PostgresSource covers pgoutput → MySQL and
// pgoutput → Postgres: the PK shape and residuals (a) and (b), each
// refusing at open and converging under the printed remedy, plus the two
// controls.
func TestStreamer_UniqueKeyDoor_PostgresSource(t *testing.T) {
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()
	myNew, myCleanup := ukcMySQLServer(t)
	defer myCleanup()
	cells := []struct {
		name    string
		target  string
		shape   ukdShape
		control string
	}{
		{"mysql/pk/stopped-drop", "mysql", ukdShapePK, ""},
		{"postgres/keyless-one-key/stopped-drop", "postgres", ukdShapeA, ""},
		{"mysql/keyless-one-key/stopped-drop", "mysql", ukdShapeA, ""},
		{"postgres/keyless-null-key/stopped-drop", "postgres", ukdShapeB, ""},
		{"mysql/pk/control-no-ddl", "mysql", ukdShapePK, "INSERT INTO kd VALUES (3,'c','three')"},
		{
			"postgres/pk/control-ddl-keeps-the-key", "postgres", ukdShapePK,
			"DROP INDEX kd_k; CREATE UNIQUE INDEX kd_k2 ON kd (k); CREATE INDEX kd_v ON kd (v); INSERT INTO kd VALUES (3,'c','three')",
		},
	}
	for i, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			tag := "ukdp" + string(rune('a'+i))
			src := pgNew(tag + "_s")
			tgt := pgNew(tag + "_t")
			if c.target == "mysql" {
				tgt = myNew(tag + "_t")
			}
			cell := twfbCell{src: src, tgt: tgt, streamID: tag, slotName: tag}
			ukdStoppedCell(t, cell, cell.streamer, c.shape, c.control)
		})
	}
	// The other remedy a pgoutput refusal prints — the clean re-copy, for a
	// stream an older sluice ran across the DROP — run exactly as printed.
	for i, remedy := range []string{"--reset-target-data", "--restart-from-scratch"} {
		t.Run("mysql/pk/stopped-drop/the clean re-copy remedy "+remedy, func(t *testing.T) {
			tag := "ukdpz" + string(rune('a'+i))
			src, tgt := pgNew(tag+"_s"), myNew(tag+"_t")
			src.exec(t, ukdShapePK.pgCreate)
			cell := twfbCell{src: src, tgt: tgt, streamID: tag, slotName: tag}
			cell.coldStartAndStop(t, "kd", 2)
			before := ukdRows(t, tgt)
			src.exec(t, ukdShapePK.pgDrop)
			src.exec(t, ukdShapePK.duplicate)
			run := startTWFBRun(cell.streamer())
			ukdAwaitExitOrConverge(t, run, src, tgt, 120*time.Second)
			refusal := ukdRequireRefusal(t, run, tgt, before, "kd.kd_k", ir.UniqueKeyDoorAtOpen)
			if !strings.Contains(refusal.Error(), remedy) {
				t.Fatalf("the refusal does not print the clean re-copy %s: %v", remedy, refusal)
			}
			ukdRunPrintedSlotDrop(t, refusal, src, cell.slotName)
			r := cell.streamer()
			r.ResetTargetData = remedy == "--reset-target-data"
			r.RestartFromScratch = remedy == "--restart-from-scratch"
			ukdConvergeAndResume(t, r, src, tgt, "INSERT INTO kd VALUES (8,'r','eight')")
		})
	}
}

// ukdRunPrintedSlotDrop runs the slot drop a refusal prints for a Postgres
// pgoutput source, through its SQL equivalent on the resolved
// (`sluice_`-prefixed) slot name, when the refusal names one.
func ukdRunPrintedSlotDrop(t *testing.T, refusal error, src twfbDB, slotName string) {
	t.Helper()
	if src.engine == "postgres" && strings.Contains(refusal.Error(), "sluice slot drop") {
		src.exec(t, "SELECT pg_drop_replication_slot('sluice_"+slotName+"')")
	}
}

// TestStreamer_UniqueKeyDoor_MySQLSource covers the binlog lane's
// stopped-window hole on residuals (a) and (b), and the keeps-the-key
// control.
func TestStreamer_UniqueKeyDoor_MySQLSource(t *testing.T) {
	myNew, myCleanup := ukcMySQLServer(t)
	defer myCleanup()
	_, pgRoot, pgCleanup := startPostgres(t)
	defer pgCleanup()
	pgRootDB := twfbDB{engine: "postgres", dsn: pgRoot}
	cells := []struct {
		name    string
		target  string
		shape   ukdShape
		control string
	}{
		{"mysql/keyless-one-key/stopped-drop", "mysql", ukdShapeA, ""},
		{"postgres/keyless-null-key/stopped-drop", "postgres", ukdShapeB, ""},
		{
			"mysql/pk/control-ddl-keeps-the-key", "mysql", ukdShapePK,
			"ALTER TABLE kd DROP INDEX kd_k, ADD UNIQUE KEY kd_k2 (k), ADD KEY kd_v (v); INSERT INTO kd VALUES (3,'c','three')",
		},
	}
	for i, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			tag := "ukdm" + string(rune('a'+i))
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
			cell := twfbCell{src: src, tgt: tgt, streamID: tag}
			ukdStoppedCell(t, cell, cell.streamer, c.shape, c.control)
		})
	}
}

// TestStreamer_UniqueKeyDoor_TriggerCDC covers a postgres-trigger source,
// whose change log records no DDL at all: the stopped window at open (and
// its drop-the-key remedy), the mid-stream re-check refusing before the
// duplicate (and the printed clean re-copy converging), the re-check
// window where the apply guard is the backstop, and a control that
// re-checks repeatedly and never refuses.
//
// Every table here has a primary key because the trigger engines refuse a
// table without one at setup (pgtrigger setup.go, ADR-0066 §14; the
// sqlite/d1 trigger setup likewise), so residuals (a) and (b) — both
// keyless — cannot occur on a trigger source at all: inside the re-check
// window the apply guard sees every merge. (Measured: pgtrigger.Setup
// refused the keyless kd this test first used.)
func TestStreamer_UniqueKeyDoor_TriggerCDC(t *testing.T) {
	if _, ok := engines.Get(pgtrigger.EngineName); !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	newCell := func(t *testing.T, tag string) (src, tgt twfbDB, newStreamer func() *Streamer, cleanup func()) {
		t.Helper()
		src, tgt, _, cleanup = kttcPairWith(t, tag, "mysql", ukdShapePK.pgCreate)
		return src, tgt, func() *Streamer { return kttcStreamer(t, src, tgt, tag) }, cleanup
	}

	t.Run("stopped-drop refuses at open", func(t *testing.T) {
		src, tgt, newStreamer, cleanup := newCell(t, "ukdta")
		defer cleanup()
		run := startTWFBRun(newStreamer())
		if !tgt.waitRow(t, "kd", 2, run, 180*time.Second) {
			t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
		}
		time.Sleep(3 * time.Second)
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("the clean stop returned %v", err)
		}
		before := ukdRows(t, tgt)
		src.exec(t, ukdShapePK.pgDrop)
		src.exec(t, ukdShapePK.duplicate)
		run = startTWFBRun(newStreamer())
		ukdAwaitExitOrConverge(t, run, src, tgt, 120*time.Second)
		refusal := ukdRequireRefusal(t, run, tgt, before, "kd.kd_k", ir.UniqueKeyDoorAtOpen)
		if refusal.SourceSeesDDLLive || !strings.Contains(refusal.Error(), "carries no DDL") {
			t.Errorf("a trigger source's refusal does not say its change stream carries no DDL: %v", refusal)
		}
		// Stopped across the DROP: the drop-the-key half of the remedy.
		tgt.exec(t, "ALTER TABLE kd DROP INDEX kd_k")
		ukdConvergeAndResume(t, newStreamer(), src, tgt, "INSERT INTO kd VALUES (8,'r','eight')")
	})

	t.Run("mid-stream re-check refuses before the merge", func(t *testing.T) {
		src, tgt, newStreamer, cleanup := newCell(t, "ukdtb")
		defer cleanup()
		s := newStreamer()
		s.uniqueKeyRecheckEvery = time.Second
		run := startTWFBRun(s)
		if !tgt.waitRow(t, "kd", 2, run, 180*time.Second) {
			t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
		}
		src.exec(t, "INSERT INTO kd VALUES (50,'p','fifty')")
		if !tgt.waitRow(t, "kd", 50, run, 60*time.Second) {
			t.Fatalf("the priming row never landed (%v)", run.stop(t))
		}
		before := ukdRows(t, tgt)
		src.exec(t, ukdShapePK.pgDrop)
		// More than a re-check period passes before the duplicate's
		// transaction reaches the intercept, so the re-check runs at its
		// TxBegin, after the DROP committed.
		time.Sleep(3 * time.Second)
		src.exec(t, ukdShapePK.duplicate)
		ukdAwaitExitOrConverge(t, run, src, tgt, 90*time.Second)
		refusal := ukdRequireRefusal(t, run, tgt, before, "kd.kd_k", ir.UniqueKeyDoorMidStream)
		if !strings.Contains(refusal.Error(), "up to 1s of them") || !strings.Contains(refusal.Error(), "--reset-target-data --yes") {
			t.Fatalf("the mid-stream refusal does not state its window and the clean re-copy: %v", refusal)
		}
		// The printed remedy: `sync start --reset-target-data --yes`, same id.
		r := newStreamer()
		r.ResetTargetData = true
		ukdConvergeAndResume(t, r, src, tgt, "INSERT INTO kd VALUES (70,'x','seventy')")
	})

	t.Run("inside one period the apply guard is the backstop", func(t *testing.T) {
		src, tgt, newStreamer, cleanup := newCell(t, "ukdtc")
		defer cleanup()
		s := newStreamer()
		s.uniqueKeyRecheckEvery = time.Hour
		run := startTWFBRun(s)
		if !tgt.waitRow(t, "kd", 2, run, 180*time.Second) {
			t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
		}
		// A CDC row first, so the stream-open door has run and the apply
		// loop is live before the DROP (the seed lands before the door).
		src.exec(t, "INSERT INTO kd VALUES (50,'p','fifty')")
		if !tgt.waitRow(t, "kd", 50, run, 60*time.Second) {
			t.Fatalf("the priming row never landed (%v)", run.stop(t))
		}
		before := ukdRows(t, tgt)
		// The DROP and the duplicate land inside one period: the re-check
		// cannot have run, so the guard must refuse the duplicate.
		src.exec(t, ukdShapePK.pgDrop)
		src.exec(t, ukdShapePK.duplicate)
		ukdAwaitExitOrConverge(t, run, src, tgt, 90*time.Second)
		if !run.exited() || !errors.Is(run.err, ir.ErrUniqueKeyConflictOtherRow) {
			_ = run.stop(t)
			t.Fatalf("the guard did not refuse inside the window: %v (target %s)", run.err, ukdRows(t, tgt))
		}
		if got := ukdRows(t, tgt); got != before {
			t.Fatalf("the target changed inside the window\nbefore %s\nafter  %s", before, got)
		}
	})

	t.Run("control: re-checks with no DDL never refuse", func(t *testing.T) {
		src, tgt, newStreamer, cleanup := newCell(t, "ukdtd")
		defer cleanup()
		s := newStreamer()
		s.uniqueKeyRecheckEvery = time.Second
		run := startTWFBRun(s)
		if !tgt.waitRow(t, "kd", 2, run, 180*time.Second) {
			t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
		}
		for i, row := range []string{"(10,'c','ten')", "(11,'d','eleven')", "(12,'e','twelve')"} {
			src.exec(t, "INSERT INTO kd VALUES "+row)
			if !tgt.waitRow(t, "kd", 10+i, run, 60*time.Second) {
				t.Fatalf("row %d never landed (%v)", 10+i, run.stop(t))
			}
			time.Sleep(1500 * time.Millisecond)
		}
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("the control stopped with %v", err)
		}
		if s, g := ukdRows(t, src), ukdRows(t, tgt); s != g {
			t.Fatalf("the control did not converge\nsource %s\ntarget %s", s, g)
		}
	})
}

// TestStreamer_UniqueKeyDoor_WorkingConfigurations pins that the door does
// not refuse the target keys sluice itself creates, across engines and key
// kinds, nor a renamed multi-database namespace, and that an operator's
// extra key refuses until accepted by name.
func TestStreamer_UniqueKeyDoor_WorkingConfigurations(t *testing.T) {
	myNew, myCleanup := ukcMySQLServer(t)
	defer myCleanup()
	pgNew, pgCleanup := ukcPGServer(t)
	defer pgCleanup()

	// Cold start, a warm restart (the door runs on both), one CDC row.
	coldThenWarm := func(t *testing.T, cell twfbCell, newStreamer func() *Streamer) {
		t.Helper()
		run := startTWFBRun(newStreamer())
		if !cell.tgt.waitRow(t, "kd", 2, run, 180*time.Second) {
			t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
		}
		cell.waitStreaming(t, run)
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("the cold start stopped with %v", err)
		}
		ukdConvergeAndResume(t, newStreamer(), cell.src, cell.tgt, "INSERT INTO kd VALUES (9,'zz','nine', 'zz', 9)")
	}
	const myKeys = "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) NOT NULL, v VARCHAR(20), c VARCHAR(40), n INT, " +
		"UNIQUE KEY kd_lk ((lower(k))), UNIQUE KEY kd_cp (c(5)), UNIQUE KEY kd_vn (v, n)); " +
		"INSERT INTO kd VALUES (1,'a','one','alpha1',1),(2,'b','two','bravo2',2)"
	const pgKeys = "CREATE TABLE kd (id int NOT NULL PRIMARY KEY, k varchar(20) NOT NULL, v varchar(20), c varchar(40), n int); " +
		"CREATE UNIQUE INDEX kd_lk ON kd (lower(k)); CREATE UNIQUE INDEX kd_pv ON kd (v) WHERE n > 0; " +
		"CREATE UNIQUE INDEX kd_vn ON kd (n, v); ALTER TABLE kd ADD CONSTRAINT kd_c UNIQUE (c); " +
		"INSERT INTO kd VALUES (1,'a','one','alpha1',1),(2,'b','two','bravo2',2)"

	t.Run("mysql to mysql: functional, prefix and composite keys", func(t *testing.T) {
		src, tgt := myNew("ukdwa_s"), myNew("ukdwa_t")
		src.exec(t, myKeys)
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdwa"}
		coldThenWarm(t, cell, cell.streamer)
	})
	t.Run("postgres to postgres: expression, partial, composite and constraint keys", func(t *testing.T) {
		src, tgt := pgNew("ukdwb_s"), pgNew("ukdwb_t")
		src.exec(t, pgKeys+"; ALTER TABLE kd REPLICA IDENTITY FULL")
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdwb", slotName: "ukdwb"}
		coldThenWarm(t, cell, cell.streamer)
	})
	t.Run("postgres to mysql: an expression key across dialects", func(t *testing.T) {
		src, tgt := pgNew("ukdwc_s"), myNew("ukdwc_t")
		src.exec(t, "CREATE TABLE kd (id int NOT NULL PRIMARY KEY, k varchar(20) NOT NULL, v varchar(20), c varchar(40), n int); "+
			"CREATE UNIQUE INDEX kd_lk ON kd (lower(k)); CREATE UNIQUE INDEX kd_vn ON kd (n, v); "+
			"INSERT INTO kd VALUES (1,'a','one','alpha1',1),(2,'b','two','bravo2',2)")
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdwc", slotName: "ukdwc"}
		coldThenWarm(t, cell, cell.streamer)
	})
	t.Run("mysql to postgres: a mixed-case table name", func(t *testing.T) {
		src, tgt := myNew("ukdwd_s"), pgNew("ukdwd_t")
		src.exec(t, "CREATE TABLE Kd_Mixed (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) NOT NULL, UNIQUE KEY km_k (k)); "+
			"INSERT INTO Kd_Mixed VALUES (1,'a')")
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdwd"}
		run := startTWFBRun(cell.streamer())
		cell.waitStreaming(t, run)
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("the cold start stopped with %v", err)
		}
		run = startTWFBRun(cell.streamer())
		time.Sleep(10 * time.Second)
		if run.exited() {
			t.Fatalf("the warm restart exited: %v", run.err)
		}
		_ = run.stop(t)
	})
	t.Run("an operator's extra target key refuses until accepted by name", func(t *testing.T) {
		src, tgt := myNew("ukdwe_s"), myNew("ukdwe_t")
		src.exec(t, ukdShapePK.myCreate)
		// The target table pre-created by the operator, with one more key.
		tgt.exec(t, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) NOT NULL, v VARCHAR(20), "+
			"UNIQUE KEY kd_k (k), UNIQUE KEY kd_v (v))")
		cell := twfbCell{src: src, tgt: tgt, streamID: "ukdwe"}
		run := startTWFBRun(cell.streamer())
		deadline := time.Now().Add(180 * time.Second)
		for time.Now().Before(deadline) && !run.exited() {
			time.Sleep(300 * time.Millisecond)
		}
		if !run.exited() || !errors.Is(run.err, ir.ErrSourceUniqueKeyAbsent) || !strings.Contains(run.err.Error(), "kd.kd_v") {
			_ = run.stop(t)
			t.Fatalf("the extra key did not refuse at open: %v", run.err)
		}
		t.Logf("refusal: %v", run.err)
		accepted := func() *Streamer {
			s := cell.streamer()
			s.AcceptTargetOnlyUniqueKeys = []string{ukdAcceptFromRefusal(t, run.err, "kd_v")}
			return s
		}
		ukdConvergeAndResume(t, accepted(), src, tgt, "INSERT INTO kd VALUES (9,'z','nine')")
	})
	t.Run("multi-database with a renamed namespace", func(t *testing.T) {
		// The fan-out names each source database; a1 lands in a renamed
		// target database. A server-level DSN reaches both.
		src := myNew("ukdwf_a1")
		home := myNew("ukdwf_home")
		src.exec(t, ukdShapePK.myCreate)
		nsMap, err := NewNamespaceRenameMap([]string{"ukdwf_a1=ukdwf_b1"})
		if err != nil {
			t.Fatal(err)
		}
		cell := twfbCell{
			src: twfbDB{engine: "mysql", dsn: serverDSN(t, src.dsn)}, tgt: home,
			streamID: "ukdwf", databases: []string{"ukdwf_a1"},
		}
		newStreamer := func() *Streamer {
			s := cell.streamer()
			s.NamespaceMap = nsMap
			return s
		}
		b1DSN, err := buildMySQLDSN(src.dsn, "ukdwf_b1")
		if err != nil {
			t.Fatal(err)
		}
		b1 := twfbDB{engine: "mysql", dsn: b1DSN}
		run := startTWFBRun(newStreamer())
		if !b1.waitRow(t, "kd", 2, run, 180*time.Second) {
			t.Fatalf("the fan-out cold start never delivered the seed (%v)", run.stop(t))
		}
		cell.waitStreaming(t, run)
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("the cold start stopped with %v", err)
		}
		// Warm restart passes (the renamed namespace is judged against its
		// own source database)...
		run = startTWFBRun(newStreamer())
		time.Sleep(10 * time.Second)
		if run.exited() {
			t.Fatalf("the warm restart exited: %v", run.err)
		}
		_ = run.stop(t)
		// ...and a stopped-window DROP there refuses, naming the TARGET namespace
		// (ukdwf_b1, the renamed one): the table the operator must change.
		src.exec(t, "ALTER TABLE kd DROP INDEX kd_k")
		run = startTWFBRun(newStreamer())
		deadline := time.Now().Add(120 * time.Second)
		for time.Now().Before(deadline) && !run.exited() {
			time.Sleep(300 * time.Millisecond)
		}
		if !run.exited() || !errors.Is(run.err, ir.ErrSourceUniqueKeyAbsent) || !strings.Contains(run.err.Error(), "ukdwf_b1.kd.kd_k") {
			_ = run.stop(t)
			t.Fatalf("the fan-out did not refuse the namespace's dropped key: %v", run.err)
		}
		t.Logf("refusal: %v", run.err)
	})
}

// TestStreamer_UniqueKeyDoor_AddTable pins the `schema add-table` entry: a
// PRE-CREATED target table carrying a unique key the source table does not
// have refuses before anything is copied, and both remedies the refusal
// prints are run — dropping the key and running add-table again (which
// converges), and accepting the key by name (where the key cannot hold the
// source's rows, so the copy must refuse rather than merge them).
func TestStreamer_UniqueKeyDoor_AddTable(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()
	applyDDLMySQL(t, srcDSN, "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL); "+
		"INSERT INTO users VALUES (1, 'alice@example.com');")
	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	const streamID = "ukd-add-table"
	streamer := &Streamer{
		Source: mysqlEng, Target: mysqlEng, SourceDSN: srcDSN, TargetDSN: tgtDSN, StreamID: streamID,
		Filter: mustNewFilter(t, []string{"users"}, nil),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()
	if !waitForRowCountMySQL(t, tgtDSN, "users", 1, 60*time.Second) {
		t.Fatal("the cold start never delivered the seed row")
	}
	// A CDC row, so the stream's control row exists for add-table's preflight.
	applyDDLMySQL(t, srcDSN, "INSERT INTO users VALUES (2, 'warmup@example.com');")
	if !waitForRowCountMySQL(t, tgtDSN, "users", 2, 60*time.Second) {
		t.Fatal("the stream never delivered the warmup row")
	}
	tgt := twfbDB{engine: "mysql", dsn: tgtDSN}

	for _, tc := range []struct{ table, remedy string }{{"orders_a", "drop"}, {"orders_b", "accept"}} {
		t.Run(tc.table+"/"+tc.remedy, func(t *testing.T) {
			applyDDLMySQL(t, srcDSN, "CREATE TABLE "+tc.table+" (id BIGINT NOT NULL PRIMARY KEY, customer BIGINT NOT NULL); "+
				"INSERT INTO "+tc.table+" VALUES (1, 7), (2, 7);")
			// The operator's table: one more key, which the source's rows
			// (two orders for customer 7) cannot both satisfy.
			applyDDLMySQL(t, tgtDSN, "CREATE TABLE "+tc.table+" (id BIGINT NOT NULL PRIMARY KEY, customer BIGINT NOT NULL, "+
				"UNIQUE KEY o_customer (customer));")
			add := func(accept []string) error {
				actx, acancel := context.WithTimeout(context.Background(), 90*time.Second)
				defer acancel()
				return (&AddTable{
					Source: mysqlEng, Target: mysqlEng, SourceDSN: srcDSN, TargetDSN: tgtDSN,
					StreamID: streamID, TableName: tc.table, LiveMode: true,
					AcceptTargetOnlyUniqueKeys: accept,
				}).Run(actx)
			}
			err := add(nil)
			var refusal *ir.SourceUniqueKeyAbsentError
			if !errors.As(err, &refusal) || refusal.When != ir.UniqueKeyDoorAddTable ||
				!strings.Contains(err.Error(), tc.table+".o_customer (customer)") {
				t.Fatalf("add-table onto the pre-created table returned %v; want the add-table SOURCE-UNIQUE-KEY-ABSENT", err)
			}
			t.Logf("refusal: %v", err)
			if n := tgt.scalar(t, "SELECT count(*) FROM "+tc.table); n != "0" {
				t.Fatalf("the refusing add-table copied %s rows", n)
			}
			if tc.remedy == "accept" {
				// The accepted key cannot hold the source's two rows: the copy
				// must refuse loudly rather than merge them.
				err = add([]string{ukdAcceptFromRefusal(t, err, "o_customer")})
				if err == nil {
					t.Fatalf("the accepted key held two source rows for customer 7: ids %s",
						tgt.scalar(t, "SELECT group_concat(id) FROM "+tc.table))
				}
				if errors.Is(err, ir.ErrSourceUniqueKeyAbsent) {
					t.Fatalf("the accepted key still refused at the door: %v", err)
				}
				t.Logf("the accepted key's copy refused, as it must: %v", err)
				return
			}
			applyDDLMySQL(t, tgtDSN, "ALTER TABLE "+tc.table+" DROP INDEX o_customer;")
			if err := add(nil); err != nil {
				t.Fatalf("add-table after the printed remedy: %v", err)
			}
			if !waitForRowCountMySQL(t, tgtDSN, tc.table, 2, 60*time.Second) {
				t.Fatal("the add-table copy did not deliver both rows after the remedy")
			}
		})
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("Streamer.Run returned %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Streamer.Run did not return after cancel")
	}
}
