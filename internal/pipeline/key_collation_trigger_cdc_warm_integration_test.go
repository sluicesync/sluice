//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The warm-resume half of KEY-COLLATION-TRIGGER-CDC-NO-HISTORY: shapes the
// first cut's pins never restarted on, found by the review of 288789f2 and
// measured there and here before the fix.
//
//   - TRIGGER-WARM-RESUME-NO-PRIOR: a narrowing --type-override
//     (varchar(50) → varchar(20), bigint → int) refused SCHEMA-CHANGE-REFUSED
//     on EVERY warm resume, on MySQL and Postgres targets. The
//     unforwarded-stream check judges an overridden column against the
//     table's prior, and the warm-resume load that supplies it ran only for
//     readers that take a schema seed, which the trigger reader does not.
//   - TYPE-WIRE-EMPTY-SLICE-SIGNATURE: a domain without a CHECK beside a
//     jsonb column wedged every warm resume onto a Postgres target ("failed
//     to encode args[1] … unknown type (OID 0)"). The schema reader spells
//     the domain's checks as an empty slice, schema history reads them back
//     as nil, the applier's history prime called the two a schema change and
//     dropped the table into its schema-dirty path, which cannot encode the
//     trigger reader's decoded jsonb.
//
// Each cell cold-starts, applies a CDC row, stops cleanly, warm-resumes and
// must apply the next row and every row after it. The independent expected
// value is the source's own row count and id set, read off the source.

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	"sluicesync.dev/sluice/internal/ir"
)

// kttwCell is one warm-resume shape: the source DDL (table wr), the rows
// written in each phase, and the stream's --type-override list.
type kttwCell struct {
	name     string
	ddl      string
	row      func(id string) string
	mappings []config.Mapping
}

func kttwCells() []kttwCell {
	return []kttwCell{
		{
			name: "narrowing-type-override",
			ddl:  "CREATE TABLE wr (id int PRIMARY KEY, note varchar(50) NOT NULL, big bigint)",
			row:  func(id string) string { return "INSERT INTO wr VALUES (" + id + ",'n" + id + "'," + id + ")" },
			mappings: []config.Mapping{
				{Table: "wr", Column: "note", TargetType: "varchar", TargetTypeOptions: map[string]any{"length": 20}},
				{Table: "wr", Column: "big", TargetType: "int"},
			},
		},
		{
			name: "domain-jsonb-arrays",
			ddl: "CREATE DOMAIN shorttext AS varchar(30); " +
				"CREATE TABLE wr (id int PRIMARY KEY, st shorttext, jb jsonb, ia int[], ta2 text[])",
			row: func(id string) string {
				return "INSERT INTO wr VALUES (" + id + ",'s" + id + "','{\"k\":" + id + "}','{" + id + "}','{{a,b},{c,d}}')"
			},
		},
	}
}

// TestKeyCollationTriggerCDC_WarmResumeShapes runs every cell on a MySQL and a
// Postgres target.
func TestKeyCollationTriggerCDC_WarmResumeShapes(t *testing.T) {
	for _, c := range kttwCells() {
		for _, target := range []string{"mysql", "postgres"} {
			t.Run(c.name+"/"+target, func(t *testing.T) { kttwRunCell(t, c, target) })
		}
	}
}

func kttwRunCell(t *testing.T, c kttwCell, target string) {
	srcDSN, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()
	tgt := twfbDB{engine: "postgres", dsn: pgTarget}
	if target == "mysql" {
		_, myRoot, myCleanup := startMySQL(t)
		defer myCleanup()
		twfbDB{engine: "mysql", dsn: myRoot}.exec(t, "CREATE DATABASE kttw_t")
		d, err := buildMySQLDSN(myRoot, "kttw_t")
		if err != nil {
			t.Fatal(err)
		}
		tgt = twfbDB{engine: "mysql", dsn: d}
	}
	src := twfbDB{engine: "postgres", dsn: srcDSN}
	src.exec(t, c.ddl+"; "+c.row("1"))
	if _, err := pgtrigger.Setup(context.Background(), srcDSN, pgtrigger.SetupOptions{Tables: []string{"wr"}, Schema: "public"}); err != nil {
		t.Fatalf("pgtrigger.Setup: %v", err)
	}
	streamer := func() *Streamer {
		s := kttcStreamer(t, src, tgt, "kttw")
		s.Mappings = c.mappings
		return s
	}
	cell := twfbCell{src: src, tgt: tgt, streamID: "kttw"}
	run := startTWFBRun(streamer())
	if !tgt.waitRow(t, "wr", 1, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	src.exec(t, c.row("2"))
	if !tgt.waitRow(t, "wr", 2, run, 60*time.Second) {
		t.Fatalf("the cold session's CDC row never landed (%v)", run.stop(t))
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("the clean stop returned %v", err)
	}

	// Two warm resumes: the second primes from the versions both earlier
	// sessions recorded.
	for i, id := range []string{"3", "4"} {
		run = startTWFBRun(streamer())
		src.exec(t, c.row(id))
		if !tgt.waitRow(t, "wr", int(id[0]-'0'), run, 60*time.Second) {
			t.Fatalf("warm resume %d did not apply row %s: %v", i+1, id, run.stop(t))
		}
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("warm resume %d stopped with %v", i+1, err)
		}
	}
	q := "SELECT string_agg(id::text, ',' ORDER BY id) FROM wr"
	if target == "mysql" {
		q = "SELECT GROUP_CONCAT(id ORDER BY id) FROM wr"
	}
	want := src.scalar(t, "SELECT string_agg(id::text, ',' ORDER BY id) FROM wr")
	if got := tgt.scalar(t, q); got != want || strings.HasPrefix(want, "<") {
		t.Errorf("target ids %q; source ids %q", got, want)
	}
}

// TestKeyCollationTriggerCDC_KeyShapes widens the steady-state pin beyond the
// one UNIQUE varchar key on the default collation: a "C" primary key, a "C"
// UNIQUE key, a two-column UNIQUE key and a CHAR key, each on a MySQL target
// with 'A' inserted beside 'a' over CDC. Each cell must land exactly or refuse
// KEY-COLLATION-MERGE with nothing merged (kcfbGrade, against the source's own
// rows). A primary key on the database DEFAULT collation is deliberately not
// here: the guard leaves it unarmed by documented choice
// (KEY-COLLATION-PG-DEFAULT-TO-MYSQL, uniqueOnly), on every Postgres lane.
func TestKeyCollationTriggerCDC_KeyShapes(t *testing.T) {
	for _, c := range []struct{ name, create, collide string }{
		{
			"pk-C",
			`CREATE TABLE kd (id int NOT NULL, k varchar(20) COLLATE "C" NOT NULL PRIMARY KEY, d int); INSERT INTO kd VALUES (1,'a',1),(2,'b',2),(3,'c',3)`,
			"INSERT INTO kd VALUES (99,'A',99)",
		},
		{
			"unique-C",
			`CREATE TABLE kd (id int PRIMARY KEY, k varchar(20) COLLATE "C" NOT NULL UNIQUE, d int); INSERT INTO kd VALUES (1,'a',1),(2,'b',2),(3,'c',3)`,
			"INSERT INTO kd VALUES (99,'A',99)",
		},
		{
			"unique-two-columns",
			`CREATE TABLE kd (id int PRIMARY KEY, k varchar(20) NOT NULL, k2 varchar(20) NOT NULL, UNIQUE (k, k2)); INSERT INTO kd VALUES (1,'a','x'),(2,'b','x'),(3,'c','x')`,
			"INSERT INTO kd VALUES (99,'A','x')",
		},
		{
			"unique-char",
			`CREATE TABLE kd (id int PRIMARY KEY, k char(10) NOT NULL UNIQUE, d int); INSERT INTO kd VALUES (1,'a',1),(2,'b',2),(3,'c',3)`,
			"INSERT INTO kd VALUES (99,'A',99)",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			src, tgt, _, cleanup := kttcPairWith(t, "ktks", "mysql", c.create)
			defer cleanup()
			cell := twfbCell{src: src, tgt: tgt, streamID: "ktks"}
			run := startTWFBRun(kttcStreamer(t, src, tgt, cell.streamID))
			if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
				t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
			}
			cell.waitStreaming(t, run)
			src.exec(t, c.collide)
			_ = tgt.waitRow(t, "kd", 99, run, 30*time.Second)
			kcfbWaitConverged(t, src, tgt, run, 15*time.Second)
			var refusal error
			if run.exited() {
				refusal = run.err
				if refusal == nil || errors.Is(refusal, context.Canceled) {
					t.Fatal("the stream exited without an error")
				}
			} else if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("the stream stopped with %v", err)
			}
			t.Logf("refusal %v; source %s target %s", refusal, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
			kcfbGrade(t, "pgtrigger key shape "+c.name, src, tgt, refusal)
			if refusal != nil && !errors.Is(refusal, ir.ErrKeyCollationMerge) {
				t.Errorf("refused with %v; want %v or an exact landing", refusal, ir.ErrKeyCollationMerge)
			}
		})
	}
}

// ktt2Pair boots a Postgres source with t1 and kd (create) captured, and a
// MySQL target database. polled strips the install to the
// --allow-polled-fingerprint tier's shape: no DDL capture function and no DDL
// event trigger, so no DDL leaves a marker.
func ktt2Pair(t *testing.T, create string, polled bool) (src, tgt twfbDB, cleanup func()) {
	t.Helper()
	src, tgt, _, cleanup = kttcPairWith(t, "ktt2", "mysql", "CREATE TABLE t1 (id int PRIMARY KEY, v text); INSERT INTO t1 VALUES (1,'x'); "+create)
	if _, err := pgtrigger.Setup(context.Background(), src.dsn, pgtrigger.SetupOptions{Tables: []string{"t1", "kd"}, Schema: "public"}); err != nil {
		cleanup()
		t.Fatalf("pgtrigger.Setup: %v", err)
	}
	if polled {
		src.exec(t, "DROP EVENT TRIGGER IF EXISTS "+pgtrigger.CaptureTriggerDDL+"; DROP EVENT TRIGGER IF EXISTS "+pgtrigger.CaptureTriggerDrop+"; "+
			"DROP FUNCTION IF EXISTS public."+pgtrigger.CaptureFunctionDDL+"(); DROP FUNCTION IF EXISTS public."+pgtrigger.CaptureFunctionDrop+"()")
	}
	return src, tgt, cleanup
}

// ktt2Settle waits for the colliding row or the run's exit and returns the
// refusal (nil if the run is still up, which it then stops).
func ktt2Settle(t *testing.T, src, tgt twfbDB, run *twfbRun) error {
	t.Helper()
	_ = tgt.waitRow(t, "kd", 99, run, 30*time.Second)
	kcfbWaitConverged(t, src, tgt, run, 15*time.Second)
	if run.exited() {
		if run.err == nil || errors.Is(run.err, context.Canceled) {
			t.Fatal("the stream exited without an error")
		}
		return run.err
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("the stream stopped with %v", err)
	}
	return nil
}

// TestKeyCollationTriggerCDC_TwoTableCacheHit grades the per-schema shape
// cache's HIT branch, which every single-table pin skipped (third review,
// P1): t1's change is the schema's first touch, so kd's boundary is served
// from the read t1 triggered. 'A' beside 'a' on kd's UNIQUE key must refuse
// KEY-COLLATION-MERGE with nothing merged — on a "C" key, and (fourth review)
// on a key under the database's default collation, which the collation read
// reports with no name, stamped deterministic, and which reaches the guard's
// unique-key-only path beside the integer primary key.
func TestKeyCollationTriggerCDC_TwoTableCacheHit(t *testing.T) {
	for name, collate := range map[string]string{"C": `COLLATE "C" `, "database-default": ""} {
		t.Run(name, func(t *testing.T) {
			src, tgt, cleanup := ktt2Pair(t, `CREATE TABLE kd (id int PRIMARY KEY, k varchar(20) `+collate+`NOT NULL UNIQUE, d int); INSERT INTO kd VALUES (1,'a',1),(2,'b',2),(3,'c',3)`, false)
			defer cleanup()
			cell := twfbCell{src: src, tgt: tgt, streamID: "ktt2"}
			run := startTWFBRun(kttcStreamer(t, src, tgt, cell.streamID))
			if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
				t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
			}
			cell.waitStreaming(t, run)
			src.exec(t, "INSERT INTO t1 VALUES (2,'y')")
			if !tgt.waitRow(t, "t1", 2, run, 60*time.Second) {
				t.Fatalf("t1's change never landed (%v)", run.stop(t))
			}
			src.exec(t, "INSERT INTO kd VALUES (99,'A',99)")
			refusal := ktt2Settle(t, src, tgt, run)
			t.Logf("refusal %v; source %s target %s", refusal, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
			kcfbGrade(t, "pgtrigger two-table cache hit ("+name+")", src, tgt, refusal)
			if !errors.Is(refusal, ir.ErrKeyCollationMerge) {
				t.Errorf("refused with %v; want %v", refusal, ir.ErrKeyCollationMerge)
			}
		})
	}
}

// TestKeyCollationTriggerCDC_TwoTableMarkerHalts is the event-trigger tier's
// half of the cache's soundness argument: a DDL on kd AFTER the per-schema
// read (t1's first touch) writes a marker, and the stream halts at it before
// any of kd's later rows is applied, so the stale cached shape never governs
// a row. The independent evidence is the target: kd row 99 is absent and kd
// gained no column.
func TestKeyCollationTriggerCDC_TwoTableMarkerHalts(t *testing.T) {
	src, tgt, cleanup := ktt2Pair(t, `CREATE TABLE kd (id int PRIMARY KEY, k varchar(20) COLLATE "C" NOT NULL UNIQUE, d int); INSERT INTO kd VALUES (1,'a',1),(2,'b',2),(3,'c',3)`, false)
	defer cleanup()
	cell := twfbCell{src: src, tgt: tgt, streamID: "ktt2"}
	run := startTWFBRun(kttcStreamer(t, src, tgt, cell.streamID))
	if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	src.exec(t, "INSERT INTO t1 VALUES (2,'y')")
	if !tgt.waitRow(t, "t1", 2, run, 60*time.Second) {
		t.Fatalf("t1's change never landed (%v)", run.stop(t))
	}
	src.exec(t, "ALTER TABLE kd ADD COLUMN extra int")
	src.exec(t, "INSERT INTO kd (id,k,d,extra) VALUES (99,'z',99,7)")
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && !run.exited() {
		time.Sleep(250 * time.Millisecond)
	}
	if !run.exited() {
		_ = run.stop(t)
		t.Fatal("the stream kept running past a DDL on a captured table; the marker must halt it")
	}
	t.Logf("halted: %v", run.err)
	if !strings.Contains(fmt.Sprint(run.err), "observed source-side DDL") {
		t.Errorf("halted with %v; want the reader's observed-DDL refusal", run.err)
	}
	if tgt.hasRow(t, "kd", 99) {
		t.Error("kd row 99, written after the DDL, reached the target under the stale cached shape")
	}
	if n := tgt.scalar(t, "SELECT CAST(COUNT(*) AS CHAR) FROM information_schema.columns WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kd' AND COLUMN_NAME = 'extra'"); n != "0" {
		t.Errorf("the target gained kd.extra (%s): the DDL was forwarded", n)
	}
}

// TestKeyCollationTriggerCDC_PolledTierKeyAddedAfterFirstTouch is the third
// review's S1 scenario end to end, on the polled tier where no DDL leaves a
// marker: t1's change is the schema's first touch; a UNIQUE (k) is then added
// to kd on both sides; kd's first change writes 'A' beside 'a'. It must refuse
// KEY-COLLATION-MERGE with nothing merged. (The guards take their keys from
// the TARGET's catalog and only the column collations from the source shape,
// so whether a stale shape could have unarmed this is graded by the mutation
// run recorded in the commit, not assumed.)
func TestKeyCollationTriggerCDC_PolledTierKeyAddedAfterFirstTouch(t *testing.T) {
	src, tgt, cleanup := ktt2Pair(t, `CREATE TABLE kd (id int PRIMARY KEY, k varchar(20) NOT NULL, d int); INSERT INTO kd VALUES (1,'a',1),(2,'b',2),(3,'c',3)`, true)
	defer cleanup()
	cell := twfbCell{src: src, tgt: tgt, streamID: "ktt2"}
	run := startTWFBRun(kttcStreamer(t, src, tgt, cell.streamID))
	if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	src.exec(t, "INSERT INTO t1 VALUES (2,'y')")
	if !tgt.waitRow(t, "t1", 2, run, 60*time.Second) {
		t.Fatalf("t1's change never landed (%v)", run.stop(t))
	}
	src.exec(t, "ALTER TABLE kd ADD CONSTRAINT kd_k UNIQUE (k)")
	tgt.exec(t, "ALTER TABLE kd ADD UNIQUE KEY kd_k (k)")
	src.exec(t, "INSERT INTO kd VALUES (99,'A',99)")
	refusal := ktt2Settle(t, src, tgt, run)
	t.Logf("refusal %v; source %s target %s", refusal, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
	kcfbGrade(t, "pgtrigger polled tier, key added after first touch", src, tgt, refusal)
	if !errors.Is(refusal, ir.ErrKeyCollationMerge) {
		t.Errorf("refused with %v; want %v", refusal, ir.ErrKeyCollationMerge)
	}
}
