//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

// KEY-COLLATION-FUNCTIONAL-KEY-PART end to end. A Postgres `UNIQUE
// (lower(k))` reaches MySQL as a functional key part `((lower(k)))` on a
// column sluice creates under the target's default `utf8mb4_0900_ai_ci`.
// Postgres keeps lower('e') and lower('é') apart; the target's functional key
// equates them, so the source's second row met the first through an
// INSERT … ON DUPLICATE KEY UPDATE and overwrote it at exit 0. The guard read
// the target's unique keys with the functional part skipped, so it saw no
// key holding the armed column and the table ran unguarded.
//
// Cells: the CDC apply path, every idempotent copy path (krwPaths), a fresh
// migrate and a sync cold start over a source already holding both rows
// (loud before the fix: the unique index is built after the copy), the
// plain-key and no-collision controls, and the reverse direction — a MySQL
// `UNIQUE ((lower(k)))` onto a Postgres key whose collation equates the pair,
// loud by construction (a Postgres upsert never arbitrates on an expression
// index, so a conflict through one is a unique_violation).
//
// The independent expected value is the source's own rows read by
// id → HEX(k) (kcfbRows) or HEX(k):v (krwRows).

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// kfkPGFunctionalTable is a Postgres table whose only unique key besides
// the primary key is functional, over a default-collation column.
const kfkPGFunctionalTable = "CREATE TABLE kd (id int NOT NULL PRIMARY KEY, k varchar(20) NOT NULL, d int); " +
	"CREATE UNIQUE INDEX kd_lower_k ON kd (lower(k)); ALTER TABLE kd REPLICA IDENTITY FULL;"

// TestKeyCollationFunctionalKey_PostgresToMySQLApply is the CDC apply cell:
// the cold start copies 'e', then the stream delivers 'é' as row 99.
func TestKeyCollationFunctionalKey_PostgresToMySQLApply(t *testing.T) {
	srcRoot, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()

	cells := []struct {
		tag, create, collide string
		wantRefusal          bool
		// tgtAlter runs on the target once the stream is in CDC.
		tgtAlter string
		// remedy, when set, is applied on the target after the refusal; the
		// restarted stream must then deliver every row exactly.
		remedy string
	}{
		// The finding: a functional key the guard skipped. Its remedy is the
		// one the refusal names — the key recreated under a binary collation.
		{
			tag: "kfa", create: kfkPGFunctionalTable, collide: "é", wantRefusal: true,
			remedy: "ALTER TABLE kd DROP INDEX kd_lower_k, ADD UNIQUE KEY kd_lower_k ((lower(cast(`k` as char charset utf8mb4)) COLLATE utf8mb4_bin))",
		},
		// Two functional parts over different columns, the merge through both.
		{
			tag: "kfb", collide: "é", wantRefusal: true,
			create: "CREATE TABLE kd (id int NOT NULL PRIMARY KEY, k varchar(20) NOT NULL, j varchar(20) NOT NULL DEFAULT 'x', d int); " +
				"CREATE UNIQUE INDEX kd_lower_kj ON kd (lower(k), upper(j)); ALTER TABLE kd REPLICA IDENTITY FULL;",
		},
		// Control: the functional key with a pair the target keeps apart
		// converges exactly — the guard must not refuse it.
		{tag: "kfc", create: kfkPGFunctionalTable, collide: "f"},
		// Control: the plain-column key, unchanged behavior (refuses).
		{
			tag: "kfd", collide: "é", wantRefusal: true,
			create: "CREATE TABLE kd (id int NOT NULL PRIMARY KEY, k varchar(20) NOT NULL UNIQUE, d int); ALTER TABLE kd REPLICA IDENTITY FULL;",
		},
		// The column altered to a binary collation after the key was built:
		// MySQL keeps the functional key's hidden column on the collation it
		// was created with, so the key still equates 'e' and 'é'. A guard that
		// re-derived the expression's collation from the column would compare
		// under utf8mb4_bin and let the merge through. The key is first
		// rebuilt without the CAST sluice emits (a MySQL source's own
		// `((lower(k)))`), whose collation follows the column's: under the
		// CAST the expression takes the character set's default whatever the
		// column says, so the re-derived and the recorded collation agree and
		// the cell would not tell them apart.
		{
			tag: "kfe", create: kfkPGFunctionalTable, collide: "é", wantRefusal: true,
			tgtAlter: "ALTER TABLE kd DROP INDEX kd_lower_k, ADD UNIQUE KEY kd_lower_k ((lower(k))); " +
				"ALTER TABLE kd MODIFY k varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL",
		},
	}
	for _, c := range cells {
		t.Run(c.tag, func(t *testing.T) {
			src, tgt := kfkNewPair(t, srcRoot, myRoot, c.tag)
			cell := twfbCell{src: src, tgt: tgt, streamID: c.tag, slotName: c.tag}
			src.exec(t, c.create+" INSERT INTO kd (id,k,d) VALUES (1,'e',1),(2,'b',2),(3,'c',3)")
			run := startTWFBRun(cell.streamer())
			if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
				t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
			}
			cell.waitStreaming(t, run)
			if c.tgtAlter != "" {
				tgt.exec(t, c.tgtAlter)
			}
			t.Logf("target: %s", tgt.scalar(t, "SELECT GROUP_CONCAT(CONCAT(index_name, ':', COALESCE(column_name, expression)) "+
				"SEPARATOR ', ') FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'kd' AND non_unique = 0"))
			src.exec(t, "INSERT INTO kd (id,k,d) VALUES (99,'"+c.collide+"',99)")
			src.exec(t, "INSERT INTO kd (id,k,d) VALUES (100,'zz',100)")
			_ = tgt.waitRow(t, "kd", 100, run, 60*time.Second)
			kcfbWaitConverged(t, src, tgt, run, 20*time.Second)
			refusal := kfkRefusal(t, run)
			if c.wantRefusal {
				if refusal == nil {
					t.Fatalf("nothing refused: the target's functional key equates 'e' with %q, so row 99 cannot land "+
						"exactly (source %s, target %s)", c.collide, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
				}
				if !errors.Is(refusal, ir.ErrKeyCollationMerge) {
					t.Errorf("refused with %v; want %v", refusal, ir.ErrKeyCollationMerge)
				}
			} else if refusal != nil {
				t.Fatalf("refused a row the target key keeps apart: %v", refusal)
			}
			kcfbGrade(t, c.tag, src, tgt, refusal)
			if c.remedy == "" {
				return
			}
			if !strings.Contains(refusal.Error(), "kd_lower_k") {
				t.Errorf("the refusal does not name the functional key to recreate: %v", refusal)
			}
			tgt.exec(t, c.remedy)
			again := startTWFBRun(cell.streamer())
			_ = tgt.waitRow(t, "kd", 100, again, 60*time.Second)
			kcfbWaitConverged(t, src, tgt, again, 20*time.Second)
			if again.exited() {
				t.Fatalf("after the remedy the stream stopped again: %v", again.err)
			}
			if err := again.stop(t); err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("after the remedy the stream stopped with %v", err)
			}
			kcfbGrade(t, c.tag+" (after the remedy)", src, tgt, nil)
		})
	}
}

// TestKeyCollationFunctionalKey_PostgresToMySQLColdStart: a source already
// holding both rows, through a fresh migrate and a sync cold start. Either
// refuses (with nothing merged) or lands exactly.
func TestKeyCollationFunctionalKey_PostgresToMySQLColdStart(t *testing.T) {
	srcRoot, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	seed := kfkPGFunctionalTable + " INSERT INTO kd (id,k,d) VALUES (1,'e',1),(2,'b',2),(3,'c',3),(99,'é',99)"

	t.Run("migrate", func(t *testing.T) {
		src, tgt := kfkNewPair(t, srcRoot, myRoot, "kfm")
		src.exec(t, seed)
		srcEng, _ := engines.Get("postgres")
		tgtEng, _ := engines.Get("mysql")
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		err := (&Migrator{Source: srcEng, Target: tgtEng, SourceDSN: src.dsn, TargetDSN: tgt.dsn}).Run(ctx)
		t.Logf("migrate: %v", err)
		kcfbGradeWith(t, "migrate", src, tgt, err, true) // row 99 is in the snapshot
	})
	t.Run("sync cold start", func(t *testing.T) {
		src, tgt := kfkNewPair(t, srcRoot, myRoot, "kfs")
		src.exec(t, seed)
		cell := twfbCell{src: src, tgt: tgt, streamID: "kfs", slotName: "kfs"}
		run := startTWFBRun(cell.streamer())
		deadline := time.Now().Add(180 * time.Second)
		for time.Now().Before(deadline) && !run.exited() && cell.persistedPosition(t) == "" {
			time.Sleep(250 * time.Millisecond)
		}
		kcfbWaitConverged(t, src, tgt, run, 10*time.Second)
		refusal := kfkRefusal(t, run)
		t.Logf("sync cold start: %v", refusal)
		kcfbGradeWith(t, "sync cold start", src, tgt, refusal, true)
	})
}

// TestKeyCollationFunctionalKey_PostgresToMySQLCopyPaths: every idempotent
// copy path, onto the table a sluice migrate creates for the source (its
// schema phase run first, then emptied).
func TestKeyCollationFunctionalKey_PostgresToMySQLCopyPaths(t *testing.T) {
	pgSrc, _, pgCleanup := startPostgres(t)
	defer pgCleanup()
	_, mysqlTgt, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	pgEng, _ := engines.Get("postgres")
	mysqlEng, _ := engines.Get("mysql")
	applyPGDDL(t, pgSrc, "CREATE TABLE kfm (id int PRIMARY KEY, k varchar(20) NOT NULL, v int); CREATE UNIQUE INDEX kfm_lk ON kfm (lower(k));"+
		"INSERT INTO kfm VALUES (1, 'e', 1), (2, 'é', 2), (3, 'b', 3);"+
		"CREATE TABLE okfm (id int PRIMARY KEY, k varchar(20) NOT NULL, v int); CREATE UNIQUE INDEX okfm_lk ON okfm (lower(k));"+
		"INSERT INTO okfm VALUES (1, 'e', 1), (2, 'f', 2), (3, 'b', 3);")
	// The target tables exactly as sluice creates them.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	only, err := migcore.NewTableFilter([]string{"okfm"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&Migrator{Source: pgEng, Target: mysqlEng, SourceDSN: pgSrc, TargetDSN: mysqlTgt, Filter: only}).Run(ctx); err != nil {
		t.Fatalf("migrate okfm: %v", err)
	}
	krwExec(t, "mysql", mysqlTgt, "CREATE TABLE kfm LIKE okfm")
	tgt := twfbDB{engine: "mysql", dsn: mysqlTgt}
	t.Logf("target kfm: %s", tgt.scalar(t, "SELECT GROUP_CONCAT(CONCAT(index_name, ':', COALESCE(column_name, expression), ':', "+
		"COALESCE(collation, '')) SEPARATOR ', ') FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'kfm'"))

	p := krwPair{"postgres lower(k) → mysql", pgEng, mysqlEng, pgSrc, mysqlTgt, "mysql", "HEX(k)"}
	for _, path := range krwPaths() {
		t.Run(path.name, func(t *testing.T) {
			src := krwSourceRows(t, p, "kfm")
			err := krwCopy(t, p, "kfm", path)
			got := krwTargetRows(t, p, "kfm")
			if !errors.Is(err, ir.ErrKeyCollationMerge) {
				t.Fatalf("err = %v; want %s (the target holds %v, the source %v)", err, ir.ErrKeyCollationMerge, got, src)
			}
			for _, r := range got {
				if !src[r] {
					t.Fatalf("the target holds %s, which is no source row (source %v)", r, src)
				}
			}
			want := krwSourceRows(t, p, "okfm")
			if err := krwCopy(t, p, "okfm", path); err != nil {
				t.Fatalf("a copy with no merge refused: %v", err)
			}
			gotOK := krwTargetRows(t, p, "okfm")
			if len(gotOK) != len(want) {
				t.Fatalf("target %v; want %v", gotOK, want)
			}
			for _, r := range gotOK {
				if !want[r] {
					t.Fatalf("target %v; want %v", gotOK, want)
				}
			}
		})
	}
}

// TestKeyCollationFunctionalKey_MySQLToPostgresApply is the reverse
// direction: a MySQL `UNIQUE ((lower(k)))` streams to Postgres, whose key is
// then moved to a collation that equates 'e' and 'é' (ICU level 1). The
// Postgres applier upserts ON CONFLICT on the primary key, which never
// infers an expression index, so the conflict through lower(k) is a
// unique_violation: loud, nothing merged.
func TestKeyCollationFunctionalKey_MySQLToPostgresApply(t *testing.T) {
	mySrc, _, myCleanup := startMySQL(t)
	defer myCleanup()
	_, pgRoot, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	twfbDB{engine: "postgres", dsn: pgRoot}.exec(t, "CREATE DATABASE kfr_t")
	tgtDSN, err := buildPGDSN(pgRoot, "kfr_t")
	if err != nil {
		t.Fatal(err)
	}
	src, tgt := twfbDB{engine: "mysql", dsn: mySrc}, twfbDB{engine: "postgres", dsn: tgtDSN}
	cell := twfbCell{src: src, tgt: tgt, streamID: "kfr"}
	src.exec(t, "CREATE TABLE kd (id int NOT NULL PRIMARY KEY, k varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, d int, "+
		"UNIQUE KEY kd_lower_k ((lower(k)))); INSERT INTO kd (id,k,d) VALUES (1,'e',1),(2,'b',2),(3,'c',3)")
	run := startTWFBRun(cell.streamer())
	if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	t.Logf("target index: %s", tgt.scalar(t, "SELECT string_agg(indexdef, '; ') FROM pg_indexes WHERE tablename = 'kd'"))
	if !strings.Contains(strings.ToLower(tgt.scalar(t, "SELECT string_agg(indexdef, '; ') FROM pg_indexes WHERE tablename = 'kd'")), "lower") {
		t.Fatal("the target carries no functional unique index; the cell measures nothing")
	}
	// The hand-made shape: a target key collation that equates the pair.
	tgt.exec(t, "CREATE COLLATION l1 (provider = icu, locale = 'und-u-ks-level1', deterministic = false); "+
		"ALTER TABLE kd ALTER COLUMN k TYPE varchar(20) COLLATE l1")
	src.exec(t, "INSERT INTO kd (id,k,d) VALUES (99,'é',99)")
	src.exec(t, "INSERT INTO kd (id,k,d) VALUES (100,'zz',100)")
	_ = tgt.waitRow(t, "kd", 100, run, 60*time.Second)
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) && !run.exited() {
		time.Sleep(300 * time.Millisecond)
	}
	refusal := kfkRefusal(t, run)
	if refusal == nil {
		t.Fatalf("nothing refused: the target's functional key equates 'e' with 'é' (target %s)", kcfbRender(kcfbRows(t, tgt)))
	}
	t.Logf("refused: %v", refusal)
	kcfbGrade(t, "mysql → postgres", src, tgt, refusal)
}

// kfkNewPair creates a Postgres source database and a MySQL target database
// named for tag.
func kfkNewPair(t *testing.T, pgRoot, myRoot, tag string) (src, tgt twfbDB) {
	t.Helper()
	twfbDB{engine: "postgres", dsn: pgRoot}.exec(t, "CREATE DATABASE "+tag+"_s")
	srcDSN, err := buildPGDSN(pgRoot, tag+"_s")
	if err != nil {
		t.Fatal(err)
	}
	twfbDB{engine: "mysql", dsn: myRoot}.exec(t, "CREATE DATABASE "+tag+"_t")
	tgtDSN, err := buildMySQLDSN(myRoot, tag+"_t")
	if err != nil {
		t.Fatal(err)
	}
	return twfbDB{engine: "postgres", dsn: srcDSN}, twfbDB{engine: "mysql", dsn: tgtDSN}
}

// kfkRefusal stops a still-running stream (nil: it did not refuse) or
// returns the error it exited with.
func kfkRefusal(t *testing.T, run *twfbRun) error {
	t.Helper()
	if run.exited() {
		if run.err == nil || errors.Is(run.err, context.Canceled) {
			t.Fatal("the stream exited without an error")
		}
		t.Logf("refused: %v", run.err)
		return run.err
	}
	if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("the stream stopped with %v", err)
	}
	return nil
}
