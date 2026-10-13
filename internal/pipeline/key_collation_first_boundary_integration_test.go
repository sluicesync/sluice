//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// KEY-COLLATION-FIRST-BOUNDARY — the pin for options (2) and (5).
//
// A key column's collation or character set changed on a MySQL source
// (`_ci` → `_bin`, a character-set change in place, PAD SPACE → NO PAD),
// then a row distinct from an existing one only under the new collation
// ('A' beside 'a', 'a ' beside 'a'). Before the fix the target kept the
// old collation whenever the stream met the change as the first boundary
// of a run or forwarded no DDL, and the row merged into the existing one at
// exit 0 (measured on MySQL 8.0/8.4, FULL and MINIMAL, PK and UNIQUE, and
// on backup incremental + chain restore).
//
// The grade is the target against the SOURCE, by exact key bytes
// (`id:HEX(k)` for every row): either the two are identical, or the stream
// refused and the target holds only rows identical to the source's, the
// colliding row absent. Never the stream's own logs.
//
// Postgres sources are in this matrix too (TestKeyCollationFirstBoundary_Postgres16):
// pgoutput carries no collation, so they measured merging on every entry
// before the fix; the reader now reads each column's collation from the
// catalog at every Relation, and the applier backstop (option 3,
// KEY-COLLATION-MERGE) refuses what a boundary does not show. The grade
// does not care which door refused.

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// kcfbScenario is one source history: create seeds ids 1..3, change makes
// the key collation change and writes row 99 (distinct from row 1 only
// under the new collation), then writes row 100, sometimes after a later
// change folded behind the first.
type kcfbScenario struct {
	name, create, change, then string
	// remedy is the key DDL alone, which the drained model applies on the
	// target after a refusal ("" where the remedy is a re-copy).
	remedy string
}

func kcfbScenarios() []kcfbScenario {
	pk := "CREATE TABLE kd (id INT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL, d INT, PRIMARY KEY (k)); INSERT INTO kd VALUES (1,'a',1),(2,'b',2),(3,'c',3);"
	uq := "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL, d INT, UNIQUE KEY uk (k)); INSERT INTO kd VALUES (1,'a',1),(2,'b',2),(3,'c',3);"
	pad := "CREATE TABLE kd (id INT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, d INT, PRIMARY KEY (k)); INSERT INTO kd VALUES (1,'a',1),(2,'b',2),(3,'c',3);"
	binDDL := "ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL;"
	asciiDDL := "ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET ascii COLLATE ascii_bin NOT NULL;"
	noPadDDL := "ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin NOT NULL;"
	toBin := binDDL + " INSERT INTO kd (id,k,d) VALUES (99,'A',99);"
	toASCII := asciiDDL + " INSERT INTO kd (id,k,d) VALUES (99,'A',99);"
	toNoPad := noPadDDL + " INSERT INTO kd (id,k,d) VALUES (99,'a ',99);"
	sentinel := "INSERT INTO kd (id,k,d) VALUES (100,'zz',100);"
	drop := "ALTER TABLE kd DROP COLUMN d; INSERT INTO kd (id,k) VALUES (100,'zz');"
	retype := "ALTER TABLE kd MODIFY k VARBINARY(80) NOT NULL; INSERT INTO kd (id,k,d) VALUES (100,'zz',100);"
	// KEY-COLLATION-REVERTED-WINDOW-RESTORE: the key is changed back after
	// 'A' moved out of the way, so the catalog never shows `_bin` again.
	reverted := toBin + " UPDATE kd SET k = 'A2' WHERE id = 99; " +
		"ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL;"
	return []kcfbScenario{
		{"pk-collation", pk, toBin, sentinel, binDDL},
		{"uq-collation", uq, toBin, sentinel, binDDL},
		{"pk-charset", pk, toASCII, sentinel, asciiDDL},
		{"pk-nopad", pad, toNoPad, sentinel, noPadDDL},
		{"pk-collation-then-drop", pk, toBin, drop, ""},
		{"uq-collation-then-drop", uq, toBin, drop, ""},
		{"pk-collation-then-retype", pk, toBin, retype, ""},
		{"uq-collation-reverted", uq, reverted, sentinel, ""},
	}
}

const (
	kcfbLiveCDC  = "live-cdc"
	kcfbLiveSeed = "live-seed"
	kcfbWarm     = "warm"
	kcfbPrimed   = "warm-primed"
	kcfbRefuse   = "refuse"
)

// kcfbRows reads kd as id → HEX(k), the exact key bytes.
func kcfbRows(t *testing.T, db twfbDB) map[string]string {
	t.Helper()
	conn, err := sql.Open(db.driver(), db.dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = conn.Close() }()
	q := "SELECT CAST(id AS CHAR), HEX(k) FROM kd"
	if db.engine == "postgres" {
		q = "SELECT id::text, upper(encode(convert_to(k::text, 'UTF8'), 'hex')) FROM kd"
	}
	rows, err := conn.Query(q)
	if err != nil {
		t.Fatalf("read kd: %v", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var id, k string
		if err := rows.Scan(&id, &k); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[id] = k
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read kd: %v", err)
	}
	return out
}

func kcfbRender(m map[string]string) string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = id + ":" + m[id]
	}
	return strings.Join(parts, ",")
}

// kcfbGrade is the matrix's only verdict. exact: the target equals the
// source key for key. A refusal: every row the target holds is the
// source's row with the same id and the same key bytes, and the row that
// collides under the old collation (99) is not there — nothing merged.
func kcfbGrade(t *testing.T, what string, src, tgt twfbDB, refusal error) {
	t.Helper()
	s, g := kcfbRows(t, src), kcfbRows(t, tgt)
	if refusal == nil {
		if kcfbRender(s) != kcfbRender(g) {
			t.Errorf("%s: nothing refused, and the target differs from the source by key bytes\nsource: %s\ntarget: %s",
				what, kcfbRender(s), kcfbRender(g))
		}
		return
	}
	for id, k := range g {
		if s[id] != k {
			t.Errorf("%s: refused, but target row %s holds key %s where the source holds %q (a merge)\nsource: %s\ntarget: %s",
				what, id, k, s[id], kcfbRender(s), kcfbRender(g))
		}
	}
	if _, ok := g["99"]; ok {
		t.Errorf("%s: refused AFTER the colliding row was applied", what)
	}
	for _, id := range []string{"1", "2", "3"} {
		if g[id] != s[id] {
			t.Errorf("%s: refused, but seeded row %s is %q on the target (source %q)", what, id, g[id], s[id])
		}
	}
}

// kcfbWaitConverged waits for the target to equal the source by key bytes,
// or for the run to exit.
func kcfbWaitConverged(t *testing.T, src, tgt twfbDB, run *twfbRun, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) && !run.exited() {
		if kcfbRender(kcfbRows(t, src)) == kcfbRender(kcfbRows(t, tgt)) {
			return
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func kcfbRunCell(t *testing.T, srv rsfServer, sc kcfbScenario, entry string, idx int) {
	tag := fmt.Sprintf("q%d", idx)
	src, tgt := srv.newDB(t, "s_"+tag), srv.newDB(t, "t_"+tag)
	cell := twfbCell{src: src, tgt: tgt, streamID: "kcfb-" + tag, slotName: "kcfb_" + tag}
	if entry == kcfbRefuse {
		cell.schemaChanges = "refuse"
	}
	src.exec(t, sc.create)
	what := fmt.Sprintf("%s/%s/%s", srv.label, sc.name, entry)
	run := startTWFBRun(cell.streamer())
	if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
		t.Fatalf("%s: the cold start never delivered the seed (%v)", what, run.stop(t))
	}
	cell.waitStreaming(t, run)
	if entry != kcfbLiveSeed && entry != kcfbWarm {
		src.exec(t, "INSERT INTO kd (id,k,d) VALUES (50,'p',50)")
		if !tgt.waitRow(t, "kd", 50, run, 60*time.Second) {
			t.Fatalf("%s: the priming row never landed (%v)", what, run.stop(t))
		}
	}
	warm := entry == kcfbWarm || entry == kcfbPrimed
	if warm {
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("%s: the clean stop returned %v", what, err)
		}
	}
	src.exec(t, sc.change)
	if !warm {
		_ = tgt.waitRow(t, "kd", 99, run, 30*time.Second)
	}
	src.exec(t, sc.then)
	if warm {
		run = startTWFBRun(cell.streamer())
	}
	_ = tgt.waitRow(t, "kd", 100, run, 60*time.Second)
	kcfbWaitConverged(t, src, tgt, run, 20*time.Second)
	var refusal error
	if run.exited() {
		refusal = run.err
		if refusal == nil || errors.Is(refusal, context.Canceled) {
			t.Fatalf("%s: the stream exited without an error", what)
		}
		t.Logf("%s refused: %v", what, refusal)
	} else if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("%s: the stream stopped with %v", what, err)
	}
	kcfbGrade(t, what, src, tgt, refusal)
	if refusal == nil || sc.remedy == "" {
		return
	}
	// The refusal's own remedy converges: apply the key change on the target
	// and restart; the stream must then deliver every row exactly. A refusal
	// whose remedy does not converge would be a wedge.
	tgt.exec(t, sc.remedy)
	again := startTWFBRun(cell.streamer())
	_ = tgt.waitRow(t, "kd", 100, again, 60*time.Second)
	kcfbWaitConverged(t, src, tgt, again, 20*time.Second)
	if again.exited() {
		t.Fatalf("%s: after the remedy the stream stopped again: %v", what, again.err)
	}
	if err := again.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("%s: after the remedy the stream stopped with %v", what, err)
	}
	kcfbGrade(t, what+" (after the remedy)", src, tgt, nil)
}

func kcfbRunServer(t *testing.T, srv rsfServer) {
	kcfbRunScenarios(t, srv, kcfbScenarios(), []string{kcfbLiveCDC, kcfbLiveSeed, kcfbWarm, kcfbPrimed, kcfbRefuse})
}

// kcfbPGScenarios are the Postgres-source shapes: a key on a
// nondeterministic ICU collation (created on each database by the server
// fixture) moved to "C", alone and then retyped. pgoutput carries no
// collation; the reader now reads it from the catalog at every Relation.
func kcfbPGScenarios() []kcfbScenario {
	pk := "CREATE TABLE kd (id int NOT NULL, k text COLLATE ci NOT NULL PRIMARY KEY, d int); INSERT INTO kd VALUES (1,'a',1),(2,'b',2),(3,'c',3);"
	pkv := "CREATE TABLE kd (id int NOT NULL, k varchar(20) COLLATE ci NOT NULL PRIMARY KEY, d int); INSERT INTO kd VALUES (1,'a',1),(2,'b',2),(3,'c',3);"
	toC := `ALTER TABLE kd ALTER COLUMN k TYPE text COLLATE "C";`
	toCv := `ALTER TABLE kd ALTER COLUMN k TYPE varchar(20) COLLATE "C";`
	return []kcfbScenario{
		{"pg-pk-collation", pk, toC + " INSERT INTO kd (id,k,d) VALUES (99,'A',99);", "INSERT INTO kd (id,k,d) VALUES (100,'zz',100);", toC},
		{"pg-pk-collation-then-retype", pkv, toCv + " INSERT INTO kd (id,k,d) VALUES (99,'A',99);", "ALTER TABLE kd ALTER COLUMN k TYPE text; INSERT INTO kd (id,k,d) VALUES (100,'zz',100);", ""},
	}
}

func kcfbRunScenarios(t *testing.T, srv rsfServer, scs []kcfbScenario, entries []string) {
	i := 0
	for _, sc := range scs {
		for _, entry := range entries {
			i++
			n, sc, entry := i, sc, entry
			t.Run(sc.name+"/"+entry, func(t *testing.T) {
				t.Parallel()
				kcfbRunCell(t, srv, sc, entry, n)
			})
		}
	}
}

// TestKeyCollationFirstBoundary_Postgres16 is the Postgres → Postgres half:
// measured merging on EVERY entry before the fix, the live one included.
// Each cell must now be exact or refuse before row 99 merges.
func TestKeyCollationFirstBoundary_Postgres16(t *testing.T) {
	srcDSN, _, cleanup := startPostgresLogicalImage(t, pgPrebakedImage, 40)
	defer cleanup()
	root := twfbDB{engine: "postgres", dsn: srcDSN}
	srv := rsfServer{
		label: "postgres-16", engine: "postgres", dialect: "postgres",
		newDB: func(t *testing.T, name string) twfbDB {
			t.Helper()
			root.exec(t, "CREATE DATABASE "+name)
			dsn, err := buildPGDSN(srcDSN, name)
			if err != nil {
				t.Fatal(err)
			}
			db := twfbDB{engine: "postgres", dsn: dsn}
			db.exec(t, "CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)")
			return db
		},
	}
	t.Run("cells", func(t *testing.T) {
		kcfbRunScenarios(t, srv, kcfbPGScenarios(), []string{kcfbLiveCDC, kcfbLiveSeed, kcfbWarm, kcfbPrimed, kcfbRefuse})
	})
}

func TestKeyCollationFirstBoundary_MySQL80_Full(t *testing.T) {
	dsn, _, cleanup := startMySQLBinlog(t)
	defer cleanup()
	rsfFullMetadata(t, dsn)
	t.Run("cells", func(t *testing.T) {
		kcfbRunServer(t, rsfFull(rsfMySQLFamily(t, "mysql-8.0/full", "mysql", dsn, buildMySQLDSN)))
	})
}

func TestKeyCollationFirstBoundary_MySQL84_Minimal(t *testing.T) {
	dsn, cleanup := rsfBootMySQL(t, "mysql:8.4")
	defer cleanup()
	t.Run("cells", func(t *testing.T) {
		kcfbRunServer(t, rsfMySQLFamily(t, "mysql-8.4/minimal", "mysql", dsn, buildMySQLDSN))
	})
}

func TestKeyCollationFirstBoundary_MySQL84_Full(t *testing.T) {
	dsn, cleanup := rsfBootMySQL(t, "mysql:8.4")
	defer cleanup()
	rsfFullMetadata(t, dsn)
	t.Run("cells", func(t *testing.T) {
		kcfbRunServer(t, rsfFull(rsfMySQLFamily(t, "mysql-8.4/full", "mysql", dsn, buildMySQLDSN)))
	})
}

// TestKeyCollationFirstBoundary_ChainRestore is option (5): a full, the
// change and its rows inside an incremental's window, then a chain
// restore. The window's collation-only delta must reach the target before
// its rows do, under both metadata settings; a fold the capture cannot
// replay refuses (the deferred-shape and look-ahead doors).
func TestKeyCollationFirstBoundary_ChainRestore(t *testing.T) {
	base, cleanup := startMySQLGTIDForShapes(t)
	defer cleanup()
	eng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	n := 0
	for _, metadata := range []string{"MINIMAL", "FULL"} {
		shapeExec(t, base, "SET GLOBAL binlog_row_metadata = "+metadata)
		for _, sc := range kcfbScenarios() {
			n++
			t.Run(metadata+"/"+sc.name, func(t *testing.T) {
				srcDSN := mysqlFreshDB(t, base, fmt.Sprintf("skc_%d", n))
				tgtDSN := mysqlFreshDB(t, base, fmt.Sprintf("tkc_%d", n))
				src, tgt := twfbDB{engine: "mysql", dsn: srcDSN}, twfbDB{engine: "mysql", dsn: tgtDSN}
				src.exec(t, sc.create)
				ctx := context.Background()
				store, err := blobcodec.NewLocalStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				if err := (&backup.Backup{Source: eng, SourceDSN: srcDSN, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
					t.Fatalf("full: %v", err)
				}
				full, err := lineage.ReadManifest(ctx, store)
				if err != nil {
					t.Fatal(err)
				}
				src.exec(t, sc.change+sc.then)
				runCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
				capErr := (&IncrementalBackup{
					Source: eng, SourceDSN: srcDSN, Store: store, ParentRef: full.BackupID,
					Window: 5 * time.Second, SluiceVersion: "test",
				}).Run(runCtx)
				cancel()
				what := "chain-restore/" + metadata + "/" + sc.name
				if capErr != nil {
					// Refused at capture: nothing to restore, nothing merged.
					t.Logf("%s: capture refused: %v", what, capErr)
					return
				}
				if err := (&backup.ChainRestore{Target: eng, TargetDSN: tgtDSN, Store: store}).Run(ctx); err != nil {
					t.Logf("%s: restore refused: %v", what, err)
					if tgt.scalar(t, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'kd'") == "1" {
						kcfbGrade(t, what, src, tgt, err)
					}
					return
				}
				kcfbGrade(t, what, src, tgt, nil)
			})
		}
	}
}
