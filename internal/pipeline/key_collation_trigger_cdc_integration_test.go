//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// KEY-COLLATION-TRIGGER-CDC-NO-HISTORY, steady state. A Postgres UNIQUE key
// beside the primary key on the database default collation keeps 'a' and
// 'A' apart; the MySQL target creates it under utf8mb4_0900_ai_ci, which
// equates them. The native pgoutput lane refuses the second row
// KEY-COLLATION-MERGE (TestKeyCollationFirstBoundary_PostgresDefaultUniqueToMySQL)
// because every Relation it decodes reaches the applier as a SchemaSnapshot
// the guard arms from. The pgtrigger reader emitted none, so the same insert
// on a postgres-trigger source applied with the guard unarmed: an ON
// DUPLICATE KEY UPDATE onto row 1, at exit 0. No collation changes anywhere.
//
// Measured before the fix (2026-10-15, postgres:16 → MySQL 8): live cold
// start and warm resume, and a chain restore of a `backup incremental` and
// of a `backup stream` window, each left target row 1 holding 'A' where the
// source holds 'a' and row 99 absent, exit 0. `sync from-backup` already
// refused (KEY-COLLATION-BROKER-UNARMED arms it from the segment's recorded
// schema) and is kept here as the control that the recorded versions do not
// disturb it.
//
// The Postgres-target cells are the must-pass half: there the key is
// case-sensitive on both sides, so 'A' lands beside 'a' exactly, and a warm
// resume must prime from the versions the first cold start recorded (an
// engine with no position orderer refuses that prime).
//
// The independent expected value is the Postgres source's own rows, read by
// id:HEX(k) (kcfbRows), never the stream's or the chain's.

package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/engines/pgtrigger"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// kttcCreate is the source table: a UNIQUE key beside the primary key, on
// the database default (deterministic, case-sensitive) collation.
const kttcCreate = "CREATE TABLE kd (id int NOT NULL PRIMARY KEY, k varchar(20) NOT NULL UNIQUE, d int); " +
	"INSERT INTO kd VALUES (1,'a',1),(2,'b',2),(3,'c',3)"

// kttcPair boots a Postgres source with kd under pgtrigger capture and an
// empty target database on target ("mysql" or "postgres"); myRoot reaches
// the MySQL server for more databases ("" for a Postgres target).
func kttcPair(t *testing.T, tag, target string) (src, tgt twfbDB, myRoot string, cleanup func()) {
	t.Helper()
	return kttcPairWith(t, tag, target, kttcCreate)
}

// kttcPairWith is [kttcPair] over the source table create (kd, captured).
func kttcPairWith(t *testing.T, tag, target, create string) (src, tgt twfbDB, myRoot string, cleanup func()) {
	t.Helper()
	srcDSN, pgTarget, pgCleanup := startPostgres(t)
	cleanup = pgCleanup
	tgt = twfbDB{engine: "postgres", dsn: pgTarget}
	if target == "mysql" {
		var myCleanup func()
		_, myRoot, myCleanup = startMySQL(t)
		cleanup = func() { myCleanup(); pgCleanup() }
		twfbDB{engine: "mysql", dsn: myRoot}.exec(t, "CREATE DATABASE "+tag+"_t")
		tgtDSN, err := buildMySQLDSN(myRoot, tag+"_t")
		if err != nil {
			cleanup()
			t.Fatal(err)
		}
		tgt = twfbDB{engine: "mysql", dsn: tgtDSN}
	}
	src = twfbDB{engine: "postgres", dsn: srcDSN}
	src.exec(t, create)
	if _, err := pgtrigger.Setup(context.Background(), srcDSN, pgtrigger.SetupOptions{Tables: []string{"kd"}, Schema: "public"}); err != nil {
		cleanup()
		t.Fatalf("pgtrigger.Setup: %v", err)
	}
	return src, tgt, myRoot, cleanup
}

// kttcStreamer is a postgres-trigger → tgt `sync start`.
func kttcStreamer(t *testing.T, src, tgt twfbDB, streamID string) *Streamer {
	t.Helper()
	srcEng, ok := engines.Get(pgtrigger.EngineName)
	if !ok {
		t.Fatal("postgres-trigger engine not registered")
	}
	tgtEng, _ := engines.Get(tgt.engine)
	return &Streamer{
		Source: srcEng, Target: tgtEng,
		SourceDSN: src.dsn, TargetDSN: tgt.dsn,
		StreamID:           streamID,
		Filter:             mustNewFilter(t, nil, nil),
		ApplyRetryAttempts: 8,
	}
}

// TestKeyCollationTriggerCDC_LiveSteadyState is the live `sync start` cell,
// cold start and then a warm resume: the colliding insert lands in CDC with
// no DDL anywhere. On MySQL it must refuse KEY-COLLATION-MERGE with nothing
// merged; on Postgres it must land exactly.
func TestKeyCollationTriggerCDC_LiveSteadyState(t *testing.T) {
	for _, target := range []string{"mysql", "postgres"} {
		for _, warm := range []bool{false, true} {
			name := target + "/cold-start"
			if warm {
				name = target + "/warm-resume"
			}
			t.Run(name, func(t *testing.T) { kttcLiveCell(t, target, warm) })
		}
	}
}

func kttcLiveCell(t *testing.T, target string, warm bool) {
	src, tgt, _, cleanup := kttcPair(t, "kttc", target)
	defer cleanup()
	cell := twfbCell{src: src, tgt: tgt, streamID: "kttc"}
	run := startTWFBRun(kttcStreamer(t, src, tgt, cell.streamID))
	if !tgt.waitRow(t, "kd", 3, run, 180*time.Second) {
		t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
	}
	cell.waitStreaming(t, run)
	// A non-colliding change first, so the stream is past its first CDC row
	// of the table before the collision (steady state).
	src.exec(t, "INSERT INTO kd VALUES (50,'p',50)")
	if !tgt.waitRow(t, "kd", 50, run, 60*time.Second) {
		t.Fatalf("the priming row never landed (%v)", run.stop(t))
	}
	if warm {
		if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("the clean stop returned %v", err)
		}
		run = startTWFBRun(kttcStreamer(t, src, tgt, cell.streamID))
		src.exec(t, "INSERT INTO kd VALUES (51,'q',51)")
		if !tgt.waitRow(t, "kd", 51, run, 60*time.Second) {
			t.Fatalf("the warm resume never applied a row (%v)", run.stop(t))
		}
	}
	src.exec(t, "INSERT INTO kd VALUES (99,'A',99)")
	src.exec(t, "INSERT INTO kd VALUES (100,'zz',100)")
	_ = tgt.waitRow(t, "kd", 100, run, 60*time.Second)
	kcfbWaitConverged(t, src, tgt, run, 20*time.Second)
	var refusal error
	if run.exited() {
		refusal = run.err
		if refusal == nil || errors.Is(refusal, context.Canceled) {
			t.Fatal("the stream exited without an error")
		}
		t.Logf("refused: %v", refusal)
	} else if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("the stream stopped with %v", err)
	}
	t.Logf("source %s target %s", kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
	kcfbGrade(t, "pgtrigger live "+target, src, tgt, refusal)

	// The stream recorded the boundary as a schema version under the source
	// engine's name — the row a warm resume's prime orders, and the one the
	// warm cell's restart did not refuse to prime from.
	q := "SELECT CAST(COUNT(*) AS CHAR) FROM sluice_cdc_schema_history WHERE stream_id = ? AND table_name = 'kd' AND source_engine = ?"
	if target == "postgres" {
		q = "SELECT COUNT(*)::text FROM sluice_cdc_schema_history WHERE stream_id = $1 AND table_name = 'kd' AND source_engine = $2"
	}
	versions := tgt.scalar(t, q, cell.streamID, pgtrigger.EngineName)
	t.Logf("schema versions recorded for kd: %s", versions)
	if versions == "0" || strings.HasPrefix(versions, "<") {
		t.Errorf("the target's schema history holds %s %s version(s) of kd; want at least one (the first-touch boundary)",
			versions, pgtrigger.EngineName)
	}

	if target == "postgres" {
		if refusal != nil {
			t.Errorf("a case-sensitive Postgres target refused a row it holds exactly: %v", refusal)
		}
		return
	}
	if refusal == nil {
		t.Fatalf("nothing refused: the target's UNIQUE key equates 'a' with 'A', so row 99 cannot land exactly")
	}
	if !errors.Is(refusal, ir.ErrKeyCollationMerge) {
		t.Errorf("refused with %v; want %v", refusal, ir.ErrKeyCollationMerge)
	}
}

// TestKeyCollationTriggerCDC_ChainRestore is the backup cell: a pgtrigger
// full, an incremental (or a backup stream) holding only the colliding
// insert, chain-restored onto a fresh MySQL database; and the same chain
// replayed by `sync from-backup` onto a seed restore, which arms from the
// segment's recorded schema (KEY-COLLATION-BROKER-UNARMED) and must keep
// refusing with the stream's history now in the manifest.
func TestKeyCollationTriggerCDC_ChainRestore(t *testing.T) {
	for _, lane := range []string{"backup-incremental", "backup-stream"} {
		t.Run(lane, func(t *testing.T) {
			src, tgt, myRoot, cleanup := kttcPair(t, "kttr", "mysql")
			defer cleanup()
			ctx := context.Background()
			srcEng, _ := engines.Get(pgtrigger.EngineName)
			myEng, _ := engines.Get("mysql")
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := (&backup.Backup{Source: srcEng, SourceDSN: src.dsn, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
				t.Fatalf("full: %v", err)
			}
			full, err := lineage.ReadManifest(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			// The broker's target is seeded from the full BEFORE the window is
			// captured: a Restore run after it would replay the whole chain
			// (and with it the colliding insert) on its own.
			twfbDB{engine: "mysql", dsn: myRoot}.exec(t, "CREATE DATABASE kttr_b")
			bDSN, err := buildMySQLDSN(myRoot, "kttr_b")
			if err != nil {
				t.Fatal(err)
			}
			btgt := twfbDB{engine: "mysql", dsn: bDSN}
			if err := (&backup.Restore{Target: myEng, TargetDSN: btgt.dsn, Store: store}).Run(ctx); err != nil {
				t.Fatalf("seed restore: %v", err)
			}
			if err := krwCapture(t, lane, srcEng, src.dsn, "", store, full.BackupID, src,
				[]string{"INSERT INTO kd VALUES (99,'A',99)", "INSERT INTO kd VALUES (100,'zz',100)"}); err != nil {
				t.Fatalf("%s: %v", lane, err)
			}

			// The window recorded the first-touch boundary in its schema
			// history, anchored under the source engine — what the restore
			// replays ahead of the window's rows.
			recs, err := lineage.ListAllSegmentManifests(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			versions := 0
			for _, rec := range recs {
				for _, h := range rec.Manifest.SchemaHistory {
					if h != nil && h.Table == "kd" && h.AnchorPosition.Engine == pgtrigger.EngineName {
						versions++
					}
				}
			}
			if versions == 0 {
				t.Errorf("%s: no segment recorded a %s schema version of kd; the first-touch boundary never reached the backup history",
					lane, pgtrigger.EngineName)
			}

			restoreErr := (&backup.ChainRestore{Target: myEng, TargetDSN: tgt.dsn, Store: store, SourceEngine: engines.Get}).Run(ctx)
			t.Logf("chain restore = %v; source %s target %s", restoreErr, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
			kcfbGrade(t, "pgtrigger chain restore ("+lane+")", src, tgt, restoreErr)
			if !errors.Is(restoreErr, ir.ErrKeyCollationMerge) {
				t.Errorf("chain restore = %v; want KEY-COLLATION-MERGE (the target's UNIQUE key equates the source's 'a' and 'A')", restoreErr)
			}

			bctx, bcancel := context.WithTimeout(ctx, 30*time.Second)
			defer bcancel()
			brokerErr := (&SyncFromBackup{
				Target: myEng, TargetDSN: btgt.dsn, Store: store,
				ChainURL: "test://kttr", StreamID: "kttr",
				PollInterval: time.Second, AtChainID: full.BackupID, SluiceVersion: "test",
			}).Run(bctx)
			if errors.Is(brokerErr, context.DeadlineExceeded) || errors.Is(brokerErr, context.Canceled) {
				brokerErr = nil
			}
			t.Logf("broker = %v; target %s", brokerErr, kcfbRender(kcfbRows(t, btgt)))
			kcfbGrade(t, "pgtrigger broker ("+lane+")", src, btgt, brokerErr)
			if !errors.Is(brokerErr, ir.ErrKeyCollationMerge) {
				t.Errorf("broker = %v; want KEY-COLLATION-MERGE", brokerErr)
			}
		})
	}
}

// TestTriggerCDCFirstTouch_StoppedStreamDDLIsNeverForwarded pins what the
// first-touch boundary must not become: a way for a source DDL to reach the
// target. A change made while the stream is stopped, behind more captured
// rows than one poll window holds, shows in the first-touch boundary before
// the reader reaches its DDL marker. On the default forward path the
// first-boundary witness would apply an ADD COLUMN to the target (and a
// `timestamp` → `timestamptz` swap is refused there by the forward
// intercept's own session-zone door); a trigger-CDC source runs the
// unforwarded-stream check instead, which refuses both with
// SCHEMA-CHANGE-REFUSED, as the engine refuses every DDL it sees. The
// independent expected value is the target's own catalog, unchanged.
func TestTriggerCDCFirstTouch_StoppedStreamDDLIsNeverForwarded(t *testing.T) {
	for _, c := range []struct {
		name, ddl string
		// untouched reports whether the target still has its pre-DDL shape.
		untouched func(t *testing.T, tgt twfbDB) (string, bool)
	}{
		{"add-column", "ALTER TABLE zs ADD COLUMN extra int", func(t *testing.T, tgt twfbDB) (string, bool) {
			n := tgt.scalar(t, "SELECT COUNT(*)::text FROM information_schema.columns WHERE table_name = 'zs' AND column_name = 'extra'")
			return "columns named extra: " + n, n == "0"
		}},
		{"zone-swap", "ALTER TABLE zs ALTER COLUMN ts TYPE timestamptz", func(t *testing.T, tgt twfbDB) (string, bool) {
			got := tgt.columnType(t, "zs", "ts")
			return "zs.ts is " + got, got == "timestamp without time zone"
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			srcDSN, tgtDSN, cleanup := startPostgres(t)
			defer cleanup()
			src, tgt := twfbDB{engine: "postgres", dsn: srcDSN}, twfbDB{engine: "postgres", dsn: tgtDSN}
			src.exec(t, "CREATE TABLE zs (id int PRIMARY KEY, ts timestamp NOT NULL); INSERT INTO zs VALUES (1, '2026-01-01 12:00:00')")
			if _, err := pgtrigger.Setup(context.Background(), srcDSN, pgtrigger.SetupOptions{Tables: []string{"zs"}, Schema: "public"}); err != nil {
				t.Fatalf("pgtrigger.Setup: %v", err)
			}
			cell := twfbCell{src: src, tgt: tgt, streamID: "ktzs"}
			run := startTWFBRun(kttcStreamer(t, src, tgt, cell.streamID))
			if !tgt.waitRow(t, "zs", 1, run, 180*time.Second) {
				t.Fatalf("the cold start never delivered the seed (%v)", run.stop(t))
			}
			cell.waitStreaming(t, run)
			if err := run.stop(t); err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("the clean stop returned %v", err)
			}
			// More captured rows than one poll window (10000), so the first
			// batch after the restart carries the table's first change and no
			// DDL marker.
			src.exec(t, "INSERT INTO zs SELECT g, '2026-01-01 12:00:00' FROM generate_series(2, 10050) g")
			src.exec(t, c.ddl)

			run = startTWFBRun(kttcStreamer(t, src, tgt, cell.streamID))
			deadline := time.Now().Add(90 * time.Second)
			for time.Now().Before(deadline) && !run.exited() {
				time.Sleep(250 * time.Millisecond)
			}
			if !run.exited() {
				_ = run.stop(t)
				t.Fatal("the stream kept running past a source schema change; it must refuse")
			}
			t.Logf("refused: %v", run.err)
			if what, ok := c.untouched(t, tgt); !ok {
				t.Errorf("the target changed (%s): the source's DDL was forwarded", what)
			}
			if !errors.Is(run.err, ir.ErrSchemaChangeRefused) {
				t.Errorf("refused with %v; want %v from the unforwarded-stream check", run.err, ir.ErrSchemaChangeRefused)
			}
		})
	}
}
