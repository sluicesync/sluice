//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// KEY-COLLATION-LOOSENED-WINDOW: the MySQL twin of
// KEY-COLLATION-PG-LOOSENED-REPLAY. The key is `utf8mb4_bin` at the full,
// holding (1,'a'); inside one window 'A' is written beside it (distinct under
// `_bin`), moved out of the way, and the key is LOOSENED to
// `utf8mb4_0900_ai_ci`. The window's delta (`_bin` → `_ci`) is applied to the
// target before any of its rows, so 'A' meets 'a' on a `_ci` key.
//
// The independent expected value is the source's rows read by id:HEX(k)
// after the window (kcfbGrade): byte-exact, or refused with nothing merged.

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
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

const (
	klwToCI = "ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL"
)

func klwKeys() map[string]string {
	return map[string]string{
		"pk":     "CREATE TABLE kd (id INT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY, d INT)",
		"unique": "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL UNIQUE, d INT)",
	}
}

func klwWindow() []string {
	return []string{"INSERT INTO kd VALUES (99,'A',99)", "UPDATE kd SET k = 'A2' WHERE id = 99", klwToCI, "INSERT INTO kd VALUES (100,'zz',100)"}
}

func TestChainRestore_KeyCollationLoosenedInsideTheWindow(t *testing.T) {
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	myEng, _ := engines.Get("mysql")
	root := twfbDB{engine: "mysql", dsn: myRoot}
	n := 0
	for _, metadata := range []string{"MINIMAL", "FULL"} {
		for _, key := range []string{"pk", "unique"} {
			for _, lane := range []string{"backup-stream", "backup-stream-rollover", "backup-incremental", "broker", "broker-rollover"} {
				n++
				name := fmt.Sprintf("%s/%s/%s", metadata, key, lane)
				create := klwKeys()[key]
				t.Run(name, func(t *testing.T) {
					root.exec(t, "SET GLOBAL binlog_row_metadata = '"+metadata+"'")
					srcDB, tgtDB := fmt.Sprintf("klw_s%d", n), fmt.Sprintf("klw_t%d", n)
					root.exec(t, "CREATE DATABASE "+srcDB+"; CREATE DATABASE "+tgtDB)
					srcDSN, err := buildMySQLDSN(myRoot, srcDB)
					if err != nil {
						t.Fatal(err)
					}
					tgtDSN, err := buildMySQLDSN(myRoot, tgtDB)
					if err != nil {
						t.Fatal(err)
					}
					src, tgt := twfbDB{engine: "mysql", dsn: srcDSN}, twfbDB{engine: "mysql", dsn: tgtDSN}
					src.exec(t, create+"; INSERT INTO kd VALUES (1,'a',1),(2,'b',2)")
					ctx := context.Background()
					store, err := blobcodec.NewLocalStore(t.TempDir())
					if err != nil {
						t.Fatal(err)
					}
					if err := (&backup.Backup{Source: myEng, SourceDSN: srcDSN, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
						t.Fatalf("full: %v", err)
					}
					full, err := lineage.ReadManifest(ctx, store)
					if err != nil {
						t.Fatal(err)
					}
					captureLane, broker := lane, strings.HasPrefix(lane, "broker")
					if broker {
						captureLane = strings.Replace(lane, "broker", "backup-stream", 1)
						if err := (&backup.Restore{Target: myEng, TargetDSN: tgtDSN, Store: store}).Run(ctx); err != nil {
							t.Fatalf("seed restore: %v", err)
						}
					}
					captureErr := krwCapture(t, captureLane, myEng, srcDSN, "", store, full.BackupID, src, klwWindow())
					if captureErr != nil {
						t.Logf("capture refused: %v", captureErr)
						return
					}
					var applyErr error
					if broker {
						bctx, bcancel := context.WithTimeout(ctx, 30*time.Second)
						applyErr = (&SyncFromBackup{
							Target: myEng, TargetDSN: tgtDSN, Store: store,
							ChainURL: "test://klwb", StreamID: "klwb",
							PollInterval: time.Second, AtChainID: full.BackupID, SluiceVersion: "test",
						}).Run(bctx)
						bcancel()
						if errors.Is(applyErr, context.DeadlineExceeded) || errors.Is(applyErr, context.Canceled) {
							applyErr = nil
						}
					} else {
						applyErr = (&backup.ChainRestore{Target: myEng, TargetDSN: tgtDSN, Store: store}).Run(ctx)
					}
					t.Logf("apply = %v; source %s target %s", applyErr, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
					unordered := applyErr != nil && strings.Contains(applyErr.Error(), "KEY-COLLATION-HISTORY-UNORDERED")
					switch {
					case lane == "backup-stream":
						// The drain-commit records no delta (KEY-COLLATION-DRAIN-NO-DELTA),
						// so the target key stays `_bin` and the window lands exactly.
						if applyErr != nil {
							t.Fatalf("apply = %v; want the window exact on the unchanged `_bin` key", applyErr)
						}
					case broker:
						// The broker places no version between a window's changes, so
						// it refuses a window that changes a key's collation.
						if !unordered {
							t.Fatalf("apply = %v; want KEY-COLLATION-HISTORY-UNORDERED", applyErr)
						}
					default:
						// The delta loosens the target first; the window-start version
						// arms the guard for 'A', written under `_bin`.
						if !errors.Is(applyErr, ir.ErrKeyCollationMerge) || unordered {
							t.Fatalf("apply = %v; want the guard's KEY-COLLATION-MERGE (from the window-start version)", applyErr)
						}
						// A restart re-applies the loosening ahead of the same rows,
						// so the remedy that converges is a fresh full backup.
						if !strings.Contains(applyErr.Error(), "take a fresh full backup") {
							t.Fatalf("apply = %v; want the fresh-full remedy", applyErr)
						}
					}
					kcfbGrade(t, name, src, tgt, applyErr)
				})
			}
		}
	}
}

// TestBackupStream_UnloggedKeyCollationChangeRefuses pins review of
// d6ecb939 finding 2 on a real server: a key collation changed with
// sql_log_bin=0 reaches the catalog and the TABLE_MAP but writes no QUERY
// event, so no schema boundary would carry it; the binlog reader refuses
// COLLATION-CHANGE-UNANNOUNCED (DEFERRED-SCHEMA-CHANGE) before decoding the
// row written under it, instead of reporting it under a shape no boundary
// announces.
func TestBackupStream_UnloggedKeyCollationChangeRefuses(t *testing.T) {
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	myEng, _ := engines.Get("mysql")
	for i, other := range []string{"", "ALTER TABLE other ADD COLUMN x INT"} {
		name := "alone"
		if other != "" {
			// A logged ALTER of an unrelated table first: routine on a busy
			// source, and it must not silence the check for kd.
			name = "after another table's logged ALTER"
		}
		t.Run(name, func(t *testing.T) {
			db := fmt.Sprintf("kul_s%d", i)
			twfbDB{engine: "mysql", dsn: myRoot}.exec(t, "CREATE DATABASE "+db)
			srcDSN, err := buildMySQLDSN(myRoot, db)
			if err != nil {
				t.Fatal(err)
			}
			src := twfbDB{engine: "mysql", dsn: srcDSN}
			src.exec(t, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL UNIQUE, d INT); "+
				"CREATE TABLE other (id INT NOT NULL PRIMARY KEY); INSERT INTO kd VALUES (1,'a',1),(2,'b',2)")
			ctx := context.Background()
			store, err := blobcodec.NewLocalStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := (&backup.Backup{Source: myEng, SourceDSN: srcDSN, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
				t.Fatalf("full: %v", err)
			}
			full, err := lineage.ReadManifest(ctx, store)
			if err != nil {
				t.Fatal(err)
			}
			// kd's first row of the window comes first, so the hidden change
			// is not its first touch.
			window := []string{"INSERT INTO kd VALUES (50,'p',50)"}
			if other != "" {
				window = append(window, other)
			}
			window = append(window,
				"SET SESSION sql_log_bin = 0; ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL; SET SESSION sql_log_bin = 1",
				"INSERT INTO kd VALUES (99,'A',99)")
			captureErr := krwCapture(t, "backup-stream", myEng, srcDSN, "", store, full.BackupID, src, window)
			if !errors.Is(captureErr, ir.ErrDeferredSchemaChange) || !strings.Contains(captureErr.Error(), "COLLATION-CHANGE-UNANNOUNCED") {
				t.Fatalf("capture = %v; want COLLATION-CHANGE-UNANNOUNCED", captureErr)
			}
		})
	}
}
