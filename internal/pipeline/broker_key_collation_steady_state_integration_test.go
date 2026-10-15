//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// KEY-COLLATION-BROKER-UNARMED, steady state. `sync from-backup` replays a
// chain's incrementals through an applier that is never primed from a schema
// version and receives no snapshot, so its key-collation guard has nothing
// to arm from. The live `sync start` sibling of this exact shape
// (TestKeyCollationFirstBoundary_PostgresDefaultUniqueToMySQL) refuses
// KEY-COLLATION-MERGE: a Postgres UNIQUE key beside the primary key on the
// default (case-sensitive) collation lands on MySQL under
// utf8mb4_0900_ai_ci, which equates 'a' and 'A'.
//
// No collation changes anywhere in this chain. The incremental only inserts
// a source-distinct row.
//
// The independent expected value is the Postgres source's own rows, read by
// id:HEX(k).

package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

func TestSyncFromBackup_PostgresToMySQL_SteadyStateKeyCollationMerge(t *testing.T) {
	pgRoot, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	pgEng, _ := engines.Get("postgres")
	myEng, _ := engines.Get("mysql")

	twfbDB{engine: "postgres", dsn: pgRoot}.exec(t, "CREATE DATABASE kbs_s")
	srcDSN, err := buildPGDSN(pgRoot, "kbs_s")
	if err != nil {
		t.Fatal(err)
	}
	twfbDB{engine: "mysql", dsn: myRoot}.exec(t, "CREATE DATABASE kbs_t")
	tgtDSN, err := buildMySQLDSN(myRoot, "kbs_t")
	if err != nil {
		t.Fatal(err)
	}
	src, tgt := twfbDB{engine: "postgres", dsn: srcDSN}, twfbDB{engine: "mysql", dsn: tgtDSN}
	src.exec(t, "CREATE TABLE kd (id int NOT NULL PRIMARY KEY, k varchar(20) NOT NULL UNIQUE, d int); "+
		"INSERT INTO kd VALUES (1,'a',1),(2,'b',2)")

	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := (&backup.Backup{
		Source: pgEng, SourceDSN: srcDSN, Store: store, SluiceVersion: "test", SlotName: "kbs_slot", ChainSlot: true,
	}).Run(ctx); err != nil {
		t.Fatalf("full: %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&backup.Restore{Target: myEng, TargetDSN: tgtDSN, Store: store}).Run(ctx); err != nil {
		t.Fatalf("seed restore: %v", err)
	}

	if err := krwCapture(t, "backup-incremental", pgEng, srcDSN, "kbs_slot", store, full.BackupID, src,
		[]string{"INSERT INTO kd VALUES (99,'A',99)"}); err != nil {
		t.Fatalf("incremental: %v", err)
	}

	bctx, bcancel := context.WithTimeout(ctx, 30*time.Second)
	defer bcancel()
	brokerErr := (&SyncFromBackup{
		Target: myEng, TargetDSN: tgtDSN, Store: store,
		ChainURL: "test://kbs", StreamID: "kbs",
		PollInterval: time.Second, AtChainID: full.BackupID, SluiceVersion: "test",
	}).Run(bctx)
	if errors.Is(brokerErr, context.DeadlineExceeded) || errors.Is(brokerErr, context.Canceled) {
		brokerErr = nil
	}
	t.Logf("broker = %v; source %s target %s", brokerErr, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
	if !errors.Is(brokerErr, ir.ErrKeyCollationMerge) {
		t.Errorf("broker = %v; want KEY-COLLATION-MERGE (the target's UNIQUE key equates the source's 'a' and 'A')", brokerErr)
	}
	kcfbGrade(t, "broker steady state", src, tgt, brokerErr)
}

// TestSyncFromBackup_MySQLSteadyStateKeyCollationMerge is the namespace half
// of the fix (the Bug 306 trap): a MySQL-family manifest records its tables
// with NO namespace while the binlog rows the broker replays name the
// database, so a seeded shape must answer for the table under the rows'
// namespace. The target's key is loosened by hand after the seed restore,
// which is the shape the guard exists for: the source keeps 'a' and 'A'
// apart under utf8mb4_bin, the target's utf8mb4_0900_ai_ci does not.
func TestSyncFromBackup_MySQLSteadyStateKeyCollationMerge(t *testing.T) {
	_, myRoot, myCleanup := startMySQL(t)
	defer myCleanup()
	myEng, _ := engines.Get("mysql")
	twfbDB{engine: "mysql", dsn: myRoot}.exec(t, "CREATE DATABASE kbm_s; CREATE DATABASE kbm_t")
	srcDSN, err := buildMySQLDSN(myRoot, "kbm_s")
	if err != nil {
		t.Fatal(err)
	}
	tgtDSN, err := buildMySQLDSN(myRoot, "kbm_t")
	if err != nil {
		t.Fatal(err)
	}
	src, tgt := twfbDB{engine: "mysql", dsn: srcDSN}, twfbDB{engine: "mysql", dsn: tgtDSN}
	src.exec(t, "CREATE TABLE kd (id INT NOT NULL PRIMARY KEY, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL UNIQUE, d INT); "+
		"INSERT INTO kd VALUES (1,'a',1),(2,'b',2)")
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
	if err := (&backup.Restore{Target: myEng, TargetDSN: tgtDSN, Store: store}).Run(ctx); err != nil {
		t.Fatalf("seed restore: %v", err)
	}
	tgt.exec(t, "ALTER TABLE kd MODIFY k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL")
	if err := krwCapture(t, "backup-incremental", myEng, srcDSN, "", store, full.BackupID, src,
		[]string{"INSERT INTO kd VALUES (99,'A',99)"}); err != nil {
		t.Fatalf("incremental: %v", err)
	}
	bctx, bcancel := context.WithTimeout(ctx, 30*time.Second)
	defer bcancel()
	brokerErr := (&SyncFromBackup{
		Target: myEng, TargetDSN: tgtDSN, Store: store,
		ChainURL: "test://kbm", StreamID: "kbm",
		PollInterval: time.Second, AtChainID: full.BackupID, SluiceVersion: "test",
	}).Run(bctx)
	if errors.Is(brokerErr, context.DeadlineExceeded) || errors.Is(brokerErr, context.Canceled) {
		brokerErr = nil
	}
	t.Logf("broker = %v; source %s target %s", brokerErr, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
	if !errors.Is(brokerErr, ir.ErrKeyCollationMerge) {
		t.Errorf("broker = %v; want KEY-COLLATION-MERGE", brokerErr)
	}
	kcfbGrade(t, "broker steady state (mysql)", src, tgt, brokerErr)
}
