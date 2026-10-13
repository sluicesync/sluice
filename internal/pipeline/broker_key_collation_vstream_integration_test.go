//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// TestSyncFromBackup_VStreamCharacterKey_NoKeyChange_ReplaysExactly pins the
// review-of-d6ecb939 finding 1 regression: a VStream window's schema version
// records no collation, the segment's recorded schema (the schema reader's)
// does, and comparing them with the version on the BEFORE side read every
// such window as a key-collation change — `sync from-backup` refused every
// incremental of a VARCHAR-keyed table with KEY-COLLATION-HISTORY-UNORDERED,
// permanently. A window that changes no schema must replay exactly.
//
// The independent expected value is the source's rows read by id:HEX(k)
// through vtgate after the window (kcfbGrade).
func TestSyncFromBackup_VStreamCharacterKey_NoKeyChange_ReplaysExactly(t *testing.T) {
	const keyspace = "kcvs"
	mysqlDSN, grpcEndpoint, _, cleanupSrc := startShardedVTTestServer(t, keyspace, 1)
	defer cleanupSrc()
	tgtDSN, cleanupTgt := startPGTarget(t)
	defer cleanupTgt()
	srcEng, _ := engines.Get("planetscale")
	pgEng, _ := engines.Get("postgres")
	src, tgt := twfbDB{engine: "mysql", dsn: mysqlDSN}, twfbDB{engine: "postgres", dsn: tgtDSN}
	src.exec(t, "CREATE TABLE kd (id INT NOT NULL, k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL, d INT, PRIMARY KEY (k))")
	src.exec(t, "INSERT INTO kd VALUES (1,'a',1),(2,'b',2)")
	sourceDSN := fmt.Sprintf("%s&vstream_endpoint=%s&vstream_transport=plaintext&vstream_auth=none&vstream_shards=0", mysqlDSN, grpcEndpoint)

	ctx := context.Background()
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := (&backup.Backup{Source: srcEng, SourceDSN: sourceDSN, Store: store, SluiceVersion: "test"}).Run(ctx); err != nil {
		t.Fatalf("full: %v", err)
	}
	full, err := lineage.ReadManifest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&backup.Restore{Target: pgEng, TargetDSN: tgtDSN, Store: store}).Run(ctx); err != nil {
		t.Fatalf("seed restore: %v", err)
	}
	src.exec(t, "INSERT INTO kd VALUES (99,'A2',99)")
	src.exec(t, "UPDATE kd SET d = 10 WHERE id = 1")
	runCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	err = (&IncrementalBackup{
		Source: srcEng, SourceDSN: sourceDSN, Store: store, ParentRef: full.BackupID,
		Window: 15 * time.Second, SluiceVersion: "test",
	}).Run(runCtx)
	cancel()
	if err != nil {
		t.Fatalf("incremental: %v", err)
	}
	bctx, bcancel := context.WithTimeout(ctx, 40*time.Second)
	defer bcancel()
	brokerErr := (&SyncFromBackup{
		Target: pgEng, TargetDSN: tgtDSN, Store: store,
		ChainURL: "test://kcvs", StreamID: "kcvs",
		PollInterval: time.Second, AtChainID: full.BackupID, SluiceVersion: "test",
	}).Run(bctx)
	if errors.Is(brokerErr, context.DeadlineExceeded) || errors.Is(brokerErr, context.Canceled) {
		brokerErr = nil
	}
	t.Logf("broker = %v; source %s target %s", brokerErr, kcfbRender(kcfbRows(t, src)), kcfbRender(kcfbRows(t, tgt)))
	if brokerErr != nil {
		t.Fatalf("broker = %v; want the window replayed (it changes no key collation)", brokerErr)
	}
	kcfbGrade(t, "vstream-broker", src, tgt, nil)
}
