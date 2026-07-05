//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Phase 3.3 integration tests for `sluice sync start
// --position-from-manifest` on MySQL sources. Covers acceptance
// criterion 5: chain handoff via the chain's terminal GTID/binlog
// position; CDC catches up cleanly.

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
)

// TestBackup_RecordsEndPosition_MySQLIntegration pins acceptance
// criterion 1 on MySQL: a v0.17.2 full records EndPosition without
// manual patching.
func TestBackup_RecordsEndPosition_MySQLIntegration(t *testing.T) {
	sourceDSN, _, cleanup := startMySQLBinlog(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('alice@example.com');
	`
	applyDDLMySQL(t, sourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	dir := t.TempDir()
	store, err := blobcodec.NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&Backup{
		Source: mysqlEng, SourceDSN: sourceDSN, Store: store,
		SluiceVersion: "v0.17.2-test",
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	full, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("lineage.ReadManifest: %v", err)
	}
	if full.EndPosition.Engine != "mysql" {
		t.Errorf("EndPosition.Engine = %q; want mysql", full.EndPosition.Engine)
	}
	// File/pos mode (test container doesn't enable gtid_mode).
	if !strings.Contains(full.EndPosition.Token, `"mode":"file_pos"`) {
		t.Errorf("EndPosition.Token = %q; want mode=file_pos", full.EndPosition.Token)
	}
	if !strings.Contains(full.EndPosition.Token, `"file":`) {
		t.Errorf("EndPosition.Token = %q; want file=<binlog>", full.EndPosition.Token)
	}
	if full.BackupID == "" {
		t.Error("BackupID empty after EndPosition recording")
	}
}

// TestStreamer_SyncStart_PositionFromManifest_MySQL_HappyPath pins acceptance
// criterion 5: take a v0.17.2 full, restore into target, insert on
// source, run sync with --position-from-manifest, verify CDC catches
// up.
func TestStreamer_SyncStart_PositionFromManifest_MySQL_HappyPath(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('alice@example.com');
	`
	applyDDLMySQL(t, sourceDSN, seedDDL)

	mysqlEng, _ := engines.Get("mysql")
	dir := t.TempDir()
	store, _ := blobcodec.NewLocalStore(dir)

	// 1. Full backup — Phase 3.3.A records EndPosition automatically.
	if err := (&Backup{
		Source: mysqlEng, SourceDSN: sourceDSN, Store: store,
		SluiceVersion: "v0.17.2-test",
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	// 2. Restore into target.
	if err := (&Restore{
		Target: mysqlEng, TargetDSN: targetDSN, Store: store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}

	// 3. Insert on source between backup and sync — these must be
	//    picked up by chain handoff.
	applyDDLMySQL(t, sourceDSN, `INSERT INTO users (email) VALUES ('bob@example.com'), ('carol@example.com')`)

	// 4. sync start --position-from-manifest. Streamer reads chain
	//    terminal position, opens binlog reader from there, applies
	//    the new rows.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	streamer := &Streamer{
		Source:                    mysqlEng,
		Target:                    mysqlEng,
		SourceDSN:                 sourceDSN,
		TargetDSN:                 targetDSN,
		StreamID:                  "test-position-from-manifest-mysql",
		PositionFromManifestStore: store,
	}
	runErr := make(chan error, 1)
	go func() { runErr <- streamer.Run(ctx) }()
	if !waitForRowCountMySQL(t, targetDSN, "users", 3, 25*time.Second) {
		cancel()
		<-runErr
		t.Fatal("target did not reach 3 rows; CDC handoff didn't catch up")
	}
	cancel()
	if err := <-runErr; err != nil && !isContextCanceled(err) {
		t.Fatalf("streamer.Run: %v", err)
	}

	emails := mysqlQueryEmails(t, targetDSN)
	want := map[string]bool{"alice@example.com": true, "bob@example.com": true, "carol@example.com": true}
	got := map[string]bool{}
	for _, e := range emails {
		got[e] = true
	}
	for w := range want {
		if !got[w] {
			t.Errorf("missing email %q on target; got %v", w, emails)
		}
	}
}
