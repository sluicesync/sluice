//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// MySQL → MySQL counterpart of stream_pg_integration_test.go. Same
// Phase 4 acceptance criteria: long-running stream produces rolling
// incrementals, stream stop, concurrent-writer protection.

package pipeline

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"
	"github.com/orware/sluice/internal/ir"

	_ "github.com/orware/sluice/internal/engines/mysql"
)

// TestBackupStream_MySQL_RolloverByMaxChanges drives the MySQL happy-
// path. Mirrors the PG version but uses the MySQL CDC reader's binlog
// stream as the change source.
//
// Acceptance criteria 2, 4, 5 (MySQL flavour).
func TestBackupStream_MySQL_RolloverByMaxChanges(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startMySQLBinlog(t)
	defer cleanup()

	const seedDDL = `
		CREATE TABLE users (
			id    BIGINT       NOT NULL AUTO_INCREMENT,
			email VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
		INSERT INTO users (email) VALUES ('alice@example.com'), ('bob@example.com');
	`
	applyDDLMySQL(t, sourceDSN, seedDDL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}

	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	// Full backup.
	if err := (&Backup{
		Source: mysqlEng, SourceDSN: sourceDSN, Store: store, SluiceVersion: "test",
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	binlogFile, binlogPos := readMySQLBinlogPos(t, sourceDSN)
	full, _ := readManifest(context.Background(), store)
	full.Kind = ir.BackupKindFull
	full.EndPosition = ir.Position{
		Engine: "mysql",
		Token:  fmt.Sprintf(`{"mode":"file_pos","file":%q,"pos":%d}`, binlogFile, binlogPos),
	}
	full.BackupID = ir.ComputeBackupID(full)
	if err := writeManifestAt(context.Background(), store, ManifestFileName, full); err != nil {
		t.Fatalf("rewrite full: %v", err)
	}

	// Drive 25 inserts after the binlog pos was captured.
	for i := 0; i < 25; i++ {
		applyDDLMySQL(t, sourceDSN, fmt.Sprintf(
			`INSERT INTO users (email) VALUES ('user%d@example.com');`, i))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stream := &BackupStream{
		Source:             mysqlEng,
		SourceDSN:          sourceDSN,
		Store:              store,
		ParentRef:          full.BackupID,
		RolloverWindow:     2 * time.Second,
		RolloverMaxChanges: 10,
		RolloverMaxBytes:   1 << 30,
		ChunkChanges:       100,
		SluiceVersion:      "test",
	}

	streamErr := make(chan error, 1)
	go func() { streamErr <- stream.Run(ctx) }()

	// Wait for the stream to commit at least 2 rollovers AND for its
	// rollover throughput to settle (no new manifest in ~3s, which is
	// > rollover-window so an idle final rollover would have committed
	// or timed out empty before we cancel) so the source's 25 INSERTs
	// have all been observed by the CDC pump and fed into a committed
	// rollover. Polling on count alone (the pre-v0.20.1 shape) cancels
	// too eagerly when each rollover takes ~250ms (Bug 38 fix's per-
	// rollover schema refresh) — by the time 2 incrementals appear on
	// disk, the cancel races with the in-flight rollover that contains
	// the trailing inserts and on MySQL the chunk-buffered events past
	// the cancel point are lost when the CDC pump exits.
	deadline := time.Now().Add(45 * time.Second)
	var lastCount, stableTicks int
	for time.Now().Before(deadline) {
		records, _ := listAllManifestsViaWalk(context.Background(), store)
		var incrCount int
		for _, r := range records {
			if r.manifest.Kind == ir.BackupKindIncremental {
				incrCount++
			}
		}
		if incrCount == lastCount {
			stableTicks++
		} else {
			stableTicks = 0
			lastCount = incrCount
		}
		if incrCount >= 2 && stableTicks >= 6 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	cancel()

	select {
	case err := <-streamErr:
		if err != nil {
			t.Errorf("stream.Run after cancel = %v; want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stream.Run did not exit within 10s")
	}

	records, _ := listAllManifestsViaWalk(context.Background(), store)
	var incrementals []*ir.Manifest
	for _, r := range records {
		if r.manifest.Kind == ir.BackupKindIncremental {
			incrementals = append(incrementals, r.manifest)
		}
	}
	if len(incrementals) < 2 {
		t.Fatalf("incremental rollovers = %d; want >= 2", len(incrementals))
	}

	total, mismatches, err := VerifyBackup(context.Background(), store)
	if err != nil {
		t.Fatalf("VerifyBackup: %v", err)
	}
	if mismatches != 0 {
		t.Errorf("VerifyBackup: %d of %d chunks failed", mismatches, total)
	}

	// Chain restore into a fresh target.
	if err := (&Restore{
		Target: mysqlEng, TargetDSN: targetDSN, Store: store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}

	got := mysqlQueryEmails(t, targetDSN)
	want := map[string]bool{
		"alice@example.com": true,
		"bob@example.com":   true,
	}
	for i := 0; i < 25; i++ {
		want[fmt.Sprintf("user%d@example.com", i)] = true
	}
	gotMap := map[string]bool{}
	for _, e := range got {
		gotMap[e] = true
	}
	for w := range want {
		if !gotMap[w] {
			t.Errorf("target missing email %q (count: %d)", w, len(got))
		}
	}
}
