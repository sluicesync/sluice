//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration pins for --chain-slot chain provisioning + the
// chain-resume preflight (task #40), against real Postgres.
//
// The silent-loss shape these exist for, observed live in the
// 2026-06-10 backup benchmark: a replication slot created AFTER the
// parent full cannot serve the WAL between the full's anchor and its
// own creation — PostgreSQL silently fast-forwards START_REPLICATION
// to the slot's confirmed_flush_lsn, so without the preflight the
// incremental SUCCEEDS while the chain silently misses those writes.
// TestIncremental_LateCreatedSlotRefused pins the loud refusal;
// TestBackup_ChainSlot_EndToEndChain pins the provisioning shape that
// makes the manual slot dance (and the hazard) unnecessary.

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// pgQueryOne runs a single-value query against dsn.
func pgQueryOne[T any](t *testing.T, dsn, query string, args ...any) T {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	defer func() { _ = db.Close() }()
	var v T
	if err := db.QueryRow(query, args...).Scan(&v); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return v
}

func pgSlotExists(t *testing.T, dsn, slot string) bool {
	t.Helper()
	return pgQueryOne[bool](t, dsn, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slot)
}

// chainSeedDDL is the shared source shape: PK table + three rows.
const chainSeedDDL = `
	CREATE TABLE users (
		id     BIGINT PRIMARY KEY,
		email  VARCHAR(255) NOT NULL,
		active BOOLEAN NOT NULL DEFAULT true
	);
	ALTER TABLE users REPLICA IDENTITY FULL;
	INSERT INTO users (id, email, active) VALUES
		(1, 'alice@example.com', true),
		(2, 'bob@example.com',   true),
		(3, 'carol@example.com', false);
`

// TestBackup_ChainSlot_EndToEndChain pins the --chain-slot happy
// path: full --chain-slot provisions slot + publication at the
// anchor; an incremental chains immediately (no manual slot dance, no
// EndPosition rewriting); chain-restore lands the full + delta on a
// fresh target with matching checksums. Before --chain-slot, this
// flow required the operator to pre-create both objects AND the test
// suite to hand-rewrite the full's EndPosition (see
// chain_restore_cross_integration_test.go) — the provisioning makes
// the recorded anchor correct by construction.
func TestBackup_ChainSlot_EndToEndChain(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()
	applyDDL(t, sourceDSN, chainSeedDDL)

	pgEng, _ := engines.Get("postgres")
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	// 1. Full backup with chain provisioning. Note: NO manual
	// publication, NO manual slot.
	if err := (&Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		SluiceVersion: "test", ChainSlot: true,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run (--chain-slot): %v", err)
	}
	defer dropPGLogicalSlot(t, sourceDSN, "sluice_slot")

	if !pgSlotExists(t, sourceDSN, "sluice_slot") {
		t.Fatal("chain slot sluice_slot missing after successful --chain-slot full (CommitFn semantics broken)")
	}
	if !pgQueryOne[bool](t, sourceDSN, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'sluice_pub')") {
		t.Fatal("publication sluice_pub missing after --chain-slot full (pgoutput cannot decode the chain without it)")
	}

	// 2. Post-full delta: insert, update, delete.
	applyDDL(t, sourceDSN, `
		INSERT INTO users (id, email, active) VALUES (4, 'dave@example.com', true);
		UPDATE users SET active = false WHERE id = 1;
		DELETE FROM users WHERE id = 3;
	`)

	// 3. Incremental — must chain with zero manual setup.
	ctx, c := context.WithTimeout(context.Background(), 90*time.Second)
	defer c()
	if err := (&IncrementalBackup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		Window: 10 * time.Second, MaxChanges: 3, ChunkChanges: 100,
	}).Run(ctx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}

	// 4. Chain-restore into the fresh target and checksum (count +
	// sum(id) + active-count — value content, not just row count).
	if err := (&ChainRestore{
		Target: pgEng, TargetDSN: targetDSN, Store: store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("ChainRestore.Run: %v", err)
	}
	type sums struct {
		n, sumID, active int64
	}
	read := func(dsn string) sums {
		return sums{
			n:      pgQueryOne[int64](t, dsn, "SELECT COUNT(*) FROM users"),
			sumID:  pgQueryOne[int64](t, dsn, "SELECT COALESCE(SUM(id),0) FROM users"),
			active: pgQueryOne[int64](t, dsn, "SELECT COUNT(*) FROM users WHERE active"),
		}
	}
	src, dst := read(sourceDSN), read(targetDSN)
	if src != dst {
		t.Errorf("chain restore checksum mismatch: source %+v != target %+v", src, dst)
	}
	if src.n != 3 { // 3 seed + 1 insert - 1 delete
		t.Errorf("source rows = %d; want 3 (test premise broken)", src.n)
	}
}

// TestIncremental_LateCreatedSlotRefused pins the silent-loss
// refusal: writes land between the parent full and the slot's
// creation, so the slot cannot serve them — the incremental must
// refuse loudly, NOT succeed with a silently gapped chain. Remove the
// preflight in incremental.go and this test fails with the
// incremental succeeding (the walsender fast-forwards past the
// delta).
func TestIncremental_LateCreatedSlotRefused(t *testing.T) {
	sourceDSN, _, cleanup := startPostgresLogical(t)
	defer cleanup()
	applyDDL(t, sourceDSN, chainSeedDDL)
	// Publication exists up-front so the ONLY defect in this scenario
	// is the late slot.
	applyDDL(t, sourceDSN, `CREATE PUBLICATION sluice_pub FOR ALL TABLES`)

	pgEng, _ := engines.Get("postgres")
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	// 1. Anchored full WITHOUT chain provisioning (the default).
	if err := (&Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store, SluiceVersion: "test",
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	// 2. Writes the chain is now obligated to carry.
	applyDDL(t, sourceDSN, `INSERT INTO users (id, email) VALUES (4, 'dave@example.com');`)

	// 3. The operator's (natural, wrong) recovery: create the slot NOW.
	if _, err := createPGLogicalSlotReturningLSN(t, sourceDSN, "sluice_slot"); err != nil {
		t.Fatalf("create late slot: %v", err)
	}
	defer dropPGLogicalSlot(t, sourceDSN, "sluice_slot")

	// 4. The incremental must refuse: confirmed_flush is AHEAD of the
	// parent's terminal position; the WAL in between is unservable.
	ctx, c := context.WithTimeout(context.Background(), 60*time.Second)
	defer c()
	err = (&IncrementalBackup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		Window: 5 * time.Second, MaxChanges: 1, ChunkChanges: 100,
	}).Run(ctx)
	if err == nil {
		t.Fatal("IncrementalBackup.Run succeeded against a late-created slot; want the loud chain-preflight refusal (silent-loss regression)")
	}
	if !strings.Contains(err.Error(), "AHEAD") {
		t.Errorf("err = %v; want the confirmed_flush-AHEAD refusal", err)
	}
}

// TestIncremental_MissingSlotRefusedWithGuidance pins the improved
// slot-missing refusal: the message must say the slot may never have
// existed and point at --chain-slot — not the misleading old "source
// has pruned" framing.
func TestIncremental_MissingSlotRefusedWithGuidance(t *testing.T) {
	sourceDSN, _, cleanup := startPostgresLogical(t)
	defer cleanup()
	applyDDL(t, sourceDSN, chainSeedDDL)

	pgEng, _ := engines.Get("postgres")
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	if err := (&Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store, SluiceVersion: "test",
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	ctx, c := context.WithTimeout(context.Background(), 60*time.Second)
	defer c()
	err = (&IncrementalBackup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		Window: 5 * time.Second, MaxChanges: 1, ChunkChanges: 100,
	}).Run(ctx)
	if err == nil {
		t.Fatal("IncrementalBackup.Run succeeded with no slot; want loud refusal")
	}
	for _, want := range []string{"does not exist", "--chain-slot"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q missing %q", err.Error(), want)
		}
	}
}

// TestBackup_ChainSlot_ExistingSlotRefused pins the anchor guard: an
// existing slot's consistent point is NOT this backup's anchor, so
// --chain-slot must refuse rather than silently reuse it.
func TestBackup_ChainSlot_ExistingSlotRefused(t *testing.T) {
	sourceDSN, _, cleanup := startPostgresLogical(t)
	defer cleanup()
	applyDDL(t, sourceDSN, chainSeedDDL)

	if _, err := createPGLogicalSlotReturningLSN(t, sourceDSN, "sluice_slot"); err != nil {
		t.Fatalf("pre-create slot: %v", err)
	}
	defer dropPGLogicalSlot(t, sourceDSN, "sluice_slot")

	pgEng, _ := engines.Get("postgres")
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}
	err = (&Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		SluiceVersion: "test", ChainSlot: true,
	}).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("err = %v; want loud already-exists refusal", err)
	}
}

// TestBackup_ChainSlot_UncommittedCloseDropsSlot pins the engine-side
// failure semantics directly: a --chain-slot snapshot Closed WITHOUT
// Commit (= the backup failed) must drop the slot so retries start
// clean; Commit-then-Close keeps it.
func TestBackup_ChainSlot_UncommittedCloseDropsSlot(t *testing.T) {
	sourceDSN, _, cleanup := startPostgresLogical(t)
	defer cleanup()
	applyDDL(t, sourceDSN, chainSeedDDL)

	pgEng, _ := engines.Get("postgres")
	opener, ok := pgEng.(irbackup.BackupSnapshotOpener)
	if !ok {
		t.Fatal("postgres engine does not implement BackupSnapshotOpener")
	}
	opts := irbackup.BackupSnapshotOptions{PersistChainSlot: true}

	// Failure shape: open → Close (no Commit) → slot gone.
	snap, err := opener.OpenBackupSnapshot(context.Background(), sourceDSN, opts)
	if err != nil {
		t.Fatalf("OpenBackupSnapshot: %v", err)
	}
	if !pgSlotExists(t, sourceDSN, "sluice_slot") {
		t.Fatal("chain slot missing while snapshot open")
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if pgSlotExists(t, sourceDSN, "sluice_slot") {
		t.Fatal("chain slot survived an UNCOMMITTED Close; failed runs must drop it")
	}

	// Success shape: open → Commit → Close → slot kept.
	snap, err = opener.OpenBackupSnapshot(context.Background(), sourceDSN, opts)
	if err != nil {
		t.Fatalf("OpenBackupSnapshot (second): %v", err)
	}
	if err := snap.Commit(context.Background()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := snap.Close(); err != nil {
		t.Fatalf("Close (after commit): %v", err)
	}
	if !pgSlotExists(t, sourceDSN, "sluice_slot") {
		t.Fatal("chain slot missing after COMMITTED Close; the chain anchor was lost")
	}
	dropPGLogicalSlot(t, sourceDSN, "sluice_slot")
}
