//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0084 restore-side integration pins: parallel per-table apply on
// real targets. The class matrix (Bug-74 doctrine — pin the class, not
// the representative):
//
//   {plaintext, per-chunk encrypted} × {PG target, MySQL target
//   (cross-engine — restore parallelism is engine-generic, unlike the
//   backup side's snapshot gate)} × {single-manifest restore, chain
//   restore (full + incremental)}
//
// all ground-truthed with per-table content checksums against the live
// source, never row counts alone. Engagement is observer-asserted
// (restoreDispatchObserver), not timing-inferred. The per-chunk
// encrypted leg pins concurrent Envelope.UnwrapCEK across peer tables
// (the read-side mirror of the backup pool's WrapCEK pin).

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/crypto"
	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/backup"
	"sluicesync.dev/sluice/internal/pipeline/blobcodec"
	"sluicesync.dev/sluice/internal/pipeline/lineage"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestRestoreParallel_PG_RoundTripChecksums pins the headline path: a
// TableParallelism=4 restore of a multi-table PG backup engages the
// parallel apply (observer-asserted), and every table's content
// checksum matches the source.
func TestRestoreParallel_PG_RoundTripChecksums(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	tables := seedParallelBackupTables(t, sourceDSN, 6, 150)
	pgEng, _ := engines.Get("postgres")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	if err := (&backup.Backup{
		Source:    pgEng,
		SourceDSN: sourceDSN,
		Store:     store,
		ChunkRows: 40, // 150 rows → 4 chunks per table
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	gotP, gotReason := observeRestoreDispatch(t)
	if err := (&backup.Restore{
		Target:           pgEng,
		TargetDSN:        targetDSN,
		Store:            store,
		TableParallelism: 4,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}
	if *gotP <= 1 {
		t.Fatalf("dispatch = serial (reason %q); want the parallel apply engaged", *gotReason)
	}
	assertTablesMatchPG(t, sourceDSN, targetDSN, tables)
}

// neutralTableChecksum returns (row count, SUM(id), SUM(CHAR_LENGTH(label)))
// for one (id, label) table — aggregates whose values are
// engine-neutral, so a PG source and a MySQL target can be compared
// directly. driver is the database/sql driver name.
func neutralTableChecksum(t *testing.T, driver, dsn, table string) (count, idSum, lenSum int64) {
	t.Helper()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open %s: %v", dsn, err)
	}
	defer func() { _ = db.Close() }()
	q := fmt.Sprintf(
		"SELECT COUNT(*), COALESCE(SUM(id), 0), COALESCE(SUM(CHAR_LENGTH(label)), 0) FROM %s", table,
	)
	if err := db.QueryRow(q).Scan(&count, &idSum, &lenSum); err != nil {
		t.Fatalf("checksum %s: %v", table, err)
	}
	return count, idSum, lenSum
}

// TestRestoreParallel_PGToMySQL_CrossEngine pins the engine-generic
// claim: restore parallelism engages on a MySQL TARGET too (no
// snapshot gate on the write side — the contrast with the backup pool,
// where MySQL stays serial). A PG backup restores cross-engine into
// MySQL at TableParallelism=4 and every table's engine-neutral
// aggregates match the source.
func TestRestoreParallel_PGToMySQL_CrossEngine(t *testing.T) {
	pgSourceDSN, _, pgCleanup := startPostgresLogical(t)
	defer pgCleanup()
	_, mysqlTargetDSN, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()

	tables := []string{"xbk_00", "xbk_01", "xbk_02", "xbk_03"}
	for i, table := range tables {
		applyDDL(t, pgSourceDSN, fmt.Sprintf(`
			CREATE TABLE %s (
				id    BIGINT PRIMARY KEY,
				label VARCHAR(64) NOT NULL
			);
			INSERT INTO %s (id, label)
			SELECT g, 'tbl-%02d-row-' || g FROM generate_series(1, 120) g;
		`, table, table, i))
	}

	pgEng, _ := engines.Get("postgres")
	mysqlEng, _ := engines.Get("mysql")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	if err := (&backup.Backup{
		Source:    pgEng,
		SourceDSN: pgSourceDSN,
		Store:     store,
		ChunkRows: 50,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	gotP, gotReason := observeRestoreDispatch(t)
	if err := (&backup.Restore{
		Target:           mysqlEng,
		TargetDSN:        mysqlTargetDSN,
		Store:            store,
		TableParallelism: 4,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run (PG → MySQL): %v", err)
	}
	if *gotP <= 1 {
		t.Fatalf("dispatch = serial (reason %q); want parallel — restore parallelism is engine-generic on the write side", *gotReason)
	}

	for _, table := range tables {
		srcN, srcID, srcLen := neutralTableChecksum(t, "pgx", pgSourceDSN, table)
		dstN, dstID, dstLen := neutralTableChecksum(t, "mysql", mysqlTargetDSN, table)
		if srcN != dstN || srcID != dstID || srcLen != dstLen {
			t.Errorf("table %s: source (n=%d id=%d len=%d) != target (n=%d id=%d len=%d)",
				table, srcN, srcID, srcLen, dstN, dstID, dstLen)
		}
	}
}

// TestRestoreParallel_PG_EncryptedPerChunkRoundTrip pins the per-chunk
// encryption leg: peer tables unwrap their chunks' CEKs CONCURRENTLY
// through one shared envelope (UnwrapCEK verified read-only on all
// four envelope impls; -race in CI is the load-bearing leg), and the
// restored contents checksum-match the source.
func TestRestoreParallel_PG_EncryptedPerChunkRoundTrip(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	tables := seedParallelBackupTables(t, sourceDSN, 4, 120)
	pgEng, _ := engines.Get("postgres")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	const passphrase = "parallel-restore-test-passphrase"
	env := newTestPassphraseEnvelope(t, passphrase)
	if err := (&backup.Backup{
		Source:     pgEng,
		SourceDSN:  sourceDSN,
		Store:      store,
		ChunkRows:  40,
		Encryption: &lineage.BackupEncryption{Envelope: env, Mode: crypto.EncryptModePerChunk},
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	gotP, gotReason := observeRestoreDispatch(t)
	if err := (&backup.Restore{
		Target:           pgEng,
		TargetDSN:        targetDSN,
		Store:            store,
		Envelope:         envelopeFromManifest(t, store, passphrase),
		TableParallelism: 4,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run (per-chunk encrypted): %v", err)
	}
	if *gotP <= 1 {
		t.Fatalf("dispatch = serial (reason %q); want parallel", *gotReason)
	}
	assertTablesMatchPG(t, sourceDSN, targetDSN, tables)
}

// TestChainRestoreParallel_PG_FullPlusIncremental pins the
// ChainRestore threading: a chain (multi-table full + one incremental
// with INSERT/UPDATE) restored at TableParallelism=4 engages the
// parallel apply for the full (observer-asserted) while the
// incremental's ordered change replay lands the post-window state —
// every table's content checksum matches the live source.
func TestChainRestoreParallel_PG_FullPlusIncremental(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	staticTables := seedParallelBackupTables(t, sourceDSN, 3, 100)
	applyDDL(t, sourceDSN, `
		CREATE TABLE users (
			id    BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
			email VARCHAR(255) NOT NULL,
			active BOOLEAN NOT NULL DEFAULT true
		);
		ALTER TABLE users REPLICA IDENTITY FULL;
		INSERT INTO users (email, active) VALUES ('alice@example.com', true);
	`)
	tables := append([]string{}, staticTables...)
	tables = append(tables, "users")

	pgEng, _ := engines.Get("postgres")
	store, err := blobcodec.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	applyDDL(t, sourceDSN, `CREATE PUBLICATION sluice_pub FOR ALL TABLES`)
	slotLSN, err := createPGLogicalSlotReturningLSN(t, sourceDSN, "sluice_slot")
	if err != nil {
		t.Fatalf("create slot: %v", err)
	}
	defer dropPGLogicalSlot(t, sourceDSN, "sluice_slot")

	// 1. Full backup, anchored at the slot's consistent point.
	if err := (&backup.Backup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		ChunkRows: 40, SluiceVersion: "test",
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}
	full, err := lineage.ReadManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("lineage.ReadManifest: %v", err)
	}
	full.Kind = irbackup.BackupKindFull
	full.EndPosition = ir.Position{
		Engine: "postgres",
		Token:  fmt.Sprintf(`{"slot":"sluice_slot","lsn":%q}`, slotLSN),
	}
	full.BackupID = irbackup.ComputeBackupID(full)
	if err := lineage.WriteManifestAt(context.Background(), store, lineage.ManifestFileName, full); err != nil {
		t.Fatalf("rewrite full: %v", err)
	}

	// 2. Incremental: INSERT bob + UPDATE alice.
	applyDDL(t, sourceDSN, `
		INSERT INTO users (email, active) VALUES ('bob@example.com', false);
		UPDATE users SET active = false WHERE email = 'alice@example.com';
	`)
	incrCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := (&IncrementalBackup{
		Source: pgEng, SourceDSN: sourceDSN, Store: store,
		ParentRef:    full.BackupID,
		Window:       10 * time.Second,
		MaxChanges:   10,
		ChunkChanges: 100,
	}).Run(incrCtx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}

	// 3. Chain restore at TableParallelism=4. The full's bulk-apply
	//    engages the pool (clamped to the 4 tables); the incremental
	//    replay path is untouched.
	gotP, gotReason := observeRestoreDispatch(t)
	if err := (&backup.ChainRestore{
		Target:           pgEng,
		TargetDSN:        targetDSN,
		Store:            store,
		TableParallelism: 4,
	}).Run(context.Background()); err != nil {
		t.Fatalf("ChainRestore.Run: %v", err)
	}
	if *gotP <= 1 {
		t.Fatalf("dispatch = serial (reason %q); want the full's parallel apply engaged via the ChainRestore threading", *gotReason)
	}

	// 4. The source already carries the post-incremental state, so a
	//    straight source-vs-target checksum covers both the full's
	//    parallel apply and the incremental replay.
	assertTablesMatchPG(t, sourceDSN, targetDSN, tables)
}
