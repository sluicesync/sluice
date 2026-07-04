//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Chain-restore standalone-sequence tail re-prime pin (item 51, delta
// review finding #2). The base full primes a carried sequence at the
// BASE manifest's captured position; incremental links then apply
// rows that consumed LATER values. Pre-fix, nothing re-primed at the
// chain tail, so the restored sequence silently re-issued every
// number the links consumed — a REGRESSION versus pre-item-51, where
// the sequence didn't exist on the target and the first nextval()
// failed loudly instead.

package pipeline

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestBackup_ChainRestore_StandaloneSequenceReprimedAtTail:
//
//  1. Source: standalone sequence (START 1000 INCREMENT 5) + a table
//     defaulting from it; two rows consume 1000, 1005.
//  2. Full backup — the manifest captures the sequence at 1005.
//  3. Two MORE source inserts consume 1010, 1015.
//  4. Incremental backup — its manifest's schema snapshot (read at
//     the incremental's start) captures the sequence at 1015.
//  5. Chain restore into a fresh target: the base full creates +
//     primes the sequence at 1005; the incremental applies the
//     1010/1015 rows; the tail re-prime advances the sequence to the
//     NEWEST captured position (1015).
//  6. The first post-restore insert must draw 1020 — pre-fix it drew
//     1010, colliding with a link-applied row.
func TestBackup_ChainRestore_StandaloneSequenceReprimedAtTail(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgresLogical(t)
	defer cleanup()

	// The PK IS the standalone-sequence column — deliberately no
	// identity column: the identity analogue of the chain-tail gap is
	// PRE-EXISTING and out of this fix's scope (SyncIdentitySequences
	// runs only after the base full, so identity sequences also lag
	// link-applied rows — filed separately; an earlier draft of this
	// test tripped it as an orders_pkey collision on the identity id).
	applyDDL(t, sourceDSN, `
		CREATE SEQUENCE order_number_seq START WITH 1000 INCREMENT BY 5;
		CREATE TABLE orders (
			order_number BIGINT PRIMARY KEY DEFAULT nextval('order_number_seq'),
			note         TEXT
		);
		ALTER TABLE orders REPLICA IDENTITY FULL;
		INSERT INTO orders (note) VALUES ('full-a');
		INSERT INTO orders (note) VALUES ('full-b');
	`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	// Chain-handoff slot + publication, mirroring the snapshot-anchored
	// backup test's setup.
	applyDDL(t, sourceDSN, `CREATE PUBLICATION sluice_pub FOR ALL TABLES`)
	if _, err := createPGLogicalSlotReturningLSN(t, sourceDSN, "sluice_slot"); err != nil {
		t.Fatalf("create chain slot: %v", err)
	}
	defer dropPGLogicalSlot(t, sourceDSN, "sluice_slot")

	if err := (&Backup{
		Source:        pgEng,
		SourceDSN:     sourceDSN,
		Store:         store,
		SluiceVersion: "v0.99.175-test",
		SlotName:      "sluice_slot",
	}).Run(context.Background()); err != nil {
		t.Fatalf("Backup.Run: %v", err)
	}

	// Post-full source activity: two more inserts consume 1010, 1015.
	applyDDL(t, sourceDSN, `
		INSERT INTO orders (note) VALUES ('incr-a');
		INSERT INTO orders (note) VALUES ('incr-b');
	`)

	full, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	incrCtx, incrCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer incrCancel()
	incr := &IncrementalBackup{
		Source:        pgEng,
		SourceDSN:     sourceDSN,
		Store:         store,
		ParentRef:     full.BackupID,
		Window:        15 * time.Second,
		MaxChanges:    50,
		ChunkChanges:  20,
		SluiceVersion: "v0.99.175-test",
	}
	if err := incr.Run(incrCtx); err != nil {
		t.Fatalf("IncrementalBackup.Run: %v", err)
	}

	if err := (&Restore{
		Target:    pgEng,
		TargetDSN: targetDSN,
		Store:     store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("Restore.Run: %v", err)
	}

	db, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Sanity: every link-applied row landed.
	var rows int
	if err := db.QueryRowContext(ctx,
		"SELECT count(*) FROM orders WHERE order_number IN (1000, 1005, 1010, 1015)").Scan(&rows); err != nil {
		t.Fatalf("count restored rows: %v", err)
	}
	if rows != 4 {
		t.Fatalf("restored rows with expected order_numbers = %d; want 4", rows)
	}

	// THE pin: the first post-restore insert continues past the
	// newest link's consumption. Pre-fix the sequence sat at the base
	// full's 1005 and this drew 1010 — colliding with a link-applied
	// row (here loudly, because the sequence column is the PK; on a
	// non-key column the same stale position is a SILENT duplicate).
	var orderNumber int64
	if err := db.QueryRowContext(ctx,
		"INSERT INTO orders (note) VALUES ('post-restore') RETURNING order_number").Scan(&orderNumber); err != nil {
		t.Fatalf("post-restore insert: %v", err)
	}
	if orderNumber != 1020 {
		t.Errorf("post-restore order_number = %d; want 1020 (chain tail must re-prime from the NEWEST link's captured position)", orderNumber)
	}
}
