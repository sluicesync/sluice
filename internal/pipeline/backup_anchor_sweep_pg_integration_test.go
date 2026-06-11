//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 137 end-to-end pin: a resumed `backup full` against a Postgres
// source sweeps the persistent anchor replication slot a backup
// crashed under a pre-fix binary left behind — and a FRESH run leaves
// it alone (the sweep's trigger is proof of a prior mid-flight death,
// i.e. an in-progress manifest). The post-run assertion that ZERO
// `sluice_backup_anchor_*` slots remain also pins the orchestrator-
// driven happy path of the protocol-TEMPORARY anchor itself: each
// run's own anchor must be gone once the run completes. The
// kill-shape (no Close at all) is pinned at the engine level in
// engines/postgres/backup_anchor_slot_integration_test.go.

package pipeline

import (
	"context"
	"database/sql"
	"slices"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// pgAnchorSlots lists the backup-anchor-prefixed replication slots on
// the source.
func pgAnchorSlots(t *testing.T, dsn string) []string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.Query(
		`SELECT slot_name FROM pg_replication_slots
		  WHERE slot_name LIKE 'sluice\_backup\_anchor\_%' ESCAPE '\'
		  ORDER BY slot_name`,
	)
	if err != nil {
		t.Fatalf("list anchor slots: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan anchor slot: %v", err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate anchor slots: %v", err)
	}
	return out
}

// waitForPGAnchorSlots polls until the anchor-slot set equals want.
// The temporary anchor's server-side release happens on walsender
// exit, which can trail snap.Close() by a beat — polling keeps the
// assertion honest without racing the backend teardown.
func waitForPGAnchorSlots(t *testing.T, dsn string, want []string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		got := pgAnchorSlots(t, dsn)
		if slices.Equal(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("anchor slots = %v after %s; want %v", got, timeout, want)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestBackup_ResumeSweepsOrphanedAnchorSlot_Postgres(t *testing.T) {
	sourceDSN, _, cleanup := startPostgresLogical(t)
	defer cleanup()

	applyDDL(t, sourceDSN, `
		CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL);
		INSERT INTO users (id, email) VALUES (1, 'alice@example.com');
	`)

	// The leak shape under test: a PERSISTENT anchor slot with an
	// ancient embedded timestamp, exactly what a backup hard-killed
	// under a pre-Bug-137 binary leaves on the source.
	const orphan = "sluice_backup_anchor_123"
	applyDDL(t, sourceDSN, `SELECT pg_create_logical_replication_slot('`+orphan+`', 'pgoutput')`)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	dir := t.TempDir()
	store, err := NewLocalStore(dir)
	if err != nil {
		t.Fatalf("NewLocalStore: %v", err)
	}

	// 1. FRESH backup: must complete, must NOT sweep the orphan (no
	//    prior manifest → no proof of a crashed run → hands off), and
	//    must not leave its own anchor behind (protocol-TEMPORARY,
	//    released on graceful close).
	if err := (&Backup{
		Source:    pgEng,
		SourceDSN: sourceDSN,
		Store:     store,
	}).Run(context.Background()); err != nil {
		t.Fatalf("fresh Backup.Run: %v", err)
	}
	// Fresh runs must not sweep, and the run's own anchor must
	// auto-release: exactly the orphan remains.
	waitForPGAnchorSlots(t, sourceDSN, []string{orphan}, 15*time.Second)

	// 2. Flip the manifest back to in-progress — the on-disk state a
	//    crashed run leaves — so the next run takes the resume path.
	manifest, err := readManifest(context.Background(), store)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	manifest.PartialState = ir.BackupStateInProgress
	if err := writeManifest(context.Background(), store, manifest); err != nil {
		t.Fatalf("writeManifest in-progress: %v", err)
	}

	// 3. RESUME run: completes AND sweeps the orphan, leaving the
	//    source with zero anchor slots — nothing left pinning WAL.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := (&Backup{
		Source:    pgEng,
		SourceDSN: sourceDSN,
		Store:     store,
	}).Run(ctx); err != nil {
		t.Fatalf("resume Backup.Run: %v", err)
	}
	// The resume must have swept the pre-fix orphan, and its own
	// anchor must auto-release: nothing is left pinning WAL (Bug 137).
	waitForPGAnchorSlots(t, sourceDSN, nil, 15*time.Second)
}
