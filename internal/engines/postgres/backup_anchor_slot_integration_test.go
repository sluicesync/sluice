//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Bug 137 integration pins, both fix shapes against live Postgres:
//
//  1. The default-shape backup anchor slot is protocol-TEMPORARY —
//     the SERVER drops it when the creating replication connection
//     dies, which is what makes a SIGKILLed `backup full` leak-free
//     (no sluice code runs on SIGKILL; only a server-side guarantee
//     helps). Simulated here via pg_terminate_backend on the slot's
//     owning walsender, the closest in-process stand-in for a hard
//     process kill.
//  2. The resume-time orphan sweep drops persistent anchors leaked
//     by pre-fix binaries — and ONLY those: young suspects, active/
//     temporary anchors, and prefix-lookalikes all survive.
//
// Per the Bug-74 "pin the class" lesson, shape 1 is pinned on BOTH
// slot-creation families: the pglogrepl path (shared PG 16 container)
// and the raw-protocol path (postgres:17 — where TEMPORARY must be
// spliced before LOGICAL and FAILOVER must be omitted; a malformed
// combination fails loudly at create time, which is exactly what the
// test would catch).

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// anchorSlotRow is the pg_replication_slots projection the pins
// assert on.
type anchorSlotRow struct {
	name      string
	temporary bool
	active    bool
	activePID int
}

// findAnchorSlots lists every slot carrying the backup-anchor prefix.
func findAnchorSlots(t *testing.T, db *sql.DB) []anchorSlotRow {
	t.Helper()
	rows, err := db.Query(
		`SELECT slot_name, temporary, active, COALESCE(active_pid, 0)
		   FROM pg_replication_slots
		  WHERE slot_name LIKE 'sluice\_backup\_anchor\_%' ESCAPE '\'
		  ORDER BY slot_name`,
	)
	if err != nil {
		t.Fatalf("list anchor slots: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var out []anchorSlotRow
	for rows.Next() {
		var r anchorSlotRow
		if err := rows.Scan(&r.name, &r.temporary, &r.active, &r.activePID); err != nil {
			t.Fatalf("scan anchor slot: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate anchor slots: %v", err)
	}
	return out
}

// waitForAnchorSlotsGone polls until no backup-anchor slot remains.
func waitForAnchorSlotsGone(t *testing.T, db *sql.DB, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		slots := findAnchorSlots(t, db)
		if len(slots) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("anchor slot(s) still present after %s: %+v", timeout, slots)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// assertTemporaryAnchorAutoDropsOnKill drives the shape-1 pin against
// the given DSN: open a default-shape backup snapshot, verify the
// anchor slot registered protocol-temporary (and session-owned, i.e.
// active — the property the sweep's exclusion filters rely on), then
// kill the owning walsender backend WITHOUT calling Close and verify
// the server reclaims the slot on its own.
func assertTemporaryAnchorAutoDropsOnKill(t *testing.T, dsn string) {
	t.Helper()
	eng := Engine{}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	snap, err := eng.OpenBackupSnapshot(ctx, dsn, irbackup.SnapshotOptions{})
	if err != nil {
		t.Fatalf("OpenBackupSnapshot: %v", err)
	}
	// Close is still called at the end (ignoring its error — we kill
	// the replication backend under it on purpose) so the snapshot tx
	// and pools don't dangle for the rest of the package run.
	defer func() { _ = snap.Close() }()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	slots := findAnchorSlots(t, db)
	if len(slots) != 1 {
		t.Fatalf("anchor slots = %+v; want exactly 1", slots)
	}
	anchor := slots[0]
	// THE Bug 137 assertion: a pre-fix binary creates this slot with
	// temporary = f, which is precisely the WAL-pinning leak shape.
	if !anchor.temporary {
		t.Fatalf("anchor slot %q has temporary = false; a hard-killed backup would leak it (Bug 137)", anchor.name)
	}
	// Temporary slots stay owned by their creating session for their
	// whole life — the property that makes a concurrent NEW-binary
	// run's anchor invisible to the orphan sweep's NOT-active filter.
	if !anchor.active || anchor.activePID == 0 {
		t.Fatalf("anchor slot %q is not session-owned (active=%v pid=%d); want active with an owning walsender", anchor.name, anchor.active, anchor.activePID)
	}

	// Simulate SIGKILL: terminate the owning walsender backend. The
	// snapshot's CloseFn never runs server-side cleanup for this slot;
	// the server must reclaim it alone.
	if _, err := db.Exec(`SELECT pg_terminate_backend($1)`, anchor.activePID); err != nil {
		t.Fatalf("pg_terminate_backend(%d): %v", anchor.activePID, err)
	}
	waitForAnchorSlotsGone(t, db, 15*time.Second)
}

// TestBackupSnapshot_TemporaryAnchorAutoDropsOnKill_PG16 pins the
// pglogrepl creation family (shared PG 16 container; under the
// SLUICE_TEST_PG_IMAGE matrix this may exercise the raw path instead
// — the assertions hold on every version either way).
func TestBackupSnapshot_TemporaryAnchorAutoDropsOnKill_PG16(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()
	assertTemporaryAnchorAutoDropsOnKill(t, dsn)
}

// TestBackupSnapshot_TemporaryAnchorAutoDropsOnKill_PG17 pins the
// raw-protocol creation family on a deliberate postgres:17 boot —
// TEMPORARY spliced before LOGICAL, FAILOVER omitted (the server
// refuses TEMPORARY+FAILOVER, so mis-building the command fails the
// OpenBackupSnapshot call loudly right here).
func TestBackupSnapshot_TemporaryAnchorAutoDropsOnKill_PG17(t *testing.T) {
	dsn, cleanup := startPostgres17ForCDC(t)
	defer cleanup()
	assertTemporaryAnchorAutoDropsOnKill(t, dsn)
}

// TestBackupSnapshot_TemporaryAnchorDroppedOnGracefulClose pins the
// happy path: Close errors-free and the anchor slot is gone right
// after (released by the replication conn's close rather than an
// explicit SQL drop — temporary slots can't be dropped cross-session).
func TestBackupSnapshot_TemporaryAnchorDroppedOnGracefulClose(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()
	eng := Engine{}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	snap, err := eng.OpenBackupSnapshot(ctx, dsn, irbackup.SnapshotOptions{})
	if err != nil {
		t.Fatalf("OpenBackupSnapshot: %v", err)
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if slots := findAnchorSlots(t, db); len(slots) != 1 {
		t.Fatalf("anchor slots before Close = %+v; want exactly 1", slots)
	}

	if err := snap.Close(); err != nil {
		t.Fatalf("Close: %v (the temporary anchor must not produce a drop error)", err)
	}
	waitForAnchorSlotsGone(t, db, 15*time.Second)
}

// TestSweepOrphanedBackupAnchors_DropsOnlyProvenOrphans pins shape 2
// against live slots. Seeded matrix (the shared container allows 4
// slots, so the non-prefixed-name case stays unit-level — the SQL
// LIKE prefix filter and Go's CutPrefix are pinned by the bogus-suffix
// row here plus TestBackupAnchorTimestamp):
//
//   - ancient persistent anchor  → swept (the pre-fix leak shape)
//   - young persistent anchor    → kept (safety margin; could be a
//     concurrent pre-fix run still in flight)
//   - active TEMPORARY anchor    → kept (a concurrent NEW-binary run)
//   - bogus-suffix lookalike     → kept (not provably ours)
func TestSweepOrphanedBackupAnchors_DropsOnlyProvenOrphans(t *testing.T) {
	dsn, cleanup := startPostgresForCDC(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const ancient = "sluice_backup_anchor_123"
	young := fmt.Sprintf("sluice_backup_anchor_%d", time.Now().UnixNano())
	const lookalike = "sluice_backup_anchor_bogus"
	const liveTemp = "sluice_backup_anchor_456"

	for _, name := range []string{ancient, young, lookalike} {
		if _, err := db.ExecContext(ctx,
			`SELECT pg_create_logical_replication_slot($1, 'pgoutput')`, name); err != nil {
			t.Fatalf("create persistent slot %q: %v", name, err)
		}
	}
	// The temporary slot needs a session that outlives the sweep: pin
	// a dedicated conn and create the slot on it.
	holder, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("pin holder conn: %v", err)
	}
	defer func() { _ = holder.Close() }()
	if _, err := holder.ExecContext(ctx,
		`SELECT pg_create_logical_replication_slot($1, 'pgoutput', true)`, liveTemp); err != nil {
		t.Fatalf("create temporary slot %q: %v", liveTemp, err)
	}

	if err := (Engine{}).SweepOrphanedBackupAnchors(ctx, dsn); err != nil {
		t.Fatalf("SweepOrphanedBackupAnchors: %v", err)
	}

	got := map[string]bool{}
	for _, s := range findAnchorSlots(t, db) {
		got[s.name] = true
	}
	if got[ancient] {
		t.Errorf("ancient persistent anchor %q survived the sweep; want dropped (the Bug 137 leak shape)", ancient)
	}
	for _, want := range []string{young, lookalike, liveTemp} {
		if !got[want] {
			t.Errorf("slot %q was swept; the sweep must never touch it", want)
		}
	}
}
