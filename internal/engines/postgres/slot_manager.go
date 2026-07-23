// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// SlotManager exposes operator-facing logical-replication slot
// management to the `sluice slot` CLI. It implements
// [ir.SlotManager].
//
// The reachable surface is intentionally tiny: list slots and drop
// one. Slot creation is the CDC reader's responsibility and happens
// implicitly on cold-start, so the operator never has to think about
// it explicitly. Drop, on the other hand, is the recovery path for
// invalidated slots (wal_status = lost) and the cleanup path for
// abandoned slots from previous failed runs.
type SlotManager struct {
	db *sql.DB
}

// Close releases the underlying connection pool.
func (m *SlotManager) Close() error {
	if m.db == nil {
		return nil
	}
	return m.db.Close()
}

// errSlotNotFound is the sentinel returned by Drop when the named
// slot doesn't exist. The CLI's `--if-exists` mode branches on
// errors.Is(err, errSlotNotFound) to swallow that case quietly.
var errSlotNotFound = errors.New("postgres: slot not found")

// List returns every replication slot visible via pg_replication_slots
// to the connecting role. Sorted by name for stable CLI output.
//
// All columns the CLI's "slot list" output needs come from a single
// query — no per-slot follow-up. wal_status is COALESCEd to "" on
// older PG versions that don't expose the column (PG < 13). Sluice's
// declared baseline is PG 14+, so this is defensive.
func (m *SlotManager) List(ctx context.Context) ([]ir.SlotInfo, error) {
	const q = `
		SELECT
			slot_name,
			COALESCE(plugin, ''),
			active,
			COALESCE(wal_status, ''),
			COALESCE(restart_lsn::text, ''),
			COALESCE(confirmed_flush_lsn::text, '')
		FROM   pg_replication_slots
		ORDER  BY slot_name`
	rows, err := m.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("postgres: list slots: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]ir.SlotInfo, 0, 4)
	for rows.Next() {
		var s ir.SlotInfo
		if err := rows.Scan(&s.Name, &s.Plugin, &s.Active, &s.WALStatus, &s.RestartLSN, &s.ConfirmedFlushLSN); err != nil {
			return nil, fmt.Errorf("postgres: scan slot row: %w", err)
		}
		// Label known platform-internal slots (Neon wal_proposer_slot,
		// Aiven-lineage pghoard_local) so the enumeration doesn't read
		// them as leaked consumers. See platform_slots.go.
		if note, ok := platformInternalSlotNote(s.Name); ok {
			s.PlatformNote = note
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list slots: %w", err)
	}
	return out, nil
}

// Drop removes the named slot via pg_drop_replication_slot. Returns
// errSlotNotFound (wrapped with context) when no row matches the
// name; the CLI's `--if-exists` mode branches on errors.Is.
//
// Drop refuses to remove an active slot (active=true in
// pg_replication_slots) unless force is set — yanking a slot out
// from under a connected consumer is the kind of mistake that's
// hard to undo (consumer crashes, data possibly missed once
// resumed).
//
// pg_drop_replication_slot is the SQL function form; the
// replication-protocol DROP_REPLICATION_SLOT command is also
// available but requires a replication-mode connection. The SQL
// function is friendlier to call from a regular *sql.DB pool.
func (m *SlotManager) Drop(ctx context.Context, name string, force bool) error {
	if name == "" {
		return errors.New("postgres: drop slot: name is empty")
	}

	// Refuse known platform-internal slots without --force, before any
	// DB round-trip: dropping one breaks the PROVIDER's machinery (its
	// backup daemon, its consensus layer), not a sluice consumer — the
	// exact opposite of the abandoned-slot cleanup this command exists
	// for. See platform_slots.go.
	if note, ok := platformInternalSlotNote(name); ok && !force {
		return fmt.Errorf(
			"postgres: drop slot %q: this slot is platform-internal (%s) — dropping it breaks the provider's own machinery, not a sluice consumer; pass --force only if the provider's support told you to remove it",
			name, note,
		)
	}

	info, err := slotInfo(ctx, m.db, name)
	if err != nil {
		return err
	}
	if info == nil {
		return fmt.Errorf("%w: %q", errSlotNotFound, name)
	}

	// Check active flag separately (slotInfo only carries the bits
	// the CDC reader uses for cold-start; active is a CLI concern).
	var active bool
	const activeQ = `SELECT active FROM pg_replication_slots WHERE slot_name = $1`
	if err := m.db.QueryRowContext(ctx, activeQ, name).Scan(&active); err != nil {
		return fmt.Errorf("postgres: drop slot: read active flag: %w", err)
	}
	if active && !force {
		return fmt.Errorf(
			"postgres: drop slot %q: slot is active (a CDC consumer is currently connected); pass --force to drop anyway. The connected consumer will fail with a clear error and can be restarted",
			name,
		)
	}

	const q = `SELECT pg_drop_replication_slot($1)`
	if _, err := m.db.ExecContext(ctx, q, name); err != nil {
		return fmt.Errorf("postgres: drop slot %q: %w", name, err)
	}
	return nil
}

// DropStreamPublication implements [ir.StreamPublicationDropper] for
// `sluice sync decommission`: drop a finished stream's RECORDED
// per-stream publication alongside its slot.
//
// The guard semantics are [dropOwnPublicationIfPerStream]'s, reused
// verbatim for the drop itself: an empty name and the shared default
// (`sluice_pub`) are NEVER dropped — every legacy deployment reads
// through the default, and it may serve other streams. The existence
// probe on top exists so the caller's report (and `--dry-run`) can
// distinguish "dropped" from "already absent" — an idempotent re-run
// after a partial failure should say what it found, not just succeed.
func (m *SlotManager) DropStreamPublication(ctx context.Context, name string, dryRun bool) (ir.PublicationDropOutcome, error) {
	if name == "" || name == defaultPublication {
		return ir.PublicationDropSkippedShared, nil
	}
	var exists bool
	const q = `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`
	if err := m.db.QueryRowContext(ctx, q, name).Scan(&exists); err != nil {
		return ir.PublicationDropSkippedShared, fmt.Errorf("postgres: check publication %q: %w", name, err)
	}
	if !exists {
		return ir.PublicationDropAlreadyAbsent, nil
	}
	if dryRun {
		return ir.PublicationDropDropped, nil
	}
	if err := dropOwnPublicationIfPerStream(ctx, m.db, name); err != nil {
		return ir.PublicationDropSkippedShared, err
	}
	return ir.PublicationDropDropped, nil
}
