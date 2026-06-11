// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// The full-backup orchestrator discovers the sweep via this optional
// surface on resume; keep the implementation pinned to the interface.
var _ ir.BackupAnchorSweeper = Engine{}

// backupAnchorOrphanMinAge is the safety margin the resume-time sweep
// applies before dropping a persistent backup-anchor slot: only
// anchors whose embedded creation timestamp is at least this old are
// swept. The margin exists for one reason — a pre-fix binary's anchor
// is persistent AND inactive even while its backup is still RUNNING
// (nothing ever streams from an anchor slot), so inactivity alone
// cannot distinguish "orphan" from "concurrent old-binary run".
// An hour comfortably exceeds the window in which a just-started
// concurrent run's anchor could be mistaken for debris, while still
// catching every realistically-leaked slot (a crashed run's anchor is
// only seen here after the operator restarts the backup, and anything
// it leaked keeps aging). Younger suspects are WARN-named but left
// alone so the operator can act manually if no other backup is
// running.
const backupAnchorOrphanMinAge = time.Hour

// SweepOrphanedBackupAnchors implements [ir.BackupAnchorSweeper]: it
// drops persistent `sluice_backup_anchor_<unixnano>` replication
// slots that a backup crashed under a pre-Bug-137 binary left on the
// source. New binaries create the anchor protocol-TEMPORARY (the
// server reclaims it on process death), so anything persistent,
// inactive, and older than the safety margin is debris — each one
// silently pins WAL at its restart_lsn until swept.
//
// Conservative by construction; a slot is dropped only when ALL of:
//
//   - its name is the anchor prefix + an all-digits Unix-nanosecond
//     timestamp (the exact shape OpenBackupSnapshot generates; any
//     other suffix is not provably ours and is left alone),
//   - it is inactive and non-temporary (a temporary anchor from a
//     concurrent NEW-binary run registers as active for its whole
//     session lifetime, so both filters exclude it),
//   - it was created on THIS database (pg_drop_replication_slot can
//     only drop logical slots from the database that owns them;
//     anchors for sibling databases are that database's resume to
//     sweep),
//   - its embedded timestamp is at least backupAnchorOrphanMinAge old.
//
// Every drop — and every suspected-but-too-young orphan deliberately
// left in place — is WARN-logged by name, per the contain-PG-
// complexity tenet: slot lifecycle is surfaced explicitly, never
// silently auto-handled.
func (e Engine) SweepOrphanedBackupAnchors(ctx context.Context, dsn string) error {
	cfg, err := e.parseDSN(dsn)
	if err != nil {
		return err
	}
	db, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return sweepOrphanedBackupAnchors(ctx, db, time.Now())
}

// sweepOrphanedBackupAnchors is the db-handle core of
// [Engine.SweepOrphanedBackupAnchors]; now is injected for the age
// arithmetic. Per-slot drop failures are logged and skipped (the
// sweep is best-effort hygiene); only the candidate listing itself
// can fail the call.
func sweepOrphanedBackupAnchors(ctx context.Context, db *sql.DB, now time.Time) error {
	// LIKE-escape the prefix's underscores so the pattern stays
	// literal (an unescaped `_` matches any character).
	pattern := strings.ReplaceAll(backupSnapshotSlotPrefix, "_", `\_`) + "%"
	rows, err := db.QueryContext(ctx, `
		SELECT slot_name
		  FROM pg_replication_slots
		 WHERE slot_name LIKE $1 ESCAPE '\'
		   AND NOT active
		   AND NOT temporary
		   AND database = current_database()
		 ORDER BY slot_name`, pattern)
	if err != nil {
		return fmt.Errorf("postgres: sweep backup anchors: list slots: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var candidates []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("postgres: sweep backup anchors: scan slot: %w", err)
		}
		candidates = append(candidates, name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: sweep backup anchors: iterate slots: %w", err)
	}

	for _, name := range candidates {
		createdAt, ok := backupAnchorTimestamp(name)
		if !ok {
			// Prefix matches but the suffix isn't our timestamp shape —
			// not provably a sluice anchor, so never touch it.
			slog.DebugContext(
				ctx, "postgres: backup anchor sweep: prefix-matching slot without a timestamp suffix; leaving it alone",
				slog.String("slot", name),
			)
			continue
		}
		if age := now.Sub(createdAt); age < backupAnchorOrphanMinAge {
			// Possibly a concurrent pre-fix-binary backup's live anchor
			// (those are persistent + inactive even mid-run). Name it
			// loudly so the operator can drop it manually if nothing
			// else is running, but don't risk WARN-noise in a live run.
			slog.WarnContext(
				ctx, "postgres: backup anchor sweep: suspected orphaned anchor slot is younger than the safety margin; NOT swept — if no other backup is running against this source, drop it manually",
				slog.String("slot", name),
				slog.Duration("age", age),
				slog.Duration("min_age", backupAnchorOrphanMinAge),
				slog.String("manual_drop", fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", name)),
			)
			continue
		}
		if _, err := db.ExecContext(ctx, "SELECT pg_drop_replication_slot($1)", name); err != nil {
			if isSlotAlreadyGoneErr(err) {
				continue // raced with manual cleanup — already gone is success
			}
			slog.WarnContext(
				ctx, "postgres: backup anchor sweep: drop failed; the slot keeps retaining WAL until cleaned up manually",
				slog.String("slot", name),
				slog.String("err", err.Error()),
			)
			continue
		}
		slog.WarnContext(
			ctx, "postgres: backup anchor sweep: dropped orphaned anchor slot leaked by a backup crashed under a pre-Bug-137-fix binary; it was silently retaining WAL on the source",
			slog.String("slot", name),
			slog.Time("created_at", createdAt),
		)
	}
	return nil
}

// backupAnchorTimestamp extracts the Unix-nanosecond creation
// timestamp OpenBackupSnapshot embeds in a default-shape anchor-slot
// name. ok is false when the name doesn't carry the prefix or the
// suffix isn't a plain non-negative integer — callers must treat such
// slots as not-ours.
func backupAnchorTimestamp(slotName string) (createdAt time.Time, ok bool) {
	suffix, found := strings.CutPrefix(slotName, backupSnapshotSlotPrefix)
	if !found || suffix == "" {
		return time.Time{}, false
	}
	ns, err := strconv.ParseInt(suffix, 10, 64)
	if err != nil || ns < 0 {
		return time.Time{}, false
	}
	return time.Unix(0, ns), true
}
