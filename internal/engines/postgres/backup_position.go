// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// CaptureBackupPosition implements [irbackup.PositionCapturer]. Returns
// the source's current WAL position paired with the supplied slot name,
// encoded into the same JSON envelope shape the CDC reader emits via
// [encodePGPos]. Phase 3.3 of the logical-backup feature: the full
// backup orchestrator records this on [irbackup.Manifest.EndPosition] so a
// subsequent incremental can resume CDC from the LSN at end-of-backup.
//
// The recorded position semantically marks the source's cursor at the
// moment of capture (just before the full's manifest flips to
// `complete`). A Phase 3 incremental chained off this manifest opens
// CDC at this LSN; if the slot named slotName exists with a
// `restart_lsn` covering this LSN, CDC resumes via slot reuse — if not,
// `sluice sync start --position-from-manifest` runs the pre-flight
// checks defined in the design doc (slot existence + `wal_keep_size`
// sufficiency + Patroni detection).
//
// Empty slotName falls back to the engine's [defaultSlot] so the
// captured shape matches what the streamer would later look for. The
// slot need not exist at capture time — the CDC handoff path guards
// the slot lifecycle.
func (r *SchemaReader) CaptureBackupPosition(ctx context.Context, slotName string) (ir.Position, error) {
	if r.db == nil {
		return ir.Position{}, errors.New("postgres: CaptureBackupPosition: reader not opened")
	}
	// A PlanetScale Neki router has no WAL position to record: it does not
	// implement pg_current_wal_lsn() (NK013 — measured 2026-09-15, where a
	// `backup full` FROM a sharded Neki finished its copy and died here), and
	// nothing could consume one anyway — a router-level replication
	// connection is refused ("must target a specific shard", 0A000), which is
	// why a Neki cannot be a continuous-sync source (ADR-0186 probe R-1).
	// Decided from the flavor probe rather than by catching NK013, so the
	// answer does not depend on the router's error text; the orchestrator
	// records no EndPosition and says so.
	//
	// SIBLING SWEEP — other pg_current_wal_lsn() readers on this engine:
	//   - diagnose.go: guarded `if err == nil`, degrades in place — EXEMPT
	//   - engine.go ReadCurrentWALPosition (schema add-table), health_reporter.go,
	//     slot_health_reporter.go: reached only by a `sync` whose SOURCE is a
	//     Neki, which fails at the replication connect (0A000) either way —
	//     loud on both roads, NOT annotated here; filed
	if r.isNeki {
		return ir.Position{}, fmt.Errorf("postgres: CaptureBackupPosition: a PlanetScale Neki router implements "+
			"neither the current-WAL-LSN function nor a router-level replication connection, so there is no CDC "+
			"position to anchor an incremental on: %w", irbackup.ErrPositionUnavailable)
	}
	if slotName == "" {
		slotName = defaultSlot
	}
	var lsn string
	if err := r.db.QueryRowContext(ctx, `SELECT pg_catalog.pg_current_wal_lsn()::text`).Scan(&lsn); err != nil {
		return ir.Position{}, fmt.Errorf("postgres: CaptureBackupPosition: %w", err)
	}
	pos, err := encodePGPos(pgPos{Slot: slotName, LSN: lsn})
	if err != nil {
		return ir.Position{}, fmt.Errorf("postgres: CaptureBackupPosition: %w", err)
	}
	return pos, nil
}
