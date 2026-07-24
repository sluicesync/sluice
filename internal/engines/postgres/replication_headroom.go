// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Postgres-side implementation of the orchestrator's replication-
// headroom preflight prober surface (roadmap item 68d; see
// `internal/pipeline/replication_headroom_preflight.go` for the
// operator-facing rationale).
//
// A slot-creating cold start against a source whose
// max_replication_slots are all in use fails MID-cold-start with the
// raw `ERROR: all replication slots are in use` (53400) — after the
// schema read and preflight work, with no hint that `sluice slot list`
// / `sync decommission` can free leftovers from finished waves. The
// orchestrator preflight reads this census UPFRONT and refuses loudly
// (with the existing slots named) while the source is still untouched.
//
// The census is advisory-read-only: `pg_settings` (via
// current_setting) and row counts over `pg_replication_slots` /
// `pg_stat_replication` are readable by any role on stock PG, but a
// managed platform could restrict them — which is why the orchestrator
// treats a probe FAILURE as a WARN-and-continue degrade, never a new
// hard failure on a path that worked before. The refusal fires only on
// a SUCCESSFUL probe that proves the ceiling.

import (
	"context"
	"database/sql"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// SourceReplicationHeadroom reports the source's replication-resource
// ceilings versus current use: max_replication_slots against the
// existing slot rows (named, with their active flag, so the refusal
// can show what occupies the ceiling) and max_wal_senders against the
// attached sender processes.
//
// Implements the orchestrator's `replicationHeadroomProber` surface
// (roadmap item 68d), on [SchemaReader] only — the headroom gates
// SOURCE-side slot creation, and the schema reader is the source-side
// handle that's live when the orchestrator probes before cold-start
// (the [SchemaReader.SourceReplicationCapability] precedent).
func (r *SchemaReader) SourceReplicationHeadroom(ctx context.Context) (ir.ReplicationHeadroom, error) {
	return probeReplicationHeadroom(ctx, r.db)
}

// probeReplicationHeadroom reads the ceilings and the slot inventory in
// one round trip each. Physical slots and other consumers' logical
// slots are counted too — the server's ceiling doesn't discriminate,
// so neither does the census.
func probeReplicationHeadroom(ctx context.Context, db *sql.DB) (ir.ReplicationHeadroom, error) {
	var h ir.ReplicationHeadroom
	const ceilings = `
SELECT current_setting('max_replication_slots')::int,
       current_setting('max_wal_senders')::int,
       (SELECT count(*) FROM pg_replication_slots),
       (SELECT count(*) FROM pg_stat_replication)`
	if err := db.QueryRowContext(ctx, ceilings).Scan(
		&h.MaxReplicationSlots, &h.MaxWALSenders, &h.SlotsInUse, &h.ActiveWALSenders,
	); err != nil {
		return ir.ReplicationHeadroom{}, fmt.Errorf("postgres: probe replication headroom: %w", err)
	}

	const slots = `SELECT slot_name, COALESCE(active, false) FROM pg_replication_slots ORDER BY slot_name`
	rows, err := db.QueryContext(ctx, slots)
	if err != nil {
		return ir.ReplicationHeadroom{}, fmt.Errorf("postgres: probe replication headroom: list slots: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var s ir.SlotInfo
		if err := rows.Scan(&s.Name, &s.Active); err != nil {
			return ir.ReplicationHeadroom{}, fmt.Errorf("postgres: probe replication headroom: scan slot: %w", err)
		}
		h.Slots = append(h.Slots, s)
	}
	if err := rows.Err(); err != nil {
		return ir.ReplicationHeadroom{}, fmt.Errorf("postgres: probe replication headroom: list slots: %w", err)
	}
	return h, nil
}
