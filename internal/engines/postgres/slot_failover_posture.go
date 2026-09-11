// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
)

// SourceSlotFailoverPosture reads the two GUCs that decide whether PG 17+
// native slot synchronization will carry sluice's slot across a failover,
// plus the server version that decides whether the mechanism exists at all.
//
// Both settings are plain `pg_settings` rows readable by any role — no
// superuser, no replication privilege, no platform API. Measured on
// PlanetScale Postgres 18.6 as the ordinary default role, 2026-09-10.
//
// This is a census, not a verdict. It cannot see Patroni permanent slots
// (PlanetScale's "Logical slot name" field), which preserve slots by a
// different mechanism that leaves no trace in pg_settings — so a cluster
// reading `off` on both may still be correctly configured. The caller owns
// saying so; see [ir.SlotFailoverPosture].
func (r *SchemaReader) SourceSlotFailoverPosture(ctx context.Context) (ir.SlotFailoverPosture, error) {
	version, err := serverVersionNum(ctx, r.db)
	if err != nil {
		return ir.SlotFailoverPosture{}, fmt.Errorf("postgres: slot-failover posture: server version: %w", err)
	}
	posture := ir.SlotFailoverPosture{ServerVersionNum: version}

	// One round trip for both settings. A GUC that does not exist on this
	// major version simply returns no row, leaving the field false — which
	// is the correct reading, since an absent GUC cannot be enabled.
	const q = `SELECT name, setting FROM pg_settings WHERE name IN ('sync_replication_slots', 'hot_standby_feedback')`
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return ir.SlotFailoverPosture{}, fmt.Errorf("postgres: slot-failover posture: read settings: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name, setting string
		if err := rows.Scan(&name, &setting); err != nil {
			return ir.SlotFailoverPosture{}, fmt.Errorf("postgres: slot-failover posture: scan: %w", err)
		}
		on := setting == "on"
		switch name {
		case "sync_replication_slots":
			posture.SyncReplicationSlots = on
		case "hot_standby_feedback":
			posture.HotStandbyFeedback = on
		}
	}
	if err := rows.Err(); err != nil {
		return ir.SlotFailoverPosture{}, fmt.Errorf("postgres: slot-failover posture: rows: %w", err)
	}
	return posture, nil
}
