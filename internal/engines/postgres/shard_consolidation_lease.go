// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// ADR-0054 Shape A Phase 2 — ShardConsolidationLeaseStore engine impl.
//
// The pipeline's LeaseManager owns the state-machine + heartbeat
// goroutine; this file's methods on *ChangeApplier own the
// engine-specific SQL via the primitives in control_table.go. The
// pipeline probes for this surface via type-assertion on the
// applier; the canonical interface lives in `ir` so engines don't
// have to import pipeline (which would create a cycle).
//
// The translation between the pipeline-facing
// [ir.ShardConsolidationLeaseRow] and this engine's
// [shardConsolidationLeaseRow] is bespoke (sql.NullTime vs HasX bool
// flags) to keep the engine package's SQL implementation pure
// database/sql.

import (
	"context"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TryAcquireLease implements [ir.ShardConsolidationLeaseStore].
func (a *ChangeApplier) TryAcquireLease(
	ctx context.Context,
	tableName, streamID string,
	expires time.Time,
) (bool, ir.ShardConsolidationLeaseRow, error) {
	acquired, row, err := tryAcquireShardLease(ctx, a.db, a.controlSchema, tableName, streamID, expires)
	if err != nil {
		return false, ir.ShardConsolidationLeaseRow{}, err
	}
	return acquired, toIRLeaseRow(row), nil
}

// HeartbeatLease implements [ir.ShardConsolidationLeaseStore].
func (a *ChangeApplier) HeartbeatLease(
	ctx context.Context,
	tableName, streamID string,
	expires time.Time,
) (bool, error) {
	return heartbeatShardLease(ctx, a.db, a.controlSchema, tableName, streamID, expires)
}

// RecordDDLText implements [ir.ShardConsolidationLeaseStore].
func (a *ChangeApplier) RecordDDLText(
	ctx context.Context,
	tableName, streamID, ddlText string,
) (bool, error) {
	return recordShardLeaseDDLText(ctx, a.db, a.controlSchema, tableName, streamID, ddlText)
}

// FinalizeLeaseApply implements [ir.ShardConsolidationLeaseStore].
//
// anchor's Token and Engine are persisted alongside the rest of the
// applied-row payload so the v0.76.0 lease GC sweep (task #21) can
// compare against every stream's persisted position via the engine's
// [ir.PositionOrderer]. A zero-value Position stores NULL (legacy
// callers / unit-test fakes that don't supply a position).
func (a *ChangeApplier) FinalizeLeaseApply(
	ctx context.Context,
	tableName, streamID, ddlText, ddlChecksum string,
	appliedSchemaVersion int64,
	anchor ir.Position,
) (bool, error) {
	return finalizeShardLeaseApply(
		ctx, a.db, a.controlSchema,
		tableName, streamID, ddlText, ddlChecksum,
		appliedSchemaVersion,
		anchor.Token, anchor.Engine,
	)
}

// DeleteLease implements [ir.ShardConsolidationLeaseDeleter] — v0.76.0
// lease GC sweep (task #21). Tolerant of the row or the lease control
// table itself being absent (returns nil).
func (a *ChangeApplier) DeleteLease(ctx context.Context, tableName string) error {
	return deleteShardLease(ctx, a.db, a.controlSchema, tableName)
}

// ObserveLease implements [ir.ShardConsolidationLeaseStore].
func (a *ChangeApplier) ObserveLease(
	ctx context.Context,
	tableName string,
) (ir.ShardConsolidationLeaseRow, bool, error) {
	row, ok, err := selectShardLease(ctx, a.db, a.controlSchema, tableName)
	if err != nil {
		return ir.ShardConsolidationLeaseRow{}, false, err
	}
	if !ok {
		return ir.ShardConsolidationLeaseRow{}, false, nil
	}
	return toIRLeaseRow(row), true, nil
}

// ListLeases implements [ir.ShardConsolidationLeaseLister] — returns
// every row in the per-target lease control table for the
// `sluice sync status` ADR-0054 §6 operator-visibility surface.
func (a *ChangeApplier) ListLeases(ctx context.Context) ([]ir.ShardConsolidationLeaseRow, error) {
	rows, err := listShardLeases(ctx, a.db, a.controlSchema)
	if err != nil {
		return nil, err
	}
	out := make([]ir.ShardConsolidationLeaseRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, toIRLeaseRow(row))
	}
	return out, nil
}

// toIRLeaseRow converts the engine's sql.NullTime-bearing row shape
// to the cross-package HasX-bool shape.
func toIRLeaseRow(row shardConsolidationLeaseRow) ir.ShardConsolidationLeaseRow {
	out := ir.ShardConsolidationLeaseRow{
		TargetTableFullName:  row.TargetTableFullName,
		LeaseHolderStreamID:  row.LeaseHolderStreamID,
		DDLText:              row.DDLText,
		DDLChecksum:          row.DDLChecksum,
		AppliedSchemaVersion: row.AppliedSchemaVersion,
	}
	if row.LeaseExpiresAt.Valid {
		out.LeaseExpiresAt = row.LeaseExpiresAt.Time
		out.HasLeaseExpiresAt = true
	}
	if row.AppliedAt.Valid {
		out.AppliedAt = row.AppliedAt.Time
		out.HasAppliedAt = true
	}
	// Reconstruct the source-side anchor Position. Both Token + Engine
	// must be present for an anchor to count as "set" — a half-populated
	// row (legacy v0.75.0 + a manually-poked anchor_position with
	// source_engine still NULL) is treated as absent so the GC sweep
	// defensively retains it.
	if row.AnchorPosition.Valid && row.AnchorEngine.Valid {
		out.AnchorPosition = ir.Position{
			Engine: row.AnchorEngine.String,
			Token:  row.AnchorPosition.String,
		}
		out.HasAnchor = true
	}
	return out
}
