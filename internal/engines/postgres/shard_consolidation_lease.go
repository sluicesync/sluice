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

	"github.com/orware/sluice/internal/ir"
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
func (a *ChangeApplier) FinalizeLeaseApply(
	ctx context.Context,
	tableName, streamID, ddlText, ddlChecksum string,
	appliedSchemaVersion int64,
) (bool, error) {
	return finalizeShardLeaseApply(ctx, a.db, a.controlSchema, tableName, streamID, ddlText, ddlChecksum, appliedSchemaVersion)
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
	return out
}
