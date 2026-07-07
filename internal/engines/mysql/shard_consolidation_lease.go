// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// ADR-0054 Shape A Phase 2 — ShardConsolidationLeaseStore engine impl.
//
// The pipeline's LeaseManager owns the state-machine + heartbeat
// goroutine; this file's methods on *ChangeApplier own the
// engine-specific SQL via the primitives in control_table.go. The
// pipeline probes for this surface via type-assertion on the applier;
// the canonical interface lives in `ir` so engines don't have to
// import pipeline (which would create a cycle).
//
// MySQL has no schema-qualification — the lease table lives in the
// applier's connected database. The acquire path uses a SELECT ...
// FOR UPDATE inside a tx (MySQL has no INSERT ... ON CONFLICT WHERE
// like PG), serialising concurrent acquires.

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
	acquired, row, err := tryAcquireShardLease(ctx, a.db, a.controlKeyspace, tableName, streamID, expires)
	if err != nil {
		return false, ir.ShardConsolidationLeaseRow{}, err
	}
	return acquired, row.ToIR(), nil
}

// HeartbeatLease implements [ir.ShardConsolidationLeaseStore].
func (a *ChangeApplier) HeartbeatLease(
	ctx context.Context,
	tableName, streamID string,
	expires time.Time,
) (bool, error) {
	return heartbeatShardLease(ctx, a.db, a.controlKeyspace, tableName, streamID, expires)
}

// RecordDDLText implements [ir.ShardConsolidationLeaseStore].
func (a *ChangeApplier) RecordDDLText(
	ctx context.Context,
	tableName, streamID, ddlText string,
) (bool, error) {
	return recordShardLeaseDDLText(ctx, a.db, a.controlKeyspace, tableName, streamID, ddlText)
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
		ctx, a.db, a.controlKeyspace,
		tableName, streamID, ddlText, ddlChecksum,
		appliedSchemaVersion,
		anchor.Token, anchor.Engine,
	)
}

// DeleteLease implements [ir.ShardConsolidationLeaseDeleter] — v0.76.0
// lease GC sweep (task #21). Tolerant of the row or the lease control
// table itself being absent (returns nil).
func (a *ChangeApplier) DeleteLease(ctx context.Context, tableName string) error {
	return deleteShardLease(ctx, a.db, a.controlKeyspace, tableName)
}

// ObserveLease implements [ir.ShardConsolidationLeaseStore].
func (a *ChangeApplier) ObserveLease(
	ctx context.Context,
	tableName string,
) (ir.ShardConsolidationLeaseRow, bool, error) {
	row, ok, err := selectShardLease(ctx, a.db, a.controlKeyspace, tableName)
	if err != nil {
		return ir.ShardConsolidationLeaseRow{}, false, err
	}
	if !ok {
		return ir.ShardConsolidationLeaseRow{}, false, nil
	}
	return row.ToIR(), true, nil
}

// ListLeases implements [ir.ShardConsolidationLeaseLister] — returns
// every row in the per-target lease control table for the `sluice
// sync status` ADR-0054 §6 surface.
func (a *ChangeApplier) ListLeases(ctx context.Context) ([]ir.ShardConsolidationLeaseRow, error) {
	rows, err := listShardLeases(ctx, a.db, a.controlKeyspace)
	if err != nil {
		return nil, err
	}
	out := make([]ir.ShardConsolidationLeaseRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.ToIR())
	}
	return out, nil
}
