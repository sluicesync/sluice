// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import "database/sql"

// Engine-neutral SQL marshalling helpers shared by every
// [TargetMetricsHistoryStore] implementation (MySQL + Postgres). They
// live in core ir — NOT in an engine package — so the *Known⇄NULL honesty
// contract has a SINGLE source of truth: an unobserved metric is written
// as NULL and reconstructed as Known=(value IS NOT NULL), identically
// across engines (the Bug-74 "one codec, every family" discipline applied
// to the metrics-history row marshalling). database/sql is stdlib, not an
// engine import, so this keeps ir engine-neutral.

// MetricNullFloat64 returns a sql.NullFloat64 that is valid only when the
// companion *Known flag is true — so an unobserved float persists as NULL,
// never a misleading 0.
func MetricNullFloat64(v float64, known bool) sql.NullFloat64 {
	return sql.NullFloat64{Float64: v, Valid: known}
}

// MetricNullInt64 is the int64 (bigint column) analogue of
// [MetricNullFloat64].
func MetricNullInt64(v int64, known bool) sql.NullInt64 {
	return sql.NullInt64{Int64: v, Valid: known}
}

// MetricNullInt32 is the int (int column) analogue: a connection count
// persists as NULL unless ConnKnown is true.
func MetricNullInt32(v int, known bool) sql.NullInt32 {
	return sql.NullInt32{Int32: int32(v), Valid: known}
}

// ApplyMetricNullables reconstructs the *Known flags + values on r from
// the scanned sql.Null* columns, mapping NULL → Known=false (value left
// zero). StorageKnown is true only when ALL THREE storage columns are
// non-NULL (they are written and cleared as a unit), so a partial NULL
// state — which the recorder never produces — degrades to "unobserved"
// rather than reporting a fabricated partial reading.
func ApplyMetricNullables(
	r *TargetMetricsHistoryRow,
	cpu, mem, storageUtil sql.NullFloat64,
	storageAvailable, storageCapacity sql.NullInt64,
	lag sql.NullFloat64,
	activeConns, maxConns sql.NullInt32,
) {
	r.CPUUtil, r.CPUKnown = cpu.Float64, cpu.Valid
	r.MemUtil, r.MemKnown = mem.Float64, mem.Valid

	r.StorageUtil = storageUtil.Float64
	r.StorageAvailableBytes = storageAvailable.Int64
	r.StorageCapacityBytes = storageCapacity.Int64
	r.StorageKnown = storageUtil.Valid && storageAvailable.Valid && storageCapacity.Valid

	r.ReplicaLagSeconds, r.LagKnown = lag.Float64, lag.Valid

	r.ActiveConnections = int(activeConns.Int32)
	r.MaxConnections = int(maxConns.Int32)
	r.ConnKnown = activeConns.Valid && maxConns.Valid
}
