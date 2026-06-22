// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0107 item 35 — target-metrics rolling history, Postgres store.
//
// Additive per-target control table (in the applier's controlSchema, the
// DSN-derived namespace — NOT the user-data schema, mirroring
// sluice_cdc_state) holding one row per polled PlanetScale target-health
// snapshot, so `sluice diagnose` (or a plain SELECT) surfaces the recent
// CPU/mem/storage/lag/conn TREND. Mirrors the sluice_cdc_schema_history
// discipline in schema_history.go exactly: schema-qualified tableRef,
// CREATE TABLE IF NOT EXISTS, run from the *sql.DB pool.
//
// The applier satisfies [ir.TargetMetricsHistoryStore] by these methods;
// nothing new is registered — the pipeline recorder sidecar type-asserts
// the interface on the opened ChangeApplier.
//
// HONESTY: an unobserved metric (the snapshot's *Known flag false) is
// stored as NULL, never 0 — and reconstructed as Known=(value IS NOT NULL)
// on read, so a recipient never mistakes "unobserved" for "idle".

const targetMetricsHistoryTableName = "sluice_target_metrics_history"

// ensureTargetMetricsHistoryTable creates the per-target
// sluice_target_metrics_history table in the named schema if it doesn't
// exist. Idempotent. ADDITIVE: never touches sluice_cdc_state /
// schema_history / user data.
//
// id is a BIGSERIAL surrogate PK (rows are append-only; the natural
// lookup key is (stream_id, sampled_at)). The utilisation / volume /
// connection columns are NULLABLE so the *Known=false "unobserved" case is
// stored as NULL, not a misleading 0. Index on (stream_id, sampled_at) for
// the ListTargetMetricsHistory ORDER BY sampled_at DESC LIMIT scan.
func ensureTargetMetricsHistoryTable(ctx context.Context, db *sql.DB, schema string) error {
	tableRef := quoteIdent(schema) + "." + quoteIdent(targetMetricsHistoryTableName)
	ddl := `
		CREATE TABLE IF NOT EXISTS ` + tableRef + ` (
			id                      BIGSERIAL    NOT NULL,
			stream_id               VARCHAR(255) NOT NULL,
			sampled_at              TIMESTAMP    NOT NULL,
			database_name           VARCHAR(255) NOT NULL DEFAULT '',
			branch                  VARCHAR(255) NOT NULL DEFAULT '',
			cpu_util                DOUBLE PRECISION NULL,
			mem_util                DOUBLE PRECISION NULL,
			storage_util            DOUBLE PRECISION NULL,
			storage_available_bytes BIGINT       NULL,
			storage_capacity_bytes  BIGINT       NULL,
			replica_lag_seconds     DOUBLE PRECISION NULL,
			active_connections      INTEGER      NULL,
			max_connections         INTEGER      NULL,
			PRIMARY KEY (id)
		)`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("postgres: ensure target-metrics-history table: %w", err)
	}
	idx := "CREATE INDEX IF NOT EXISTS idx_" + targetMetricsHistoryTableName + "_stream_sampled " +
		"ON " + tableRef + " (stream_id, sampled_at)"
	if _, err := db.ExecContext(ctx, idx); err != nil {
		return fmt.Errorf("postgres: ensure target-metrics-history index: %w", err)
	}
	return nil
}

// EnsureTargetMetricsHistory implements [ir.TargetMetricsHistoryStore].
func (a *ChangeApplier) EnsureTargetMetricsHistory(ctx context.Context) error {
	return ensureTargetMetricsHistoryTable(ctx, a.db, a.controlSchema)
}

// RecordTargetMetricsSample implements [ir.TargetMetricsHistoryStore]. It
// INSERTs one row; every *Known=false field is written as SQL NULL via the
// shared ir.MetricNull* helpers, so the "unobserved" state round-trips
// faithfully (read reconstructs Known from NULLness).
func (a *ChangeApplier) RecordTargetMetricsSample(ctx context.Context, s ir.TargetMetricsSample) error {
	tableRef := quoteIdent(a.controlSchema) + "." + quoteIdent(targetMetricsHistoryTableName)
	q := "INSERT INTO " + tableRef + " " +
		"(stream_id, sampled_at, database_name, branch, " +
		"cpu_util, mem_util, storage_util, storage_available_bytes, storage_capacity_bytes, " +
		"replica_lag_seconds, active_connections, max_connections) " +
		"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)"
	_, err := a.db.ExecContext(
		ctx, q,
		s.StreamID, s.SampledAt.UTC(), s.Database, s.Branch,
		ir.MetricNullFloat64(s.CPUUtil, s.CPUKnown),
		ir.MetricNullFloat64(s.MemUtil, s.MemKnown),
		ir.MetricNullFloat64(s.StorageUtil, s.StorageKnown),
		ir.MetricNullInt64(s.StorageAvailableBytes, s.StorageKnown),
		ir.MetricNullInt64(s.StorageCapacityBytes, s.StorageKnown),
		ir.MetricNullFloat64(s.ReplicaLagSeconds, s.LagKnown),
		ir.MetricNullInt32(s.ActiveConnections, s.ConnKnown),
		ir.MetricNullInt32(s.MaxConnections, s.ConnKnown),
	)
	if err != nil {
		return fmt.Errorf("postgres: record target-metrics sample: %w", err)
	}
	return nil
}

// PruneTargetMetricsHistory implements [ir.TargetMetricsHistoryStore]:
// DELETE every row older than now-retain, keeping the table bounded.
func (a *ChangeApplier) PruneTargetMetricsHistory(ctx context.Context, retain time.Duration) error {
	if retain <= 0 {
		return nil
	}
	tableRef := quoteIdent(a.controlSchema) + "." + quoteIdent(targetMetricsHistoryTableName)
	cutoff := time.Now().UTC().Add(-retain)
	q := "DELETE FROM " + tableRef + " WHERE sampled_at < $1"
	if _, err := a.db.ExecContext(ctx, q, cutoff); err != nil {
		// Tolerate the table not existing yet (record never ran).
		if isUndefinedTableErr(err) {
			return nil
		}
		return fmt.Errorf("postgres: prune target-metrics history: %w", err)
	}
	return nil
}

// ListTargetMetricsHistory implements [ir.TargetMetricsHistoryStore]:
// the most-recent limit rows for streamID, sampled_at DESC, with each
// *Known flag reconstructed from the column's NULLness. Tolerant of the
// table being absent (returns an empty slice).
func (a *ChangeApplier) ListTargetMetricsHistory(ctx context.Context, streamID string, limit int) ([]ir.TargetMetricsHistoryRow, error) {
	if limit <= 0 {
		limit = 100
	}
	tableRef := quoteIdent(a.controlSchema) + "." + quoteIdent(targetMetricsHistoryTableName)
	q := "SELECT sampled_at, database_name, branch, " +
		"cpu_util, mem_util, storage_util, storage_available_bytes, storage_capacity_bytes, " +
		"replica_lag_seconds, active_connections, max_connections " +
		"FROM " + tableRef + " " +
		"WHERE stream_id = $1 " +
		"ORDER BY sampled_at DESC " +
		"LIMIT $2"
	rows, err := a.db.QueryContext(ctx, q, streamID, limit)
	if err != nil {
		if isUndefinedTableErr(err) {
			return []ir.TargetMetricsHistoryRow{}, nil
		}
		return nil, fmt.Errorf("postgres: list target-metrics history: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []ir.TargetMetricsHistoryRow{}
	for rows.Next() {
		var (
			r        ir.TargetMetricsHistoryRow
			cpu      sql.NullFloat64
			mem      sql.NullFloat64
			storUtil sql.NullFloat64
			storAvl  sql.NullInt64
			storCap  sql.NullInt64
			lag      sql.NullFloat64
			actConn  sql.NullInt32
			maxConn  sql.NullInt32
		)
		if err := rows.Scan(&r.SampledAt, &r.Database, &r.Branch,
			&cpu, &mem, &storUtil, &storAvl, &storCap, &lag, &actConn, &maxConn); err != nil {
			return nil, fmt.Errorf("postgres: scan target-metrics row: %w", err)
		}
		r.StreamID = streamID
		ir.ApplyMetricNullables(&r, cpu, mem, storUtil, storAvl, storCap, lag, actConn, maxConn)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list target-metrics history: %w", err)
	}
	return out, nil
}
