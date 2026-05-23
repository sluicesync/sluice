// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/orware/sluice/internal/ir"
)

// ADR-0056 diagnose surface for the Postgres engine.
//
// The applier implements [ir.SchemaHistoryReader] (cdc_schema_history
// row enumeration); the schema reader implements [ir.DiagnoseProber]
// (slot state + version metadata). Splitting the two interfaces
// across the two engine surfaces matches the existing pattern (the
// applier reads target-side state; the schema reader probes source-
// side state).

// ListSchemaHistory implements [ir.SchemaHistoryReader]. Returns the
// most-recent limit rows from sluice_cdc_schema_history scoped to
// streamID, ordered by created_at DESC. Tolerant of the table being
// absent (returns an empty slice) so a diagnose-bundle assembled
// against a target that pre-dates ADR-0049 still produces a useful
// bundle.
func (a *ChangeApplier) ListSchemaHistory(ctx context.Context, streamID string, limit int) ([]ir.RetainedSchemaVersionRow, error) {
	if limit <= 0 {
		limit = 100
	}
	tableRef := quoteIdent(a.controlSchema) + "." + quoteIdent(schemaHistoryTableName)
	// LIMIT $2 keeps the result bounded; ORDER BY created_at DESC
	// picks the most-recent rows the operator's bundle recipient is
	// most likely to need.
	q := "SELECT version_key, stream_id, schema_name, table_name, anchor_position, ir_schema_json " +
		"FROM " + tableRef + " " +
		"WHERE stream_id = $1 " +
		"ORDER BY created_at DESC " +
		"LIMIT $2"
	rows, err := a.db.QueryContext(ctx, q, streamID, limit)
	if err != nil {
		// 42P01 undefined_table — the per-target schema_history table
		// hasn't been created yet (the stream pre-dates ADR-0049, or
		// no boundary has been observed). Treat as "no rows" rather
		// than failing the diagnose bundle.
		if isUndefinedTableErr(err) {
			return []ir.RetainedSchemaVersionRow{}, nil
		}
		return nil, fmt.Errorf("postgres: list schema history: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []ir.RetainedSchemaVersionRow{}
	for rows.Next() {
		var r ir.RetainedSchemaVersionRow
		var payload string
		if err := rows.Scan(&r.VersionKey, &r.StreamID, &r.SchemaName, &r.TableName, &r.AnchorPosition, &payload); err != nil {
			return nil, fmt.Errorf("postgres: scan schema history row: %w", err)
		}
		r.TableJSON = []byte(payload)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list schema history: %w", err)
	}
	return out, nil
}

// isUndefinedTableErr reports whether err is a pgconn.PgError with
// SQLSTATE 42P01 (undefined_table) — the "the table doesn't exist
// yet" case that diagnose handles silently rather than failing.
func isUndefinedTableErr(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "42P01"
}

// DiagnoseBundle implements [ir.DiagnoseProber] for the Postgres
// schema reader. Surfaces:
//
//   - SELECT version() — server version string.
//   - DSN host:port + database name (the schema reader carries the
//     DSN; we render the redacted shape into the snapshot).
//   - pg_replication_slots row for the active slot — slot_name,
//     plugin, slot_type, database, active, restart_lsn,
//     confirmed_flush_lsn, wal_status. The streamID is used to pick
//     the slot when the schema reader doesn't have a slot context
//     (the schema reader scopes to the source DSN, not the stream;
//     we surface ALL sluice-named slots — those with names matching
//     `sluice_*` — and let the bundle recipient correlate).
//
// Errors at any sub-section don't fail the whole probe — they're
// embedded in the returned EngineState JSON as section-level reason
// strings. The orchestrator surfaces the snapshot verbatim.
func (r *SchemaReader) DiagnoseBundle(ctx context.Context, streamID string) (ir.DiagnoseSnapshot, error) {
	snap := ir.DiagnoseSnapshot{
		EngineName:   "postgres",
		Capabilities: capabilities,
	}

	// Server version. Errors here surface as an empty EngineVersion;
	// the rest of the probe can still produce useful state.
	var v string
	if err := r.db.QueryRowContext(ctx, "SELECT version()").Scan(&v); err == nil {
		snap.EngineVersion = v
	}

	// Engine-state JSON blob — pg_replication_slots rows matching
	// sluice's naming convention. The slot enumeration is best-effort;
	// a permission-denied response (cloud PG with restricted catalog
	// access) surfaces as an empty slots list.
	state := map[string]any{}
	slots, err := r.probeSluiceSlots(ctx)
	if err != nil {
		state["slots_reason"] = err.Error()
	} else {
		state["slots"] = slots
	}
	// Current LSN for cross-referencing with each slot's
	// confirmed_flush_lsn. Skipped silently on error.
	var lsn string
	if err := r.db.QueryRowContext(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn); err == nil {
		state["current_wal_lsn"] = lsn
	}
	state["stream_id_scope"] = streamID

	payload, err := json.Marshal(state)
	if err != nil {
		return snap, fmt.Errorf("postgres: marshal diagnose state: %w", err)
	}
	snap.EngineState = payload
	return snap, nil
}

// probeSluiceSlots enumerates pg_replication_slots rows whose name
// starts with the sluice slot-name convention. The list is bounded
// (sluice doesn't create unbounded slots) so we don't apply a LIMIT.
func (r *SchemaReader) probeSluiceSlots(ctx context.Context) ([]map[string]any, error) {
	const q = `
		SELECT slot_name, plugin, slot_type, database, active,
		       restart_lsn::text, confirmed_flush_lsn::text,
		       COALESCE(wal_status, 'unknown')
		FROM pg_replication_slots
		WHERE slot_name LIKE 'sluice%'`
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := []map[string]any{}
	for rows.Next() {
		var (
			name, plugin, kind, database, walStatus string
			active                                  bool
			restartLSN, confirmedLSN                sql.NullString
		)
		if err := rows.Scan(&name, &plugin, &kind, &database, &active, &restartLSN, &confirmedLSN, &walStatus); err != nil {
			return nil, err
		}
		entry := map[string]any{
			"slot_name":  name,
			"plugin":     plugin,
			"slot_type":  kind,
			"database":   database,
			"active":     active,
			"wal_status": walStatus,
		}
		if restartLSN.Valid {
			entry["restart_lsn"] = restartLSN.String
		}
		if confirmedLSN.Valid {
			entry["confirmed_flush_lsn"] = confirmedLSN.String
		}
		out = append(out, entry)
	}
	return out, rows.Err()
}
