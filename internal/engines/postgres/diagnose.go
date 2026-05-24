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

	// Row-level security state (task #52 sub-deliverable 1). Per-table
	// `relrowsecurity` / `relforcerowsecurity` plus the connected
	// role's `rolbypassrls` attribute. Lets an operator run
	// `sluice diagnose` and see whether the RLS preflight will refuse
	// without running a full migration first. Best-effort: any sub-
	// probe failure surfaces as a `*_reason` entry rather than
	// propagating.
	if rlsSection, err := r.probeRLSState(ctx); err != nil {
		state["rls_reason"] = err.Error()
	} else {
		state["rls"] = rlsSection
	}

	payload, err := json.Marshal(state)
	if err != nil {
		return snap, fmt.Errorf("postgres: marshal diagnose state: %w", err)
	}
	snap.EngineState = payload
	return snap, nil
}

// probeRLSState collects every base table's RLS-enable flags in the
// reader's bound schema plus the connected role's BYPASSRLS attribute.
// Surfaced under `state.rls` in the diagnose bundle so an operator
// can see exactly which tables would trigger the RLS preflight
// refusal without running a full migration. Catalog-only reads (no
// row data); pg_class + pg_namespace + pg_roles are all readable by
// every role.
func (r *SchemaReader) probeRLSState(ctx context.Context) (map[string]any, error) {
	out := map[string]any{}

	// Role attribute first — short-circuits the operator-facing
	// diagnosis (if the role has BYPASSRLS, no per-table state matters).
	bypass, role, err := probeCurrentRoleBypassesRLS(ctx, r.db)
	if err != nil {
		out["role_reason"] = err.Error()
	} else {
		out["role"] = role
		out["rolbypassrls"] = bypass
	}

	// Per-table relrowsecurity / relforcerowsecurity.
	const q = `
		SELECT cl.relname, cl.relrowsecurity, cl.relforcerowsecurity
		FROM   pg_class     cl
		JOIN   pg_namespace n ON n.oid = cl.relnamespace
		WHERE  n.nspname  = $1
		  AND  cl.relkind IN ('r', 'p')
		ORDER  BY cl.relname`
	rows, err := r.db.QueryContext(ctx, q, r.schema)
	if err != nil {
		out["tables_reason"] = err.Error()
		return out, nil
	}
	defer func() { _ = rows.Close() }()

	var tables []map[string]any
	for rows.Next() {
		var (
			name            string
			enabled, forced bool
		)
		if err := rows.Scan(&name, &enabled, &forced); err != nil {
			out["tables_reason"] = err.Error()
			return out, nil
		}
		// Only surface tables that actually carry RLS state — the
		// common case (RLS off everywhere) keeps the bundle compact.
		if !enabled && !forced {
			continue
		}
		tables = append(tables, map[string]any{
			"table":               name,
			"relrowsecurity":      enabled,
			"relforcerowsecurity": forced,
		})
	}
	if err := rows.Err(); err != nil {
		out["tables_reason"] = err.Error()
		return out, nil
	}
	out["tables_with_rls"] = tables
	return out, nil
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
