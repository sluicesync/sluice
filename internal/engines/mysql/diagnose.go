// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"

	"sluicesync.dev/sluice/internal/ir"
)

// ADR-0056 diagnose surface for the MySQL engine.
//
// Mirrors the Postgres pattern: applier implements
// [ir.SchemaHistoryReader] for target-side cdc_schema_history row
// enumeration; schema reader implements [ir.DiagnoseProber] for
// source-side binlog / GTID state. The MySQL engine also covers the
// PlanetScale flavour — capabilities() returns a flavour-specific
// value, so the snapshot's Capabilities field carries the operator's
// declared flavour.

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
	q := "SELECT version_key, stream_id, schema_name, table_name, anchor_position, ir_schema_json " +
		"FROM `" + schemaHistoryTableName + "` " +
		"WHERE stream_id = ? " +
		"ORDER BY created_at DESC " +
		"LIMIT ?"
	rows, err := a.db.QueryContext(ctx, q, streamID, limit)
	if err != nil {
		// MySQL error 1146 (ER_NO_SUCH_TABLE) — table doesn't exist
		// yet (stream pre-dates ADR-0049, or no boundary has been
		// observed). Treat as "no rows".
		if isNoSuchTableErr(err) {
			return []ir.RetainedSchemaVersionRow{}, nil
		}
		return nil, fmt.Errorf("mysql: list schema history: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []ir.RetainedSchemaVersionRow{}
	for rows.Next() {
		var r ir.RetainedSchemaVersionRow
		var payload string
		if err := rows.Scan(&r.VersionKey, &r.StreamID, &r.SchemaName, &r.TableName, &r.AnchorPosition, &payload); err != nil {
			return nil, fmt.Errorf("mysql: scan schema history row: %w", err)
		}
		r.TableJSON = []byte(payload)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: list schema history: %w", err)
	}
	return out, nil
}

// isNoSuchTableErr reports whether err is a *mysql.MySQLError with
// error number 1146 (ER_NO_SUCH_TABLE) — the "the table doesn't
// exist yet" case that diagnose handles silently rather than failing.
func isNoSuchTableErr(err error) bool {
	var mErr *mysql.MySQLError
	if !errors.As(err, &mErr) {
		return false
	}
	return mErr.Number == 1146
}

// DiagnoseBundle implements [ir.DiagnoseProber] for the MySQL schema
// reader. Surfaces:
//
//   - SELECT VERSION() — server version string.
//   - Master-status: SHOW MASTER STATUS (binlog file + position,
//     executed GTID set). gtid_mode + log_bin variable values too.
//   - The PlanetScale flavour adds an empty notes section (real
//     PlanetScale state lives in their Insights UI, not in MySQL
//     metadata).
//
// Errors at any sub-section don't fail the whole probe — they're
// embedded in the EngineState JSON as section-level reason strings.
func (r *SchemaReader) DiagnoseBundle(ctx context.Context, streamID string) (ir.DiagnoseSnapshot, error) {
	snap := ir.DiagnoseSnapshot{
		EngineName:   r.flavor.String(),
		Capabilities: r.flavor.capabilities(),
	}

	var v string
	if err := r.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&v); err == nil {
		snap.EngineVersion = v
	}

	state := map[string]any{}
	state["stream_id_scope"] = streamID

	// gtid_mode + log_bin via SHOW VARIABLES (works on every MySQL
	// flavour including PlanetScale's read-only view). The PS flavour
	// suppresses SHOW MASTER STATUS in the streaming path — we mirror
	// that here by not failing the probe when the call returns 0
	// rows.
	if vars, err := probeShowVariables(ctx, r.db, []string{"gtid_mode", "log_bin", "server_uuid", "version_comment"}); err == nil {
		state["variables"] = vars
	} else {
		state["variables_reason"] = err.Error()
	}

	if status, err := probeMasterStatus(ctx, r.db); err == nil && status != nil {
		state["master_status"] = status
	} else if err != nil {
		state["master_status_reason"] = err.Error()
	}

	payload, err := json.Marshal(state)
	if err != nil {
		return snap, fmt.Errorf("mysql: marshal diagnose state: %w", err)
	}
	snap.EngineState = payload
	return snap, nil
}

// probeShowVariables runs SHOW VARIABLES WHERE Variable_name IN (...)
// and returns a map of the rows. Bounded; only the requested
// variables come back. Errors propagate to the caller.
func probeShowVariables(ctx context.Context, db *sql.DB, names []string) (map[string]string, error) {
	if len(names) == 0 {
		return map[string]string{}, nil
	}
	// Build the IN clause with positional placeholders.
	placeholders := ""
	args := make([]any, 0, len(names))
	for i, n := range names {
		if i > 0 {
			placeholders += ","
		}
		placeholders += "?"
		args = append(args, n)
	}
	q := "SHOW VARIABLES WHERE Variable_name IN (" + placeholders + ")"
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]string{}
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		out[name] = value
	}
	return out, rows.Err()
}

// probeMasterStatus runs SHOW MASTER STATUS and returns the row as a
// map. SHOW MASTER STATUS returns at most one row; empty result
// (binlog disabled or PS read-only view) surfaces as nil, nil so the
// caller can render an "unavailable" section rather than a failure.
func probeMasterStatus(ctx context.Context, db *sql.DB) (map[string]any, error) {
	rows, err := db.QueryContext(ctx, "SHOW MASTER STATUS")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, rows.Err()
	}
	scan := make([]any, len(cols))
	vals := make([]sql.NullString, len(cols))
	for i := range scan {
		scan[i] = &vals[i]
	}
	if err := rows.Scan(scan...); err != nil {
		return nil, err
	}
	out := map[string]any{}
	for i, c := range cols {
		if vals[i].Valid {
			out[c] = vals[i].String
		}
	}
	return out, rows.Err()
}
