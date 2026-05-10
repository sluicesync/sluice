// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// controlTableName is the per-target table that holds CDC stream
// positions. ADR-0007 picks the name; v1 honors it verbatim.
const controlTableName = "sluice_cdc_state"

// ensureControlTable creates the per-target sluice_cdc_state table
// in the named schema if it doesn't exist. Idempotent — second-and-
// later calls are no-ops courtesy of CREATE TABLE IF NOT EXISTS.
//
// Postgres has namespaced schemas, so the table lives in the schema
// passed in (taken from the DSN's `schema` query parameter, default
// "public"). The Streamer reads the schema from the engine config
// and threads it through.
//
// The stop_requested_at column is added with ADD COLUMN IF NOT
// EXISTS so v0.2.x deployments that pre-date the column pick it up
// transparently on the next call. Existing rows keep their data;
// the new column starts NULL (i.e. "no stop requested").
func ensureControlTable(ctx context.Context, db *sql.DB, schema string) error {
	tableRef := quoteIdent(schema) + "." + quoteIdent(controlTableName)
	ddl := `
		CREATE TABLE IF NOT EXISTS ` + tableRef + ` (
			stream_id         VARCHAR(255) NOT NULL,
			source_position   TEXT         NOT NULL,
			updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			stop_requested_at TIMESTAMP    NULL,
			PRIMARY KEY (stream_id)
		)`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("postgres: ensure control table: %w", err)
	}
	// Migration path for pre-`sync stop` deployments: the CREATE
	// TABLE IF NOT EXISTS above is a no-op on existing tables, so
	// the new column has to be added explicitly. ADD COLUMN IF NOT
	// EXISTS is supported in every PG version sluice targets.
	alter := "ALTER TABLE " + tableRef + " ADD COLUMN IF NOT EXISTS stop_requested_at TIMESTAMP NULL"
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("postgres: ensure control table: add stop_requested_at: %w", err)
	}
	// Migration path for pre-Phase-2 (v0.24.0) deployments: the
	// slot_name column lets `sluice schema add-table --no-drain`
	// recover the active stream's slot name without operator input.
	// NULL on legacy rows; the position-write path UPSERTs the
	// streamer's resolved slot name on every apply tx.
	alter = "ALTER TABLE " + tableRef + " ADD COLUMN IF NOT EXISTS slot_name TEXT NULL"
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("postgres: ensure control table: add slot_name: %w", err)
	}
	// Migration path for pre-v0.25.0 deployments: the
	// source_dsn_fingerprint column powers stream-id collision
	// detection (ADR-0031). NULL on legacy rows; the streamer's
	// startup write upserts the truncated SHA-256 of the source
	// DSN's host+port+database tuple on every apply tx.
	alter = "ALTER TABLE " + tableRef + " ADD COLUMN IF NOT EXISTS source_dsn_fingerprint TEXT NULL"
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("postgres: ensure control table: add source_dsn_fingerprint: %w", err)
	}
	// Migration path for pre-v0.25.1 deployments: the target_schema
	// column records the operator-supplied `--target-schema NAME`
	// (ADR-0031) on each position-write so a later
	// `sluice schema add-table` knows which namespace the active
	// stream's CDC applier is routing events to. NULL on legacy
	// rows / streams that didn't pass --target-schema (treated as
	// "use the DSN default schema"). Bug 46.
	alter = "ALTER TABLE " + tableRef + " ADD COLUMN IF NOT EXISTS target_schema TEXT NULL"
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("postgres: ensure control table: add target_schema: %w", err)
	}
	return nil
}

// readPosition returns the persisted source_position for streamID,
// or ok=false when no row exists. Engine on the returned Position
// is set by the caller — only the Token survives across runs.
//
// Tolerant of the control table being absent: a missing-relation
// error is reported as ok=false (same as "no row") so dry-run
// flows that skip EnsureControlTable still work. Missing-relation
// detection uses the same string-match helper as the schema reader's
// PostGIS lookup.
func readPosition(ctx context.Context, db *sql.DB, schema, streamID string) (token string, ok bool, err error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(controlTableName)
	q := "SELECT source_position FROM " + tableRef + " WHERE stream_id = $1"
	row := db.QueryRowContext(ctx, q, streamID)
	switch err := row.Scan(&token); {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case isUndefinedRelationErr(err):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("postgres: read position: %w", err)
	}
	return token, true, nil
}

// listStreams returns every row in the per-target control table.
// Tolerant of the table being absent (treated as "no streams") so
// `sluice sync status` works against a target that hasn't been a
// CDC destination yet.
//
// The Position values returned set Engine to the
// engine-specific identifier the binlog or pgoutput readers use,
// since the token alone is opaque without that context. The CLI
// is the consumer; it doesn't strictly need Engine populated, but
// keeping the shape consistent with ReadPosition's return matters
// for any future caller that might.
func listStreams(ctx context.Context, db *sql.DB, schema, engineName string) ([]ir.StreamStatus, error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(controlTableName)
	// COALESCE on slot_name + source_dsn_fingerprint + target_schema
	// so legacy rows that pre-date those columns (NULL values)
	// surface as empty strings in the StreamStatus — callers branch
	// on empty-string rather than handling sql.NullString. The
	// fingerprint check (ADR-0031) treats empty as "unknown —
	// allow," so legacy rows don't trip false-positive stream-id
	// collisions; the target_schema check (Bug 46) treats empty as
	// "operator did not pass --target-schema; use the DSN default
	// schema."
	q := "SELECT stream_id, source_position, updated_at, " +
		"COALESCE(slot_name, ''), " +
		"COALESCE(source_dsn_fingerprint, ''), " +
		"COALESCE(target_schema, '') " +
		"FROM " + tableRef
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		// Best-effort tolerance: missing-relation = no streams.
		// The schema reader uses the same string-match approach for
		// PostGIS detection (see schema_reader.go).
		if isUndefinedRelationErr(err) {
			return []ir.StreamStatus{}, nil
		}
		return nil, fmt.Errorf("postgres: list streams: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []ir.StreamStatus{}
	for rows.Next() {
		var (
			streamID     string
			token        string
			updated      time.Time
			slotName     string
			fingerprint  string
			targetSchema string
		)
		if err := rows.Scan(&streamID, &token, &updated, &slotName, &fingerprint, &targetSchema); err != nil {
			return nil, fmt.Errorf("postgres: scan streams: %w", err)
		}
		out = append(out, ir.StreamStatus{
			StreamID:             streamID,
			Position:             ir.Position{Engine: engineName, Token: token},
			UpdatedAt:            updated,
			SlotName:             slotName,
			SourceDSNFingerprint: fingerprint,
			TargetSchema:         targetSchema,
		})
	}
	return out, rows.Err()
}

// writePositionTx upserts the (streamID, token, slotName) row inside
// an open transaction. Called from the applier's per-change tx after
// the data write; same atomicity guarantee as the MySQL counterpart.
//
// The updated_at column is refreshed on every upsert via
// CURRENT_TIMESTAMP — diagnostic info for operators inspecting the
// control table by hand. stop_requested_at is left untouched: a
// position write is the streamer making forward progress, which
// must not clear an in-flight stop request.
//
// slotName carries the active stream's resolved replication-slot
// name (per ADR-0030's Phase 2 mid-stream live add-table). When
// non-empty, every position-write refreshes the row's slot_name
// column so a later live-add knows which slot's confirmed_flush_lsn
// to read for the LSN-floor check. Empty slotName preserves any
// previously-recorded value (chain handoff via WritePosition doesn't
// know the streamer's slot; the row's existing slot_name stays put).
//
// sourceFingerprint carries the streamer's source DSN fingerprint
// (ADR-0031) on the same COALESCE-tolerant pattern: non-empty
// overwrites; empty preserves the row's existing value. Powers
// stream-id collision detection on subsequent `sync start` runs.
//
// targetSchema carries the streamer's operator-supplied
// `--target-schema NAME` (ADR-0031, Bug 46) on the same
// COALESCE-tolerant pattern. Recorded on every position-write so
// `sluice schema add-table` can recover the active stream's
// target-schema namespace and refuse a mismatch loudly. Empty input
// preserves the row's existing value (legacy / streams started
// without --target-schema / chain-handoff WritePosition without
// streamer context).
func writePositionTx(ctx context.Context, tx *sql.Tx, schema, streamID, token, slotName, sourceFingerprint, targetSchema string) error {
	tableRef := quoteIdent(schema) + "." + quoteIdent(controlTableName)
	// COALESCE on the conflict path lets a non-empty slotName /
	// sourceFingerprint / targetSchema overwrite, while an empty value
	// falls back to whichever value the existing row already carries —
	// so a chain-handoff position-write that lacks streamer context
	// doesn't clobber the streamer's previously-recorded values.
	q := "INSERT INTO " + tableRef + " (stream_id, source_position, updated_at, slot_name, source_dsn_fingerprint, target_schema) " +
		"VALUES ($1, $2, CURRENT_TIMESTAMP, NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, '')) " +
		"ON CONFLICT (stream_id) DO UPDATE SET " +
		"source_position = EXCLUDED.source_position, " +
		"updated_at = EXCLUDED.updated_at, " +
		"slot_name = COALESCE(EXCLUDED.slot_name, " + tableRef + ".slot_name), " +
		"source_dsn_fingerprint = COALESCE(EXCLUDED.source_dsn_fingerprint, " + tableRef + ".source_dsn_fingerprint), " +
		"target_schema = COALESCE(EXCLUDED.target_schema, " + tableRef + ".target_schema)"
	if _, err := tx.ExecContext(ctx, q, streamID, token, slotName, sourceFingerprint, targetSchema); err != nil {
		return fmt.Errorf("postgres: write position: %w", err)
	}
	return nil
}

// readStopRequested returns true when the named stream's row has a
// non-NULL stop_requested_at column. Tolerant of the table being
// absent (returns false, nil) so dry-run / unconfigured-target paths
// don't error.
//
// Returns (false, nil) when the row doesn't exist — a stop signal
// that hasn't been recorded is, by definition, not present. The
// Streamer's poll loop calls this every few seconds via the
// receiver method on ChangeApplier; the lint pass can't see that
// cross-package usage, hence the nolint.
//
//nolint:unused // called by pipeline poll loop via ChangeApplier receiver
func readStopRequested(ctx context.Context, db *sql.DB, schema, streamID string) (bool, error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(controlTableName)
	q := "SELECT stop_requested_at IS NOT NULL FROM " + tableRef + " WHERE stream_id = $1"
	var stopRequested bool
	err := db.QueryRowContext(ctx, q, streamID).Scan(&stopRequested)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case isUndefinedRelationErr(err):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("postgres: read stop flag: %w", err)
	}
	return stopRequested, nil
}

// requestStop flips the stop flag on the named stream's row. Returns
// errStreamNotFound when no row exists (the operator likely typoed
// the stream ID; the CLI surfaces a friendly message).
//
// Idempotent: repeated calls land the same flag. updated_at is left
// alone so the "age" column in `sync status` continues to reflect
// real apply activity rather than stop-request bookkeeping.
func requestStop(ctx context.Context, db *sql.DB, schema, streamID string) error {
	tableRef := quoteIdent(schema) + "." + quoteIdent(controlTableName)
	q := "UPDATE " + tableRef + " SET stop_requested_at = CURRENT_TIMESTAMP WHERE stream_id = $1"
	res, err := db.ExecContext(ctx, q, streamID)
	if err != nil {
		return fmt.Errorf("postgres: request stop: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postgres: request stop: rows affected: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("%w: %q", errStreamNotFound, streamID)
	}
	return nil
}

// errStreamNotFound is returned by [requestStop] (and thus
// [ChangeApplier.RequestStop]) when no row matches the requested
// stream_id. The CLI string-matches the wrapped engine error rather
// than importing this sentinel, mirroring the slot-not-found shape.
var errStreamNotFound = errors.New("postgres: stream not found")

// clearStopRequested resets the stop_requested_at flag to NULL for
// the named stream. Called by [pipeline.Streamer] at startup so a
// previous `sluice sync stop` doesn't leave a sticky stop signal
// that immediately exits the next `sluice sync start`. Idempotent
// and tolerant of a missing row (returns nil) — the next position-
// write commit will populate the row.
//
// Why not clear on consumption? The polling goroutine doesn't own
// a transaction with the applier's data writes, so a clear-on-read
// could lose the signal if the data write rolls back after seeing
// the flag. Clearing at startup is structurally simpler: the
// streamer's lifecycle owns the flag's lifecycle.
func clearStopRequested(ctx context.Context, db *sql.DB, schema, streamID string) error {
	tableRef := quoteIdent(schema) + "." + quoteIdent(controlTableName)
	q := "UPDATE " + tableRef + " SET stop_requested_at = NULL WHERE stream_id = $1"
	if _, err := db.ExecContext(ctx, q, streamID); err != nil {
		// Tolerant of the table being absent — same shape as
		// readPosition. EnsureControlTable runs first and creates
		// the table, but a brand-new target may have an in-flight
		// schema-apply at this point.
		if isUndefinedRelationErr(err) {
			return nil
		}
		return fmt.Errorf("postgres: clear stop signal: %w", err)
	}
	return nil
}

// clearStream deletes the named stream's row from the per-target
// control table. Idempotent and tolerant of a missing row or table —
// re-running `--reset-target-data` after a partial failure proceeds
// cleanly. See [ChangeApplier.ClearStream] for the recovery flow.
func clearStream(ctx context.Context, db *sql.DB, schema, streamID string) error {
	tableRef := quoteIdent(schema) + "." + quoteIdent(controlTableName)
	q := "DELETE FROM " + tableRef + " WHERE stream_id = $1"
	if _, err := db.ExecContext(ctx, q, streamID); err != nil {
		if isUndefinedRelationErr(err) {
			return nil
		}
		return fmt.Errorf("postgres: clear stream: %w", err)
	}
	return nil
}
