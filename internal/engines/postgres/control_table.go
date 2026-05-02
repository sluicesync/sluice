package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
func ensureControlTable(ctx context.Context, db *sql.DB, schema string) error {
	tableRef := quoteIdent(schema) + "." + quoteIdent(controlTableName)
	ddl := `
		CREATE TABLE IF NOT EXISTS ` + tableRef + ` (
			stream_id       VARCHAR(255) NOT NULL,
			source_position TEXT         NOT NULL,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (stream_id)
		)`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("postgres: ensure control table: %w", err)
	}
	return nil
}

// readPosition returns the persisted source_position for streamID,
// or ok=false when no row exists. Engine on the returned Position
// is set by the caller — only the Token survives across runs.
func readPosition(ctx context.Context, db *sql.DB, schema, streamID string) (token string, ok bool, err error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(controlTableName)
	q := "SELECT source_position FROM " + tableRef + " WHERE stream_id = $1"
	row := db.QueryRowContext(ctx, q, streamID)
	switch err := row.Scan(&token); {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("postgres: read position: %w", err)
	}
	return token, true, nil
}

// writePositionTx upserts the (streamID, token) row inside an open
// transaction. Called from the applier's per-change tx after the
// data write; same atomicity guarantee as the MySQL counterpart.
//
// The updated_at column is refreshed on every upsert via
// CURRENT_TIMESTAMP — diagnostic info for operators inspecting the
// control table by hand.
func writePositionTx(ctx context.Context, tx *sql.Tx, schema, streamID, token string) error {
	tableRef := quoteIdent(schema) + "." + quoteIdent(controlTableName)
	q := "INSERT INTO " + tableRef + " (stream_id, source_position, updated_at) " +
		"VALUES ($1, $2, CURRENT_TIMESTAMP) " +
		"ON CONFLICT (stream_id) DO UPDATE SET " +
		"source_position = EXCLUDED.source_position, " +
		"updated_at = EXCLUDED.updated_at"
	if _, err := tx.ExecContext(ctx, q, streamID, token); err != nil {
		return fmt.Errorf("postgres: write position: %w", err)
	}
	return nil
}
