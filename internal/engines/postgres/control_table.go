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

// shardConsolidationLeaseTableName is the ADR-0054 per-target control
// table that holds the cross-shard DDL-coordination lease (one row per
// consolidated target table). Lives in the same controlSchema as
// sluice_cdc_state; additive, never touches existing data. See
// ensureShardConsolidationLeaseTable.
const shardConsolidationLeaseTableName = "sluice_shard_consolidation_lease"

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

// shardConsolidationLeaseRow is the engine-internal mirror of
// pipeline.ShardConsolidationLeaseRow. Defined here so the engine's
// lease primitives don't import the pipeline package (which would
// create a cycle); the ChangeApplier's interface methods (in
// shard_consolidation_lease.go) translate between this and the
// pipeline shape.
type shardConsolidationLeaseRow struct {
	TargetTableFullName  string
	LeaseHolderStreamID  string
	LeaseExpiresAt       sql.NullTime
	DDLText              string
	DDLChecksum          string
	AppliedSchemaVersion int64
	AppliedAt            sql.NullTime
}

// tryAcquireShardLease conditionally INSERTs / UPDATEs a lease row
// under the row-level lock of the conflict-on-PK ON CONFLICT path.
// The acquire wins iff one of:
//
//   - The row is ABSENT (the INSERT lands cleanly), or
//   - The row's lease_expires_at <= now() AND applied_at IS NULL
//     (EXPIRED takeover-eligible).
//
// A failed acquire (contention or APPLIED state) returns the current
// row so the caller can classify into LeaseState.
//
// Implementation: a single SQL statement does the conditional path.
// We use an INSERT ... ON CONFLICT (target_table_full_name) DO UPDATE
// SET ... WHERE clause: the ON CONFLICT path conditionally updates iff
// the existing row is takeover-eligible. RETURNING + a follow-up
// SELECT (under PG's snapshot semantics inside the same tx) gives the
// caller a consistent view.
func tryAcquireShardLease(ctx context.Context, db *sql.DB, schema, tableName, streamID string, expires time.Time) (acquired bool, current shardConsolidationLeaseRow, err error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(shardConsolidationLeaseTableName)
	// Single-statement upsert; RETURNING gives back whether the
	// statement actually wrote (acquired) or hit the WHERE-guard
	// (contended). When acquired, the returned row reflects the
	// just-acquired state (including any prior-holder ddl_text the
	// guard preserves below). When contended, we run a follow-up
	// SELECT to load the current row so the caller can classify.
	//
	// The ON CONFLICT WHERE clause is the locked-down conditional:
	// only the EXPIRED-takeover case wins on conflict. APPLIED rows
	// (applied_at IS NOT NULL) and HELD rows (lease_expires_at > now)
	// both fall through to no-update, the RETURNING is empty, and the
	// SELECT below loads the current row.
	q := `
		INSERT INTO ` + tableRef + ` (
			target_table_full_name,
			lease_holder_stream_id,
			lease_expires_at,
			applied_schema_version,
			created_at
		)
		VALUES ($1, $2, $3, 0, CURRENT_TIMESTAMP)
		ON CONFLICT (target_table_full_name)
		DO UPDATE SET
			lease_holder_stream_id = EXCLUDED.lease_holder_stream_id,
			lease_expires_at       = EXCLUDED.lease_expires_at
		WHERE
			` + tableRef + `.lease_expires_at <= CURRENT_TIMESTAMP
			AND ` + tableRef + `.applied_at IS NULL
		RETURNING
			target_table_full_name,
			COALESCE(lease_holder_stream_id, ''),
			lease_expires_at,
			COALESCE(ddl_text, ''),
			COALESCE(ddl_checksum, ''),
			applied_schema_version,
			applied_at`
	row := db.QueryRowContext(ctx, q, tableName, streamID, expires)
	var got shardConsolidationLeaseRow
	scanErr := row.Scan(
		&got.TargetTableFullName,
		&got.LeaseHolderStreamID,
		&got.LeaseExpiresAt,
		&got.DDLText,
		&got.DDLChecksum,
		&got.AppliedSchemaVersion,
		&got.AppliedAt,
	)
	switch {
	case errors.Is(scanErr, sql.ErrNoRows):
		// Conditional upsert WHERE-guard rejected the conflict path:
		// row exists but is HELD or APPLIED. Load it so the caller
		// can classify.
		got, ok, err := selectShardLease(ctx, db, schema, tableName)
		if err != nil {
			return false, shardConsolidationLeaseRow{}, err
		}
		if !ok {
			// Very rare: row vanished between the failed RETURNING
			// and the SELECT (some other stream applied + a GC
			// sweep). Treat as transient — caller will retry.
			return false, shardConsolidationLeaseRow{}, errors.New("postgres: lease acquire: row vanished between insert and reload")
		}
		return false, got, nil
	case scanErr != nil:
		return false, shardConsolidationLeaseRow{}, fmt.Errorf("postgres: lease acquire: %w", scanErr)
	}
	return true, got, nil
}

// heartbeatShardLease extends lease_expires_at to expires iff the row
// is still held by streamID. Returns extended=false when a peer has
// taken over (the holder's apply path must exit).
func heartbeatShardLease(ctx context.Context, db *sql.DB, schema, tableName, streamID string, expires time.Time) (extended bool, err error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(shardConsolidationLeaseTableName)
	// The WHERE guard makes the UPDATE conditional on continued
	// ownership: holder must match AND applied_at must still be
	// NULL. A holder that has already finalized would no-op here, but
	// the heartbeat loop doesn't fire after Apply closes stopCh, so
	// this case shouldn't arise in practice.
	q := "UPDATE " + tableRef + " SET lease_expires_at = $1 " +
		"WHERE target_table_full_name = $2 " +
		"AND lease_holder_stream_id = $3 " +
		"AND applied_at IS NULL"
	res, err := db.ExecContext(ctx, q, expires, tableName, streamID)
	if err != nil {
		return false, fmt.Errorf("postgres: lease heartbeat: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("postgres: lease heartbeat: rows affected: %w", err)
	}
	return n > 0, nil
}

// recordShardLeaseDDLText UPDATEs ddl_text for the held lease. Same
// holder-guard as heartbeatShardLease.
func recordShardLeaseDDLText(ctx context.Context, db *sql.DB, schema, tableName, streamID, ddlText string) (recorded bool, err error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(shardConsolidationLeaseTableName)
	q := "UPDATE " + tableRef + " SET ddl_text = $1 " +
		"WHERE target_table_full_name = $2 " +
		"AND lease_holder_stream_id = $3 " +
		"AND applied_at IS NULL"
	res, err := db.ExecContext(ctx, q, ddlText, tableName, streamID)
	if err != nil {
		return false, fmt.Errorf("postgres: lease record ddl: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("postgres: lease record ddl: rows affected: %w", err)
	}
	return n > 0, nil
}

// finalizeShardLeaseApply records applied_at + ddl_text + ddl_checksum
// + applied_schema_version atomically, gated on continued ownership.
func finalizeShardLeaseApply(ctx context.Context, db *sql.DB, schema, tableName, streamID, ddlText, ddlChecksum string, version int64) (finalized bool, err error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(shardConsolidationLeaseTableName)
	q := "UPDATE " + tableRef + " SET " +
		"ddl_text = $1, ddl_checksum = $2, " +
		"applied_schema_version = $3, applied_at = CURRENT_TIMESTAMP " +
		"WHERE target_table_full_name = $4 " +
		"AND lease_holder_stream_id = $5 " +
		"AND applied_at IS NULL"
	res, err := db.ExecContext(ctx, q, ddlText, ddlChecksum, version, tableName, streamID)
	if err != nil {
		return false, fmt.Errorf("postgres: lease finalize: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("postgres: lease finalize: rows affected: %w", err)
	}
	return n > 0, nil
}

// listShardLeases returns every row in the per-target lease table.
// Tolerant of the table being absent. ADR-0054 §6 operator-visibility
// surface.
func listShardLeases(ctx context.Context, db *sql.DB, schema string) ([]shardConsolidationLeaseRow, error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(shardConsolidationLeaseTableName)
	q := "SELECT target_table_full_name, COALESCE(lease_holder_stream_id, ''), " +
		"lease_expires_at, COALESCE(ddl_text, ''), COALESCE(ddl_checksum, ''), " +
		"applied_schema_version, applied_at FROM " + tableRef
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		if isUndefinedRelationErr(err) {
			return []shardConsolidationLeaseRow{}, nil
		}
		return nil, fmt.Errorf("postgres: list leases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []shardConsolidationLeaseRow{}
	for rows.Next() {
		var row shardConsolidationLeaseRow
		if err := rows.Scan(
			&row.TargetTableFullName,
			&row.LeaseHolderStreamID,
			&row.LeaseExpiresAt,
			&row.DDLText,
			&row.DDLChecksum,
			&row.AppliedSchemaVersion,
			&row.AppliedAt,
		); err != nil {
			return nil, fmt.Errorf("postgres: scan lease: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// selectShardLease loads the row for tableName. Returns ok=false when
// no row exists (ABSENT) or when the table doesn't exist (dry-run
// path before EnsureControlTable).
func selectShardLease(ctx context.Context, db *sql.DB, schema, tableName string) (row shardConsolidationLeaseRow, ok bool, err error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(shardConsolidationLeaseTableName)
	q := "SELECT target_table_full_name, COALESCE(lease_holder_stream_id, ''), " +
		"lease_expires_at, COALESCE(ddl_text, ''), COALESCE(ddl_checksum, ''), " +
		"applied_schema_version, applied_at " +
		"FROM " + tableRef + " WHERE target_table_full_name = $1"
	r := db.QueryRowContext(ctx, q, tableName)
	scanErr := r.Scan(
		&row.TargetTableFullName,
		&row.LeaseHolderStreamID,
		&row.LeaseExpiresAt,
		&row.DDLText,
		&row.DDLChecksum,
		&row.AppliedSchemaVersion,
		&row.AppliedAt,
	)
	switch {
	case errors.Is(scanErr, sql.ErrNoRows):
		return shardConsolidationLeaseRow{}, false, nil
	case isUndefinedRelationErr(scanErr):
		return shardConsolidationLeaseRow{}, false, nil
	case scanErr != nil:
		return shardConsolidationLeaseRow{}, false, fmt.Errorf("postgres: lease select: %w", scanErr)
	}
	return row, true, nil
}

// ensureShardConsolidationLeaseTable creates the per-target
// sluice_shard_consolidation_lease control table (ADR-0054 §1) in the
// named schema if it doesn't exist. Idempotent — second-and-later
// calls are no-ops courtesy of CREATE TABLE IF NOT EXISTS. ADDITIVE:
// never touches sluice_cdc_state, sluice_cdc_schema_history, or any
// existing data.
//
// The table holds one row per consolidated target table. All shards'
// streams converge on the same row for the same target table; the
// conditional-UPDATE acquire path (LeaseManager.Acquire) provides the
// mutex semantics, and the heartbeat goroutine extends expires_at on
// the holder's RetryPeriod cadence. See ADR-0054 §1 for the state
// machine and §2 for the timing defaults.
func ensureShardConsolidationLeaseTable(ctx context.Context, db *sql.DB, schema string) error {
	tableRef := quoteIdent(schema) + "." + quoteIdent(shardConsolidationLeaseTableName)
	ddl := `
		CREATE TABLE IF NOT EXISTS ` + tableRef + ` (
			target_table_full_name        VARCHAR(512) NOT NULL,
			lease_holder_stream_id        VARCHAR(64)  NULL,
			lease_expires_at              TIMESTAMP    NULL,
			ddl_text                      TEXT         NULL,
			ddl_checksum                  VARCHAR(64)  NULL,
			applied_schema_version        BIGINT       NOT NULL DEFAULT 0,
			applied_at                    TIMESTAMP    NULL,
			created_at                    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (target_table_full_name)
		)`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("postgres: ensure shard consolidation lease table: %w", err)
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
