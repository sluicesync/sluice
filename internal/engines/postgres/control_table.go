// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
	"sluicesync.dev/sluice/internal/ir"
)

// controlTableName is the per-target table that holds CDC stream
// positions. ADR-0007 picks the name; v1 honors it verbatim. Aliased
// from appliershared so both engines share one source of truth.
const controlTableName = appliershared.ControlTableName

// shardConsolidationLeaseTableName is the ADR-0054 per-target control
// table that holds the cross-shard DDL-coordination lease (one row per
// consolidated target table). Lives in the same controlSchema as
// sluice_cdc_state; additive, never touches existing data. See
// ensureShardConsolidationLeaseTable.
const shardConsolidationLeaseTableName = appliershared.ShardConsolidationLeaseTableName

// controlCfg is the ADR-0081 tier-c dialect seam for the shared
// control-table CRUD in internal/appliershared: the engine-constant
// leaves (error prefix, the 42P01 missing-relation classifier, the
// stream-not-found sentinel) the shared skeletons need. SQL text is
// built at each call site — quoting, placeholder style, and the
// controlSchema qualification stay in this package.
// RowsAffectedIsChangedRows stays false: Postgres RowsAffected
// reports matched rows, so RequestStop's zero-rows check detects a
// missing stream row directly.
var controlCfg = &appliershared.ControlTableConfig{
	EngineName:        "postgres",
	IsMissingTable:    isUndefinedRelationErr,
	ErrStreamNotFound: errStreamNotFound,
}

// controlTableRef returns the schema-qualified, quoted reference to
// the sluice_cdc_state table. Postgres has namespaced schemas (unlike
// MySQL's flat per-connection database), so every control-table
// statement qualifies the name with the schema threaded from the
// DSN's `schema` query parameter (default "public").
func controlTableRef(schema string) string {
	return quoteIdent(schema) + "." + quoteIdent(controlTableName)
}

// shardLeaseTableRef is controlTableRef's counterpart for the
// ADR-0054 lease table.
func shardLeaseTableRef(schema string) string {
	return quoteIdent(schema) + "." + quoteIdent(shardConsolidationLeaseTableName)
}

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
	tableRef := controlTableRef(schema)
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
	// Migration path for pre-ADR-0176-prerequisite deployments: the
	// publication_name column is slot_name's exact sibling — it records
	// the publication the stream reads through, so a warm resume can
	// ratchet onto the SAME publication the stream cold-started with
	// without the operator re-passing --publication-name. NULL on
	// legacy rows (== "engine default `sluice_pub`", byte-identical to
	// pre-column behaviour); the position-write path UPSERTs the
	// streamer's effective publication name on every apply tx.
	alter = "ALTER TABLE " + tableRef + " ADD COLUMN IF NOT EXISTS publication_name TEXT NULL"
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("postgres: ensure control table: add publication_name: %w", err)
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
	// Migration path for pre-ADR-0156-phase-2 deployments: the
	// rows_applied column is the lifetime cumulative row-level-DML-applied
	// counter surfaced in `sync start`'s live panel. NOT NULL DEFAULT 0
	// backfills legacy rows to 0 (an honest cumulative starting point —
	// pre-upgrade applies were never tracked). ADD COLUMN IF NOT EXISTS is
	// supported in every PG version sluice targets.
	alter = "ALTER TABLE " + tableRef + " ADD COLUMN IF NOT EXISTS rows_applied BIGINT NOT NULL DEFAULT 0"
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("postgres: ensure control table: add rows_applied: %w", err)
	}
	return nil
}

// shardConsolidationLeaseRow aliases the shared lease-row mirror of
// ir.ShardConsolidationLeaseRow (ADR-0081 tier c); the ChangeApplier's
// interface methods (in shard_consolidation_lease.go) translate
// between this and the pipeline shape.
type shardConsolidationLeaseRow = appliershared.ShardLeaseRow

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
//
// UTC contract (task #44, the lease-TZ class): lease_expires_at is a
// naive TIMESTAMP column, and pgx encodes a time.Time parameter as the
// value's wall-clock digits in its own location. A host running
// TZ=UTC-7 would therefore store digits seven hours behind the
// server's clock, making the takeover guard true the instant the
// lease is written (instantly stealable); a TZ-ahead host gets stuck
// leases. Every client-supplied expiry is normalized to .UTC() before
// binding, and the SQL guard compares against timezone('utc', now())
// — a naive timestamp holding UTC digits — instead of
// CURRENT_TIMESTAMP, whose timestamptz comparison would coerce the
// column through the session TimeZone.
func tryAcquireShardLease(ctx context.Context, db *sql.DB, schema, tableName, streamID string, expires time.Time) (acquired bool, current shardConsolidationLeaseRow, err error) {
	tableRef := shardLeaseTableRef(schema)
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
			` + tableRef + `.lease_expires_at <= timezone('utc', now())
			AND ` + tableRef + `.applied_at IS NULL
		RETURNING
			target_table_full_name,
			COALESCE(lease_holder_stream_id, ''),
			lease_expires_at,
			COALESCE(ddl_text, ''),
			COALESCE(ddl_checksum, ''),
			applied_schema_version,
			applied_at,
			anchor_position,
			source_engine`
	var got shardConsolidationLeaseRow
	scanErr := appliershared.ScanShardLeaseRow(db.QueryRowContext(ctx, q, tableName, streamID, expires.UTC()), &got)
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
//
// The WHERE guard makes the UPDATE conditional on continued
// ownership: holder must match AND applied_at must still be
// NULL. A holder that has already finalized would no-op here, but
// the heartbeat loop doesn't fire after Apply closes stopCh, so
// this case shouldn't arise in practice.
//
// expires is normalized to .UTC() before binding — same naive-
// TIMESTAMP contract as tryAcquireShardLease (task #44).
func heartbeatShardLease(ctx context.Context, db *sql.DB, schema, tableName, streamID string, expires time.Time) (extended bool, err error) {
	q := "UPDATE " + shardLeaseTableRef(schema) + " SET lease_expires_at = $1 " +
		"WHERE target_table_full_name = $2 " +
		"AND lease_holder_stream_id = $3 " +
		"AND applied_at IS NULL"
	return appliershared.GuardedExec(ctx, db, controlCfg, "lease heartbeat", q, expires.UTC(), tableName, streamID)
}

// recordShardLeaseDDLText UPDATEs ddl_text for the held lease. Same
// holder-guard as heartbeatShardLease.
func recordShardLeaseDDLText(ctx context.Context, db *sql.DB, schema, tableName, streamID, ddlText string) (recorded bool, err error) {
	q := "UPDATE " + shardLeaseTableRef(schema) + " SET ddl_text = $1 " +
		"WHERE target_table_full_name = $2 " +
		"AND lease_holder_stream_id = $3 " +
		"AND applied_at IS NULL"
	return appliershared.GuardedExec(ctx, db, controlCfg, "lease record ddl", q, ddlText, tableName, streamID)
}

// finalizeShardLeaseApply records applied_at + ddl_text + ddl_checksum
// + applied_schema_version + anchor_position + source_engine
// atomically, gated on continued ownership.
//
// anchorPos / anchorEngine carry the source-side CDC position the
// boundary was observed at (v0.76.0+); empty strings store NULL so
// legacy callers (and the unit-test fakes that don't supply an anchor)
// preserve the pre-anchor shape.
func finalizeShardLeaseApply(ctx context.Context, db *sql.DB, schema, tableName, streamID, ddlText, ddlChecksum string, version int64, anchorPos, anchorEngine string) (finalized bool, err error) {
	q := "UPDATE " + shardLeaseTableRef(schema) + " SET " +
		"ddl_text = $1, ddl_checksum = $2, " +
		"applied_schema_version = $3, applied_at = CURRENT_TIMESTAMP, " +
		"anchor_position = NULLIF($4, ''), source_engine = NULLIF($5, '') " +
		"WHERE target_table_full_name = $6 " +
		"AND lease_holder_stream_id = $7 " +
		"AND applied_at IS NULL"
	return appliershared.GuardedExec(ctx, db, controlCfg, "lease finalize", q,
		ddlText, ddlChecksum, version, anchorPos, anchorEngine, tableName, streamID)
}

// listShardLeases returns every row in the per-target lease table.
// Tolerant of the table being absent. ADR-0054 §6 operator-visibility
// surface, plus the v0.76.0 lease GC sweep's enumeration source.
func listShardLeases(ctx context.Context, db *sql.DB, schema string) ([]shardConsolidationLeaseRow, error) {
	q := "SELECT target_table_full_name, COALESCE(lease_holder_stream_id, ''), " +
		"lease_expires_at, COALESCE(ddl_text, ''), COALESCE(ddl_checksum, ''), " +
		"applied_schema_version, applied_at, anchor_position, source_engine FROM " + shardLeaseTableRef(schema)
	return appliershared.ListShardLeases(ctx, db, controlCfg, q)
}

// deleteShardLease removes the row keyed by tableName. Tolerant of the
// row being absent (DELETE on a missing PK is a no-op) and of the table
// itself being absent (returns nil so a GC sweep against a pre-Ensure
// target is a no-op). v0.76.0 lease GC sweep (task #21).
func deleteShardLease(ctx context.Context, db *sql.DB, schema, tableName string) error {
	q := "DELETE FROM " + shardLeaseTableRef(schema) + " WHERE target_table_full_name = $1"
	return appliershared.TolerantExec(ctx, db, controlCfg, "lease delete", q, tableName)
}

// selectShardLease loads the row for tableName. Returns ok=false when
// no row exists (ABSENT) or when the table doesn't exist (dry-run
// path before EnsureControlTable).
func selectShardLease(ctx context.Context, db *sql.DB, schema, tableName string) (row shardConsolidationLeaseRow, ok bool, err error) {
	q := "SELECT target_table_full_name, COALESCE(lease_holder_stream_id, ''), " +
		"lease_expires_at, COALESCE(ddl_text, ''), COALESCE(ddl_checksum, ''), " +
		"applied_schema_version, applied_at, anchor_position, source_engine " +
		"FROM " + shardLeaseTableRef(schema) + " WHERE target_table_full_name = $1"
	return appliershared.SelectShardLease(ctx, db, controlCfg, q, tableName)
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
	tableRef := shardLeaseTableRef(schema)
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
			anchor_position               TEXT         NULL,
			source_engine                 TEXT         NULL,
			PRIMARY KEY (target_table_full_name)
		)`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("postgres: ensure shard consolidation lease table: %w", err)
	}
	// Migration path for v0.75.0 deployments whose
	// sluice_shard_consolidation_lease table pre-dates the v0.76.0
	// anchor columns. ADD COLUMN IF NOT EXISTS is supported in every
	// PG version sluice targets; mirrors the additive migrations on
	// sluice_cdc_state + sluice_cdc_schema_history. Task #21 (lease GC
	// sweep) reads anchor_position; legacy rows have NULL and are
	// defensively retained by the sweeper.
	for _, alter := range []string{
		"ALTER TABLE " + tableRef + " ADD COLUMN IF NOT EXISTS anchor_position TEXT NULL",
		"ALTER TABLE " + tableRef + " ADD COLUMN IF NOT EXISTS source_engine TEXT NULL",
	} {
		if _, err := db.ExecContext(ctx, alter); err != nil {
			return fmt.Errorf("postgres: ensure shard consolidation lease table: add anchor columns: %w", err)
		}
	}
	return nil
}

// readPosition returns the persisted source_position for streamID,
// or ok=false when no row exists. Engine on the returned Position
// is set by the caller — only the Token survives across runs.
// Missing-relation tolerance and error shape live in the shared
// skeleton; the classifier is the same string-match helper the
// schema reader's PostGIS lookup uses.
func readPosition(ctx context.Context, db *sql.DB, schema, streamID string) (token string, ok bool, err error) {
	q := "SELECT source_position FROM " + controlTableRef(schema) + " WHERE stream_id = $1"
	return appliershared.ReadPosition(ctx, db, controlCfg, q, streamID)
}

// listStreams returns every row in the per-target control table via
// the shared skeleton (missing relation → "no streams", so
// `sluice sync status` works against a target that hasn't been a CDC
// destination yet; COALESCE on slot_name / publication_name /
// source_dsn_fingerprint / target_schema so legacy NULL rows surface
// as empty strings — the fingerprint check (ADR-0031) treats empty as
// "unknown — allow", the target_schema check (Bug 46) as "operator
// did not pass --target-schema", and an empty publication_name as
// "engine default publication" (ADR-0176 prerequisite)). The Position
// values set Engine to the engine-specific identifier for symmetry
// with ReadPosition's contract.
func listStreams(ctx context.Context, db *sql.DB, schema, engineName string) ([]ir.StreamStatus, error) {
	q := "SELECT stream_id, source_position, updated_at, " +
		"COALESCE(slot_name, ''), " +
		"COALESCE(publication_name, ''), " +
		"COALESCE(source_dsn_fingerprint, ''), " +
		"COALESCE(target_schema, ''), " +
		"COALESCE(rows_applied, 0) " +
		"FROM " + controlTableRef(schema)
	return appliershared.ListStreams(ctx, db, controlCfg, q, engineName)
}

// writePositionTx upserts the (streamID, token, slotName) row inside
// an open transaction. Called from the applier's per-change tx after
// the data write; same atomicity guarantee as the MySQL counterpart.
//
// Deliberately NOT routed through the appliershared tier-c skeletons:
// the upsert SQL is the ADR-0007/ADR-0010 resume contract and wholly
// dialect (ON CONFLICT here vs row-alias ON DUPLICATE KEY on MySQL),
// so each engine byte-owns it.
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
// publicationName is slot_name's exact sibling (ADR-0176 prerequisite
// chunk): the publication the stream reads through on a PG source,
// recorded so warm resume ratchets onto the same publication without
// the operator re-passing --publication-name. Same COALESCE-tolerant
// shape: non-empty overwrites, empty preserves (legacy streams and
// non-PG sources record nothing, and the row stays NULL == "engine
// default publication").
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
//
// rowsApplied is the number of row-level DML changes this position write
// makes durable; it is ADDED to the row's cumulative rows_applied in the
// SAME upsert so the counter advances atomically with the position
// (ADR-0156 phase 2). 0 for a no-data position write.
func writePositionTx(ctx context.Context, tx *sql.Tx, schema, streamID, token, slotName, publicationName, sourceFingerprint, targetSchema string, rowsApplied int64) error {
	q, args := buildWritePositionSQL(schema, streamID, token, slotName, publicationName, sourceFingerprint, targetSchema, rowsApplied)
	if _, err := tx.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("postgres: write position: %w", err)
	}
	return nil
}

// buildWritePositionSQL returns the position-upsert (sql, args) shared by
// both the serial exec path ([writePositionTx]) and the ADR-0092
// pipelined queue path ([ChangeApplier.writePositionPipelined]). Factoring
// the build out keeps the ADR-0007/ADR-0010 row shape + COALESCE
// preservation semantics single-sourced — the two callers cannot drift.
//
// COALESCE on the conflict path lets a non-empty slotName /
// publicationName / sourceFingerprint / targetSchema overwrite, while an
// empty value falls back to whichever value the existing row already
// carries — so a chain-handoff position-write that lacks streamer context
// doesn't clobber the streamer's previously-recorded values.
func buildWritePositionSQL(schema, streamID, token, slotName, publicationName, sourceFingerprint, targetSchema string, rowsApplied int64) (stmt string, args []any) {
	tableRef := controlTableRef(schema)
	q := "INSERT INTO " + tableRef + " (stream_id, source_position, updated_at, slot_name, publication_name, source_dsn_fingerprint, target_schema, rows_applied) " +
		"VALUES ($1, $2, CURRENT_TIMESTAMP, NULLIF($3, ''), NULLIF($4, ''), NULLIF($5, ''), NULLIF($6, ''), $7) " +
		"ON CONFLICT (stream_id) DO UPDATE SET " +
		"source_position = EXCLUDED.source_position, " +
		"updated_at = EXCLUDED.updated_at, " +
		"slot_name = COALESCE(EXCLUDED.slot_name, " + tableRef + ".slot_name), " +
		"publication_name = COALESCE(EXCLUDED.publication_name, " + tableRef + ".publication_name), " +
		"source_dsn_fingerprint = COALESCE(EXCLUDED.source_dsn_fingerprint, " + tableRef + ".source_dsn_fingerprint), " +
		"target_schema = COALESCE(EXCLUDED.target_schema, " + tableRef + ".target_schema), " +
		// rows_applied ACCUMULATES: add this write's delta to the existing
		// count (COALESCE guards a legacy NULL row, though NOT NULL DEFAULT 0
		// means the column is never NULL post-migration). EXCLUDED.rows_applied
		// is the $7 delta, NOT a replacement value.
		"rows_applied = COALESCE(" + tableRef + ".rows_applied, 0) + EXCLUDED.rows_applied"
	return q, []any{streamID, token, slotName, publicationName, sourceFingerprint, targetSchema, rowsApplied}
}

// readStopRequested returns true when the named stream's row has a
// non-NULL stop_requested_at column, via the shared skeleton (missing
// relation / missing row → false, nil — a stop signal that hasn't
// been recorded is, by definition, not present; dry-run /
// unconfigured-target paths don't error). The Streamer's poll loop
// calls this every few seconds via the receiver method on
// ChangeApplier; the lint pass can't see that cross-package usage,
// hence the nolint.
//
//nolint:unused // called by pipeline poll loop via ChangeApplier receiver
func readStopRequested(ctx context.Context, db *sql.DB, schema, streamID string) (bool, error) {
	q := "SELECT stop_requested_at IS NOT NULL FROM " + controlTableRef(schema) + " WHERE stream_id = $1"
	return appliershared.ReadStopRequested(ctx, db, controlCfg, q, streamID)
}

// requestStop flips the stop flag on the named stream's row. Returns
// errStreamNotFound when no row exists (the operator likely typoed
// the stream ID; the CLI surfaces a friendly message). Idempotent;
// updated_at is left alone so the "age" column in `sync status`
// continues to reflect real apply activity rather than stop-request
// bookkeeping. The shared skeleton's matched-rows branch (a single
// UPDATE with a zero-rows check) applies — see controlCfg.
func requestStop(ctx context.Context, db *sql.DB, schema, streamID string) error {
	q := "UPDATE " + controlTableRef(schema) + " SET stop_requested_at = CURRENT_TIMESTAMP WHERE stream_id = $1"
	return appliershared.RequestStop(ctx, db, controlCfg, "", q, streamID)
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
// and tolerant of a missing row or table (returns nil) — the next
// position-write commit will populate the row. (EnsureControlTable
// runs first and creates the table, but a brand-new target may have
// an in-flight schema-apply at this point.)
//
// Why not clear on consumption? The polling goroutine doesn't own
// a transaction with the applier's data writes, so a clear-on-read
// could lose the signal if the data write rolls back after seeing
// the flag. Clearing at startup is structurally simpler: the
// streamer's lifecycle owns the flag's lifecycle.
func clearStopRequested(ctx context.Context, db *sql.DB, schema, streamID string) error {
	q := "UPDATE " + controlTableRef(schema) + " SET stop_requested_at = NULL WHERE stream_id = $1"
	return appliershared.TolerantExec(ctx, db, controlCfg, "clear stop signal", q, streamID)
}

// clearStream deletes the named stream's row from the per-target
// control table. Idempotent and tolerant of a missing row or table —
// re-running `--reset-target-data` after a partial failure proceeds
// cleanly. See [ChangeApplier.ClearStream] for the recovery flow.
func clearStream(ctx context.Context, db *sql.DB, schema, streamID string) error {
	q := "DELETE FROM " + controlTableRef(schema) + " WHERE stream_id = $1"
	return appliershared.TolerantExec(ctx, db, controlCfg, "clear stream", q, streamID)
}
