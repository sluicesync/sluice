// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package appliershared

// # Shared control-table CRUD (ADR-0081 tier c)
//
// The sluice_cdc_state and sluice_shard_consolidation_lease
// persistence helpers used to be maintained twice, once per engine,
// as deliberate mirrors: identical scan / tolerance / error-shape
// control flow with dialect differences (identifier quoting,
// placeholder style, the missing-table error surface) interleaved
// line by line — 11 of ~15 historical commits to either copy had to
// touch both. This file is those skeletons, hoisted once, behind the
// [ControlTableConfig] dialect seam, following the [BatchConfig]
// precedent: each engine builds its dialect SQL (quoting,
// placeholders, upsert syntax stay engine-side — the tier-a finding
// that sharing SQL text would force exactly the dialect abstraction
// the IR-first tenet keeps out of shared code) and hands the
// finished statement plus a small config of the genuinely divergent
// leaves to the shared control flow.
//
// What deliberately did NOT move (the divergence is structural, not
// skeletal): the ensure-table DDL + per-engine column-migration
// mechanism (detect-then-ALTER vs ADD COLUMN IF NOT EXISTS), the
// position-write upsert (writePositionTx — ON DUPLICATE KEY vs ON
// CONFLICT; its SQL semantics are the ADR-0007/ADR-0010 resume
// contract and stay byte-owned by each engine), the lease acquire
// state machines (MySQL's SELECT … FOR UPDATE + deadlock retry vs
// PG's single conditional upsert), and MySQL's live-added-tables
// surface (ADR-0034 — an engine-only feature, not a mirror).

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// ControlTableName is the per-target table that holds CDC stream
// positions. ADR-0007 picks the name; v1 honors it verbatim. A
// configurable prefix lands as part of roadmap §10. Each engine
// aliases this constant so its const SQL strings keep a single
// source of truth for the name.
const ControlTableName = "sluice_cdc_state"

// ShardConsolidationLeaseTableName is the ADR-0054 per-target control
// table that holds the cross-shard DDL-coordination lease (one row
// per consolidated target table). On Postgres it lives in the same
// controlSchema as sluice_cdc_state; on MySQL in the connection's
// default database.
const ShardConsolidationLeaseTableName = "sluice_shard_consolidation_lease"

// ControlTableConfig is the dialect seam for the shared control-table
// CRUD (ADR-0081 tier c). Each engine keeps one package-level value
// (the fields are engine constants, not per-stream state) and passes
// it alongside the engine-built SQL. Everything NOT in this struct —
// scan loops, ErrNoRows / missing-table tolerance, error-shape
// wrapping, the RowsAffected protocols — is shared control flow that
// lives in this file.
type ControlTableConfig struct {
	// EngineName prefixes every error this package wraps
	// ("mysql: read position: …", "postgres: list streams: …") so
	// operator-facing output is byte-identical to the pre-extraction
	// per-engine helpers.
	EngineName string

	// IsMissingTable classifies the dialect's "control table absent"
	// error (MySQL Error 1146, PG SQLSTATE 42P01). The read/clear
	// paths tolerate a missing control table — dry-run flows and
	// `sluice sync status` against a never-CDC'd target degrade to
	// "no rows" rather than erroring. Must return false on nil.
	IsMissingTable func(error) bool

	// ErrStreamNotFound is the engine's sentinel for [RequestStop]
	// against an unknown stream_id. It stays engine-owned so the
	// engines' integration pins (errors.Is against the package-local
	// sentinel) hold unchanged; the CLI string-matches the wrapped
	// text rather than importing either sentinel.
	ErrStreamNotFound error

	// RowsAffectedIsChangedRows names the one structural divergence
	// in the stop-flag write path: whether the driver's RowsAffected
	// reports *changed* rows rather than *matched* rows.
	//
	//   - true (MySQL): go-sql-driver defaults to changed-rows
	//     semantics — an UPDATE that rewrites a row with the same
	//     value reports 0, so "0 rows means missing row" is
	//     unreliable. [RequestStop] uses a SELECT-then-UPDATE pair
	//     instead. The two queries don't need a transaction: the
	//     UPDATE is itself atomic, and a stream row is only ever
	//     inserted by the streamer's position write, so the race is
	//     benign (transient missing row → operator retry → success).
	//   - false (Postgres): RowsAffected reports matched rows, so a
	//     single UPDATE with a zero-rows check detects the missing
	//     row directly.
	RowsAffectedIsChangedRows bool

	// ListStreamsFallback, when non-nil, is consulted when the
	// [ListStreams] *query* fails with something other than a
	// missing table (scan errors never reach it). MySQL wires its
	// unknown-column (Error 1054) → legacy-column-set fallback here,
	// keeping `sluice sync status` working against a pre-v0.32.2
	// control table mid-upgrade; the legacy query and its 3-column
	// scan stay wholly engine-side. handled=false falls through to
	// the normal wrapped error. Nil on Postgres.
	ListStreamsFallback func(ctx context.Context, queryErr error) (out []ir.StreamStatus, handled bool, err error)
}

// ReadPosition returns the persisted source_position for streamID,
// or ok=false when no row exists. The Engine field of the position is
// set by the caller — only the Token survives across runs (the engine
// reading is implicitly the engine that wrote).
//
// query is the engine-built SELECT with a single placeholder for
// stream_id. Tolerant of the control table being absent: a
// missing-table error is reported as ok=false (same as "no row") so
// dry-run flows that skip EnsureControlTable still work.
func ReadPosition(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, query, streamID string) (token string, ok bool, err error) {
	row := db.QueryRowContext(ctx, query, streamID)
	switch err := row.Scan(&token); {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case cfg.IsMissingTable(err):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("%s: read position: %w", cfg.EngineName, err)
	}
	return token, true, nil
}

// ListStreams returns every row in the per-target control table.
// Tolerant of the table being absent (treated as "no streams") so
// `sluice sync status` works against a target that hasn't been a CDC
// destination yet.
//
// query is the engine-built SELECT over (stream_id, source_position,
// updated_at, slot_name, source_dsn_fingerprint, target_schema,
// rows_applied) with COALESCE(”) on the string columns and
// COALESCE(…, 0) on rows_applied, so legacy rows that pre-date those
// columns surface as empty strings / 0 in the StreamStatus — callers
// branch on empty-string rather than handling sql.NullString. The
// fingerprint check (ADR-0031) treats empty as "unknown — allow," so
// legacy rows don't trip false-positive stream-id collisions; the
// target_schema check (Bug 46) treats empty as "operator did not pass
// --target-schema; use the DSN default schema"; rows_applied 0 on a
// legacy row is the honest cumulative starting point (pre-upgrade
// applies were never tracked).
//
// positionEngine is stamped onto each returned Position for symmetry
// with ReadPosition's contract — the token alone is opaque without
// the engine context.
func ListStreams(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, query, positionEngine string) ([]ir.StreamStatus, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		if cfg.IsMissingTable(err) {
			return []ir.StreamStatus{}, nil
		}
		if cfg.ListStreamsFallback != nil {
			if out, handled, ferr := cfg.ListStreamsFallback(ctx, err); handled {
				return out, ferr
			}
		}
		return nil, fmt.Errorf("%s: list streams: %w", cfg.EngineName, err)
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
			rowsApplied  int64
		)
		if err := rows.Scan(&streamID, &token, &updated, &slotName, &fingerprint, &targetSchema, &rowsApplied); err != nil {
			return nil, fmt.Errorf("%s: scan streams: %w", cfg.EngineName, err)
		}
		out = append(out, ir.StreamStatus{
			StreamID:             streamID,
			Position:             ir.Position{Engine: positionEngine, Token: token},
			UpdatedAt:            updated,
			SlotName:             slotName,
			SourceDSNFingerprint: fingerprint,
			TargetSchema:         targetSchema,
			RowsApplied:          rowsApplied,
		})
	}
	return out, rows.Err()
}

// ReadStopRequested returns true when the named stream's row has a
// non-NULL stop_requested_at column. Tolerant of the table being
// absent (returns false, nil) so polling-loop startup races and
// dry-run / unconfigured-target paths don't surface as errors.
//
// Returns (false, nil) when the row doesn't exist — a stop signal
// that hasn't been recorded is, by definition, not present.
//
// query is the engine-built `SELECT stop_requested_at IS NOT NULL …`
// with a single placeholder for stream_id.
func ReadStopRequested(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, query, streamID string) (bool, error) {
	var stopRequested bool
	err := db.QueryRowContext(ctx, query, streamID).Scan(&stopRequested)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case cfg.IsMissingTable(err):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("%s: read stop flag: %w", cfg.EngineName, err)
	}
	return stopRequested, nil
}

// RequestStop flips the stop flag on the named stream's row. Returns
// an error wrapping cfg.ErrStreamNotFound when no row exists (the
// operator likely typoed the stream ID; the CLI surfaces a friendly
// message).
//
// Idempotent: repeated calls land the same flag (the timestamp
// updates, but the streamer treats any non-NULL value as "stop
// requested" so the repeat is harmless). updated_at is left alone by
// both engines' SQL so the "age" column in `sync status` continues
// to reflect real apply activity rather than stop-request
// bookkeeping.
//
// updateQuery is the engine-built UPDATE with one placeholder for
// stream_id. existsQuery (a `SELECT 1 … WHERE stream_id = ?`) is
// required only when cfg.RowsAffectedIsChangedRows is true — the
// missing-row probe for drivers whose RowsAffected can't be trusted
// for it (see the [ControlTableConfig.RowsAffectedIsChangedRows]
// doc); both detection shapes live here so the divergence stays a
// named flag, not a re-implemented loop.
func RequestStop(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, existsQuery, updateQuery, streamID string) error {
	if cfg.RowsAffectedIsChangedRows {
		// Changed-rows drivers (MySQL): probe for the row first, then
		// UPDATE unconditionally, ignoring its RowsAffected.
		var dummy int
		switch err := db.QueryRowContext(ctx, existsQuery, streamID).Scan(&dummy); {
		case errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("%w: %q", cfg.ErrStreamNotFound, streamID)
		case err != nil:
			return fmt.Errorf("%s: request stop: existence check: %w", cfg.EngineName, err)
		}
		if _, err := db.ExecContext(ctx, updateQuery, streamID); err != nil {
			return fmt.Errorf("%s: request stop: %w", cfg.EngineName, err)
		}
		return nil
	}
	// Matched-rows drivers (Postgres): a single UPDATE; zero rows
	// affected means the row doesn't exist.
	res, err := db.ExecContext(ctx, updateQuery, streamID)
	if err != nil {
		return fmt.Errorf("%s: request stop: %w", cfg.EngineName, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: request stop: rows affected: %w", cfg.EngineName, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: %q", cfg.ErrStreamNotFound, streamID)
	}
	return nil
}

// TolerantExec runs an engine-built statement, tolerating the control
// table being absent (returns nil — the callers are all idempotent
// clear/delete paths where "table not there yet" means "nothing to
// clear"). op labels the wrapped error ("clear stop signal",
// "clear stream", "lease delete") so the error text stays
// byte-identical to the pre-extraction per-engine helpers.
func TolerantExec(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, op, query string, args ...any) error {
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		if cfg.IsMissingTable(err) {
			return nil
		}
		return fmt.Errorf("%s: %s: %w", cfg.EngineName, op, err)
	}
	return nil
}

// GuardedExec runs an engine-built guarded UPDATE (the ADR-0054 lease
// heartbeat / record-ddl / finalize family — WHERE holder = ? AND
// applied_at IS NULL) and reports whether any row changed. op labels
// the wrapped errors ("lease heartbeat", "lease record ddl",
// "lease finalize").
//
// The n > 0 return feeds the lease state machine's "still the
// holder?" decision; on MySQL the changed-rows RowsAffected caveat
// doesn't bite here because each of these UPDATEs always changes a
// column (a later expiry, new ddl_text, NULL→CURRENT_TIMESTAMP
// applied_at), so 0 reliably means "guard rejected".
func GuardedExec(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, op, query string, args ...any) (bool, error) {
	res, err := db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("%s: %s: %w", cfg.EngineName, op, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("%s: %s: rows affected: %w", cfg.EngineName, op, err)
	}
	return n > 0, nil
}

// ShardLeaseRow is the engine-internal mirror of
// ir.ShardConsolidationLeaseRow, shared by both engines' lease
// primitives (each aliases it as its package-local
// shardConsolidationLeaseRow). Defined with database/sql Null types
// so the engines' scan paths stay direct; [ShardLeaseRow.ToIR]
// converts to the pipeline-facing shape.
type ShardLeaseRow struct {
	TargetTableFullName  string
	LeaseHolderStreamID  string
	LeaseExpiresAt       sql.NullTime
	DDLText              string
	DDLChecksum          string
	AppliedSchemaVersion int64
	AppliedAt            sql.NullTime

	// AnchorPosition + AnchorEngine: the source-side CDC position +
	// engine identity at which the recorded boundary's DDL was
	// observed. Populated by the engines' finalize paths in v0.76.0+;
	// NULL on legacy rows that pre-date the additive migration (the
	// lease GC sweep defensively retains NULL-anchor rows). Mirrors
	// the anchor_position / source_engine columns the
	// sluice_cdc_schema_history table carries (ADR-0049 / Bug 78).
	AnchorPosition sql.NullString
	AnchorEngine   sql.NullString
}

// ToIR converts the engine's sql.NullTime-bearing row shape to the
// cross-package HasX-bool shape. ADR-0081 tier (d): the conversion was
// byte-identical in both engines' lease wrapper files; together with
// ShardLeaseRow + [ScanShardLeaseRow] it gives the whole lease-row
// projection contract a single owner.
func (r ShardLeaseRow) ToIR() ir.ShardConsolidationLeaseRow {
	out := ir.ShardConsolidationLeaseRow{
		TargetTableFullName:  r.TargetTableFullName,
		LeaseHolderStreamID:  r.LeaseHolderStreamID,
		DDLText:              r.DDLText,
		DDLChecksum:          r.DDLChecksum,
		AppliedSchemaVersion: r.AppliedSchemaVersion,
	}
	if r.LeaseExpiresAt.Valid {
		out.LeaseExpiresAt = r.LeaseExpiresAt.Time
		out.HasLeaseExpiresAt = true
	}
	if r.AppliedAt.Valid {
		out.AppliedAt = r.AppliedAt.Time
		out.HasAppliedAt = true
	}
	// Reconstruct the source-side anchor Position. Both Token + Engine
	// must be present for an anchor to count as "set" — a half-populated
	// row (legacy v0.75.0 + a manually-poked anchor_position with
	// source_engine still NULL) is treated as absent so the GC sweep
	// defensively retains it.
	if r.AnchorPosition.Valid && r.AnchorEngine.Valid {
		out.AnchorPosition = ir.Position{
			Engine: r.AnchorEngine.String,
			Token:  r.AnchorPosition.String,
		}
		out.HasAnchor = true
	}
	return out
}

// RowScanner is the scan surface shared by *sql.Row and *sql.Rows.
type RowScanner interface {
	Scan(dest ...any) error
}

// ScanShardLeaseRow scans the canonical 9-column lease projection —
// target_table_full_name, COALESCE(lease_holder_stream_id, ”),
// lease_expires_at, COALESCE(ddl_text, ”), COALESCE(ddl_checksum,
// ”), applied_schema_version, applied_at, anchor_position,
// source_engine — into row. Every lease SELECT (the shared
// list/select below and both engines' acquire paths) uses this one
// scan so the projection order has a single owner.
func ScanShardLeaseRow(s RowScanner, row *ShardLeaseRow) error {
	return s.Scan(
		&row.TargetTableFullName,
		&row.LeaseHolderStreamID,
		&row.LeaseExpiresAt,
		&row.DDLText,
		&row.DDLChecksum,
		&row.AppliedSchemaVersion,
		&row.AppliedAt,
		&row.AnchorPosition,
		&row.AnchorEngine,
	)
}

// ListShardLeases returns every row in the per-target lease table.
// Tolerant of the table being absent. ADR-0054 §6 operator-visibility
// surface used by `sluice sync status`, plus the v0.76.0 lease GC
// sweep's enumeration source. query is the engine-built SELECT over
// the [ScanShardLeaseRow] projection.
func ListShardLeases(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, query string) ([]ShardLeaseRow, error) {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		if cfg.IsMissingTable(err) {
			return []ShardLeaseRow{}, nil
		}
		return nil, fmt.Errorf("%s: list leases: %w", cfg.EngineName, err)
	}
	defer func() { _ = rows.Close() }()

	out := []ShardLeaseRow{}
	for rows.Next() {
		var row ShardLeaseRow
		if err := ScanShardLeaseRow(rows, &row); err != nil {
			return nil, fmt.Errorf("%s: scan lease: %w", cfg.EngineName, err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// SelectShardLease loads the lease row for one target table. Returns
// ok=false when no row exists (ABSENT) or when the table itself is
// missing (pre-Ensure inspection / dry-run path). query is the
// engine-built SELECT over the [ScanShardLeaseRow] projection with a
// single placeholder for target_table_full_name.
func SelectShardLease(ctx context.Context, db *sql.DB, cfg *ControlTableConfig, query, tableName string) (row ShardLeaseRow, ok bool, err error) {
	scanErr := ScanShardLeaseRow(db.QueryRowContext(ctx, query, tableName), &row)
	switch {
	case errors.Is(scanErr, sql.ErrNoRows):
		return ShardLeaseRow{}, false, nil
	case cfg.IsMissingTable(scanErr):
		return ShardLeaseRow{}, false, nil
	case scanErr != nil:
		return ShardLeaseRow{}, false, fmt.Errorf("%s: lease select: %w", cfg.EngineName, scanErr)
	}
	return row, true, nil
}
