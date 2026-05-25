// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/orware/sluice/internal/ir"
)

// EnsureHeartbeatTable / WriteHeartbeat / PruneHeartbeat implement
// [ir.HeartbeatWriter] for Postgres — severity-A finding F17 of the
// 2026-05-22 Reddit-research run, see ADR-0061.
//
// The heartbeat table lives in the SchemaReader's bound schema (the
// DSN's `schema` query parameter, default "public"). It carries three
// columns:
//
//   - id (BIGSERIAL PRIMARY KEY) — surrogate so older PG releases that
//     can't use IDENTITY have a clean PK type;
//   - ts (TIMESTAMPTZ DEFAULT NOW()) — server-side timestamp, so the
//     prune logic can compare against the source's clock rather than
//     trusting the writer's;
//   - stream_id (TEXT NOT NULL) — labels the row with the originating
//     stream so an operator inspecting `sluice_heartbeat` by hand can
//     see which sluice instance produced it.
//
// The table name is operator-configurable via the pipeline wiring
// (default `sluice_heartbeat`); the engine surface accepts it as a
// parameter so the contract is engine-neutral and the IR-first tenet
// is preserved.
//
// **Permission detection.** When the connecting role lacks CREATE TABLE
// privilege, PG surfaces SQLSTATE 42501 (insufficient_privilege). The
// EnsureHeartbeatTable path wraps that case in [ir.ErrHeartbeatPermission]
// so the pipeline wiring can `errors.Is` check it and degrade to
// WARN-once-skip without classifying the whole stream as failed.
// WriteHeartbeat / PruneHeartbeat unwrap the same sentinel; a stream
// that loses INSERT privilege mid-run downgrades cleanly.

// pgSQLStateInsufficientPrivilege is the SQLSTATE class returned by
// Postgres when the connecting role lacks the required privilege on
// the requested object. The class string is stable across PG versions
// (defined in the wire-protocol spec).
const pgSQLStateInsufficientPrivilege = "42501"

// EnsureHeartbeatTable implements [ir.HeartbeatWriter]. Creates the
// heartbeat table in the SchemaReader's bound schema if it doesn't
// already exist; idempotent on second-and-later calls.
//
// Returns [ir.ErrHeartbeatPermission]-wrapped on the
// insufficient-privilege class so the pipeline wiring can `errors.Is`
// check and degrade gracefully. Other DDL failures pass through verbatim.
func (r *SchemaReader) EnsureHeartbeatTable(ctx context.Context, tableName string) error {
	if r.db == nil {
		return errors.New("postgres: EnsureHeartbeatTable: reader not opened")
	}
	if tableName == "" {
		return errors.New("postgres: EnsureHeartbeatTable: tableName is empty")
	}
	schema := r.schema
	if schema == "" {
		schema = "public"
	}
	tableRef := quoteIdent(schema) + "." + quoteIdent(tableName)
	ddl := `
		CREATE TABLE IF NOT EXISTS ` + tableRef + ` (
			id        BIGSERIAL    PRIMARY KEY,
			ts        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			stream_id TEXT         NOT NULL
		)`
	if _, err := r.db.ExecContext(ctx, ddl); err != nil {
		if isPGPermissionDenied(err) {
			return errors.Join(ir.ErrHeartbeatPermission, err)
		}
		return fmt.Errorf("postgres: ensure heartbeat table %q: %w", tableName, err)
	}
	return nil
}

// WriteHeartbeat implements [ir.HeartbeatWriter]. INSERTs one row with
// the server-side current timestamp and the supplied streamID.
//
// Returns [ir.ErrHeartbeatPermission]-wrapped on the
// insufficient-privilege class so a mid-stream privilege revocation
// degrades to WARN-once-skip rather than failing the streamer.
func (r *SchemaReader) WriteHeartbeat(ctx context.Context, tableName, streamID string) error {
	if r.db == nil {
		return errors.New("postgres: WriteHeartbeat: reader not opened")
	}
	if tableName == "" {
		return errors.New("postgres: WriteHeartbeat: tableName is empty")
	}
	schema := r.schema
	if schema == "" {
		schema = "public"
	}
	tableRef := quoteIdent(schema) + "." + quoteIdent(tableName)
	// ts column has a NOW() default; the INSERT supplies stream_id only.
	// Letting PG fill ts keeps every row's timestamp on the source's
	// clock, which the prune comparison depends on.
	q := "INSERT INTO " + tableRef + " (stream_id) VALUES ($1)"
	if _, err := r.db.ExecContext(ctx, q, streamID); err != nil {
		if isPGPermissionDenied(err) {
			return errors.Join(ir.ErrHeartbeatPermission, err)
		}
		return fmt.Errorf("postgres: write heartbeat row: %w", err)
	}
	return nil
}

// PruneHeartbeat implements [ir.HeartbeatWriter]. DELETEs rows whose
// ts column is older than (NOW() - olderThan). The comparison happens
// server-side so the prune doesn't trust the writer's clock.
//
// olderThan <= 0 is a no-op (returns 0, nil) so the pipeline wiring
// can gate the prune cadence without paying a round-trip on every
// tick.
func (r *SchemaReader) PruneHeartbeat(ctx context.Context, tableName string, olderThan time.Duration) (int64, error) {
	if r.db == nil {
		return 0, errors.New("postgres: PruneHeartbeat: reader not opened")
	}
	if tableName == "" {
		return 0, errors.New("postgres: PruneHeartbeat: tableName is empty")
	}
	if olderThan <= 0 {
		return 0, nil
	}
	schema := r.schema
	if schema == "" {
		schema = "public"
	}
	tableRef := quoteIdent(schema) + "." + quoteIdent(tableName)
	seconds := int64(olderThan / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	// Embed the second-count directly in the SQL — it's a server-validated
	// integer derived from a Go time.Duration, no operator input reaches
	// this path so there's no injection surface. Avoids pgx's bind-type
	// inference resolving the parameter to `text` when concatenated with
	// 'seconds' (the prior shape that tripped the v0.81 CI gate).
	q := fmt.Sprintf("DELETE FROM %s WHERE ts < NOW() - INTERVAL '%d seconds'", tableRef, seconds)
	res, err := r.db.ExecContext(ctx, q)
	if err != nil {
		if isPGPermissionDenied(err) {
			return 0, errors.Join(ir.ErrHeartbeatPermission, err)
		}
		return 0, fmt.Errorf("postgres: prune heartbeat table: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("postgres: prune heartbeat table: rows affected: %w", err)
	}
	return n, nil
}

// isPGPermissionDenied reports whether err is a Postgres
// insufficient_privilege error (SQLSTATE 42501). pgx surfaces these via
// *pgconn.PgError; the SQLSTATE class string is stable across PG
// versions. Used by the heartbeat writer to detect the
// insufficient-privilege class deterministically (rather than
// string-matching the error message, which would be fragile across PG
// locale settings).
func isPGPermissionDenied(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == pgSQLStateInsufficientPrivilege
	}
	return false
}
