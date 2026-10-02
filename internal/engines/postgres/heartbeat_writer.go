// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
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
	// Detect first (GC-40 (a)): PostgreSQL checks CREATE on the schema
	// before it evaluates IF NOT EXISTS, so a source role holding only
	// INSERT/DELETE on a heartbeat table an owner pre-created would
	// otherwise take the permission-degrade path below and write no
	// heartbeats at all.
	present, err := relationPresent(ctx, r.db, tableRef)
	if err != nil {
		if isPGPermissionDenied(err) {
			return errors.Join(ir.ErrHeartbeatPermission, err)
		}
		return fmt.Errorf("postgres: ensure heartbeat table %q: detect: %w", tableName, err)
	}
	if present {
		if err := checkHeartbeatTableShape(ctx, r.db, tableRef, tableName); err != nil {
			return err
		}
		// The id is BIGSERIAL, so every INSERT draws from its sequence: a
		// role granted INSERT on the table but not USAGE on the sequence
		// fails every heartbeat. Say so now, with the grant, on the same
		// degrade path a denied write takes.
		var seqOK bool
		if err := r.db.QueryRowContext(ctx,
			`SELECT COALESCE(pg_catalog.has_sequence_privilege(pg_catalog.pg_get_serial_sequence($1, 'id'), 'USAGE'), true)`,
			tableRef).Scan(&seqOK); err != nil {
			return fmt.Errorf("postgres: ensure heartbeat table %q: detect id sequence privilege: %w", tableName, err)
		}
		if !seqOK {
			return errors.Join(ir.ErrHeartbeatPermission, fmt.Errorf(
				"postgres: heartbeat table %s exists but this role lacks USAGE on its id sequence, so every heartbeat INSERT would fail — "+
					"GRANT USAGE ON SEQUENCE <the id column's sequence, normally %s_id_seq> TO this role", tableRef, tableName,
			))
		}
		return nil
	}
	ddl := `
		CREATE TABLE IF NOT EXISTS ` + tableRef + ` (
			id        BIGSERIAL    PRIMARY KEY,
			ts        TIMESTAMPTZ  NOT NULL DEFAULT pg_catalog.NOW(),
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

// heartbeatTableShape is the column set EnsureHeartbeatTable creates, by
// pg_catalog.format_type. Unchanged since the writer shipped (v0.82.0), so a
// table any release created matches it.
var heartbeatTableShape = map[string]string{"id": "bigint", "ts": "timestamp with time zone", "stream_id": "text"}

// heartbeatTableShapeText renders [heartbeatTableShape] for the refusal.
const heartbeatTableShapeText = "(id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ, stream_id TEXT) and no other column"

// checkHeartbeatTableShape refuses a relation already present under the
// heartbeat's name unless it has exactly the shape EnsureHeartbeatTable
// creates. The writer INSERTs into it and its prune DELETEs by ts, so a name
// that collides with a user table — a typo in --source-heartbeat-table-name
// — would write into and delete from the SOURCE's data. Exactly the three
// columns, with their types: any other column is evidence the relation holds
// something besides heartbeats. Only an ordinary table qualifies: a view or
// foreign table reads back no columns here and is refused (a DELETE through
// a simple view deletes from the table under it). ref is the quoted
// schema-qualified name.
func checkHeartbeatTableShape(ctx context.Context, db *sql.DB, ref, tableName string) error {
	rows, err := db.QueryContext(ctx, `
		SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)
		FROM pg_catalog.pg_attribute a
		JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
		WHERE a.attrelid = pg_catalog.to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped
		  AND c.relkind = 'r'
		ORDER BY a.attnum`, ref)
	if err != nil {
		return fmt.Errorf("postgres: ensure heartbeat table %q: read its columns: %w", tableName, err)
	}
	defer func() { _ = rows.Close() }()
	var found []string
	matched := 0
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return fmt.Errorf("postgres: ensure heartbeat table %q: read its columns: %w", tableName, err)
		}
		found = append(found, name+" "+typ)
		if heartbeatTableShape[name] == typ {
			matched++
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: ensure heartbeat table %q: read its columns: %w", tableName, err)
	}
	if matched == len(heartbeatTableShape) && len(found) == len(heartbeatTableShape) {
		return nil
	}
	return fmt.Errorf(
		"postgres: %w: relation %s already exists on the source and is not sluice's heartbeat table "+
			"(its columns: %s; sluice's heartbeat table has exactly %s). The heartbeat would INSERT rows into it and "+
			"DELETE its rows by ts, so sluice refuses to touch it. Pick a --source-heartbeat-table-name that does not "+
			"exist (sluice creates it), or turn the heartbeat off (drop --source-heartbeat-interval, or pass --no-source-heartbeat)",
		ir.ErrHeartbeatTableNotSluices, ref, describeFoundColumns(found), heartbeatTableShapeText,
	)
}

// describeFoundColumns renders the columns the shape check read; none means
// the relation is not an ordinary table (a view, say).
func describeFoundColumns(found []string) string {
	if len(found) == 0 {
		return "none, it is not an ordinary table"
	}
	return strings.Join(found, ", ")
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
	q := fmt.Sprintf("DELETE FROM %s WHERE ts < pg_catalog.NOW() - INTERVAL '%d seconds'", tableRef, seconds)
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
