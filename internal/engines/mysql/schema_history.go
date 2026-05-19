// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// ADR-0049 CDC schema-history — MySQL store (Chunk A).
//
// Additive per-target control table holding, at every detected DDL
// boundary, a snapshot of the affected table's IR schema keyed by the
// source position the boundary was observed at. Mirrors the
// sluice_cdc_state control-table discipline in control_table.go
// exactly: flat namespace (database implicit in the connection),
// CREATE TABLE IF NOT EXISTS, ENGINE=InnoDB DEFAULT CHARSET=utf8mb4,
// run from the *sql.DB pool at applier startup (MySQL implicit-commits
// DDL so it cannot live inside the per-change tx).
//
// Chunk-A scope fence: storage + serialization + resolve only. No
// DDL-boundary detection, no hot-path cache, no applier wiring (those
// are ADR-0049 chunks B/C/D). The write/load functions take an
// executor/queryer interface (not a concrete *sql.DB) specifically so
// a LATER chunk (decision #4a) can pass the ADR-0007 position-write
// *sql.Tx and get the version write into the SAME target tx as the
// position write. Chunk A deliberately does NOT wire that — it only
// makes the API tx-ready.

const schemaHistoryTableName = "sluice_cdc_schema_history"

// schemaHistoryExecer is the minimal write surface shared by *sql.DB
// and *sql.Tx. writeSchemaVersion takes this (not a concrete type) so
// a later chunk can hand it the position-write tx without an API
// change.
//
//nolint:unused // ADR-0049 Chunk A storage-only; consumers wire in B/C (see file scope fence).
type schemaHistoryExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// schemaHistoryQueryer is the minimal read surface shared by *sql.DB
// and *sql.Tx.
//
//nolint:unused // ADR-0049 Chunk A storage-only; consumers wire in B/C (see file scope fence).
type schemaHistoryQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// ensureSchemaHistoryTable creates the per-target
// sluice_cdc_schema_history table if it doesn't exist. Idempotent —
// second-and-later calls are no-ops courtesy of CREATE TABLE IF NOT
// EXISTS. ADDITIVE: it never touches sluice_cdc_state or any existing
// data; a target that already has cdc-state rows is unaffected.
//
// Same MySQL-can't-CREATE-in-an-explicit-tx caveat as
// ensureControlTable — callers run this from the *sql.DB pool at
// applier startup, alongside ensureControlTable.
//
// anchor_position is LONGTEXT: it holds the engine-opaque position
// token (ADR-0007), which for GTID sets can be long. ir_schema_json is
// LONGTEXT: the MarshalTable codec output. created_at defaults to
// CURRENT_TIMESTAMP for operator diagnostics.
//
// PK is version_key (CHAR(64)) — a fixed-width [ir.SchemaVersionKey]
// SHA-256 surrogate over the natural tuple (stream_id, schema_name,
// table_name, anchor_position). The natural tuple cannot be the PK
// directly: InnoDB caps a key at 3072 bytes (four utf8mb4
// VARCHAR(255)s = 4080), and a prefix index on the unbounded anchor
// would let two distinct long anchors sharing a prefix collide and
// silently overwrite each other (a silent-loss class). The surrogate
// is collision-free and index-safe; the natural columns remain stored
// (NOT NULL) so the resolver round-trips the full anchor token.
func ensureSchemaHistoryTable(ctx context.Context, db *sql.DB) error {
	const ddl = `
		CREATE TABLE IF NOT EXISTS ` + "`" + schemaHistoryTableName + "`" + ` (
			version_key     CHAR(64)     NOT NULL,
			stream_id       VARCHAR(255) NOT NULL,
			schema_name     VARCHAR(255) NOT NULL,
			table_name      VARCHAR(255) NOT NULL,
			anchor_position LONGTEXT     NOT NULL,
			ir_schema_json  LONGTEXT     NOT NULL,
			created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (version_key)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("mysql: ensure schema-history table: %w", wrapDDLError(err))
	}
	return nil
}

// writeSchemaVersion serializes t via the ADR-0049 decision-#1 codec
// ([ir.MarshalTable], which composes the backup tagged-union Column
// codec) and UPSERTs the (streamID, schema, table, anchor) row.
//
// Idempotent on the PK, matching sluice_cdc_state's writePositionTx
// upsert behaviour: re-writing the same anchor overwrites
// ir_schema_json (the schema at a given boundary position is
// immutable, so an overwrite is a value-identical no-op in practice;
// the upsert form keeps the call idempotent and avoids a
// duplicate-key error on a retried boundary).
//
// Takes a [schemaHistoryExecer] so a later chunk can pass the
// ADR-0007 position-write *sql.Tx (decision #4a) — Chunk A does not
// wire that path; this only makes the API tx-ready.
//
// Chunk A is storage-only: the applier wiring that calls this lands in
// Chunk B/C (file-level scope fence). It is exercised by the
// integration-tagged round-trip test, which the unused linter (run
// without the integration tag) can't see — same documented pattern as
// control_table.go's readStopRequested.
//
//nolint:unused // ADR-0049 Chunk A storage-only; consumers wire in B/C.
func writeSchemaVersion(ctx context.Context, exec schemaHistoryExecer, streamID, schema, table string, anchor ir.Position, t *ir.Table) error {
	if t == nil {
		return errors.New("mysql: write schema version: table is nil")
	}
	payload, err := ir.MarshalTable(t)
	if err != nil {
		return fmt.Errorf("mysql: write schema version: marshal table: %w", err)
	}
	vk := ir.SchemaVersionKey(streamID, schema, table, anchor.Token)
	const q = "INSERT INTO `" + schemaHistoryTableName + "` " +
		"(version_key, stream_id, schema_name, table_name, anchor_position, ir_schema_json) " +
		"VALUES (?, ?, ?, ?, ?, ?) " +
		"AS new ON DUPLICATE KEY UPDATE ir_schema_json = new.ir_schema_json"
	if _, err := exec.ExecContext(ctx, q, vk, streamID, schema, table, anchor.Token, string(payload)); err != nil {
		return fmt.Errorf("mysql: write schema version: %w", err)
	}
	return nil
}

// loadRetainedSchemaVersions reads every retained
// (anchor_position, ir_schema_json) pair for the (streamID, schema,
// table) key. The Engine field of each reconstructed [ir.Position] is
// stamped engineNameMySQL so the decoder accepts it on the resolve
// path (the persisted token alone is opaque without its engine tag).
//
//nolint:unused // ADR-0049 Chunk A storage-only; see writeSchemaVersion.
func loadRetainedSchemaVersions(ctx context.Context, q schemaHistoryQueryer, streamID, schema, table string) ([]ir.RetainedSchemaVersion, error) {
	const sel = "SELECT anchor_position, ir_schema_json FROM `" + schemaHistoryTableName + "` " +
		"WHERE stream_id = ? AND schema_name = ? AND table_name = ?"
	rows, err := q.QueryContext(ctx, sel, streamID, schema, table)
	if err != nil {
		return nil, fmt.Errorf("mysql: load schema versions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []ir.RetainedSchemaVersion{}
	for rows.Next() {
		var (
			anchorTok string
			payload   string
		)
		if err := rows.Scan(&anchorTok, &payload); err != nil {
			return nil, fmt.Errorf("mysql: scan schema version: %w", err)
		}
		out = append(out, ir.RetainedSchemaVersion{
			Anchor:    ir.Position{Engine: engineNameMySQL, Token: anchorTok},
			TableJSON: []byte(payload),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("mysql: load schema versions: %w", err)
	}
	return out, nil
}

// resolveSchemaVersion loads the retained versions for the key and
// delegates the ordering + loud-floor decision to the engine-neutral
// [ir.ResolveSchemaVersion], supplying this engine as the
// [ir.PositionOrderer]. A position below the retention floor / before
// the first boundary surfaces as an error wrapping
// [ir.ErrPositionInvalid] (→ ADR-0022 cold-start).
//
//nolint:unused // ADR-0049 Chunk A storage-only; see writeSchemaVersion.
func resolveSchemaVersion(ctx context.Context, q schemaHistoryQueryer, orderer ir.PositionOrderer, streamID, schema, table string, p ir.Position) (*ir.Table, error) {
	versions, err := loadRetainedSchemaVersions(ctx, q, streamID, schema, table)
	if err != nil {
		return nil, err
	}
	return ir.ResolveSchemaVersion(orderer, versions, p)
}
