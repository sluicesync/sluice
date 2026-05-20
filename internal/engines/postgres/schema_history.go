// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// ADR-0049 CDC schema-history — Postgres store (Chunk A).
//
// Additive per-target control table holding, at every detected DDL
// boundary, a snapshot of the affected table's IR schema keyed by the
// source position the boundary was observed at. Mirrors the
// sluice_cdc_state control-table discipline in control_table.go
// exactly: schema-qualified tableRef (PG namespaced schemas),
// CREATE TABLE IF NOT EXISTS, run alongside ensureControlTable.
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
// sluice_cdc_schema_history table in the named schema if it doesn't
// exist. Idempotent — second-and-later calls are no-ops courtesy of
// CREATE TABLE IF NOT EXISTS. ADDITIVE: it never touches
// sluice_cdc_state or any existing data; a target that already has
// cdc-state rows is unaffected.
//
// anchor_position and ir_schema_json are TEXT (PG TEXT is unbounded).
// created_at defaults to CURRENT_TIMESTAMP for operator diagnostics.
//
// source_engine is the engine tag of the anchor token's producer (the
// source-side engine that emitted the [ir.SchemaSnapshot]). It is
// NULLABLE so a v0.70.0-shape table that pre-dates the column
// upgrade-migrates cleanly via the ADD COLUMN IF NOT EXISTS below; on
// the read path a NULL value falls back to the applier's own engine
// name (the pre-fix behaviour, correct for same-engine streams).
// Persisting it is what fixes Bug 78: cross-engine chain-restore
// preserves the source token's engine identity through the
// [ir.PositionOrderer] strict engine-tag check.
//
// PK is version_key (CHAR(64)) — the fixed-width [ir.SchemaVersionKey]
// SHA-256 surrogate over the natural tuple (stream_id, schema_name,
// table_name, anchor_position). PG would tolerate the natural tuple as
// a PK, but the surrogate is kept ENGINE-IDENTICAL with MySQL (where
// InnoDB's 3072-byte key limit and the prefix-collision silent-loss
// hazard force it) so the two stores stay structurally congruent and
// the key derivation has a single source of truth. Natural columns
// remain stored (NOT NULL) so the resolver round-trips the full anchor.
func ensureSchemaHistoryTable(ctx context.Context, db *sql.DB, schema string) error {
	tableRef := quoteIdent(schema) + "." + quoteIdent(schemaHistoryTableName)
	ddl := `
		CREATE TABLE IF NOT EXISTS ` + tableRef + ` (
			version_key     CHAR(64)     NOT NULL,
			stream_id       VARCHAR(255) NOT NULL,
			schema_name     VARCHAR(255) NOT NULL,
			table_name      VARCHAR(255) NOT NULL,
			anchor_position TEXT         NOT NULL,
			ir_schema_json  TEXT         NOT NULL,
			source_engine   TEXT         NULL,
			created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (version_key)
		)`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("postgres: ensure schema-history table: %w", err)
	}
	// Migration path for v0.70.0 deployments whose
	// sluice_cdc_schema_history table pre-dates the source_engine column
	// (Bug 78 fix, v0.70.1). NULLABLE: legacy rows have NULL, the read
	// path falls back to engineNamePostgres (the pre-fix behaviour, which
	// is correct for same-engine streams — same-engine chain-restore
	// worked pre-fix). ADD COLUMN IF NOT EXISTS is supported in every PG
	// version sluice targets; mirrors the additive migrations in
	// control_table.go.
	alter := "ALTER TABLE " + tableRef + " ADD COLUMN IF NOT EXISTS source_engine TEXT NULL"
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("postgres: ensure schema-history table: add source_engine: %w", err)
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
func writeSchemaVersion(ctx context.Context, exec schemaHistoryExecer, schema, streamID, schemaName, table string, anchor ir.Position, t *ir.Table) error {
	if t == nil {
		return errors.New("postgres: write schema version: table is nil")
	}
	payload, err := ir.MarshalTable(t)
	if err != nil {
		return fmt.Errorf("postgres: write schema version: marshal table: %w", err)
	}
	tableRef := quoteIdent(schema) + "." + quoteIdent(schemaHistoryTableName)
	vk := ir.SchemaVersionKey(streamID, schemaName, table, anchor.Token)
	// source_engine carries anchor.Engine — the engine identity of the
	// anchor token's producer. NULLIF on empty preserves the legacy
	// (pre-Bug-78) NULL shape for any future caller that omits the
	// engine tag; today the applier dispatch always passes a populated
	// anchor (from the in-stream SchemaSnapshot's Position.Engine), so
	// the empty case is defensive.
	q := "INSERT INTO " + tableRef + " " +
		"(version_key, stream_id, schema_name, table_name, anchor_position, ir_schema_json, source_engine) " +
		"VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, '')) " +
		"ON CONFLICT (version_key) DO UPDATE SET " +
		"ir_schema_json = EXCLUDED.ir_schema_json, " +
		"source_engine = COALESCE(EXCLUDED.source_engine, " + tableRef + ".source_engine)"
	if _, err := exec.ExecContext(ctx, q, vk, streamID, schemaName, table, anchor.Token, string(payload), anchor.Engine); err != nil {
		return fmt.Errorf("postgres: write schema version: %w", err)
	}
	return nil
}

// loadRetainedSchemaVersions reads every retained
// (anchor_position, ir_schema_json, source_engine) tuple for the
// (streamID, schemaName, table) key. The Engine field of each
// reconstructed [ir.Position] is the persisted source_engine — the
// engine that PRODUCED the anchor token, NOT the applier's own engine
// (Bug 78: a cross-engine chain-restore persisted a MySQL GTID token
// under a PG applier; stamping it engineNamePostgres on read made the
// engine-strict [ir.PositionOrderer] reject it during cache-prime).
//
// Legacy v0.70.0 rows have source_engine NULL (the column did not
// exist before the Bug 78 fix); a NULL is treated as "use the
// applier's own engine name" — i.e. the pre-fix behaviour. That is
// correct for same-engine streams (target == source), which are the
// only streams that worked pre-fix.
//
//nolint:unused // ADR-0049 Chunk A storage-only; see writeSchemaVersion.
func loadRetainedSchemaVersions(ctx context.Context, q schemaHistoryQueryer, schema, streamID, schemaName, table string) ([]ir.RetainedSchemaVersion, error) {
	tableRef := quoteIdent(schema) + "." + quoteIdent(schemaHistoryTableName)
	sel := "SELECT anchor_position, ir_schema_json, COALESCE(source_engine, '') FROM " + tableRef + " " +
		"WHERE stream_id = $1 AND schema_name = $2 AND table_name = $3"
	rows, err := q.QueryContext(ctx, sel, streamID, schemaName, table)
	if err != nil {
		return nil, fmt.Errorf("postgres: load schema versions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []ir.RetainedSchemaVersion{}
	for rows.Next() {
		var (
			anchorTok    string
			payload      string
			sourceEngine string
		)
		if err := rows.Scan(&anchorTok, &payload, &sourceEngine); err != nil {
			return nil, fmt.Errorf("postgres: scan schema version: %w", err)
		}
		engine := sourceEngine
		if engine == "" {
			// Legacy v0.70.0 row from before the source_engine column
			// existed: fall back to the applier's own engine name. This
			// preserves the pre-fix behaviour (correct for same-engine
			// streams, which are the only streams that worked pre-fix
			// anyway).
			engine = engineNamePostgres
		}
		out = append(out, ir.RetainedSchemaVersion{
			Anchor:    ir.Position{Engine: engine, Token: anchorTok},
			TableJSON: []byte(payload),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: load schema versions: %w", err)
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
func resolveSchemaVersion(ctx context.Context, q schemaHistoryQueryer, orderer ir.PositionOrderer, schema, streamID, schemaName, table string, p ir.Position) (*ir.Table, error) {
	versions, err := loadRetainedSchemaVersions(ctx, q, schema, streamID, schemaName, table)
	if err != nil {
		return nil, err
	}
	return ir.ResolveSchemaVersion(orderer, versions, p)
}

// schemaHistoryExecQuerier is the read+write surface compactSchemaHistoryBelow
// needs (SELECT to scan candidate rows, DELETE to drop strict-older ones).
// Concrete *sql.DB and *sql.Tx both satisfy it.
//
//nolint:unused // ADR-0049 Chunk D storage-only; consumer wires in chain_prune.
type schemaHistoryExecQuerier interface {
	schemaHistoryExecer
	schemaHistoryQueryer
}

// compactSchemaHistoryBelow deletes every sluice_cdc_schema_history row
// in the named controlSchema whose anchor_position is STRICTLY OLDER
// than floor under the engine's [ir.PositionOrderer] (ADR-0049 Chunk D,
// DP-2 retention floor: min(ADR-0007 safe-point, oldest retained backup
// resume position) — the caller computes the combined floor; this
// primitive applies the delete).
//
// Strict-older means: floor.PositionAtOrAfter(anchor) AND NOT
// anchor.PositionAtOrAfter(floor). Rows AT the floor and AFTER the floor
// remain — the locked design preserves resolve at the floor and leaves
// the loud-floor sentinel intact (a [ResolveSchemaVersion] for a position
// BELOW the oldest retained anchor still wraps [ir.ErrPositionInvalid]
// → ADR-0022 cold-start; compaction can only make a resume fall below
// the floor, which is by construction LOUD, never silent).
//
// Returns the count of rows deleted (operator-facing for `sluice backup
// prune --vv` diagnostics).
//
//nolint:unused // ADR-0049 Chunk D storage-only; consumer wires in chain_prune.
func compactSchemaHistoryBelow(ctx context.Context, exec schemaHistoryExecQuerier, orderer ir.PositionOrderer, schema string, floor ir.Position) (int, error) {
	if orderer == nil {
		return 0, errors.New("postgres: compact schema-history: orderer is nil; ordering is a correctness primitive (loud-failure tenet)")
	}
	tableRef := quoteIdent(schema) + "." + quoteIdent(schemaHistoryTableName)
	sel := "SELECT version_key, anchor_position FROM " + tableRef
	rows, err := exec.QueryContext(ctx, sel)
	if err != nil {
		return 0, fmt.Errorf("postgres: compact schema-history: select: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var toDelete []string
	for rows.Next() {
		var (
			vk        string
			anchorTok string
		)
		if err := rows.Scan(&vk, &anchorTok); err != nil {
			return 0, fmt.Errorf("postgres: compact schema-history: scan: %w", err)
		}
		anchor := ir.Position{Engine: engineNamePostgres, Token: anchorTok}
		floorAtOrAfter, err := orderer.PositionAtOrAfter(floor, anchor)
		if err != nil {
			return 0, fmt.Errorf("postgres: compact schema-history: order floor vs %+v: %w", anchor, err)
		}
		if !floorAtOrAfter {
			continue
		}
		anchorAtOrAfter, err := orderer.PositionAtOrAfter(anchor, floor)
		if err != nil {
			return 0, fmt.Errorf("postgres: compact schema-history: order %+v vs floor: %w", anchor, err)
		}
		if anchorAtOrAfter {
			continue
		}
		toDelete = append(toDelete, vk)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("postgres: compact schema-history: rows iter: %w", err)
	}
	_ = rows.Close()

	if len(toDelete) == 0 {
		return 0, nil
	}
	del := "DELETE FROM " + tableRef + " WHERE version_key = $1"
	for _, vk := range toDelete {
		if _, err := exec.ExecContext(ctx, del, vk); err != nil {
			return 0, fmt.Errorf("postgres: compact schema-history: delete %q: %w", vk, err)
		}
	}
	return len(toDelete), nil
}
