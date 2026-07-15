// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"sluicesync.dev/sluice/internal/ir"
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
type schemaHistoryExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// schemaHistoryQueryer is the minimal read surface shared by *sql.DB
// and *sql.Tx.
type schemaHistoryQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// ensureSchemaHistoryTable creates the per-target
// sluice_cdc_schema_history table if it doesn't exist. Idempotent —
// second-and-later calls detect the table and issue no DDL at all
// (detect-then-create, the safe-migrations constraint — see
// [ensureControlTable]). ADDITIVE: it never touches sluice_cdc_state
// or any existing data; a target that already has cdc-state rows is
// unaffected.
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
// source_engine is the engine tag of the anchor token's producer (the
// source-side engine that emitted the [ir.SchemaSnapshot]). It is
// NULLABLE so a v0.70.0-shape table that pre-dates the column
// upgrade-migrates cleanly via the detect-then-ALTER below; on the
// read path a NULL value falls back to the applier's own engine name
// (the pre-fix behaviour, correct for same-engine streams).
// Persisting it is what fixes Bug 78: cross-engine chain-restore
// preserves the source token's engine identity through the
// [ir.PositionOrderer] strict engine-tag check.
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
func ensureSchemaHistoryTable(ctx context.Context, db *sql.DB, controlKeyspace string) error {
	exists, err := controlTableExists(ctx, db, controlKeyspace, schemaHistoryTableName)
	if err != nil {
		return fmt.Errorf("mysql: ensure schema-history table: %w", err)
	}
	if !exists {
		ddl := schemaHistoryTableDDL(controlKeyspace)
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("mysql: ensure schema-history table: %w", wrapControlTableBootstrapError(wrapDDLError(err), ddl))
		}
	}
	// Migration path for v0.70.0 deployments whose
	// sluice_cdc_schema_history table pre-dates the source_engine column
	// (Bug 78 fix, v0.70.1). Detect-then-ALTER (NOT ADD COLUMN IF NOT
	// EXISTS) to stay portable across MySQL 8.0.x versions older than
	// 8.0.29, mirroring ensureLiveAddedTablesColumn /
	// ensureCrossEngineParityColumn in control_table.go. NULLABLE:
	// legacy rows have NULL, the read path falls back to
	// engineNameMySQL (the pre-fix behaviour, correct for same-engine
	// streams).
	return ensureSchemaHistorySourceEngineColumn(ctx, db, controlKeyspace)
}

// schemaHistoryTableDDL renders the ADR-0049 schema-history CREATE
// statement — single-sourced between [ensureSchemaHistoryTable] and
// the bootstrap printer ([Engine.ControlTableDDL], ADR-0165).
func schemaHistoryTableDDL(controlKeyspace string) string {
	return `CREATE TABLE IF NOT EXISTS ` + controlTableRef(controlKeyspace, schemaHistoryTableName) + ` (
	version_key     CHAR(64)     NOT NULL,
	stream_id       VARCHAR(255) NOT NULL,
	schema_name     VARCHAR(255) NOT NULL,
	table_name      VARCHAR(255) NOT NULL,
	anchor_position LONGTEXT     NOT NULL,
	ir_schema_json  LONGTEXT     NOT NULL,
	source_engine   VARCHAR(64)  NULL,
	created_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (version_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`
}

// ensureSchemaHistorySourceEngineColumn adds the source_engine column
// to an existing sluice_cdc_schema_history table when missing. Same
// detect-then-ALTER shape as ensureCrossEngineParityColumn — keeps the
// migration portable across MySQL 8.0.x versions older than 8.0.29
// (which is where ADD COLUMN IF NOT EXISTS landed). Bug 78 fix.
func ensureSchemaHistorySourceEngineColumn(ctx context.Context, db *sql.DB, controlKeyspace string) error {
	schemaRHS, schemaArgs := controlSchemaPredicate(controlKeyspace)
	checkQ := "SELECT COUNT(*) FROM information_schema.COLUMNS " +
		"WHERE TABLE_SCHEMA = " + schemaRHS + " AND TABLE_NAME = ? AND COLUMN_NAME = 'source_engine'"
	args := append(append([]any{}, schemaArgs...), schemaHistoryTableName)
	var n int
	if err := db.QueryRowContext(ctx, checkQ, args...).Scan(&n); err != nil {
		return fmt.Errorf("mysql: ensure schema-history table: detect source_engine: %w", err)
	}
	if n > 0 {
		return nil
	}
	alter := "ALTER TABLE " + controlTableRef(controlKeyspace, schemaHistoryTableName) + " ADD COLUMN source_engine VARCHAR(64) NULL"
	if _, err := db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("mysql: ensure schema-history table: add source_engine: %w", wrapControlTableBootstrapError(err, alter))
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
func writeSchemaVersion(ctx context.Context, exec schemaHistoryExecer, controlKeyspace, streamID, schema, table string, anchor ir.Position, t *ir.Table) error {
	if t == nil {
		return errors.New("mysql: write schema version: table is nil")
	}
	payload, err := ir.MarshalTable(t)
	if err != nil {
		return fmt.Errorf("mysql: write schema version: marshal table: %w", err)
	}
	vk := ir.SchemaVersionKey(streamID, schema, table, anchor.Token)
	// source_engine carries anchor.Engine — the engine identity of the
	// anchor token's producer. NULLIF on empty preserves the legacy
	// (pre-Bug-78) NULL shape for any future caller that omits the
	// engine tag; today the applier dispatch always passes a populated
	// anchor (from the in-stream SchemaSnapshot's Position.Engine), so
	// the empty case is defensive. COALESCE on the conflict path means
	// a non-empty incoming value overwrites and an empty value
	// preserves the existing row's tag.
	ref := controlTableRef(controlKeyspace, schemaHistoryTableName)
	q := "INSERT INTO " + ref + " " +
		"(version_key, stream_id, schema_name, table_name, anchor_position, ir_schema_json, source_engine) " +
		"VALUES (?, ?, ?, ?, ?, ?, NULLIF(?, '')) " +
		"AS new ON DUPLICATE KEY UPDATE " +
		"ir_schema_json = new.ir_schema_json, " +
		"source_engine = COALESCE(new.source_engine, " + ref + ".source_engine)"
	if _, err := exec.ExecContext(ctx, q, vk, streamID, schema, table, anchor.Token, string(payload), anchor.Engine); err != nil {
		return fmt.Errorf("mysql: write schema version: %w", err)
	}
	return nil
}

// loadRetainedSchemaVersions reads every retained
// (anchor_position, ir_schema_json, source_engine) tuple for the
// (streamID, schema, table) key. The Engine field of each
// reconstructed [ir.Position] is the persisted source_engine — the
// engine that PRODUCED the anchor token, NOT the applier's own engine
// (Bug 78: a cross-engine chain-restore persisted a PG LSN token under
// a MySQL applier; stamping it engineNameMySQL on read made the
// engine-strict [ir.PositionOrderer] reject it during cache-prime).
//
// Legacy v0.70.0 rows have source_engine NULL (the column did not
// exist before the Bug 78 fix); a NULL is treated as "use the
// applier's own engine name" — i.e. the pre-fix behaviour. That is
// correct for same-engine streams (target == source), which are the
// only streams that worked pre-fix.
func loadRetainedSchemaVersions(ctx context.Context, q schemaHistoryQueryer, controlKeyspace, streamID, schema, table string) ([]ir.RetainedSchemaVersion, error) {
	sel := "SELECT anchor_position, ir_schema_json, COALESCE(source_engine, '') FROM " + controlTableRef(controlKeyspace, schemaHistoryTableName) + " " +
		"WHERE stream_id = ? AND schema_name = ? AND table_name = ?"
	rows, err := q.QueryContext(ctx, sel, streamID, schema, table)
	if err != nil {
		return nil, fmt.Errorf("mysql: load schema versions: %w", err)
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
			return nil, fmt.Errorf("mysql: scan schema version: %w", err)
		}
		engine := sourceEngine
		if engine == "" {
			// Legacy v0.70.0 row from before the source_engine column
			// existed: fall back to the applier's own engine name. This
			// preserves the pre-fix behaviour (correct for same-engine
			// streams, which are the only streams that worked pre-fix
			// anyway).
			engine = engineNameMySQL
		}
		out = append(out, ir.RetainedSchemaVersion{
			Anchor:    ir.Position{Engine: engine, Token: anchorTok},
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
func resolveSchemaVersion(ctx context.Context, q schemaHistoryQueryer, controlKeyspace string, orderer ir.PositionOrderer, streamID, schema, table string, p ir.Position) (*ir.Table, error) {
	versions, err := loadRetainedSchemaVersions(ctx, q, controlKeyspace, streamID, schema, table)
	if err != nil {
		return nil, err
	}
	return ir.ResolveSchemaVersion(orderer, versions, p)
}

// schemaHistoryExecQuerier is the read+write surface compactSchemaHistoryBelow
// needs (SELECT to scan candidate rows, DELETE to drop strict-older ones).
// Concrete *sql.DB and *sql.Tx both satisfy it.
type schemaHistoryExecQuerier interface {
	schemaHistoryExecer
	schemaHistoryQueryer
}

// compactSchemaHistoryBelow deletes every sluice_cdc_schema_history row
// whose anchor_position is STRICTLY OLDER than floor under the engine's
// [ir.PositionOrderer] (ADR-0049 Chunk D, DP-2 retention floor:
// min(ADR-0007 safe-point, oldest retained backup resume position) — the
// caller computes the combined floor; this primitive applies the delete).
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
func compactSchemaHistoryBelow(ctx context.Context, exec schemaHistoryExecQuerier, controlKeyspace string, orderer ir.PositionOrderer, floor ir.Position) (int, error) {
	if orderer == nil {
		return 0, errors.New("mysql: compact schema-history: orderer is nil; ordering is a correctness primitive (loud-failure tenet)")
	}
	// Scan every row's (version_key, anchor_position, source_engine).
	// The PK is the fixed-width SHA-256 surrogate; we delete by it so
	// multiple equal-length keys can't alias on a prefix.
	//
	// Bug 78 (extended in review): source_engine joins the SELECT so
	// the orderer compares anchors against the floor using the engine
	// that PRODUCED the anchor token, not this applier's own engine.
	// Same fix shape as loadRetainedSchemaVersions; this compactor is
	// the only other on-disk-anchor consumer. Currently nolint:unused
	// (Chunk D storage-only, consumer wires in chain_prune later) so
	// the bug is latent today, but the class is the same — fix both
	// per the pin-the-class discipline. NULL row (pre-v0.70.1
	// storage) falls back to engineNameMySQL, the pre-fix behaviour.
	sel := "SELECT version_key, anchor_position, COALESCE(source_engine, '') FROM " + controlTableRef(controlKeyspace, schemaHistoryTableName)
	rows, err := exec.QueryContext(ctx, sel)
	if err != nil {
		return 0, fmt.Errorf("mysql: compact schema-history: select: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var toDelete []string
	for rows.Next() {
		var (
			vk           string
			anchorTok    string
			sourceEngine string
		)
		if err := rows.Scan(&vk, &anchorTok, &sourceEngine); err != nil {
			return 0, fmt.Errorf("mysql: compact schema-history: scan: %w", err)
		}
		if sourceEngine == "" {
			sourceEngine = engineNameMySQL
		}
		anchor := ir.Position{Engine: sourceEngine, Token: anchorTok}
		floorAtOrAfter, err := orderer.PositionAtOrAfter(floor, anchor)
		if err != nil {
			return 0, fmt.Errorf("mysql: compact schema-history: order floor vs %+v: %w", anchor, err)
		}
		if !floorAtOrAfter {
			// anchor > floor (or incomparable): keep.
			continue
		}
		anchorAtOrAfter, err := orderer.PositionAtOrAfter(anchor, floor)
		if err != nil {
			return 0, fmt.Errorf("mysql: compact schema-history: order %+v vs floor: %w", anchor, err)
		}
		if anchorAtOrAfter {
			// anchor == floor (mutually at-or-after): keep — locked
			// design "leaves the at-floor row intact" + ResolveSchemaVersion
			// at the at-floor position must still succeed.
			continue
		}
		toDelete = append(toDelete, vk)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("mysql: compact schema-history: rows iter: %w", err)
	}
	_ = rows.Close()

	if len(toDelete) == 0 {
		return 0, nil
	}
	del := "DELETE FROM " + controlTableRef(controlKeyspace, schemaHistoryTableName) + " WHERE version_key = ?"
	for _, vk := range toDelete {
		if _, err := exec.ExecContext(ctx, del, vk); err != nil {
			return 0, fmt.Errorf("mysql: compact schema-history: delete %q: %w", vk, err)
		}
	}
	return len(toDelete), nil
}
