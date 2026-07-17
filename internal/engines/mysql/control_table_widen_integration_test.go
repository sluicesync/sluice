//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Roadmap item 65a integration pins against real MySQL: the
// TEXT → LONGTEXT widen of sluice_cdc_state.source_position and
// sluice_shard_consolidation_lease.anchor_position — legacy-table
// migration with data preserved, fresh-CREATE LONGTEXT, idempotent
// re-ensure, and the payoff: a >64 KB position token round-trips.
// The exactly-once / safe-migrations-refusal shapes are unit-pinned
// in control_table_widen_test.go (fake driver).

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// columnDataType reads information_schema DATA_TYPE for one column of
// one table in the connected database.
func columnDataType(t *testing.T, ctx context.Context, db *sql.DB, table, column string) string {
	t.Helper()
	var dataType string
	if err := db.QueryRowContext(ctx, `
		SELECT DATA_TYPE
		FROM   information_schema.COLUMNS
		WHERE  TABLE_SCHEMA = DATABASE()
		  AND  TABLE_NAME   = ?
		  AND  COLUMN_NAME  = ?
	`, table, column).Scan(&dataType); err != nil {
		t.Fatalf("DATA_TYPE lookup %s.%s: %v", table, column, err)
	}
	return dataType
}

func TestEnsureControlTable_WidensPositionColumnsToLongText(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	// Pre-item-65a shapes: TEXT source_position on the cdc-state table
	// (with a seeded row so the widen's data-preservation property has
	// something to assert against) and TEXT anchor_position on the
	// v0.76.0-shape lease table.
	applyMySQLApplier(t, dsn, "CREATE TABLE `sluice_cdc_state` ("+
		"  stream_id         VARCHAR(255) NOT NULL,"+
		"  source_position   TEXT         NOT NULL,"+
		"  updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"+
		"  stop_requested_at TIMESTAMP    NULL,"+
		"  PRIMARY KEY (stream_id)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"+
		"INSERT INTO `sluice_cdc_state` (stream_id, source_position) VALUES ('legacy-stream', 'legacy-token');"+
		"CREATE TABLE `sluice_shard_consolidation_lease` ("+
		"  target_table_full_name VARCHAR(512) NOT NULL,"+
		"  lease_holder_stream_id VARCHAR(64)  NULL,"+
		"  lease_expires_at       TIMESTAMP    NULL,"+
		"  ddl_text               TEXT         NULL,"+
		"  ddl_checksum           VARCHAR(64)  NULL,"+
		"  applied_schema_version BIGINT       NOT NULL DEFAULT 0,"+
		"  applied_at             TIMESTAMP    NULL,"+
		"  created_at             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,"+
		"  anchor_position        TEXT         NULL,"+
		"  source_engine          TEXT         NULL,"+
		"  PRIMARY KEY (target_table_full_name)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Both position columns widened.
	if got := columnDataType(t, ctx, db, "sluice_cdc_state", "source_position"); got != "longtext" {
		t.Errorf("source_position DATA_TYPE after ensure = %q; want longtext", got)
	}
	if got := columnDataType(t, ctx, db, "sluice_shard_consolidation_lease", "anchor_position"); got != "longtext" {
		t.Errorf("anchor_position DATA_TYPE after ensure = %q; want longtext", got)
	}

	// The legacy row survived the MODIFY with its token intact.
	var token string
	if err := db.QueryRowContext(
		ctx,
		"SELECT source_position FROM `sluice_cdc_state` WHERE stream_id = ?",
		"legacy-stream",
	).Scan(&token); err != nil {
		t.Fatalf("legacy row select: %v", err)
	}
	if token != "legacy-token" {
		t.Errorf("legacy token = %q; want %q", token, "legacy-token")
	}

	// Second ensure is a clean no-op (the detect sees longtext).
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("second EnsureControlTable: %v", err)
	}

	// The payoff the widen exists for: a >64 KB position token (over
	// TEXT's cap) round-trips through the real position-write UPSERT.
	bigToken := strings.Repeat("0aa1b2c3-d4e5-6789-abcd-ef0123456789:1-1000000,", 2048) // ~96 KB
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := writePositionTx(ctx, tx, "", "big-stream", bigToken, "", "", "", 0, upsertRowAlias); err != nil {
		t.Fatalf("writePositionTx with >64KB token: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	got, ok, err := readPosition(ctx, db, "", "big-stream")
	if err != nil || !ok {
		t.Fatalf("readPosition: ok=%v err=%v", ok, err)
	}
	if got != bigToken {
		t.Errorf("round-tripped token differs: len(got)=%d len(want)=%d", len(got), len(bigToken))
	}
}

// TestEnsureControlTable_FreshCreateIsLongText pins the fresh-CREATE
// path: on a database with no pre-existing control tables, both
// position columns are LONGTEXT directly (no widen ALTER needed —
// which is also what keeps a fresh bootstrap clean on a PlanetScale
// safe-migrations branch where the tables arrive via deploy request).
func TestEnsureControlTable_FreshCreateIsLongText(t *testing.T) {
	dsn, cleanup := startMySQLForApplier(t)
	defer cleanup()

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if got := columnDataType(t, ctx, db, "sluice_cdc_state", "source_position"); got != "longtext" {
		t.Errorf("fresh source_position DATA_TYPE = %q; want longtext", got)
	}
	if got := columnDataType(t, ctx, db, "sluice_shard_consolidation_lease", "anchor_position"); got != "longtext" {
		t.Errorf("fresh anchor_position DATA_TYPE = %q; want longtext", got)
	}
}
