//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared/tablesetstest"
	"sluicesync.dev/sluice/internal/ir"
)

// TestPendingAddedTablesStore_RoundTrip pins the Postgres add-table pending
// record (INCLUDE-TABLE-SCOPE-DRIFT) against a real server, from a control
// table a pre-column binary created: no record reads as empty, Record adds
// the column itself, every name and both timestamps round-trip, Record of a
// missing stream row is a loud error, and Clear is exact and idempotent —
// ending NULL, not an empty string.
func TestPendingAddedTablesStore_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, applier := tableSetsStoreFixture(ctx, t)
	tablesetstest.PendingRoundTrip(ctx, t, applier.(ir.PendingAddedTablesStore), errStreamNotFound)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var stored *string
	if err := db.QueryRowContext(ctx, `SELECT pending_added_tables FROM sluice_cdc_state WHERE stream_id = 's1'`).Scan(&stored); err != nil || stored != nil {
		t.Fatalf("an emptied record is stored as NULL; got %v, %v", stored, err)
	}
}

// TestEstablishedTablesStore_RoundTrip pins the established/dropped record
// against a real server (see [tablesetstest.EstablishedRoundTrip]).
func TestEstablishedTablesStore_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, applier := tableSetsStoreFixture(ctx, t)
	tablesetstest.EstablishedRoundTrip(ctx, t, applier.(tablesetstest.Store), errStreamNotFound)
}

// tableSetsStoreFixture starts a server holding a control table a pre-column
// binary created, with stream s1's row, and opens an applier on it.
func tableSetsStoreFixture(ctx context.Context, t *testing.T) (string, ir.ChangeApplier) {
	t.Helper()
	dsn, cleanup := startPostgresForApplier(t)
	t.Cleanup(cleanup)
	applyPGApplier(t, dsn, `
		CREATE TABLE "public"."sluice_cdc_state" (
			stream_id       VARCHAR(255) NOT NULL,
			source_position TEXT         NOT NULL,
			updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (stream_id)
		);
		INSERT INTO "public"."sluice_cdc_state" (stream_id, source_position) VALUES ('s1', 'tok');
	`)
	applier, err := Engine{}.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	t.Cleanup(func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	})
	return dsn, applier
}
