//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared/tablesetstest"
	"sluicesync.dev/sluice/internal/ir"
)

// TestPendingAddedTablesStore_RoundTrip pins the MySQL add-table pending
// record (INCLUDE-TABLE-SCOPE-DRIFT) against a real server, from a control
// table a pre-column binary created — a repeated identical Record included,
// which MySQL's changed-rows RowsAffected reports as zero rows (see
// [tablesetstest.PendingRoundTrip]) — ending NULL, not an empty string.
func TestPendingAddedTablesStore_RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, applier := tableSetsStoreFixture(ctx, t)
	tablesetstest.PendingRoundTrip(ctx, t, applier.(ir.PendingAddedTablesStore), errStreamNotFound)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var stored *string
	if err := db.QueryRowContext(ctx, "SELECT pending_added_tables FROM sluice_cdc_state WHERE stream_id = 's1'").Scan(&stored); err != nil || stored != nil {
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

// TestMySQLServerNowMillis_DSTFallBack is R4-3: the pending-add clock reads
// the true instant under a DST session time_zone in the fall-back hour.
// `SET timestamp` pins the session's clock to 06:30:00.250 UTC on
// 2026-11-01 — 01:30:00.250 EST in America/New_York, the SECOND occurrence of
// that local time (05:30 UTC was 01:30 EDT). The expression must return that
// instant to the millisecond.
func TestMySQLServerNowMillis_DSTFallBack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn, _ := tableSetsStoreFixture(ctx, t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	// The test image loads no named zones, so install one with New York's
	// 2026 rules (EDT from 2026-03-08 07:00 UTC, EST from 2026-11-01 06:00
	// UTC) straight into the mysql.time_zone tables, as root.
	for _, q := range []string{
		"DELETE FROM mysql.time_zone_name WHERE Name = 'Sluice/FallBack'", // a -count=N re-run on the shared server
		"INSERT INTO mysql.time_zone (Use_leap_seconds) VALUES ('N')",
		"SET @tz = LAST_INSERT_ID()",
		"INSERT INTO mysql.time_zone_name (Name, Time_zone_id) VALUES ('Sluice/FallBack', @tz)",
		"INSERT INTO mysql.time_zone_transition_type (Time_zone_id, Transition_type_id, Offset, Is_DST, Abbreviation) VALUES " +
			"(@tz, 0, -18000, 0, 'EST'), (@tz, 1, -14400, 1, 'EDT')",
		"INSERT INTO mysql.time_zone_transition (Time_zone_id, Transition_time, Transition_type_id) VALUES " +
			"(@tz, 1772953200, 1), (@tz, 1793512800, 0)",
		"SET time_zone = 'Sluice/FallBack'",
	} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	instant := time.Date(2026, 11, 1, 6, 30, 0, 250_000_000, time.UTC)
	if _, err := conn.ExecContext(ctx, "SET timestamp = ?", float64(instant.UnixMicro())/1e6); err != nil {
		t.Fatal(err)
	}
	var local string
	if err := conn.QueryRowContext(ctx, "SELECT CAST(NOW(3) AS CHAR)").Scan(&local); err != nil || local != "2026-11-01 01:30:00.250" {
		t.Fatalf("session local time %q (%v); want the ambiguous 2026-11-01 01:30:00.250", local, err)
	}
	var got int64
	if err := conn.QueryRowContext(ctx, mysqlServerNowMillis).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != instant.UnixMilli() {
		t.Fatalf("server now = %d (%v); want %d (%v)", got, time.UnixMilli(got).UTC(), instant.UnixMilli(), instant)
	}
	// Anti-vacuity: the cell must reach the ambiguity. The round-4
	// expression reads this instant an hour early (measured: 05:30:00.250).
	var old float64
	if err := conn.QueryRowContext(ctx, "SELECT UNIX_TIMESTAMP(NOW(6)) * 1000").Scan(&old); err != nil {
		t.Fatal(err)
	}
	if int64(old) == instant.UnixMilli() {
		t.Fatal("UNIX_TIMESTAMP(NOW(6)) read the instant correctly, so this cell no longer reaches the fall-back ambiguity")
	}
	t.Logf("the round-4 expression read %v", time.UnixMilli(int64(old)).UTC())
}

// tableSetsStoreFixture starts a server holding a control table a pre-column
// binary created, with stream s1's row, and opens an applier on it.
func tableSetsStoreFixture(ctx context.Context, t *testing.T) (string, ir.ChangeApplier) {
	t.Helper()
	dsn, cleanup := startMySQLForApplier(t)
	t.Cleanup(cleanup)
	applyMySQLApplier(t, dsn, "CREATE TABLE `sluice_cdc_state` ("+
		"stream_id VARCHAR(255) NOT NULL, "+
		"source_position LONGTEXT NOT NULL, "+
		"updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, "+
		"PRIMARY KEY (stream_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	applyMySQLApplier(t, dsn, "INSERT INTO `sluice_cdc_state` (stream_id, source_position) VALUES ('s1', 'tok')")
	applier, err := Engine{Flavor: FlavorVanilla}.OpenChangeApplier(ctx, dsn)
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
