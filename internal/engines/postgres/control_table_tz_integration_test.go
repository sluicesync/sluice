//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/appliershared"
)

// TestControlTables_TimestampsAreUTC_UnderANonUTCDatabaseZone pins GC-39
// item 2 (the naive-TIMESTAMP freshness class, sibling of
// TestMigrationStateStore_TimezoneProof): the control tables' timestamp
// columns are naive TIMESTAMP, pgx reads them back as UTC, and the callers
// age them against time.Now() (sync health --max-stale-seconds,
// sluice_seconds_since_last_apply, sync status). A CURRENT_TIMESTAMP write
// stores the SESSION zone's wall-clock digits, so on a database whose
// default zone is behind UTC every stream read hours stale (false alarms)
// and on one AHEAD of UTC a stalled stream read as fresh or even
// negative-aged — the stall alarm failing open, silently. The writes now
// go through pg_catalog.timezone('utc', pg_catalog.now()).
//
// The independent expected value is the test process's own clock: a row
// written moments ago must age ≈ 0 whatever the database zone. Both skew
// directions, per-database GUC so every pooled connection inherits it.
func TestControlTables_TimestampsAreUTC_UnderANonUTCDatabaseZone(t *testing.T) {
	for _, tc := range []struct {
		name, db, tz string
	}{
		{"utc-behind-server", "ctl_tz_la_db", "America/Los_Angeles"},
		{"utc-ahead-server", "ctl_tz_tokyo_db", "Asia/Tokyo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn, cleanup := newSharedPGDB(t, tc.db)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			admin, err := sql.Open("pgx", dsn)
			if err != nil {
				t.Fatalf("open admin: %v", err)
			}
			if _, err := admin.ExecContext(ctx, "ALTER DATABASE "+tc.db+" SET timezone = '"+tc.tz+"'"); err != nil {
				_ = admin.Close()
				t.Fatalf("set database timezone: %v", err)
			}
			_ = admin.Close()

			db, err := sql.Open("pgx", dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer func() { _ = db.Close() }()
			var zone string
			if err := db.QueryRowContext(ctx, "SHOW timezone").Scan(&zone); err != nil || zone != tc.tz {
				t.Fatalf("session zone = %q (err %v); want %q — the test is not measuring a skewed server", zone, err, tc.tz)
			}

			if err := ensureControlTable(ctx, db, "public"); err != nil {
				t.Fatalf("ensureControlTable: %v", err)
			}
			if err := ensureSkippedTablesTable(ctx, db, "public"); err != nil {
				t.Fatalf("ensureSkippedTablesTable: %v", err)
			}
			if err := ensureShardConsolidationLeaseTable(ctx, db, "public"); err != nil {
				t.Fatalf("ensureShardConsolidationLeaseTable: %v", err)
			}

			const maxSkew = 2 * time.Minute
			fresh := func(what string, at time.Time) {
				t.Helper()
				age := time.Since(at)
				if age < -maxSkew || age > maxSkew {
					t.Errorf("%s written just now ages %v on a %s database; want ≈0 — the column holds the session zone's digits, not UTC",
						what, age.Round(time.Second), tc.tz)
				}
			}

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if err := writePositionTx(ctx, tx, "public", "tz-stream", "tok", "", "", "", "", "", 0); err != nil {
				t.Fatalf("writePositionTx: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("commit: %v", err)
			}
			streams, err := listStreams(ctx, db, "public", "postgres")
			if err != nil || len(streams) != 1 {
				t.Fatalf("listStreams = %v, %v; want one row", streams, err)
			}
			fresh("sluice_cdc_state.updated_at (position upsert)", streams[0].UpdatedAt)

			// The DDL DEFAULT, not only the upsert: a row inserted without
			// updated_at must take the UTC default too.
			if _, err := db.ExecContext(ctx, "INSERT INTO "+controlTableRef("public")+" (stream_id, source_position) VALUES ('tz-default', 'tok')"); err != nil {
				t.Fatalf("insert relying on the updated_at default: %v", err)
			}
			streams, err = listStreams(ctx, db, "public", "postgres")
			if err != nil {
				t.Fatalf("listStreams: %v", err)
			}
			for _, s := range streams {
				if s.StreamID == "tz-default" {
					fresh("sluice_cdc_state.updated_at (DDL default)", s.UpdatedAt)
				}
			}

			if err := upsertSkippedTable(ctx, db, "public", "tz-stream", "t1", appliershared.SkipLedgerEntry{Count: 1, FirstPos: "a", LastPos: "a"}); err != nil {
				t.Fatalf("upsertSkippedTable (insert): %v", err)
			}
			if err := upsertSkippedTable(ctx, db, "public", "tz-stream", "t1", appliershared.SkipLedgerEntry{Count: 1, FirstPos: "b", LastPos: "b"}); err != nil {
				t.Fatalf("upsertSkippedTable (update): %v", err)
			}
			skips, err := listSkippedTables(ctx, db, "public")
			if err != nil || len(skips) != 1 {
				t.Fatalf("listSkippedTables = %v, %v; want one row", skips, err)
			}
			fresh("sluice_skipped_tables.first_skipped_at", skips[0].FirstSkippedAt)
			fresh("sluice_skipped_tables.last_skipped_at", skips[0].LastSkippedAt)

			acquired, _, err := tryAcquireShardLease(ctx, db, "public", "public.tz_t", "tz-stream", time.Now().UTC().Add(time.Minute))
			if err != nil || !acquired {
				t.Fatalf("tryAcquireShardLease = %v, %v; want acquired", acquired, err)
			}
			finalized, err := finalizeShardLeaseApply(ctx, db, "public", "public.tz_t", "tz-stream", "ALTER TABLE tz_t ADD COLUMN c INT", "sum", 1, "", "")
			if err != nil || !finalized {
				t.Fatalf("finalizeShardLeaseApply = %v, %v; want finalized", finalized, err)
			}
			lease, ok, err := selectShardLease(ctx, db, "public", "public.tz_t")
			if err != nil || !ok || !lease.AppliedAt.Valid {
				t.Fatalf("selectShardLease = %+v, %v, %v; want an applied lease", lease, ok, err)
			}
			fresh("shard lease applied_at", lease.AppliedAt.Time)

			// created_at is never read back by a caller; hold the SQL value
			// itself to the same contract so the next reader cannot inherit
			// a skewed column.
			var createdAge float64
			if err := db.QueryRowContext(ctx,
				"SELECT EXTRACT(EPOCH FROM (pg_catalog.timezone('utc', pg_catalog.now()) - created_at)) FROM "+
					shardLeaseTableRef("public")+" WHERE target_table_full_name = 'public.tz_t'").Scan(&createdAge); err != nil {
				t.Fatalf("read created_at: %v", err)
			}
			if createdAge < -maxSkew.Seconds() || createdAge > maxSkew.Seconds() {
				t.Errorf("shard lease created_at is %.0fs off UTC now on a %s database; want ≈0", createdAge, tc.tz)
			}

			if err := requestStop(ctx, db, "public", "tz-stream"); err != nil {
				t.Fatalf("requestStop: %v", err)
			}
			var stopAge float64
			if err := db.QueryRowContext(ctx,
				"SELECT EXTRACT(EPOCH FROM (pg_catalog.timezone('utc', pg_catalog.now()) - stop_requested_at)) FROM "+
					controlTableRef("public")+" WHERE stream_id = 'tz-stream'").Scan(&stopAge); err != nil {
				t.Fatalf("read stop_requested_at: %v", err)
			}
			if stopAge < -maxSkew.Seconds() || stopAge > maxSkew.Seconds() {
				t.Errorf("stop_requested_at is %.0fs off UTC now on a %s database; want ≈0", stopAge, tc.tz)
			}
		})
	}
}
