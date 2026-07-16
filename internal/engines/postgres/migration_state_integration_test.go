//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the per-target sluice_migrate_state control
// table. Same shape as control_table_integration_test.go but for the
// migration-state surface — round-trip of state rows + JSON
// table_progress encoding.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestMigrationStateStore_RoundTrip writes a state row, reads it
// back, and confirms the JSON map plus phase/error fields land
// untouched.
func TestMigrationStateStore_RoundTrip(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := eng.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenMigrationStateStore: %v", err)
	}
	defer func() { _ = store.Close() }()

	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	want := ir.MigrationState{
		MigrationID: "round-trip",
		Phase:       ir.MigrationPhaseBulkCopy,
		TableProgress: map[string]ir.TableProgress{
			"users":  {State: ir.TableProgressComplete},
			"orders": {State: ir.TableProgressInProgress, LastPK: []any{int64(123)}, RowsCopied: 123},
		},
		LastError: "phase failed: connection reset",
	}
	if err := store.Write(ctx, want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got, ok, err := store.Read(ctx, "round-trip")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !ok {
		t.Fatal("Read ok=false after Write")
	}
	if got.Phase != want.Phase {
		t.Errorf("phase = %q; want %q", got.Phase, want.Phase)
	}
	if got.TableProgress["users"].State != ir.TableProgressComplete {
		t.Errorf("TableProgress[users].State = %q; want complete", got.TableProgress["users"].State)
	}
	if got.TableProgress["orders"].State != ir.TableProgressInProgress {
		t.Errorf("TableProgress[orders].State = %q; want in_progress", got.TableProgress["orders"].State)
	}
	if got.TableProgress["orders"].RowsCopied != 123 {
		t.Errorf("TableProgress[orders].RowsCopied = %d; want 123", got.TableProgress["orders"].RowsCopied)
	}
	if len(got.TableProgress["orders"].LastPK) != 1 {
		t.Errorf("TableProgress[orders].LastPK len = %d; want 1", len(got.TableProgress["orders"].LastPK))
	}
	if got.LastError != want.LastError {
		t.Errorf("LastError = %q; want %q", got.LastError, want.LastError)
	}
	if got.StartedAt.IsZero() {
		t.Error("StartedAt is zero; want server-set timestamp")
	}
}

// TestMigrationStateStore_PreservesStartedAt confirms the
// COALESCE-on-conflict trick: on the second Write for the same
// migration_id, started_at stays at the original value while
// updated_at advances.
func TestMigrationStateStore_PreservesStartedAt(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := eng.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenMigrationStateStore: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}

	first := ir.MigrationState{MigrationID: "preserve", Phase: ir.MigrationPhaseTables}
	if err := store.Write(ctx, first); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	gotFirst, _, err := store.Read(ctx, "preserve")
	if err != nil {
		t.Fatalf("first Read: %v", err)
	}

	// Sleep a beat so the timestamps differ noticeably.
	time.Sleep(1100 * time.Millisecond)

	second := ir.MigrationState{MigrationID: "preserve", Phase: ir.MigrationPhaseBulkCopy}
	if err := store.Write(ctx, second); err != nil {
		t.Fatalf("second Write: %v", err)
	}
	gotSecond, _, err := store.Read(ctx, "preserve")
	if err != nil {
		t.Fatalf("second Read: %v", err)
	}

	if !gotSecond.StartedAt.Equal(gotFirst.StartedAt) {
		t.Errorf("StartedAt changed across writes: %v -> %v", gotFirst.StartedAt, gotSecond.StartedAt)
	}
	if !gotSecond.UpdatedAt.After(gotFirst.UpdatedAt) {
		t.Errorf("UpdatedAt did not advance: %v -> %v", gotFirst.UpdatedAt, gotSecond.UpdatedAt)
	}
	if gotSecond.Phase != ir.MigrationPhaseBulkCopy {
		t.Errorf("phase after second Write = %q; want bulk_copy", gotSecond.Phase)
	}
}

// TestMigrationStateStore_ReadMissingTolerated confirms a read
// against a missing table returns ok=false rather than erroring —
// matches the dry-run / pre-EnsureControlTable inspection shape.
func TestMigrationStateStore_ReadMissingTolerated(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	eng := Engine{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := eng.OpenMigrationStateStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenMigrationStateStore: %v", err)
	}
	defer func() { _ = store.Close() }()

	// Note: EnsureControlTable deliberately NOT called.
	_, ok, err := store.Read(ctx, "absent")
	if err != nil {
		t.Errorf("Read on missing table errored: %v", err)
	}
	if ok {
		t.Error("Read ok=true on missing table")
	}
}

// TestMigrationStateStore_TimezoneProof is the audit 2026-07-16 M1.3
// pin (the task-#44 lease-TZ class, applied to migrate-state
// timestamps): started_at/updated_at are naive TIMESTAMP columns that
// pgx reads back as UTC digits, so they must be WRITTEN as UTC digits
// — timezone('utc', now()), never CURRENT_TIMESTAMP's session-TZ cast.
// Pinned on a database whose timezone is set to America/Los_Angeles
// (UTC-behind: pre-fix the read-back lagged real time by hours, so the
// backfill concurrent-run heartbeat guard saw a LIVE walk as stale and
// let a second walk interleave) AND Europe/Berlin (UTC-ahead: the
// read-back led real time, so the guard over-refused for hours after
// a finished run) — both skew directions, per-database GUC so every
// pooled connection the store opens inherits it.
func TestMigrationStateStore_TimezoneProof(t *testing.T) {
	for _, tc := range []struct {
		name, db, tz string
	}{
		{"utc-behind-server", "tz_la_db", "America/Los_Angeles"},
		{"utc-ahead-server", "tz_berlin_db", "Europe/Berlin"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn, cleanup := newSharedPGDB(t, tc.db)
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			// Skew the database's timezone BEFORE the store opens its
			// pool: per-database GUCs apply at connection start.
			admin, err := sql.Open("pgx", dsn)
			if err != nil {
				t.Fatalf("open admin: %v", err)
			}
			if _, err := admin.ExecContext(ctx,
				"ALTER DATABASE "+tc.db+" SET timezone = '"+tc.tz+"'"); err != nil {
				_ = admin.Close()
				t.Fatalf("set database timezone: %v", err)
			}
			_ = admin.Close()

			eng := Engine{}
			store, err := eng.OpenMigrationStateStore(ctx, dsn)
			if err != nil {
				t.Fatalf("OpenMigrationStateStore: %v", err)
			}
			defer func() { _ = store.Close() }()
			if err := store.EnsureControlTable(ctx); err != nil {
				t.Fatalf("EnsureControlTable: %v", err)
			}

			// Exercise BOTH write paths: the header upsert and the
			// per-table progress upsert (the backfill heartbeat rides on
			// whichever is most recent).
			state := ir.MigrationState{MigrationID: "tz-proof", Phase: ir.MigrationPhaseBulkCopy}
			if err := store.Write(ctx, state); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := store.WriteTableProgress(ctx, "tz-proof", "users",
				ir.TableProgress{State: ir.TableProgressInProgress, RowsCopied: 1}); err != nil {
				t.Fatalf("WriteTableProgress: %v", err)
			}

			got, ok, err := store.Read(ctx, "tz-proof")
			if err != nil || !ok {
				t.Fatalf("Read: ok=%v err=%v", ok, err)
			}
			// The heartbeat guard's exact computation: a just-written row
			// must read as FRESH (age ≈ 0) regardless of the server TZ.
			// Pre-fix: LA read ~+7h stale, Berlin ~-2h (in the future).
			for _, ts := range []struct {
				name string
				at   time.Time
			}{{"updated_at", got.UpdatedAt}, {"started_at", got.StartedAt}} {
				age := time.Since(ts.at)
				if age < -time.Minute || age > 2*time.Minute {
					t.Errorf("%s under timezone=%s reads back with age %s; want ~0 (naive-TIMESTAMP digits must be UTC)",
						ts.name, tc.tz, age.Round(time.Second))
				}
			}
		})
	}
}
