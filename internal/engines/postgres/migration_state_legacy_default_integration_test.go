//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// TestMigrateState_LegacySessionClockDefault_UnderTokyo pins GC-40 LOW-1/2 on
// a real Postgres whose database zone is Asia/Tokyo. The migrate-state writes
// leave started_at/updated_at to the column DEFAULT (the Neki router note in
// newMigrationStateStore); a table created before v0.99.263 defaults to
// CURRENT_TIMESTAMP, which stores the session zone's digits — nine hours in
// the future here — and the backfill concurrent-run guard and the cold-start
// ages then read an impossible reading.
//
// The fixture is what the older binary left behind: the current tables with
// their timestamp defaults set back to CURRENT_TIMESTAMP (the exact default
// fe3320da's CREATE carried). The independent expected value is the test
// process's clock: a row written now must age ≈0.
//
// Arms: the premise (the legacy default really stores +9h); the owner's
// ensure re-points the defaults and the next write ages ≈0; a role that may
// not ALTER is refused on the Tokyo session, and only WARNed on a UTC one.
func TestMigrateState_LegacySessionClockDefault_UnderTokyo(t *testing.T) {
	adminDSN, cleanup := newSharedPGDB(t, "gc40_legacy_ms_db")
	defer cleanup()
	host, port, _, _ := ensureSharedPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatalf("open admin: %v", err)
	}
	defer func() { _ = admin.Close() }()
	exec := func(stmt string) {
		t.Helper()
		if _, err := admin.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("admin exec %q: %v", stmt, err)
		}
	}
	exec(`ALTER DATABASE gc40_legacy_ms_db SET timezone = 'Asia/Tokyo'`)
	for _, t := range migrateStateTables("public") {
		exec(t.create)
		for _, c := range t.columns {
			exec(t.addColumnDDL(c))
		}
	}
	legacy := func() {
		t.Helper()
		exec(`ALTER TABLE public.sluice_migrate_state ALTER COLUMN started_at SET DEFAULT CURRENT_TIMESTAMP`)
		exec(`ALTER TABLE public.sluice_migrate_state ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP`)
		exec(`ALTER TABLE public.sluice_migrate_table_progress ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP`)
	}
	legacy()

	// A fresh pool: the ALTER DATABASE zone applies to new sessions only.
	open := func(dsn string) *MigrationStateStore {
		t.Helper()
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return newMigrationStateStore(db, "public", false, "")
	}
	age := func(s *MigrationStateStore, id string) time.Duration {
		t.Helper()
		if err := s.Write(ctx, ir.MigrationState{MigrationID: id, Phase: "backfill"}); err != nil {
			t.Fatalf("write %s: %v", id, err)
		}
		st, ok, err := s.Read(ctx, id)
		if err != nil || !ok {
			t.Fatalf("read %s: ok=%v err=%v", id, ok, err)
		}
		return time.Since(st.UpdatedAt)
	}
	owner := open(adminDSN)

	// The premise: on this session zone the legacy default is 9h ahead.
	if a := age(owner, "legacy-premise"); a > -8*time.Hour || a < -10*time.Hour {
		t.Fatalf("legacy CURRENT_TIMESTAMP row ages %v; want about -9h — the fixture is not the defect", a.Round(time.Second))
	}

	t.Run("owner re-points the defaults", func(t *testing.T) {
		if err := owner.EnsureControlTable(ctx); err != nil {
			t.Fatalf("ensure: %v", err)
		}
		if a := age(owner, "after-ensure"); a < -time.Minute || a > time.Minute {
			t.Errorf("a row written after the ensure ages %v; want ≈0", a.Round(time.Second))
		}
		var def string
		if err := admin.QueryRowContext(ctx, `SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid)
			FROM pg_catalog.pg_attrdef d JOIN pg_catalog.pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
			WHERE d.adrelid = 'public.sluice_migrate_table_progress'::regclass AND a.attname = 'updated_at'`).Scan(&def); err != nil ||
			!strings.Contains(def, "timezone('utc'") {
			t.Errorf("progress updated_at default = %q (err %v); want the UTC expression", def, err)
		}
	})

	role := fmt.Sprintf("gc40ms_%d", time.Now().UnixNano())
	exec(`CREATE ROLE ` + role + ` LOGIN PASSWORD 'pw'`)
	defer func() {
		_, _ = admin.ExecContext(context.Background(), `DROP OWNED BY `+role+`; DROP ROLE IF EXISTS `+role)
	}()
	exec(`REVOKE CREATE ON SCHEMA public FROM PUBLIC`)
	exec(`GRANT USAGE ON SCHEMA public TO ` + role)
	exec(`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ` + role)
	roleDSN := sharedPGDSN(host, port, role, "pw", "gc40_legacy_ms_db")

	t.Run("a role that cannot ALTER is refused on a Tokyo session", func(t *testing.T) {
		legacy()
		err := open(roleDSN).EnsureControlTable(ctx)
		for _, want := range []string{ControlTableDDLRequiredMarker, "session-clock DEFAULT on", "SET DEFAULT", "42501"} {
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("ensure = %v; want a refusal containing %q", err, want)
			}
		}
	})

	t.Run("a role that cannot ALTER is only warned on a UTC session", func(t *testing.T) {
		legacy()
		exec(`ALTER ROLE ` + role + ` IN DATABASE gc40_legacy_ms_db SET timezone = 'UTC'`)
		logs := &logcapture.Buffer{}
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
		defer slog.SetDefault(prev)
		if err := open(roleDSN).EnsureControlTable(ctx); err != nil {
			t.Fatalf("ensure on a UTC session: %v; want a WARN and a start", err)
		}
		if !strings.Contains(logs.String(), ControlTableDDLRequiredMarker) {
			t.Errorf("no %s WARN:\n%s", ControlTableDDLRequiredMarker, logs.String())
		}
	})
}
