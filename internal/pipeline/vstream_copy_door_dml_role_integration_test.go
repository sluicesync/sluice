//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

// TestStreamer_VStreamCopyDoor_ControlTablesDMLOnlyRole pins F6 of the Bug 327
// review on a real Postgres target, in the style of
// TestControlTables_DMLOnlyRole: a login role that holds only DML on the
// control tables, and no CREATE on their schema, reaches the
// VSTREAM-COPY-COMPLETION-UNPROVEN door with sluice_migrate_state missing, or
// present but out of date. Out of date, the header read's EnsureControlTable
// needs DDL the role cannot run, and that refusal must carry the no-restart sentinel (a
// fleet supervisor would otherwise restart into it forever) and name the
// `sluice control-tables ddl` remedy — with and without --assert-copy-finished.
func TestStreamer_VStreamCopyDoor_ControlTablesDMLOnlyRole(t *testing.T) {
	_, adminDSN, cleanup := startPostgres(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	admin, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = admin.Close() }()
	exec := func(stmt string) {
		t.Helper()
		if _, err := admin.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("admin exec %q: %v", stmt, err)
		}
	}
	role := fmt.Sprintf("vcdml_%d", time.Now().UnixNano())
	exec(`CREATE ROLE ` + role + ` LOGIN PASSWORD 'pw'`)
	exec(`REVOKE CREATE ON SCHEMA public FROM PUBLIC`)
	exec(`GRANT USAGE ON SCHEMA public TO ` + role)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	provider, ok := pgEng.(ir.ControlTableDDLProvider)
	if !ok {
		t.Fatal("RIG: the postgres engine prints no control-table DDL")
	}
	// The owner bootstraps every control table but the migrate-state pair.
	for _, st := range provider.ControlTableDDL() {
		if strings.HasPrefix(st.Table, "sluice_migrate_") {
			continue
		}
		exec(st.DDL)
	}
	exec(`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ` + role)

	u, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, "pw")
	roleDSN := u.String()

	var canCreate bool
	rdb, err := sql.Open("pgx", roleDSN)
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.QueryRowContext(ctx, `SELECT pg_catalog.has_schema_privilege('public', 'CREATE')`).Scan(&canCreate); err != nil || canCreate {
		t.Fatalf("premise: role can CREATE on public = %v (err %v); the test is not measuring a DML-only role", canCreate, err)
	}
	_ = rdb.Close()

	door := func(t *testing.T, assert bool) error {
		t.Helper()
		applier, err := pgEng.OpenChangeApplier(ctx, roleDSN)
		if err != nil {
			t.Fatalf("open applier as the DML role: %v", err)
		}
		defer migcore.CloseIf(applier)
		src := &ukDoorEngine{
			resumeDispatchEngine: &resumeDispatchEngine{name: "planetscale", caps: ir.Capabilities{CDC: ir.CDCVStream}},
			schemas:              map[string]*ir.Schema{"src-dsn": driftSchema("t1")},
		}
		s := &Streamer{Source: src, Target: pgEng, SourceDSN: "src-dsn", TargetDSN: roleDSN, AssertCopyFinished: assert}
		_, err = s.phaseRefuseUnprovenVStreamCopy(ctx, applier, "vcdml-stream", cursorless)
		return err
	}
	requireDDLRefusal := func(t *testing.T, err error) {
		t.Helper()
		if err == nil || !errors.Is(err, ir.ErrIncludeTableScopeDrift) || refusalARestartRepeats(err) == nil ||
			!strings.Contains(err.Error(), "sluice control-tables ddl") {
			t.Fatalf("want the terminal control-tables refusal under the no-restart sentinel, got %v", err)
		}
		if !errors.Is(err, ir.ErrControlTableDDLRequired) {
			t.Errorf("the engine's DDL refusal did not carry ir.ErrControlTableDDLRequired: %v", err)
		}
	}

	// Missing: the store reads a missing relation as "no row" (measured here),
	// so no DDL is attempted — the ordinary refusal, and the flag passes.
	t.Run("migrate-state table missing", func(t *testing.T) {
		requireUnproven(t, door(t, false))
		if err := door(t, true); err != nil {
			t.Fatalf("with --assert-copy-finished and no header: %v", err)
		}
	})
	t.Run("migrate-state table out of date", func(t *testing.T) {
		for _, st := range provider.ControlTableDDL() {
			if strings.HasPrefix(st.Table, "sluice_migrate_") {
				exec(st.DDL)
			}
		}
		exec(`ALTER TABLE sluice_migrate_state DROP COLUMN source_identity`)
		exec(`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ` + role)
		requireDDLRefusal(t, door(t, false))
		requireDDLRefusal(t, door(t, true))
	})
}
