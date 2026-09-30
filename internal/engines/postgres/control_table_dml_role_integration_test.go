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

	"sluicesync.dev/sluice/internal/applymarks"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/logcapture"
)

// TestControlTables_DMLOnlyRole pins GC-40 (a) (Bug 291) on a real Postgres:
// a login role that holds only SELECT/INSERT/UPDATE/DELETE on control tables
// another role created, and no CREATE on their schema, reaches the stream.
// Before the fix every ensure ran CREATE TABLE IF NOT EXISTS and ALTER TABLE
// … ADD COLUMN IF NOT EXISTS on every start; PostgreSQL checks schema CREATE
// before IF NOT EXISTS and ownership before the ALTER, so this role stopped at
// "ensure control table" with "permission denied for schema public".
//
// Arms:
//   - precreated: the owner applied exactly what `sluice control-tables ddl
//     --engine postgres` prints (the independent check that the printed set is
//     the complete current shape); the role ensures with no DDL, applies a row
//     and does NOT warn APPLY-MARKS-UNAVAILABLE.
//   - marks-missing: the mark table is absent; the role still starts, and the
//     apply run WARNs APPLY-MARKS-UNAVAILABLE (as documented) and applies.
//   - column-missing / table-missing: DDL the role cannot run is refused
//     loudly, naming the column or table and the control-tables ddl remedy.
func TestControlTables_DMLOnlyRole(t *testing.T) {
	adminDSN, cleanup := newSharedPGDB(t, "gc40_dml_db")
	defer cleanup()
	host, port, _, _ := ensureSharedPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	role := fmt.Sprintf("gc40dml_%d", time.Now().UnixNano())
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
	exec(`CREATE ROLE ` + role + ` LOGIN PASSWORD 'pw'`)
	defer func() {
		_, _ = admin.ExecContext(context.Background(), `DROP OWNED BY `+role+`; DROP ROLE IF EXISTS `+role)
	}()
	// USAGE but no CREATE on the schema; the premise is asserted below.
	exec(`REVOKE CREATE ON SCHEMA public FROM PUBLIC`)
	exec(`GRANT USAGE ON SCHEMA public TO ` + role)
	exec(`CREATE TABLE gc40_items (id BIGINT PRIMARY KEY, code TEXT UNIQUE)`)
	exec(`GRANT SELECT, INSERT, UPDATE, DELETE ON gc40_items TO ` + role)

	// The owner bootstraps from the printed set, verbatim.
	for _, st := range (Engine{}).ControlTableDDL() {
		exec(st.DDL)
	}
	grant := func() {
		t.Helper()
		exec(`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO ` + role)
	}
	grant()

	roleDSN := sharedPGDSN(host, port, role, "pw", "gc40_dml_db")
	var canCreate bool
	rdb, err := sql.Open("pgx", roleDSN)
	if err != nil {
		t.Fatalf("open role: %v", err)
	}
	if err := rdb.QueryRowContext(ctx, `SELECT pg_catalog.has_schema_privilege('public', 'CREATE')`).Scan(&canCreate); err != nil || canCreate {
		t.Fatalf("premise: role can CREATE on public = %v (err %v); the test is not measuring a DML-only role", canCreate, err)
	}
	_ = rdb.Close()

	logs := &logcapture.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	defer slog.SetDefault(prev)

	start := func() (*ChangeApplier, error) {
		t.Helper()
		a, err := Engine{}.OpenChangeApplier(ctx, roleDSN)
		if err != nil {
			t.Fatalf("open applier as the DML role: %v", err)
		}
		pa := a.(*ChangeApplier)
		if err := pa.EnsureControlTable(ctx); err != nil {
			_ = pa.Close()
			return nil, err
		}
		return pa, nil
	}
	apply := func(a *ChangeApplier, id int64) {
		t.Helper()
		ch := make(chan ir.Change, 1)
		ch <- ir.Insert{
			Schema: "public", Table: "gc40_items",
			Row:      ir.Row{"id": id, "code": fmt.Sprintf("c%d", id)},
			Position: ir.Position{Engine: "postgres", Token: fmt.Sprintf(`{"lsn":"0/%X"}`, 0x16B2C10+id)},
		}
		close(ch)
		if err := a.Apply(ctx, "gc40-stream", ch); err != nil {
			t.Fatalf("apply as the DML role: %v", err)
		}
		var n int
		if err := admin.QueryRowContext(ctx, `SELECT count(*) FROM gc40_items WHERE id = $1`, id).Scan(&n); err != nil || n != 1 {
			t.Fatalf("row %d on the target: count %d (err %v)", id, n, err)
		}
	}

	t.Run("precreated", func(t *testing.T) {
		logs.Reset()
		a, err := start()
		if err != nil {
			t.Fatalf("EnsureControlTable as a DML-only role on a bootstrapped target: %v", err)
		}
		defer func() { _ = a.Close() }()
		if a.applyMarksEnsureErr != nil {
			t.Errorf("apply-mark ensure failed on a bootstrapped target: %v", a.applyMarksEnsureErr)
		}
		if err := a.EnsureUnforwardedRefusalStorage(ctx); err != nil {
			t.Errorf("unforwarded-refusal storage as the DML role: %v", err)
		}
		apply(a, 1)
		if strings.Contains(logs.String(), applymarks.UnavailableMarker) {
			t.Errorf("a bootstrapped target WARNed %s:\n%s", applymarks.UnavailableMarker, logs.String())
		}
		ms := &MigrationStateStore{db: a.db, schema: "public"}
		if err := ms.EnsureControlTable(ctx); err != nil {
			t.Errorf("migrate-state ensure as the DML role: %v", err)
		}
	})

	t.Run("marks-missing", func(t *testing.T) {
		exec(`DROP TABLE public.sluice_cdc_apply_marks`)
		logs.Reset()
		a, err := start()
		if err != nil {
			t.Fatalf("EnsureControlTable with the mark table absent: %v", err)
		}
		defer func() { _ = a.Close() }()
		apply(a, 2)
		if !strings.Contains(logs.String(), applymarks.UnavailableMarker) {
			t.Errorf("no %s WARN with the mark table absent:\n%s", applymarks.UnavailableMarker, logs.String())
		}
	})

	t.Run("column-missing", func(t *testing.T) {
		exec(`ALTER TABLE public.sluice_cdc_state DROP COLUMN rows_applied`)
		defer func() {
			exec(`ALTER TABLE public.sluice_cdc_state ADD COLUMN rows_applied BIGINT NOT NULL DEFAULT 0`)
		}()
		_, err := start()
		for _, want := range []string{ControlTableDDLRequiredMarker, "lacks column rows_applied", "control-tables ddl --engine postgres", "42501"} {
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("column-missing start = %v; want it to contain %q", err, want)
			}
		}
	})

	t.Run("table-missing", func(t *testing.T) {
		exec(`DROP TABLE public.sluice_cdc_skipped_tables`)
		_, err := start()
		for _, want := range []string{ControlTableDDLRequiredMarker, `"public"."sluice_cdc_skipped_tables" does not exist`, "control-tables ddl --engine postgres", "42501"} {
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("table-missing start = %v; want it to contain %q", err, want)
			}
		}
	})
}
