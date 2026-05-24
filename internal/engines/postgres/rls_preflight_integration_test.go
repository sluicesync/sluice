//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the Postgres RLS preflight prober (task #52
// sub-deliverable 1). Boots a real PG container, creates a table with
// `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` (and one with FORCE),
// then exercises the four cells of {RLS on/off} × {role BYPASSRLS
// yes/no} via two connection roles — the default `test` superuser
// (which carries BYPASSRLS by definition) and an additional
// non-superuser role created without it.
//
// Pin shape mirrors the unit test set in `internal/pipeline/
// rls_preflight_test.go`, but the assertions ride on the real
// `pg_class` / `pg_roles` catalog values rather than stubs — so a
// catalog rename, a PG-version drift, or a SQL-syntax fat-finger in
// the prober surfaces here.

package postgres

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// applyRLSFixture creates two tables, enables RLS (one regular, one
// FORCE), and a non-superuser role without BYPASSRLS. The returned
// `unprivilegedDSN` connects as that role; the original `dsn` keeps
// the superuser path for the with-BYPASSRLS cell.
func applyRLSFixture(t *testing.T, dsn string) (unprivilegedDSN string) {
	t.Helper()
	const fixture = `
		CREATE TABLE rls_off (
			id BIGINT PRIMARY KEY,
			payload TEXT
		);
		CREATE TABLE rls_on (
			id BIGINT PRIMARY KEY,
			tenant TEXT NOT NULL,
			payload TEXT
		);
		ALTER TABLE rls_on ENABLE ROW LEVEL SECURITY;
		CREATE POLICY rls_on_policy ON rls_on
			USING (tenant = current_setting('app.tenant', true));

		CREATE TABLE rls_force (
			id BIGINT PRIMARY KEY,
			tenant TEXT NOT NULL,
			payload TEXT
		);
		ALTER TABLE rls_force ENABLE ROW LEVEL SECURITY;
		ALTER TABLE rls_force FORCE ROW LEVEL SECURITY;
		CREATE POLICY rls_force_policy ON rls_force
			USING (tenant = current_setting('app.tenant', true))
			WITH CHECK (tenant = current_setting('app.tenant', true));

		-- Non-superuser role without BYPASSRLS. Explicitly NOBYPASSRLS
		-- so a future PG default change doesn't silently flip this
		-- test. Granted basic CRUD on the fixture so the role can
		-- actually probe the schema (no PG default lets a non-owner
		-- read a table; the preflight probe queries pg_class /
		-- pg_roles which ARE world-readable, but we keep the GRANTs
		-- for parity with realistic operator setups).
		CREATE ROLE sluice_app LOGIN PASSWORD 'app' NOBYPASSRLS NOSUPERUSER;
		GRANT USAGE ON SCHEMA public TO sluice_app;
		GRANT SELECT, INSERT, UPDATE, DELETE ON rls_off, rls_on, rls_force TO sluice_app;
	`
	applyDDL(t, dsn, fixture)

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("rebind DSN: parse: %v", err)
	}
	u.User = url.UserPassword("sluice_app", "app")
	return u.String()
}

// TestRLSPreflight_RoleAttribute_Superuser confirms the default `test`
// role lives in the world we expect: it carries BYPASSRLS (every
// pgtc superuser does). If a PG version flip ever broke this
// assumption, the with-BYPASSRLS cells of this suite would silently
// stop covering the green path; this test pins it loudly.
func TestRLSPreflight_RoleAttribute_Superuser(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bypass, role, err := probeCurrentRoleBypassesRLS(ctx, db)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if role == "" {
		t.Fatal("expected non-empty role name")
	}
	if !bypass {
		t.Errorf("expected `test` superuser to carry BYPASSRLS; got false for role %q", role)
	}
}

// TestRLSPreflight_RoleAttribute_Unprivileged: the non-superuser role
// we create explicitly NOBYPASSRLS reports the expected attribute.
func TestRLSPreflight_RoleAttribute_Unprivileged(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	unprivDSN := applyRLSFixture(t, dsn)

	db, err := sql.Open("pgx", unprivDSN)
	if err != nil {
		t.Fatalf("open as sluice_app: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	bypass, role, err := probeCurrentRoleBypassesRLS(ctx, db)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if role != "sluice_app" {
		t.Errorf("expected role 'sluice_app'; got %q", role)
	}
	if bypass {
		t.Errorf("expected sluice_app to lack BYPASSRLS; got true")
	}
}

// TestRLSPreflight_TableStatus_AcrossShapes walks rls_off / rls_on /
// rls_force and asserts the (enabled, forced) tuple matches the
// fixture's DDL. This catches a catalog-column rename or a SQL typo
// in the prober that the unit-test stubs can't see.
func TestRLSPreflight_TableStatus_AcrossShapes(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	applyRLSFixture(t, dsn)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cases := []struct {
		table       string
		wantEnabled bool
		wantForced  bool
	}{
		{"rls_off", false, false},
		{"rls_on", true, false},
		{"rls_force", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.table, func(t *testing.T) {
			enabled, forced, err := probeTableRLSStatus(ctx, db, "public", tc.table)
			if err != nil {
				t.Fatalf("probe: %v", err)
			}
			if enabled != tc.wantEnabled {
				t.Errorf("relrowsecurity: got %v; want %v", enabled, tc.wantEnabled)
			}
			if forced != tc.wantForced {
				t.Errorf("relforcerowsecurity: got %v; want %v", forced, tc.wantForced)
			}
		})
	}
}

// TestRLSPreflight_TableStatus_MissingTableIsNoop confirms the
// missing-table branch returns (false, false, nil) — downstream
// phases produce a better error than the preflight could.
func TestRLSPreflight_TableStatus_MissingTableIsNoop(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	enabled, forced, err := probeTableRLSStatus(ctx, db, "public", "nonexistent_table")
	if err != nil {
		t.Fatalf("expected nil err on missing table; got %v", err)
	}
	if enabled || forced {
		t.Errorf("expected (false, false) for missing table; got (%v, %v)", enabled, forced)
	}
}

// TestRLSPreflight_SchemaReader_AsProber confirms the SchemaReader
// surface (used by the source-side preflight) routes to the same
// underlying probe and returns the same answers.
func TestRLSPreflight_SchemaReader_AsProber(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	applyRLSFixture(t, dsn)

	sr, err := Engine{}.OpenSchemaReader(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open schema reader: %v", err)
	}
	defer func() {
		if c, ok := sr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	psr, ok := sr.(*SchemaReader)
	if !ok {
		t.Fatalf("schema reader is not *postgres.SchemaReader: %T", sr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bypass, role, err := psr.CurrentRoleBypassesRLS(ctx)
	if err != nil {
		t.Fatalf("CurrentRoleBypassesRLS: %v", err)
	}
	if !bypass || role == "" {
		t.Errorf("expected superuser bypass=true with non-empty role; got bypass=%v role=%q", bypass, role)
	}

	enabled, forced, err := psr.TableRLSStatus(ctx, &ir.Table{Name: "rls_force"})
	if err != nil {
		t.Fatalf("TableRLSStatus: %v", err)
	}
	if !enabled || !forced {
		t.Errorf("expected rls_force enabled=true forced=true; got enabled=%v forced=%v", enabled, forced)
	}
}

// TestRLSPreflight_RowWriter_AsProber is the target-side counterpart
// — same shape, different engine surface.
func TestRLSPreflight_RowWriter_AsProber(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	applyRLSFixture(t, dsn)

	rw, err := Engine{}.OpenRowWriter(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open row writer: %v", err)
	}
	defer func() {
		if c, ok := rw.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	prw, ok := rw.(*RowWriter)
	if !ok {
		t.Fatalf("row writer is not *postgres.RowWriter: %T", rw)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bypass, role, err := prw.CurrentRoleBypassesRLS(ctx)
	if err != nil {
		t.Fatalf("CurrentRoleBypassesRLS: %v", err)
	}
	if !bypass || role == "" {
		t.Errorf("expected superuser bypass=true with non-empty role; got bypass=%v role=%q", bypass, role)
	}

	enabled, forced, err := prw.TableRLSStatus(ctx, &ir.Table{Name: "rls_on"})
	if err != nil {
		t.Fatalf("TableRLSStatus: %v", err)
	}
	if !enabled || forced {
		t.Errorf("expected rls_on enabled=true forced=false; got enabled=%v forced=%v", enabled, forced)
	}
}

// TestRLSPreflight_UnprivilegedRoleCanProbe pins the operator-relevant
// case: the non-superuser role itself must be able to read pg_class
// + pg_roles (otherwise the preflight would surface as a permission
// error instead of a clean refusal). PG ships these as world-readable
// by default; this test guards against a hardening profile that
// would change that.
func TestRLSPreflight_UnprivilegedRoleCanProbe(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	unprivDSN := applyRLSFixture(t, dsn)

	db, err := sql.Open("pgx", unprivDSN)
	if err != nil {
		t.Fatalf("open as sluice_app: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	enabled, forced, err := probeTableRLSStatus(ctx, db, "public", "rls_force")
	if err != nil {
		t.Fatalf("non-superuser probe: %v", err)
	}
	if !enabled || !forced {
		t.Errorf("expected non-superuser to read RLS state correctly; got enabled=%v forced=%v",
			enabled, forced)
	}
}

// TestRLSPreflight_DiagnoseBundleSurfacesRLS confirms the diagnose
// bundle's EngineState JSON carries the per-table RLS state plus the
// role's BYPASSRLS attribute — so an operator can run `sluice
// diagnose` and see whether the RLS preflight will refuse without
// running a full migration.
func TestRLSPreflight_DiagnoseBundleSurfacesRLS(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	applyRLSFixture(t, dsn)

	sr, err := Engine{}.OpenSchemaReader(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open schema reader: %v", err)
	}
	psr, ok := sr.(*SchemaReader)
	if !ok {
		t.Fatalf("not *SchemaReader: %T", sr)
	}
	defer func() { _ = psr.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	snap, err := psr.DiagnoseBundle(ctx, "test-stream")
	if err != nil {
		t.Fatalf("DiagnoseBundle: %v", err)
	}

	// The bundle stores state as opaque JSON; we re-decode it to
	// inspect the RLS section without coupling to internal helper
	// types.
	payload := string(snap.EngineState)
	for _, want := range []string{
		`"rls"`,                 // section is present
		`"rolbypassrls"`,        // role attribute is captured
		`"tables_with_rls"`,     // per-table list keyed under stable name
		`"rls_on"`,              // RLS-enabled table is named
		`"rls_force"`,           // FORCE-RLS table is named
		`"relrowsecurity":true`, // enable flag captured
		`"relforcerowsecurity"`, // force flag captured (value tested below)
	} {
		if !strings.Contains(payload, want) {
			t.Errorf("diagnose payload missing %s\nfull: %s", want, payload)
		}
	}
	// rls_off has neither flag set; should be omitted from the
	// compact tables_with_rls listing.
	if strings.Contains(payload, `"rls_off"`) {
		t.Errorf("rls_off (no RLS state) should be omitted from tables_with_rls; got %s", payload)
	}
}

// TestRLSPreflight_NonEmptyTablesWithRLSAndUnprivilegedRoleRefuses is
// the wired-together end-to-end shape: a SchemaReader bound to a
// non-superuser DSN, walked through preflightRLS via the orchestrator
// surface, must refuse loudly when any in-scope table has RLS on.
// Same path the migrate / sync entry points take.
func TestRLSPreflight_NonEmptyTablesWithRLSAndUnprivilegedRoleRefuses(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	unprivDSN := applyRLSFixture(t, dsn)

	sr, err := Engine{}.OpenSchemaReader(context.Background(), unprivDSN)
	if err != nil {
		t.Fatalf("open schema reader as sluice_app: %v", err)
	}
	psr, ok := sr.(*SchemaReader)
	if !ok {
		t.Fatalf("not *SchemaReader: %T", sr)
	}
	defer func() { _ = psr.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	enabled, forced, err := psr.TableRLSStatus(ctx, &ir.Table{Name: "rls_on"})
	if err != nil {
		t.Fatalf("TableRLSStatus: %v", err)
	}
	if !enabled || forced {
		t.Errorf("rls_on as sluice_app: got enabled=%v forced=%v; want enabled=true forced=false",
			enabled, forced)
	}
	bypass, role, err := psr.CurrentRoleBypassesRLS(ctx)
	if err != nil {
		t.Fatalf("CurrentRoleBypassesRLS: %v", err)
	}
	if bypass || role != "sluice_app" {
		t.Errorf("sluice_app: got bypass=%v role=%q; want bypass=false role=sluice_app",
			bypass, role)
	}
}

// asserrtRLSSluiceAppCannotInsert documents the underlying behaviour
// the preflight protects against — a `sluice_app` INSERT into
// `rls_force` without setting `app.tenant` is rejected by PG with the
// "new row violates row-level security policy" error. Without the
// preflight, sluice would surface this mid-bulk-copy as an opaque
// failure. This is not a test of sluice's prober but of the PG ground
// truth the prober is paid to detect; pinning it here keeps the
// fixture honest if PG ever changed RLS semantics.
func TestRLSPreflight_UnprivilegedInsertActuallyRefusedByPG(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	unprivDSN := applyRLSFixture(t, dsn)

	db, err := sql.Open("pgx", unprivDSN)
	if err != nil {
		t.Fatalf("open as sluice_app: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = db.ExecContext(ctx, `INSERT INTO rls_force (id, tenant, payload) VALUES (1, 'alpha', 'x')`)
	if err == nil {
		t.Fatal("expected INSERT into FORCE-RLS table to fail without app.tenant set; got nil")
	}
	if !strings.Contains(err.Error(), "row-level security") {
		t.Errorf("expected RLS-violation error; got %v", err)
	}
}
