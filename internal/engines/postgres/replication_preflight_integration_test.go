//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the Postgres replication-capability preflight
// prober (task #61). Boots a real PG container, creates a Heroku-like
// non-superuser role explicitly NOREPLICATION (mirroring the Heroku
// Postgres Essential / Render Basic / Supabase free tier that forbids
// the REPLICATION attribute), and asserts the prober reads the catalog
// truth: the low-priv role reports canReplicate=false, while the default
// superuser reports true.
//
// Pin shape mirrors rls_preflight_integration_test.go, but rides on the
// real `pg_roles.rolsuper / rolreplication` catalog values rather than
// stubs — so a catalog rename, a PG-version drift, or a SQL fat-finger
// in the prober surfaces here. The orchestrator-side gate + refusal
// (including the KEY postgres-trigger exclusion, which is engine-name
// gated in the pipeline package) is unit-tested in
// internal/pipeline/replication_preflight_test.go with a delegating
// stub; this file ground-truths the engine-side probe the gate rides on.

package postgres

import (
	"context"
	"database/sql"
	"net/url"
	"testing"
	"time"
)

// applyReplicationFixture creates a Heroku-like non-superuser role
// explicitly NOREPLICATION, grants it just enough to read the schema
// (USAGE on the schema; pg_roles is world-readable so no GRANT is
// needed for the probe itself), and returns a DSN bound to that role.
// The original `dsn` keeps the superuser path for the positive control.
func applyReplicationFixture(t *testing.T, dsn string) (lowPrivDSN string) {
	t.Helper()
	const fixture = `
		CREATE TABLE repl_probe (
			id BIGINT PRIMARY KEY,
			payload TEXT
		);

		-- Heroku-like managed-PG role: can log in and read schema, but
		-- explicitly cannot create a replication slot. NOREPLICATION /
		-- NOSUPERUSER are stated explicitly so a future PG default flip
		-- doesn't silently change what this test covers.
		CREATE ROLE heroku_like LOGIN PASSWORD 'app' NOSUPERUSER NOREPLICATION;
		GRANT USAGE ON SCHEMA public TO heroku_like;
		GRANT SELECT ON repl_probe TO heroku_like;
	`
	applyDDL(t, dsn, fixture)

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("rebind DSN: parse: %v", err)
	}
	u.User = url.UserPassword("heroku_like", "app")
	return u.String()
}

// TestReplicationPreflight_Superuser is the positive control: the
// default `test` superuser carries replication capability (every pgtc
// superuser does, via rolsuper). If a PG version flip ever broke this,
// the green path of this suite would silently stop covering it.
func TestReplicationPreflight_Superuser(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	canReplicate, role, err := probeSourceReplicationCapability(ctx, db)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if role == "" {
		t.Fatal("expected non-empty role name")
	}
	if !canReplicate {
		t.Errorf("expected `test` superuser to be replication-capable; got false for role %q", role)
	}
}

// TestReplicationPreflight_HerokuLikeRoleCannotReplicate is the core
// managed-PG case: a NOSUPERUSER NOREPLICATION role reports
// canReplicate=false — the signal the orchestrator preflight refuses on
// (pointing the operator at --source-driver=postgres-trigger).
func TestReplicationPreflight_HerokuLikeRoleCannotReplicate(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	lowPrivDSN := applyReplicationFixture(t, dsn)

	db, err := sql.Open("pgx", lowPrivDSN)
	if err != nil {
		t.Fatalf("open as heroku_like: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	canReplicate, role, err := probeSourceReplicationCapability(ctx, db)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if role != "heroku_like" {
		t.Errorf("expected role 'heroku_like'; got %q", role)
	}
	if canReplicate {
		t.Errorf("expected heroku_like to lack replication capability; got true")
	}
}

// TestReplicationPreflight_LowPrivRoleCanProbe pins the operator-relevant
// case: the low-priv role itself must be able to read pg_roles
// (otherwise the preflight would surface as a permission error instead
// of a clean refusal). PG ships pg_roles world-readable by default; this
// guards against a hardening profile that would change that.
func TestReplicationPreflight_LowPrivRoleCanProbe(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	lowPrivDSN := applyReplicationFixture(t, dsn)

	db, err := sql.Open("pgx", lowPrivDSN)
	if err != nil {
		t.Fatalf("open as heroku_like: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The probe reads its OWN role's attributes; a clean (false, role,
	// nil) is the contract the orchestrator gate depends on.
	canReplicate, role, err := probeSourceReplicationCapability(ctx, db)
	if err != nil {
		t.Fatalf("low-priv probe should not error (pg_roles is world-readable): %v", err)
	}
	if canReplicate || role != "heroku_like" {
		t.Errorf("expected (false, heroku_like); got (%v, %q)", canReplicate, role)
	}
}

// applyRDSLikeFixture models AWS RDS / Aurora Postgres: a
// `rds_replication` grant role exists, the master-like login role is
// NOSUPERUSER NOREPLICATION but a MEMBER of it, and a second custom
// login role exists WITHOUT the membership. Both attribute columns stay
// false — on RDS slot capability is pure role membership (live-proven
// on RDS PG 16, 2026-07-16; see replication_preflight.go).
func applyRDSLikeFixture(t *testing.T, dsn string) (masterLikeDSN, customRoleDSN string) {
	t.Helper()
	const fixture = `
		-- The provider grant role. NOLOGIN like the real one.
		CREATE ROLE rds_replication NOLOGIN;

		-- RDS-master-like: no superuser, no REPLICATION attribute, but
		-- a member of rds_replication — must probe capable.
		CREATE ROLE rds_master_like LOGIN PASSWORD 'app' NOSUPERUSER NOREPLICATION;
		GRANT rds_replication TO rds_master_like;

		-- Custom app role WITHOUT the membership — must probe incapable
		-- even though the rds_replication role EXISTS (membership, not
		-- mere role presence, is the capability).
		CREATE ROLE rds_custom_like LOGIN PASSWORD 'app' NOSUPERUSER NOREPLICATION;
	`
	applyDDL(t, dsn, fixture)

	rebind := func(user string) string {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatalf("rebind DSN: parse: %v", err)
		}
		u.User = url.UserPassword(user, "app")
		return u.String()
	}
	return rebind("rds_master_like"), rebind("rds_custom_like")
}

// TestReplicationPreflight_RDSLikeMembershipGrantsCapability is the F1
// core pin (RDS validation 2026-07-16): a NOSUPERUSER NOREPLICATION
// role that is a MEMBER of `rds_replication` probes capable, while a
// sibling role WITHOUT the membership — on the SAME server, where the
// grant role exists — still refuses. The pair pins both new predicate
// branches: the CASE arm firing true on membership, and NOT degrading
// into "the rds_replication role exists, so everyone passes".
func TestReplicationPreflight_RDSLikeMembershipGrantsCapability(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	masterLikeDSN, customRoleDSN := applyRDSLikeFixture(t, dsn)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Member role: capable.
	dbMaster, err := sql.Open("pgx", masterLikeDSN)
	if err != nil {
		t.Fatalf("open as rds_master_like: %v", err)
	}
	defer func() { _ = dbMaster.Close() }()
	canReplicate, role, err := probeSourceReplicationCapability(ctx, dbMaster)
	if err != nil {
		t.Fatalf("probe (rds_master_like): %v", err)
	}
	if !canReplicate || role != "rds_master_like" {
		t.Errorf("expected (true, rds_master_like) via rds_replication membership; got (%v, %q)", canReplicate, role)
	}

	// Non-member role on the same server: still refused.
	dbCustom, err := sql.Open("pgx", customRoleDSN)
	if err != nil {
		t.Fatalf("open as rds_custom_like: %v", err)
	}
	defer func() { _ = dbCustom.Close() }()
	canReplicate, role, err = probeSourceReplicationCapability(ctx, dbCustom)
	if err != nil {
		t.Fatalf("probe (rds_custom_like): %v", err)
	}
	if canReplicate || role != "rds_custom_like" {
		t.Errorf("expected (false, rds_custom_like) — rds_replication EXISTS but this role is not a member; got (%v, %q)", canReplicate, role)
	}
}

// TestReplicationPreflight_SchemaReader_AsProber confirms the
// SchemaReader surface (the source-side handle the orchestrator probes)
// routes to the same underlying probe and returns the same answers.
func TestReplicationPreflight_SchemaReader_AsProber(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()
	lowPrivDSN := applyReplicationFixture(t, dsn)

	// Superuser SchemaReader: replication-capable.
	srSuper, err := Engine{}.OpenSchemaReader(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open superuser schema reader: %v", err)
	}
	psrSuper, ok := srSuper.(*SchemaReader)
	if !ok {
		t.Fatalf("not *SchemaReader: %T", srSuper)
	}
	defer func() { _ = psrSuper.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	canReplicate, role, err := psrSuper.SourceReplicationCapability(ctx)
	if err != nil {
		t.Fatalf("SourceReplicationCapability (superuser): %v", err)
	}
	if !canReplicate || role == "" {
		t.Errorf("expected superuser canReplicate=true with non-empty role; got canReplicate=%v role=%q",
			canReplicate, role)
	}

	// Heroku-like SchemaReader: NOT replication-capable.
	srLow, err := Engine{}.OpenSchemaReader(context.Background(), lowPrivDSN)
	if err != nil {
		t.Fatalf("open heroku_like schema reader: %v", err)
	}
	psrLow, ok := srLow.(*SchemaReader)
	if !ok {
		t.Fatalf("not *SchemaReader: %T", srLow)
	}
	defer func() { _ = psrLow.Close() }()

	canReplicate, role, err = psrLow.SourceReplicationCapability(ctx)
	if err != nil {
		t.Fatalf("SourceReplicationCapability (heroku_like): %v", err)
	}
	if canReplicate || role != "heroku_like" {
		t.Errorf("expected heroku_like canReplicate=false role=heroku_like; got canReplicate=%v role=%q",
			canReplicate, role)
	}
}
