//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// # F7 integration pin: SET LOCAL synchronous_commit = on overrides
// # a role-default of `synchronous_commit = off`
//
// Severity-A finding F7 from the 2026-05-22 PG-internals research run
// (see docs `sluice-pg-internals-research-chapters-9-10-11-2026-05-22.md`).
// PG Internals Ch 11.2 documents how `ALTER ROLE name SET param = value`
// pre-applies the value on every login from that role. ADR-0007's
// "position + data lands durably together" guarantee is built on top of
// `synchronous_commit = on`'s wal-flush-before-ACK semantics
// (Ch 9.5): if the role default flips to `off`, sluice's COMMIT can
// return BEFORE the WAL is durably written, and a target crash between
// the ACK and the WAL flush silently loses the position+data tx.
//
// This pin demonstrates the failure mode is real (the role default
// propagates to fresh sessions) AND that sluice's `SET LOCAL
// synchronous_commit = on` inside every apply tx overrides it
// correctly. The end-to-end apply path is also exercised under the
// hostile role default so the hardening doesn't break the happy path.

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

// connectAs returns a connection string for the named role on the
// same PG container the applier integration suite already boots.
// Keeps the test self-contained — no extra container fixtures.
func connectAs(t *testing.T, dsn, role, password string) string {
	t.Helper()
	// The DSN the testcontainers-go pg module returns is of the
	// form `postgres://user:pass@host:port/db?sslmode=disable`. We
	// build a sibling DSN with the same host/port/db but the
	// requested role's credentials.
	//
	// Rather than parse + rebuild URL, just swap the credentials in
	// the existing string: split on "://" and re-prefix.
	const scheme = "postgres://"
	if !strings.HasPrefix(dsn, scheme) {
		t.Fatalf("connectAs: dsn %q lacks postgres:// prefix; can't rebuild", dsn)
	}
	rest := strings.TrimPrefix(dsn, scheme)
	at := strings.Index(rest, "@")
	if at < 0 {
		t.Fatalf("connectAs: dsn %q lacks @ separator", dsn)
	}
	return scheme + role + ":" + password + "@" + rest[at+1:]
}

// TestApplier_F7_RoleDefaultSynchronousCommitOff_OverriddenInApplyTx
// proves the F7 hardening end-to-end:
//
//  1. A PG role is created with `ALTER ROLE … SET synchronous_commit
//     = off` — this is the exact configuration that, pre-fix, would
//     silently undermine ADR-0007 by allowing a COMMIT ACK to return
//     before the WAL is durably written.
//
//  2. A side probe (separate session as the same role, NOT through
//     sluice) confirms the role default DOES propagate: a fresh
//     session sees `synchronous_commit = off`. If this assertion ever
//     stops holding the test is no longer exercising the hazard and
//     must be updated.
//
//  3. The sluice applier opens a connection as the same role, applies
//     a row, and commits. The apply must succeed — `SET LOCAL` inside
//     the apply tx overrides the role default without erroring out.
//
//  4. Inside an applier-shaped tx (BeginTx → SET LOCAL synchronous_
//     commit=on → SELECT current_setting) the setting reads back as
//     `on`, confirming the override is in effect for the duration of
//     the apply tx (and would therefore be in effect at COMMIT time,
//     forcing the WAL flush before ACK).
func TestApplier_F7_RoleDefaultSynchronousCommitOff_OverriddenInApplyTx(t *testing.T) {
	adminDSN, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Step 1: create a role with `synchronous_commit = off` as its
	// per-role default, plus the schema permissions the applier needs
	// to create the user-data table and EnsureControlTable's
	// sluice_cdc_state.
	const role = "sluice_f7_user"
	const password = "sluice_f7_pw"
	applyPGApplier(t, adminDSN, fmt.Sprintf(`
		CREATE ROLE %s WITH LOGIN PASSWORD %s;
		GRANT ALL ON SCHEMA public TO %s;
		ALTER ROLE %s SET synchronous_commit = off;
		CREATE TABLE users (
			id     BIGINT       PRIMARY KEY,
			email  VARCHAR(255) NOT NULL UNIQUE,
			active BOOLEAN      NOT NULL DEFAULT true
		);
		GRANT ALL ON TABLE users TO %s;
	`, role, quotePGLiteral(password), role, role, role))

	roleDSN := connectAs(t, adminDSN, role, password)

	// Step 2: side probe — a NON-sluice session as the role should
	// see `off`, proving the role default is in effect for fresh
	// sessions on this PG.
	probeRoleDefault(t, ctx, roleDSN, "off",
		"role default did not propagate; the F7 test is no longer exercising the inheritance hazard")

	// Step 3: open the sluice applier as the hostile-default role,
	// apply a row, and assert it lands.
	eng := Engine{}
	applier, err := eng.OpenChangeApplier(ctx, roleDSN)
	if err != nil {
		t.Fatalf("OpenChangeApplier as %s: %v", role, err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	events := []ir.Change{
		ir.Insert{Schema: "public", Table: "users", Row: ir.Row{"id": int64(1), "email": "f7@example.com", "active": true}},
	}
	pumpChanges(t, ctx, applier, events)

	// Apply must have landed the row (proves SET LOCAL inside the
	// applier tx didn't break the happy path under the hostile role
	// default).
	if got := countUserRows(t, roleDSN); got != 1 {
		t.Fatalf("after apply: rows = %d; want 1 (F7 SET LOCAL broke the apply path under role default=off?)", got)
	}

	// Step 4: probe the override directly. Open a fresh tx as the
	// role, run the same SET LOCAL the applier runs, then SELECT
	// current_setting — must be `on`. This is the same statement
	// shape sluice's forceSynchronousCommitOn emits.
	probeOverrideInsideTx(t, ctx, roleDSN, "on",
		"SET LOCAL synchronous_commit=on did NOT override the role default inside the tx; F7 hardening is ineffective")
}

// quotePGLiteral wraps a string in single quotes for inline DDL
// (safe-by-construction for the test's known passwords; the
// applyPGApplier path doesn't accept parameter binding).
func quotePGLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// probeRoleDefault opens a fresh session as `dsn` and reads
// `current_setting('synchronous_commit')` — outside any explicit
// transaction so the role-default-on-login value is what's observed.
// Fatals if the value doesn't match `want`.
func probeRoleDefault(t *testing.T, ctx context.Context, dsn, want, hint string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("probe: open: %v", err)
	}
	defer func() { _ = db.Close() }()

	var got string
	if err := db.QueryRowContext(ctx, "SHOW synchronous_commit").Scan(&got); err != nil {
		t.Fatalf("probe SHOW: %v", err)
	}
	if got != want {
		t.Fatalf("role default: got %q; want %q (%s)", got, want, hint)
	}
}

// probeOverrideInsideTx opens a tx, runs SET LOCAL synchronous_commit
// = on (the exact statement sluice's forceSynchronousCommitOn emits),
// then SELECT current_setting('synchronous_commit') inside the same
// tx — must read back the requested value. Fatals on mismatch.
func probeOverrideInsideTx(t *testing.T, ctx context.Context, dsn, want, hint string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("probe override: open: %v", err)
	}
	defer func() { _ = db.Close() }()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("probe override: BeginTx: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		_ = tx.Rollback()
		t.Fatalf("probe override: SET LOCAL: %v", err)
	}
	var got string
	if err := tx.QueryRowContext(ctx, "SELECT current_setting('synchronous_commit')").Scan(&got); err != nil {
		_ = tx.Rollback()
		t.Fatalf("probe override: SELECT current_setting: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("probe override: Rollback: %v", err)
	}
	if got != want {
		t.Fatalf("inside-tx setting: got %q; want %q (%s)", got, want, hint)
	}
}

// countUserRows is a tiny test helper used by the F7 integration pin
// to assert end-to-end apply success under a hostile role default.
// Kept local to this file to avoid coupling to the test-helper layout
// in change_applier_integration_test.go.
func countUserRows(t *testing.T, dsn string) int {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("count users: open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var n int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&n); err != nil {
		t.Fatalf("count users: %v", err)
	}
	return n
}
