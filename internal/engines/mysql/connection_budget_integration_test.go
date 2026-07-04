//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the MySQL connection-budget prober (ADR-0116).
// Boots the shared MySQL container and ground-truths the probe: the raw
// numbers must be sane, a per-user MAX_USER_CONNECTIONS must dominate the
// budget, an operator --max-target-connections ceiling must cap the
// effective parallelism, and the buffer-pool tier cap (Part B) must be
// applied from the real @@innodb_buffer_pool_size. The pure formula
// (refuse-when-<1, the min across limits, the tier buckets) is
// exhaustively unit-tested in connection_budget_test.go +
// buffer_pool_tier_cap_test.go; this file rides the real server variables
// so a SQL fat-finger or a MySQL-version drift surfaces here. Mirrors the
// Postgres connection_budget_integration_test.go.

package mysql

import (
	"context"
	"testing"
	"time"
)

// TestProbeConnectionBudget_SaneNumbers asserts the raw probe reads
// plausible values against the shared container: max_connections is the
// server default (>= 10), at least one connection is in use (our own),
// and a fresh budget yields a non-refusing copy budget.
func TestProbeConnectionBudget_SaneNumbers(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	cfg, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := openDB(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("openDB: %v", err)
	}
	defer func() { _ = db.Close() }()

	p, err := probeConnectionBudget(ctx, db)
	if err != nil {
		t.Fatalf("probeConnectionBudget: %v", err)
	}
	if p.maxConnections < 10 {
		t.Errorf("max_connections = %d, implausibly low", p.maxConnections)
	}
	if p.inUse < 1 {
		t.Errorf("Threads_connected = %d, want >= 1 (our own connection)", p.inUse)
	}
	// The shared container runs as root with no per-user limit configured,
	// so the role term should be unlimited.
	if p.roleLimit != unlimited {
		t.Errorf("root per-user limit = %d, want unlimited (%d)", p.roleLimit, unlimited)
	}
	// @@innodb_buffer_pool_size must be readable on a real InnoDB server;
	// the tier cap must therefore be APPLIED (>0), not the no-op sentinel.
	if p.bufferPoolBytes <= 0 {
		t.Errorf("@@innodb_buffer_pool_size = %d, want > 0 on a real server", p.bufferPoolBytes)
	}
	if cap := bufferPoolParallelismCap(p.bufferPoolBytes); cap <= 0 {
		t.Errorf("tier cap from buffer pool %d = %d, want > 0 (cap must be applied on a real server)", p.bufferPoolBytes, cap)
	}

	// applyTierCap=true to exercise the cap-applied (PlanetScale) branch on a
	// real server with a readable buffer pool; CopyBudget >= 1 holds either way.
	budget := computeConnectionBudget(p, connBudgetReserve, true)
	if budget.CopyBudget < 1 {
		t.Errorf("a fresh shared container should have copy budget >= 1; got %d", budget.CopyBudget)
	}
}

// TestProbeTargetConnectionBudget_CeilingCaps asserts that an operator
// --max-target-connections ceiling caps the effective parallelism even
// when the catalog budget is roomy.
func TestProbeTargetConnectionBudget_CeilingCaps(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Request 8, ceiling 2 → effective must be 2 (ceiling-bounded), Capped.
	// Engine{} is FlavorVanilla, so the buffer-pool tier cap is NOT applied
	// (v0.99.122 — it is PlanetScale-only); the connection budget is roomy, so
	// the operator ceiling of 2 is the binding constraint here.
	report, err := Engine{}.ProbeTargetConnectionBudget(ctx, dsn, 8, 2)
	if err != nil {
		t.Fatalf("ProbeTargetConnectionBudget: %v", err)
	}
	if report.Refuse {
		t.Fatalf("did not expect a refusal on a fresh container: %v", report.RefusalError)
	}
	if report.EffectiveParallelism != 2 {
		t.Errorf("effective parallelism = %d, want 2 (ceiling-capped)", report.EffectiveParallelism)
	}
	if !report.Capped {
		t.Error("expected Capped=true when the ceiling bounds the request below 8")
	}
	// MySQL has no per-database connection cap — the report must say so.
	if report.DatabaseLimit != unlimited {
		t.Errorf("DatabaseLimit = %d, want unlimited (%d); MySQL has no per-db cap", report.DatabaseLimit, unlimited)
	}
}

// TestProbeTargetConnectionBudget_RoleLimitDominates creates a user with a
// tight MAX_USER_CONNECTIONS and asserts that per-user limit dominates the
// budget — the effective parallelism is bounded by the user's cap, not the
// roomy global max_connections. This also exercises the
// [probeRoleLimit] path against the real mysql.user catalog.
func TestProbeTargetConnectionBudget_RoleLimitDominates(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	// Create a low-limit user that can connect to the test database. The
	// shared container runs as root, so the GRANT succeeds. MAX_USER_
	// CONNECTIONS 5 becomes the dominating budget term (global
	// max_connections is far higher on a default container). The user
	// needs no data privileges for the probe — only CONNECT + the ability
	// to read its own row's max_user_connections from mysql.user, which we
	// grant via SELECT on mysql.user so the per-account override is read
	// (proving probeRoleLimit's mysql.user query works end to end).
	cfg, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	fixture := `
		CREATE USER IF NOT EXISTS 'budget_capped'@'%' IDENTIFIED BY 'app' WITH MAX_USER_CONNECTIONS 5;
		GRANT ALL PRIVILEGES ON ` + "`" + cfg.DBName + "`" + `.* TO 'budget_capped'@'%';
		GRANT SELECT ON ` + "`" + `mysql` + "`" + `.` + "`" + `user` + "`" + ` TO 'budget_capped'@'%';
		FLUSH PRIVILEGES;
	`
	applyDDL(t, dsn, fixture)

	cappedDSN := rebindUser(t, dsn, "budget_capped", "app", cfg.DBName)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	report, err := Engine{}.ProbeTargetConnectionBudget(ctx, cappedDSN, 8, 0)
	if err != nil {
		t.Fatalf("ProbeTargetConnectionBudget: %v", err)
	}
	if report.RoleLimit != 5 {
		t.Errorf("RoleLimit = %d, want 5 (the MAX_USER_CONNECTIONS)", report.RoleLimit)
	}
	// available = min(global, 5) = 5; copy = min(5 - reserve(4), tierCap).
	// reserve alone drives copy to 1, so the effective parallelism is
	// clamped hard (1). Assert the bound rather than coupling to the exact
	// tier cap.
	if !report.Refuse && report.EffectiveParallelism > 1 {
		t.Errorf("per-user limit of 5 should bound effective parallelism to <=1; got %d (copy_budget=%d)",
			report.EffectiveParallelism, report.CopyBudget)
	}
}

// TestProbeTargetConnectionBudget_PermissionDeniedDegrades asserts the
// critical managed-quirk path: a user that CANNOT read mysql.user (the
// common managed-MySQL / PlanetScale posture) must NOT fail the probe —
// probeRoleLimit degrades the per-account term to unlimited and the budget
// proceeds from the always-readable global variables, returning a
// non-refusing report. The safety check must never break an otherwise-
// working migration.
func TestProbeTargetConnectionBudget_PermissionDeniedDegrades(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	cfg, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	// A user with NO MAX_USER_CONNECTIONS and NO grant on mysql.user — so
	// the per-account read inside probeRoleLimit hits an access-denied
	// error, exactly the managed quirk. It only gets ALL on the test DB so
	// it can connect and run the server-variable probes.
	fixture := `
		CREATE USER IF NOT EXISTS 'no_mysql_select'@'%' IDENTIFIED BY 'app';
		GRANT ALL PRIVILEGES ON ` + "`" + cfg.DBName + "`" + `.* TO 'no_mysql_select'@'%';
		FLUSH PRIVILEGES;
	`
	applyDDL(t, dsn, fixture)

	restrictedDSN := rebindUser(t, dsn, "no_mysql_select", "app", cfg.DBName)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	report, err := Engine{}.ProbeTargetConnectionBudget(ctx, restrictedDSN, 4, 0)
	if err != nil {
		t.Fatalf("ProbeTargetConnectionBudget returned a hard error on a permission-gated probe; "+
			"the managed quirk must degrade, not fail: %v", err)
	}
	// The probe must NOT report ProbeFailed (the always-readable variables
	// succeeded) and must NOT refuse — it degrades the per-user term to
	// unlimited and proceeds.
	if report.ProbeFailed {
		t.Errorf("ProbeFailed = true on a mysql.user-denied probe; the per-user term must degrade silently, "+
			"not mark the whole probe failed: warning=%q", report.Warning)
	}
	if report.Refuse {
		t.Fatalf("did not expect a refusal on a fresh container with a degraded per-user term: %v", report.RefusalError)
	}
	if report.RoleLimit != unlimited {
		t.Errorf("RoleLimit = %d, want unlimited (%d) when mysql.user is denied", report.RoleLimit, unlimited)
	}
	if report.EffectiveParallelism < 1 {
		t.Errorf("effective parallelism = %d, want >= 1", report.EffectiveParallelism)
	}
}

// TestProbeTargetConnectionBudget_BadDSNErrors asserts that a
// connection-open failure (a wrong DSN) surfaces as a non-nil error — the
// one case that is worth failing on (it's the operator's own DSN, not the
// safety check breaking a working migration).
func TestProbeTargetConnectionBudget_BadDSNErrors(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	badDSN := rebindUser(t, dsn, "nonexistent_user", "wrongpw", "")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := Engine{}.ProbeTargetConnectionBudget(ctx, badDSN, 4, 0)
	if err == nil {
		t.Fatal("expected a non-nil error from a bad-credential DSN (connection-open failure)")
	}
}

// rebindUser returns a clone of dsn whose user/password are swapped to the
// given values (and database to db, when non-empty). The shared-DSN form
// is `user:pw@tcp(host:port)/db?params`; rebind splices the credential
// prefix before the first `@`.
func rebindUser(t *testing.T, dsn, user, pw, db string) string {
	t.Helper()
	cfg, err := parseDSN(dsn)
	if err != nil {
		t.Fatalf("rebindUser parseDSN: %v", err)
	}
	clone := cfg.Clone()
	clone.User = user
	clone.Passwd = pw
	if db != "" {
		clone.DBName = db
	}
	// FormatDSN re-emits the keep-alive network name finishParseDSN swapped
	// in; reset to wire-standard tcp so the re-parse routes cleanly.
	if clone.Net == keepaliveNet {
		clone.Net = "tcp"
	}
	return clone.FormatDSN()
}
