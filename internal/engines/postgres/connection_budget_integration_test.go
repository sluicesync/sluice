//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the connection-budget preflight (connection-
// resilience item 4). Boots a real PG container and ground-truths the
// catalog probe + the ProbeTargetConnectionBudget capability: the raw
// numbers must be sane, an operator --max-target-connections ceiling must
// cap the effective parallelism, and a role-level CONNECTION LIMIT must
// dominate the budget. The pure formula (refuse-when-<1, the min across
// limits) is exhaustively unit-tested in connection_budget_test.go; this
// file rides the real pg_stat_activity / pg_roles / pg_database catalog so
// a SQL fat-finger or a PG-version drift in the probe surfaces here.

package postgres

import (
	"context"
	"net/url"
	"testing"
	"time"
)

// TestProbeConnectionBudget_SaneNumbers asserts the raw probe reads
// plausible values against a default container: max_connections is the
// PG default (100) or whatever the image sets, at least one connection is
// in use (our own), and the default superuser has unlimited role/db
// limits.
func TestProbeConnectionBudget_SaneNumbers(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	cfg, err := Engine{}.parseDSN(dsn)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := openDBAs(ctx, cfg, roleControl)
	if err != nil {
		t.Fatalf("openDBAs: %v", err)
	}
	defer func() { _ = db.Close() }()

	p, err := probeConnectionBudget(ctx, db)
	if err != nil {
		t.Fatalf("probeConnectionBudget: %v", err)
	}
	if p.maxConnections < 10 {
		t.Errorf("max_connections = %d, implausibly low", p.maxConnections)
	}
	if p.currentTotal < 1 {
		t.Errorf("pg_stat_activity count = %d, want >= 1 (our own connection)", p.currentTotal)
	}
	// Regression pin: currentTotal must count ONLY client backends, not
	// the server's background processes (checkpointer, wal/bg writer,
	// autovacuum launcher, archiver, logical-replication launcher, PG18+
	// io workers). Those don't consume a max_connections slot; counting
	// them inflated in_use and produced a FALSE budget-exhausted refusal
	// on tight managed PG (max_connections=25 + ~9 bg procs). Assert the
	// probe equals the client-backend count AND that background processes
	// exist (so the filter is load-bearing, not a no-op).
	var total, clients int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity`).Scan(&total); err != nil {
		t.Fatalf("total pg_stat_activity count: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend'`).Scan(&clients); err != nil {
		t.Fatalf("client-backend count: %v", err)
	}
	if p.currentTotal != clients {
		t.Errorf("currentTotal = %d, want %d (client backends only; bg processes must be excluded)", p.currentTotal, clients)
	}
	if total <= clients {
		t.Errorf("expected background processes to exist (total=%d clients=%d); the backend_type filter must be load-bearing", total, clients)
	}
	if p.rolConnLimit != unlimited {
		t.Errorf("default superuser rolconnlimit = %d, want -1 (unlimited)", p.rolConnLimit)
	}
	if p.datConnLimit != unlimited {
		t.Errorf("default datconnlimit = %d, want -1 (unlimited)", p.datConnLimit)
	}

	budget := computeConnectionBudget(p, connBudgetReserve)
	if budget.CopyBudget < 1 {
		t.Errorf("a fresh default container should have copy budget >= 1; got %d", budget.CopyBudget)
	}
}

// TestProbeTargetConnectionBudget_CeilingCaps asserts that an operator
// --max-target-connections ceiling caps the effective parallelism even
// when the catalog budget is roomy.
func TestProbeTargetConnectionBudget_CeilingCaps(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Request 8, ceiling 2 → effective must be 2 (ceiling-bounded), Capped.
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
}

// TestProbeTargetConnectionBudget_RoleLimitDominates creates a role with a
// tight CONNECTION LIMIT and asserts that limit dominates the budget — the
// effective parallelism is bounded by the role's free slots, not the roomy
// global max_connections.
func TestProbeTargetConnectionBudget_RoleLimitDominates(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	const fixture = `
		-- A role capped at 5 concurrent connections. rolconnlimit=5
		-- becomes the dominating budget term (global max_connections is
		-- far higher on a default container).
		CREATE ROLE budget_capped LOGIN PASSWORD 'app' NOSUPERUSER CONNECTION LIMIT 5;
		GRANT USAGE ON SCHEMA public TO budget_capped;
	`
	applyDDL(t, dsn, fixture)

	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("rebind DSN: %v", err)
	}
	u.User = url.UserPassword("budget_capped", "app")
	cappedDSN := u.String()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Request 8, no operator ceiling. The role limit (5) minus the role's
	// own in-use connections minus the reserve (4) must bound the budget
	// well below 8 — so the report is capped and the effective value is
	// the role-derived copy budget.
	report, err := Engine{}.ProbeTargetConnectionBudget(ctx, cappedDSN, 8, 0)
	if err != nil {
		t.Fatalf("ProbeTargetConnectionBudget: %v", err)
	}
	if report.RoleLimit != 5 {
		t.Errorf("RoleLimit = %d, want 5 (the CONNECTION LIMIT)", report.RoleLimit)
	}
	// CopyBudget = role_available - reserve. role_available <= 5, reserve
	// is 4, so the copy budget is <= 1 and the effective parallelism is
	// clamped hard. The exact value depends on how many connections the
	// role holds during the probe; assert the bound rather than an exact.
	if !report.Refuse && report.EffectiveParallelism > 1 {
		t.Errorf("role limit of 5 should bound effective parallelism to <=1; got %d (copy_budget=%d)",
			report.EffectiveParallelism, report.CopyBudget)
	}
}
