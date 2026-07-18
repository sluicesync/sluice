//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Cross-engine coverage for ADR-0173 Phase 1 — the per-table `--where`
// row filter — on a MySQL source → Postgres target. This exercises the
// MySQL reader's buildSelect/buildBatchedSelect push-down AND the MySQL
// verifier's count push-down, which the PG-source sibling
// (migrate_where_pg_integration_test.go) does not. Both must filter on the
// source so only matching rows land and verify agrees.

package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

const whereSeedMySQL = `
	CREATE TABLE users (
		id     BIGINT       NOT NULL,
		region VARCHAR(2)   NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

	CREATE TABLE widgets (
		id   BIGINT NOT NULL,
		name VARCHAR(64) NOT NULL,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

	INSERT INTO users (id, region) VALUES
		(1,'US'),(2,'US'),(3,'CA'),(4,'GB'),(5,'CA');

	INSERT INTO widgets (id, name) VALUES (100,'a'),(101,'b'),(102,'c');
`

func TestMigrate_WhereFilter_MySQLToPostgres(t *testing.T) {
	mysqlSource, _, mysqlCleanup := startMySQL(t)
	defer mysqlCleanup()
	_, pgTarget, pgCleanup := startPostgres(t)
	defer pgCleanup()

	applyMySQLDDL(t, mysqlSource, whereSeedMySQL)

	mysqlEng, ok := engines.Get("mysql")
	if !ok {
		t.Fatal("mysql engine not registered")
	}
	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	// Filter users to US (2 rows); leave widgets unfiltered (3 rows). No FK
	// between them, so no orphan concern — this test isolates the read/verify
	// push-down on the MySQL source.
	filters := map[string]string{"users": "region = 'US'"}
	mig := &Migrator{
		Source: mysqlEng, Target: pgEng,
		SourceDSN: mysqlSource, TargetDSN: pgTarget,
		RowFilters: filters,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}

	if got := pgScalarInt(t, pgTarget, `SELECT COUNT(*) FROM users`); got != 2 {
		t.Errorf("target users = %d; want 2 (only US)", got)
	}
	if got := pgScalarInt(t, pgTarget, `SELECT COUNT(*) FROM widgets`); got != 3 {
		t.Errorf("target widgets = %d; want 3 (unfiltered)", got)
	}
	if got := pgScalarInt(t, pgTarget, `SELECT COUNT(*) FROM users WHERE region <> 'US'`); got != 0 {
		t.Errorf("target has %d non-US users; want 0 (silent-leak guard)", got)
	}

	// verify threads the SAME predicate to the MySQL source count → PASS;
	// without it the MySQL source count (5) disagrees with the PG target
	// (2) → FAIL (non-vacuous).
	t.Run("verify with filter passes", func(t *testing.T) {
		res := runWhereVerifyPair(t, mysqlEng, pgEng, mysqlSource, pgTarget, filters)
		if res.Summary.TablesMismatch != 0 {
			t.Errorf("verify with filter: %d mismatched; want 0\n%+v", res.Summary.TablesMismatch, res.Tables)
		}
	})
	t.Run("verify without filter fails (non-vacuous)", func(t *testing.T) {
		res := runWhereVerifyPair(t, mysqlEng, pgEng, mysqlSource, pgTarget, nil)
		if !countMismatched(res, "users") {
			t.Errorf("expected users to mismatch without the filter\n%+v", res.Tables)
		}
	})
}

// runWhereVerifyPair is the cross-engine verify runner (distinct source and
// target engines), returning the result.
func runWhereVerifyPair(t *testing.T, src, tgt ir.Engine, sourceDSN, targetDSN string, filters map[string]string) *VerifyResult {
	t.Helper()
	v := &Verifier{
		Source: src, Target: tgt,
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		Depth:      VerifyDepthCount,
		RowFilters: filters,
		Out:        &strings.Builder{},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	res, err := v.Run(ctx)
	if err != nil {
		t.Fatalf("Verifier.Run: %v", err)
	}
	return res
}
