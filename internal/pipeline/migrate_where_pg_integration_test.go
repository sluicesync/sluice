//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// End-to-end integration coverage for ADR-0173 Phase 1 — the per-table
// `--where` row filter — on a Postgres source → Postgres target. It pins:
//
//   - only matching rows land; multiple --where keys in one run; an
//     unfiltered table alongside filtered ones; a zero-match filter
//     creates an empty table.
//   - verify threads the SAME predicate: a filtered migrate + `verify
//     --depth count` PASSES with the filter, and (the non-vacuous proof)
//     FAILS without it.
//   - FK-orphan handling: filtering a parent without --allow-degraded-fks
//     refuses loudly (coded, naming the parent); with it (PG target) the
//     FK degrades to NOT VALID and the child rows still land.
//   - the --where key matches the SOURCE table name even when the target
//     schema is renamed (--target-schema).
//
// The MySQL-source read path is pinned in the cross-engine sibling
// (migrate_where_cross_integration_test.go).

package pipeline

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"
	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
	"sluicesync.dev/sluice/internal/sluicecode"

	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

const whereSeedPG = `
	CREATE TABLE users (
		id     BIGINT PRIMARY KEY,
		region VARCHAR(2) NOT NULL
	);
	CREATE TABLE orders (
		id      BIGINT PRIMARY KEY,
		user_id BIGINT NOT NULL,
		region  VARCHAR(2) NOT NULL,
		CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES users (id)
	);
	CREATE TABLE widgets (
		id   BIGINT PRIMARY KEY,
		name TEXT NOT NULL
	);
	CREATE TABLE gadgets (
		id   BIGINT PRIMARY KEY,
		name TEXT NOT NULL
	);

	-- users: 2 US, 3 non-US.
	INSERT INTO users (id, region) VALUES
		(1,'US'),(2,'US'),(3,'CA'),(4,'GB'),(5,'CA');

	-- orders: each order's region matches its referenced user's region,
	-- so a consistent US filter on both leaves no orphan.
	INSERT INTO orders (id, user_id, region) VALUES
		(10,1,'US'),(11,2,'US'),(12,3,'CA'),(13,5,'CA'),(14,4,'GB');

	INSERT INTO widgets (id, name) VALUES (100,'a'),(101,'b'),(102,'c');
	INSERT INTO gadgets (id, name) VALUES (200,'x'),(201,'y');
`

func pgScalarInt(t *testing.T, dsn, query string) int64 {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var n int64
	if err := db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}

// freshPGTarget creates a new empty database in the same container as
// sourceDSN and returns a DSN pointing at it, so several migrate runs (each
// needing a cold-start-empty target) can share one container.
func freshPGTarget(t *testing.T, sourceDSN, dbName string) string {
	t.Helper()
	db, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := db.ExecContext(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Fatalf("create %s: %v", dbName, err)
	}
	dsn, err := buildPGDSN(sourceDSN, dbName)
	if err != nil {
		t.Fatalf("build DSN: %v", err)
	}
	return dsn
}

func TestMigrate_WhereFilter_PostgresToPostgres(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()
	applyPGDDL(t, sourceDSN, whereSeedPG)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	// ---- 1. Happy path: multi-key filter + an unfiltered table + a
	// zero-match filter, all in one run. users/orders filtered to US
	// (consistent, no orphan), widgets unfiltered, gadgets zero-match.
	filters := map[string]string{
		"users":   "region = 'US'",
		"orders":  "region = 'US'",
		"gadgets": "name = 'no-such-name'",
	}
	runWhereMigrate(t, pgEng, sourceDSN, targetDSN, &Migrator{RowFilters: filters})

	if got := pgScalarInt(t, targetDSN, `SELECT COUNT(*) FROM users`); got != 2 {
		t.Errorf("target users = %d; want 2 (only US)", got)
	}
	if got := pgScalarInt(t, targetDSN, `SELECT COUNT(*) FROM orders`); got != 2 {
		t.Errorf("target orders = %d; want 2 (only US)", got)
	}
	if got := pgScalarInt(t, targetDSN, `SELECT COUNT(*) FROM widgets`); got != 3 {
		t.Errorf("target widgets = %d; want 3 (unfiltered)", got)
	}
	// Zero-match: the table is CREATED but empty.
	if got := pgScalarInt(t, targetDSN, `SELECT COUNT(*) FROM gadgets`); got != 0 {
		t.Errorf("target gadgets = %d; want 0 (zero-match filter)", got)
	}
	// No US-region leaked in: every landed users row is US.
	if got := pgScalarInt(t, targetDSN, `SELECT COUNT(*) FROM users WHERE region <> 'US'`); got != 0 {
		t.Errorf("target has %d non-US users; want 0 (silent-leak guard)", got)
	}

	// ---- 2. verify threads the SAME predicate → PASS; without it → FAIL
	// (non-vacuous).
	t.Run("verify with the filter passes", func(t *testing.T) {
		res := runWhereVerify(t, pgEng, sourceDSN, targetDSN, filters)
		if res.Summary.TablesMismatch != 0 {
			t.Errorf("verify with filter: %d table(s) mismatched; want 0\n%+v",
				res.Summary.TablesMismatch, res.Tables)
		}
	})
	t.Run("verify WITHOUT the filter fails (non-vacuous)", func(t *testing.T) {
		res := runWhereVerify(t, pgEng, sourceDSN, targetDSN, nil)
		// users: source 5 vs target 2; orders: source 5 vs target 2.
		if res.Summary.TablesMismatch == 0 {
			t.Fatalf("verify without filter reported no mismatch; the threading test is vacuous\n%+v", res.Tables)
		}
		if !countMismatched(res, "users") {
			t.Errorf("expected users to mismatch without the filter\n%+v", res.Tables)
		}
	})
}

// TestMigrate_WhereFilter_ChunkedReadPath_PostgresToPostgres forces the
// parallel within-table chunked read (buildBatchedSelect /
// ReadRowsBatchBounded) — distinct from the full-scan path the small-table
// tests take — and confirms the --where predicate is ANDed into the keyset
// chunk bounds too, so a chunked filtered copy lands exactly the matching
// rows.
func TestMigrate_WhereFilter_ChunkedReadPath_PostgresToPostgres(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()
	applyPGDDL(t, sourceDSN, whereSeedPG)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	// Scope to the FK-free widgets table (integer PK) and force chunking:
	// BulkParallelMinRows=1 puts even a tiny table on the parallel path, and
	// BulkParallelism=2 splits its PK range.
	filter, err := migcore.NewTableFilter([]string{"widgets"}, nil)
	if err != nil {
		t.Fatalf("table filter: %v", err)
	}
	mig := &Migrator{
		Source: pgEng, Target: pgEng,
		SourceDSN: sourceDSN, TargetDSN: targetDSN,
		Filter:              filter,
		BulkParallelism:     2,
		BulkParallelMinRows: 1,
		RowFilters:          map[string]string{"widgets": "name IN ('a','c')"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if got := pgScalarInt(t, targetDSN, `SELECT COUNT(*) FROM widgets`); got != 2 {
		t.Errorf("chunked filtered widgets = %d; want 2 (only 'a','c')", got)
	}
	if got := pgScalarInt(t, targetDSN, `SELECT COUNT(*) FROM widgets WHERE name = 'b'`); got != 0 {
		t.Errorf("chunked read leaked the filtered-out 'b' row: got %d; want 0", got)
	}
}

func TestMigrate_WhereFilter_FKOrphan_PostgresToPostgres(t *testing.T) {
	sourceDSN, targetDSN, cleanup := startPostgres(t)
	defer cleanup()
	applyPGDDL(t, sourceDSN, whereSeedPG)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}

	// Filter the PARENT (users) but NOT the child (orders): orders 12/13/14
	// reference now-excluded non-US users → the deferred FK add hits 23503.
	orphanFilter := map[string]string{"users": "region = 'US'"}

	t.Run("without --allow-degraded-fks: coded refusal naming the parent", func(t *testing.T) {
		mig := &Migrator{
			Source: pgEng, Target: pgEng,
			SourceDSN: sourceDSN, TargetDSN: targetDSN,
			RowFilters: orphanFilter,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		err := mig.Run(ctx)
		if err == nil {
			t.Fatal("migrate with an orphaning --where succeeded; want a loud refusal")
		}
		ce, ok := sluicecode.FromError(err)
		if !ok || ce.Code != sluicecode.CodeWhereFilterFKOrphan {
			t.Fatalf("want %s; got %v", sluicecode.CodeWhereFilterFKOrphan, err)
		}
		if !strings.Contains(err.Error(), "users") {
			t.Errorf("refusal does not name the filtered parent 'users': %v", err)
		}
	})

	t.Run("with --allow-degraded-fks (PG target): degrades to NOT VALID, child rows land", func(t *testing.T) {
		degradedTarget := freshPGTarget(t, sourceDSN, "degraded_target")
		mig := &Migrator{
			Source: pgEng, Target: pgEng,
			SourceDSN: sourceDSN, TargetDSN: degradedTarget,
			RowFilters:       orphanFilter,
			AllowDegradedFKs: true,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := mig.Run(ctx); err != nil {
			t.Fatalf("migrate with --allow-degraded-fks failed: %v", err)
		}
		// users filtered (US only), orders unfiltered (all 5 land).
		if got := pgScalarInt(t, degradedTarget, `SELECT COUNT(*) FROM users`); got != 2 {
			t.Errorf("degraded target users = %d; want 2", got)
		}
		if got := pgScalarInt(t, degradedTarget, `SELECT COUNT(*) FROM orders`); got != 5 {
			t.Errorf("degraded target orders = %d; want 5 (unfiltered)", got)
		}
		// The FK exists but is NOT VALID (convalidated = false) — no silent
		// orphan; the constraint still guards new writes.
		notValid := pgScalarInt(t, degradedTarget, `
			SELECT COUNT(*) FROM pg_constraint
			WHERE conname = 'orders_user_fk' AND contype = 'f' AND NOT convalidated`)
		if notValid != 1 {
			t.Errorf("orders_user_fk NOT VALID count = %d; want 1 (degraded, not silently dropped)", notValid)
		}
	})
}

// TestMigrate_WhereFilter_KeyMatchesSourceUnderTargetRename pins that the
// --where key is the SOURCE table name even when the target schema is
// renamed (--target-schema, ADR-0031) — the source-keyed semantics --redact
// uses. If the key were resolved against the target namespace it would miss.
func TestMigrate_WhereFilter_KeyMatchesSourceUnderTargetRename(t *testing.T) {
	sourceDSN, _, cleanup := startPostgres(t)
	defer cleanup()
	applyPGDDL(t, sourceDSN, whereSeedPG)

	pgEng, ok := engines.Get("postgres")
	if !ok {
		t.Fatal("postgres engine not registered")
	}
	renamedTarget := freshPGTarget(t, sourceDSN, "renamed_target")

	// Scope to the FK-free `widgets` table only: this test is about the
	// filter KEY resolving against the SOURCE name under a target rename, and
	// --target-schema + cross-table FKs is a separate (pre-existing)
	// interaction we deliberately keep out of scope here.
	filter, err := migcore.NewTableFilter([]string{"widgets"}, nil)
	if err != nil {
		t.Fatalf("table filter: %v", err)
	}
	mig := &Migrator{
		Source: pgEng, Target: pgEng,
		SourceDSN: sourceDSN, TargetDSN: renamedTarget,
		TargetSchema: "app_prod",
		Filter:       filter,
		// The key is the SOURCE name "widgets" even though the target table
		// lands in the renamed schema app_prod.
		RowFilters: map[string]string{"widgets": "name = 'a'"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// The filter (keyed by source name "widgets") applied even though the
	// target table lives in the renamed schema app_prod.
	if got := pgScalarInt(t, renamedTarget, `SELECT COUNT(*) FROM app_prod.widgets`); got != 1 {
		t.Errorf("app_prod.widgets = %d; want 1 (source-keyed --where applied under --target-schema)", got)
	}
}

// runWhereMigrate runs a same-engine PG migrate, filling in the engine +
// DSN fields the caller left on the Migrator.
func runWhereMigrate(t *testing.T, eng ir.Engine, sourceDSN, targetDSN string, mig *Migrator) {
	t.Helper()
	mig.Source = eng
	mig.Target = eng
	mig.SourceDSN = sourceDSN
	mig.TargetDSN = targetDSN
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := mig.Run(ctx); err != nil {
		t.Fatalf("Migrator.Run: %v", err)
	}
}

// runWhereVerify runs `verify --depth count` with the given source-side
// row filters and returns the result.
func runWhereVerify(t *testing.T, eng ir.Engine, sourceDSN, targetDSN string, filters map[string]string) *VerifyResult {
	t.Helper()
	v := &Verifier{
		Source: eng, Target: eng,
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

func countMismatched(res *VerifyResult, table string) bool {
	for _, tr := range res.Tables {
		if tr.Name == table && tr.CountMismatch {
			return true
		}
	}
	return false
}
