//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Regression test for ADR-0042 finding N1: PG CountRows must not
// report ~0 for a freshly-loaded, never-ANALYZEd table. pg_class.
// reltuples is -1 (PG14+) / 0 (older) until autovacuum/ANALYZE
// runs, and shouldParallelChunk consumes CountRows for parallel-
// copy eligibility — so a stale ~0 silently disabled parallel copy
// on the normal migrate cold-start (load/restore a source, then
// migrate). The fix: reltuples<=0 falls back to an exact COUNT(*).

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestCountRows_N1_NeverAnalyzedSourceUsesExactCount(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	const populated = 1000
	stmt := `
		CREATE TABLE "public"."n1demo" (id BIGINT PRIMARY KEY, v TEXT NOT NULL);
		CREATE TABLE "public"."n1empty" (id BIGINT PRIMARY KEY);
		INSERT INTO "public"."n1demo" (id, v)
		SELECT g, 'row-' || g FROM generate_series(1, ` + fmt.Sprint(populated) + `) g;
	`
	// Deliberately NO ANALYZE — this is the cold-start state N1 is about.
	applyPGApplier(t, dsn, stmt)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rr, err := Engine{}.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer func() {
		if c, ok := rr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	rc, ok := rr.(ir.RowCounter)
	if !ok {
		t.Fatalf("postgres RowReader does not implement ir.RowCounter")
	}

	// Populated-but-never-analyzed: reltuples is the -1/0 sentinel;
	// the exact-COUNT(*) fallback must return the true cardinality.
	// Pre-fix this returned 0 and the table never parallelized.
	got, err := rc.CountRows(ctx, &ir.Table{Name: "n1demo"})
	if err != nil {
		t.Fatalf("CountRows(n1demo): %v", err)
	}
	if got != populated {
		t.Errorf("CountRows(n1demo) = %d; want %d (exact-count fallback for stale reltuples — ADR-0042 N1)", got, populated)
	}

	// Genuinely empty: non-positive reltuples here too, but the
	// exact count is legitimately 0 — must not error or misreport.
	gotEmpty, err := rc.CountRows(ctx, &ir.Table{Name: "n1empty"})
	if err != nil {
		t.Fatalf("CountRows(n1empty): %v", err)
	}
	if gotEmpty != 0 {
		t.Errorf("CountRows(n1empty) = %d; want 0", gotEmpty)
	}

	// After ANALYZE, reltuples is populated and positive: the fast
	// catalog path short-circuits before the seq scan and still
	// reports a sane count (estimate may differ slightly from exact;
	// assert it's in a tight band, not exact).
	applyPGApplier(t, dsn, `ANALYZE "public"."n1demo";`)
	gotAnalyzed, err := rc.CountRows(ctx, &ir.Table{Name: "n1demo"})
	if err != nil {
		t.Fatalf("CountRows(n1demo, post-ANALYZE): %v", err)
	}
	if gotAnalyzed < populated*9/10 || gotAnalyzed > populated*11/10 {
		t.Errorf("CountRows(n1demo, post-ANALYZE) = %d; want ~%d (reltuples fast path)", gotAnalyzed, populated)
	}
}
