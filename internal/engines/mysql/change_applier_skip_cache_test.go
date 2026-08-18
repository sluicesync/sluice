// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// TestColTypesFor_SkipVerdictShortCircuitsProbes is the audit-P-1 cost gate:
// a table with a CONFIRMED skip verdict must NOT re-run colTypesFor's catalog
// probe chain — the per-event crawl H-4's UPSERT coalescing left behind. The
// applier points at an UNREACHABLE DSN so any probe fails fast; with the
// negative-cache short-circuit present, colTypesFor returns the
// errUnknownTargetTable sentinel WITHOUT probing, so the unreachable DSN is
// never touched. If the short-circuit is removed, colTypesFor calls
// loadTableSchema against the dead DSN and returns a connection error — NOT
// errUnknownTargetTable — so this test fails. Mutation-verified.
func TestColTypesFor_SkipVerdictShortCircuitsProbes(t *testing.T) {
	// Port 1 is unusable; sql.Open is lazy, so the *sql.DB constructs fine and
	// only a real query would try (and fail) to connect.
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:1)/nodb?timeout=1s")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	a := &ChangeApplier{db: db}
	qn := qualifiedName("app", "orders")
	a.storeSkipVerdict(qn) // the CONFIRMED skip verdict a prior probe reached

	_, gotErr := a.colTypesFor(context.Background(), nil, "app", "orders")
	if !errors.Is(gotErr, errUnknownTargetTable) {
		t.Fatalf("colTypesFor on a cached-skip table returned %v; want errUnknownTargetTable "+
			"WITHOUT probing — the short-circuit either is gone (probe hit the dead DSN) or misclassified (audit P-1)", gotErr)
	}
}

// TestSkipVerdictCache_TTLAndInvalidation pins the negative cache's semantics:
// a stored verdict is cached within TTL, EXPIRES past it (so a mid-stream
// `schema add-table` recovery is re-probed), and a schema-change invalidation
// drops it. skipVerdictTTL is a package var so expiry is deterministic.
func TestSkipVerdictCache_TTLAndInvalidation(t *testing.T) {
	a := &ChangeApplier{}
	qn := qualifiedName("app", "orders")

	if a.cachedSkipVerdict(qn) {
		t.Fatal("empty cache must not report a verdict")
	}
	a.storeSkipVerdict(qn)
	if !a.cachedSkipVerdict(qn) {
		t.Fatal("a stored verdict must be cached within TTL")
	}

	// Expiry: a negative TTL makes every stored verdict stale, forcing a
	// re-probe — the property that preserves add-table mid-stream recovery.
	orig := skipVerdictTTL
	skipVerdictTTL = -1 * time.Second
	if a.cachedSkipVerdict(qn) {
		t.Error("a verdict past its TTL must NOT be cached (re-probe for add-table recovery)")
	}
	skipVerdictTTL = orig

	// A same-stream DDL barrier drops the verdict so the table is picked up
	// immediately rather than waiting out the TTL.
	if !a.cachedSkipVerdict(qn) {
		t.Fatal("verdict should be live again after restoring TTL")
	}
	a.invalidateMetadataCaches(qn)
	if a.cachedSkipVerdict(qn) {
		t.Error("invalidateMetadataCaches must drop the skip verdict (DDL/add-table pickup)")
	}
}
