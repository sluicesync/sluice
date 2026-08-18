// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// TestColTypesFor_SkipVerdictShortCircuitsProbes is the audit-P-1 cost gate for
// the PG applier — where the crawl is heaviest (an enum-type sweep + the
// columns query + both PostGIS view lookups per event). A CONFIRMED skip
// verdict must short-circuit loadColumnTypes entirely: the applier points at an
// UNREACHABLE DSN, so with the short-circuit present colTypesFor returns
// errUnknownTable WITHOUT probing; if it is removed, the dead-DSN probe returns
// a connection error instead. Mutation-verified.
func TestColTypesFor_SkipVerdictShortCircuitsProbes(t *testing.T) {
	db, err := sql.Open("pgx", "postgres://u@127.0.0.1:1/nodb?connect_timeout=1")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer func() { _ = db.Close() }()

	a := &ChangeApplier{db: db}
	qn := schemaTableKey("app", "orders")
	a.storeSkipVerdict(qn)

	_, gotErr := a.colTypesFor(context.Background(), "app", "orders")
	if !errors.Is(gotErr, errUnknownTable) {
		t.Fatalf("colTypesFor on a cached-skip table returned %v; want errUnknownTable WITHOUT probing (audit P-1)", gotErr)
	}
}

// TestSkipVerdictCache_TTLAndInvalidation pins the negative cache's semantics
// on the PG applier (store within TTL → cached; past TTL → re-probe; DDL
// barrier → dropped).
func TestSkipVerdictCache_TTLAndInvalidation(t *testing.T) {
	a := &ChangeApplier{}
	qn := schemaTableKey("app", "orders")

	if a.cachedSkipVerdict(qn) {
		t.Fatal("empty cache must not report a verdict")
	}
	a.storeSkipVerdict(qn)
	if !a.cachedSkipVerdict(qn) {
		t.Fatal("a stored verdict must be cached within TTL")
	}

	orig := skipVerdictTTL
	skipVerdictTTL = -1 * time.Second
	if a.cachedSkipVerdict(qn) {
		t.Error("a verdict past its TTL must NOT be cached (re-probe for add-table recovery)")
	}
	skipVerdictTTL = orig

	if !a.cachedSkipVerdict(qn) {
		t.Fatal("verdict should be live again after restoring TTL")
	}
	a.invalidateMetadataCaches(qn)
	if a.cachedSkipVerdict(qn) {
		t.Error("invalidateMetadataCaches must drop the skip verdict")
	}
}
