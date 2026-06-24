// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"sluicesync.dev/sluice/internal/ir"
)

// Bug #114 pin: buildOneIndex must wrap its CREATE INDEX exec in
// retryOnCatalogRace so a pg_class catalog-race 23505 (the unique_violation
// that fires when the ADR-0114 whole-phase reparent-retry re-issues a
// CREATE INDEX IF NOT EXISTS overlapping a just-committed concurrent build)
// is ridden out in-place instead of aborting the restore. A USER-table 23505
// must still surface loudly (the wart stays narrowly scoped per ADR-0038).
//
// This is the pin-catches-the-regression test: removing the retryOnCatalogRace
// wrap from buildOneIndex makes TestBuildOneIndex_RetriesCatalogRace FAIL (the
// 23505 escapes on the first attempt). The exec is injected via the
// indexStmtExec seam so the path is testable without a live connection.
func newCatalogRaceJob() indexBuildJob {
	return indexBuildJob{
		tableName: "widgets",
		idx: &ir.Index{
			Name:    "widgets_name_idx",
			Kind:    ir.IndexKindBTree,
			Columns: []ir.IndexColumn{{Column: "name"}},
		},
	}
}

func stubIndexStmtExec(t *testing.T, fn func() error) {
	t.Helper()
	orig := indexStmtExec
	indexStmtExec = func(_ context.Context, _ *sql.Conn, _ string) error { return fn() }
	t.Cleanup(func() { indexStmtExec = orig })
}

func TestBuildOneIndex_RetriesCatalogRace(t *testing.T) {
	calls := 0
	stubIndexStmtExec(t, func() error {
		calls++
		if calls == 1 {
			// The catalog-race shape: 23505 on pg_class's name-uniqueness index.
			return &pgconn.PgError{Code: "23505", ConstraintName: "pg_class_relname_nsp_index"}
		}
		return nil
	})

	w := &SchemaWriter{}
	// conn is nil: the stubbed exec ignores it.
	if err := w.buildOneIndex(context.Background(), nil, newCatalogRaceJob()); err != nil {
		t.Fatalf("buildOneIndex must ride the catalog-race 23505, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 exec attempts (1 catalog-race + 1 success), got %d", calls)
	}
}

func TestBuildOneIndex_PgTypeCatalogRaceAlsoRetried(t *testing.T) {
	calls := 0
	stubIndexStmtExec(t, func() error {
		calls++
		if calls == 1 {
			return &pgconn.PgError{Code: "23505", ConstraintName: "pg_type_typname_nsp_index"}
		}
		return nil
	})
	w := &SchemaWriter{}
	if err := w.buildOneIndex(context.Background(), nil, newCatalogRaceJob()); err != nil {
		t.Fatalf("buildOneIndex must ride the pg_type catalog-race, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 attempts, got %d", calls)
	}
}

// TestBuildOneIndex_UserDupKeyStaysLoud pins the scope: a 23505 that is NOT a
// catalog-race (a user-table / non-catalog constraint) must surface on the
// FIRST attempt with no retry — the catalog-race wart must never swallow a
// real uniqueness violation (ADR-0038 loud-failure).
func TestBuildOneIndex_UserDupKeyStaysLoud(t *testing.T) {
	calls := 0
	userErr := &pgconn.PgError{Code: "23505", ConstraintName: "widgets_sku_unique"}
	stubIndexStmtExec(t, func() error { calls++; return userErr })

	w := &SchemaWriter{}
	err := w.buildOneIndex(context.Background(), nil, newCatalogRaceJob())
	if !errors.Is(err, userErr) {
		t.Fatalf("a user-table 23505 must surface loudly unchanged, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("a non-catalog-race 23505 must NOT retry; got %d attempts", calls)
	}
}

// TestBuildOneIndex_NonCatalogErrorNoRetry: a plain non-23505 error (e.g. a
// syntax/relation error) returns immediately, unchanged, with no retry.
func TestBuildOneIndex_NonCatalogErrorNoRetry(t *testing.T) {
	calls := 0
	boom := errors.New("boom")
	stubIndexStmtExec(t, func() error { calls++; return boom })
	w := &SchemaWriter{}
	if err := w.buildOneIndex(context.Background(), nil, newCatalogRaceJob()); !errors.Is(err, boom) {
		t.Fatalf("non-catalog error must return unchanged, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("non-catalog error must not retry; got %d attempts", calls)
	}
}
