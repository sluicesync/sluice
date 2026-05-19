//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// ADR-0049 Chunk C — integration pins for the PG applier-side
// active-version cache + boundary swap. Mirrors the MySQL sibling
// (change_applier_schema_cache_integration_test.go).

package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/orware/sluice/internal/ir"
)

func TestPrimeSchemaHistoryCache_Integration_SeedsFromStorage(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const schema = "public"
	if err := ensureSchemaHistoryTable(ctx, db, schema); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	mkPos := func(lsn string) ir.Position {
		p, err := encodePGPos(pgPos{Slot: "s1", LSN: lsn})
		if err != nil {
			t.Fatalf("encodePGPos: %v", err)
		}
		return p
	}

	anchorUsers := mkPos("0/1000000")
	anchorOrders := mkPos("0/2000000")
	tblUsers := &ir.Table{Schema: schema, Name: "users", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "email", Type: ir.Varchar{Length: 255}, Nullable: true},
	}}
	tblOrders := &ir.Table{Schema: schema, Name: "orders", Columns: []*ir.Column{
		{Name: "order_id", Type: ir.Integer{Width: 64}},
	}}
	if err := writeSchemaVersion(ctx, db, schema, "stream-1", schema, "users", anchorUsers, tblUsers); err != nil {
		t.Fatalf("seed users: %v", err)
	}
	if err := writeSchemaVersion(ctx, db, schema, "stream-1", schema, "orders", anchorOrders, tblOrders); err != nil {
		t.Fatalf("seed orders: %v", err)
	}

	eng := Engine{}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	a := applier.(*ChangeApplier)

	resumePos := mkPos("0/3000000")
	if err := a.PrimeSchemaHistoryCache(ctx, "stream-1", resumePos); err != nil {
		t.Fatalf("PrimeSchemaHistoryCache: %v", err)
	}

	if got := a.resolveCallsForTest.Load(); got != 2 {
		t.Errorf("resolveCallsForTest after prime: got %d, want 2 (one per primed table)", got)
	}

	gotU, okU := a.ActiveSchema(schema, "users")
	gotO, okO := a.ActiveSchema(schema, "orders")
	if !okU || !okO {
		t.Fatalf("ActiveSchema lookup after prime: users ok=%v, orders ok=%v (want both true)", okU, okO)
	}
	if len(gotU.Columns) != 2 || gotU.Columns[1].Name != "email" {
		t.Errorf("users IR after prime: got %d cols (want 2)", len(gotU.Columns))
	}
	if len(gotO.Columns) != 1 || gotO.Columns[0].Name != "order_id" {
		t.Errorf("orders IR after prime: cols=%+v (want 1 col, order_id)", gotO.Columns)
	}
}

func TestPrimeSchemaHistoryCache_Integration_BrandNewStreamIsNoOp(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	eng := Engine{}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	a := applier.(*ChangeApplier)

	if err := a.PrimeSchemaHistoryCache(ctx, "fresh", ir.Position{}); err != nil {
		t.Fatalf("brand-new-stream prime: %v", err)
	}
	if got := a.resolveCallsForTest.Load(); got != 0 {
		t.Errorf("brand-new-stream resolveCallsForTest: got %d, want 0", got)
	}
	if _, ok := a.ActiveSchema("public", "anything"); ok {
		t.Error("ActiveSchema after brand-new-stream prime: expected miss (cache empty)")
	}
}

func TestPrimeSchemaHistoryCache_Integration_ResumeWithNoHistory(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	eng := Engine{}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable: %v", err)
	}
	a := applier.(*ChangeApplier)

	resumePos, err := encodePGPos(pgPos{Slot: "s1", LSN: "0/9000000"})
	if err != nil {
		t.Fatalf("encodePGPos: %v", err)
	}
	if err := a.PrimeSchemaHistoryCache(ctx, "no-history-stream", resumePos); err != nil {
		t.Fatalf("resume-with-no-history prime: %v", err)
	}
	if got := a.resolveCallsForTest.Load(); got != 0 {
		t.Errorf("resume-with-no-history resolveCallsForTest: got %d, want 0", got)
	}
	if _, ok := a.ActiveSchema("public", "users"); ok {
		t.Error("ActiveSchema after no-history prime: expected miss (cache empty)")
	}
}

func TestPrimeSchemaHistoryCache_Integration_BelowFloorIsLoud(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	const schema = "public"
	if err := ensureSchemaHistoryTable(ctx, db, schema); err != nil {
		t.Fatalf("ensureSchemaHistoryTable: %v", err)
	}

	mkPos := func(lsn string) ir.Position {
		p, err := encodePGPos(pgPos{Slot: "s1", LSN: lsn})
		if err != nil {
			t.Fatalf("encodePGPos: %v", err)
		}
		return p
	}

	anchor := mkPos("0/2000000")
	tbl := &ir.Table{Schema: schema, Name: "users", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}}}
	if err := writeSchemaVersion(ctx, db, schema, "stream-below", schema, "users", anchor, tbl); err != nil {
		t.Fatalf("seed: %v", err)
	}

	eng := Engine{}
	applier, err := eng.OpenChangeApplier(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	a := applier.(*ChangeApplier)

	pBelow := mkPos("0/1000000")
	err = a.PrimeSchemaHistoryCache(ctx, "stream-below", pBelow)
	if err == nil {
		t.Fatal("below-floor prime must surface a loud error (got nil)")
	}
	if !errors.Is(err, ir.ErrPositionInvalid) {
		t.Fatalf("below-floor prime error must wrap ir.ErrPositionInvalid; got: %v", err)
	}
}
