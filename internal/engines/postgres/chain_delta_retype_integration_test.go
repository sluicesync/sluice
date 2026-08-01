//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Ground truth for the chain-replay retype fix, against a real
// Postgres: an incremental window that carries ONLY a column-type
// change (no added columns) must reach the target's
// `ALTER TABLE ... ALTER COLUMN ... TYPE`, because the alternative is
// not a visible error — Postgres ROUNDS a numeric(p,s) on insert and
// returns success.
//
// The first half of the test pins that premise on the server rather
// than asserting it from memory; the second half runs the real
// migcore.ApplyAlterDelta through the real SchemaWriter and shows the
// same value surviving intact.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

func TestChainDeltaRetype_WidensNumericOnRealPostgres(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx,
		`CREATE TABLE "public"."orders" (id BIGINT PRIMARY KEY, amount NUMERIC(10,2))`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// PREMISE: the restore recreates the column at its pre-window width
	// and the replay then writes a post-window value into it. Postgres
	// does NOT error — it rounds and returns success, which is exactly
	// why a skipped retype is silent loss rather than a failed restore.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO "public"."orders" (id, amount) VALUES (1, 12345.12345678)`); err != nil {
		t.Fatalf("insert into the narrow column: %v", err)
	}
	var narrow string
	if err := db.QueryRowContext(ctx,
		`SELECT amount::text FROM "public"."orders" WHERE id = 1`).Scan(&narrow); err != nil {
		t.Fatalf("read back narrow value: %v", err)
	}
	if narrow != "12345.12" {
		t.Fatalf("narrow amount = %q; want the silently ROUNDED 12345.12 — the premise of the fix no longer holds on this server", narrow)
	}

	// The window's delta: a pure retype, NO added columns — the shape
	// both replay sites used to `continue` past.
	before := &ir.Table{Schema: "public", Name: "orders", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "amount", Type: ir.Decimal{Precision: 10, Scale: 2}, Nullable: true},
	}}
	after := &ir.Table{Schema: "public", Name: "orders", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "amount", Type: ir.Decimal{Precision: 20, Scale: 8}, Nullable: true},
	}}
	if got := migcore.DiffSchemas(
		&ir.Schema{Tables: []*ir.Table{before}},
		&ir.Schema{Tables: []*ir.Table{after}},
	); len(got) != 1 || got[0].Kind != irbackup.SchemaDeltaAlterTable {
		t.Fatalf("DiffSchemas = %+v; want one alter_table entry", got)
	}

	eng := Engine{}
	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() {
		if c, ok := sw.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	if err := migcore.ApplyAlterDelta(ctx, sw, &irbackup.SchemaDeltaEntry{
		Kind:   irbackup.SchemaDeltaAlterTable,
		Schema: "public",
		Table:  "orders",
		Before: before,
		After:  after,
	}, migcore.AlterDeltaContext{
		SourceEngine: "postgres", TargetEngine: "postgres",
		BackupID: "incr-0001", Origin: "chain restore",
	}); err != nil {
		t.Fatalf("ApplyAlterDelta: %v", err)
	}

	var precision, scale int
	if err := db.QueryRowContext(
		ctx, `
		SELECT numeric_precision, numeric_scale
		  FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = 'orders' AND column_name = 'amount'`,
	).Scan(&precision, &scale); err != nil {
		t.Fatalf("read column type: %v", err)
	}
	if precision != 20 || scale != 8 {
		t.Fatalf("amount is numeric(%d,%d); want numeric(20,8) — the delta was not applied", precision, scale)
	}

	if _, err := db.ExecContext(ctx,
		`INSERT INTO "public"."orders" (id, amount) VALUES (2, 12345.12345678)`); err != nil {
		t.Fatalf("insert into the widened column: %v", err)
	}
	var wide string
	if err := db.QueryRowContext(ctx,
		`SELECT amount::text FROM "public"."orders" WHERE id = 2`).Scan(&wide); err != nil {
		t.Fatalf("read back widened value: %v", err)
	}
	if wide != "12345.12345678" {
		t.Errorf("amount = %q; want 12345.12345678 preserved exactly", wide)
	}
}

// TestChainDeltaRetype_DroppedColumnRefusesOnRealPostgres pins the
// other half of the disposition rule against a live target: a shape
// with no faithful replay refuses BEFORE it touches the schema, so the
// target is left exactly as it was.
func TestChainDeltaRetype_DroppedColumnRefusesOnRealPostgres(t *testing.T) {
	dsn, cleanup := startPostgresForApplier(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx,
		`CREATE TABLE "public"."shipments" (id BIGINT PRIMARY KEY, legacy_note TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	before := &ir.Table{Schema: "public", Name: "shipments", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "legacy_note", Type: ir.Text{}, Nullable: true},
	}}
	after := &ir.Table{Schema: "public", Name: "shipments", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
	}}

	eng := Engine{}
	sw, err := eng.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() {
		if c, ok := sw.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	err = migcore.ApplyAlterDelta(ctx, sw, &irbackup.SchemaDeltaEntry{
		Kind: irbackup.SchemaDeltaAlterTable, Schema: "public", Table: "shipments",
		Before: before, After: after,
	}, migcore.AlterDeltaContext{
		SourceEngine: "postgres", TargetEngine: "postgres",
		BackupID: "incr-0002", Origin: "chain restore",
	})
	if err == nil {
		t.Fatal("ApplyAlterDelta on a dropped column returned nil; want the coded refusal")
	}
	var present int
	if err := db.QueryRowContext(
		ctx, `
		SELECT count(*) FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = 'shipments' AND column_name = 'legacy_note'`,
	).Scan(&present); err != nil {
		t.Fatalf("check column: %v", err)
	}
	if present != 1 {
		t.Error("the refused delta still dropped the column — a refusal must touch nothing")
	}
}
