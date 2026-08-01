//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The MySQL half of the chain-replay retype ground truth (the PG half
// lives in internal/engines/postgres/chain_delta_retype_integration_test.go).
// The shared classifier dispatches to a per-engine DDL surface, so the
// target engine is a family axis of its own: a green PG pin says
// nothing about what MySQL's writer emits for the same delta.
//
// MySQL rounds a too-wide DECIMAL the same way Postgres does — in the
// default (non-strict-for-this-case) rounding sense: the INSERT
// succeeds with a note, not an error — so a skipped retype is the same
// silent loss here.

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/migcore"
)

func TestChainDeltaRetype_WidensDecimalOnRealMySQL(t *testing.T) {
	dsn, cleanup := startMySQL(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.ExecContext(ctx,
		"CREATE TABLE `orders_retype` (id BIGINT PRIMARY KEY, amount DECIMAL(10,2))"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// PREMISE, ground-truthed rather than remembered: the narrow column
	// accepts the wide value and stores a rounded one.
	if _, err := db.ExecContext(ctx,
		"INSERT INTO `orders_retype` (id, amount) VALUES (1, 12345.12345678)"); err != nil {
		t.Fatalf("insert into the narrow column: %v", err)
	}
	var narrow string
	if err := db.QueryRowContext(ctx,
		"SELECT CAST(amount AS CHAR) FROM `orders_retype` WHERE id = 1").Scan(&narrow); err != nil {
		t.Fatalf("read back narrow value: %v", err)
	}
	if narrow != "12345.12" {
		t.Fatalf("narrow amount = %q; want the silently ROUNDED 12345.12 — the premise of the fix no longer holds on this server", narrow)
	}

	before := &ir.Table{Name: "orders_retype", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "amount", Type: ir.Decimal{Precision: 10, Scale: 2}, Nullable: true},
	}}
	after := &ir.Table{Name: "orders_retype", Columns: []*ir.Column{
		{Name: "id", Type: ir.Integer{Width: 64}},
		{Name: "amount", Type: ir.Decimal{Precision: 20, Scale: 8}, Nullable: true},
	}}

	sw, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("open schema writer: %v", err)
	}
	defer func() {
		if c, ok := sw.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	if err := migcore.ApplyAlterDelta(ctx, sw, &irbackup.SchemaDeltaEntry{
		Kind: irbackup.SchemaDeltaAlterTable, Table: "orders_retype",
		Before: before, After: after,
	}, migcore.AlterDeltaContext{
		SourceEngine: "mysql", TargetEngine: "mysql",
		BackupID: "incr-0001", Origin: "chain restore",
	}); err != nil {
		t.Fatalf("ApplyAlterDelta: %v", err)
	}

	var precision, scale int
	if err := db.QueryRowContext(
		ctx, `
		SELECT numeric_precision, numeric_scale
		  FROM information_schema.columns
		 WHERE table_schema = DATABASE() AND table_name = 'orders_retype' AND column_name = 'amount'`,
	).Scan(&precision, &scale); err != nil {
		t.Fatalf("read column type: %v", err)
	}
	if precision != 20 || scale != 8 {
		t.Fatalf("amount is decimal(%d,%d); want decimal(20,8) — the delta was not applied", precision, scale)
	}

	if _, err := db.ExecContext(ctx,
		"INSERT INTO `orders_retype` (id, amount) VALUES (2, 12345.12345678)"); err != nil {
		t.Fatalf("insert into the widened column: %v", err)
	}
	var wide string
	if err := db.QueryRowContext(ctx,
		"SELECT CAST(amount AS CHAR) FROM `orders_retype` WHERE id = 2").Scan(&wide); err != nil {
		t.Fatalf("read back widened value: %v", err)
	}
	if wide != "12345.12345678" {
		t.Errorf("amount = %q; want 12345.12345678 preserved exactly", wide)
	}
}
