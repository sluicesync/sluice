//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestRowReader_EstimateTableBytes_ReadsSourceSize pins the ADR-0184
// source-size estimator against a real Postgres: the simple-mode reader
// (the one migrate hands to migcore.ApplyIndexSplitSizeHint) must report a
// real on-disk byte size for a seeded table — this is what arms the
// per-index ALTER split on a Postgres→PlanetScale/Vitess migrate, which was
// inert for every Postgres source before the estimator existed — and must
// report 0 ("unknown", never "empty") for a table Postgres has no relation
// for, so an absent estimate can never masquerade as a size.
func TestRowReader_EstimateTableBytes_ReadsSourceSize(t *testing.T) {
	dsn, cleanup := newSharedPGDB(t, "bytesize_est_db")
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `CREATE TABLE bytes_est (id INT PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO bytes_est SELECT g, repeat('x', 100) FROM generate_series(1, 2000) g`); err != nil {
		t.Fatalf("seed rows: %v", err)
	}

	eng := Engine{}
	reader, err := eng.OpenRowReader(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer func() {
		if c, ok := reader.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	est, ok := reader.(ir.TableByteSizeEstimator)
	if !ok {
		t.Fatal("the simple-mode postgres RowReader does not implement ir.TableByteSizeEstimator — the ADR-0184 split is inert for Postgres sources again")
	}

	table := &ir.Table{Name: "bytes_est", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}}}
	n, err := est.EstimateTableBytes(ctx, table)
	if err != nil {
		t.Fatalf("EstimateTableBytes: %v", err)
	}
	// 2000 rows × ~100-byte payloads is at minimum several 8 KB heap pages;
	// asserting a real floor (not just >0) proves the query read the actual
	// relation size rather than some truthy constant.
	if n < 8192 {
		t.Fatalf("EstimateTableBytes(bytes_est) = %d; want >= 8192 (a seeded table has at least one heap page)", n)
	}

	// Unknown relation → (0, nil): "no estimate", never an error and never
	// a fabricated size — the writer must keep its safe combined ALTER.
	missing := &ir.Table{Name: "no_such_table_anywhere", Columns: table.Columns}
	n, err = est.EstimateTableBytes(ctx, missing)
	if err != nil {
		t.Fatalf("EstimateTableBytes(missing): %v", err)
	}
	if n != 0 {
		t.Fatalf("EstimateTableBytes(missing) = %d; want 0", n)
	}
}
