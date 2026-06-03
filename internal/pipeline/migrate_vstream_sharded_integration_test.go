//go:build integration && vstream

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Track-1a CI-SMOKE: static-sharded Vitess source -> sluice Migrator
// -> src==dst. This is the cheap, fast half of the Track-1a Vitess
// validation (the heavy reshard-chaos core lives under the separate
// `vitessreshard` tag in internal/engines/mysql). It runs under the
// EXISTING `vstream` tag so it executes in the normal vstream
// integration pass alongside the engine-level VStream basics — no new
// image cost (vitess/vttestserver is already the vstream tag's
// defining image).
//
// REUSE (per the Track-1a Phase-A mandate — generalise, don't
// reinvent): every container helper here is the package-private
// scaffolding the Roadmap-#4 Shape-A spike already proved out, in the
// same package + same build tag:
//   - startShardedVTTestServer  (sharded vttestserver, N shards)
//   - startMySQLTarget / startPGTarget (stock testcontainers targets)
//   - applySQL / pgRowCount     (DDL/DML + read-back)
//   - closeIf / ctx2min         (pipeline package helpers)
//
// What this adds over the engine-level VStream suite: the FULL
// Migrator path (schema read -> create -> bulk copy -> indexes ->
// constraints) against a *multi-shard* source, asserting the
// scatter/cross-shard bulk read returns every row exactly once on
// both a same-engine (Vitess->MySQL) and cross-engine (Vitess->PG)
// target. It also exercises Vitess's no-runtime-FK behaviour vs
// sluice's FK-DDL phase (the source declares an FK; vttestserver does
// not enforce it at runtime, but sluice still emits the constraint on
// the target).
//
// Usage:
//
//	go test -tags='integration vstream' -v -count=1 -timeout=20m \
//	  -run 'TestMigrate_VStreamShardedSource' ./internal/pipeline/...

package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/engines"

	_ "sluicesync.dev/sluice/internal/engines/mysql"
	_ "sluicesync.dev/sluice/internal/engines/postgres"
)

// TestMigrate_VStreamShardedSource_SrcEqualsDst migrates a 2-shard
// Vitess keyspace through the Migrator into a fresh target and
// asserts src==dst by full per-PK content comparison (not just
// "Run returned nil" and not just a row count).
//
// Topology: keyspace `commerce`, shards -80 / 80-, two related
// tables:
//
//	customer(customer_id PK, email, region)         hash-vindexed
//	orders  (order_id PK, customer_id FK->customer, amount_cents)
//
// The hash vindex scatters rows across both shards; the Migrator's
// bulk-copy phase must fan the per-shard reads back into one
// per-table row set with no gap and no dup. The FK is declared on
// the source schema: Vitess does NOT enforce it at runtime
// (well-known Vitess behaviour), but sluice's constraint phase still
// re-creates it on the target — verified by reading the target's
// constraint back.
func TestMigrate_VStreamShardedSource_SrcEqualsDst(t *testing.T) {
	const keyspace = "commerce"

	cases := []struct {
		name       string
		targetKind string // "mysql" | "pg"
		targetEng  string
	}{
		{"VitessShardedToMySQL", "mysql", "mysql"},
		{"VitessShardedToPostgres", "pg", "postgres"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// grpcEndpoint (VStream) is not needed: a static-sharded
			// cold migrate is pure SQL over vtgate's MySQL frontend.
			mysqlDSN, _, vtCleanup := startShardedVTTestServer(t, keyspace, 2)
			defer vtCleanup()

			// Two related tables. customer is hash-vindexed so vtgate
			// scatters its rows across -80/80-; orders carries an FK
			// to customer (declared, not runtime-enforced by Vitess).
			applySQL(t, mysqlDSN, `
				CREATE TABLE customer (
					customer_id BIGINT       NOT NULL,
					email       VARCHAR(255) NOT NULL,
					region      VARCHAR(64)  NOT NULL,
					PRIMARY KEY (customer_id)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
			applySQL(t, mysqlDSN, `
				CREATE TABLE orders (
					order_id    BIGINT NOT NULL,
					customer_id BIGINT NOT NULL,
					amount_cents BIGINT NOT NULL,
					PRIMARY KEY (order_id),
					CONSTRAINT fk_orders_customer
						FOREIGN KEY (customer_id) REFERENCES customer (customer_id)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
			applySQL(t, mysqlDSN, `ALTER VSCHEMA ON commerce.customer ADD VINDEX hash(customer_id) USING hash`)
			applySQL(t, mysqlDSN, `ALTER VSCHEMA ON commerce.orders ADD VINDEX hash(customer_id) USING hash`)
			time.Sleep(3 * time.Second) // schema-tracker settle

			// Seed: 24 customers across regions, 1-2 orders each. The
			// hash vindex distributes by customer_id; the test asserts
			// on full content equality, not a fixed shard assignment.
			var cb, ob []string
			oid := 9000
			for i := 1; i <= 24; i++ {
				cb = append(cb, fmt.Sprintf("(%d,'c%d@ex.com','r%d')", i, i, i%4))
				ob = append(ob, fmt.Sprintf("(%d,%d,%d)", oid, i, i*100))
				oid++
				if i%2 == 0 {
					ob = append(ob, fmt.Sprintf("(%d,%d,%d)", oid, i, i*250))
					oid++
				}
			}
			applySQL(t, mysqlDSN+"&multiStatements=true",
				"INSERT INTO customer (customer_id,email,region) VALUES "+joinVals(cb))
			applySQL(t, mysqlDSN+"&multiStatements=true",
				"INSERT INTO orders (order_id,customer_id,amount_cents) VALUES "+joinVals(ob))
			time.Sleep(2 * time.Second)

			var targetDSN string
			switch tc.targetKind {
			case "mysql":
				dsn, cl := startMySQLTarget(t)
				defer cl()
				targetDSN = dsn
			case "pg":
				dsn, cl := startPGTarget(t)
				defer cl()
				targetDSN = dsn
			}

			// A STATIC-sharded cold migrate is pure SQL over vtgate's
			// MySQL frontend: schema read + scatter bulk-copy +
			// indexes + constraints. The planetscale engine only
			// branches to VStream for OpenCDCReader/OpenSnapshotStream
			// (engine.go) — OpenRowReader/OpenSchemaReader are plain
			// SQL for both flavors. So the source DSN here is the
			// PLAIN vtgate MySQL DSN with NO vstream_* params: those
			// params are only meaningful to the CDC/snapshot path and,
			// if present, go-sql-driver would emit them as a bogus
			// `SET vstream_endpoint=host:port` that vtgate's parser
			// rejects (ground-truthed, Track-1a). grpcEndpoint is
			// unused for the static-sharded cold path; the heavy
			// reshard tag covers the VStream path end-to-end.
			//
			// Both flavors read identically here; use the vanilla
			// mysql engine (the scatter behaviour under test is
			// vtgate's, exercised the same way regardless of flavor).
			srcEng, ok := engines.Get("mysql")
			if !ok {
				t.Fatal("source engine \"mysql\" not registered")
			}
			tgtEng, ok := engines.Get(tc.targetEng)
			if !ok {
				t.Fatalf("target engine %q not registered", tc.targetEng)
			}

			mig := &Migrator{
				Source:    srcEng,
				Target:    tgtEng,
				SourceDSN: mysqlDSN,
				TargetDSN: targetDSN,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()
			if err := mig.Run(ctx); err != nil {
				t.Fatalf("Migrator.Run (sharded Vitess -> %s): %v", tc.targetKind, err)
			}

			// ---- ORACLE: src==dst by full per-PK content compare ----
			// Source of truth: read every customer/orders row straight
			// off vtgate's MySQL frontend (scatter query). Compare the
			// canonicalised row sets against the target.
			srcCust := mysqlRows(t, mysqlDSN,
				"SELECT customer_id,email,region FROM customer ORDER BY customer_id")
			srcOrd := mysqlRows(t, mysqlDSN,
				"SELECT order_id,customer_id,amount_cents FROM orders ORDER BY order_id")
			if len(srcCust) != 24 {
				t.Fatalf("source customer count = %d; want 24 (scatter read incomplete?)", len(srcCust))
			}

			var dstCust, dstOrd []string
			switch tc.targetKind {
			case "mysql":
				dstCust = mysqlRows(t, targetDSN,
					"SELECT customer_id,email,region FROM customer ORDER BY customer_id")
				dstOrd = mysqlRows(t, targetDSN,
					"SELECT order_id,customer_id,amount_cents FROM orders ORDER BY order_id")
			case "pg":
				dstCust = pgRows(t, targetDSN,
					"SELECT customer_id,email,region FROM customer ORDER BY customer_id")
				dstOrd = pgRows(t, targetDSN,
					"SELECT order_id,customer_id,amount_cents FROM orders ORDER BY order_id")
			}

			if !equalRowSets(srcCust, dstCust) {
				t.Fatalf("customer src!=dst across the scatter migrate:\n src(%d)=%v\n dst(%d)=%v",
					len(srcCust), srcCust, len(dstCust), dstCust)
			}
			if !equalRowSets(srcOrd, dstOrd) {
				t.Fatalf("orders src!=dst across the scatter migrate:\n src(%d)=%v\n dst(%d)=%v",
					len(srcOrd), srcOrd, len(dstOrd), dstOrd)
			}

			// Vitess-no-runtime-FK vs sluice-FK-DDL: the source FK was
			// declared but Vitess does not enforce it at runtime;
			// sluice's constraint phase must still have re-created it
			// on the target. Verify the constraint exists.
			assertOrdersFKExists(t, tc.targetKind, targetDSN)

			t.Logf("ORACLE PASSED (%s): %d customers + %d orders migrated from 2-shard Vitess, src==dst by content, FK re-created on target",
				tc.name, len(srcCust), len(srcOrd))
		})
	}
}

// joinVals comma-joins pre-formatted VALUES tuples.
func joinVals(vals []string) string {
	out := ""
	for i, v := range vals {
		if i > 0 {
			out += ","
		}
		out += v
	}
	return out
}

// mysqlRows runs q and returns each row as a canonical
// pipe-delimited string so two result sets can be compared by value
// regardless of driver scan typing.
func mysqlRows(t *testing.T, dsn, q string) []string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("mysql open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return scanCanonical(t, ctx, db, q)
}

// pgRows is the Postgres counterpart of mysqlRows (pgx driver).
func pgRows(t *testing.T, dsn, q string) []string {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("pg open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return scanCanonical(t, ctx, db, q)
}

// scanCanonical scans every row of q into a "v1|v2|..." string using
// the column count from the result metadata, so MySQL and PG rows
// canonicalise identically (numeric/text both rendered via %v on the
// driver's default scan type, which is consistent within a column).
func scanCanonical(t *testing.T, ctx context.Context, db *sql.DB, q string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		t.Fatalf("query %q: %v", q, err)
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns: %v", err)
	}
	var out []string
	for rows.Next() {
		cells := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		s := ""
		for i, c := range cells {
			if i > 0 {
				s += "|"
			}
			// []byte (MySQL) vs string/int64 (pg) — normalise to a
			// string rendering so the cross-engine compare is stable.
			switch v := c.(type) {
			case []byte:
				s += string(v)
			default:
				s += fmt.Sprintf("%v", v)
			}
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows iteration: %v", err)
	}
	sort.Strings(out)
	return out
}

// equalRowSets compares two canonicalised, pre-sorted row slices.
func equalRowSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// assertOrdersFKExists verifies sluice's constraint phase re-created
// the orders->customer FK on the target, even though Vitess never
// enforced it on the source at runtime.
func assertOrdersFKExists(t *testing.T, kind, dsn string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	switch kind {
	case "mysql":
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("mysql open: %v", err)
		}
		defer func() { _ = db.Close() }()
		var n int
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.referential_constraints
			WHERE constraint_schema = DATABASE()
			  AND table_name = 'orders'`).Scan(&n); err != nil {
			t.Fatalf("mysql FK introspect: %v", err)
		}
		if n < 1 {
			t.Fatalf("target MySQL orders has %d FK constraints; want >=1 (sluice FK-DDL phase did not re-create the FK Vitess never enforced)", n)
		}
	case "pg":
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("pg open: %v", err)
		}
		defer func() { _ = db.Close() }()
		var n int
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM information_schema.table_constraints
			WHERE table_name = 'orders' AND constraint_type = 'FOREIGN KEY'`).Scan(&n); err != nil {
			t.Fatalf("pg FK introspect: %v", err)
		}
		if n < 1 {
			t.Fatalf("target PG orders has %d FK constraints; want >=1 (sluice FK-DDL phase did not re-create the FK Vitess never enforced)", n)
		}
	}
}
