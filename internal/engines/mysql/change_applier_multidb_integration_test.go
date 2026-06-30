//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration test for the ADR-0074 Phase 1b multi-database apply
// routing on the MySQL ChangeApplier (part B). One applier instance,
// one position write per batch, per-change namespace routing keyed on
// change.Schema:
//
//   - With routing ENABLED, a change whose Schema differs from the
//     applier's bound database lands in `Schema`.`table` (the
//     cross-database case); a change whose Schema is empty or equals the
//     bound database lands unqualified in the bound database
//     (byte-identical single-database behaviour).
//
// The target databases are pre-created here (the cold-start owns
// namespace creation in Phase 1b.2; the applier assumes they exist).

package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// countRowsInDB counts rows of `schema`.`table` using a server-level
// (database-unqualified DSN) connection so the test can verify rows
// landed in the CORRECT target database, not just the bound one.
func countRowsInDB(t *testing.T, dsn, schema, table string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var n int
	q := "SELECT COUNT(*) FROM `" + schema + "`.`" + table + "`"
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		t.Fatalf("count %s.%s: %v", schema, table, err)
	}
	return n
}

// TestChangeApplier_MultiDatabaseRouting pins the part-B routing class on
// MySQL: with routing enabled, changes fan out to same-named target
// databases by change.Schema; with the bound-database schema (the
// single-database shape) they land unqualified in the bound database.
func TestChangeApplier_MultiDatabaseRouting(t *testing.T) {
	const (
		boundDB = "md_target_bound"
		dbA     = "md_target_a"
		dbB     = "md_target_b"
	)
	// Bound DSN + two additional target databases on the same server.
	boundDSN, _ := newSharedDB(t, boundDB)
	newSharedDB(t, dbA)
	newSharedDB(t, dbB)

	const seed = `
		CREATE TABLE widgets (
			id   BIGINT       NOT NULL AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`
	// Pre-create the table in all three databases (the applier does not
	// create the namespace or table — cold-start owns that).
	host, port, user, password := sharedPrimitives()
	applyMySQLApplier(t, sharedDSN(host, port, user, password, boundDB), seed)
	applyMySQLApplier(t, sharedDSN(host, port, user, password, dbA), seed)
	applyMySQLApplier(t, sharedDSN(host, port, user, password, dbB), seed)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, boundDSN)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	router, ok := applier.(ir.MultiDatabaseRouter)
	if !ok {
		t.Fatalf("applier %T does not implement ir.MultiDatabaseRouter", applier)
	}
	router.SetMultiDatabaseRouting(true, nil)

	// Mixed stream:
	//   - two cross-database changes (dbA, dbB) — must route to those DBs;
	//   - one change bound to boundDB explicitly — stays unqualified/bound;
	//   - one change with an EMPTY Schema — the single-database shape,
	//     also lands unqualified in the bound database.
	events := []ir.Change{
		ir.Insert{Schema: dbA, Table: "widgets", Row: ir.Row{"id": int64(1), "name": "a1"}},
		ir.Insert{Schema: dbB, Table: "widgets", Row: ir.Row{"id": int64(1), "name": "b1"}},
		ir.Insert{Schema: dbA, Table: "widgets", Row: ir.Row{"id": int64(2), "name": "a2"}},
		ir.Insert{Schema: boundDB, Table: "widgets", Row: ir.Row{"id": int64(1), "name": "bound-explicit"}},
		ir.Insert{Schema: "", Table: "widgets", Row: ir.Row{"id": int64(2), "name": "bound-empty"}},
	}
	pumpChanges(t, ctx, applier, events)

	// dbA got 2 rows, dbB got 1, boundDB got the two bound writes.
	if got := countRowsInDB(t, boundDSN, dbA, "widgets"); got != 2 {
		t.Errorf("dbA (%q) row count = %d; want 2 (cross-database routing missed)", dbA, got)
	}
	if got := countRowsInDB(t, boundDSN, dbB, "widgets"); got != 1 {
		t.Errorf("dbB (%q) row count = %d; want 1 (cross-database routing missed)", dbB, got)
	}
	if got := countRowsInDB(t, boundDSN, boundDB, "widgets"); got != 2 {
		t.Errorf("boundDB (%q) row count = %d; want 2 (bound + empty-Schema must stay bound)", boundDB, got)
	}

	// Cross-namespace UPDATE + DELETE also route: update dbA id=1, delete
	// dbB id=1. If routing failed these would miss (zero rows) and the
	// counts below would be wrong.
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Update{
			Schema: dbA, Table: "widgets",
			Before: ir.Row{"id": int64(1), "name": "a1"},
			After:  ir.Row{"id": int64(1), "name": "a1-updated"},
		},
		ir.Delete{
			Schema: dbB, Table: "widgets",
			Before: ir.Row{"id": int64(1), "name": "b1"},
		},
	})

	if got := scalarStringDB(t, boundDSN, "SELECT name FROM `"+dbA+"`.`widgets` WHERE id = 1"); got != "a1-updated" {
		t.Errorf("dbA id=1 name = %q; want a1-updated (cross-database UPDATE missed)", got)
	}
	if got := countRowsInDB(t, boundDSN, dbB, "widgets"); got != 0 {
		t.Errorf("dbB (%q) row count after delete = %d; want 0 (cross-database DELETE missed)", dbB, got)
	}
}

// TestChangeApplier_RoutingDisabled_IsByteIdentical is the back-compat
// pin: with routing DISABLED (the default), a change whose Schema
// differs from the bound database MUST still land in the bound database
// — the cross-engine single-database case where a namespaced source
// already populates change.Schema. This guards the Phase-1a
// over-qualification regression on the apply path.
func TestChangeApplier_RoutingDisabled_IsByteIdentical(t *testing.T) {
	const boundDB = "md_nroute_bound"
	boundDSN, _ := newSharedDB(t, boundDB)

	applyMySQLApplier(t, boundDSN, `
		CREATE TABLE widgets (
			id   BIGINT       NOT NULL AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
	`)

	eng := Engine{Flavor: FlavorVanilla}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	applier, err := eng.OpenChangeApplier(ctx, boundDSN)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()

	// Routing left at its default (disabled). The change carries a
	// DIFFERING source schema ("public", as a PG source would) — with
	// routing off the applier ignores it and writes into the bound
	// database. (No `public` database exists; if the guard regressed and
	// it qualified, this Apply would error with Unknown database.)
	pumpChanges(t, ctx, applier, []ir.Change{
		ir.Insert{Schema: "public", Table: "widgets", Row: ir.Row{"id": int64(1), "name": "x"}},
	})

	if got := countRowsInDB(t, boundDSN, boundDB, "widgets"); got != 1 {
		t.Errorf("boundDB row count = %d; want 1 (routing-off differing schema must stay bound)", got)
	}
}

// scalarStringDB runs a single-string-column query on a server-level
// connection (the DSN's bound database is irrelevant — the query
// fully-qualifies its table).
func scalarStringDB(t *testing.T, dsn, query string) string {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var s string
	if err := db.QueryRowContext(ctx, query).Scan(&s); err != nil {
		t.Fatalf("scalarStringDB %q: %v", query, err)
	}
	return s
}
