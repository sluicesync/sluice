//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestChangeApplier_PrivilegeInvisibleTable_HaltsNotSkips is the audit-PG-1
// pin, and it pins the load-bearing PREMISE the fix rests on: to_regclass sees
// a table the PRIVILEGE-FILTERED information_schema hides. A CDC event for a
// target table that EXISTS but on which the apply role holds no grant must HALT
// loudly (a privilege fault) — NOT be classified as the recoverable C-11
// missing-table skip, which would advance the stream position past DML for an
// existing table (silent loss; pre-C-11 the INSERT failed loudly with 42501).
func TestChangeApplier_PrivilegeInvisibleTable_HaltsNotSkips(t *testing.T) {
	adminDSN, cleanup := startPostgresForApplier(t)
	defer cleanup()
	host, port, _, _ := ensureSharedPostgres(t)

	// Unique role (roles are CLUSTER-global on the shared test DB).
	role := fmt.Sprintf("pg1role_%d", time.Now().UnixNano())
	table := fmt.Sprintf("pg1_orders_%d", time.Now().UnixNano())

	// As superuser: create the target table, and a login role with schema
	// USAGE + CREATE (so its own control tables can be created) but NO grant
	// on the target table — the exact "privilege-invisible" shape.
	applyPGApplier(t, adminDSN, fmt.Sprintf(`
		CREATE TABLE %s (id BIGINT PRIMARY KEY, v TEXT);
		CREATE ROLE %s LOGIN PASSWORD 'pw';
		GRANT ALL ON SCHEMA public TO %s;
	`, table, role, role))
	defer applyPGApplier(t, adminDSN, fmt.Sprintf(
		`DROP OWNED BY %s; DROP ROLE IF EXISTS %s; DROP TABLE IF EXISTS %s;`, role, role, table,
	))

	// Sanity-check the PREMISE against the live server before trusting it:
	// as the restricted role, information_schema hides the table while
	// to_regclass sees it. If PG ever changed either, the fix's disambiguation
	// would be built on sand — so assert it here (the premise-naming rule).
	roleDSN := sharedPGDSN(host, port, role, "pw", "target_db")
	rdb, err := sql.Open("pgx", roleDSN)
	if err != nil {
		t.Fatalf("open role dsn: %v", err)
	}
	defer func() { _ = rdb.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var isCols, inCatalog bool
	if err := rdb.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name=$1)`,
		table).Scan(&isCols); err != nil {
		t.Fatalf("information_schema probe: %v", err)
	}
	if err := rdb.QueryRowContext(ctx, `SELECT to_regclass('public.'||quote_ident($1)) IS NOT NULL`, table).Scan(&inCatalog); err != nil {
		t.Fatalf("to_regclass probe: %v", err)
	}
	if isCols {
		t.Fatalf("premise broken: information_schema.columns SHOWS %q to the restricted role — the privilege-invisible shape did not materialize", table)
	}
	if !inCatalog {
		t.Fatalf("premise broken: to_regclass did NOT see %q for the restricted role — the fix's disambiguation cannot work (PG-1)", table)
	}

	// Now drive a CDC event for the invisible table through the applier
	// connected AS the restricted role. It must HALT, not skip.
	eng := Engine{}
	applier, err := eng.OpenChangeApplier(ctx, roleDSN)
	if err != nil {
		t.Fatalf("OpenChangeApplier: %v", err)
	}
	defer func() {
		if c, ok := applier.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	if err := applier.EnsureControlTable(ctx); err != nil {
		t.Fatalf("EnsureControlTable (role has CREATE): %v", err)
	}

	ch := make(chan ir.Change, 1)
	ch <- ir.Insert{
		Schema: "public", Table: table,
		Row:      ir.Row{"id": int64(1), "v": "x"},
		Position: ir.Position{Engine: "postgres", Token: `{"lsn":"0/16B2C10"}`},
	}
	close(ch)
	applyErr := applier.Apply(ctx, testStreamID, ch)
	if applyErr == nil {
		t.Fatal("PG-1: Apply into a privilege-invisible existing table returned nil — it SKIPPED (silent loss), must halt")
	}
	if !strings.Contains(applyErr.Error(), "privilege") && !strings.Contains(applyErr.Error(), "PRIVILEGE") {
		t.Fatalf("PG-1: Apply halted but not with the privilege diagnosis: %v", applyErr)
	}

	// And the skip ledger must carry NO row — the table was never skipped.
	for _, rec := range listSkips(t, ctx, applier) {
		if strings.Contains(rec.Table, table) {
			t.Fatalf("PG-1: the privilege-invisible table was recorded in the skip ledger (%+v) — it must halt, not skip", rec)
		}
	}
}
