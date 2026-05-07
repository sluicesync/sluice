//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// Integration tests for the verify orchestrator. Boots real Postgres
// + MySQL containers, exercises sluice verify against same-engine and
// cross-engine pairs with both clean and intentionally-drifted state,
// and asserts the orchestrator produces the right exit-code-shape
// signal + structured output.
//
// Same harness shape as diff_integration_test.go and
// migrate_integration_test.go (testcontainers-go via the shared
// startPostgres / startMySQL helpers).

package pipeline

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orware/sluice/internal/engines"

	_ "github.com/orware/sluice/internal/engines/mysql"
	_ "github.com/orware/sluice/internal/engines/postgres"
)

// TestVerify_PostgresToPostgres_Clean — happy path. Same DDL +
// matching row counts on both sides; verify exits cleanly with
// every table reporting OK.
func TestVerify_PostgresToPostgres_Clean(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgres(t)
	defer cleanup()

	const ddl = `
		CREATE TABLE customers (id BIGINT PRIMARY KEY, name TEXT NOT NULL);
		CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL, total NUMERIC(10,2) NOT NULL);
	`
	const seed = `
		INSERT INTO customers (id, name) VALUES (1,'a'),(2,'b'),(3,'c');
		INSERT INTO orders (id, customer_id, total) VALUES (10,1,10.5),(11,2,20.0),(12,3,30.25),(13,1,5.50);
	`
	applyPGDDL(t, srcDSN, ddl+seed)
	applyPGDDL(t, tgtDSN, ddl+seed)

	pg, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var buf bytes.Buffer
	v := &Verifier{Source: pg, Target: pg, SourceDSN: srcDSN, TargetDSN: tgtDSN, Out: &buf}
	r, err := v.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.HasMismatch() {
		t.Errorf("expected no mismatch on identical state; got %+v", r.Summary)
	}
	if r.Summary.TablesChecked != 2 || r.Summary.TablesClean != 2 {
		t.Errorf("expected 2 checked / 2 clean; got %+v", r.Summary)
	}
	out := buf.String()
	for _, want := range []string{"customers", "orders", "OK rows=3", "OK rows=4"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected %q in text output; got:\n%s", want, out)
		}
	}
}

// TestVerify_PostgresToPostgres_Mismatch — verify catches a manual
// row deletion on the target. Pins the structured exit-code signal
// + the operator-facing delta line.
func TestVerify_PostgresToPostgres_Mismatch(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgres(t)
	defer cleanup()

	const ddl = `CREATE TABLE products (id BIGINT PRIMARY KEY, name TEXT NOT NULL);`
	const seed = `INSERT INTO products (id, name) VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');`
	applyPGDDL(t, srcDSN, ddl+seed)
	applyPGDDL(t, tgtDSN, ddl+seed)
	// Drift the target: delete two rows.
	applyPGDDL(t, tgtDSN, `DELETE FROM products WHERE id IN (4, 5);`)

	pg, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var buf bytes.Buffer
	v := &Verifier{Source: pg, Target: pg, SourceDSN: srcDSN, TargetDSN: tgtDSN, Out: &buf}
	r, err := v.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !r.HasMismatch() {
		t.Errorf("expected mismatch on drifted state; got %+v", r.Summary)
	}
	if r.Summary.TablesMismatch != 1 {
		t.Errorf("expected 1 mismatched table; got %d", r.Summary.TablesMismatch)
	}
	if r.Tables[0].SourceRowCount != 5 || r.Tables[0].TargetRowCount != 3 {
		t.Errorf("expected source=5 target=3; got %+v", r.Tables[0])
	}
	out := buf.String()
	if !strings.Contains(out, "MISMATCH source=5 target=3 (delta=-2)") {
		t.Errorf("expected MISMATCH line with delta; got:\n%s", out)
	}
}

// TestVerify_PostgresToPostgres_ExtraOnTarget pins the v0.13.x
// enhancement: extra tables on target are reported informationally,
// NOT as mismatches.
func TestVerify_PostgresToPostgres_ExtraOnTarget(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startPostgres(t)
	defer cleanup()

	applyPGDDL(t, srcDSN, `CREATE TABLE shared (id BIGINT PRIMARY KEY); INSERT INTO shared VALUES (1);`)
	applyPGDDL(t, tgtDSN, `
		CREATE TABLE shared (id BIGINT PRIMARY KEY); INSERT INTO shared VALUES (1);
		CREATE TABLE other_app_table (id BIGINT PRIMARY KEY);
	`)

	pg, _ := engines.Get("postgres")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var buf bytes.Buffer
	v := &Verifier{Source: pg, Target: pg, SourceDSN: srcDSN, TargetDSN: tgtDSN, Out: &buf}
	r, err := v.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.HasMismatch() {
		t.Errorf("extra-on-target should NOT cause mismatch; got %+v", r.Summary)
	}
	if r.Summary.TablesExtraOnTarget != 1 {
		t.Errorf("expected 1 extra-on-target; got %d", r.Summary.TablesExtraOnTarget)
	}
	if !strings.Contains(buf.String(), "other_app_table") {
		t.Errorf("expected other_app_table in extra-on-target list; got:\n%s", buf.String())
	}
}

// TestVerify_MySQLToMySQL_Clean — same shape as the PG sibling but
// against MySQL, confirming the MySQL ExactRowCount implementation
// works end-to-end.
func TestVerify_MySQLToMySQL_Clean(t *testing.T) {
	srcDSN, tgtDSN, cleanup := startMySQL(t)
	defer cleanup()

	const ddl = `CREATE TABLE widgets (id BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL) ENGINE=InnoDB;`
	const seed = `INSERT INTO widgets (id, name) VALUES (1,'a'),(2,'b'),(3,'c');`
	applyMySQLDDL(t, srcDSN, ddl)
	applyMySQLDDL(t, srcDSN, seed)
	applyMySQLDDL(t, tgtDSN, ddl)
	applyMySQLDDL(t, tgtDSN, seed)

	my, _ := engines.Get("mysql")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var buf bytes.Buffer
	v := &Verifier{Source: my, Target: my, SourceDSN: srcDSN, TargetDSN: tgtDSN, Out: &buf}
	r, err := v.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.HasMismatch() {
		t.Errorf("expected no mismatch on identical MySQL state; got %+v", r.Summary)
	}
	if r.Tables[0].SourceRowCount != 3 || r.Tables[0].TargetRowCount != 3 {
		t.Errorf("expected source=target=3; got %+v", r.Tables[0])
	}
}
