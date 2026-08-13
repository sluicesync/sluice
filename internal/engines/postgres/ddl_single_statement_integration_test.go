//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// The C1-1 end-to-end pin on a REAL Postgres: the audit's observed
// scenario was a restore whose manifest carried a CHECK body that
// closed the surrounding syntax and appended `; DROP TABLE canary;` —
// executed via pgx's simple protocol at exit 0, canary gone, `backup
// verify` green. The restore path reaches the target through this
// package's SchemaWriter, so the pin drives the same hostile body
// through CreateTables against a live server and requires BOTH halves:
// the coded refusal, and the canary still standing (nothing executed).
package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

func TestSchemaWriter_MultiStatementInjection_RefusedBeforeExec(t *testing.T) {
	dsn, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, `CREATE TABLE canary (id int)`); err != nil {
		t.Fatalf("create canary: %v", err)
	}

	swIface, err := (Engine{}).OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	defer func() {
		if c, ok := swIface.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}()
	sw := swIface.(*SchemaWriter)

	// The injection body, shaped as a tampered manifest would record it:
	// close the CHECK's parens and the CREATE TABLE's paren, run the
	// attack, and re-open matching syntax so the WHOLE string stays
	// superficially plausible. (The validator refuses on the top-level
	// `;` regardless of how the tail balances.)
	hostile := &ir.Table{
		Name: "t_injected",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "c", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		CheckConstraints: []*ir.CheckConstraint{{
			Name: "chk_c",
			Expr: `true)); DROP TABLE canary; CREATE TABLE t_x (y int CHECK ((true`,
		}},
	}
	// Table-level CHECKs emit INLINE in the CREATE TABLE statement, so
	// the door fires in the table phase — before the table exists at
	// all, which is the earliest it can.
	s := &ir.Schema{Tables: []*ir.Table{hostile}}
	err = sw.CreateTablesWithoutConstraints(ctx, s)
	if err == nil {
		t.Fatal("the schema writer accepted a CHECK body carrying `; DROP TABLE canary;` — " +
			"the C1-1 injection executed")
	}
	ce, ok := sluicecode.FromError(err)
	if !ok || ce.Code != sluicecode.CodeDDLEmitMultiStatement {
		t.Fatalf("want %s, got: %v", sluicecode.CodeDDLEmitMultiStatement, err)
	}
	if !strings.Contains(err.Error(), "single statement") {
		t.Errorf("refusal does not explain the single-statement invariant: %v", err)
	}

	// The other half of the claim: nothing executed. The canary stands.
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM canary`).Scan(&n); err != nil {
		t.Fatalf("the canary table is gone — the injection reached the server: %v", err)
	}
}
