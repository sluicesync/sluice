//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

// GC-6 follow-up: the expression-level VIRTUAL refusals PostgreSQL 18
// raises at CREATE TABLE — a user-defined function or a user-defined /
// domain type in the generation expression or as the column type — are
// caught by [SchemaWriter.createTableRetryingVirtualPromotion], which
// retries the table once with its VIRTUAL columns promoted to STORED.
// Every entry of [pgVirtualGeneratedRefusals] is a quoted server string,
// i.e. an environmental fact, so each is bound here to a measurement on
// a pinned postgres:18: the shape is written VIRTUAL through the real
// writer, the server's refusal is observed by the retry, and the table
// lands with attgenerated 's' and the promotion WARN naming the reason.

package postgres

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

func TestGeneratedColumns_VirtualPromotionRetry_PG18(t *testing.T) {
	dsn, cleanup := startPostgresForCDCImage(t, "postgres:18")
	defer cleanup()
	if v := pgServerVersionNum(t, dsn); v < pgVersionVirtualGeneratedColumns {
		t.Fatalf("postgres:18 booted with server_version_num=%d", v)
	}
	applyPGSQL(t, dsn, `
		CREATE FUNCTION udf_double(int) RETURNS int IMMUTABLE LANGUAGE sql AS 'SELECT $1 * 2';
		CREATE DOMAIN posint AS int CHECK (VALUE > 0);`)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	virt := func(name, expr string) *ir.Column {
		return &ir.Column{Name: name, Type: ir.Integer{Width: 32}, Nullable: true, GeneratedExpr: expr, GeneratedExprDialect: dialectName}
	}
	cases := []struct {
		name       string
		table      *ir.Table
		wantReason string
	}{
		{
			// The class the static predicate cannot see: a UDF in the body.
			name: "user-defined function in the expression",
			table: &ir.Table{Name: "t_udf", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "c", Type: ir.Integer{Width: 32}},
				virt("v", "udf_double(c)"),
			}, PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}},
			wantReason: "generation expression uses user-defined function",
		},
		{
			name: "user-defined (domain) type in the expression",
			table: &ir.Table{Name: "t_udt_expr", Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 32}},
				{Name: "c", Type: ir.Integer{Width: 32}},
				virt("v", "((c::posint)::int)"),
			}, PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}}},
			wantReason: "generation expression uses user-defined type",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureGeneratedWarns(t)
			writeSchemaTo(ctx, t, dsn, &ir.Schema{Tables: []*ir.Table{tc.table}})
			if got := pgAttGeneratedByColumn(t, dsn, "public", tc.table.Name)["v"]; got != "s" {
				t.Errorf("%s.v attgenerated = %q; want s (promoted by the CREATE TABLE retry)", tc.table.Name, got)
			}
			out := buf.String()
			if !strings.Contains(out, generatedVirtualPromotedMarker) || !strings.Contains(out, tc.wantReason) {
				t.Errorf("retry WARN missing the marker or the server reason %q:\n%s", tc.wantReason, out)
			}
			// The value computes on the promoted column.
			applyPGSQL(t, dsn, `INSERT INTO `+tc.table.Name+` (id, c) VALUES (1, 21)`)
			if got := pgQueryString(t, dsn, `SELECT v::text FROM `+tc.table.Name+` WHERE id = 1`); got != "42" && got != "21" {
				t.Errorf("%s.v = %q; want the computed value", tc.table.Name, got)
			}
		})
	}

	// The column-TYPE half is caught statically ([virtualBlockedBy]), so
	// no retry is needed — but the server strings for it are in the
	// retry list too; bind them by issuing the raw DDL and reading the
	// refusal, so a reworded server fails this test rather than the
	// matcher silently going blind.
	t.Run("server strings for the type refusals are still what the matcher expects", func(t *testing.T) {
		for _, stmt := range []string{
			`CREATE TABLE t_dom (id int PRIMARY KEY, c int, v posint GENERATED ALWAYS AS (c) VIRTUAL)`,
			`CREATE TYPE mood AS ENUM ('a'); CREATE TABLE t_enum (id int PRIMARY KEY, c mood, v mood GENERATED ALWAYS AS (c) VIRTUAL)`,
		} {
			err := execPGSQLErr(t, dsn, stmt)
			if _, ok := isVirtualGeneratedRefusal(err); !ok {
				t.Errorf("%s: err = %v; want one of pgVirtualGeneratedRefusals", stmt, err)
			}
		}
	})
}

// execPGSQLErr runs stmt and returns its error (nil on success).
func execPGSQLErr(t *testing.T, dsn, stmt string) error {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = db.ExecContext(ctx, stmt)
	return err
}
