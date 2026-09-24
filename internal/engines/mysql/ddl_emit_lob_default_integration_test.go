//go:build integration

// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"sluicesync.dev/sluice/internal/ir"
)

// TestAlterAddColumn_LOBDefault_FillsPreexistingRows is GC-36 item 4 on
// a real server, through the writer path a forwarded ADD COLUMN takes
// (SchemaWriter.AlterAddColumn). Every large-object family's DEFAULT must
// fill the rows the table ALREADY holds with the declared value; the
// expected values are the literals fed in, never the target catalog.
//
// It also binds the two server facts the emitter's argument rests on:
// that this server's VERSION() classifies as one that takes the
// parenthesised form (the probe at OpenSchemaWriter), and the
// MYSQL-EXPRESSION-DEFAULT-BACKSLASH wart — a default holding a quote,
// backslash or control character that is first
// evaluated by a NO_BACKSLASH_ESCAPES session after the table definition
// is reloaded must still read back the declared value.
//
// SLUICE_TEST_MYSQL_IMAGE runs it on any MySQL version (8.0 by default;
// measured on 8.4 too).
func TestAlterAddColumn_LOBDefault_FillsPreexistingRows(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "lob_default_fill")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	exec := func(q string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	exec("CREATE TABLE w (id INT PRIMARY KEY)")
	exec("INSERT INTO w (id) VALUES (1), (2)")

	sw, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	w := sw.(*SchemaWriter)
	defer func() { _ = w.Close() }()
	if w.emitter.lobDefaults != lobDefaultMySQL {
		t.Fatalf("the writer classified this server as lobDefaultForm %d; want lobDefaultMySQL — the probe no longer "+
			"recognises a server that takes the parenthesised form", w.emitter.lobDefaults)
	}

	lit := func(s string) ir.DefaultValue { return ir.DefaultLiteral{Value: s} }
	cells := []struct {
		col  *ir.Column
		read string // SELECT expression
		want string
	}{
		{&ir.Column{Name: "t_text", Type: ir.Text{Size: ir.TextLong}, Default: lit("it's")}, "CAST(t_text AS CHAR)", "it's"},
		{&ir.Column{Name: "t_bslash", Type: ir.Text{Size: ir.TextLong}, Default: lit(`a\b`)}, "HEX(t_bslash)", "615C62"},
		{&ir.Column{Name: "t_ctrl", Type: ir.Text{Size: ir.TextLong}, Default: lit("a\nb\rc\x1ad\x00e\"f\tg")}, "HEX(t_ctrl)", "610A620D631A64006522660967"},
		{&ir.Column{Name: "t_unicode", Type: ir.Text{Size: ir.TextRegular}, Default: lit("héllo ✓ 日本")}, "HEX(t_unicode)", strings.ToUpper("68c3a96c6c6f20e29c9320e697a5e69cac")},
		{&ir.Column{Name: "t_wide", Type: ir.Varchar{Length: 20000}, Default: lit("wide")}, "t_wide", "wide"},
		{&ir.Column{Name: "b_hex", Type: ir.Blob{Size: ir.BlobLong}, Default: ir.DefaultExpression{Expr: "0x00AB00", Dialect: hexLiteralDialect}}, "HEX(b_hex)", "00AB00"},
		{&ir.Column{Name: "b_raw", Type: ir.Blob{Size: ir.BlobRegular}, Default: lit("abc")}, "HEX(b_raw)", "616263"},
		{&ir.Column{Name: "j_doc", Type: ir.JSON{Binary: true}, Default: lit(`{"k": "x\\y"}`)}, "CAST(j_doc AS CHAR)", `{"k": "x\\y"}`},
		{&ir.Column{Name: "a_int", Type: ir.JSON{Binary: true}, SourceColumnType: ir.Array{Element: ir.Integer{Width: 32}}, Default: lit("{1,-2}")}, "CAST(a_int AS CHAR)", "[1, -2]"},
		{&ir.Column{Name: "g_pt", Type: ir.Geometry{Subtype: ir.GeometryUnspecified}, Default: ir.DefaultExpression{Expr: "st_geomfromtext('POINT(1 2)')", Dialect: "mysql"}}, "ST_AsText(g_pt)", "POINT(1 2)"},
	}
	table := &ir.Table{Name: "w", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}}}
	for _, c := range cells {
		if err := w.AlterAddColumn(ctx, table, []*ir.Column{c.col}); err != nil {
			t.Fatalf("AlterAddColumn %s: %v", c.col.Name, err)
		}
	}

	// Reload the table definition, then let a NO_BACKSLASH_ESCAPES session
	// be the first to evaluate the defaults (row 3).
	exec("FLUSH TABLES")
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION sql_mode = CONCAT(@@sql_mode, ',NO_BACKSLASH_ESCAPES')"); err != nil {
		t.Fatalf("set NBE: %v", err)
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO w (id) VALUES (3)"); err != nil {
		t.Fatalf("insert under NBE: %v", err)
	}
	_ = conn.Close()

	for _, c := range cells {
		for id := 1; id <= 3; id++ {
			var got sql.NullString
			if err := db.QueryRowContext(ctx, "SELECT "+c.read+" FROM w WHERE id = ?", id).Scan(&got); err != nil {
				t.Fatalf("read %s id=%d: %v", c.col.Name, id, err)
			}
			if !got.Valid || got.String != c.want {
				t.Errorf("%s row id=%d holds [%s] (valid=%v); want [%s]", c.col.Name, id, got.String, got.Valid, c.want)
			}
		}
	}
}

// TestAlterAddColumn_LOBDefault_RefusedWhenTheServerCannotHoldIt is the
// loud half: on a server that takes no DEFAULT on a large-object column
// (MySQL before 8.0.13 — simulated by the emitter's own classification,
// since no such server runs here), the forwarded ADD COLUMN is refused
// BEFORE the ALTER, so no pre-existing row is filled with NULL.
func TestAlterAddColumn_LOBDefault_RefusedWhenTheServerCannotHoldIt(t *testing.T) {
	dsn, cleanup := newSharedDB(t, "lob_default_refuse")
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "CREATE TABLE w (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO w (id) VALUES (1)"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	sw, err := Engine{}.OpenSchemaWriter(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenSchemaWriter: %v", err)
	}
	w := sw.(*SchemaWriter)
	defer func() { _ = w.Close() }()
	w.emitter.lobDefaults = lobDefaultNone

	table := &ir.Table{Name: "w", Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 32}}}}
	col := &ir.Column{Name: "t_text", Type: ir.Text{Size: ir.TextLong}, Default: ir.DefaultLiteral{Value: "abc"}}
	err = w.AlterAddColumn(ctx, table, []*ir.Column{col})
	if err == nil || !strings.Contains(err.Error(), "LOB-DEFAULT-NOT-CARRIED") {
		t.Fatalf("AlterAddColumn = %v; want the LOB-DEFAULT-NOT-CARRIED refusal", err)
	}
	exists, err := columnExists(ctx, db, "lob_default_refuse", "w", "t_text")
	if err != nil {
		t.Fatalf("columnExists: %v", err)
	}
	if exists {
		t.Error("the column was added although the DEFAULT was refused — the pre-existing row now holds NULL")
	}

	// A column with no DEFAULT is not the class: it still adds.
	plain := &ir.Column{Name: "t_plain", Type: ir.Text{Size: ir.TextLong}}
	if err := w.AlterAddColumn(ctx, table, []*ir.Column{plain}); err != nil {
		t.Fatalf("AlterAddColumn without a default: %v", err)
	}
}
