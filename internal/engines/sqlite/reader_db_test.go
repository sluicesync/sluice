// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"sluicesync.dev/sluice/internal/ir"
)

// bgCtx is a tiny helper so seed Exec calls satisfy the noctx linter.
func bgCtx() context.Context { return context.Background() }

// seedDB creates a fresh SQLite file at a temp path, applies the DDL/DML
// statements against a read-write connection, and returns the path. The
// engine then reopens it read-only.
func seedDB(t *testing.T, stmts ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "app.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open seed db: %v", err)
	}
	defer func() { _ = db.Close() }()
	for _, s := range stmts {
		if _, err := db.ExecContext(bgCtx(), s); err != nil {
			t.Fatalf("seed exec %q: %v", s, err)
		}
	}
	return path
}

// TestSchemaReader pins schema extraction over a real temp file: tables,
// columns (with affinity-resolved IR types), the INTEGER-PK rowid-alias
// auto-increment flag, a composite PK, a unique index, and a foreign key.
func TestSchemaReader(t *testing.T) {
	path := seedDB(
		t,
		`CREATE TABLE users (
			id    INTEGER PRIMARY KEY,
			email TEXT NOT NULL,
			score NUMERIC,
			rate  REAL,
			photo BLOB
		)`,
		`CREATE UNIQUE INDEX users_email_uq ON users(email)`,
		`CREATE TABLE posts (
			id      INTEGER PRIMARY KEY,
			user_id INTEGER NOT NULL,
			body    TEXT,
			FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE post_tags (
			post_id INTEGER NOT NULL,
			tag     TEXT NOT NULL,
			PRIMARY KEY (post_id, tag)
		)`,
	)

	eng := Engine{}
	ctx := context.Background()
	sr, err := eng.OpenSchemaReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(*SchemaReader).Close() }()

	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	if len(schema.Tables) != 3 {
		t.Fatalf("tables = %d; want 3", len(schema.Tables))
	}

	users := tableByName(schema, "users")
	if users == nil {
		t.Fatal("users table missing")
	}
	// Column IR types from affinity.
	wantTypes := map[string]ir.Type{
		"id":    ir.Integer{Width: 64, AutoIncrement: true}, // INTEGER PRIMARY KEY = rowid alias
		"email": ir.Text{Size: ir.TextLong},
		"score": ir.Decimal{Unconstrained: true},
		"rate":  ir.Float{Precision: ir.FloatDouble},
		"photo": ir.Blob{Size: ir.BlobLong},
	}
	for name, want := range wantTypes {
		c := columnByName(users, name)
		if c == nil {
			t.Errorf("users.%s missing", name)
			continue
		}
		if c.Type != want {
			t.Errorf("users.%s type = %#v; want %#v", name, c.Type, want)
		}
	}
	// email NOT NULL, score nullable.
	if c := columnByName(users, "email"); c != nil && c.Nullable {
		t.Error("users.email should be NOT NULL")
	}
	if c := columnByName(users, "score"); c != nil && !c.Nullable {
		t.Error("users.score should be nullable")
	}
	// PK on id.
	if users.PrimaryKey == nil || len(users.PrimaryKey.Columns) != 1 || users.PrimaryKey.Columns[0].Column != "id" {
		t.Errorf("users PK = %#v; want PK on id", users.PrimaryKey)
	}
	// Unique index on email.
	hasUq := false
	for _, ix := range users.Indexes {
		if ix.Unique && len(ix.Columns) == 1 && ix.Columns[0].Column == "email" {
			hasUq = true
		}
	}
	if !hasUq {
		t.Errorf("users indexes = %#v; want a unique index on email", users.Indexes)
	}

	// posts FK → users(id) ON DELETE CASCADE.
	posts := tableByName(schema, "posts")
	if posts == nil || len(posts.ForeignKeys) != 1 {
		t.Fatalf("posts FKs = %v; want 1", posts)
	}
	fk := posts.ForeignKeys[0]
	if fk.ReferencedTable != "users" || len(fk.Columns) != 1 || fk.Columns[0] != "user_id" ||
		len(fk.ReferencedColumns) != 1 || fk.ReferencedColumns[0] != "id" || fk.OnDelete != ir.FKActionCascade {
		t.Errorf("posts FK = %+v; want user_id → users(id) ON DELETE CASCADE", fk)
	}

	// post_tags composite PK.
	pt := tableByName(schema, "post_tags")
	if pt == nil || pt.PrimaryKey == nil || len(pt.PrimaryKey.Columns) != 2 {
		t.Fatalf("post_tags PK = %v; want composite (post_id, tag)", pt)
	}
	if pt.PrimaryKey.Columns[0].Column != "post_id" || pt.PrimaryKey.Columns[1].Column != "tag" {
		t.Errorf("post_tags PK cols = %v; want [post_id tag]", pt.PrimaryKey.Columns)
	}
}

// TestRowReader_FaithfulStorageClasses validates, over a real file, that
// modernc returns each storage class as the Go type the decoder expects and
// that faithful values round-trip — including affinity coercion on INSERT
// (an integer into a REAL column comes back float64; into a TEXT column
// comes back the string "42").
func TestRowReader_FaithfulStorageClasses(t *testing.T) {
	path := seedDB(
		t,
		`CREATE TABLE t_int  (v INTEGER)`,
		`INSERT INTO t_int  (v) VALUES (42), (NULL)`,
		`CREATE TABLE t_real (v REAL)`,
		`INSERT INTO t_real (v) VALUES (3.5), (42), (NULL)`,
		`CREATE TABLE t_text (v TEXT)`,
		`INSERT INTO t_text (v) VALUES ('hi'), (42), (NULL)`,
		`CREATE TABLE t_blob (v BLOB)`,
		`INSERT INTO t_blob (v) VALUES (x'00ff'), (NULL)`,
		`CREATE TABLE t_num  (v NUMERIC)`,
		`INSERT INTO t_num  (v) VALUES (7), (3.5), (NULL)`,
	)
	cases := []struct {
		table string
		want  []any
	}{
		{"t_int", []any{int64(42), nil}},
		{"t_real", []any{float64(3.5), float64(42), nil}},
		{"t_text", []any{"hi", "42", nil}},
		{"t_blob", []any{[]byte{0x00, 0xff}, nil}},
		{"t_num", []any{"7", "3.5", nil}},
	}
	for _, c := range cases {
		t.Run(c.table, func(t *testing.T) {
			rows := readTable(t, path, c.table)
			if len(rows) != len(c.want) {
				t.Fatalf("%s rows = %d; want %d", c.table, len(rows), len(c.want))
			}
			for i, w := range c.want {
				if !valuesEqual(rows[i]["v"], w) {
					t.Errorf("%s row %d v = %#v; want %#v", c.table, i, rows[i]["v"], w)
				}
			}
		})
	}
}

// TestRowReader_RefusesStorageClassMismatch pins the loud-failure half over
// a real file: a value whose stored class can't be faithfully held in its
// column's affinity type aborts the read via Err(), and the error names the
// table, column, and the offending storage class. Every refusal cell from
// the decode matrix that CAN occur given SQLite's insert-time affinity
// coercion is exercised here.
func TestRowReader_RefusesStorageClassMismatch(t *testing.T) {
	cases := []struct {
		name      string
		ddl       string
		insert    string
		column    string
		wantClass string
	}{
		{"int_holds_real", `CREATE TABLE m (v INTEGER)`, `INSERT INTO m (v) VALUES (1.5)`, "v", "REAL"},
		{"int_holds_text", `CREATE TABLE m (v INTEGER)`, `INSERT INTO m (v) VALUES ('abc')`, "v", "TEXT"},
		{"int_holds_blob", `CREATE TABLE m (v INTEGER)`, `INSERT INTO m (v) VALUES (x'00')`, "v", "BLOB"},
		{"real_holds_text", `CREATE TABLE m (v REAL)`, `INSERT INTO m (v) VALUES ('abc')`, "v", "TEXT"},
		{"real_holds_blob", `CREATE TABLE m (v REAL)`, `INSERT INTO m (v) VALUES (x'00')`, "v", "BLOB"},
		{"text_holds_blob", `CREATE TABLE m (v TEXT)`, `INSERT INTO m (v) VALUES (x'00ff')`, "v", "BLOB"},
		{"blob_holds_int", `CREATE TABLE m (v BLOB)`, `INSERT INTO m (v) VALUES (5)`, "v", "INTEGER"},
		{"blob_holds_real", `CREATE TABLE m (v BLOB)`, `INSERT INTO m (v) VALUES (2.5)`, "v", "REAL"},
		{"blob_holds_text", `CREATE TABLE m (v BLOB)`, `INSERT INTO m (v) VALUES ('hi')`, "v", "TEXT"},
		{"num_holds_text", `CREATE TABLE m (v NUMERIC)`, `INSERT INTO m (v) VALUES ('notnum')`, "v", "TEXT"},
		{"num_holds_blob", `CREATE TABLE m (v NUMERIC)`, `INSERT INTO m (v) VALUES (x'00')`, "v", "BLOB"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			path := seedDB(t, c.ddl, c.insert)
			eng := Engine{}
			ctx := context.Background()
			sr, err := eng.OpenSchemaReader(ctx, path)
			if err != nil {
				t.Fatalf("OpenSchemaReader: %v", err)
			}
			defer func() { _ = sr.(*SchemaReader).Close() }()
			schema, err := sr.ReadSchema(ctx)
			if err != nil {
				t.Fatalf("ReadSchema: %v", err)
			}
			tbl := tableByName(schema, "m")

			rr, err := eng.OpenRowReader(ctx, path)
			if err != nil {
				t.Fatalf("OpenRowReader: %v", err)
			}
			defer func() { _ = rr.(*RowReader).Close() }()

			ch, err := rr.ReadRows(ctx, tbl)
			if err != nil {
				t.Fatalf("ReadRows: %v", err)
			}
			for range ch { //nolint:revive // drain the channel so Err() is final
			}
			rerr := rr.Err()
			if rerr == nil {
				t.Fatal("Err() = nil; want a LOUD storage-class refusal (silent coercion is the failure mode)")
			}
			msg := rerr.Error()
			for _, must := range []string{"m", c.column, c.wantClass, "mismatch"} {
				if !strings.Contains(msg, must) {
					t.Errorf("refusal %q must contain %q (table/column/storage-class/mismatch)", msg, must)
				}
			}
		})
	}
}

func readTable(t *testing.T, path, table string) []ir.Row {
	t.Helper()
	eng := Engine{}
	ctx := context.Background()
	sr, err := eng.OpenSchemaReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(*SchemaReader).Close() }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tbl := tableByName(schema, table)
	if tbl == nil {
		t.Fatalf("table %q missing", table)
	}
	rr, err := eng.OpenRowReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenRowReader: %v", err)
	}
	defer func() { _ = rr.(*RowReader).Close() }()
	ch, err := rr.ReadRows(ctx, tbl)
	if err != nil {
		t.Fatalf("ReadRows: %v", err)
	}
	var out []ir.Row
	for row := range ch {
		out = append(out, row)
	}
	if err := rr.Err(); err != nil {
		t.Fatalf("Err after read of %q: %v", table, err)
	}
	return out
}

func tableByName(s *ir.Schema, name string) *ir.Table {
	for _, t := range s.Tables {
		if t.Name == name {
			return t
		}
	}
	return nil
}

func columnByName(t *ir.Table, name string) *ir.Column {
	for _, c := range t.Columns {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// TestSchemaReader_DefaultClassification_RealDriver ground-truths default
// classification through the REAL driver (modernc) end-to-end: each family of
// the DEFAULT matrix is seeded as source DDL, PRAGMA reports it (stripping
// the OUTER parens off parenthesised expressions — the surface trap that let
// `DEFAULT ('a' || 'b')` masquerade as a quote-endpointed "literal"), and the
// SchemaReader must classify the REPORTED form correctly. The companion
// TestParseDefault_Classification pins the reported-form matrix at unit
// level; this test proves the reported forms are what modernc actually
// reports.
func TestSchemaReader_DefaultClassification_RealDriver(t *testing.T) {
	path := seedDB(
		t,
		`CREATE TABLE dflts (
			id       INTEGER PRIMARY KEY,
			lit_p    TEXT DEFAULT 'abc',
			lit_q    TEXT DEFAULT 'it''s',
			lit_t    TEXT DEFAULT 'x''',
			lit_e    TEXT DEFAULT '',
			lit_bs   TEXT DEFAULT 'a\b',
			lit_par  TEXT DEFAULT ('wrapped'),
			ex_cat   TEXT DEFAULT ('a' || 'b'),
			ex_cat3  TEXT DEFAULT ('a' || 'b' || 'c'),
			ex_fn    INTEGER DEFAULT (abs(-1)),
			ex_nest  TEXT DEFAULT (('x')),
			ex_arith INTEGER DEFAULT (1+2),
			kw_ts    TEXT DEFAULT CURRENT_TIMESTAMP,
			kw_true  INTEGER DEFAULT TRUE,
			bl_hex   BLOB DEFAULT x'00ff',
			nx_hex   INTEGER DEFAULT 0x1A,
			dq_mis   TEXT DEFAULT "misfeature",
			num_i    INTEGER DEFAULT 42,
			num_n    INTEGER DEFAULT -7,
			num_f    REAL DEFAULT 1.5,
			no_dflt  TEXT,
			nul_kw   TEXT DEFAULT NULL
		)`,
	)

	eng := Engine{}
	ctx := bgCtx()
	sr, err := eng.OpenSchemaReader(ctx, path)
	if err != nil {
		t.Fatalf("OpenSchemaReader: %v", err)
	}
	defer func() { _ = sr.(*SchemaReader).Close() }()
	schema, err := sr.ReadSchema(ctx)
	if err != nil {
		t.Fatalf("ReadSchema: %v", err)
	}
	tbl := tableByName(schema, "dflts")
	if tbl == nil {
		t.Fatal("table dflts not read")
	}

	lit := func(v string) ir.DefaultValue { return ir.DefaultLiteral{Value: v} }
	expr := func(e string) ir.DefaultValue { return ir.DefaultExpression{Expr: e, Dialect: "sqlite"} }
	want := map[string]ir.DefaultValue{
		"lit_p":   lit("abc"),
		"lit_q":   lit("it's"),
		"lit_t":   lit("x'"),
		"lit_e":   lit(""),
		"lit_bs":  lit(`a\b`),
		"lit_par": lit("wrapped"), // PRAGMA strips the parens → well-formed literal
		// The bug class: PRAGMA strips the outer parens, so these arrive
		// quote-endpointed and MUST still classify as expressions.
		"ex_cat":   expr(`'a' || 'b'`),
		"ex_cat3":  expr(`'a' || 'b' || 'c'`),
		"ex_fn":    expr(`abs(-1)`),
		"ex_nest":  expr(`('x')`), // PRAGMA strips only the outermost level
		"ex_arith": expr(`1+2`),
		"kw_ts":    expr(`CURRENT_TIMESTAMP`),
		"kw_true":  expr(`TRUE`),
		"bl_hex":   expr(`x'00ff'`),
		"nx_hex":   expr(`0x1A`),
		"dq_mis":   expr(`"misfeature"`),
		"num_i":    lit("42"),
		"num_n":    lit("-7"),
		"num_f":    lit("1.5"),
		"no_dflt":  ir.DefaultNone{},
		"nul_kw":   ir.DefaultNone{},
	}
	for name, w := range want {
		col := columnByName(tbl, name)
		if col == nil {
			t.Errorf("column %q not read", name)
			continue
		}
		if col.Default != w {
			t.Errorf("column %q Default = %#v; want %#v", name, col.Default, w)
		}
	}
}
