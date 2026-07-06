// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// TestEmitColumnType pins the IR → SQLite declared-type map for every IR
// type family, AND that each emitted type reads BACK to the same IR type
// via the reader's resolveColumnType (the round-trip-faithfulness contract,
// ADR-0134 §1). This is the writer half of the Bug-74 family matrix.
func TestEmitColumnType(t *testing.T) {
	cases := []struct {
		name     string
		typ      ir.Type
		want     string
		readback func(ir.Type) bool
	}{
		{"boolean", ir.Boolean{}, "BOOLEAN", isType[ir.Boolean]},
		{"integer", ir.Integer{Width: 64}, "INTEGER", isType[ir.Integer]},
		{"integer8", ir.Integer{Width: 8}, "INTEGER", isType[ir.Integer]},
		{"float", ir.Float{Precision: ir.FloatDouble}, "REAL", isType[ir.Float]},
		{"float single", ir.Float{Precision: ir.FloatSingle}, "REAL", isType[ir.Float]},
		// Bug 162: decimals emit TEXT affinity (exact value), reading back as
		// ir.Text — NUMERIC/DECIMAL affinity silently coerces e.g. 19.99 to a
		// binary float. Value-faithful type downgrade, like json/uuid→TEXT.
		{"decimal unconstrained", ir.Decimal{Unconstrained: true}, "TEXT", isType[ir.Text]},
		{"decimal p,s", ir.Decimal{Precision: 10, Scale: 2}, "TEXT", isType[ir.Text]},
		{"char", ir.Char{Length: 4}, "TEXT", isType[ir.Text]},
		{"varchar", ir.Varchar{Length: 20}, "TEXT", isType[ir.Text]},
		{"text", ir.Text{Size: ir.TextLong}, "TEXT", isType[ir.Text]},
		{"binary", ir.Binary{Length: 4}, "BLOB", isType[ir.Blob]},
		{"varbinary", ir.Varbinary{Length: 4}, "BLOB", isType[ir.Blob]},
		{"blob", ir.Blob{Size: ir.BlobLong}, "BLOB", isType[ir.Blob]},
		{"date", ir.Date{}, "DATE", isType[ir.Date]},
		{"time", ir.Time{}, "TIME", isType[ir.Time]},
		{"datetime", ir.DateTime{}, "DATETIME", isType[ir.Timestamp]},
		{"timestamp", ir.Timestamp{}, "DATETIME", isType[ir.Timestamp]},
		{"timestamptz", ir.Timestamp{WithTimeZone: true}, "DATETIME", isType[ir.Timestamp]},
		{"json", ir.JSON{Binary: true}, "TEXT", isType[ir.Text]},
		{"uuid", ir.UUID{}, "TEXT", isType[ir.Text]},
		{"enum", ir.Enum{Values: []string{"a"}}, "TEXT", isType[ir.Text]},
		{"set", ir.Set{Values: []string{"a"}}, "TEXT", isType[ir.Text]},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := emitColumnType(c.typ)
			if err != nil {
				t.Fatalf("emitColumnType(%s) error: %v", c.typ, err)
			}
			if got != c.want {
				t.Fatalf("emitColumnType(%s) = %q; want %q", c.typ, got, c.want)
			}
			// Round-trip: the emitted declared type must resolve back to the
			// same IR family via the reader (the inverse-of-the-reader contract).
			if back := resolveColumnType(got); !c.readback(back) {
				t.Errorf("resolveColumnType(%q) = %#v; does not read back to the source family", got, back)
			}
		})
	}
}

func isType[T ir.Type](t ir.Type) bool { _, ok := t.(T); return ok }

// TestEmitColumnTypeRefusals pins the loud refusals for IR types SQLite
// cannot faithfully hold (never coerced to a silently-wrong text column).
func TestEmitColumnTypeRefusals(t *testing.T) {
	refused := []ir.Type{
		ir.Geometry{},
		ir.Inet{},
		ir.Cidr{},
		ir.Macaddr{},
		ir.Bit{Length: 8},
		ir.Bit{Length: 8, Varying: true},
		ir.Interval{},
		ir.Array{Element: ir.Integer{Width: 32}},
		ir.Domain{Name: "d", BaseType: ir.Integer{Width: 32}},
		ir.ExtensionType{Extension: "hstore"},
	}
	for _, typ := range refused {
		if _, err := emitColumnType(typ); err == nil {
			t.Errorf("emitColumnType(%s) err = nil; want a loud refusal", typ)
		}
	}
}

// TestEmitTableDef pins the inline-everything CREATE TABLE: a single
// INTEGER PK lands inline (rowid alias, no NOT NULL), CHECK + FK are
// inline, and a composite PK uses a table-level clause (ADR-0134 §3).
func TestEmitTableDef(t *testing.T) {
	tbl := &ir.Table{
		Name: "posts",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "user_id", Type: ir.Integer{Width: 64}, Nullable: false},
			{Name: "body", Type: ir.Text{}, Nullable: true},
			{Name: "qty", Type: ir.Integer{Width: 32}, Nullable: false, Default: ir.DefaultLiteral{Value: "0"}},
		},
		PrimaryKey:       &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
		CheckConstraints: []*ir.CheckConstraint{{Name: "qty_nn", Expr: "qty >= 0"}},
		ForeignKeys: []*ir.ForeignKey{{
			Name: "posts_user_fk", Columns: []string{"user_id"},
			ReferencedTable: "users", ReferencedColumns: []string{"id"},
			OnDelete: ir.FKActionCascade,
		}},
	}
	ddl, err := emitTableDef(tbl)
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	wantContains := []string{
		`"id" INTEGER PRIMARY KEY`,
		`"user_id" INTEGER NOT NULL`,
		`"qty" INTEGER NOT NULL DEFAULT '0'`,
		`CONSTRAINT "qty_nn" CHECK (qty >= 0)`,
		`CONSTRAINT "posts_user_fk" FOREIGN KEY ("user_id") REFERENCES "users" ("id") ON DELETE CASCADE`,
	}
	for _, w := range wantContains {
		if !strings.Contains(ddl, w) {
			t.Errorf("emitTableDef missing %q\nDDL:\n%s", w, ddl)
		}
	}
	// The single integer PK is inline, so NO table-level PRIMARY KEY clause
	// and NO NOT NULL on the rowid alias.
	if strings.Contains(ddl, `PRIMARY KEY ("id")`) {
		t.Errorf("single INTEGER PK should be inline, not a table-level clause:\n%s", ddl)
	}
	if strings.Contains(ddl, `"id" INTEGER PRIMARY KEY NOT NULL`) {
		t.Errorf("rowid-alias PK must not carry NOT NULL (breaks auto-increment):\n%s", ddl)
	}

	// Composite PK → table-level clause, no inline PRIMARY KEY on a column.
	comp := &ir.Table{
		Name: "post_tags",
		Columns: []*ir.Column{
			{Name: "post_id", Type: ir.Integer{Width: 64}},
			{Name: "tag", Type: ir.Text{}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "post_id"}, {Column: "tag"}}, Unique: true},
	}
	cddl, err := emitTableDef(comp)
	if err != nil {
		t.Fatalf("emitTableDef composite: %v", err)
	}
	if !strings.Contains(cddl, `PRIMARY KEY ("post_id", "tag")`) {
		t.Errorf("composite PK should be a table-level clause:\n%s", cddl)
	}
}

// TestEmitTableDefGeneratedColumn pins the generated-column emit (STORED /
// VIRTUAL, verbatim body, no DEFAULT).
func TestEmitTableDefGeneratedColumn(t *testing.T) {
	tbl := &ir.Table{
		Name: "li",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "qty", Type: ir.Integer{Width: 32}, Nullable: false},
			{Name: "total", Type: ir.Integer{Width: 64}, GeneratedExpr: "qty * 2", GeneratedStored: true},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}, Unique: true},
	}
	ddl, err := emitTableDef(tbl)
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	if !strings.Contains(ddl, `"total" INTEGER AS (qty * 2) STORED`) {
		t.Errorf("generated column not emitted as expected:\n%s", ddl)
	}
}

// TestEmitCreateIndex pins a secondary, a unique, and a partial index.
func TestEmitCreateIndex(t *testing.T) {
	plain, err := emitCreateIndex("t", &ir.Index{Name: "t_a", Columns: []ir.IndexColumn{{Column: "a"}}})
	if err != nil {
		t.Fatal(err)
	}
	if plain != `CREATE INDEX IF NOT EXISTS "t_a" ON "t" ("a")` {
		t.Errorf("plain index = %q", plain)
	}
	uniq, _ := emitCreateIndex("t", &ir.Index{Name: "t_b", Unique: true, Columns: []ir.IndexColumn{{Column: "b", Desc: true}}})
	if !strings.Contains(uniq, "UNIQUE INDEX") || !strings.Contains(uniq, `"b" DESC`) {
		t.Errorf("unique/desc index = %q", uniq)
	}
	part, _ := emitCreateIndex("t", &ir.Index{
		Name: "t_c", Columns: []ir.IndexColumn{{Column: "c"}}, Predicate: "active = 1",
	})
	if !strings.HasSuffix(part, "WHERE active = 1") {
		t.Errorf("partial index = %q", part)
	}
}

// TestEmitCreateView pins the verbatim view body.
func TestEmitCreateView(t *testing.T) {
	got := emitCreateView(&ir.View{Name: "v", Definition: "SELECT id FROM t;"})
	if got != `CREATE VIEW IF NOT EXISTS "v" AS SELECT id FROM t` {
		t.Errorf("emitCreateView = %q", got)
	}
}

// TestWrapSQLiteExpressionDefault pins the DEFAULT-body re-parenthesisation:
// SQLite's grammar accepts an expression DEFAULT only parenthesised, and the
// IR carries bodies with PRAGMA's outer parens stripped — every bare family
// wraps, an already-wrapped body passes through.
func TestWrapSQLiteExpressionDefault(t *testing.T) {
	cases := []struct{ in, want string }{
		{`'a' || 'b'`, `('a' || 'b')`},
		{`datetime('now')`, `(datetime('now'))`},
		{`CURRENT_TIMESTAMP`, `(CURRENT_TIMESTAMP)`},
		{`x'00ff'`, `(x'00ff')`},
		{`TRUE`, `(TRUE)`},
		{`1+2`, `(1+2)`},
		{`('x')`, `('x')`}, // already wrapped — pass through
		{`(datetime('now'))`, `(datetime('now'))`},
	}
	for _, tc := range cases {
		if got := wrapSQLiteExpressionDefault(tc.in); got != tc.want {
			t.Errorf("wrapSQLiteExpressionDefault(%q) = %q; want %q", tc.in, got, tc.want)
		}
	}
}

// TestEmitDefault_HexLiteralBlob pins the BINARY/VARBINARY DEFAULT round-trip
// on the SQLite side: a hexLiteralDialect-tagged `0x<hex>` default (the MySQL
// reader's tag) renders as a SQLite BLOB literal `X'<hex>'`, NOT the generic
// `(0x…)` wrap — which SQLite parses as an overflowing integer, silently
// landing a wrong default. Covers the class:
//
//   - exact-width fixed BINARY(14) → unchanged payload;
//   - VARBINARY → NEVER padded (width-agnostic on storage);
//   - NUL-padded shorter-than-width BINARY(8): MySQL reports the UNpadded
//     literal `0x6162` but stores 8 bytes, so the writer zero-fills to the
//     declared width — a width-agnostic BLOB can't re-pad at INSERT time.
func TestEmitDefault_HexLiteralBlob(t *testing.T) {
	cases := []struct {
		name string
		typ  ir.Type
		in   string
		want string
	}{
		{"binary_exact", ir.Binary{Length: 14}, "0x3139373030313031303030303030", `X'3139373030313031303030303030'`},
		{"varbinary_unpadded", ir.Varbinary{Length: 20}, "0x68656c6c6f", `X'68656c6c6f'`},
		{"binary_nul_padded", ir.Binary{Length: 8}, "0x6162", `X'6162000000000000'`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := emitDefault(ir.DefaultExpression{Expr: tc.in, Dialect: hexLiteralDialect}, tc.typ)
			if !ok {
				t.Fatalf("emitDefault(%q) ok=false; want it emitted", tc.in)
			}
			if got != tc.want {
				t.Errorf("emitDefault(%q) on %s = %q; want %q (BLOB literal, not (0x…) integer wrap)", tc.in, tc.typ, got, tc.want)
			}
		})
	}
}

// TestQuoteSQLString_BackslashDeliberatelyNotEscaped is the SEC-1b
// reverse-direction pin (MySQL-source backslash-bearing literals → SQLite
// target). SQLite's quoteSQLString doubles ONLY the interior single quote; it
// deliberately does NOT escape backslashes, because SQLite has NO backslash
// escapes in '…' string literals at all — a backslash is always an ordinary
// character, so the raw value bytes round-trip verbatim. Doubling would STORE a
// second backslash (silent corruption). The MySQL reader hands RAW value bytes
// to the writer (COLUMN_DEFAULT arrives decoded — ground-truthed on MySQL 8.0),
// so these inputs are exactly what a MySQL source produces for a DefaultLiteral.
// The {plain, trailing, doubled, quote-adjacent} backslash matrix:
func TestQuoteSQLString_BackslashDeliberatelyNotEscaped(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"plain interior backslash", `a\b`, `'a\b'`},
		{"trailing backslash", `ab\`, `'ab\'`},
		{"doubled backslash", `a\\b`, `'a\\b'`},
		{"quote-adjacent backslash", `a\'b`, `'a\''b'`},
	}
	for _, c := range cases {
		if got := quoteSQLString(c.in); got != c.want {
			t.Errorf("%s: quoteSQLString(%q) = %q; want %q (SQLite has no backslash escapes — do NOT double it)", c.name, c.in, got, c.want)
		}
	}
}
