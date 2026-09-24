// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// lobDefaultCase is one (column family × default shape) cell of the
// GC-36 item 4 matrix. want maps each server form to the exact DEFAULT
// clause body; an empty string means the default must be DROPPED from the
// column def and reported lost.
type lobDefaultCase struct {
	name string
	typ  ir.Type
	dflt ir.DefaultValue
	want map[lobDefaultForm]string
}

// lobDefaultMatrix covers every large-object family mysqlLOBFamily names
// (text, the wide-VARCHAR text tier, a DOMAIN over text, blob, json,
// array, hstore, geometry) × {literal, expression} × every server form,
// plus the non-LOB control that must be untouched by any of it.
func lobDefaultMatrix() []lobDefaultCase {
	all := func(s string) map[lobDefaultForm]string {
		return map[lobDefaultForm]string{lobDefaultMySQL: s, lobDefaultMariaDB: s, lobDefaultNone: ""}
	}
	lostEverywhere := map[lobDefaultForm]string{lobDefaultMySQL: "", lobDefaultMariaDB: "", lobDefaultNone: ""}
	lit := func(s string) ir.DefaultValue { return ir.DefaultLiteral{Value: s} }
	mysqlExpr := func(s string) ir.DefaultValue { return ir.DefaultExpression{Expr: s, Dialect: "mysql"} }
	text := ir.Text{Size: ir.TextRegular}
	return []lobDefaultCase{
		// ---- text ----
		{"text/plain", text, lit("abc"), all("('abc')")},
		{"text/empty", text, lit(""), all("('')")},
		{"text/quote", text, lit("it's"), map[lobDefaultForm]string{
			lobDefaultMySQL:   "(CONVERT(UNHEX('69742773') USING utf8mb4))",
			lobDefaultMariaDB: "('it''s')",
			lobDefaultNone:    "",
		}},
		{"text/LF", text, lit("a\nb"), map[lobDefaultForm]string{
			lobDefaultMySQL:   "(CONVERT(UNHEX('610A62') USING utf8mb4))",
			lobDefaultMariaDB: "('a\nb')",
			lobDefaultNone:    "",
		}},
		{"text/CR", text, lit("a\rb"), map[lobDefaultForm]string{
			lobDefaultMySQL:   "(CONVERT(UNHEX('610D62') USING utf8mb4))",
			lobDefaultMariaDB: "('a\rb')",
			lobDefaultNone:    "",
		}},
		{"text/ctrl-Z", text, lit("a\x1ab"), map[lobDefaultForm]string{
			lobDefaultMySQL:   "(CONVERT(UNHEX('611A62') USING utf8mb4))",
			lobDefaultMariaDB: "('a\x1ab')",
			lobDefaultNone:    "",
		}},
		{"text/double quote is printed raw", text, lit(`a"b`), all(`('a"b')`)},
		{"text/unicode", text, lit("héllo ✓ 日本"), all("('héllo ✓ 日本')")},
		{"text/the string NULL", text, lit("NULL"), all("('NULL')")},
		{"text/tab", ir.Text{Size: ir.TextLong}, lit("tab\there"), all("('tab\there')")},
		// MYSQL-EXPRESSION-DEFAULT-BACKSLASH: MySQL re-parses a stored
		// expression under the opening session's sql_mode, so a backslash
		// (or NUL) string is spelled with no escape at all there.
		{"text/backslash", text, lit(`a\b`), map[lobDefaultForm]string{
			lobDefaultMySQL:   "(CONVERT(UNHEX('615C62') USING utf8mb4))",
			lobDefaultMariaDB: `('a\\b')`,
			lobDefaultNone:    "",
		}},
		{"text/NUL", text, lit("a\x00b"), map[lobDefaultForm]string{
			lobDefaultMySQL:   "(CONVERT(UNHEX('610062') USING utf8mb4))",
			lobDefaultMariaDB: "('a\x00b')",
			lobDefaultNone:    "",
		}},
		{"text/wide varchar down-maps to a TEXT tier", ir.Varchar{Length: 20000}, lit("abc"), all("('abc')")},
		{"text/domain over text", ir.Domain{Name: "d", BaseType: text}, lit("abc"), all("('abc')")},
		{"text/expression", text, mysqlExpr("'txt'"), all("('txt')")},
		{"text/function expression", text, mysqlExpr("uuid()"), all("(uuid())")},

		// ---- blob ----
		{"blob/plain bytes", ir.Blob{Size: ir.BlobLong}, lit("abc"), all("(X'616263')")},
		{"blob/empty", ir.Blob{Size: ir.BlobRegular}, lit(""), all("(X'')")},
		{"blob/high bytes", ir.Blob{Size: ir.BlobRegular}, lit("\xff\x00\x80"), all("(X'FF0080')")},
		// BLOB-DEFAULT-LITERAL-ENCODING: raw bytes or a PG rendering —
		// nothing says which, so it is refused, never read by inspection.
		{"blob/backslash literal is ambiguous", ir.Blob{Size: ir.BlobLong}, lit(`\x00ab00`), lostEverywhere},
		{"blob/escape-format literal is ambiguous", ir.Blob{Size: ir.BlobLong}, lit(`a\\b`), lostEverywhere},
		{"blob/hex-bytes expression", ir.Blob{Size: ir.BlobLong}, ir.DefaultExpression{Expr: "0x00AB00", Dialect: hexLiteralDialect}, all("(0x00AB00)")},
		{"blob/mysql expression", ir.Blob{Size: ir.BlobRegular}, mysqlExpr("0x00ff"), all("(0x00ff)")},

		// ---- json ----
		{"json/object", ir.JSON{Binary: true}, lit(`{"a": 1}`), all(`('{"a": 1}')`)},
		{"json/empty object", ir.JSON{}, lit(`{}`), all(`('{}')`)},
		{"json/escaped string", ir.JSON{}, lit(`["x\\y"]`), map[lobDefaultForm]string{
			lobDefaultMySQL:   "(CONVERT(UNHEX('5B22785C5C79225D') USING utf8mb4))",
			lobDefaultMariaDB: `('["x\\\\y"]')`,
			lobDefaultNone:    "",
		}},
		{"json/not a document", ir.JSON{}, lit(`{a`), lostEverywhere},
		{"json/expression", ir.JSON{}, mysqlExpr("json_object()"), all("(json_object())")},

		// ---- array (emitted as JSON; the literal is a PG array literal) ----
		{"array/int", ir.Array{Element: ir.Integer{Width: 32}}, lit("{1,2}"), all("('[1,2]')")},
		{"array/text with quoted element", ir.Array{Element: ir.Text{Size: ir.TextLong}}, lit(`{a,"b c"}`), all(`('["a","b c"]')`)},
		{"array/NULL element", ir.Array{Element: ir.Integer{Width: 32}}, lit("{1,NULL}"), all("('[1,null]')")},
		{"array/empty", ir.Array{Element: ir.Integer{Width: 32}}, lit("{}"), all("('[]')")},
		{"array/multi-dimensional is refused", ir.Array{Element: ir.Integer{Width: 32}}, lit("{{1},{2}}"), lostEverywhere},
		{"array/boolean", ir.Array{Element: ir.Boolean{}}, lit("{t,f}"), all("('[true,false]')")},
		{"array/varchar", ir.Array{Element: ir.Varchar{Length: 5}}, lit(`{"x\\y"}`), map[lobDefaultForm]string{
			lobDefaultMySQL:   "(CONVERT(UNHEX('5B22785C5C79225D') USING utf8mb4))",
			lobDefaultMariaDB: `('["x\\\\y"]')`,
			lobDefaultNone:    "",
		}},
		{"array/bigint unsigned max", ir.Array{Element: ir.Integer{Width: 64}}, lit("{18446744073709551615}"), all("('[18446744073709551615]')")},
		{"array/a non-integer in an int array is refused", ir.Array{Element: ir.Integer{Width: 32}}, lit("{x}"), lostEverywhere},
		{"array/float elements are refused", ir.Array{Element: ir.Float{Precision: ir.FloatDouble}}, lit("{1.5}"), lostEverywhere},
		{"array/numeric elements are refused", ir.Array{Element: ir.Decimal{Precision: 5, Scale: 2}}, lit("{1.50}"), lostEverywhere},
		{"array/uuid elements are refused", ir.Array{Element: ir.UUID{}}, lit("{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}"), lostEverywhere},
		{"array/timestamp elements are refused", ir.Array{Element: ir.Timestamp{}}, lit(`{"2024-01-02 03:04:05"}`), lostEverywhere},
		{"array/bytea elements are refused", ir.Array{Element: ir.Blob{Size: ir.BlobLong}}, lit(`{"\\xdead"}`), lostEverywhere},
		{"array/json elements are refused", ir.Array{Element: ir.JSON{}}, lit(`{"{}"}`), lostEverywhere},
		{"array/no element type is refused", ir.Array{}, lit("{1}"), lostEverywhere},

		// ---- hstore (emitted as JSON) ----
		{"hstore/pairs", ir.ExtensionType{Extension: "hstore", Name: "hstore"}, lit(`"a"=>"1"`), all(`('{"a":"1"}')`)},

		// ---- geometry ----
		{"geometry/literal has no provable spelling", ir.Geometry{Subtype: ir.GeometryPoint}, lit("0101000000"), lostEverywhere},
		{"geometry/expression", ir.Geometry{Subtype: ir.GeometryUnspecified}, mysqlExpr("st_geomfromtext('POINT(1 2)')"), all("(st_geomfromtext('POINT(1 2)'))")},
	}
}

// TestEmitColumnDef_LOBDefaultMatrix pins GC-36 item 4: every
// large-object family's DEFAULT is carried in the parenthesised form both
// MySQL 8.0.13+ and MariaDB accept (measured — see
// ddl_emit_lob_default.go), translated per family, or dropped when the
// server or the value cannot hold it. The server-side proof that each
// carried form reads back the source value is the pre-existing-row gate
// (internal/pipeline, TestStreamer_AddColumnForward_PreexistingRowDefaults_*)
// and TestMigrate_LOBDefaultsLandOnTheTarget_*.
func TestEmitColumnDef_LOBDefaultMatrix(t *testing.T) {
	forms := map[lobDefaultForm]string{lobDefaultMySQL: "mysql-8.0.13+", lobDefaultMariaDB: "mariadb", lobDefaultNone: "mysql-pre-8.0.13"}
	for _, c := range lobDefaultMatrix() {
		for form, formName := range forms {
			t.Run(c.name+"/"+formName, func(t *testing.T) {
				m := stdEmitter
				m.lobDefaults = form
				col := &ir.Column{Name: "c", Type: c.typ, Nullable: true, Default: c.dflt}
				got, err := m.emitColumnDef("t", col)
				if err != nil {
					t.Fatalf("emitColumnDef: %v", err)
				}
				want := c.want[form]
				lost := m.lobDefaultLoss(col)
				if want == "" {
					if strings.Contains(got, " DEFAULT ") {
						t.Errorf("emitColumnDef = %q; want the DEFAULT dropped", got)
					}
					if lost == "" {
						t.Error("lobDefaultLoss = \"\"; want a reason (AlterAddColumn must refuse this default)")
					}
					return
				}
				if !strings.HasSuffix(got, " DEFAULT "+want) {
					t.Errorf("emitColumnDef = %q; want it to end in DEFAULT %s", got, want)
				}
				if lost != "" {
					t.Errorf("lobDefaultLoss = %q; want \"\" for a carried default", lost)
				}
			})
		}
	}
}

// TestEmitColumnDef_LOBDefaultReadsThroughARetarget pins the forwarded
// ADD COLUMN shape: the PG → MySQL retarget (and `--type-override`)
// rewrites an array or hstore column to JSON and parks the original in
// SourceColumnType, while the DEFAULT is still the source's literal. The
// family comes from the provenance, as the row writer's array lane does.
func TestEmitColumnDef_LOBDefaultReadsThroughARetarget(t *testing.T) {
	m := stdEmitter
	m.lobDefaults = lobDefaultMySQL
	cases := []struct {
		name string
		src  ir.Type
		dflt string
		want string // "" = dropped
	}{
		{"int array", ir.Array{Element: ir.Integer{Width: 32}}, "{1,2}", "('[1,2]')"},
		{"text array", ir.Array{Element: ir.Text{Size: ir.TextLong}}, `{a,"b c"}`, `('["a","b c"]')`},
		{"empty array is an array, not the JSON object {}", ir.Array{Element: ir.Integer{Width: 32}}, "{}", "('[]')"},
		{"hstore", ir.ExtensionType{Extension: "hstore", Name: "hstore"}, `"a"=>"1"`, `('{"a":"1"}')`},
		{"text overridden to json keeps the JSON reading", ir.Text{Size: ir.TextLong}, `{"a": 1}`, `('{"a": 1}')`},
		{"float array still refused", ir.Array{Element: ir.Float{Precision: ir.FloatDouble}}, "{1.5}", ""},
	}
	for _, c := range cases {
		col := &ir.Column{Name: "c", Type: ir.JSON{Binary: true}, SourceColumnType: c.src, Nullable: true, Default: ir.DefaultLiteral{Value: c.dflt}}
		got, err := m.emitColumnDef("t", col)
		if err != nil {
			t.Fatalf("%s: emitColumnDef: %v", c.name, err)
		}
		switch {
		case c.want == "" && strings.Contains(got, " DEFAULT "):
			t.Errorf("%s: emitColumnDef = %q; want the DEFAULT dropped", c.name, got)
		case c.want != "" && !strings.HasSuffix(got, " DEFAULT "+c.want):
			t.Errorf("%s: emitColumnDef = %q; want it to end in DEFAULT %s", c.name, got, c.want)
		}
	}
}

// TestEmitColumnDef_NonLOBDefaultsUntouchedByServerForm is the control: a
// column outside the large-object families renders its DEFAULT exactly as
// before, whatever the server accepts on a LOB column.
func TestEmitColumnDef_NonLOBDefaultsUntouchedByServerForm(t *testing.T) {
	cols := []*ir.Column{
		{Name: "v", Type: ir.Varchar{Length: 20}, Default: ir.DefaultLiteral{Value: `a\b`}},
		{Name: "i", Type: ir.Integer{Width: 32}, Default: ir.DefaultLiteral{Value: "42"}},
		{Name: "b", Type: ir.Varbinary{Length: 4}, Default: ir.DefaultExpression{Expr: "0xAB00", Dialect: hexLiteralDialect}},
		{Name: "g", Type: ir.Integer{Width: 32}, Default: ir.DefaultExpression{Expr: "1 + 2", Dialect: "mysql"}},
	}
	for _, col := range cols {
		want, err := stdEmitter.emitColumnDef("t", col)
		if err != nil {
			t.Fatalf("emitColumnDef(%s): %v", col.Name, err)
		}
		for _, form := range []lobDefaultForm{lobDefaultMySQL, lobDefaultMariaDB} {
			m := stdEmitter
			m.lobDefaults = form
			got, err := m.emitColumnDef("t", col)
			if err != nil {
				t.Fatalf("emitColumnDef(%s): %v", col.Name, err)
			}
			if got != want {
				t.Errorf("column %s, form %d: %q; want the unchanged %q", col.Name, form, got, want)
			}
		}
	}
}

// TestLOBDefaultFormFor pins the SELECT VERSION() classification,
// including the 8.0.12 / 8.0.13 boundary where MySQL gained expression
// defaults and the shapes Vitess and MariaDB report.
func TestLOBDefaultFormFor(t *testing.T) {
	cases := map[string]lobDefaultForm{
		"8.0.12":                   lobDefaultNone,
		"8.0.13":                   lobDefaultMySQL,
		"8.0.46":                   lobDefaultMySQL,
		"8.4.11":                   lobDefaultMySQL,
		"9.1.0":                    lobDefaultMySQL,
		"5.7.44-log":               lobDefaultNone,
		"8.0.40-Vitess":            lobDefaultMySQL,
		"11.4.13-MariaDB-ubu2404":  lobDefaultMariaDB,
		"5.5.5-10.11.8-MariaDB":    lobDefaultMariaDB,
		"10.2.44-MariaDB":          lobDefaultMariaDB,
		"10.1.48-MariaDB":          lobDefaultNone,
		"MariaDB-without-a-number": lobDefaultNone,
		"not a version":            lobDefaultNone,
		"":                         lobDefaultNone,
	}
	for version, want := range cases {
		if got := lobDefaultFormFor(version); got != want {
			t.Errorf("lobDefaultFormFor(%q) = %d; want %d", version, got, want)
		}
	}
}

// TestRefuseLOBDefaultNotCarried pins the ADD COLUMN refusal's shape: a
// coded error carrying the grep-stable marker and the column it names.
func TestRefuseLOBDefaultNotCarried(t *testing.T) {
	err := refuseLOBDefaultNotCarried("w", "c", "because")
	coded, ok := sluicecode.FromError(err)
	if !ok || coded.Code != sluicecode.CodeValueUnrepresentable {
		t.Fatalf("refusal is not coded %s: %v", sluicecode.CodeValueUnrepresentable, err)
	}
	for _, want := range []string{"LOB-DEFAULT-NOT-CARRIED", "`w`.`c`", "because"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not carry %q", err.Error(), want)
		}
	}
}

// TestTranslateMariaDBDefault_BlobLiteralIsHexBytes pins the MariaDB read
// boundary half of BLOB-DEFAULT-LITERAL-ENCODING: MariaDB takes a literal
// DEFAULT on BLOB (MySQL does not), and it is carried as the hex-bytes
// expression, so a backslash in the bytes is not left for a writer to
// read one way or the other.
func TestTranslateMariaDBDefault_BlobLiteralIsHexBytes(t *testing.T) {
	valid := func(s string) sql.NullString { return sql.NullString{String: s, Valid: true} }
	cases := []struct {
		raw  string
		want ir.DefaultValue
	}{
		{`'abc'`, ir.DefaultExpression{Expr: "0x616263", Dialect: hexLiteralDialect}},
		{`'a\\b'`, ir.DefaultExpression{Expr: "0x615C62", Dialect: hexLiteralDialect}},
		{`'\\x41'`, ir.DefaultExpression{Expr: "0x5C783431", Dialect: hexLiteralDialect}},
		{`''`, ir.DefaultLiteral{Value: ""}},
	}
	for _, c := range cases {
		got := translateMariaDBDefault(valid(c.raw), "", ir.Blob{Size: ir.BlobRegular})
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("translateMariaDBDefault(%s) on BLOB = %#v; want %#v", c.raw, got, c.want)
		}
	}
}
