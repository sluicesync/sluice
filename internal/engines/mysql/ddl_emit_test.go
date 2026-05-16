// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

func TestEmitColumnType(t *testing.T) {
	cases := []struct {
		name string
		in   ir.Type
		want string
	}{
		// ---- Numeric / Boolean ----
		{"boolean", ir.Boolean{}, "TINYINT(1)"},
		{"tinyint", ir.Integer{Width: 8}, "TINYINT"},
		{"smallint", ir.Integer{Width: 16}, "SMALLINT"},
		{"mediumint", ir.Integer{Width: 24}, "MEDIUMINT"},
		{"int", ir.Integer{Width: 32}, "INT"},
		{"bigint", ir.Integer{Width: 64}, "BIGINT"},
		{"int unsigned", ir.Integer{Width: 32, Unsigned: true}, "INT UNSIGNED"},
		{"bigint unsigned auto_increment", ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true}, "BIGINT UNSIGNED AUTO_INCREMENT"},
		{"decimal", ir.Decimal{Precision: 10, Scale: 2}, "DECIMAL(10,2)"},
		{"float single", ir.Float{Precision: ir.FloatSingle}, "FLOAT"},
		{"float double", ir.Float{Precision: ir.FloatDouble}, "DOUBLE"},

		// ---- Character ----
		{"char no charset", ir.Char{Length: 10}, "CHAR(10)"},
		{
			"varchar with charset",
			ir.Varchar{Length: 255, Charset: "utf8mb4", Collation: "utf8mb4_unicode_ci"},
			"VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
		},
		{"text tiny", ir.Text{Size: ir.TextTiny}, "TINYTEXT"},
		{"text regular", ir.Text{Size: ir.TextRegular}, "TEXT"},
		{"text medium", ir.Text{Size: ir.TextMedium}, "MEDIUMTEXT"},
		{"text long", ir.Text{Size: ir.TextLong}, "LONGTEXT"},
		{"text with charset", ir.Text{Size: ir.TextRegular, Charset: "utf8mb4"}, "TEXT CHARACTER SET utf8mb4"},

		// ---- Binary ----
		{"binary", ir.Binary{Length: 16}, "BINARY(16)"},
		{"varbinary", ir.Varbinary{Length: 64}, "VARBINARY(64)"},
		{"blob tiny", ir.Blob{Size: ir.BlobTiny}, "TINYBLOB"},
		{"blob regular", ir.Blob{Size: ir.BlobRegular}, "BLOB"},
		{"blob medium", ir.Blob{Size: ir.BlobMedium}, "MEDIUMBLOB"},
		{"blob long", ir.Blob{Size: ir.BlobLong}, "LONGBLOB"},

		// ---- Temporal ----
		{"date", ir.Date{}, "DATE"},
		{"time precision 0", ir.Time{Precision: 0}, "TIME"},
		{"time precision 6", ir.Time{Precision: 6}, "TIME(6)"},
		{"datetime precision 0", ir.DateTime{Precision: 0}, "DATETIME"},
		{"datetime precision 3", ir.DateTime{Precision: 3}, "DATETIME(3)"},
		{"timestamp precision 0", ir.Timestamp{Precision: 0, WithTimeZone: true}, "TIMESTAMP"},
		{"timestamp precision 6", ir.Timestamp{Precision: 6, WithTimeZone: true}, "TIMESTAMP(6)"},

		// ---- Structured ----
		{"json", ir.JSON{Binary: true}, "JSON"},

		// ---- Categorical ----
		{"enum", ir.Enum{Values: []string{"red", "green", "blue"}}, "ENUM('red','green','blue')"},
		{"enum with apostrophe", ir.Enum{Values: []string{"it's"}}, "ENUM('it''s')"},
		{"set", ir.Set{Values: []string{"a", "b"}}, "SET('a','b')"},

		// ---- Identity / spatial ----
		{"uuid", ir.UUID{}, "CHAR(36)"},
		{"geometry default", ir.Geometry{Subtype: ir.GeometryUnspecified}, "GEOMETRY"},
		{"point", ir.Geometry{Subtype: ir.GeometryPoint}, "POINT"},
		{"polygon", ir.Geometry{Subtype: ir.GeometryPolygon}, "POLYGON"},
		{"geometrycollection", ir.Geometry{Subtype: ir.GeometryCollection}, "GEOMETRYCOLLECTION"},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := emitColumnType(c.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("emitColumnType(%T) = %q; want %q", c.in, got, c.want)
			}
		})
	}
}

// TestEmitColumnType_ExtensionTypeRefuses confirms the MySQL writer
// refuses ir.ExtensionType columns without a default cross-engine
// translator (pgvector, pg_trgm, postgis) loudly (ADR-0032). The
// cross-engine refusal in pipeline.checkCrossEngineSupportable
// normally fires before MySQL's writer is invoked, but this defends
// in depth against hand-constructed IR.
func TestEmitColumnType_ExtensionTypeRefuses(t *testing.T) {
	col := ir.ExtensionType{
		Extension: "vector",
		Name:      "vector",
		Modifiers: []int{384},
	}
	_, err := emitColumnType(col)
	if err == nil {
		t.Fatal("expected error on PG ExtensionType in MySQL writer; got nil")
	}
	if !strings.Contains(err.Error(), "PG extension") {
		t.Errorf("err = %v; want mention of \"PG extension\"", err)
	}
	if !strings.Contains(err.Error(), "--type-override") {
		t.Errorf("err = %v; want hint mentioning --type-override", err)
	}
}

// TestEmitColumnType_HstoreCrossEngine emits MySQL JSON for an
// ir.ExtensionType{Extension: "hstore"} column. The cross-engine
// translator (ADR-0032 § "Cross-engine policy") is wired into the
// MySQL writer directly so the migrate path lands hstore columns as
// MySQL JSON without operator intervention.
func TestEmitColumnType_HstoreCrossEngine(t *testing.T) {
	got, err := emitColumnType(ir.ExtensionType{Extension: "hstore", Name: "hstore"})
	if err != nil {
		t.Fatalf("emitColumnType hstore: %v", err)
	}
	if got != "JSON" {
		t.Errorf("hstore emit = %q; want %q", got, "JSON")
	}
}

// TestEmitColumnType_CiTextCrossEngine emits MySQL VARCHAR with the
// case-insensitive collation utf8mb4_0900_ai_ci for an
// ir.ExtensionType{Extension: "citext"} column. The collation
// suffix is the load-bearing piece — without it the cross-engine
// behaviour change (case-insensitive comparison) is lost.
func TestEmitColumnType_CiTextCrossEngine(t *testing.T) {
	got, err := emitColumnType(ir.ExtensionType{Extension: "citext", Name: "citext"})
	if err != nil {
		t.Fatalf("emitColumnType citext: %v", err)
	}
	if !strings.Contains(got, "VARCHAR") {
		t.Errorf("citext emit = %q; want VARCHAR base", got)
	}
	if !strings.Contains(got, "utf8mb4_0900_ai_ci") {
		t.Errorf("citext emit = %q; want utf8mb4_0900_ai_ci collation", got)
	}
}

// TestEmitColumnType_PGNativeAutoEmit verifies the v0.7.0 auto-emit
// of PG-native types that lack a direct MySQL equivalent. Pre-v0.7.0
// these returned an error pointing at --type-override; v0.7.0 emits
// a sensible default so PG→MySQL migrations don't require per-column
// intervention. Operators wanting strict syntactic validation (e.g.
// CHECK regex on Inet) still use --type-override; the schema-preview
// command surfaces the auto-emit choice so it isn't silent.
func TestEmitColumnType_PGNativeAutoEmit(t *testing.T) {
	cases := []struct {
		typ  ir.Type
		want string
	}{
		{ir.Inet{}, "VARCHAR(45)"},
		{ir.Cidr{}, "VARCHAR(45)"},
		{ir.Macaddr{}, "VARCHAR(30)"},
		{ir.Array{Element: ir.Integer{Width: 32}}, "JSON"},
	}
	for _, c := range cases {
		c := c
		t.Run(typeName(c.typ), func(t *testing.T) {
			got, err := emitColumnType(c.typ)
			if err != nil {
				t.Fatalf("emitColumnType(%T) returned error: %v", c.typ, err)
			}
			if got != c.want {
				t.Errorf("emitColumnType(%T) = %q; want %q", c.typ, got, c.want)
			}
		})
	}
}

func TestEmitDefault(t *testing.T) {
	cases := []struct {
		name     string
		in       ir.DefaultValue
		colType  ir.Type
		want     string
		wantEmit bool
	}{
		{"none", ir.DefaultNone{}, ir.Integer{Width: 32}, "", false},
		{"nil", nil, ir.Integer{Width: 32}, "", false},
		{"literal zero", ir.DefaultLiteral{Value: "0"}, ir.Integer{Width: 32}, "'0'", true},
		{"literal text", ir.DefaultLiteral{Value: "hello"}, ir.Varchar{Length: 32}, "'hello'", true},
		{"literal with quote", ir.DefaultLiteral{Value: "it's"}, ir.Varchar{Length: 32}, "'it''s'", true},
		{"expression", ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP"}, ir.Timestamp{}, "CURRENT_TIMESTAMP", true},

		// Boolean literal translation: PG hands us "true"/"false",
		// MySQL needs "1"/"0".
		{"bool literal true", ir.DefaultLiteral{Value: "true"}, ir.Boolean{}, "1", true},
		{"bool literal false", ir.DefaultLiteral{Value: "false"}, ir.Boolean{}, "0", true},
		{"bool literal TRUE uppercase", ir.DefaultLiteral{Value: "TRUE"}, ir.Boolean{}, "1", true},
		{"bool short t", ir.DefaultLiteral{Value: "t"}, ir.Boolean{}, "1", true},
		{"bool short f", ir.DefaultLiteral{Value: "f"}, ir.Boolean{}, "0", true},
		// "1"/"0" on a bool column emit unquoted — they arrive either
		// from MySQL itself or, post catalog-#4, from the reader's
		// bit-literal → decimal conversion (bit(1) → ir.Boolean). The
		// unquoted form is the clean MySQL TINYINT(1) default and
		// avoids a strict-mode quoted-numeric coercion.
		{"bool literal 1 unquoted", ir.DefaultLiteral{Value: "1"}, ir.Boolean{}, "1", true},
		{"bool literal 0 unquoted (bit b'0' → 0)", ir.DefaultLiteral{Value: "0"}, ir.Boolean{}, "0", true},

		// PG → MySQL DefaultExpression translation. PG's canonical
		// "current timestamp" function is now(); MySQL's is
		// CURRENT_TIMESTAMP. Lookup is case-insensitive after trim.
		{"pg now()", ir.DefaultExpression{Expr: "now()"}, ir.Timestamp{}, "CURRENT_TIMESTAMP", true},
		{"pg NOW() uppercase", ir.DefaultExpression{Expr: "NOW()"}, ir.Timestamp{}, "CURRENT_TIMESTAMP", true},
		{"pg now() with whitespace", ir.DefaultExpression{Expr: " now() "}, ir.Timestamp{}, "CURRENT_TIMESTAMP", true},
		// Already-canonical CURRENT_TIMESTAMP (from MySQL or from
		// stripTypeCast on PG) passes through unchanged when the
		// column has zero fractional precision.
		{"current_timestamp passthrough", ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP"}, ir.Timestamp{}, "CURRENT_TIMESTAMP", true},

		// Precision-matching for CURRENT_TIMESTAMP defaults: MySQL
		// rejects "TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP" because the
		// function-call precision must equal the column's declared
		// precision. The translator promotes the bare CURRENT_TIMESTAMP
		// to CURRENT_TIMESTAMP(N) when the column carries a non-zero N.
		// Common path: PG TIMESTAMPTZ DEFAULT now() — PG reports
		// Precision=6 (its default), now() → CURRENT_TIMESTAMP, then
		// promoted here to CURRENT_TIMESTAMP(6).
		{"pg now() on TIMESTAMP(6) is precision-matched", ir.DefaultExpression{Expr: "now()"}, ir.Timestamp{Precision: 6, WithTimeZone: true}, "CURRENT_TIMESTAMP(6)", true},
		{"pg now() on TIMESTAMP(3) is precision-matched", ir.DefaultExpression{Expr: "now()"}, ir.Timestamp{Precision: 3}, "CURRENT_TIMESTAMP(3)", true},
		{"current_timestamp on DATETIME(6) is precision-matched", ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP"}, ir.DateTime{Precision: 6}, "CURRENT_TIMESTAMP(6)", true},
		// An expression that *already* declares a precision passes
		// through unchanged — the caller is asserting that precision.
		{"explicit precision passthrough", ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP(6)"}, ir.Timestamp{Precision: 6}, "CURRENT_TIMESTAMP(6)", true},
		// Mismatched explicit precision *is* preserved verbatim — the
		// translator doesn't second-guess a hand-written expression.
		// MySQL will reject it loudly if it's wrong, which matches
		// the project policy.
		{"explicit mismatched precision passthrough", ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP(3)"}, ir.Timestamp{Precision: 6}, "CURRENT_TIMESTAMP(3)", true},

		// PG → MySQL UUID default translation (Bug 42). PG's
		// gen_random_uuid() doesn't exist in MySQL; its canonical
		// MySQL equivalent is UUID() wrapped in the outer parens that
		// MySQL 8.0+ requires for function-call expression defaults.
		// The retarget pass rewrites the column type from UUID to
		// CHAR(36); this rule rewrites the matching default so the
		// CREATE TABLE statement's pieces stay coherent.
		{"pg gen_random_uuid()", ir.DefaultExpression{Expr: "gen_random_uuid()"}, ir.Char{Length: 36}, "(UUID())", true},
		{"pg GEN_RANDOM_UUID() uppercase", ir.DefaultExpression{Expr: "GEN_RANDOM_UUID()"}, ir.Char{Length: 36}, "(UUID())", true},
		{"pg gen_random_uuid() with whitespace", ir.DefaultExpression{Expr: " gen_random_uuid() "}, ir.Char{Length: 36}, "(UUID())", true},

		// PG → MySQL random() default translation (symmetric reverse
		// of v0.11.3's Bug 29 fix). PG's argless random() returns
		// [0, 1); MySQL's RAND() returns the same range. The MySQL
		// 8.0+ outer-parens requirement applies to expression
		// defaults the same way it does for UUID.
		{"pg random()", ir.DefaultExpression{Expr: "random()"}, ir.Float{Precision: ir.FloatDouble}, "(RAND())", true},
		{"pg RANDOM() uppercase", ir.DefaultExpression{Expr: "RANDOM()"}, ir.Float{Precision: ir.FloatDouble}, "(RAND())", true},

		// Bug 44 — same-engine MySQL → MySQL function-call expression
		// defaults need outer parens for MySQL 8.0+. Pre-fix the writer
		// emitted `DEFAULT uuid()` (no parens) → MySQL Error 1064. The
		// wrap helper recognises function-call shape and adds parens;
		// already-wrapped translations like `(UUID())` from Bug 42's
		// pgToMySQLDefaultExpr entry don't get double-wrapped.
		{"bug44 mysql uuid() bare", ir.DefaultExpression{Expr: "uuid()"}, ir.Char{Length: 36}, "(uuid())", true},
		{"bug44 mysql rand() bare", ir.DefaultExpression{Expr: "rand()"}, ir.Float{Precision: ir.FloatDouble}, "(rand())", true},
		{"bug44 mysql UUID() uppercase bare", ir.DefaultExpression{Expr: "UUID()"}, ir.Char{Length: 36}, "(UUID())", true},
		// Already-wrapped expressions (from translations or operator-
		// supplied) pass through unchanged — no double-wrap.
		{"bug44 already-wrapped (UUID()) passthrough", ir.DefaultExpression{Expr: "(UUID())"}, ir.Char{Length: 36}, "(UUID())", true},
		{"bug44 already-wrapped (RAND()*100) passthrough", ir.DefaultExpression{Expr: "(RAND() * 100)"}, ir.Decimal{Precision: 10, Scale: 4}, "(RAND() * 100)", true},
		// Bare temporal keywords stay bare — wrapping them is a syntax
		// error in MySQL (the temporal-keyword grammar is separate
		// from the function-call grammar).
		{"bug44 current_timestamp lowercase passthrough", ir.DefaultExpression{Expr: "current_timestamp"}, ir.Timestamp{}, "current_timestamp", true},
		{"bug44 current_timestamp() empty-parens passthrough", ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP()"}, ir.Timestamp{}, "CURRENT_TIMESTAMP()", true},
		{"bug44 LOCALTIMESTAMP passthrough", ir.DefaultExpression{Expr: "LOCALTIMESTAMP"}, ir.Timestamp{}, "LOCALTIMESTAMP", true},
		{"bug44 LOCALTIME(3) passthrough", ir.DefaultExpression{Expr: "LOCALTIME(3)"}, ir.Time{Precision: 3}, "LOCALTIME(3)", true},
		{"bug44 NOW() bare passthrough", ir.DefaultExpression{Expr: "NOW()"}, ir.Timestamp{}, "CURRENT_TIMESTAMP", true},
		{"bug44 CURRENT_DATE passthrough", ir.DefaultExpression{Expr: "CURRENT_DATE"}, ir.Date{}, "CURRENT_DATE", true},

		// Unrelated expressions still surface MySQL's loud failure on
		// the target — but they get wrapped first per the function-call
		// rule, so the rejection happens for the right reason (MySQL
		// doesn't have the function), not the wrong reason (missing
		// outer parens for the function-call form).
		{"unrelated expr wrapped then loud-fail on target", ir.DefaultExpression{Expr: "uuid_generate_v4()"}, ir.UUID{}, "(uuid_generate_v4())", true},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, ok := emitDefault(c.in, c.colType)
			if ok != c.wantEmit {
				t.Errorf("emit flag = %v; want %v", ok, c.wantEmit)
			}
			if got != c.want {
				t.Errorf("emitDefault = %q; want %q", got, c.want)
			}
		})
	}
}

// TestEmitColumnDef_SuppressesDefaultOnForbidddenTypes pins the v0.32.2
// fix: MySQL rejects DEFAULT clauses on JSON, TEXT, BLOB, and GEOMETRY
// columns (Error 1101). The cross-engine PG → MySQL path is the
// motivating case — a PG source with `jsonb NOT NULL DEFAULT '{}'::jsonb`
// would die at CREATE TABLE on the target. The writer now drops the
// DEFAULT clause; the emit shape stays identical when the column has
// no Default. ir.Array also routes through the suppression because
// emitColumnType maps it to MySQL JSON.
func TestEmitColumnDef_SuppressesDefaultOnForbidddenTypes(t *testing.T) {
	cases := []struct {
		name      string
		colType   ir.Type
		typeIsStr string // expected substring of the emitted type
	}{
		{"json", ir.JSON{Binary: true}, "JSON"},
		{"text regular", ir.Text{Size: ir.TextRegular}, "TEXT"},
		{"text long", ir.Text{Size: ir.TextLong}, "LONGTEXT"},
		{"blob regular", ir.Blob{Size: ir.BlobRegular}, "BLOB"},
		{"blob medium", ir.Blob{Size: ir.BlobMedium}, "MEDIUMBLOB"},
		{"geometry default", ir.Geometry{Subtype: ir.GeometryUnspecified}, "GEOMETRY"},
		{"geometry point", ir.Geometry{Subtype: ir.GeometryPoint}, "POINT"},
		{"array routes to JSON", ir.Array{Element: ir.Integer{Width: 32}}, "JSON"},
		{"hstore extension routes to JSON", ir.ExtensionType{Extension: "hstore", Name: "hstore"}, "JSON"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name+"/with default suppressed", func(t *testing.T) {
			col := &ir.Column{
				Name:    "data",
				Type:    c.colType,
				Default: ir.DefaultExpression{Expr: "'{}'::jsonb"},
			}
			got, err := emitColumnDef(col)
			if err != nil {
				t.Fatalf("emitColumnDef: %v", err)
			}
			if strings.Contains(got, "DEFAULT") {
				t.Errorf("emitColumnDef = %q; want no DEFAULT clause on %s", got, c.typeIsStr)
			}
			if !strings.Contains(got, c.typeIsStr) {
				t.Errorf("emitColumnDef = %q; want substring %q", got, c.typeIsStr)
			}
		})
		t.Run(c.name+"/without default unchanged", func(t *testing.T) {
			col := &ir.Column{Name: "data", Type: c.colType}
			got, err := emitColumnDef(col)
			if err != nil {
				t.Fatalf("emitColumnDef: %v", err)
			}
			if strings.Contains(got, "DEFAULT") {
				t.Errorf("emitColumnDef = %q; should never have DEFAULT (no Default set)", got)
			}
		})
	}
}

// TestEmitColumnDef_PreservesDefaultOnAllowedTypes is the symmetric
// regression-guard: columns whose types DO accept DEFAULT (boolean,
// integer, varchar, timestamp, etc.) still emit the DEFAULT clause
// after the v0.32.2 suppression. Without this pin a too-broad
// helper change could quietly strip DEFAULTs across the board.
func TestEmitColumnDef_PreservesDefaultOnAllowedTypes(t *testing.T) {
	cases := []*ir.Column{
		{Name: "active", Type: ir.Boolean{}, Default: ir.DefaultLiteral{Value: "true"}},
		{Name: "count", Type: ir.Integer{Width: 32}, Default: ir.DefaultLiteral{Value: "0"}},
		{Name: "name", Type: ir.Varchar{Length: 64}, Default: ir.DefaultLiteral{Value: "anon"}},
		{Name: "created_at", Type: ir.Timestamp{Precision: 6, WithTimeZone: true}, Default: ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP(6)"}},
	}
	for _, col := range cases {
		col := col
		t.Run(col.Name, func(t *testing.T) {
			got, err := emitColumnDef(col)
			if err != nil {
				t.Fatalf("emitColumnDef: %v", err)
			}
			if !strings.Contains(got, "DEFAULT") {
				t.Errorf("emitColumnDef = %q; expected DEFAULT clause to be preserved on %T", got, col.Type)
			}
		})
	}
}

func TestEmitColumnDef(t *testing.T) {
	cases := []struct {
		name string
		in   *ir.Column
		want string
	}{
		{
			name: "id bigint unsigned auto_increment not null",
			in: &ir.Column{
				Name: "id",
				Type: ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true},
			},
			want: "`id` BIGINT UNSIGNED AUTO_INCREMENT NOT NULL",
		},
		{
			name: "email varchar not null",
			in: &ir.Column{
				Name: "email",
				Type: ir.Varchar{Length: 255, Charset: "utf8mb4", Collation: "utf8mb4_0900_ai_ci"},
			},
			want: "`email` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL",
		},
		{
			name: "active boolean default 1",
			in: &ir.Column{
				Name:    "active",
				Type:    ir.Boolean{},
				Default: ir.DefaultLiteral{Value: "1"},
			},
			// "1"/"0" on a bool column emit unquoted (catalog #4): the
			// reader maps bit(1) → ir.Boolean and bit-literal defaults
			// to "0"/"1"; the clean MySQL TINYINT(1) form is unquoted.
			want: "`active` TINYINT(1) NOT NULL DEFAULT 1",
		},
		{
			// Regression: a Postgres source hands MySQL a boolean
			// default of "true" (ir.DefaultLiteral). Without the
			// translation step in emitDefault, MySQL sees DEFAULT
			// 'true' and rejects it under strict mode.
			name: "active boolean default true (pg-style)",
			in: &ir.Column{
				Name:    "active",
				Type:    ir.Boolean{},
				Default: ir.DefaultLiteral{Value: "true"},
			},
			want: "`active` TINYINT(1) NOT NULL DEFAULT 1",
		},
		{
			name: "created_at default current_timestamp",
			in: &ir.Column{
				Name:    "created_at",
				Type:    ir.Timestamp{Precision: 6, WithTimeZone: true},
				Default: ir.DefaultExpression{Expr: "CURRENT_TIMESTAMP(6)"},
			},
			want: "`created_at` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)",
		},
		{
			name: "nullable with comment",
			in: &ir.Column{
				Name:     "notes",
				Type:     ir.Text{Size: ir.TextRegular},
				Nullable: true,
				Comment:  "User notes",
			},
			want: "`notes` TEXT COMMENT 'User notes'",
		},
		{
			// Bug 26 PG → MySQL: a PG `geometry(POINT, 4326)` column
			// lands as `POINT NOT NULL SRID 4326` so ST_SRID(loc) on
			// the target returns 4326 instead of dropping to 0.
			name: "geometry point with srid 4326",
			in: &ir.Column{
				Name: "loc",
				Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326},
			},
			want: "`loc` POINT NOT NULL SRID 4326",
		},
		{
			// SRID 0 (no spatial reference declared) is identical to
			// omitting the clause — SRID 0 is MySQL's "no SRS" sentinel.
			// The DDL stays bare so cross-engine pre-Bug-26 schemas
			// don't suddenly grow `SRID 0` text.
			name: "geometry polygon with srid 0",
			in: &ir.Column{
				Name: "boundary",
				Type: ir.Geometry{Subtype: ir.GeometryPolygon, SRID: 0},
			},
			want: "`boundary` POLYGON NOT NULL",
		},
		{
			name: "geometry nullable with srid 3857",
			in: &ir.Column{
				Name:     "shape",
				Type:     ir.Geometry{Subtype: ir.GeometryPolygon, SRID: 3857},
				Nullable: true,
			},
			want: "`shape` POLYGON SRID 3857",
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := emitColumnDef(c.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestEmitColumnDef_Generated covers GENERATED ALWAYS AS (...)
// emission for both STORED and VIRTUAL storage classes. Verbatim-
// passthrough policy: the expression text is preserved as-is, so the
// caller can spot dialect mismatches at apply time rather than
// debug a guessed translation.
func TestEmitColumnDef_Generated(t *testing.T) {
	cases := []struct {
		name string
		in   *ir.Column
		want string
	}{
		{
			name: "stored generated bigint",
			in: &ir.Column{
				Name:            "total",
				Type:            ir.Integer{Width: 64},
				GeneratedExpr:   "qty * price",
				GeneratedStored: true,
			},
			want: "`total` BIGINT GENERATED ALWAYS AS (qty * price) STORED NOT NULL",
		},
		{
			name: "virtual generated varchar",
			in: &ir.Column{
				Name:            "label",
				Type:            ir.Varchar{Length: 64},
				GeneratedExpr:   "CONCAT(first_name, ' ', last_name)",
				GeneratedStored: false,
			},
			want: "`label` VARCHAR(64) GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)) VIRTUAL NOT NULL",
		},
		{
			name: "stored generated nullable",
			in: &ir.Column{
				Name:            "tax",
				Type:            ir.Decimal{Precision: 10, Scale: 2},
				Nullable:        true,
				GeneratedExpr:   "subtotal * 0.07",
				GeneratedStored: true,
			},
			want: "`tax` DECIMAL(10,2) GENERATED ALWAYS AS (subtotal * 0.07) STORED",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := emitColumnDef(c.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestEmitCheckConstraint covers the standalone CHECK fragment used
// inline in CREATE TABLE bodies. Verbatim-passthrough policy: the
// expression text is preserved as-is.
func TestEmitCheckConstraint(t *testing.T) {
	cases := []struct {
		name string
		in   *ir.CheckConstraint
		want string
	}{
		{
			name: "named with comparison",
			in:   &ir.CheckConstraint{Name: "orders_qty_chk", Expr: "qty >= 0"},
			want: "CONSTRAINT `orders_qty_chk` CHECK (qty >= 0)",
		},
		{
			name: "named with IN list",
			in: &ir.CheckConstraint{
				Name: "orders_status_chk",
				Expr: "status IN ('open','closed','cancelled')",
			},
			want: "CONSTRAINT `orders_status_chk` CHECK (status IN ('open','closed','cancelled'))",
		},
		{
			name: "unnamed",
			in:   &ir.CheckConstraint{Expr: "start_date <= end_date"},
			want: "CHECK (start_date <= end_date)",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := emitCheckConstraint(c.in); got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

// TestEmitTableDef_CheckConstraints exercises the inline-emission
// path: CHECK clauses appear after the columns and the primary key,
// each on its own line, with correct comma punctuation. Both cases
// (with and without a primary key) need to land valid DDL.
func TestEmitTableDef_CheckConstraints(t *testing.T) {
	t.Run("with primary key", func(t *testing.T) {
		tbl := &ir.Table{
			Name: "orders",
			Columns: []*ir.Column{
				{Name: "id", Type: ir.Integer{Width: 64}},
				{Name: "qty", Type: ir.Integer{Width: 32}},
				{Name: "status", Type: ir.Varchar{Length: 20}},
			},
			PrimaryKey: &ir.Index{
				Name:    "PRIMARY",
				Unique:  true,
				Columns: []ir.IndexColumn{{Column: "id"}},
			},
			CheckConstraints: []*ir.CheckConstraint{
				{Name: "orders_qty_chk", Expr: "qty >= 0"},
				{Name: "orders_status_chk", Expr: "status IN ('open','closed')"},
			},
		}
		got, err := emitTableDef(tbl)
		if err != nil {
			t.Fatalf("emitTableDef: %v", err)
		}
		wants := []string{
			"PRIMARY KEY (`id`),",
			"CONSTRAINT `orders_qty_chk` CHECK (qty >= 0),",
			"CONSTRAINT `orders_status_chk` CHECK (status IN ('open','closed'))",
		}
		for _, w := range wants {
			if !strings.Contains(got, w) {
				t.Errorf("output missing %q; got:\n%s", w, got)
			}
		}
		// The last constraint line must NOT end with a trailing comma
		// before the closing paren — that would be a MySQL parse error.
		if strings.Contains(got, "))\n)") {
			// A bare-eyeball check: make sure we don't have a stray
			// comma before the closing `)`.
			if strings.Contains(got, "),\n)") {
				t.Errorf("trailing comma before closing paren; got:\n%s", got)
			}
		}
	})

	t.Run("without primary key", func(t *testing.T) {
		tbl := &ir.Table{
			Name: "audit_events",
			Columns: []*ir.Column{
				{Name: "kind", Type: ir.Varchar{Length: 32}},
			},
			CheckConstraints: []*ir.CheckConstraint{
				{Name: "ae_kind_chk", Expr: "kind IN ('a','b')"},
			},
		}
		got, err := emitTableDef(tbl)
		if err != nil {
			t.Fatalf("emitTableDef: %v", err)
		}
		// The column line must end with a trailing comma so the CHECK
		// line that follows it is grammatical.
		if !strings.Contains(got, "`kind` VARCHAR(32) NOT NULL,") {
			t.Errorf("column line missing trailing comma before CHECK; got:\n%s", got)
		}
		if !strings.Contains(got, "CONSTRAINT `ae_kind_chk` CHECK (kind IN ('a','b'))") {
			t.Errorf("CHECK clause missing; got:\n%s", got)
		}
	})
}

func TestEmitTableDef(t *testing.T) {
	table := &ir.Table{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, Unsigned: true, AutoIncrement: true}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
		PrimaryKey: &ir.Index{
			Name:    "PRIMARY",
			Unique:  true,
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
	}
	got, err := emitTableDef(table)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Spot checks rather than exact-string match: the multi-line
	// CREATE TABLE is verbose enough that a structural check reads
	// more clearly than a giant string literal.
	wants := []string{
		"CREATE TABLE IF NOT EXISTS `users` (",
		"`id` BIGINT UNSIGNED AUTO_INCREMENT NOT NULL,",
		"`email` VARCHAR(255) NOT NULL,",
		"PRIMARY KEY (`id`)",
		"ENGINE=InnoDB",
	}
	for _, w := range wants {
		if !strings.Contains(got, w) {
			t.Errorf("output missing %q; got:\n%s", w, got)
		}
	}
}

// TestEmitTableDef_AutoIncrementNonPK_GitHub25 pins the GitHub #25
// fix: a table with AUTO_INCREMENT on a non-PK column, supported by
// a UNIQUE KEY, must emit that UNIQUE KEY inline at CREATE TABLE
// time so MySQL/Vitess doesn't reject with Error 1075 (Incorrect
// table definition; there can be only one auto column and it must
// be defined as a key).
//
// Pre-v0.49.0 emitTableDef deferred ALL secondary indexes to phase 2,
// which made the CREATE land without the auto column's supporting
// key. v0.49.0's inlineAutoIncrementIndex detects this exact pattern
// and threads the supporting index through emitTableDef's body.
func TestEmitTableDef_AutoIncrementNonPK_GitHub25(t *testing.T) {
	table := &ir.Table{
		Name: "cell",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Varchar{Length: 50}},
			{Name: "increment_id", Type: ir.Integer{Width: 32, AutoIncrement: true}},
		},
		PrimaryKey: &ir.Index{
			Name:    "PRIMARY",
			Unique:  true,
			Columns: []ir.IndexColumn{{Column: "id"}},
		},
		Indexes: []*ir.Index{
			{
				Name:    "uq_cell_increment_id",
				Unique:  true,
				Kind:    ir.IndexKindBTree,
				Columns: []ir.IndexColumn{{Column: "increment_id"}},
			},
		},
	}
	got, err := emitTableDef(table)
	if err != nil {
		t.Fatalf("emitTableDef: %v", err)
	}
	wants := []string{
		"CREATE TABLE IF NOT EXISTS `cell` (",
		"`increment_id` INT AUTO_INCREMENT NOT NULL,",
		"PRIMARY KEY (`id`),",
		"UNIQUE KEY `uq_cell_increment_id` (`increment_id`)",
	}
	for _, w := range wants {
		if !strings.Contains(got, w) {
			t.Errorf("output missing %q; got:\n%s", w, got)
		}
	}
}

// TestInlineAutoIncrementIndex_DetectionTable covers the detector
// directly: PK auto column → no inline (existing common case),
// non-PK auto column with supporting unique → inline that index,
// non-PK auto column without supporting → nil (existing behavior;
// MySQL will reject with same error pre/post-fix, distinct surface).
func TestInlineAutoIncrementIndex_DetectionTable(t *testing.T) {
	pkAuto := &ir.Table{
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64, AutoIncrement: true}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes:    []*ir.Index{{Name: "noise", Columns: []ir.IndexColumn{{Column: "other"}}}},
	}
	if got := inlineAutoIncrementIndex(pkAuto); got != nil {
		t.Errorf("PK auto column should return nil; got %+v", got)
	}

	nonPKAutoWithUnique := &ir.Table{
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Varchar{Length: 50}},
			{Name: "seq_id", Type: ir.Integer{Width: 32, AutoIncrement: true}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			{Name: "uq_seq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "seq_id"}}},
		},
	}
	got := inlineAutoIncrementIndex(nonPKAutoWithUnique)
	if got == nil || got.Name != "uq_seq_id" {
		t.Errorf("non-PK auto with supporting unique should return uq_seq_id; got %+v", got)
	}

	nonPKAutoNoSupport := &ir.Table{
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Varchar{Length: 50}},
			{Name: "seq_id", Type: ir.Integer{Width: 32, AutoIncrement: true}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			{Name: "uq_other", Unique: true, Columns: []ir.IndexColumn{{Column: "other"}}},
		},
	}
	if got := inlineAutoIncrementIndex(nonPKAutoNoSupport); got != nil {
		t.Errorf("non-PK auto without supporting index should return nil; got %+v", got)
	}

	// Prefer unique over non-unique when both have the auto col first.
	nonPKAutoBoth := &ir.Table{
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Varchar{Length: 50}},
			{Name: "seq_id", Type: ir.Integer{Width: 32, AutoIncrement: true}},
		},
		PrimaryKey: &ir.Index{Columns: []ir.IndexColumn{{Column: "id"}}},
		Indexes: []*ir.Index{
			{Name: "kx_seq_id", Unique: false, Columns: []ir.IndexColumn{{Column: "seq_id"}}},
			{Name: "uq_seq_id", Unique: true, Columns: []ir.IndexColumn{{Column: "seq_id"}}},
		},
	}
	got = inlineAutoIncrementIndex(nonPKAutoBoth)
	if got == nil || got.Name != "uq_seq_id" {
		t.Errorf("should prefer unique index when both match; got %+v", got)
	}
}

func TestEmitCreateIndex(t *testing.T) {
	cases := []struct {
		name string
		idx  *ir.Index
		want string
	}{
		{
			name: "secondary unique",
			idx: &ir.Index{
				Name:    "users_email_unique",
				Unique:  true,
				Kind:    ir.IndexKindBTree,
				Columns: []ir.IndexColumn{{Column: "email"}},
			},
			want: "ALTER TABLE `users` ADD UNIQUE INDEX `users_email_unique` (`email`) USING BTREE;",
		},
		{
			name: "non-unique multi-column",
			idx: &ir.Index{
				Name: "users_lookup",
				Kind: ir.IndexKindBTree,
				Columns: []ir.IndexColumn{
					{Column: "tenant_id"},
					{Column: "created_at", Desc: true},
				},
			},
			want: "ALTER TABLE `users` ADD INDEX `users_lookup` (`tenant_id`, `created_at` DESC) USING BTREE;",
		},
		{
			name: "fulltext",
			idx: &ir.Index{
				Name:    "posts_body_ft",
				Kind:    ir.IndexKindFullText,
				Columns: []ir.IndexColumn{{Column: "body"}},
			},
			want: "ALTER TABLE `users` ADD FULLTEXT INDEX `posts_body_ft` (`body`);",
		},
		{
			name: "prefix length",
			idx: &ir.Index{
				Name:    "users_name_prefix",
				Kind:    ir.IndexKindBTree,
				Columns: []ir.IndexColumn{{Column: "name", Length: 16}},
			},
			want: "ALTER TABLE `users` ADD INDEX `users_name_prefix` (`name`(16)) USING BTREE;",
		},
		{
			// Bug 16: a functional/expression index entry (MySQL
			// 8.0.13+) renders the expression in parens. Combined with
			// the outer column-list parens this produces MySQL's
			// canonical double-parens form `((LOWER(email)))`.
			name: "expression entry",
			idx: &ir.Index{
				Name:    "idx_lower_email",
				Kind:    ir.IndexKindBTree,
				Columns: []ir.IndexColumn{{Expression: "lower(email)"}},
			},
			want: "ALTER TABLE `users` ADD INDEX `idx_lower_email` ((lower(email))) USING BTREE;",
		},
		{
			// Mixed entries: a plain column followed by an expression.
			// Both forms coexist in a single index in MySQL 8.0.13+.
			name: "mixed plain and expression entries",
			idx: &ir.Index{
				Name: "users_mixed",
				Kind: ir.IndexKindBTree,
				Columns: []ir.IndexColumn{
					{Column: "tenant_id"},
					{Expression: "lower(email)"},
				},
			},
			want: "ALTER TABLE `users` ADD INDEX `users_mixed` (`tenant_id`, (lower(email))) USING BTREE;",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := emitCreateIndex("users", c.idx)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Errorf("\n got  %q\n want %q", got, c.want)
			}
		})
	}
}

func TestEmitCreateIndexRejectsPrimary(t *testing.T) {
	idx := &ir.Index{
		Name:    "PRIMARY",
		Columns: []ir.IndexColumn{{Column: "id"}},
	}
	if _, err := emitCreateIndex("t", idx); err == nil {
		t.Error("expected error for PRIMARY index; got nil")
	}
}

func TestEmitAddForeignKey(t *testing.T) {
	fk := &ir.ForeignKey{
		Name:              "posts_user_id_fk",
		Columns:           []string{"user_id"},
		ReferencedTable:   "users",
		ReferencedColumns: []string{"id"},
		OnDelete:          ir.FKActionCascade,
		OnUpdate:          ir.FKActionRestrict,
	}
	got, err := emitAddForeignKey("posts", fk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "ALTER TABLE `posts` ADD CONSTRAINT `posts_user_id_fk` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE ON UPDATE RESTRICT;"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

func TestEmitAddForeignKey_NoActions(t *testing.T) {
	// FKs with NoAction (the MySQL default) shouldn't emit ON DELETE
	// / ON UPDATE clauses — they'd be redundant noise.
	fk := &ir.ForeignKey{
		Name:              "fk",
		Columns:           []string{"a"},
		ReferencedTable:   "parent",
		ReferencedColumns: []string{"id"},
		OnDelete:          ir.FKActionNoAction,
		OnUpdate:          ir.FKActionNoAction,
	}
	got, err := emitAddForeignKey("child", fk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(got, "ON DELETE") || strings.Contains(got, "ON UPDATE") {
		t.Errorf("output should not contain ON DELETE/ON UPDATE for NoAction; got %q", got)
	}
}

// TestEmitAddForeignKey_SelfReferential — same shape as the PG sibling.
// Pinned per design-schema-completeness.md so self-ref FK support
// can't regress silently.
func TestEmitAddForeignKey_SelfReferential(t *testing.T) {
	fk := &ir.ForeignKey{
		Name:              "employees_manager_fk",
		Columns:           []string{"manager_id"},
		ReferencedTable:   "employees",
		ReferencedColumns: []string{"id"},
		OnDelete:          ir.FKActionSetNull,
	}
	got, err := emitAddForeignKey("employees", fk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "ALTER TABLE `employees` ADD CONSTRAINT `employees_manager_fk` FOREIGN KEY (`manager_id`) REFERENCES `employees` (`id`) ON DELETE SET NULL;"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestEmitAddForeignKey_CompositePK — composite-key FK shape (real-
// world tenant-scoped models with `(tenant_id, id)` PKs).
func TestEmitAddForeignKey_CompositePK(t *testing.T) {
	fk := &ir.ForeignKey{
		Name:              "orders_customer_fk",
		Columns:           []string{"tenant_id", "customer_id"},
		ReferencedTable:   "customers",
		ReferencedColumns: []string{"tenant_id", "id"},
		OnDelete:          ir.FKActionCascade,
		OnUpdate:          ir.FKActionCascade,
	}
	got, err := emitAddForeignKey("orders", fk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "ALTER TABLE `orders` ADD CONSTRAINT `orders_customer_fk` FOREIGN KEY (`tenant_id`, `customer_id`) REFERENCES `customers` (`tenant_id`, `id`) ON DELETE CASCADE ON UPDATE CASCADE;"
	if got != want {
		t.Errorf("\n got  %q\n want %q", got, want)
	}
}

// TestEmitAddForeignKey_AllOnDeleteActions pins every supported
// FKAction's MySQL keyword. Same shape as the PG sibling. A
// regression that swapped two of them would silently change the
// cascade behavior on the target.
func TestEmitAddForeignKey_AllOnDeleteActions(t *testing.T) {
	cases := []struct {
		action ir.FKAction
		want   string
	}{
		{ir.FKActionRestrict, "RESTRICT"},
		{ir.FKActionCascade, "CASCADE"},
		{ir.FKActionSetNull, "SET NULL"},
		{ir.FKActionSetDefault, "SET DEFAULT"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.action.String(), func(t *testing.T) {
			fk := &ir.ForeignKey{
				Name:              "fk_test",
				Columns:           []string{"x"},
				ReferencedTable:   "parent",
				ReferencedColumns: []string{"id"},
				OnDelete:          c.action,
			}
			got, err := emitAddForeignKey("child", fk)
			if err != nil {
				t.Fatalf("emitAddForeignKey: %v", err)
			}
			if !strings.Contains(got, "ON DELETE "+c.want) {
				t.Errorf("expected ON DELETE %s; got %q", c.want, got)
			}
		})
	}
}

func TestQuoteSQLString(t *testing.T) {
	cases := []struct{ in, want string }{
		{"hello", "'hello'"},
		{"it's", "'it''s'"},
		{"", "''"},
		{"two''quotes", "'two''''quotes'"},
	}
	for _, c := range cases {
		if got := quoteSQLString(c.in); got != c.want {
			t.Errorf("quoteSQLString(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}
