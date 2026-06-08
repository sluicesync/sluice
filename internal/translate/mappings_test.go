// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"reflect"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/config"
	"sluicesync.dev/sluice/internal/ir"
)

// schemaWith returns a small fixture schema with the named columns,
// each typed as Varchar(255). Tests rewrite specific columns and
// assert the rest are unaffected.
func schemaWith(tableSpecs ...struct {
	table string
	cols  []string
},
) *ir.Schema {
	out := &ir.Schema{}
	for _, s := range tableSpecs {
		t := &ir.Table{Name: s.table}
		for _, c := range s.cols {
			t.Columns = append(t.Columns, &ir.Column{
				Name: c,
				Type: ir.Varchar{Length: 255},
			})
		}
		out.Tables = append(out.Tables, t)
	}
	return out
}

func TestApplyMappings_NoMappings_ReturnsSameSchema(t *testing.T) {
	s := schemaWith(struct {
		table string
		cols  []string
	}{"users", []string{"id", "email"}})

	got, err := ApplyMappings(s, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != s {
		t.Errorf("expected unchanged pointer when mappings is empty; got new schema")
	}
}

func TestApplyMappings_NilSchema_Errors(t *testing.T) {
	_, err := ApplyMappings(nil, []config.Mapping{{Table: "x", Column: "y", TargetType: "text"}})
	if err == nil {
		t.Fatal("expected error for nil schema")
	}
}

func TestApplyMappings_BasicOverride(t *testing.T) {
	s := schemaWith(struct {
		table string
		cols  []string
	}{"orders", []string{"id", "status"}})

	got, err := ApplyMappings(s, []config.Mapping{
		{Table: "orders", Column: "status", TargetType: "text"},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if got == s {
		t.Errorf("expected new schema; got source pointer")
	}
	if got.Tables[0] == s.Tables[0] {
		t.Errorf("expected new orders table; got source pointer")
	}

	statusCol := got.Tables[0].Columns[1]
	if _, ok := statusCol.Type.(ir.Text); !ok {
		t.Errorf("status type = %T; want ir.Text", statusCol.Type)
	}
	// Pre-override source type is captured on the overridden column
	// so writers can disambiguate ambiguous value shapes (Bug 47).
	if _, ok := statusCol.SourceColumnType.(ir.Varchar); !ok {
		t.Errorf("status SourceColumnType = %T; want ir.Varchar (the pre-override type)", statusCol.SourceColumnType)
	}
	// id should still be Varchar — only status was mapped.
	if _, ok := got.Tables[0].Columns[0].Type.(ir.Varchar); !ok {
		t.Errorf("id type = %T; want unchanged ir.Varchar", got.Tables[0].Columns[0].Type)
	}
	// id pointer should NOT have changed (untouched columns share).
	if got.Tables[0].Columns[0] != s.Tables[0].Columns[0] {
		t.Errorf("expected unchanged id column pointer for unmodified column")
	}
	// id has no override, so SourceColumnType stays nil — the field
	// is override-context-only.
	if got.Tables[0].Columns[0].SourceColumnType != nil {
		t.Errorf("id SourceColumnType = %v; want nil (no override)", got.Tables[0].Columns[0].SourceColumnType)
	}
}

func TestApplyMappings_UnaffectedTablesSharePointer(t *testing.T) {
	s := schemaWith(
		struct {
			table string
			cols  []string
		}{"orders", []string{"id"}},
		struct {
			table string
			cols  []string
		}{"users", []string{"email"}},
	)

	got, err := ApplyMappings(s, []config.Mapping{
		{Table: "orders", Column: "id", TargetType: "bytea"},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got.Tables[1] != s.Tables[1] {
		t.Errorf("expected users (unaffected) to share pointer")
	}
}

func TestApplyMappings_RegistryAliases(t *testing.T) {
	cases := []struct {
		alias string
		want  ir.Type
	}{
		{"text", ir.Text{Size: ir.TextLong}},
		{"text_array", ir.Array{Element: ir.Text{Size: ir.TextLong}}},
		{"jsonb", ir.JSON{Binary: true}},
		{"json", ir.JSON{Binary: false}},
		{"bytea", ir.Blob{Size: ir.BlobLong}},
		{"timestamptz", ir.Timestamp{Precision: 6, WithTimeZone: true}},
		// datetime: the out-of-range-timestamp escape hatch (→ MySQL
		// DATETIME(6), range 1000–9999) for PG timestamptz outside MySQL
		// TIMESTAMP's 1970–2038 window.
		{"datetime", ir.DateTime{Precision: 6}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.alias, func(t *testing.T) {
			s := schemaWith(struct {
				table string
				cols  []string
			}{"t", []string{"col"}})
			got, err := ApplyMappings(s, []config.Mapping{
				{Table: "t", Column: "col", TargetType: c.alias},
			})
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			gotTy := got.Tables[0].Columns[0].Type
			if !reflect.DeepEqual(gotTy, c.want) {
				t.Errorf("got %#v; want %#v", gotTy, c.want)
			}
		})
	}
}

func TestApplyMappings_VarcharOptions(t *testing.T) {
	s := schemaWith(struct {
		table string
		cols  []string
	}{"users", []string{"username"}})

	got, err := ApplyMappings(s, []config.Mapping{
		{
			Table: "users", Column: "username", TargetType: "varchar",
			TargetTypeOptions: map[string]any{"length": 64},
		},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	v, ok := got.Tables[0].Columns[0].Type.(ir.Varchar)
	if !ok {
		t.Fatalf("type = %T; want ir.Varchar", got.Tables[0].Columns[0].Type)
	}
	if v.Length != 64 {
		t.Errorf("length = %d; want 64", v.Length)
	}
}

func TestApplyMappings_VarcharDefaultsTo255(t *testing.T) {
	s := schemaWith(struct {
		table string
		cols  []string
	}{"users", []string{"username"}})

	got, err := ApplyMappings(s, []config.Mapping{
		{Table: "users", Column: "username", TargetType: "varchar"},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	v, _ := got.Tables[0].Columns[0].Type.(ir.Varchar)
	if v.Length != 255 {
		t.Errorf("default length = %d; want 255", v.Length)
	}
}

func TestApplyMappings_PostgisAliases(t *testing.T) {
	cases := []struct {
		alias    string
		opts     map[string]any
		wantSub  ir.GeometrySubtype
		wantSRID int
	}{
		{"postgis_point", nil, ir.GeometryPoint, 0},
		{"postgis_point", map[string]any{"srid": 4326}, ir.GeometryPoint, 4326},
		{"postgis_polygon", map[string]any{"srid": int64(3857)}, ir.GeometryPolygon, 3857},
		{"postgis_linestring", map[string]any{"srid": float64(1234)}, ir.GeometryLineString, 1234},
		{"postgis_multipoint", nil, ir.GeometryMultiPoint, 0},
		{"postgis_multilinestring", nil, ir.GeometryMultiLineString, 0},
		{"postgis_multipolygon", nil, ir.GeometryMultiPolygon, 0},
		{"postgis_geometrycollection", nil, ir.GeometryCollection, 0},
		{"postgis_geometry", nil, ir.GeometryUnspecified, 0},
	}
	for _, c := range cases {
		c := c
		t.Run(c.alias, func(t *testing.T) {
			s := schemaWith(struct {
				table string
				cols  []string
			}{"t", []string{"col"}})
			got, err := ApplyMappings(s, []config.Mapping{
				{Table: "t", Column: "col", TargetType: c.alias, TargetTypeOptions: c.opts},
			})
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			geom, ok := got.Tables[0].Columns[0].Type.(ir.Geometry)
			if !ok {
				t.Fatalf("type = %T; want ir.Geometry", got.Tables[0].Columns[0].Type)
			}
			if geom.Subtype != c.wantSub {
				t.Errorf("subtype = %v; want %v", geom.Subtype, c.wantSub)
			}
			if geom.SRID != c.wantSRID {
				t.Errorf("srid = %d; want %d", geom.SRID, c.wantSRID)
			}
		})
	}
}

func TestApplyMappings_PostgisInvalidSRID(t *testing.T) {
	s := schemaWith(struct {
		table string
		cols  []string
	}{"t", []string{"c"}})
	_, err := ApplyMappings(s, []config.Mapping{
		{
			Table: "t", Column: "c", TargetType: "postgis_point",
			TargetTypeOptions: map[string]any{"srid": "not-a-number"},
		},
	})
	if err == nil {
		t.Fatal("expected error for non-integer srid")
	}
	if !strings.Contains(err.Error(), "srid") {
		t.Errorf("err = %q; want substring 'srid'", err.Error())
	}
}

func TestApplyMappings_VarcharLengthFloat(t *testing.T) {
	// koanf decodes plain numbers as float64 in some sources;
	// resolveTargetType handles that.
	s := schemaWith(struct {
		table string
		cols  []string
	}{"t", []string{"c"}})
	got, err := ApplyMappings(s, []config.Mapping{
		{
			Table: "t", Column: "c", TargetType: "varchar",
			TargetTypeOptions: map[string]any{"length": float64(128)},
		},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got.Tables[0].Columns[0].Type.(ir.Varchar).Length != 128 {
		t.Errorf("length = %d; want 128", got.Tables[0].Columns[0].Type.(ir.Varchar).Length)
	}
}

func TestApplyMappings_Errors(t *testing.T) {
	s := schemaWith(struct {
		table string
		cols  []string
	}{"users", []string{"id", "email"}})

	cases := []struct {
		name    string
		m       []config.Mapping
		wantSub string
	}{
		{
			"unknown target_type",
			[]config.Mapping{{Table: "users", Column: "id", TargetType: "snowflake"}},
			`unknown target_type "snowflake"`,
		},
		{
			"missing table",
			[]config.Mapping{{Table: "orders", Column: "id", TargetType: "text"}},
			`unknown table "orders"`,
		},
		{
			"missing column",
			[]config.Mapping{{Table: "users", Column: "ghost", TargetType: "text"}},
			`unknown column users.ghost`,
		},
		{
			"empty table",
			[]config.Mapping{{Column: "id", TargetType: "text"}},
			"table is required",
		},
		{
			"empty column",
			[]config.Mapping{{Table: "users", TargetType: "text"}},
			"column is required",
		},
		{
			"empty target_type",
			[]config.Mapping{{Table: "users", Column: "id"}},
			"target_type is required",
		},
		{
			"duplicate (table, column)",
			[]config.Mapping{
				{Table: "users", Column: "id", TargetType: "text"},
				{Table: "users", Column: "id", TargetType: "bytea"},
			},
			"duplicate override",
		},
		{
			"varchar length non-integer",
			[]config.Mapping{{
				Table: "users", Column: "id", TargetType: "varchar",
				TargetTypeOptions: map[string]any{"length": "lots"},
			}},
			"length` must be an integer",
		},
		{
			"varchar length zero",
			[]config.Mapping{{
				Table: "users", Column: "id", TargetType: "varchar",
				TargetTypeOptions: map[string]any{"length": 0},
			}},
			"must be positive",
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			_, err := ApplyMappings(s, c.m)
			if err == nil {
				t.Fatalf("expected error containing %q; got nil", c.wantSub)
			}
			if !strings.Contains(err.Error(), c.wantSub) {
				t.Errorf("err = %q; want substring %q", err.Error(), c.wantSub)
			}
		})
	}
}

func TestApplyMappings_DoesNotMutateSource(t *testing.T) {
	s := schemaWith(struct {
		table string
		cols  []string
	}{"orders", []string{"status"}})
	origType := s.Tables[0].Columns[0].Type

	_, err := ApplyMappings(s, []config.Mapping{
		{Table: "orders", Column: "status", TargetType: "text"},
	})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !reflect.DeepEqual(s.Tables[0].Columns[0].Type, origType) {
		t.Errorf("source schema mutated: column type = %#v; want %#v",
			s.Tables[0].Columns[0].Type, origType)
	}
}
