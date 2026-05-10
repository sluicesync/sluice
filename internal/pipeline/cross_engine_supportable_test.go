// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

func TestCheckCrossEngineSupportable_SameEngineNil(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "places",
		Columns: []*ir.Column{
			{Name: "loc", Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326}},
		},
	}}}
	if err := checkCrossEngineSupportable(s, "postgres", "postgres", "test"); err != nil {
		t.Errorf("same-engine err = %v; want nil", err)
	}
}

func TestCheckCrossEngineSupportable_PGtoMySQL_PostGISRefuses(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "places",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "loc", Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326}},
		},
	}}}
	err := checkCrossEngineSupportable(s, "postgres", "mysql", "incremental incr-0001")
	if err == nil {
		t.Fatal("err = nil; want PostGIS refusal")
	}
	if !strings.Contains(err.Error(), "PostGIS") {
		t.Errorf("err = %v; want mention of PostGIS", err)
	}
	if !strings.Contains(err.Error(), "places") {
		t.Errorf("err = %v; want mention of table 'places'", err)
	}
	if !strings.Contains(err.Error(), "loc") {
		t.Errorf("err = %v; want mention of column 'loc'", err)
	}
	if !strings.Contains(err.Error(), "--exclude-table") {
		t.Errorf("err = %v; want hint mentioning --exclude-table", err)
	}
}

func TestCheckCrossEngineSupportable_PGtoMySQL_PortableTypesOK(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "uid", Type: ir.UUID{}},
			{Name: "ip", Type: ir.Inet{}},
			{Name: "tags", Type: ir.Array{Element: ir.Text{}}},
			{Name: "data", Type: ir.JSON{Binary: true}},
		},
	}}}
	if err := checkCrossEngineSupportable(s, "postgres", "mysql", "test"); err != nil {
		t.Errorf("portable types err = %v; want nil", err)
	}
}

func TestCheckCrossEngineSupportable_UnknownPairOK(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "places",
		Columns: []*ir.Column{
			{Name: "loc", Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326}},
		},
	}}}
	// future-engine pairs fall through as "supportable" — the engine
	// emitter handles its own error.
	if err := checkCrossEngineSupportable(s, "mysql", "future", "test"); err != nil {
		t.Errorf("unknown pair err = %v; want nil", err)
	}
}

func TestCheckCrossEngineDeltaSupportable_AddTableWithPostGIS(t *testing.T) {
	deltas := []*ir.SchemaDeltaEntry{
		{
			Kind:  ir.SchemaDeltaAddTable,
			Table: "places",
			After: &ir.Table{
				Name: "places",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
					{Name: "loc", Type: ir.Geometry{Subtype: ir.GeometryPolygon}},
				},
			},
		},
	}
	err := checkCrossEngineDeltaSupportable(deltas, "postgres", "mysql", "incr-0001")
	if err == nil {
		t.Fatal("err = nil; want PostGIS refusal")
	}
	if !strings.Contains(err.Error(), "incr-0001") {
		t.Errorf("err = %v; want backup-id 'incr-0001'", err)
	}
}

func TestCheckCrossEngineDeltaSupportable_AlterTableAddPostGIS(t *testing.T) {
	deltas := []*ir.SchemaDeltaEntry{
		{
			Kind:  ir.SchemaDeltaAlterTable,
			Table: "places",
			Before: &ir.Table{
				Name: "places",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
				},
			},
			After: &ir.Table{
				Name: "places",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
					{Name: "loc", Type: ir.Geometry{Subtype: ir.GeometryPoint}},
				},
			},
		},
	}
	err := checkCrossEngineDeltaSupportable(deltas, "postgres", "mysql", "incr-0002")
	if err == nil {
		t.Fatal("err = nil; want PostGIS refusal")
	}
}

func TestCheckCrossEngineDeltaSupportable_DropTableNoCheck(t *testing.T) {
	// DropTable carries no After-shape — skipped.
	deltas := []*ir.SchemaDeltaEntry{
		{Kind: ir.SchemaDeltaDropTable, Table: "old"},
	}
	if err := checkCrossEngineDeltaSupportable(deltas, "postgres", "mysql", "incr-0001"); err != nil {
		t.Errorf("err = %v; want nil for drop-only delta", err)
	}
}

// TestCheckCrossEngineSupportable_PGtoMySQL_ExtensionTypeRefuses
// exercises the ADR-0032 cross-engine refusal: PG → MySQL with an
// ExtensionType column (e.g. pgvector) keeps the loud-failure
// default even when the operator opted into --enable-pg-extension
// on the source side.
func TestCheckCrossEngineSupportable_PGtoMySQL_ExtensionTypeRefuses(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "items",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "embedding", Type: ir.ExtensionType{
				Extension: "vector",
				Name:      "vector",
				Modifiers: []int{384},
			}},
		},
	}}}
	err := checkCrossEngineSupportable(s, "postgres", "mysql", "items-migration")
	if err == nil {
		t.Fatal("err = nil; want ExtensionType refusal")
	}
	if !strings.Contains(err.Error(), "vector.vector") {
		t.Errorf("err = %v; want mention of \"vector.vector\"", err)
	}
	if !strings.Contains(err.Error(), "embedding") {
		t.Errorf("err = %v; want mention of column 'embedding'", err)
	}
	if !strings.Contains(err.Error(), "--type-override") {
		t.Errorf("err = %v; want hint mentioning --type-override", err)
	}
}
