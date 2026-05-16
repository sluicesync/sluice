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

// TestCheckCrossEngineSupportable_PGtoMySQL_PostGISAllowed asserts the
// post-ADR-0035 behaviour: PG → MySQL geometry no longer refuses. The
// IR carries Subtype + SRID; the MySQL writer emits the matching
// spatial type with `SRID <n>` so ST_SRID round-trips on the target.
// Pre-v0.28.0 this case raised a "PostGIS geometry type" refusal — kept
// as a regression guard against accidental re-introduction of the
// blanket refusal.
func TestCheckCrossEngineSupportable_PGtoMySQL_PostGISAllowed(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "places",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "loc", Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326}},
		},
	}}}
	if err := checkCrossEngineSupportable(s, "postgres", "mysql", "incremental incr-0001"); err != nil {
		t.Errorf("err = %v; want nil (geometry now allowed PG → MySQL)", err)
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

// TestCheckCrossEngineDeltaSupportable_AddTableWithPostGISAllowed
// asserts the post-ADR-0035 behaviour: an incremental that adds a
// table with a geometry column no longer refuses PG → MySQL.
func TestCheckCrossEngineDeltaSupportable_AddTableWithPostGISAllowed(t *testing.T) {
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
	if err := checkCrossEngineDeltaSupportable(deltas, "postgres", "mysql", "incr-0001"); err != nil {
		t.Errorf("err = %v; want nil (geometry now allowed PG → MySQL)", err)
	}
}

// TestCheckCrossEngineDeltaSupportable_AlterTableAddPostGISAllowed
// asserts the post-ADR-0035 behaviour for ALTER TABLE deltas.
func TestCheckCrossEngineDeltaSupportable_AlterTableAddPostGISAllowed(t *testing.T) {
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
	if err := checkCrossEngineDeltaSupportable(deltas, "postgres", "mysql", "incr-0002"); err != nil {
		t.Errorf("err = %v; want nil (geometry now allowed PG → MySQL)", err)
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

// TestCheckCrossEngineSupportable_PGtoMySQL_VerbatimTypeRefuses pins
// the ADR-0047 cross-engine guard: an uncatalogued verbatim PG
// extension type has no portable MySQL form and MUST refuse loudly —
// the cross-engine default is not weakened by the same-engine verbatim
// tier. Mirrors the ExtensionType refusal.
func TestCheckCrossEngineSupportable_PGtoMySQL_VerbatimTypeRefuses(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "docs",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "path", Type: ir.VerbatimType{Definition: "ltree"}},
		},
	}}}
	err := checkCrossEngineSupportable(s, "postgres", "mysql", "docs-migration")
	if err == nil {
		t.Fatal("err = nil; want VerbatimType cross-engine refusal")
	}
	if !strings.Contains(err.Error(), "ltree") {
		t.Errorf("err = %v; want mention of the verbatim definition \"ltree\"", err)
	}
	if !strings.Contains(err.Error(), "path") {
		t.Errorf("err = %v; want mention of column 'path'", err)
	}
	if !strings.Contains(err.Error(), "ADR-0047") {
		t.Errorf("err = %v; want ADR-0047 reference", err)
	}
}

// TestCheckCrossEngineSupportable_PGtoPG_VerbatimTypeOK confirms the
// same-engine path is unaffected (the verbatim tier's whole point).
func TestCheckCrossEngineSupportable_PGtoPG_VerbatimTypeOK(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name:    "docs",
		Columns: []*ir.Column{{Name: "path", Type: ir.VerbatimType{Definition: "ltree"}}},
	}}}
	if err := checkCrossEngineSupportable(s, "postgres", "postgres", "x"); err != nil {
		t.Errorf("same-engine PG → PG with VerbatimType should be OK; got %v", err)
	}
}

// TestCheckCrossEngineSupportable_PGtoMySQL_TrgmIndexRefuses exercises
// the ADR-0032 Tier 2 lite cross-engine refusal: pg_trgm has no new
// column type (the column is plain `text`), so the column-level
// refusal can't fire — the index-level refusal closes the gap. PG →
// MySQL with a `gin (col gin_trgm_ops)` index keeps the loud-failure
// default; operators wanting MySQL fuzzy search must drop the index
// or supply a workload-specific override.
func TestCheckCrossEngineSupportable_PGtoMySQL_TrgmIndexRefuses(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "documents",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "body", Type: ir.Text{}},
		},
		Indexes: []*ir.Index{
			{
				Name: "documents_body_trgm",
				Kind: ir.IndexKindGIN,
				Columns: []ir.IndexColumn{
					{Column: "body", OperatorClass: "gin_trgm_ops"},
				},
			},
		},
	}}}
	err := checkCrossEngineSupportable(s, "postgres", "mysql", "documents-migration")
	if err == nil {
		t.Fatal("err = nil; want pg_trgm index refusal")
	}
	if !strings.Contains(err.Error(), "gin_trgm_ops") {
		t.Errorf("err = %v; want mention of \"gin_trgm_ops\"", err)
	}
	if !strings.Contains(err.Error(), "documents_body_trgm") {
		t.Errorf("err = %v; want mention of index 'documents_body_trgm'", err)
	}
	if !strings.Contains(err.Error(), "documents") {
		t.Errorf("err = %v; want mention of table 'documents'", err)
	}
}

// TestCheckCrossEngineSupportable_PGtoMySQL_TrgmGistIndexRefuses pins
// the same refusal for the GiST flavour (`gist_trgm_ops`). pg_trgm
// supports both GIN and GiST; sluice refuses both cross-engine.
func TestCheckCrossEngineSupportable_PGtoMySQL_TrgmGistIndexRefuses(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "documents",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "body", Type: ir.Text{}},
		},
		Indexes: []*ir.Index{
			{
				Name: "documents_body_trgm_gist",
				Kind: ir.IndexKindGIST,
				Columns: []ir.IndexColumn{
					{Column: "body", OperatorClass: "gist_trgm_ops"},
				},
			},
		},
	}}}
	err := checkCrossEngineSupportable(s, "postgres", "mysql", "documents-migration")
	if err == nil {
		t.Fatal("err = nil; want pg_trgm gist index refusal")
	}
	if !strings.Contains(err.Error(), "gist_trgm_ops") {
		t.Errorf("err = %v; want mention of \"gist_trgm_ops\"", err)
	}
}

// TestCheckCrossEngineSupportable_SameEngineTrgmAllowed pins same-
// engine round-trips: a pg_trgm index PG → PG must NOT be refused
// (the refusal is cross-engine-only).
func TestCheckCrossEngineSupportable_SameEngineTrgmAllowed(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "documents",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "body", Type: ir.Text{}},
		},
		Indexes: []*ir.Index{
			{
				Name: "documents_body_trgm",
				Kind: ir.IndexKindGIN,
				Columns: []ir.IndexColumn{
					{Column: "body", OperatorClass: "gin_trgm_ops"},
				},
			},
		},
	}}}
	if err := checkCrossEngineSupportable(s, "postgres", "postgres", "documents-pg-pg"); err != nil {
		t.Errorf("same-engine err = %v; want nil (pg_trgm passthrough on PG → PG)", err)
	}
}

// TestCheckCrossEngineSupportable_PGtoMySQL_GinKindRefuses_NoOpclass
// pins the v0.30.1 broadening: when the operator runs cross-engine
// PG → MySQL without --enable-pg-extension pg_trgm, the schema reader
// strips OperatorClass from IR (loud-failure default) but `idx.Kind`
// stays `IndexKindGIN`. Pre-v0.30.1 the opclass-only refusal missed
// this and the operator got MySQL Error 1170 after bulk-copy. The
// refusal now catches the Kind directly. (idx.Method stays "" for
// core-PG gin/gist; only extension-introduced AMs like ivfflat/hnsw
// populate it.)
func TestCheckCrossEngineSupportable_PGtoMySQL_GinKindRefuses_NoOpclass(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "documents",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "body", Type: ir.Text{}},
		},
		Indexes: []*ir.Index{
			{
				Name: "documents_body_trgm",
				Kind: ir.IndexKindGIN,
				// No OperatorClass — the schema-reader gate stripped it
				// (operator omitted --enable-pg-extension pg_trgm).
				// idx.Method is "" — core gin AM isn't extension-owned.
				Columns: []ir.IndexColumn{
					{Column: "body"},
				},
			},
		},
	}}}
	err := checkCrossEngineSupportable(s, "postgres", "mysql", "documents-pg-mysql")
	if err == nil {
		t.Fatal("err = nil; want refusal for PG GIN index with no MySQL counterpart")
	}
	if !strings.Contains(err.Error(), "GIN") {
		t.Errorf("err = %v; want mention of \"GIN\"", err)
	}
	if !strings.Contains(err.Error(), "MySQL") {
		t.Errorf("err = %v; want mention of MySQL counterpart", err)
	}
}

// TestCheckCrossEngineSupportable_PGtoMySQL_GistKindRefuses_NoOpclass
// mirrors the GIN test for GiST (the second pg_trgm AM, also the
// PostGIS spatial AM though spatial gets auto-emitted).
func TestCheckCrossEngineSupportable_PGtoMySQL_GistKindRefuses_NoOpclass(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "documents",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "body", Type: ir.Text{}},
		},
		Indexes: []*ir.Index{
			{
				Name: "documents_body_trgm_gist",
				Kind: ir.IndexKindGIST,
				Columns: []ir.IndexColumn{
					{Column: "body"},
				},
			},
		},
	}}}
	err := checkCrossEngineSupportable(s, "postgres", "mysql", "documents-pg-mysql")
	if err == nil {
		t.Fatal("err = nil; want refusal for PG GiST index with no MySQL counterpart")
	}
	if !strings.Contains(err.Error(), "GiST") {
		t.Errorf("err = %v; want mention of \"GiST\"", err)
	}
}

// TestCheckCrossEngineSupportable_PGtoMySQL_SPGistKindRefuses pins
// the v0.33.1 broadening (Bug 50): the SP-GiST / BRIN index kinds
// joined IndexKindGIN / IndexKindGIST as PG-only access methods with
// no MySQL counterpart. The refusal now catches them too so the
// operator gets a clear refusal instead of a downstream CREATE
// INDEX failure on the MySQL target.
func TestCheckCrossEngineSupportable_PGtoMySQL_SPGistKindRefuses(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "spatial",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "geom", Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326}},
		},
		Indexes: []*ir.Index{
			{
				Name:    "spatial_geom_spgist",
				Kind:    ir.IndexKindSPGist,
				Columns: []ir.IndexColumn{{Column: "geom"}},
			},
		},
	}}}
	err := checkCrossEngineSupportable(s, "postgres", "mysql", "spatial-pg-mysql")
	if err == nil {
		t.Fatal("err = nil; want refusal for PG SP-GiST index with no MySQL counterpart")
	}
	if !strings.Contains(err.Error(), "SP-GiST") {
		t.Errorf("err = %v; want mention of \"SP-GiST\"", err)
	}
}

// TestCheckCrossEngineSupportable_PGtoMySQL_BRINKindRefuses — see
// SP-GiST counterpart above.
func TestCheckCrossEngineSupportable_PGtoMySQL_BRINKindRefuses(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "spatial",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "geom", Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326}},
		},
		Indexes: []*ir.Index{
			{
				Name:    "spatial_geom_brin",
				Kind:    ir.IndexKindBRIN,
				Columns: []ir.IndexColumn{{Column: "geom"}},
			},
		},
	}}}
	err := checkCrossEngineSupportable(s, "postgres", "mysql", "spatial-pg-mysql")
	if err == nil {
		t.Fatal("err = nil; want refusal for PG BRIN index with no MySQL counterpart")
	}
	if !strings.Contains(err.Error(), "BRIN") {
		t.Errorf("err = %v; want mention of \"BRIN\"", err)
	}
}

// TestCheckCrossEngineSupportable_PGtoMySQL_HstoreSupportable pins
// the hstore carve-out from the ExtensionType refusal: hstore has a
// default MySQL translator (→ JSON) declared in the catalog's
// `crossEngineDefaultTranslatedExtensions`, so the column passes
// through cleanly. Mirrors the postgis carve-out shape — same intent,
// different translator surface.
func TestCheckCrossEngineSupportable_PGtoMySQL_HstoreSupportable(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "attrs",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "tags", Type: ir.ExtensionType{Extension: "hstore", Name: "hstore"}},
		},
	}}}
	if err := checkCrossEngineSupportable(s, "postgres", "mysql", "attrs-migration"); err != nil {
		t.Errorf("hstore PG → MySQL err = %v; want nil (default translator → JSON)", err)
	}
}

// TestCheckCrossEngineSupportable_PGtoMySQL_CiTextSupportable pins
// the citext carve-out — translator maps to VARCHAR with case-
// insensitive collation. The preflight must not refuse.
func TestCheckCrossEngineSupportable_PGtoMySQL_CiTextSupportable(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.ExtensionType{Extension: "citext", Name: "citext"}},
		},
	}}}
	if err := checkCrossEngineSupportable(s, "postgres", "mysql", "users-migration"); err != nil {
		t.Errorf("citext PG → MySQL err = %v; want nil (default translator → VARCHAR _ci)", err)
	}
}

// TestIsCrossEngineTranslatablePGExtension pins the small static set
// the pipeline package mirrors against the postgres engine's catalog
// declaration. Both lists must stay in lock-step.
func TestIsCrossEngineTranslatablePGExtension(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"hstore", true},
		{"citext", true},
		{"vector", false},
		{"pg_trgm", false},
		{"postgis", false},
		{"unknown", false},
		{"", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := isCrossEngineTranslatablePGExtension(c.name)
			if got != c.want {
				t.Errorf("isCrossEngineTranslatablePGExtension(%q) = %v; want %v",
					c.name, got, c.want)
			}
		})
	}
}

// TestCheckCrossEngineSupportable_PGtoMySQL_BtreePassesThrough confirms
// the new Kind-based gate is narrow: btree indexes (the PG default for
// every non-extension index) must NOT be refused.
func TestCheckCrossEngineSupportable_PGtoMySQL_BtreePassesThrough(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "users",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "email", Type: ir.Varchar{Length: 255}},
		},
		Indexes: []*ir.Index{
			{
				Name: "users_email_idx",
				Kind: ir.IndexKindBTree,
				Columns: []ir.IndexColumn{
					{Column: "email"},
				},
			},
		},
	}}}
	if err := checkCrossEngineSupportable(s, "postgres", "mysql", "users-pg-mysql"); err != nil {
		t.Errorf("err = %v; want nil (btree is MySQL-portable)", err)
	}
}
