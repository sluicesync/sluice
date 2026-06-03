// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pipeline

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
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

// ADR-0053: PG → MySQL with an EXCLUDE constraint must refuse loudly.
// MySQL has no equivalent type or semantics; pre-ADR sluice silently
// dropped the constraint from the IR (the reader never queried
// contype='x'), so a cross-engine restore landed tables missing the
// source's semantic invariant. The refusal stops that silent loss.
func TestCheckCrossEngineSupportable_PGtoMySQL_ExcludeRefuses(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "ci_partitions",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
		},
		ExcludeConstraints: []*ir.ExcludeConstraint{{
			Name:       "check_ci_partitions_builds_id_range_no_overlap",
			Definition: "EXCLUDE USING gist (builds_id_range WITH &&)",
		}},
	}}}
	err := checkCrossEngineSupportable(s, "postgres", "mysql", "test")
	if err == nil {
		t.Fatal("PG → MySQL with EXCLUDE: expected refusal, got nil")
	}
	// Operator-actionable message must name the constraint, the table,
	// and the --exclude-table recovery flag.
	msg := err.Error()
	wantSubstrings := []string{
		"EXCLUDE constraint",
		"check_ci_partitions_builds_id_range_no_overlap",
		"ci_partitions",
		"--exclude-table",
	}
	for _, want := range wantSubstrings {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal message missing %q\n--- got ---\n%s", want, msg)
		}
	}
}

// ADR-0053: PG → PG with an EXCLUDE constraint must NOT refuse
// (same-engine carries verbatim). Regression guard against an
// accidentally-too-broad refusal that would block the load-bearing
// happy path.
func TestCheckCrossEngineSupportable_PGtoPG_ExcludeAllowed(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "ci_partitions",
		ExcludeConstraints: []*ir.ExcludeConstraint{{
			Name:       "ex",
			Definition: "EXCLUDE USING gist (builds_id_range WITH &&)",
		}},
	}}}
	if err := checkCrossEngineSupportable(s, "postgres", "postgres", "test"); err != nil {
		t.Errorf("PG → PG with EXCLUDE err = %v; want nil (same-engine carry)", err)
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

// ---- task #72: postgres-trigger is a PG source for cross-engine refusals.

// TestCheckCrossEngineSupportable_PGTriggerToMySQL_ExcludeRefuses pins the
// task #72 gate fix: a `postgres-trigger` source carries the full PG-native
// type surface (its schema reader delegates to vanilla postgres), so a
// cross-engine `postgres-trigger` → MySQL with an EXCLUDE constraint must
// refuse with the SAME message a `postgres` source does. Pre-fix the gate
// only matched sourceEngine=="postgres", so a trigger source silently
// skipped every PG-native refusal — a Phase-2 cross-engine silent-loss
// hole.
func TestCheckCrossEngineSupportable_PGTriggerToMySQL_ExcludeRefuses(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "ci_partitions",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
		},
		ExcludeConstraints: []*ir.ExcludeConstraint{{
			Name:       "check_ci_partitions_builds_id_range_no_overlap",
			Definition: "EXCLUDE USING gist (builds_id_range WITH &&)",
		}},
	}}}
	// Trigger source must refuse identically to a `postgres` source.
	trigErr := checkCrossEngineSupportable(s, "postgres-trigger", "mysql", "test")
	if trigErr == nil {
		t.Fatal("postgres-trigger → MySQL with EXCLUDE: expected refusal, got nil")
	}
	pgErr := checkCrossEngineSupportable(s, "postgres", "mysql", "test")
	if pgErr == nil {
		t.Fatal("postgres → MySQL with EXCLUDE: expected refusal, got nil (test fixture invalid)")
	}
	if trigErr.Error() != pgErr.Error() {
		t.Errorf("trigger-source refusal differs from postgres-source refusal:\n  trigger: %s\n  postgres: %s",
			trigErr.Error(), pgErr.Error())
	}
}

// TestCheckCrossEngineSupportable_PGTriggerToPlanetScale_GeometryRefusesIndex
// confirms the gate fix covers the planetscale target too AND the
// index-level (pg_trgm / GIN) refusal, not just EXCLUDE — a trigger source
// with a PG GIN index must refuse cross-engine the same as a postgres
// source.
func TestCheckCrossEngineSupportable_PGTriggerToPlanetScale_GinIndexRefuses(t *testing.T) {
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
	err := checkCrossEngineSupportable(s, "postgres-trigger", "planetscale", "documents-migration")
	if err == nil {
		t.Fatal("postgres-trigger → planetscale with GIN index: expected refusal, got nil")
	}
	if !strings.Contains(err.Error(), "gin_trgm_ops") {
		t.Errorf("err = %v; want mention of \"gin_trgm_ops\"", err)
	}
}

// TestCheckCrossEngineSupportable_PGTriggerToPGTrigger_ExcludeAllowed pins
// that the gate fix does NOT over-broaden: same-engine postgres-trigger →
// postgres-trigger carries verbatim (the trigger engine's same-engine path
// is the Phase-1 shipped shape), so an EXCLUDE constraint must NOT refuse.
func TestCheckCrossEngineSupportable_PGTriggerToPGTrigger_ExcludeAllowed(t *testing.T) {
	s := &ir.Schema{Tables: []*ir.Table{{
		Name: "ci_partitions",
		ExcludeConstraints: []*ir.ExcludeConstraint{{
			Name:       "ex",
			Definition: "EXCLUDE USING gist (builds_id_range WITH &&)",
		}},
	}}}
	if err := checkCrossEngineSupportable(s, "postgres-trigger", "postgres-trigger", "test"); err != nil {
		t.Errorf("postgres-trigger → postgres-trigger with EXCLUDE err = %v; want nil (same-engine carry)", err)
	}
}

// TestCheckCrossEngineDeltaSupportable_PGTriggerAddTableGeometryAllowed
// confirms the delta path inherits the gate fix (it delegates to
// checkCrossEngineSupportable): an incremental adding a geometry-bearing
// table from a postgres-trigger source no longer skips the (now-allowed)
// PostGIS path, and a refusable shape still refuses. Geometry is allowed
// post-ADR-0035, so this asserts the no-refuse case — proving the trigger
// source is routed through the same delta gate as postgres.
func TestCheckCrossEngineDeltaSupportable_PGTriggerAddTableGeometryAllowed(t *testing.T) {
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
	if err := checkCrossEngineDeltaSupportable(deltas, "postgres-trigger", "mysql", "incr-0001"); err != nil {
		t.Errorf("err = %v; want nil (geometry allowed PG-trigger → MySQL)", err)
	}
}

// TestCheckCrossEngineDeltaSupportable_PGTriggerAddTableExtensionRefuses
// pins the delta refusal for the trigger source: an incremental adding a
// table with an uncatalogued ExtensionType column must refuse the same as a
// postgres source would.
func TestCheckCrossEngineDeltaSupportable_PGTriggerAddTableExtensionRefuses(t *testing.T) {
	deltas := []*ir.SchemaDeltaEntry{
		{
			Kind:  ir.SchemaDeltaAddTable,
			Table: "items",
			After: &ir.Table{
				Name: "items",
				Columns: []*ir.Column{
					{Name: "id", Type: ir.Integer{Width: 64}},
					{Name: "embedding", Type: ir.ExtensionType{Extension: "vector", Name: "vector"}},
				},
			},
		},
	}
	err := checkCrossEngineDeltaSupportable(deltas, "postgres-trigger", "mysql", "incr-0002")
	if err == nil {
		t.Fatal("postgres-trigger delta AddTable with ExtensionType: expected refusal, got nil")
	}
	if !strings.Contains(err.Error(), "vector.vector") {
		t.Errorf("err = %v; want mention of \"vector.vector\"", err)
	}
}

// TestIsPGSourceEngine pins the small helper that drives the gate fix.
func TestIsPGSourceEngine(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"postgres", true},
		{"postgres-trigger", true},
		{"mysql", false},
		{"planetscale", false},
		{"", false},
		{"future", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := isPGSourceEngine(c.name); got != c.want {
				t.Errorf("isPGSourceEngine(%q) = %v; want %v", c.name, got, c.want)
			}
		})
	}
}

// stubNoShardColumnSetter is a target type that intentionally does
// NOT implement ir.ShardColumnSetter. Used to pin the Shape-A
// cross-engine refusal — a future engine that ships without the
// surface must surface the refusal at openApplier-time before any
// CDC apply runs.
type stubNoShardColumnSetter struct{}

func TestCheckShardColumnSupport_DisengagedSkips(t *testing.T) {
	// Shape A not engaged → nil regardless of target shape.
	if err := checkShardColumnSupport(stubNoShardColumnSetter{}, ShardColumnSpec{}, "sync"); err != nil {
		t.Errorf("expected nil when not engaged; got %v", err)
	}
}

// stubShardColumnSetter implements ir.ShardColumnSetter — the
// engaged-but-supported happy path.
type stubShardColumnSetter struct {
	gotName string
	gotVal  any
}

func (s *stubShardColumnSetter) SetShardColumn(name string, value any) {
	s.gotName = name
	s.gotVal = value
}

func TestCheckShardColumnSupport_EngagedSupportedOK(t *testing.T) {
	target := &stubShardColumnSetter{}
	err := checkShardColumnSupport(target, ShardColumnSpec{Name: "shard", Value: "v1"}, "sync")
	if err != nil {
		t.Errorf("expected nil when target implements setter; got %v", err)
	}
}

func TestCheckShardColumnSupport_EngagedUnsupportedRefuses(t *testing.T) {
	err := checkShardColumnSupport(stubNoShardColumnSetter{}, ShardColumnSpec{Name: "shard", Value: "v1"}, "sync")
	if err == nil {
		t.Fatal("expected refusal; got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "ir.ShardColumnSetter") {
		t.Errorf("error %q missing interface name", msg)
	}
	if !strings.Contains(msg, "shard=v1") {
		t.Errorf("error %q missing shard/value", msg)
	}
	if !strings.Contains(msg, "ADR-0048") {
		t.Errorf("error %q missing ADR reference", msg)
	}
}
