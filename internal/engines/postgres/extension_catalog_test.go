// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"strings"
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestPGExtensionCatalog_ContainsVector confirms the v0.26.0 baseline
// shape: pgvector is present, with the typesByName / emitColumn /
// indexAccessMethods set populated (the load-bearing surface for
// schema reader recognition + writer emit + index passthrough).
func TestPGExtensionCatalog_ContainsVector(t *testing.T) {
	def, ok := pgExtensionCatalog["vector"]
	if !ok {
		t.Fatal("pgExtensionCatalog missing 'vector' entry")
	}
	if _, ok := def.typesByName["vector"]; !ok {
		t.Errorf("pgvector typesByName missing 'vector'")
	}
	if def.emitColumn == nil {
		t.Errorf("pgvector emitColumn is nil")
	}
	if _, ok := def.indexAccessMethods["ivfflat"]; !ok {
		t.Errorf("pgvector indexAccessMethods missing 'ivfflat'")
	}
	if _, ok := def.indexAccessMethods["hnsw"]; !ok {
		t.Errorf("pgvector indexAccessMethods missing 'hnsw'")
	}
}

// TestPGVectorEmit_DimensionPositive emits the canonical
// `vector(N)` form for a column with a positive dimension modifier.
func TestPGVectorEmit_DimensionPositive(t *testing.T) {
	def := pgExtensionCatalog["vector"]
	got, err := def.emitColumn(ir.ExtensionType{
		Extension: "vector",
		Name:      "vector",
		Modifiers: []int{384},
	})
	if err != nil {
		t.Fatalf("emitColumn: %v", err)
	}
	if got != "vector(384)" {
		t.Errorf("emitColumn = %q; want %q", got, "vector(384)")
	}
}

// TestPGVectorEmit_NoDimension emits bare `vector` when no modifier
// is present (legal pgvector shape; column accepts any dimension).
func TestPGVectorEmit_NoDimension(t *testing.T) {
	def := pgExtensionCatalog["vector"]
	got, err := def.emitColumn(ir.ExtensionType{
		Extension: "vector",
		Name:      "vector",
	})
	if err != nil {
		t.Fatalf("emitColumn: %v", err)
	}
	if got != "vector" {
		t.Errorf("emitColumn = %q; want %q", got, "vector")
	}
}

// TestPGVectorEmit_BadDimension refuses non-positive dimensions
// loudly rather than emitting invalid DDL.
func TestPGVectorEmit_BadDimension(t *testing.T) {
	def := pgExtensionCatalog["vector"]
	_, err := def.emitColumn(ir.ExtensionType{
		Extension: "vector",
		Name:      "vector",
		Modifiers: []int{0},
	})
	if err == nil {
		t.Fatal("expected error on dimension=0; got nil")
	}
	if !strings.Contains(err.Error(), "must be > 0") {
		t.Errorf("err = %v; want contains \"must be > 0\"", err)
	}
}

// TestPGVectorEmit_TooManyModifiers refuses a malformed Modifiers
// slice (pgvector only carries one modifier — dimension).
func TestPGVectorEmit_TooManyModifiers(t *testing.T) {
	def := pgExtensionCatalog["vector"]
	_, err := def.emitColumn(ir.ExtensionType{
		Extension: "vector",
		Name:      "vector",
		Modifiers: []int{384, 99},
	})
	if err == nil {
		t.Fatal("expected error on 2 modifiers; got nil")
	}
}

// TestPGVectorEmit_MismatchedExtension refuses a request that names
// the wrong extension (defensive — operator can't trip this via the
// CLI since the catalog dispatch keys on the name, but the shape
// should be safe).
func TestPGVectorEmit_MismatchedExtension(t *testing.T) {
	def := pgExtensionCatalog["vector"]
	_, err := def.emitColumn(ir.ExtensionType{
		Extension: "hstore",
		Name:      "hstore",
	})
	if err == nil {
		t.Fatal("expected error on extension mismatch; got nil")
	}
}

// TestPGVectorBuild_DimFromTypmod decodes the dimension out of
// pg_attribute.atttypmod the way pgvector encodes it. The reference
// is `vector_typmod_in` in pgvector/src/vector.c: typmod IS the
// dimension verbatim (no offset). -1 is PG's "no typmod" sentinel.
//
// Bug 47 fix: an earlier revision of this test (and the helper)
// assumed a `dimension + 4` offset; pgvector itself does not use
// such an offset, and the integration test surfaced the mismatch
// when the source IR's Modifiers came back empty for a `vector(3)`
// column whose atttypmod was 3.
func TestPGVectorBuild_DimFromTypmod(t *testing.T) {
	cases := []struct {
		typmod int32
		want   []int
	}{
		{-1, []int{}},         // no typmod
		{0, []int{}},          // sentinel — defensive, shouldn't surface from PG
		{1, []int{1}},         // dimension 1 (pgvector's minimum)
		{3, []int{3}},         // dimension 3 — the integration test's seed
		{4, []int{4}},         // dimension 4
		{384, []int{384}},     // dimension 384
		{16000, []int{16000}}, // pgvector's documented max
	}
	def := pgExtensionCatalog["vector"]
	for _, c := range cases {
		c := c
		got, err := def.build("vector", c.typmod)
		if err != nil {
			t.Fatalf("build(typmod=%d): %v", c.typmod, err)
		}
		if got.Extension != "vector" || got.Name != "vector" {
			t.Errorf("build(typmod=%d): got Extension=%q Name=%q; want vector/vector",
				c.typmod, got.Extension, got.Name)
		}
		if len(got.Modifiers) != len(c.want) {
			t.Errorf("build(typmod=%d): Modifiers = %v; want %v",
				c.typmod, got.Modifiers, c.want)
			continue
		}
		for i := range c.want {
			if got.Modifiers[i] != c.want[i] {
				t.Errorf("build(typmod=%d): Modifiers[%d] = %d; want %d",
					c.typmod, i, got.Modifiers[i], c.want[i])
			}
		}
	}
}

// TestRecognisedPGExtensionNames returns the catalog's keys sorted.
// As the v1 shortlist lands the recognised set grows; this test
// pins each load-bearing entry (vector since v0.26.0; pg_trgm since
// v0.30.0; hstore + citext since v0.31.0; postgis since v0.33.0 —
// the full ADR-0032 v1 shortlist) and the sort-order invariant.
func TestRecognisedPGExtensionNames(t *testing.T) {
	names := recognisedPGExtensionNames()
	if len(names) == 0 {
		t.Fatal("recognisedPGExtensionNames returned empty")
	}
	want := map[string]bool{
		"vector":  false,
		"pg_trgm": false,
		"hstore":  false,
		"citext":  false,
		"postgis": false,
	}
	for _, n := range names {
		if _, ok := want[n]; ok {
			want[n] = true
		}
	}
	for n, found := range want {
		if !found {
			t.Errorf("recognisedPGExtensionNames = %v; want to contain %q", names, n)
		}
	}
	// Ensure sort order: previous entry < current entry for every i>0.
	for i := 1; i < len(names); i++ {
		if names[i-1] >= names[i] {
			t.Errorf("recognisedPGExtensionNames not sorted: %v", names)
			break
		}
	}
}

// TestEmitExtensionColumn_UnknownExtension surfaces the operator-
// actionable message when an IR ExtensionType references an
// extension not in the catalog (typically: hand-constructed IR or
// operator forgot --enable-pg-extension).
func TestEmitExtensionColumn_UnknownExtension(t *testing.T) {
	_, err := emitExtensionColumn(ir.ExtensionType{
		Extension: "unknown_ext",
		Name:      "some_type",
	})
	if err == nil {
		t.Fatal("expected error on unknown extension; got nil")
	}
	if !strings.Contains(err.Error(), "not in the catalog") {
		t.Errorf("err = %v; want contains \"not in the catalog\"", err)
	}
}

// TestExtensionAccessMethodEnabled gates the index-method
// passthrough on the operator's --enable-pg-extension allowlist.
// A method from a known extension that's NOT enabled should not
// be passthrough-able.
func TestExtensionAccessMethodEnabled(t *testing.T) {
	cases := []struct {
		name    string
		method  string
		enabled map[string]bool
		want    bool
	}{
		{
			name:    "ivfflat with vector enabled",
			method:  "ivfflat",
			enabled: map[string]bool{"vector": true},
			want:    true,
		},
		{
			name:    "hnsw with vector enabled",
			method:  "hnsw",
			enabled: map[string]bool{"vector": true},
			want:    true,
		},
		{
			name:    "ivfflat with vector NOT enabled",
			method:  "ivfflat",
			enabled: map[string]bool{},
			want:    false,
		},
		{
			name:    "btree never matches (it's core PG)",
			method:  "btree",
			enabled: map[string]bool{"vector": true},
			want:    false,
		},
		{
			name:    "unknown method",
			method:  "voodoo",
			enabled: map[string]bool{"vector": true},
			want:    false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := extensionAccessMethodEnabled(c.method, c.enabled)
			if got != c.want {
				t.Errorf("extensionAccessMethodEnabled(%q, %v) = %v; want %v",
					c.method, c.enabled, got, c.want)
			}
		})
	}
}

// TestLookupExtensionForType matches a udt_name against an enabled
// extension's typesByName set.
func TestLookupExtensionForType(t *testing.T) {
	cases := []struct {
		name        string
		udtName     string
		enabled     map[string]bool
		wantExt     string
		wantMatched bool
	}{
		{
			name:        "vector type with vector enabled",
			udtName:     "vector",
			enabled:     map[string]bool{"vector": true},
			wantExt:     "vector",
			wantMatched: true,
		},
		{
			name:        "vector type without enabling",
			udtName:     "vector",
			enabled:     map[string]bool{},
			wantMatched: false,
		},
		{
			name:        "unknown udt with vector enabled",
			udtName:     "geometry",
			enabled:     map[string]bool{"vector": true},
			wantMatched: false,
		},
		{
			name:        "empty udt",
			udtName:     "",
			enabled:     map[string]bool{"vector": true},
			wantMatched: false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			ext, _, ok := lookupExtensionForType(c.udtName, c.enabled)
			if ok != c.wantMatched {
				t.Errorf("lookupExtensionForType(%q, %v) matched=%v; want %v",
					c.udtName, c.enabled, ok, c.wantMatched)
			}
			if ok && ext != c.wantExt {
				t.Errorf("ext = %q; want %q", ext, c.wantExt)
			}
		})
	}
}

// TestPGExtensionCatalog_ContainsTrgm pins the pg_trgm catalog entry's
// load-bearing shape: it owns no column types, declares no new index
// access methods (it rides on core PG `gin` / `gist`), and registers
// the two operator classes (`gin_trgm_ops`, `gist_trgm_ops`) the
// schema reader uses to round-trip pg_trgm indexes via the
// IndexColumn.OperatorClass field.
func TestPGExtensionCatalog_ContainsTrgm(t *testing.T) {
	def, ok := pgExtensionCatalog["pg_trgm"]
	if !ok {
		t.Fatal("pgExtensionCatalog missing 'pg_trgm' entry")
	}
	if len(def.typesByName) != 0 {
		t.Errorf("pg_trgm typesByName = %v; want empty (no column types)", def.typesByName)
	}
	if len(def.indexAccessMethods) != 0 {
		t.Errorf("pg_trgm indexAccessMethods = %v; want empty (rides on core gin / gist)",
			def.indexAccessMethods)
	}
	if _, ok := def.indexOperatorClasses["gin_trgm_ops"]; !ok {
		t.Errorf("pg_trgm indexOperatorClasses missing 'gin_trgm_ops'")
	}
	if _, ok := def.indexOperatorClasses["gist_trgm_ops"]; !ok {
		t.Errorf("pg_trgm indexOperatorClasses missing 'gist_trgm_ops'")
	}
}

// TestPGTrgmEmit_SentinelRefusal exercises the operator-class-only
// extension's emitColumn sentinel: any caller hitting it has a
// hand-constructed IR or a framework bug, since pg_trgm has no
// column types. The refusal must be loud and operator-actionable.
func TestPGTrgmEmit_SentinelRefusal(t *testing.T) {
	def := pgExtensionCatalog["pg_trgm"]
	_, err := def.emitColumn(ir.ExtensionType{
		Extension: "pg_trgm",
		Name:      "trgm",
	})
	if err == nil {
		t.Fatal("expected sentinel refusal on pg_trgm emitColumn; got nil")
	}
	if !strings.Contains(err.Error(), "no column types") {
		t.Errorf("err = %v; want contains \"no column types\"", err)
	}
}

// TestPGTrgmBuild_SentinelRefusal mirrors the emit sentinel for the
// build path: the schema reader's lookupExtensionForType is gated on
// typesByName (which pg_trgm leaves empty), so build is unreachable
// on the read path. The sentinel is defensive against framework
// regressions.
func TestPGTrgmBuild_SentinelRefusal(t *testing.T) {
	def := pgExtensionCatalog["pg_trgm"]
	_, err := def.build("anything", -1)
	if err == nil {
		t.Fatal("expected sentinel refusal on pg_trgm build; got nil")
	}
	if !strings.Contains(err.Error(), "no column types") {
		t.Errorf("err = %v; want contains \"no column types\"", err)
	}
}

// TestExtensionOperatorClassEnabled is the load-bearing gate the
// schema reader uses to decide whether to carry an opclass forward
// into IR. pg_trgm's `gin_trgm_ops` rides on core PG `gin`, so the
// `idx.Method != ""` check (which only fires for extension-introduced
// AMs like pgvector's hnsw) doesn't catch it; the per-opclass gate
// here closes that gap.
func TestExtensionOperatorClassEnabled(t *testing.T) {
	cases := []struct {
		name    string
		opclass string
		enabled map[string]bool
		want    bool
	}{
		{
			name:    "gin_trgm_ops with pg_trgm enabled",
			opclass: "gin_trgm_ops",
			enabled: map[string]bool{"pg_trgm": true},
			want:    true,
		},
		{
			name:    "gist_trgm_ops with pg_trgm enabled",
			opclass: "gist_trgm_ops",
			enabled: map[string]bool{"pg_trgm": true},
			want:    true,
		},
		{
			name:    "gin_trgm_ops with pg_trgm NOT enabled",
			opclass: "gin_trgm_ops",
			enabled: map[string]bool{},
			want:    false,
		},
		{
			name:    "vector_l2_ops with vector enabled (cross-extension lookup)",
			opclass: "vector_l2_ops",
			enabled: map[string]bool{"vector": true},
			want:    true,
		},
		{
			name:    "vector_l2_ops with only pg_trgm enabled (different extension)",
			opclass: "vector_l2_ops",
			enabled: map[string]bool{"pg_trgm": true},
			want:    false,
		},
		{
			name:    "empty opclass",
			opclass: "",
			enabled: map[string]bool{"pg_trgm": true},
			want:    false,
		},
		{
			name:    "unknown opclass",
			opclass: "voodoo_ops",
			enabled: map[string]bool{"pg_trgm": true},
			want:    false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := extensionOperatorClassEnabled(c.opclass, c.enabled)
			if got != c.want {
				t.Errorf("extensionOperatorClassEnabled(%q, %v) = %v; want %v",
					c.opclass, c.enabled, got, c.want)
			}
		})
	}
}

// TestExtensionOperatorClassRegistered is the catalog-wide variant
// (no per-extension enabled gate) used by the cross-engine refusal
// path. Any opclass populated by sluice into [ir.IndexColumn.OperatorClass]
// is by construction extension-owned (Bug 47 design), so this helper
// just verifies the catalog membership for symmetry with the
// extensionAccessMethodEnabled / extensionOperatorClassEnabled pair.
func TestExtensionOperatorClassRegistered(t *testing.T) {
	cases := []struct {
		opclass string
		want    bool
	}{
		{"gin_trgm_ops", true},
		{"gist_trgm_ops", true},
		{"vector_l2_ops", true},
		{"vector_cosine_ops", true},
		{"text_ops", false}, // core-PG opclass, never registered
		{"", false},
	}
	for _, c := range cases {
		c := c
		t.Run(c.opclass, func(t *testing.T) {
			got := extensionOperatorClassRegistered(c.opclass)
			if got != c.want {
				t.Errorf("extensionOperatorClassRegistered(%q) = %v; want %v",
					c.opclass, got, c.want)
			}
		})
	}
}

// TestEmitExtensionColumn_TrgmRefusal pins the operator-actionable
// shape when an [ir.ExtensionType] is hand-constructed naming
// pg_trgm — the dispatcher returns the catalog's per-entry sentinel
// rather than emitting invalid DDL.
func TestEmitExtensionColumn_TrgmRefusal(t *testing.T) {
	_, err := emitExtensionColumn(ir.ExtensionType{
		Extension: "pg_trgm",
		Name:      "trgm",
	})
	if err == nil {
		t.Fatal("expected error on pg_trgm column emit; got nil")
	}
	if !strings.Contains(err.Error(), "no column types") {
		t.Errorf("err = %v; want contains \"no column types\"", err)
	}
}

// TestPGExtensionCatalog_ContainsHstore pins the hstore catalog
// entry's load-bearing shape: it owns the `hstore` column type and
// declares no new index access methods or operator classes (the GIN /
// GiST opclasses are out-of-v1-scope per the research doc).
func TestPGExtensionCatalog_ContainsHstore(t *testing.T) {
	def, ok := pgExtensionCatalog["hstore"]
	if !ok {
		t.Fatal("pgExtensionCatalog missing 'hstore' entry")
	}
	if _, ok := def.typesByName["hstore"]; !ok {
		t.Errorf("hstore typesByName missing 'hstore'")
	}
	if def.emitColumn == nil {
		t.Errorf("hstore emitColumn is nil")
	}
	if len(def.indexAccessMethods) != 0 {
		t.Errorf("hstore indexAccessMethods = %v; want empty (no extension-AMs)",
			def.indexAccessMethods)
	}
	if len(def.indexOperatorClasses) != 0 {
		t.Errorf("hstore indexOperatorClasses = %v; want empty (opclasses are v2)",
			def.indexOperatorClasses)
	}
}

// TestPGHstoreEmit pins the bareword `hstore` DDL form. No modifiers
// allowed (hstore has no per-column length/cardinality).
func TestPGHstoreEmit(t *testing.T) {
	def := pgExtensionCatalog["hstore"]
	got, err := def.emitColumn(ir.ExtensionType{
		Extension: "hstore",
		Name:      "hstore",
	})
	if err != nil {
		t.Fatalf("emitColumn: %v", err)
	}
	if got != "hstore" {
		t.Errorf("emitColumn = %q; want %q", got, "hstore")
	}
}

// TestPGHstoreEmit_RejectsModifiers refuses an IR shape with
// modifiers — hstore has none, so any modifier is a programmer error.
func TestPGHstoreEmit_RejectsModifiers(t *testing.T) {
	def := pgExtensionCatalog["hstore"]
	_, err := def.emitColumn(ir.ExtensionType{
		Extension: "hstore",
		Name:      "hstore",
		Modifiers: []int{1},
	})
	if err == nil {
		t.Fatal("expected error on hstore with modifiers; got nil")
	}
}

// TestPGHstoreBuild decodes the IR shape from the udt_name. No
// modifiers regardless of typmod (hstore has no per-column type
// modifier — atttypmod is always -1).
func TestPGHstoreBuild(t *testing.T) {
	def := pgExtensionCatalog["hstore"]
	got, err := def.build("hstore", -1)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if got.Extension != "hstore" || got.Name != "hstore" {
		t.Errorf("build Extension=%q Name=%q; want hstore/hstore",
			got.Extension, got.Name)
	}
	if len(got.Modifiers) != 0 {
		t.Errorf("build Modifiers = %v; want empty", got.Modifiers)
	}
}

// TestPGHstoreBuild_RejectsWrongUDT defends the catalog dispatcher —
// calling build with the wrong udt_name is a programmer error.
func TestPGHstoreBuild_RejectsWrongUDT(t *testing.T) {
	def := pgExtensionCatalog["hstore"]
	_, err := def.build("not_hstore", -1)
	if err == nil {
		t.Fatal("expected error on wrong udt; got nil")
	}
}

// TestPGExtensionCatalog_ContainsCiText pins the citext catalog
// entry's shape: owns the `citext` type, no new AMs / opclasses
// (citext rides on core PG btree / gin / gist).
func TestPGExtensionCatalog_ContainsCiText(t *testing.T) {
	def, ok := pgExtensionCatalog["citext"]
	if !ok {
		t.Fatal("pgExtensionCatalog missing 'citext' entry")
	}
	if _, ok := def.typesByName["citext"]; !ok {
		t.Errorf("citext typesByName missing 'citext'")
	}
	if def.emitColumn == nil {
		t.Errorf("citext emitColumn is nil")
	}
	if len(def.indexAccessMethods) != 0 {
		t.Errorf("citext indexAccessMethods = %v; want empty", def.indexAccessMethods)
	}
	if len(def.indexOperatorClasses) != 0 {
		t.Errorf("citext indexOperatorClasses = %v; want empty", def.indexOperatorClasses)
	}
}

// TestPGCiTextEmit pins the bareword `citext` DDL form.
func TestPGCiTextEmit(t *testing.T) {
	def := pgExtensionCatalog["citext"]
	got, err := def.emitColumn(ir.ExtensionType{
		Extension: "citext",
		Name:      "citext",
	})
	if err != nil {
		t.Fatalf("emitColumn: %v", err)
	}
	if got != "citext" {
		t.Errorf("emitColumn = %q; want %q", got, "citext")
	}
}

// TestPGCiTextEmit_RejectsModifiers refuses an IR shape with
// modifiers — citext has none (it's text with a custom collation,
// no length parameter).
func TestPGCiTextEmit_RejectsModifiers(t *testing.T) {
	def := pgExtensionCatalog["citext"]
	_, err := def.emitColumn(ir.ExtensionType{
		Extension: "citext",
		Name:      "citext",
		Modifiers: []int{255},
	})
	if err == nil {
		t.Fatal("expected error on citext with modifiers; got nil")
	}
}

// TestPGCiTextBuild decodes the IR shape from the udt_name.
func TestPGCiTextBuild(t *testing.T) {
	def := pgExtensionCatalog["citext"]
	got, err := def.build("citext", -1)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if got.Extension != "citext" || got.Name != "citext" {
		t.Errorf("build Extension=%q Name=%q; want citext/citext",
			got.Extension, got.Name)
	}
	if len(got.Modifiers) != 0 {
		t.Errorf("build Modifiers = %v; want empty", got.Modifiers)
	}
}

// TestHasCrossEngineDefaultTranslator pins the catalog-side
// declaration the pipeline's cross-engine preflight consults. Today
// hstore and citext qualify (Tier 1 type-only with defensible MySQL
// mappings per the research doc § 5); vector / pg_trgm / postgis do
// not — they preserve the loud-failure default cross-engine.
func TestHasCrossEngineDefaultTranslator(t *testing.T) {
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
			got := HasCrossEngineDefaultTranslator(c.name)
			if got != c.want {
				t.Errorf("HasCrossEngineDefaultTranslator(%q) = %v; want %v",
					c.name, got, c.want)
			}
		})
	}
}

// TestLookupExtensionForType_Hstore matches a `hstore` udt against
// the enabled extension set, mirroring the pgvector test pattern.
func TestLookupExtensionForType_Hstore(t *testing.T) {
	cases := []struct {
		name        string
		udtName     string
		enabled     map[string]bool
		wantExt     string
		wantMatched bool
	}{
		{
			name:        "hstore type with hstore enabled",
			udtName:     "hstore",
			enabled:     map[string]bool{"hstore": true},
			wantExt:     "hstore",
			wantMatched: true,
		},
		{
			name:        "citext type with citext enabled",
			udtName:     "citext",
			enabled:     map[string]bool{"citext": true},
			wantExt:     "citext",
			wantMatched: true,
		},
		{
			name:        "hstore type without enabling",
			udtName:     "hstore",
			enabled:     map[string]bool{},
			wantMatched: false,
		},
		{
			name:        "citext type without enabling",
			udtName:     "citext",
			enabled:     map[string]bool{},
			wantMatched: false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			ext, _, ok := lookupExtensionForType(c.udtName, c.enabled)
			if ok != c.wantMatched {
				t.Errorf("lookupExtensionForType(%q, %v) matched=%v; want %v",
					c.udtName, c.enabled, ok, c.wantMatched)
			}
			if ok && ext != c.wantExt {
				t.Errorf("ext = %q; want %q", ext, c.wantExt)
			}
		})
	}
}

// TestPGExtensionCatalog_ContainsPostGIS pins the postgis catalog
// entry's load-bearing shape: it owns NO column types via typesByName
// (PostGIS's `geometry` rides on the first-class [ir.Geometry] IR
// type per ADR-0035, not [ir.ExtensionType]), declares NO new index
// access methods (gist / spgist / brin are core PG), and registers
// the operator-class names PostGIS introduces for those AMs over
// geometry / geography columns. `geometry` and `geography` are
// recorded in hintTypeNames so the schema reader's
// `extensionOwningType` hint path surfaces the
// `--enable-pg-extension postgis` pointer for unenabled-extension
// columns.
func TestPGExtensionCatalog_ContainsPostGIS(t *testing.T) {
	def, ok := pgExtensionCatalog["postgis"]
	if !ok {
		t.Fatal("pgExtensionCatalog missing 'postgis' entry")
	}
	if len(def.typesByName) != 0 {
		t.Errorf("postgis typesByName = %v; want empty "+
			"(geometry routes via ir.Geometry not ir.ExtensionType)",
			def.typesByName)
	}
	if len(def.indexAccessMethods) != 0 {
		t.Errorf("postgis indexAccessMethods = %v; want empty "+
			"(gist/spgist/brin are core PG)",
			def.indexAccessMethods)
	}
	// Load-bearing opclasses — GiST, SP-GiST, BRIN covered.
	wantOpclasses := []string{
		"gist_geometry_ops_2d",
		"gist_geometry_ops_nd",
		"gist_geography_ops",
		"spgist_geometry_ops_2d",
		"spgist_geometry_ops_3d",
		"spgist_geometry_ops_nd",
		"brin_geometry_inclusion_ops_2d",
		"brin_geometry_inclusion_ops_4d",
		"brin_geometry_inclusion_ops_nd",
	}
	for _, op := range wantOpclasses {
		if _, ok := def.indexOperatorClasses[op]; !ok {
			t.Errorf("postgis indexOperatorClasses missing %q", op)
		}
	}
	// Hint-only types: geometry + geography. The IR uses ir.Geometry
	// directly for geometry; geography has no IR shape today but
	// surfaces in the hint path nonetheless.
	for _, name := range []string{"geometry", "geography"} {
		if _, ok := def.hintTypeNames[name]; !ok {
			t.Errorf("postgis hintTypeNames missing %q", name)
		}
	}
}

// TestPGPostGISEmit_SentinelRefusal exercises the "framework misuse"
// path: any caller invoking `emitColumn` on the postgis entry has a
// hand-constructed IR or a framework bug, since PostGIS types route
// through [ir.Geometry], not [ir.ExtensionType]. The refusal must be
// loud and operator-actionable.
func TestPGPostGISEmit_SentinelRefusal(t *testing.T) {
	def := pgExtensionCatalog["postgis"]
	_, err := def.emitColumn(ir.ExtensionType{
		Extension: "postgis",
		Name:      "geometry",
	})
	if err == nil {
		t.Fatal("expected sentinel refusal on postgis emitColumn; got nil")
	}
	if !strings.Contains(err.Error(), "ir.Geometry") {
		t.Errorf("err = %v; want mention of \"ir.Geometry\"", err)
	}
}

// TestPGPostGISBuild_SentinelRefusal mirrors the emit refusal on the
// build side: the schema reader's catalog lookup is gated on
// typesByName (which postgis leaves empty), so build is unreachable
// in practice — the sentinel exists as a defensive guard.
func TestPGPostGISBuild_SentinelRefusal(t *testing.T) {
	def := pgExtensionCatalog["postgis"]
	_, err := def.build("geometry", -1)
	if err == nil {
		t.Fatal("expected sentinel refusal on postgis build; got nil")
	}
	if !strings.Contains(err.Error(), "ir.Geometry") {
		t.Errorf("err = %v; want mention of \"ir.Geometry\"", err)
	}
}

// TestExtensionOwningType_PostGIS pins the operator-actionable hint
// surface: a `geometry` or `geography` column whose owning extension
// isn't enabled surfaces the postgis pointer at translateType. The
// hint rides on hintTypeNames (not typesByName) because PostGIS's
// IR shape pre-dates ADR-0032 — see the catalog entry's comments.
func TestExtensionOwningType_PostGIS(t *testing.T) {
	cases := []struct {
		udt  string
		want string
	}{
		{"geometry", "postgis"},
		{"geography", "postgis"},
		{"vector", "vector"},
		{"hstore", "hstore"},
		{"citext", "citext"},
		{"int4", ""},
		{"", ""},
		{"unknown_udt", ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.udt, func(t *testing.T) {
			got := extensionOwningType(c.udt)
			if got != c.want {
				t.Errorf("extensionOwningType(%q) = %q; want %q",
					c.udt, got, c.want)
			}
		})
	}
}

// TestExtensionOperatorClassEnabled_PostGIS covers the per-opclass
// passthrough gate for postgis. The schema reader's index-population
// path consults this helper to decide whether to preserve the
// opclass on [ir.IndexColumn.OperatorClass]; without
// `--enable-pg-extension postgis`, the opclass is dropped.
func TestExtensionOperatorClassEnabled_PostGIS(t *testing.T) {
	cases := []struct {
		name    string
		opclass string
		enabled map[string]bool
		want    bool
	}{
		{
			name:    "gist_geometry_ops_2d with postgis enabled",
			opclass: "gist_geometry_ops_2d",
			enabled: map[string]bool{"postgis": true},
			want:    true,
		},
		{
			name:    "gist_geometry_ops_nd with postgis enabled",
			opclass: "gist_geometry_ops_nd",
			enabled: map[string]bool{"postgis": true},
			want:    true,
		},
		{
			name:    "gist_geography_ops with postgis enabled",
			opclass: "gist_geography_ops",
			enabled: map[string]bool{"postgis": true},
			want:    true,
		},
		{
			name:    "spgist_geometry_ops_2d with postgis enabled",
			opclass: "spgist_geometry_ops_2d",
			enabled: map[string]bool{"postgis": true},
			want:    true,
		},
		{
			name:    "brin_geometry_inclusion_ops_2d with postgis enabled",
			opclass: "brin_geometry_inclusion_ops_2d",
			enabled: map[string]bool{"postgis": true},
			want:    true,
		},
		{
			name:    "gist_geometry_ops_2d without postgis",
			opclass: "gist_geometry_ops_2d",
			enabled: map[string]bool{},
			want:    false,
		},
		{
			name:    "gist_geometry_ops_2d with only pg_trgm enabled",
			opclass: "gist_geometry_ops_2d",
			enabled: map[string]bool{"pg_trgm": true},
			want:    false,
		},
		{
			name:    "gin_trgm_ops with postgis enabled (different extension)",
			opclass: "gin_trgm_ops",
			enabled: map[string]bool{"postgis": true},
			want:    false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got := extensionOperatorClassEnabled(c.opclass, c.enabled)
			if got != c.want {
				t.Errorf("extensionOperatorClassEnabled(%q, %v) = %v; want %v",
					c.opclass, c.enabled, got, c.want)
			}
		})
	}
}

// TestExtensionOwningOperatorClass_PostGIS verifies the writer-side
// hint dispatcher attributes postgis-owned opclasses correctly.
// Mirrors the pg_trgm and pgvector hint coverage.
func TestExtensionOwningOperatorClass_PostGIS(t *testing.T) {
	cases := []struct {
		opclass string
		want    string
	}{
		{"gist_geometry_ops_2d", "postgis"},
		{"gist_geometry_ops_nd", "postgis"},
		{"gist_geography_ops", "postgis"},
		{"spgist_geometry_ops_nd", "postgis"},
		{"brin_geometry_inclusion_ops_2d", "postgis"},
		{"gin_trgm_ops", "pg_trgm"},
		{"vector_l2_ops", "vector"},
		{"", ""},
		{"not_an_opclass", ""},
	}
	for _, c := range cases {
		c := c
		t.Run(c.opclass, func(t *testing.T) {
			got := extensionOwningOperatorClass(c.opclass)
			if got != c.want {
				t.Errorf("extensionOwningOperatorClass(%q) = %q; want %q",
					c.opclass, got, c.want)
			}
		})
	}
}

// TestLookupExtensionForType_PostGISNotRouted pins the design choice
// that PostGIS's `geometry` / `geography` types do NOT route through
// the catalog's build/emit machinery — they have first-class IR
// shapes (ir.Geometry). lookupExtensionForType (which gates IR
// dispatch on typesByName) must therefore return false for them
// regardless of the operator's --enable-pg-extension allowlist; the
// `c.UDTName == "geometry"` branch in types.go::translateType
// continues to own the dispatch.
func TestLookupExtensionForType_PostGISNotRouted(t *testing.T) {
	for _, udt := range []string{"geometry", "geography"} {
		udt := udt
		t.Run(udt, func(t *testing.T) {
			_, _, ok := lookupExtensionForType(udt, map[string]bool{"postgis": true})
			if ok {
				t.Errorf("lookupExtensionForType(%q, postgis=true) = matched; "+
					"want unmatched (geometry/geography route via ir.Geometry, "+
					"not the catalog's build/emit path)", udt)
			}
		})
	}
}
