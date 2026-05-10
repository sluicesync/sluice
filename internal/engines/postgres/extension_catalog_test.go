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
// v0.26.0 baseline is just "vector"; the test will need updating
// when pg_trgm / hstore / citext / postgis follow.
func TestRecognisedPGExtensionNames(t *testing.T) {
	names := recognisedPGExtensionNames()
	if len(names) == 0 {
		t.Fatal("recognisedPGExtensionNames returned empty")
	}
	found := false
	for _, n := range names {
		if n == "vector" {
			found = true
		}
	}
	if !found {
		t.Errorf("recognisedPGExtensionNames = %v; want to contain \"vector\"", names)
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
