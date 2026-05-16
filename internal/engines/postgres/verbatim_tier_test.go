// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// TestVerbatimTierFor pins the ADR-0047 (b)-vs-(c) determination. Tier
// (a) is decided earlier by the catalog dispatch and never reaches
// this predicate, so the only fork here is "same-engine guarantee
// present" → verbatim, else → refuse (the loud-fail default).
func TestVerbatimTierFor(t *testing.T) {
	if got := verbatimTierFor(true); got != verbatimTierVerbatim {
		t.Errorf("verbatimTierFor(true) = %v; want verbatimTierVerbatim", got)
	}
	if got := verbatimTierFor(false); got != verbatimTierRefuse {
		t.Errorf("verbatimTierFor(false) = %v; want verbatimTierRefuse", got)
	}
	// The zero value MUST be refuse so a reader that was never told
	// otherwise defaults to the loud refusal (tier c) — not a silent
	// verbatim carry.
	var zero verbatimTier
	if zero != verbatimTierRefuse {
		t.Errorf("zero verbatimTier = %v; want verbatimTierRefuse (loud-fail default)", zero)
	}
}

// TestTranslateType_VerbatimTier covers the three ADR-0047 tiers as
// they surface in translateType for an uncatalogued USER-DEFINED type.
func TestTranslateType_VerbatimTier(t *testing.T) {
	t.Run("tier b: eligible uncatalogued type → ir.VerbatimType", func(t *testing.T) {
		typ, err := translateType(columnMeta{
			DataType:         "USER-DEFINED",
			UDTName:          "ltree",
			FormatType:       "ltree",
			VerbatimEligible: true,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		v, ok := typ.(ir.VerbatimType)
		if !ok {
			t.Fatalf("got %T; want ir.VerbatimType", typ)
		}
		if v.Definition != "ltree" {
			t.Errorf("Definition = %q; want %q", v.Definition, "ltree")
		}
	})

	t.Run("tier b: format_type spelling carried verbatim incl. modifiers", func(t *testing.T) {
		typ, err := translateType(columnMeta{
			DataType:         "USER-DEFINED",
			UDTName:          "cube",
			FormatType:       "public.weird_type(3,2)",
			VerbatimEligible: true,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := typ.(ir.VerbatimType).Definition; got != "public.weird_type(3,2)" {
			t.Errorf("Definition = %q; want verbatim format_type", got)
		}
	})

	t.Run("tier c: NOT eligible → loud refusal preserved (no weakening)", func(t *testing.T) {
		_, err := translateType(columnMeta{
			DataType:   "USER-DEFINED",
			UDTName:    "ltree",
			FormatType: "ltree",
			// VerbatimEligible false — cross-engine / no same-engine
			// guarantee. Must still refuse loudly.
		})
		if err == nil {
			t.Fatal("expected loud refusal for uncatalogued user-defined type without same-engine guarantee; got nil")
		}
	})

	t.Run("regression: enum still wins over verbatim (first-class IR shape)", func(t *testing.T) {
		typ, err := translateType(columnMeta{
			DataType:         "USER-DEFINED",
			UDTName:          "mood",
			FormatType:       "mood",
			EnumValues:       []string{"sad", "ok", "happy"},
			VerbatimEligible: true, // eligible, but enum must take precedence
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := typ.(ir.Enum); !ok {
			t.Fatalf("got %T; want ir.Enum (enum must not be shadowed by verbatim tier)", typ)
		}
	})

	t.Run("regression: geometry still wins over verbatim", func(t *testing.T) {
		typ, err := translateType(columnMeta{
			DataType:         "USER-DEFINED",
			UDTName:          "geometry",
			FormatType:       "geometry",
			GeometryInfo:     &geometryColumnInfo{Subtype: "POINT", SRID: 4326},
			VerbatimEligible: true,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if _, ok := typ.(ir.Geometry); !ok {
			t.Fatalf("got %T; want ir.Geometry (geometry must not be shadowed by verbatim tier)", typ)
		}
	})

	t.Run("eligible but empty format_type → loud sluice-bug error", func(t *testing.T) {
		_, err := translateType(columnMeta{
			DataType:         "USER-DEFINED",
			UDTName:          "ltree",
			FormatType:       "",
			VerbatimEligible: true,
		})
		if err == nil {
			t.Fatal("expected loud error when format_type is empty; got nil")
		}
	})
}

// TestSetVerbatimExtensionPassthrough verifies the engine surface flips
// the reader's flag (the orchestrator's only lever for tier (b)).
func TestSetVerbatimExtensionPassthrough(t *testing.T) {
	r := &SchemaReader{}
	if r.verbatimPassthrough {
		t.Fatal("zero-value SchemaReader must default to verbatimPassthrough=false (tier c)")
	}
	// Reader implements the optional ir.VerbatimExtensionAware surface.
	var _ ir.VerbatimExtensionAware = r
	r.SetVerbatimExtensionPassthrough(true)
	if !r.verbatimPassthrough {
		t.Error("SetVerbatimExtensionPassthrough(true) did not enable the flag")
	}
	r.SetVerbatimExtensionPassthrough(false)
	if r.verbatimPassthrough {
		t.Error("SetVerbatimExtensionPassthrough(false) did not disable the flag")
	}
}
