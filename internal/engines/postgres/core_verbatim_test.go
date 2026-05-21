// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"testing"

	"github.com/orware/sluice/internal/ir"
)

// ADR-0051 pins. Sibling tier to ADR-0047's USER-DEFINED uncatalogued
// verbatim path: same IR shape (ir.VerbatimType), same downstream
// pipeline, different dispatch point in translateType. The unit-test
// matrix mirrors ADR-0047's verbatim_tier_test.go matrix so a future
// reviewer can audit both tiers against one consistent shape.

// TestTranslateType_CoreVerbatim_Allowlist covers every Stage 1 type
// name in coreVerbatimEligibleTypes. Eligible + non-empty FormatType
// → ir.VerbatimType{Definition: FormatType}. Family grouping mirrors
// the ADR's allowlist scope so a reviewer can re-derive the matrix.
func TestTranslateType_CoreVerbatim_Allowlist(t *testing.T) {
	families := map[string][]string{
		"FTS family (catalog Bug 17 consolidated)": {
			"tsvector", "tsquery",
		},
		"range family": {
			"int4range", "int8range", "numrange",
			"tsrange", "tstzrange", "daterange",
		},
		"multirange family (PG 14+)": {
			"int4multirange", "int8multirange", "nummultirange",
			"tsmultirange", "tstzmultirange", "datemultirange",
		},
	}
	for family, dts := range families {
		family, dts := family, dts
		t.Run(family, func(t *testing.T) {
			for _, dt := range dts {
				dt := dt
				t.Run(dt, func(t *testing.T) {
					got, err := translateType(columnMeta{
						DataType:         dt,
						UDTName:          dt,
						FormatType:       dt,
						VerbatimEligible: true,
					})
					if err != nil {
						t.Fatalf("%s eligible: unexpected error: %v", dt, err)
					}
					v, ok := got.(ir.VerbatimType)
					if !ok {
						t.Fatalf("%s: got %T; want ir.VerbatimType", dt, got)
					}
					if v.Definition != dt {
						t.Errorf("%s: Definition = %q; want %q",
							dt, v.Definition, dt)
					}
				})
			}
		})
	}
}

// TestTranslateType_CoreVerbatim_RefusedWhenNotEligible pins the
// cross-engine load-bearing safety: with VerbatimEligible=false the
// same Stage 1 types still loud-refuse (no MySQL-portable form). The
// schema reader sets VerbatimEligible only on a same-engine-PG run, so
// this is the cross-engine path's runtime shape.
func TestTranslateType_CoreVerbatim_RefusedWhenNotEligible(t *testing.T) {
	// One representative per family is sufficient — the allowlist gates
	// the dispatch identically; the per-type behaviour comes from the
	// VerbatimEligible flag, not the type name.
	for _, dt := range []string{"tsvector", "int8range", "tsmultirange"} {
		dt := dt
		t.Run(dt, func(t *testing.T) {
			_, err := translateType(columnMeta{
				DataType:   dt,
				UDTName:    dt,
				FormatType: dt,
				// VerbatimEligible deliberately false.
			})
			if err == nil {
				t.Fatalf("%s not eligible: expected loud refusal, got nil "+
					"(cross-engine safety would be weakened)", dt)
			}
		})
	}
}

// TestTranslateType_CoreVerbatim_EmptyFormatTypeIsLoudBug pins the
// sluice-bug-not-silent-loss path: a Stage 1 type with VerbatimEligible
// but an empty FormatType is a schema-reader bug (format_type is
// populated unconditionally in populateColumns); surface it loudly
// rather than emitting a column with no type spelling.
func TestTranslateType_CoreVerbatim_EmptyFormatTypeIsLoudBug(t *testing.T) {
	_, err := translateType(columnMeta{
		DataType:         "int8range",
		UDTName:          "int8range",
		FormatType:       "",
		VerbatimEligible: true,
	})
	if err == nil {
		t.Fatal("expected loud sluice-bug error when FormatType is empty; got nil")
	}
}

// TestTranslateType_CoreVerbatim_NonAllowlistedRefuses pins the
// loud-failure tenet's spirit: a core type that is NOT on the
// allowlist still refuses, even with VerbatimEligible=true. This is
// the load-bearing rejection of "default fall-through" — a future PG
// version's new core type does NOT silently reach the verbatim path
// without an ADR review.
func TestTranslateType_CoreVerbatim_NonAllowlistedRefuses(t *testing.T) {
	// Stage 2 candidates per ADR-0051 — explicitly NOT in the allowlist.
	// Each is documented in the ADR as needing per-type validation.
	for _, dt := range []string{"xml", "money", "pg_lsn", "txid_snapshot", "pg_snapshot"} {
		dt := dt
		t.Run(dt, func(t *testing.T) {
			_, err := translateType(columnMeta{
				DataType:         dt,
				UDTName:          dt,
				FormatType:       dt,
				VerbatimEligible: true, // eligible, but NOT allowlisted
			})
			if err == nil {
				t.Fatalf("%s: not in allowlist → expected loud refusal, got nil "+
					"(allowlist would be silently broadened)", dt)
			}
		})
	}
}

// TestCoreVerbatimEligibleTypes_AllowlistShape is a structural pin:
// adding a type later should be an additive one-line change; removing
// one (without an ADR update) would be a stealth scope reduction. This
// test fixes the Stage 1 membership exactly so review notices.
func TestCoreVerbatimEligibleTypes_AllowlistShape(t *testing.T) {
	stage1 := []string{
		"tsvector", "tsquery",
		"int4range", "int8range", "numrange",
		"tsrange", "tstzrange", "daterange",
		"int4multirange", "int8multirange", "nummultirange",
		"tsmultirange", "tstzmultirange", "datemultirange",
	}
	if got, want := len(coreVerbatimEligibleTypes), len(stage1); got != want {
		t.Errorf("allowlist length = %d; want %d (Stage 1 per ADR-0051; "+
			"if you ADDED a type, update this pin AND ADR-0051 §Stage 1)",
			got, want)
	}
	for _, dt := range stage1 {
		if !coreVerbatimEligibleTypes[dt] {
			t.Errorf("Stage 1 allowlist missing %q", dt)
		}
	}
}
