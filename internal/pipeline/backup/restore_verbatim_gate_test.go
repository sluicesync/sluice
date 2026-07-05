// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

import (
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

func verbatimSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Schema: "public",
		Name:   "docs",
		Columns: []*ir.Column{
			{Name: "id", Type: ir.Integer{Width: 64}},
			{Name: "path", Type: ir.VerbatimType{Definition: "ltree"}},
			{Name: "loc", Type: ir.VerbatimType{Definition: "cube"}},
		},
	}}}
}

func nonVerbatimSchema() *ir.Schema {
	return &ir.Schema{Tables: []*ir.Table{{
		Name:    "users",
		Columns: []*ir.Column{{Name: "id", Type: ir.Integer{Width: 64}}},
	}}}
}

// TestVerbatimExtensionColumnsIn pins the marker derivation: only
// ir.VerbatimType columns are reported, schema-qualified, sorted, and
// the common (no verbatim) case yields nil so the marker stays absent.
func TestVerbatimExtensionColumnsIn(t *testing.T) {
	got := lineage.VerbatimExtensionColumnsIn(verbatimSchema())
	want := []string{"public.docs.loc", "public.docs.path"}
	if len(got) != len(want) {
		t.Fatalf("got %v; want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got[%d] = %q; want %q", i, got[i], want[i])
		}
	}
	if lineage.VerbatimExtensionColumnsIn(nonVerbatimSchema()) != nil {
		t.Error("non-verbatim schema must yield nil (marker absent in common case)")
	}
	if lineage.VerbatimExtensionColumnsIn(nil) != nil {
		t.Error("nil schema must yield nil")
	}
}

// TestRefuseVerbatimRestoreToNonPG is the load-bearing safety-pin: a
// verbatim-marked lineage restored to MySQL refuses LOUDLY; to PG it
// passes; an unmarked lineage is unaffected on any target (legacy /
// non-verbatim backups keep working).
func TestRefuseVerbatimRestoreToNonPG(t *testing.T) {
	marked := &lineage.Catalog{Segments: []lineage.Segment{{
		SegmentID:                "seg0",
		VerbatimExtensionColumns: []string{"public.docs.path", "public.docs.loc"},
	}}}
	unmarked := &lineage.Catalog{Segments: []lineage.Segment{{SegmentID: "seg0"}}}

	t.Run("marked → mysql refuses loudly", func(t *testing.T) {
		err := refuseVerbatimRestoreToNonPG(marked, capsEngine{name: "mysql", caps: capsMySQL})
		if err == nil {
			t.Fatal("expected loud refusal restoring verbatim-marked backup to mysql; got nil")
		}
		for _, want := range []string{"PG-restore-only", "public.docs.path", "mysql", "ADR-0047"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("err = %v; want substring %q", err, want)
			}
		}
	})

	t.Run("marked → planetscale refuses loudly", func(t *testing.T) {
		if err := refuseVerbatimRestoreToNonPG(marked, capsEngine{name: "planetscale", caps: capsTxKiller}); err == nil {
			t.Fatal("expected refusal restoring verbatim-marked backup to planetscale; got nil")
		}
	})

	t.Run("marked → postgres OK", func(t *testing.T) {
		if err := refuseVerbatimRestoreToNonPG(marked, capsEngine{name: "postgres", caps: capsSlotPG}); err != nil {
			t.Errorf("verbatim-marked backup → postgres should be OK; got %v", err)
		}
	})

	t.Run("unmarked → mysql OK (legacy/non-verbatim unaffected)", func(t *testing.T) {
		if err := refuseVerbatimRestoreToNonPG(unmarked, capsEngine{name: "mysql", caps: capsMySQL}); err != nil {
			t.Errorf("unmarked lineage → mysql should be OK; got %v", err)
		}
	})

	t.Run("nil catalog OK", func(t *testing.T) {
		if err := refuseVerbatimRestoreToNonPG(nil, capsEngine{name: "mysql", caps: capsMySQL}); err != nil {
			t.Errorf("nil catalog should be OK; got %v", err)
		}
	})

	t.Run("multi-segment: any marked segment trips the gate", func(t *testing.T) {
		multi := &lineage.Catalog{Segments: []lineage.Segment{
			{SegmentID: "seg0"},
			{SegmentID: "seg1", VerbatimExtensionColumns: []string{"public.t.c"}},
		}}
		if err := refuseVerbatimRestoreToNonPG(multi, capsEngine{name: "mysql", caps: capsMySQL}); err == nil {
			t.Fatal("a verbatim-marked LATER segment must still trip the gate; got nil")
		}
	})
}

// TestRefuseVerbatimManifestRestoreToNonPG covers the single-manifest
// path counterpart (gates on the manifest schema directly).
func TestRefuseVerbatimManifestRestoreToNonPG(t *testing.T) {
	if err := refuseVerbatimManifestRestoreToNonPG(verbatimSchema(), capsEngine{name: "mysql", caps: capsMySQL}); err == nil {
		t.Fatal("single-manifest verbatim → mysql must refuse; got nil")
	}
	if err := refuseVerbatimManifestRestoreToNonPG(verbatimSchema(), capsEngine{name: "postgres", caps: capsSlotPG}); err != nil {
		t.Errorf("single-manifest verbatim → postgres OK; got %v", err)
	}
	if err := refuseVerbatimManifestRestoreToNonPG(nonVerbatimSchema(), capsEngine{name: "mysql", caps: capsMySQL}); err != nil {
		t.Errorf("single-manifest non-verbatim → mysql OK (unaffected); got %v", err)
	}
}

// TestLineageSegment_HasVerbatimExtensionColumns pins the marker
// predicate.
func TestLineageSegment_HasVerbatimExtensionColumns(t *testing.T) {
	if (&lineage.Segment{}).HasVerbatimExtensionColumns() {
		t.Error("empty segment must not report verbatim columns")
	}
	if !(&lineage.Segment{VerbatimExtensionColumns: []string{"a.b.c"}}).HasVerbatimExtensionColumns() {
		t.Error("segment with recorded verbatim columns must report true")
	}
}
