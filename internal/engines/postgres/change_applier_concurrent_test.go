// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// Unit pins for the Postgres side of the ADR-0105 [laneapply.LaneApplier]
// seam that are testable WITHOUT a database: the guarded metadata-cache
// accessors and the PK-routing decision (which, with the PK cache
// pre-seeded, never touches the DB). The DB-backed exactly-once / value-
// fidelity / warm-resume pins live in change_applier_concurrent_integration_test.go.

// newCacheTestApplier builds a ChangeApplier with the metadata-cache maps
// allocated (as OpenChangeApplier does) but no DB — enough to exercise the
// guarded accessors and the cache-seeded routing decision.
func newCacheTestApplier() *ChangeApplier {
	return &ChangeApplier{
		schema:           "public",
		controlSchema:    "public",
		pkCache:          make(map[string][]string),
		conflictKeyCache: make(map[string][]string),
		colTypeCache:     make(map[string]map[string]*ir.Column),
		activeSchema:     make(map[string]activeSchemaVersion),
	}
}

// TestGuardedCacheAccessors_RoundTrip pins that each guarded accessor stores
// and reads back through cacheMu, and that markWarnedKeyless is a one-shot
// check-and-set. (A -race run of the integration suite is the authoritative
// proof the LOCKING is correct; this is the functional contract.)
func TestGuardedCacheAccessors_RoundTrip(t *testing.T) {
	a := newCacheTestApplier()
	const qn = "public.t"

	if _, ok := a.cachedPK(qn); ok {
		t.Fatal("cachedPK on empty cache returned ok=true")
	}
	a.storePK(qn, []string{"id"})
	if got, ok := a.cachedPK(qn); !ok || !reflect.DeepEqual(got, []string{"id"}) {
		t.Fatalf("cachedPK = %v ok=%v; want [id] true", got, ok)
	}

	cols := map[string]*ir.Column{"id": {Name: "id", Type: ir.Integer{Width: 64}}}
	a.storeColTypes(qn, cols)
	if got, ok := a.cachedColTypes(qn); !ok || !reflect.DeepEqual(got, cols) {
		t.Fatalf("cachedColTypes round-trip mismatch: %v ok=%v", got, ok)
	}

	a.storeConflictKey(qn, []string{"id"})
	if got, ok := a.cachedConflictKey(qn); !ok || !reflect.DeepEqual(got, []string{"id"}) {
		t.Fatalf("cachedConflictKey = %v ok=%v; want [id] true", got, ok)
	}

	if a.tableSchemaDirty(qn) {
		t.Fatal("tableSchemaDirty true before any invalidation")
	}
	if !a.markWarnedKeyless(qn) {
		t.Fatal("first markWarnedKeyless returned false; want true (this call recorded it)")
	}
	if a.markWarnedKeyless(qn) {
		t.Fatal("second markWarnedKeyless returned true; want false (already recorded)")
	}

	// invalidateMetadataCaches drops the three per-table caches AND marks the
	// table schema-dirty — the SAME set the serial boundary invalidation drops.
	a.invalidateMetadataCaches(qn)
	if _, ok := a.cachedPK(qn); ok {
		t.Error("invalidateMetadataCaches left pkCache entry")
	}
	if _, ok := a.cachedColTypes(qn); ok {
		t.Error("invalidateMetadataCaches left colTypeCache entry")
	}
	if _, ok := a.cachedConflictKey(qn); ok {
		t.Error("invalidateMetadataCaches left conflictKeyCache entry")
	}
	if !a.tableSchemaDirty(qn) {
		t.Error("invalidateMetadataCaches did not mark the table schema-dirty")
	}
}

// TestPKValuesForRouting_Decision pins the lane-routing decision: a keyed row
// change routes (ok=true, qualified+PK values); a keyless table, a malformed
// change, and a PK-changing UPDATE all fall to the barrier path (ok=false).
// The PK cache is pre-seeded so pkForRedact never hits a DB.
func TestPKValuesForRouting_Decision(t *testing.T) {
	a := newCacheTestApplier()
	// Routed schema for a single-database run is the bound schema ("public");
	// seed both the keyed and keyless tables.
	a.storePK("public.keyed", []string{"id"})
	a.storePK("public.keyless", []string{}) // table exists, no PK

	la := &laneApplierAdapter{a: a, streamID: testStreamIDUnit}
	ctx := context.Background()

	t.Run("keyed insert routes", func(t *testing.T) {
		qn, vals, ok, err := la.PKValuesForRouting(ctx, ir.Insert{Schema: "public", Table: "keyed", Row: ir.Row{"id": int64(7), "v": "x"}})
		if err != nil || !ok {
			t.Fatalf("ok=%v err=%v; want ok=true", ok, err)
		}
		if qn != "public.keyed" {
			t.Errorf("qualified = %q; want public.keyed", qn)
		}
		if !reflect.DeepEqual(vals, []any{int64(7)}) {
			t.Errorf("pkVals = %v; want [7]", vals)
		}
	})

	t.Run("keyless table → barrier", func(t *testing.T) {
		_, _, ok, err := la.PKValuesForRouting(ctx, ir.Insert{Schema: "public", Table: "keyless", Row: ir.Row{"v": "x"}})
		if err != nil || ok {
			t.Fatalf("ok=%v err=%v; want ok=false (keyless → barrier)", ok, err)
		}
	})

	t.Run("malformed (PK col absent) → barrier", func(t *testing.T) {
		_, _, ok, err := la.PKValuesForRouting(ctx, ir.Insert{Schema: "public", Table: "keyed", Row: ir.Row{"v": "x"}})
		if err != nil || ok {
			t.Fatalf("ok=%v err=%v; want ok=false (missing PK col → barrier)", ok, err)
		}
	})

	t.Run("PK-changing update → barrier", func(t *testing.T) {
		u := ir.Update{Schema: "public", Table: "keyed", Before: ir.Row{"id": int64(1)}, After: ir.Row{"id": int64(2)}}
		_, _, ok, err := la.PKValuesForRouting(ctx, u)
		if err != nil || ok {
			t.Fatalf("ok=%v err=%v; want ok=false (PK migration → barrier)", ok, err)
		}
	})

	t.Run("same-PK update routes", func(t *testing.T) {
		u := ir.Update{Schema: "public", Table: "keyed", Before: ir.Row{"id": int64(3), "v": "a"}, After: ir.Row{"id": int64(3), "v": "b"}}
		qn, vals, ok, err := la.PKValuesForRouting(ctx, u)
		if err != nil || !ok {
			t.Fatalf("ok=%v err=%v; want ok=true (same-PK update routes)", ok, err)
		}
		if qn != "public.keyed" || !reflect.DeepEqual(vals, []any{int64(3)}) {
			t.Errorf("qn=%q vals=%v; want public.keyed [3]", qn, vals)
		}
	})
}

// testStreamIDUnit is a fixed stream id for the unit pins (the integration
// suite's testStreamID lives behind the integration build tag).
const testStreamIDUnit = "unit-stream"
