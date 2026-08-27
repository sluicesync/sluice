// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
)

// TestTypmodProjectionGate_EveryTypmodFamily is the TYPMOD-PROJECTION-GATE
// enumeration (audit 2026-08-27 A2 / the VF-review filing it turns into
// code): for EVERY typmod-consuming family the wire can present, a typmod
// delta must either MOVE the projected IR signature — so the forward
// intercept sees a boundary and forwards the ALTER — or the family must sit
// on the documented refuses-under-both-modes list (ADR-0091 impl note),
// which checkSchemaRace enforces loudly under forward mode. This closes the
// class "typmod delta visible to the raw compare, invisible to the
// projected IR" by enumeration instead of discovery: a new (or refactored)
// family that silently drops its typmod from the projection lands in
// neither bucket and fails here.
//
// Scope of the universe, and its independent source: the scalar families
// are the PG built-in types with a typmod input function (pg_type.typmodin
// != 0) intersected with oidToType's domain — numeric, varchar, bpchar,
// bit, varbit, time, timetz, timestamp, timestamptz, interval. That is
// PG-catalog knowledge, deliberately NOT derived from oidToType itself
// (deriving "typmod-consuming" from whether the projection moves would make
// the gate self-referential — the fixture-from-post-change-values defect).
// The array universe IS derived mechanically: every entry of
// pgArrayElementOID whose element is a typmod-capable scalar (a
// varchar(10)[] column's wire typmod carries the element's encoding).
// Anti-vacuity floor: the scalar roster must hold all ten families and the
// derived array roster must be non-empty, so an accidental emptying cannot
// green the loop.
//
// Both directions are load-bearing under mutation:
//   - mutate a projection to drop its typmod (e.g. make the varchar arm
//     ignore typmod) → that family stops moving and is not on the list →
//     FAIL (the A2 defect shape, caught by enumeration);
//   - mutate checkSchemaRace to wave the list members through under
//     forward → the behavioural half FAILS (the refusal is pinned, not
//     just the classification).
//
// If a listed family ever STARTS moving (say array elements begin carrying
// typmods), this gate fails too — the correct outcome: the documented list
// and the ADR must shrink in the same change.
func TestTypmodProjectionGate_EveryTypmodFamily(t *testing.T) {
	numericTM := func(p, s int32) int32 { return ((p << 16) | s) + 4 }
	charTM := func(n int32) int32 { return n + 4 }
	intervalTM := func(p int32) int32 { return (0x7FFF << 16) | p }

	// Scalar universe: PG's typmod-capable built-ins (pg_type.typmodin != 0)
	// within oidToType's domain, each with a representative shrink pair in
	// its OWN encoding (the encodings differ per family — the Bug 74 reason
	// one representative proves nothing about the others).
	type tmPair struct{ from, to int32 }
	scalars := map[uint32]tmPair{
		pgtype.NumericOID:     {numericTM(10, 4), numericTM(10, 1)},
		pgtype.VarcharOID:     {charTM(20), charTM(10)},
		pgtype.BPCharOID:      {charTM(20), charTM(10)},
		pgtype.BitOID:         {8, 4},
		pgtype.VarbitOID:      {8, 4},
		pgtype.TimeOID:        {5, 3},
		pgtype.TimetzOID:      {5, 3},
		pgtype.TimestampOID:   {5, 3},
		pgtype.TimestamptzOID: {5, 3},
		pgtype.IntervalOID:    {intervalTM(6), intervalTM(3)},
	}
	if len(scalars) != 10 {
		t.Fatalf("scalar typmod-capable roster holds %d families; want 10 — the universe was edited without updating the floor", len(scalars))
	}

	// The documented refuses-under-both-modes list (ADR-0091 impl note,
	// "detected but cannot be forwarded"): interval's projection is the
	// empty ir.Interval{}, and every array element resolves at typmod -1 by
	// design. Everything else must move its projection.
	refusesUnderBoth := map[uint32]bool{pgtype.IntervalOID: true}

	// Array universe, derived from the production element map: any array
	// whose element family is typmod-capable can present a typmod delta on
	// the wire, and all of them project the element at -1 → all on the list.
	arrays := map[uint32]tmPair{}
	for arrOID, elemOID := range pgArrayElementOID {
		if pair, ok := scalars[elemOID]; ok {
			arrays[arrOID] = pair
			refusesUnderBoth[arrOID] = true
		}
	}
	if len(arrays) == 0 {
		t.Fatal("derived array roster is empty — pgArrayElementOID lost its typmod-capable element families (anti-vacuity floor)")
	}

	families := make(map[uint32]tmPair, len(scalars)+len(arrays))
	for oid, pair := range scalars {
		families[oid] = pair
	}
	for oid, pair := range arrays {
		families[oid] = pair
	}

	for oid, pair := range families {
		t.Run(fmt.Sprintf("oid_%d", oid), func(t *testing.T) {
			prev := &relationCacheEntry{
				Schema: "public", Name: "t",
				Columns: []relationColumn{typedCol(t, "v", oid, pair.from)},
			}
			curr := &relationCacheEntry{
				Schema: "public", Name: "t",
				Columns: []relationColumn{typedCol(t, "v", oid, pair.to)},
			}

			// Anti-vacuity: the raw compare must SEE the delta at all —
			// otherwise the "moved or listed" question below is about
			// nothing (this is what the pre-G3 classifier got wrong).
			if kind := classifyRelationChange(prev, curr).Kind; kind != relationChangeAlterColumnType {
				t.Fatalf("typmod delta %d→%d classified %v; want AlterColumnType", pair.from, pair.to, kind)
			}

			moved := !ir.SchemaSignatureOf(projectRelation(prev)).Equal(ir.SchemaSignatureOf(projectRelation(curr)))
			switch {
			case moved && refusesUnderBoth[oid]:
				t.Errorf("family OID %d moves its projected signature but sits on the documented refuse list — the projection now carries this typmod; shrink the list (and ADR-0091's impl note) in this change", oid)
			case !moved && !refusesUnderBoth[oid]:
				t.Errorf("family OID %d: typmod delta %d→%d does NOT move the projected signature and the family is not on the documented refuse list — the forward intercept would never see this ALTER and the source rewrite would silently diverge (the A2 shape)", oid, pair.from, pair.to)
			}

			// Behavioural half: the list is only honest if the reader
			// actually refuses those members under forward mode, and only
			// safe if the moving members still pass to the intercept.
			relations := map[uint32]*relationCacheEntry{16400: prev}
			err := checkSchemaRace(relations, 16400, curr, true)
			if refusesUnderBoth[oid] && err == nil {
				t.Errorf("family OID %d is on the refuse list but checkSchemaRace passed it under forward mode — the TYPMOD-PROJECTION-GATE refusal is not reaching it", oid)
			}
			if !refusesUnderBoth[oid] && err != nil {
				t.Errorf("family OID %d moves its projection and must forward; checkSchemaRace refused: %v", oid, err)
			}
		})
	}
}
