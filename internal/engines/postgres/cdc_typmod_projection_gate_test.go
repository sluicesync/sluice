// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"strings"
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

// TestSessionTZSwapGate_EveryZoneSiblingPair is the SWAP axis the
// TYPMOD-PROJECTION-GATE above deliberately does not carry: that gate
// enumerates typmod deltas WITHIN one OID, and the OID-SWAP class sat next
// to it unrostered — which is how `time[]`⇄`timetz[]` lost its refusal
// inside the v0.134.0 delta with nothing going red (audit 2026-08-31 SL-3).
//
// The universe is derived from the PROJECTIONS, not from
// [sessionTZSwapPair]: two OIDs are a session-TZ swap pair iff their
// projected IR types describe the same temporal family and disagree on
// whether the value carries a zone. That derivation is independent of the
// predicate under test — a pair the predicate forgot still appears here —
// which is the point. Deriving the universe from the predicate would make
// the gate self-referential (the frozen-golden defect).
//
// Scope, stated so it cannot be read as broader: this reaches the PG
// pgoutput lane only. The MySQL binlog / VStream lanes declare their own
// pairs and are covered by TestSessionGUCCastRoster_EveryCDCLane
// (internal/docsync) plus mysql's own TestSessionTZSwapPair cells.
//
// Both directions are load-bearing under mutation: drop an array OID from
// the predicate's unwrap and the array cells redden; broaden the predicate
// to "any two temporal OIDs" and the not-a-pair half reddens.
func TestSessionTZSwapGate_EveryZoneSiblingPair(t *testing.T) {
	// Scalar universe: PG's temporal built-ins inside oidToType's domain,
	// listed from pg_catalog (date / time / timetz / timestamp /
	// timestamptz / interval) rather than derived from the projection, so a
	// projection that stopped resolving one of them shows up as a failure
	// here instead of silently shrinking the universe.
	scalars := []uint32{
		pgtype.DateOID, pgtype.TimeOID, pgtype.TimetzOID,
		pgtype.TimestampOID, pgtype.TimestamptzOID, pgtype.IntervalOID,
	}
	if len(scalars) != 6 {
		t.Fatalf("temporal scalar universe holds %d OIDs; want PG's six", len(scalars))
	}
	inScalars := make(map[uint32]bool, len(scalars))
	for _, oid := range scalars {
		inScalars[oid] = true
	}
	// Array half derived mechanically from the production element map — the
	// same source sessionTZSwapPair unwraps through, so a family added
	// there joins this roster automatically.
	universe := append([]uint32(nil), scalars...)
	for arrOID, elemOID := range pgArrayElementOID {
		if inScalars[elemOID] {
			universe = append(universe, arrOID)
		}
	}
	if len(universe) < 10 {
		t.Fatalf("temporal universe holds %d OIDs (%v); floor 10 (six scalars + at least the four temporal arrays) — the derivation went vacuous",
			len(universe), universe)
	}

	type zoneClassResult struct {
		family string
		zoned  bool
		ok     bool
	}
	// zoneClass renders a projected IR type as (family, carries-a-zone).
	// ok=false for anything outside the temporal families this class covers.
	var zoneClass func(ir.Type) zoneClassResult
	zoneClass = func(typ ir.Type) zoneClassResult {
		switch v := typ.(type) {
		case ir.Date:
			return zoneClassResult{family: "date", ok: true}
		case ir.Interval:
			return zoneClassResult{family: "interval", ok: true}
		case ir.Time:
			return zoneClassResult{family: "time-of-day", zoned: v.WithTimeZone, ok: true}
		case ir.DateTime:
			return zoneClassResult{family: "timestamp", ok: true}
		case ir.Timestamp:
			return zoneClassResult{family: "timestamp", zoned: v.WithTimeZone, ok: true}
		case ir.Array:
			inner := zoneClass(v.Element)
			inner.family += "[]"
			return inner
		}
		return zoneClassResult{}
	}

	classOf := make(map[uint32]zoneClassResult, len(universe))
	for _, oid := range universe {
		typ, err := oidToType(oid, -1)
		if err != nil {
			t.Fatalf("oidToType(%d, -1): %v", oid, err)
		}
		class := zoneClass(typ)
		if !class.ok {
			t.Fatalf("OID %d projects %T, which zoneClass does not recognise — a temporal family reached oidToType without anyone deciding whether it carries a zone, and that decision is exactly what the swap refusal keys on", oid, typ)
		}
		classOf[oid] = class
	}

	// The derived pair set: same family, disagreeing on the zone.
	wantSwap := map[[2]uint32]bool{}
	for _, a := range universe {
		for _, b := range universe {
			if a == b {
				continue
			}
			if classOf[a].family == classOf[b].family && classOf[a].zoned != classOf[b].zoned {
				wantSwap[[2]uint32{a, b}] = true
			}
		}
	}
	// Anti-vacuity floor: four unordered pairs — time/timetz,
	// timestamp/timestamptz, each scalar and array — so eight ordered.
	if len(wantSwap) != 8 {
		t.Fatalf("derived %d ordered zone-sibling pairs; want 8 (time⇄timetz, timestamp⇄timestamptz, each scalar and array)", len(wantSwap))
	}

	for _, a := range universe {
		for _, b := range universe {
			if a == b {
				continue
			}
			pair, matched := sessionTZSwapPair(
				relationColumn{Name: "v", OID: a, TypeMod: -1},
				relationColumn{Name: "v", OID: b, TypeMod: -1},
			)
			want := wantSwap[[2]uint32{a, b}]
			switch {
			case want && !matched:
				t.Errorf("OID %d→%d is a zone-sibling swap (%s, zoned %v→%v) but sessionTZSwapPair does not match it — the SOURCE ALTER's session TimeZone decided the cast and a forwarded ALTER re-casts pre-existing target rows against the TARGET's",
					a, b, classOf[a].family, classOf[a].zoned, classOf[b].zoned)
			case !want && matched:
				t.Errorf("OID %d→%d is NOT a zone-sibling swap (%s/%v → %s/%v) but sessionTZSwapPair matched it as %q — the predicate is broader than the class and would false-refuse a forwardable ALTER",
					a, b, classOf[a].family, classOf[a].zoned, classOf[b].family, classOf[b].zoned, pair)
			case want && pair == "":
				t.Errorf("OID %d→%d matched with an empty pair name; the refusal text names the pair", a, b)
			}

			// Behavioural half: matching is only worth something if the
			// reader actually refuses, under BOTH modes.
			if !want {
				continue
			}
			prev := &relationCacheEntry{Schema: "public", Name: "t", Columns: []relationColumn{typedCol(t, "v", a, -1)}}
			curr := &relationCacheEntry{Schema: "public", Name: "t", Columns: []relationColumn{typedCol(t, "v", b, -1)}}
			relations := map[uint32]*relationCacheEntry{16400: prev}
			for _, forward := range []bool{true, false} {
				err := checkSchemaRace(relations, 16400, curr, forward)
				if err == nil {
					t.Errorf("OID %d→%d (%s) passed checkSchemaRace with forward=%v; the session-TZ swap must refuse under both modes", a, b, pair, forward)
					continue
				}
				if forward && !strings.Contains(err.Error(), "TimeZone") {
					t.Errorf("OID %d→%d refused without naming the TimeZone mechanism: %v", a, b, err)
				}
			}
		}
	}
}

// TestSessionTZSwapPair_ArrayOIDsGroundTruthed pins the four array OIDs the
// unwrap depends on against pg_catalog's published values. They are not
// derivable (an array OID is NOT element+1 — macaddr is 829 while _macaddr
// is 1040), and the whole array arm goes silently inert if
// pgArrayElementOID ever maps one of them to the wrong element. The
// live-server counterpart is
// TestCDCSchemaForward_SessionTZArraySwapRefuses_PG (integration), which
// reads these same four OIDs out of a real pg_type.
func TestSessionTZSwapPair_ArrayOIDsGroundTruthed(t *testing.T) {
	for _, tc := range []struct {
		name        string
		arrOID      uint32
		wantElemOID uint32
	}{
		{"_time", 1183, pgtype.TimeOID},
		{"_timetz", 1270, pgtype.TimetzOID},
		{"_timestamp", 1115, pgtype.TimestampOID},
		{"_timestamptz", 1185, pgtype.TimestamptzOID},
	} {
		elem, ok := pgArrayElementOID[tc.arrOID]
		if !ok {
			t.Errorf("pgArrayElementOID has no entry for %s (OID %d) — the array session-TZ arm is inert for it", tc.name, tc.arrOID)
			continue
		}
		if elem != tc.wantElemOID {
			t.Errorf("pgArrayElementOID[%s=%d] = %d; want %d", tc.name, tc.arrOID, elem, tc.wantElemOID)
		}
	}
}
