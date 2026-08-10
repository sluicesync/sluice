// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package diff

import (
	"fmt"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// The geometry-SRID axis of the existing-target shape gate, pinned
// because the protection it provides is INCIDENTAL and was unpinned.
//
// # What this defends and why it needed its own file
//
// A geometry value's SRID is what makes a coordinate pair name a place,
// and sluice carries it per COLUMN, never per row: every reader strips
// the value's own framing and every writer re-frames from the column
// ([ir.CheckGeometryRowSRID]). On the WRITE side that means the SRID a
// row lands with is the SRID the TARGET column declares — so a target
// column declaring a different SRID than the source's silently
// re-stamps every row, and MySQL/PostGIS both accept the result without
// complaint (measured: an undeclared MySQL 8.4 `POINT` and an
// unconstrained PostGIS `geometry` each hold ST_SRID 4326 happily, so
// nothing downstream refuses it either).
//
// The thing that stops that reaching an operator is this gate:
// [TableColumnShape] compares the source-derived intended column
// against the pre-existing target column, and `migrate` and the `sync`
// cold start both refuse the run when they differ
// (internal/pipeline/migrate_existing_tables.go). It catches the SRID
// divergence — but only as a side effect of [ir.Geometry.String]
// choosing to render the SRID. Nothing in this package or the gate's
// own tests mentioned SRID before this file, so a rendering change made
// for unrelated reasons could have reopened a silent-value-alteration
// door with every test green.
//
// # Why the matrix, rather than one `POINT SRID 4326` case
//
// The Bug 74 discipline: the render dispatches on a type FAMILY — the
// geometry/geography flag, the subtype, and the Z/M dimension suffixes
// each take their own branch in [ir.Geometry.String] — and an SRID
// dropped from ONE of those branches is exactly the shape a single
// representative pin cannot see. So every branch is exercised against
// every SRID-difference direction.
//
// # What this gate does NOT reach, stated rather than implied
//
// It pins the COMPARISON only. That the comparison is consulted before
// any data moves is pinned end-to-end, on a real server, by
// TestStreamer_GeometrySRIDTargetDivergence_RefusedBeforeAnyDataMoves
// (internal/pipeline). And neither gate covers a target whose geometry
// column is ALTERed AFTER cold start: nothing re-runs this comparison
// on a warm resume, so that divergence is not detected — see the wart
// note on the MySQL writer's geometry arm (row_writer.go).

// geometryShapeFamilies enumerates every branch [ir.Geometry.String]
// can take, so the SRID assertions below run against all of them.
func geometryShapeFamilies() []ir.Geometry {
	subtypes := []ir.GeometrySubtype{
		ir.GeometryUnspecified,
		ir.GeometryPoint,
		ir.GeometryLineString,
		ir.GeometryPolygon,
		ir.GeometryMultiPoint,
		ir.GeometryMultiLineString,
		ir.GeometryMultiPolygon,
		ir.GeometryCollection,
	}
	dims := []struct{ z, m bool }{{false, false}, {true, false}, {false, true}, {true, true}}

	out := make([]ir.Geometry, 0, 2*len(subtypes)*len(dims))
	for _, geography := range []bool{false, true} {
		for _, st := range subtypes {
			for _, d := range dims {
				out = append(out, ir.Geometry{
					Subtype:     st,
					IsGeography: geography,
					HasZ:        d.z,
					HasM:        d.m,
				})
			}
		}
	}
	return out
}

// geometryFamilyName labels a matrix cell in failure output.
func geometryFamilyName(g ir.Geometry) string {
	kind := "geometry"
	if g.IsGeography {
		kind = "geography"
	}
	return fmt.Sprintf("%s/%s/z=%t/m=%t", kind, g.Subtype, g.HasZ, g.HasM)
}

// withSRID returns g carrying the given SRID.
func withSRID(g ir.Geometry, srid int) ir.Geometry {
	g.SRID = srid
	return g
}

// TestTableColumnShape_GeometrySRIDDivergenceRefused pins that an
// SRID-ONLY difference is a mismatch on every geometry family, in every
// direction that matters:
//
//   - declared → undeclared (4326 vs 0) — the shape the write side
//     re-stamps to 0, the silent-loss direction;
//   - undeclared → declared (0 vs 4326) — the target constrains what
//     the source does not, so MySQL/PostGIS would reject mid-copy;
//   - declared → differently declared (4326 vs 3857) — geographic vs
//     projected, the pair whose coordinates are not even comparable.
func TestTableColumnShape_GeometrySRIDDivergenceRefused(t *testing.T) {
	directions := []struct {
		name           string
		expSRID        int
		actSRID        int
		wantInExpected string
		wantInActual   string
	}{
		{"declared-vs-undeclared", 4326, 0, "SRID=4326", ""},
		{"undeclared-vs-declared", 0, 4326, "", "SRID=4326"},
		{"declared-vs-other", 4326, 3857, "SRID=4326", "SRID=3857"},
	}

	families := geometryShapeFamilies()
	if len(families) < 32 {
		t.Fatalf("matrix collapsed to %d families — the enumeration is broken, not the tree", len(families))
	}

	for _, g := range families {
		for _, d := range directions {
			name := geometryFamilyName(g) + "/" + d.name
			t.Run(name, func(t *testing.T) {
				expected := shapeTable(
					"t", intPK("id"),
					&ir.Column{Name: "id", Type: ir.Integer{Width: 64}},
					&ir.Column{Name: "loc", Type: withSRID(g, d.expSRID)},
				)
				actual := shapeTable(
					"t", intPK("id"),
					&ir.Column{Name: "id", Type: ir.Integer{Width: 64}},
					&ir.Column{Name: "loc", Type: withSRID(g, d.actSRID)},
				)
				got := TableColumnShape(expected, actual)
				if len(got) != 1 || got[0].Column != "loc" {
					t.Fatalf("SRID %d vs %d: got %+v; want exactly the loc mismatch — an SRID "+
						"difference the gate does not see lets the target silently re-stamp "+
						"every row of this column", d.expSRID, d.actSRID, got)
				}
				// The rendered sides are what the refusal message shows the
				// operator; an SRID it does not name cannot be acted on.
				if d.wantInExpected != "" && !strings.Contains(got[0].Expected, d.wantInExpected) {
					t.Errorf("expected side %q does not name %q", got[0].Expected, d.wantInExpected)
				}
				if d.wantInActual != "" && !strings.Contains(got[0].Actual, d.wantInActual) {
					t.Errorf("actual side %q does not name %q", got[0].Actual, d.wantInActual)
				}
			})
		}
	}
}

// TestTableColumnShape_GeometrySRIDEqualAcceptsEveryFamily is the
// anti-vacuity half: the matrix above proves the gate refuses, and this
// proves it is not simply refusing every geometry column. A target
// sluice itself created carries the source's SRID (both engines' DDL
// emitters render it), so the equal case is the common one and a false
// refusal here would break every re-run over an existing target.
func TestTableColumnShape_GeometrySRIDEqualAcceptsEveryFamily(t *testing.T) {
	for _, g := range geometryShapeFamilies() {
		for _, srid := range []int{0, 4326, 3857} {
			name := fmt.Sprintf("%s/srid=%d", geometryFamilyName(g), srid)
			t.Run(name, func(t *testing.T) {
				col := func() *ir.Column {
					return &ir.Column{Name: "loc", Type: withSRID(g, srid)}
				}
				expected := shapeTable("t", intPK("id"),
					&ir.Column{Name: "id", Type: ir.Integer{Width: 64}}, col())
				actual := shapeTable("t", intPK("id"),
					&ir.Column{Name: "id", Type: ir.Integer{Width: 64}}, col())
				if got := TableColumnShape(expected, actual); len(got) != 0 {
					t.Errorf("identical geometry columns compared unequal: %+v", got)
				}
			})
		}
	}
}
