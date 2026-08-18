// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
)

// TestCheckGeometryRowSRID_KnownVersusUnknownColumn pins the distinction the
// sentinel exists for: a column that DECLARES 0 and a column nobody has read
// are different, and only the first may refuse.
//
// The failing direction is not hypothetical. The C-14 check shipped to the
// Postgres CDC lane comparing against a bare `ir.Geometry{}`, whose SRID 0 was
// never read from anything — so a `geometry(Point,4326)` column refused every
// row and wedged the stream on the first spatial DML. The PostGIS integration
// job caught it pre-release.
func TestCheckGeometryRowSRID_KnownVersusUnknownColumn(t *testing.T) {
	cases := []struct {
		name       string
		columnSRID int
		rowSRID    uint32
		wantErr    bool
	}{
		{"declared 4326, row 4326 — the ordinary declared case", 4326, 4326, false},
		{"declared 0, row 0 — no spatial reference anywhere", 0, 0, false},
		{
			name: "declared 0, row 4326 — the silent re-stamp C-14 exists to refuse",
			// An unconstrained PostGIS column accepts a per-row SRID and reports
			// srid 0 itself, so this really happens; the decoder strips the
			// framing and the writer re-frames from the column, landing valid
			// geometry in the wrong place.
			columnSRID: 0, rowSRID: 4326, wantErr: true,
		},
		{"declared 4326, row 3857 — mismatch in the other direction", 4326, 3857, true},
		{
			name: "UNKNOWN column, row 4326 — must NOT refuse",
			// The CDC regression in one line: pgoutput cannot carry the SRID, so
			// refusing here refuses every SRID-bearing row on the lane.
			columnSRID: ir.GeometrySRIDUnknown, rowSRID: 4326, wantErr: false,
		},
		{"UNKNOWN column, row 0", ir.GeometrySRIDUnknown, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ir.CheckGeometryRowSRID(tc.columnSRID, tc.rowSRID)
			if tc.wantErr && err == nil {
				t.Fatalf("CheckGeometryRowSRID(%d, %d) = nil; want a refusal", tc.columnSRID, tc.rowSRID)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("CheckGeometryRowSRID(%d, %d) = %v; want nil", tc.columnSRID, tc.rowSRID, err)
			}
			if tc.wantErr && !errors.Is(err, ir.ErrGeometryRowSRIDMismatch) {
				t.Errorf("refusal should wrap ErrGeometryRowSRIDMismatch; got %v", err)
			}
		})
	}
}

// TestBuildRelationCacheEntry_GeometryStartsUnknown pins the CDC lane's
// STARTING value, which is the half a checker test cannot see.
//
// CheckGeometryRowSRID can be perfectly correct and the lane still broken, if
// what reaches it is a zero that was never read. This asserts the relation
// cache hands over the sentinel, so the resolver — not a default — is what
// decides.
func TestBuildRelationCacheEntry_GeometryStartsUnknown(t *testing.T) {
	const geomOID = 18000
	entry, err := buildRelationCacheEntry(relationMessageWithGeometry(geomOID), geomOID, nil, nil)
	if err != nil {
		t.Fatalf("buildRelationCacheEntry: %v", err)
	}
	var found bool
	for _, c := range entry.Columns {
		g, ok := c.Type.(ir.Geometry)
		if !ok {
			continue
		}
		found = true
		if g.SRID != ir.GeometrySRIDUnknown {
			t.Errorf("column %q: SRID = %d; want ir.GeometrySRIDUnknown (%d). A 0 here asserts "+
				"'declares 0' about a column pgoutput never described, which refuses every row of a "+
				"geometry(Point,4326) table", c.Name, g.SRID, ir.GeometrySRIDUnknown)
		}
	}
	if !found {
		t.Fatal("no geometry column in the built entry; the fixture no longer exercises the path")
	}
}

// TestProjectRelation_ContainsTheSRIDSentinel pins the containment: the
// sentinel is for the decoder and must not escape into anything that renders
// DDL or compares shapes, where -1 is not a valid SRID.
func TestProjectRelation_ContainsTheSRIDSentinel(t *testing.T) {
	rel := &relationCacheEntry{
		Schema: "public", Name: "geo",
		Columns: []relationColumn{
			{Name: "g", Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: ir.GeometrySRIDUnknown}},
			{Name: "g2", Type: ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326}},
		},
	}
	tbl := projectRelation(rel)
	if got := tbl.Columns[0].Type.(ir.Geometry).SRID; got != 0 {
		t.Errorf("unknown SRID projected as %d; want 0 — schema-forward would emit geometry(Point,%d)", got, got)
	}
	if got := tbl.Columns[1].Type.(ir.Geometry).SRID; got != 4326 {
		t.Errorf("a resolved SRID must project unchanged; got %d", got)
	}
}

// relationMessageWithGeometry builds a minimal RelationMessage carrying one
// geometry column under the dynamic OID PostGIS assigns at CREATE EXTENSION.
func relationMessageWithGeometry(geomOID uint32) pglogrepl.RelationMessage {
	return pglogrepl.RelationMessage{
		RelationID:      16400,
		Namespace:       "public",
		RelationName:    "geo",
		ReplicaIdentity: 'd',
		ColumnNum:       2,
		Columns: []*pglogrepl.RelationMessageColumn{
			{Flags: 1, Name: "id", DataType: pgtype.Int8OID, TypeModifier: -1},
			{Flags: 0, Name: "g_pt", DataType: geomOID, TypeModifier: -1},
		},
	}
}
