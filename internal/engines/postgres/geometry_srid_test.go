// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// Audit 2026-08-05 C-14: the per-row geometry SRID guard, pinned as a
// MATRIX rather than a representative — see the MySQL sibling
// (TestDecodeMySQLGeometry_SRIDFamilyMatrix) for why the diagonal is the
// one case a single test would have covered and the only case the
// pre-fix code got right.
//
// Two extra axes matter on this engine and not the other:
//
//   - The INPUT SHAPE. A geometry value reaches the decoder as a hex-EWKB
//     string (cold-start scan), hex-EWKB []byte (pgoutput text-format
//     CDC), or raw binary EWKB. All three must reach the same verdict —
//     a guard on one lane and not the others is Bug 74's shape again.
//   - The DOMAIN wrapper. `CREATE DOMAIN d AS geometry(POINT,4326)` is a
//     real column type here, decodeValue recurses through it, and its
//     column SRID came back as 0 until the schema reader learned to read
//     a domain's typmod (addDomainGeometryColumnInfo). That made the
//     domain lane the one that could never match its own values.

import (
	"encoding/hex"
	"errors"
	"strconv"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// geomSRIDMatrixAxis is the matrix axis: PostGIS's no-SRS sentinel plus
// WGS 84 and Web Mercator — the pair operators actually mix, and the pair
// whose confusion moves a point by kilometres.
var geomSRIDMatrixAxis = []int{0, 4326, 3857}

// pgGeomEWKB builds a PostGIS EWKB POINT at the given SRID. SRID 0 is
// spelled as PostGIS spells it: raw WKB with no SRID-present flag.
func pgGeomEWKB(t *testing.T, srid int) []byte {
	t.Helper()
	if srid == 0 {
		return wkbPointLE()
	}
	ewkb, err := wkbToEWKB(wkbPointLE(), uint32(srid))
	if err != nil {
		t.Fatalf("wkbToEWKB(%d): %v", srid, err)
	}
	return ewkb
}

// TestDecodePGGeometry_SRIDFamilyMatrix walks column SRID × row SRID ×
// input shape × {bare geometry, DOMAIN over geometry}.
func TestDecodePGGeometry_SRIDFamilyMatrix(t *testing.T) {
	shapes := []struct {
		name string
		wrap func(ewkb []byte) any
	}{
		{"hex-string (cold-start scan)", func(e []byte) any { return hex.EncodeToString(e) }},
		{"hex-bytes (pgoutput CDC)", func(e []byte) any { return []byte(hex.EncodeToString(e)) }},
		{"raw EWKB bytes", func(e []byte) any { return e }},
	}
	columnTypes := []struct {
		name string
		typ  func(srid int) ir.Type
	}{
		{"geometry", func(srid int) ir.Type { return ir.Geometry{SRID: srid} }},
		{"DOMAIN over geometry", func(srid int) ir.Type {
			return ir.Domain{Name: "d_geom", BaseType: ir.Geometry{SRID: srid}}
		}},
	}

	for _, ct := range columnTypes {
		for _, shape := range shapes {
			for _, colSRID := range geomSRIDMatrixAxis {
				for _, rowSRID := range geomSRIDMatrixAxis {
					name := ct.name + "/" + shape.name + "/col" +
						strconv.Itoa(colSRID) + "/row" + strconv.Itoa(rowSRID)
					t.Run(name, func(t *testing.T) {
						raw := shape.wrap(pgGeomEWKB(t, rowSRID))
						got, err := decodeValueFromBinary(raw, ct.typ(colSRID))
						if colSRID == rowSRID {
							if err != nil {
								t.Fatalf("matching SRIDs must decode: %v", err)
							}
							b, ok := got.([]byte)
							if !ok || len(b) != len(wkbPointLE()) {
								t.Fatalf("got %T (len %d); want the bare %d-byte WKB",
									got, len(b), len(wkbPointLE()))
							}
							return
						}
						if err == nil {
							t.Fatalf("column SRID %d vs row SRID %d decoded to %v — "+
								"the row's SRID was silently dropped", colSRID, rowSRID, got)
						}
						if !errors.Is(err, ir.ErrGeometryRowSRIDMismatch) {
							t.Errorf("error %v does not wrap ir.ErrGeometryRowSRIDMismatch", err)
						}
					})
				}
			}
		}
	}
}

// TestEWKBSRID reads the SRID accessor directly, including the two shapes
// that must report 0 rather than guess: a value with no SRID-present flag
// (PostGIS's spelling of "no spatial reference"), and a truncated one
// (whose real diagnosis belongs to ewkbToWKB, so reporting a bogus SRID
// here would give one bad value two contradictory error messages).
func TestEWKBSRID(t *testing.T) {
	if got := ewkbSRID(ewkbPointLE()); got != 4326 {
		t.Errorf("little-endian EWKB SRID = %d; want 4326", got)
	}
	if got := ewkbSRID(ewkbPointBE()); got != 4326 {
		t.Errorf("big-endian EWKB SRID = %d; want 4326", got)
	}
	if got := ewkbSRID(wkbPointLE()); got != 0 {
		t.Errorf("raw WKB (no SRID flag) SRID = %d; want 0", got)
	}
	if got := ewkbSRID([]byte{0x01, 0x01, 0x00}); got != 0 {
		t.Errorf("truncated value SRID = %d; want 0", got)
	}
	if got := ewkbSRID(nil); got != 0 {
		t.Errorf("nil SRID = %d; want 0", got)
	}
}
