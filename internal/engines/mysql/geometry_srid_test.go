// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

// Audit 2026-08-05 C-14: the per-row geometry SRID guard, pinned as a
// MATRIX rather than a representative.
//
// The Bug-74 reason to pin the class here: the guard's inputs are two
// independent SRIDs, and the only case the pre-fix code got right was the
// diagonal (row == column) — which is also the only case a single
// representative test would have exercised. Every off-diagonal cell was a
// silent re-stamp, and the 0-vs-nonzero cells in BOTH directions matter
// separately: SRID 0 is MySQL's "no spatial reference declared" sentinel,
// so (col 0, row 4326) is the common real-world loss and (col 4326, row 0)
// is the shape a mis-seeded source produces.

import (
	"encoding/binary"
	"errors"
	"strconv"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

// mysqlGeomBytes builds MySQL's on-wire geometry form for a POINT:
// `<srid uint32 LE><wkb>`.
func mysqlGeomBytes(srid uint32) []byte {
	wkb := []byte{
		0x01,                   // little-endian
		0x01, 0x00, 0x00, 0x00, // POINT
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // x = 2.0
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // y = 3.0
	}
	out := make([]byte, 4+len(wkb))
	binary.LittleEndian.PutUint32(out[:4], srid)
	copy(out[4:], wkb)
	return out
}

// geomSRIDMatrixAxis is the matrix axis: the no-SRS sentinel plus two real, distinct
// spatial references (WGS 84 and Web Mercator — the pair operators
// actually mix, and the pair whose confusion moves a point by kilometres).
var geomSRIDMatrixAxis = []int{0, 4326, 3857}

// TestDecodeMySQLGeometry_SRIDFamilyMatrix walks column × row SRID for the
// decoder that every MySQL read path except VStream funnels through: bulk
// copy, binlog CDC, the flatfile shim, and mydumper.
func TestDecodeMySQLGeometry_SRIDFamilyMatrix(t *testing.T) {
	for _, colSRID := range geomSRIDMatrixAxis {
		for _, rowSRID := range geomSRIDMatrixAxis {
			t.Run(nameSRIDCell(colSRID, rowSRID), func(t *testing.T) {
				got, err := decodeValue(mysqlGeomBytes(uint32(rowSRID)), ir.Geometry{SRID: colSRID})
				if colSRID == rowSRID {
					if err != nil {
						t.Fatalf("matching SRIDs must decode: %v", err)
					}
					b, ok := got.([]byte)
					if !ok || len(b) != 21 {
						t.Fatalf("got %T (len %d); want the 21-byte bare WKB", got, len(b))
					}
					if b[0] != 0x01 {
						t.Errorf("decoded WKB starts 0x%02x; want the byte-order flag 0x01 (the SRID prefix was not stripped)", b[0])
					}
					return
				}
				if err == nil {
					t.Fatalf("column SRID %d vs row SRID %d decoded to %v — the row's SRID was silently dropped", colSRID, rowSRID, got)
				}
				if !errors.Is(err, ir.ErrGeometryRowSRIDMismatch) {
					t.Errorf("error %v does not wrap ir.ErrGeometryRowSRIDMismatch", err)
				}
			})
		}
	}
}

// nameSRIDCell renders a matrix cell name.
func nameSRIDCell(colSRID, rowSRID int) string {
	return "col" + strconv.Itoa(colSRID) + "/row" + strconv.Itoa(rowSRID)
}
