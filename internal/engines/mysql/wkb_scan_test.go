// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// The SPAT-1 unit matrix (audit 2026-08-11): every WKB family × the two
// poison shapes (NaN coordinate, ±Inf coordinate) plus the dimensional and
// endianness spellings that circulate in IR values — ISO Z/M/ZM offsets AND
// PostGIS's EWKB high-bit flags — and the structural refusals. The
// integration leg (wkb_nanpoint_integration_test.go) proves the wiring and
// the environmental premise on a real server; this matrix owns the breadth.

// wkbw is a tiny WKB writer for building test geometries byte-precisely.
type wkbw struct {
	buf    []byte
	endian binary.ByteOrder
}

func newWKBW(le bool) *wkbw {
	w := &wkbw{}
	if le {
		w.endian = binary.LittleEndian
	} else {
		w.endian = binary.BigEndian
	}
	return w
}

func (w *wkbw) header(typ uint32) *wkbw {
	if w.endian == binary.LittleEndian {
		w.buf = append(w.buf, 1)
	} else {
		w.buf = append(w.buf, 0)
	}
	return w.u32(typ)
}

func (w *wkbw) u32(v uint32) *wkbw {
	var b [4]byte
	w.endian.PutUint32(b[:], v)
	w.buf = append(w.buf, b[:]...)
	return w
}

func (w *wkbw) f64(v float64) *wkbw {
	var b [8]byte
	w.endian.PutUint64(b[:], math.Float64bits(v))
	w.buf = append(w.buf, b[:]...)
	return w
}

func (w *wkbw) raw(b []byte) *wkbw { w.buf = append(w.buf, b...); return w }

func point(le bool, coords ...float64) []byte {
	typ := uint32(1)
	switch len(coords) {
	case 3:
		typ = 1001 // ISO Z
	case 4:
		typ = 3001 // ISO ZM
	}
	w := newWKBW(le).header(typ)
	for _, c := range coords {
		w.f64(c)
	}
	return w.buf
}

func TestScanWKB_PoisonMatrix(t *testing.T) {
	nan := math.NaN()
	poison := []struct {
		name string
		wkb  []byte
	}{
		{"Point NaN X (POINT EMPTY rendering)", point(true, nan, nan)},
		{"Point NaN Y only", point(true, 1, nan)},
		{"Point +Inf", point(true, math.Inf(1), 2)},
		{"Point -Inf", point(true, 3, math.Inf(-1))},
		{"Point NaN big-endian", point(false, nan, nan)},
		{"Point Z (ISO) NaN in Z", point(true, 1, 2, nan)},
		{"Point ZM (ISO) NaN in M", point(true, 1, 2, 3, nan)},
		{"Point Z (EWKB high-bit flag) NaN in Z", newWKBW(true).header(1 | wkbZFlag).f64(1).f64(2).f64(nan).buf},
		{"Point M (EWKB high-bit flag) NaN in M", newWKBW(true).header(1 | wkbMFlag).f64(1).f64(2).f64(nan).buf},
		{"LineString NaN in 2nd point", newWKBW(true).header(2).u32(2).f64(0).f64(0).f64(nan).f64(1).buf},
		{"Polygon NaN in ring", newWKBW(true).header(3).u32(1).u32(4).
			f64(0).f64(0).f64(1).f64(0).f64(nan).f64(1).f64(0).f64(0).buf},
		{"MultiPoint NaN in nested point", newWKBW(true).header(4).u32(2).
			raw(point(true, 1, 2)).raw(point(true, nan, 4)).buf},
		{"MultiLineString NaN nested", newWKBW(true).header(5).u32(1).
			raw(newWKBW(true).header(2).u32(2).f64(0).f64(0).f64(math.Inf(1)).f64(1).buf).buf},
		{"MultiPolygon NaN nested", newWKBW(true).header(6).u32(1).
			raw(newWKBW(true).header(3).u32(1).u32(4).
				f64(0).f64(0).f64(1).f64(0).f64(1).f64(nan).f64(0).f64(0).buf).buf},
		{"GeometryCollection NaN two levels down", newWKBW(true).header(7).u32(2).
			raw(point(true, 1, 2)).
			raw(newWKBW(true).header(7).u32(1).raw(point(true, nan, 9)).buf).buf},
		{"mixed endianness: BE collection, LE poison element", newWKBW(false).header(7).u32(1).
			raw(point(true, nan, 1)).buf},
	}
	for _, tc := range poison {
		t.Run(tc.name, func(t *testing.T) {
			issue, err := scanWKB(tc.wkb)
			if err != nil {
				t.Fatalf("scanWKB structural error %v; want a clean scan reporting a non-finite issue", err)
			}
			if issue == "" {
				t.Fatal("scanWKB found no issue; a non-finite coordinate slipped through — this is the exact " +
					"silent-poison shape SPAT-1 exists to refuse")
			}
			if !strings.Contains(issue, "non-finite") {
				t.Errorf("issue %q should name the non-finite coordinate", issue)
			}
		})
	}

	clean := []struct {
		name string
		wkb  []byte
	}{
		{"Point", point(true, 12.5, -3.25)},
		{"Point big-endian", point(false, 12.5, -3.25)},
		{"Point Z (ISO)", point(true, 1, 2, 3)},
		{"Point ZM (ISO)", point(true, 1, 2, 3, 4)},
		{"Point Z (EWKB high-bit flag)", newWKBW(true).header(1 | wkbZFlag).f64(1).f64(2).f64(3).buf},
		{"LineString", newWKBW(true).header(2).u32(2).f64(0).f64(0).f64(1).f64(1).buf},
		{"empty LineString (server refuses 1416 loudly — not ours to catch)", newWKBW(true).header(2).u32(0).buf},
		{"Polygon", newWKBW(true).header(3).u32(1).u32(4).
			f64(0).f64(0).f64(1).f64(0).f64(1).f64(1).f64(0).f64(0).buf},
		{"MultiPoint", newWKBW(true).header(4).u32(1).raw(point(true, 5, 6)).buf},
		{"MultiLineString", newWKBW(true).header(5).u32(1).
			raw(newWKBW(true).header(2).u32(2).f64(0).f64(0).f64(1).f64(1).buf).buf},
		{"MultiPolygon", newWKBW(true).header(6).u32(1).
			raw(newWKBW(true).header(3).u32(1).u32(4).
				f64(0).f64(0).f64(1).f64(0).f64(1).f64(1).f64(0).f64(0).buf).buf},
		{"GeometryCollection mixed", newWKBW(true).header(7).u32(2).
			raw(point(true, 1, 2)).
			raw(newWKBW(true).header(2).u32(2).f64(0).f64(0).f64(1).f64(1).buf).buf},
		{"nested element carrying an EWKB SRID word", newWKBW(true).header(7).u32(1).
			raw(newWKBW(true).header(1 | wkbSRIDFlag).u32(4326).f64(1).f64(2).buf).buf},
	}
	for _, tc := range clean {
		t.Run("clean/"+tc.name, func(t *testing.T) {
			issue, err := scanWKB(tc.wkb)
			if err != nil || issue != "" {
				t.Fatalf("scanWKB(%s) = (%q, %v); want clean — over-refusal breaks working migrations", tc.name, issue, err)
			}
		})
	}
}

func TestScanWKB_StructuralRefusals(t *testing.T) {
	cases := []struct {
		name string
		wkb  []byte
		want string
	}{
		{"empty", nil, "need 5 bytes"},
		{"truncated point", point(true, 1, 2)[:12], "have"},
		{"bad byte order", append([]byte{9}, point(true, 1, 2)[1:]...), "byte-order"},
		{"curve type (CircularString, no MySQL shape)", newWKBW(true).header(8).u32(0).buf, "unknown geometry base type"},
		{"absurd ISO offset", newWKBW(true).header(9001).buf, "unknown geometry type word"},
		{"trailing bytes", append(point(true, 1, 2), 0xEE), "trailing"},
		{"hostile element count", newWKBW(true).header(2).u32(0xFFFFFFFF).buf, "exceeds"},
		{"nesting bomb", func() []byte {
			b := point(true, 1, 2)
			for i := 0; i < wkbMaxNesting+2; i++ {
				b = newWKBW(true).header(7).u32(1).raw(b).buf
			}
			return b
		}(), "nests deeper"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := scanWKB(tc.wkb)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("scanWKB err = %v; want a structural refusal containing %q", err, tc.want)
			}
		})
	}
}

// TestPrepareValue_RefusesNonFiniteGeometry pins the wiring: the poison
// refuses through prepareValue with the ADR-0153 code (the same one the
// scalar NaN guard carries), and a clean value still gets the SRID wrap.
func TestPrepareValue_RefusesNonFiniteGeometry(t *testing.T) {
	col := &ir.Column{Name: "loc", Type: ir.Geometry{SRID: 4326}}

	_, err := prepareValue(point(true, math.NaN(), math.NaN()), col)
	if err == nil {
		t.Fatal("prepareValue accepted a NaN-coordinate geometry; SPAT-1 regressed — the poison would land silently")
	}
	if ce, ok := sluicecode.FromError(err); !ok || ce.Code != sluicecode.CodeValueUnrepresentable {
		t.Errorf("refusal should carry %s structurally; got: %v", sluicecode.CodeValueUnrepresentable, err)
	}
	if !strings.Contains(err.Error(), `"loc"`) {
		t.Errorf("refusal should name the column; got: %v", err)
	}

	out, err := prepareValue(point(true, 1, 2), col)
	if err != nil {
		t.Fatalf("clean geometry refused: %v", err)
	}
	b, ok := out.([]byte)
	if !ok || len(b) != 4+21 {
		t.Fatalf("clean geometry not SRID-wrapped: %T len %d", out, len(b))
	}
	if got := binary.LittleEndian.Uint32(b[:4]); got != 4326 {
		t.Fatalf("SRID prefix = %d; want 4326 (the wrap must be unchanged)", got)
	}
}
