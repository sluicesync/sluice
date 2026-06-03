// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"bytes"
	"reflect"
	"testing"

	"sluicesync.dev/sluice/internal/ir"
)

func TestPostgisSubtypeName(t *testing.T) {
	cases := []struct {
		name string
		in   ir.Geometry
		want string
	}{
		{"POINT", ir.Geometry{Subtype: ir.GeometryPoint}, "POINT"},
		{"LINESTRING", ir.Geometry{Subtype: ir.GeometryLineString}, "LINESTRING"},
		{"POLYGON", ir.Geometry{Subtype: ir.GeometryPolygon}, "POLYGON"},
		{"MULTIPOINT", ir.Geometry{Subtype: ir.GeometryMultiPoint}, "MULTIPOINT"},
		{"MULTILINESTRING", ir.Geometry{Subtype: ir.GeometryMultiLineString}, "MULTILINESTRING"},
		{"MULTIPOLYGON", ir.Geometry{Subtype: ir.GeometryMultiPolygon}, "MULTIPOLYGON"},
		{"GEOMETRYCOLLECTION", ir.Geometry{Subtype: ir.GeometryCollection}, "GEOMETRYCOLLECTION"},
		{"GEOMETRY (unspecified)", ir.Geometry{Subtype: ir.GeometryUnspecified}, "GEOMETRY"},
		{"GEOMETRY (unknown)", ir.Geometry{Subtype: ir.GeometrySubtype(255)}, "GEOMETRY"},
		// Bug 52: Z / M / ZM dimensional variants append to the base.
		{"POINTZ", ir.Geometry{Subtype: ir.GeometryPoint, HasZ: true}, "POINTZ"},
		{"POINTM", ir.Geometry{Subtype: ir.GeometryPoint, HasM: true}, "POINTM"},
		{"POINTZM", ir.Geometry{Subtype: ir.GeometryPoint, HasZ: true, HasM: true}, "POINTZM"},
		{"LINESTRINGZ", ir.Geometry{Subtype: ir.GeometryLineString, HasZ: true}, "LINESTRINGZ"},
		{"MULTIPOLYGONZM", ir.Geometry{Subtype: ir.GeometryMultiPolygon, HasZ: true, HasM: true}, "MULTIPOLYGONZM"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			if got := postgisSubtypeName(c.in); got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
}

// WKB POINT(2.0, 3.0) bytes used as the byte-level test fixture.
// Layout (little-endian):
//
//	byte_order  = 01
//	geom_type   = 01 00 00 00      (POINT, uint32 LE)
//	x = 2.0 LE  = 00 00 00 00 00 00 00 40
//	y = 3.0 LE  = 00 00 00 00 00 00 08 40
//
// Total: 21 bytes.
func wkbPointLE() []byte {
	return []byte{
		0x01,
		0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40,
	}
}

// EWKB POINT(2.0, 3.0) with SRID 4326. Layout:
//
//	byte_order  = 01
//	geom_type   = 01 00 00 20      (POINT | 0x20000000 SRID flag, LE)
//	srid        = E6 10 00 00      (4326 LE)
//	x, y        = same f64 LE bytes as wkbPointLE
//
// Total: 25 bytes.
func ewkbPointLE() []byte {
	return []byte{
		0x01,
		0x01, 0x00, 0x00, 0x20,
		0xE6, 0x10, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40,
	}
}

// WKB POINT(2.0, 3.0) in big-endian. Same coordinates, BE
// byte order throughout. Total: 21 bytes.
func wkbPointBE() []byte {
	return []byte{
		0x00,
		0x00, 0x00, 0x00, 0x01,
		0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x40, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
}

// EWKB BE counterpart with SRID 4326. Total: 25 bytes.
func ewkbPointBE() []byte {
	return []byte{
		0x00,
		0x20, 0x00, 0x00, 0x01,
		0x00, 0x00, 0x10, 0xE6,
		0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x40, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
}

// TestWKBToEWKB walks the byte-level conversion from raw WKB to
// PostGIS EWKB.
func TestWKBToEWKB(t *testing.T) {
	t.Run("appends SRID and sets the present-bit on type", func(t *testing.T) {
		got, err := wkbToEWKB(wkbPointLE(), 4326)
		if err != nil {
			t.Fatalf("wkbToEWKB: %v", err)
		}
		want := ewkbPointLE()
		if !bytes.Equal(got, want) {
			t.Errorf("\n got:  %x\nwant: %x", got, want)
		}
	})

	t.Run("preserves big-endian byte order", func(t *testing.T) {
		got, err := wkbToEWKB(wkbPointBE(), 4326)
		if err != nil {
			t.Fatalf("wkbToEWKB: %v", err)
		}
		want := ewkbPointBE()
		if !bytes.Equal(got, want) {
			t.Errorf("\n got:  %x\nwant: %x", got, want)
		}
	})

	t.Run("passes through values that already look EWKB", func(t *testing.T) {
		ewkb := ewkbPointLE()
		got, err := wkbToEWKB(ewkb, 9999)
		if err != nil {
			t.Fatalf("wkbToEWKB: %v", err)
		}
		if !bytes.Equal(got, ewkb) {
			t.Errorf("\n got:  %x\nwant: %x", got, ewkb)
		}
	})

	t.Run("rejects too-short inputs", func(t *testing.T) {
		if _, err := wkbToEWKB([]byte{0x01, 0x01, 0x00}, 0); err == nil {
			t.Error("expected error for 3-byte input")
		}
		if _, err := wkbToEWKB(nil, 0); err == nil {
			t.Error("expected error for nil input")
		}
	})

	t.Run("rejects unknown byte-order flag", func(t *testing.T) {
		bad := []byte{0x42, 0x00, 0x00, 0x00, 0x00}
		if _, err := wkbToEWKB(bad, 0); err == nil {
			t.Error("expected error for byte-order=0x42")
		}
	})
}

// TestEWKBToWKB walks the inverse byte-level conversion, stripping
// PostGIS EWKB framing back to raw WKB. Used by the PG row reader
// to normalise pgx's text-format geometry output into the IR's
// canonical "raw WKB" shape (per docs/value-types.md).
func TestEWKBToWKB(t *testing.T) {
	t.Run("strips SRID and clears the present-bit on type", func(t *testing.T) {
		got, err := ewkbToWKB(ewkbPointLE())
		if err != nil {
			t.Fatalf("ewkbToWKB: %v", err)
		}
		want := wkbPointLE()
		if !bytes.Equal(got, want) {
			t.Errorf("\n got:  %x\nwant: %x", got, want)
		}
	})

	t.Run("preserves big-endian byte order", func(t *testing.T) {
		got, err := ewkbToWKB(ewkbPointBE())
		if err != nil {
			t.Fatalf("ewkbToWKB: %v", err)
		}
		want := wkbPointBE()
		if !bytes.Equal(got, want) {
			t.Errorf("\n got:  %x\nwant: %x", got, want)
		}
	})

	t.Run("passes through values that are already raw WKB", func(t *testing.T) {
		wkb := wkbPointLE()
		got, err := ewkbToWKB(wkb)
		if err != nil {
			t.Fatalf("ewkbToWKB: %v", err)
		}
		if !bytes.Equal(got, wkb) {
			t.Errorf("\n got:  %x\nwant: %x", got, wkb)
		}
		// And the result must be a fresh copy — not the input slice.
		if len(got) > 0 && len(wkb) > 0 && &got[0] == &wkb[0] {
			t.Error("ewkbToWKB returned the input slice; expected a copy")
		}
	})

	t.Run("rejects too-short inputs", func(t *testing.T) {
		if _, err := ewkbToWKB([]byte{0x01, 0x01, 0x00}); err == nil {
			t.Error("expected error for 3-byte input")
		}
		if _, err := ewkbToWKB(nil); err == nil {
			t.Error("expected error for nil input")
		}
	})

	t.Run("rejects unknown byte-order flag", func(t *testing.T) {
		bad := []byte{0x42, 0x00, 0x00, 0x00, 0x00}
		if _, err := ewkbToWKB(bad); err == nil {
			t.Error("expected error for byte-order=0x42")
		}
	})

	t.Run("rejects EWKB declaring SRID-present without body", func(t *testing.T) {
		// Type word with SRID-present flag set but no SRID word follows.
		bad := []byte{0x01, 0x01, 0x00, 0x00, 0x20}
		if _, err := ewkbToWKB(bad); err == nil {
			t.Error("expected error for truncated EWKB")
		}
	})

	t.Run("round-trips with wkbToEWKB", func(t *testing.T) {
		original := wkbPointLE()
		ewkb, err := wkbToEWKB(original, 4326)
		if err != nil {
			t.Fatalf("wkbToEWKB: %v", err)
		}
		got, err := ewkbToWKB(ewkb)
		if err != nil {
			t.Fatalf("ewkbToWKB: %v", err)
		}
		if !bytes.Equal(got, original) {
			t.Errorf("round-trip\n got:  %x\nwant: %x", got, original)
		}
	})
}

// mysqlGeometry returns a MySQL on-wire geometry value: SRID prefix
// (4 bytes LE) + WKB. With srid=4326 the prefix is E6 10 00 00.
func mysqlGeometry(srid uint32, wkb []byte) []byte {
	out := make([]byte, 4+len(wkb))
	out[0] = byte(srid)
	out[1] = byte(srid >> 8)
	out[2] = byte(srid >> 16)
	out[3] = byte(srid >> 24)
	copy(out[4:], wkb)
	return out
}

func TestMysqlGeometryToWKB(t *testing.T) {
	t.Run("strips four-byte SRID prefix", func(t *testing.T) {
		in := mysqlGeometry(4326, wkbPointLE())
		wkb, srid, err := mysqlGeometryToWKB(in)
		if err != nil {
			t.Fatalf("mysqlGeometryToWKB: %v", err)
		}
		if srid != 4326 {
			t.Errorf("srid = %d; want 4326", srid)
		}
		wantWKB := in[4:]
		if !bytes.Equal(wkb, wantWKB) {
			t.Errorf("\n got:  %x\nwant: %x", wkb, wantWKB)
		}
	})

	t.Run("zero-SRID stays zero", func(t *testing.T) {
		in := mysqlGeometry(0, wkbPointLE())
		_, srid, err := mysqlGeometryToWKB(in)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if srid != 0 {
			t.Errorf("srid = %d; want 0", srid)
		}
	})

	t.Run("rejects too-short inputs", func(t *testing.T) {
		if _, _, err := mysqlGeometryToWKB([]byte{0x01, 0x02, 0x03}); err == nil {
			t.Error("expected error for 3-byte input")
		}
		if _, _, err := mysqlGeometryToWKB(nil); err == nil {
			t.Error("expected error for nil input")
		}
	})
}

func TestMysqlWrapWKB(t *testing.T) {
	// Round-trip: strip then wrap with the same SRID should reproduce
	// the original bytes.
	original := mysqlGeometry(4326, wkbPointLE())
	wkb, srid, err := mysqlGeometryToWKB(original)
	if err != nil {
		t.Fatalf("mysqlGeometryToWKB: %v", err)
	}
	got := mysqlWrapWKB(wkb, srid)
	if !bytes.Equal(got, original) {
		t.Errorf("round-trip\n got:  %x\nwant: %x", got, original)
	}

	t.Run("zero SRID prefix when wrapping fresh WKB", func(t *testing.T) {
		fresh := wkbPointLE()
		got := mysqlWrapWKB(fresh, 0)
		if len(got) != len(fresh)+4 {
			t.Errorf("length = %d; want %d", len(got), len(fresh)+4)
		}
		if !bytes.Equal(got[:4], []byte{0, 0, 0, 0}) {
			t.Errorf("first 4 bytes = %x; want 00000000", got[:4])
		}
		if !bytes.Equal(got[4:], fresh) {
			t.Errorf("payload mismatch")
		}
	})
}

// TestEmitColumnTypeGeometry exercises the PostGIS-aware DDL
// emission. With opts.HasPostGIS == false the column is rejected
// (matches the original v1 behaviour); with HasPostGIS == true the
// column emits as `geometry(<subtype>, <srid>)` carrying both the
// IR's Subtype and SRID through verbatim.
func TestEmitColumnTypeGeometry(t *testing.T) {
	t.Run("rejected without postgis", func(t *testing.T) {
		_, err := emitColumnType(ir.Geometry{Subtype: ir.GeometryPoint}, emitOpts{})
		if err == nil {
			t.Error("expected error when HasPostGIS=false")
		}
	})

	cases := []struct {
		name    string
		in      ir.Geometry
		want    string
		hasGIS  bool
		wantErr bool
	}{
		{"point srid 0", ir.Geometry{Subtype: ir.GeometryPoint}, "geometry(POINT, 0)", true, false},
		{"point srid 4326", ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326}, "geometry(POINT, 4326)", true, false},
		{"polygon", ir.Geometry{Subtype: ir.GeometryPolygon, SRID: 3857}, "geometry(POLYGON, 3857)", true, false},
		{"unspecified", ir.Geometry{Subtype: ir.GeometryUnspecified}, "geometry(GEOMETRY, 0)", true, false},
		{"multipoint", ir.Geometry{Subtype: ir.GeometryMultiPoint}, "geometry(MULTIPOINT, 0)", true, false},
		// IsGeography flips the type name to `geography(...)`. Bug 49:
		// without this branch the PG writer emitted geography columns
		// as `geometry(...)` (or, before this fix, the schema reader
		// refused upstream).
		{
			"geography point srid 4326",
			ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326, IsGeography: true},
			"geography(POINT, 4326)", true, false,
		},
		{
			"geography polygon srid 4326",
			ir.Geometry{Subtype: ir.GeometryPolygon, SRID: 4326, IsGeography: true},
			"geography(POLYGON, 4326)", true, false,
		},
		{
			"geography unspecified",
			ir.Geometry{Subtype: ir.GeometryUnspecified, IsGeography: true},
			"geography(GEOMETRY, 0)", true, false,
		},
		// Bug 52: Z / M / ZM dimensional variants — the writer
		// reconstructs the suffix on emit so PG → PG round-trips
		// `geometry(POINTZ, 4326)` byte-for-byte.
		{
			"geometry POINTZ",
			ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326, HasZ: true},
			"geometry(POINTZ, 4326)", true, false,
		},
		{
			"geometry POINTM",
			ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326, HasM: true},
			"geometry(POINTM, 4326)", true, false,
		},
		{
			"geometry POINTZM",
			ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326, HasZ: true, HasM: true},
			"geometry(POINTZM, 4326)", true, false,
		},
		{
			"geography POINTZ",
			ir.Geometry{Subtype: ir.GeometryPoint, SRID: 4326, IsGeography: true, HasZ: true},
			"geography(POINTZ, 4326)", true, false,
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			got, err := emitColumnType(c.in, emitOpts{HasPostGIS: c.hasGIS})
			if c.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("err: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
}

// guard against trivial regressions to the canonical-EWKB-passthrough behaviour
var _ = reflect.DeepEqual
