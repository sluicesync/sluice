// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"

	"github.com/orware/sluice/internal/ir"
)

// detectPostGIS reports whether the postgis extension is installed
// in the connected database. The query is read-only and runs
// against pg_extension, which any role can SELECT from. Failure to
// query (the database isn't reachable, the role doesn't have
// connect privileges) is propagated; "no row" is reported as
// "not installed" without an error.
func detectPostGIS(ctx context.Context, db *sql.DB) (bool, error) {
	var present bool
	err := db.QueryRowContext(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis')",
	).Scan(&present)
	if err != nil {
		return false, fmt.Errorf("postgres: detect postgis: %w", err)
	}
	return present, nil
}

// postgisSubtypeName maps an [ir.GeometrySubtype] to the PostGIS
// type-modifier name. The result is the bareword that goes inside
// `geometry(<NAME>, <SRID>)`. The names are the upper-case forms
// PostGIS itself uses and accepts.
func postgisSubtypeName(s ir.GeometrySubtype) string {
	switch s {
	case ir.GeometryPoint:
		return "POINT"
	case ir.GeometryLineString:
		return "LINESTRING"
	case ir.GeometryPolygon:
		return "POLYGON"
	case ir.GeometryMultiPoint:
		return "MULTIPOINT"
	case ir.GeometryMultiLineString:
		return "MULTILINESTRING"
	case ir.GeometryMultiPolygon:
		return "MULTIPOLYGON"
	case ir.GeometryCollection:
		return "GEOMETRYCOLLECTION"
	default:
		// ir.GeometryUnspecified plus any unknown subtype values fall
		// through here. "GEOMETRY" is PostGIS's wildcard subtype that
		// accepts any concrete shape; rejecting at write time would
		// mask source data, so we permit through with the wildcard.
		return "GEOMETRY"
	}
}

// wkbToEWKB takes a WKB geometry (per the IR contract for
// [ir.Geometry] values) and wraps it in PostGIS EWKB framing with
// the given SRID. EWKB differs from WKB in two places:
//
//   - The geometry-type integer (4 bytes after the byte-order flag)
//     has high bit 0x20000000 set to signal "SRID present".
//   - A 4-byte SRID immediately follows the geometry-type integer.
//
// The byte order flag (1 byte, 0=BE, 1=LE) is preserved from the
// input. Returns the input unchanged if it already looks EWKB-shaped
// (high bit set on the type integer) — same-engine PG→PG already
// produces EWKB.
//
// Layout (little-endian source):
//
//	WKB:   [00 00 00 01]  = byte_order(LE) + 0x00 0x00 + type(LE)
//	       └─ pos 0 ──────┘
//	       byte 0 = byte order
//	       bytes 1..4 = geometry type (LE uint32) — actual layout is
//	                    1 byte order then 4 bytes type, total 5 bytes
//
// Real layout: byte 0 = order, bytes 1..4 = type uint32 in that
// order, bytes 5+ = type-specific payload. EWKB is the same shape
// with the SRID-present bit set on the type uint32 and a SRID
// uint32 inserted between bytes 4 and 5.
func wkbToEWKB(wkb []byte, srid uint32) ([]byte, error) {
	if len(wkb) < 5 {
		return nil, fmt.Errorf("wkb too short (%d bytes; need >=5)", len(wkb))
	}
	byteOrder := wkb[0]
	var endian binary.ByteOrder
	switch byteOrder {
	case 0:
		endian = binary.BigEndian
	case 1:
		endian = binary.LittleEndian
	default:
		return nil, fmt.Errorf("wkb has unknown byte-order flag 0x%02x (want 0 or 1)", byteOrder)
	}

	geomType := endian.Uint32(wkb[1:5])
	const sridFlag uint32 = 0x20000000
	if geomType&sridFlag != 0 {
		// Already EWKB-shaped. Same-engine PG→PG paths produce this.
		return wkb, nil
	}

	out := make([]byte, len(wkb)+4)
	out[0] = byteOrder
	endian.PutUint32(out[1:5], geomType|sridFlag)
	endian.PutUint32(out[5:9], srid)
	copy(out[9:], wkb[5:])
	return out, nil
}

// mysqlGeometryToWKB strips MySQL's 4-byte little-endian SRID
// prefix from a geometry value's bytes, returning the trailing WKB
// payload and the SRID itself.
//
// MySQL stores geometry on the wire as `<srid uint32 LE><wkb>`. The
// IR contract for [ir.Geometry] values is "raw WKB" (per
// docs/value-types.md), so the MySQL value decoder normalises to
// that form by stripping the prefix; the SRID is returned in case a
// caller (e.g. the PG writer's EWKB framing) wants it.
//
// Returns an error for input shorter than 5 bytes (the SRID prefix
// alone is 4; a valid WKB body needs at least 1 more byte for the
// byte-order flag). Empty/nil input is reported as "no geometry".
func mysqlGeometryToWKB(b []byte) (wkb []byte, srid uint32, err error) {
	if len(b) < 5 {
		return nil, 0, fmt.Errorf("mysql geometry too short (%d bytes; need >=5)", len(b))
	}
	srid = binary.LittleEndian.Uint32(b[:4])
	wkb = b[4:]
	return wkb, srid, nil
}

// mysqlWrapWKB is the inverse of mysqlGeometryToWKB: prepends a
// 4-byte little-endian SRID to a WKB payload, producing MySQL's
// on-wire geometry format. Used by the MySQL row writer when
// preparing an [ir.Geometry] value for INSERT.
//
// Input contract is "raw WKB" (matches docs/value-types.md). When
// the source bytes already begin with a 4-byte SRID prefix (the
// shape MySQL itself emits), callers must strip it first via
// mysqlGeometryToWKB; passing a SRID-prefixed value here would
// produce a doubled prefix and break the MySQL parser.
func mysqlWrapWKB(wkb []byte, srid uint32) []byte {
	out := make([]byte, 4+len(wkb))
	binary.LittleEndian.PutUint32(out[:4], srid)
	copy(out[4:], wkb)
	return out
}
