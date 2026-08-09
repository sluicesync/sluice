// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"encoding/binary"
	"fmt"
)

// Axis-order normalisation for MySQL geometry values (Bug 238).
//
// # The defect
//
// MySQL 8 applies each spatial reference system's OWN axis order. For a
// GEOGRAPHIC SRS — EPSG 4326 and 544 others on a stock 8.0 — that order is
// (latitude, longitude). MariaDB implements no axis order at all and stores the
// GIS convention (longitude, latitude), as does PostGIS. sluice carries the WKB
// verbatim, so the NUMBERS survive a cross-engine copy and their MEANING does
// not.
//
// Measured on stock containers, both directions:
//
//   - MariaDB `POINT(12.5 41.9)` — Rome, lon 12.5 / lat 41.9 — lands on MySQL as
//     latitude 12.5, longitude 41.9: a point off the Somali coast, exit 0.
//     When the longitude exceeds ±90 the same path is loud instead
//     (`ERROR 3617 … Latitude -122.400000 is out of range`), which is why the
//     defect looked like an occasional error rather than a data-meaning bug.
//   - MySQL `POINT(37.8 -122.4)` — San Francisco, lat 37.8 / lon -122.4 — lands
//     on PostGIS as lon 37.8, lat -122.4. PostGIS range-checks nothing, so it
//     silently stores an impossible latitude.
//
// # Why normalise at the engine boundary rather than special-case the pair
//
// The IR's geometry value is defined as canonical (longitude, latitude). Only
// the vanilla-MySQL reader and writer then need to change: MariaDB and PostGIS
// already match the canonical order. Every engine pair falls out correct
// without any of them knowing which engine is on the other side — and
// MySQL→MySQL stays byte-identical, because swap-on-read and swap-on-write
// compose to the identity.
//
// # Why sluice swaps the bytes rather than asking MySQL to
//
// MySQL exposes `ST_AsBinary(g, 'axis-order=long-lat')` and the matching
// `ST_GeomFromWKB` option, which is a better tool for the bulk-copy SELECT. It
// cannot serve the CDC path: binlog row images arrive as the stored bytes with
// no SQL projection to attach an option to. Using the server option on one path
// and a byte swap on the other would let cold-start and CDC disagree about what
// a coordinate means — the divergence class this project has paid for
// repeatedly — so both paths take the same code.
//
// # Scope, stated because a walker that silently skips a shape is worse than none
//
// MySQL geometries are 2D only: `ST_GeomFromText('POINT Z (1 2 3)', 4326)` is
// ERROR 3037 on 8.0, verified rather than assumed. So this walks the seven
// standard WKB types at two coordinates each. Anything it does not recognise —
// a Z/M type code from a future server, a truncated buffer — is an ERROR, never
// a silent pass-through, because passing a geometry through unswapped is
// exactly the silent relocation this exists to prevent.

// wkbByteOrder and the standard WKB geometry type codes.
const (
	wkbBigEndian    = 0x00
	wkbLittleEndian = 0x01

	wkbPoint              = 1
	wkbLineString         = 2
	wkbPolygon            = 3
	wkbMultiPoint         = 4
	wkbMultiLineString    = 5
	wkbMultiPolygon       = 6
	wkbGeometryCollection = 7
)

// swapWKBAxes returns b with every coordinate pair's X and Y exchanged.
//
// b is standard WKB with no SRID prefix — MySQL keeps the SRID in a separate
// 4-byte header that the caller has already stripped. The input is not
// modified; the returned slice is a fresh copy.
func swapWKBAxes(b []byte) ([]byte, error) {
	out := make([]byte, len(b))
	copy(out, b)
	n, err := swapGeometry(out, 0)
	if err != nil {
		return nil, err
	}
	if n != len(out) {
		return nil, fmt.Errorf("mysql: geometry axis swap: %d trailing byte(s) after a complete geometry", len(out)-n)
	}
	return out, nil
}

// swapGeometry swaps one geometry starting at off, in place, and returns the
// offset just past it.
func swapGeometry(b []byte, off int) (int, error) {
	if off+5 > len(b) {
		return 0, fmt.Errorf("mysql: geometry axis swap: truncated header at byte %d", off)
	}
	order := b[off]
	var bo binary.ByteOrder
	switch order {
	case wkbLittleEndian:
		bo = binary.LittleEndian
	case wkbBigEndian:
		bo = binary.BigEndian
	default:
		return 0, fmt.Errorf("mysql: geometry axis swap: byte-order byte %#x at %d is neither 0x00 nor 0x01", order, off)
	}
	typ := bo.Uint32(b[off+1 : off+5])
	off += 5

	switch typ {
	case wkbPoint:
		return swapPoints(b, off, 1, bo)
	case wkbLineString:
		// A bare run of coordinate pairs behind one count.
		count, next, err := readCount(b, off, bo)
		if err != nil {
			return 0, err
		}
		return swapPoints(b, next, count, bo)
	case wkbMultiPoint:
		// Per the WKB spec a MultiPoint's elements are complete Point
		// GEOMETRIES — each with its own byte-order byte and type code — not
		// bare coordinate pairs. Treating them as bare pairs is the classic
		// WKB-walker bug: it reads the next point's 5-byte header as
		// coordinate bytes and corrupts the value.
		return swapNested(b, off, bo)
	case wkbPolygon:
		// A polygon is a count of rings, each a count of points.
		rings, off, err := readCount(b, off, bo)
		if err != nil {
			return 0, err
		}
		for i := uint32(0); i < rings; i++ {
			pts, next, err := readCount(b, off, bo)
			if err != nil {
				return 0, err
			}
			if off, err = swapPoints(b, next, pts, bo); err != nil {
				return 0, err
			}
		}
		return off, nil
	case wkbMultiLineString, wkbMultiPolygon, wkbGeometryCollection:
		return swapNested(b, off, bo)
	default:
		// Includes the Z/M type codes (1000/2000/3000 ranges). MySQL 8 cannot
		// produce them, so reaching here means the assumption this walker rests
		// on has changed — refuse rather than emit a half-swapped geometry.
		return 0, fmt.Errorf("mysql: geometry axis swap: unsupported WKB type %d at byte %d "+
			"(MySQL is 2D-only; a Z/M or extended geometry cannot be axis-normalised by this walker)", typ, off-5)
	}
}

// swapNested walks a count-prefixed list of complete sub-geometries, each
// carrying its own byte-order byte and type code.
func swapNested(b []byte, off int, bo binary.ByteOrder) (int, error) {
	n, off, err := readCount(b, off, bo)
	if err != nil {
		return 0, err
	}
	for i := uint32(0); i < n; i++ {
		if off, err = swapGeometry(b, off); err != nil {
			return 0, err
		}
	}
	return off, nil
}

// swapPoints exchanges the two float64s of each of count coordinate pairs
// starting at off, and returns the offset just past them.
func swapPoints(b []byte, off int, count uint32, bo binary.ByteOrder) (int, error) {
	for i := uint32(0); i < count; i++ {
		if off+16 > len(b) {
			return 0, fmt.Errorf("mysql: geometry axis swap: truncated coordinate at byte %d", off)
		}
		x := bo.Uint64(b[off : off+8])
		y := bo.Uint64(b[off+8 : off+16])
		bo.PutUint64(b[off:off+8], y)
		bo.PutUint64(b[off+8:off+16], x)
		off += 16
	}
	return off, nil
}

// readCount reads a uint32 element count and returns it with the offset just
// past it.
func readCount(b []byte, off int, bo binary.ByteOrder) (count uint32, next int, err error) {
	if off+4 > len(b) {
		return 0, 0, fmt.Errorf("mysql: geometry axis swap: truncated element count at byte %d", off)
	}
	return bo.Uint32(b[off : off+4]), off + 4, nil
}
