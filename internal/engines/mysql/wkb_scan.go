// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import (
	"encoding/binary"
	"fmt"
	"math"

	"sluicesync.dev/sluice/internal/ir"
	"sluicesync.dev/sluice/internal/sluicecode"
)

// refuseNonFiniteWKB is the geometry sibling of [refuseUnrepresentableFloat]
// (ADR-0153), closing audit 2026-08-11 SPAT-1: sluice carries geometry
// byte-verbatim, and MySQL performs no coordinate validation on raw
// internal-format geometry inserts — a NaN-coordinate value (including
// PostGIS's `POINT EMPTY`, whose WKB representation IS a NaN/NaN point)
// stores silently and lands a poison value MySQL itself cannot produce:
// `ST_AsText` returns NULL, `ST_Longitude` errors 3037, exit 0 throughout
// (observed live). Empty LINESTRING/POLYGON/MULTI* are refused loudly by the
// server (error 1416), so NaN/Inf coordinates are exactly the silent residue.
//
// PostGIS legitimately holds NaN/Inf coordinates and empty points, so a
// PG→MySQL run can absolutely feed one in; the refusal is MySQL-family
// TARGET policy, which is why it lives here and not in the IR. It runs at
// [prepareValue]'s geometry branch — the single choke point: the batched
// INSERT core binds prepareValue output, LOAD DATA falls back to the batched
// core for geometry columns, and the CDC applier routes through
// prepareApplierValue → prepareValue.
//
// The environmental premise — that MySQL still ACCEPTS the raw NaN point, so
// this client-side scan is load-bearing rather than duplicating a server
// refusal — is pinned by TestMySQLAcceptsRawNaNPointPremise against a real
// server.
func refuseNonFiniteWKB(b []byte, col *ir.Column) error {
	issue, err := scanWKB(b)
	if err != nil {
		// Structurally unparseable WKB toward a target that validates none of
		// it is refused loudly too: passing it through unscanned would be a
		// skip-branch whose safety nothing proves (a NaN can hide behind any
		// malformed prefix), and no engine reader produces malformed WKB.
		return sluicecode.Wrap(
			sluicecode.CodeValueUnrepresentable,
			"the value is not decodable WKB; repair or exclude it at the source",
			fmt.Errorf("column %q carries a geometry value whose WKB cannot be scanned (%v); "+
				"refusing rather than landing undecodable bytes MySQL will not validate",
				columnNameForError(col), err),
		)
	}
	if issue == "" {
		return nil
	}
	return sluicecode.Wrap(
		sluicecode.CodeValueUnrepresentable,
		"filter or transform the source value (e.g. exclude the row, or ST_MakeValid / NULLIF it "+
			"on the source query); note PostGIS renders POINT EMPTY as NaN coordinates and MySQL "+
			"can hold neither",
		fmt.Errorf("column %q carries a geometry with %s, which MySQL cannot represent — the server "+
			"accepts the raw bytes silently and produces a value it cannot itself create "+
			"(ST_AsText NULL / error 3037 on access); refusing loudly instead of landing poison",
			columnNameForError(col), issue),
	)
}

// WKB/EWKB geometry type words. The base type is the low word (mod 1000 for
// the ISO Z/M/ZM offsets); PostGIS EWKB instead sets high-bit flags. Values
// reaching the MySQL writer have their EWKB SRID framing stripped by the PG
// decoder, but the Z/M HIGH-BIT flags and the ISO offsets both circulate in
// IR values (the v0.120.0 336-cell matrix carries both spellings), so the
// scan honours both.
const (
	wkbZFlag    uint32 = 0x80000000
	wkbMFlag    uint32 = 0x40000000
	wkbSRIDFlag uint32 = 0x20000000
)

var wkbBaseNames = map[uint32]string{
	1: "Point", 2: "LineString", 3: "Polygon",
	4: "MultiPoint", 5: "MultiLineString", 6: "MultiPolygon",
	7: "GeometryCollection",
}

// scanWKB walks every coordinate double in a WKB/EWKB geometry and reports
// the first non-finite one as a human-readable issue ("" when clean), or an
// error when the bytes are not structurally WKB. It never modifies anything.
func scanWKB(b []byte) (issue string, err error) {
	c := &wkbCursor{b: b}
	if issue, err = c.geometry(0); err != nil || issue != "" {
		return issue, err
	}
	if c.off != len(b) {
		return "", fmt.Errorf("%d trailing byte(s) after the geometry ends at offset %d", len(b)-c.off, c.off)
	}
	return "", nil
}

type wkbCursor struct {
	b   []byte
	off int
}

// wkbMaxNesting bounds GeometryCollection recursion. Real data nests a
// handful of levels; the cap only exists so hostile bytes cannot recurse
// the stack away.
const wkbMaxNesting = 64

func (c *wkbCursor) geometry(depth int) (issue string, err error) {
	if depth > wkbMaxNesting {
		return "", fmt.Errorf("geometry nests deeper than %d levels at offset %d", wkbMaxNesting, c.off)
	}
	if len(c.b)-c.off < 5 {
		return "", fmt.Errorf("need 5 bytes for byte-order + type at offset %d, have %d", c.off, len(c.b)-c.off)
	}
	var endian binary.ByteOrder
	switch c.b[c.off] {
	case 0:
		endian = binary.BigEndian
	case 1:
		endian = binary.LittleEndian
	default:
		return "", fmt.Errorf("unknown byte-order flag 0x%02x at offset %d (want 0 or 1)", c.b[c.off], c.off)
	}
	typ := endian.Uint32(c.b[c.off+1 : c.off+5])
	c.off += 5

	hasZ := typ&wkbZFlag != 0
	hasM := typ&wkbMFlag != 0
	if typ&wkbSRIDFlag != 0 {
		// EWKB SRID framing normally never reaches this writer (the PG decoder
		// strips it), but a nested element could carry it; skip the SRID word.
		if len(c.b)-c.off < 4 {
			return "", fmt.Errorf("EWKB SRID flag set but no SRID word at offset %d", c.off)
		}
		c.off += 4
	}
	typ &^= wkbZFlag | wkbMFlag | wkbSRIDFlag
	base := typ % 1000
	switch typ / 1000 {
	case 0:
	case 1:
		hasZ = true
	case 2:
		hasM = true
	case 3:
		hasZ, hasM = true, true
	default:
		return "", fmt.Errorf("unknown geometry type word %d at offset %d", typ, c.off-4)
	}
	name, known := wkbBaseNames[base]
	if !known {
		return "", fmt.Errorf("unknown geometry base type %d at offset %d (curve types have no MySQL shape)", base, c.off-4)
	}
	dim := 2
	if hasZ {
		dim++
	}
	if hasM {
		dim++
	}

	switch base {
	case 1: // Point: dim doubles, no count word
		return c.coords(endian, name, dim, 1)
	case 2: // LineString: n × dim
		n, err := c.count(endian, name)
		if err != nil {
			return "", err
		}
		return c.coords(endian, name, dim, n)
	case 3: // Polygon: rings × (n × dim)
		rings, err := c.count(endian, name)
		if err != nil {
			return "", err
		}
		for r := uint32(0); r < rings; r++ {
			n, err := c.count(endian, name)
			if err != nil {
				return "", err
			}
			if issue, err := c.coords(endian, name, dim, n); err != nil || issue != "" {
				return issue, err
			}
		}
		return "", nil
	default: // Multi*/GeometryCollection: n × nested geometries, each self-framed
		n, err := c.count(endian, name)
		if err != nil {
			return "", err
		}
		for i := uint32(0); i < n; i++ {
			if issue, err := c.geometry(depth + 1); err != nil || issue != "" {
				return issue, err
			}
		}
		return "", nil
	}
}

// count reads one uint32 element count, bounds-checked against the bytes that
// could possibly satisfy it (each counted element needs at least 1 byte, and
// coordinates need 8 each — the callers' reads re-check exactly, this guard
// just keeps a hostile count from looping the scan).
func (c *wkbCursor) count(endian binary.ByteOrder, family string) (uint32, error) {
	if len(c.b)-c.off < 4 {
		return 0, fmt.Errorf("%s: need 4 bytes for an element count at offset %d, have %d", family, c.off, len(c.b)-c.off)
	}
	n := endian.Uint32(c.b[c.off : c.off+4])
	c.off += 4
	if int64(n) > int64(len(c.b)-c.off) {
		return 0, fmt.Errorf("%s: element count %d at offset %d exceeds the %d bytes remaining", family, n, c.off-4, len(c.b)-c.off)
	}
	return n, nil
}

// coords reads n points of dim doubles each, reporting the first NaN/±Inf.
func (c *wkbCursor) coords(endian binary.ByteOrder, family string, dim int, n uint32) (issue string, err error) {
	need := int64(n) * int64(dim) * 8
	if need > int64(len(c.b)-c.off) {
		return "", fmt.Errorf("%s: %d point(s) × %d coordinate(s) need %d bytes at offset %d, have %d",
			family, n, dim, need, c.off, len(c.b)-c.off)
	}
	axes := [4]string{"X", "Y", "Z/M", "M"}
	for p := uint32(0); p < n; p++ {
		for d := 0; d < dim; d++ {
			f := math.Float64frombits(endian.Uint64(c.b[c.off : c.off+8]))
			c.off += 8
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return fmt.Sprintf("a non-finite %s coordinate (%v) in a %s at byte offset %d",
					axes[d], f, family, c.off-8), nil
			}
		}
	}
	return "", nil
}
