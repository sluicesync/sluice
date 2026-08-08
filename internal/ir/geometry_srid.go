// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package ir

import (
	"errors"
	"fmt"
)

// ErrGeometryRowSRIDMismatch is the sentinel for a geometry VALUE whose
// spatial-reference id differs from the one its COLUMN declares. Errors
// wrapping it are matchable with [errors.Is].
var ErrGeometryRowSRIDMismatch = errors.New("row geometry carries an SRID the column does not declare")

// CheckGeometryRowSRID enforces the one premise the IR's geometry value
// contract rests on, and which nothing checked before (audit 2026-08-05
// C-14).
//
// docs/value-types.md says an [Geometry] value is RAW WKB — no SRID — and
// every engine's decoder honours that by stripping the SRID its wire format
// carries (Postgres: the EWKB SRID word; MySQL: the 4-byte little-endian
// prefix). Each writer then puts one back, taken from [Geometry.SRID] on the
// COLUMN. The round trip is lossless if and only if
//
//	every row's SRID == the column's declared SRID
//
// and that is an assertion about the world outside the code, not a property
// of it. It is FALSE on both engines. PostGIS's unconstrained `geometry`
// type (no typmod) accepts a different SRID per row and reports srid=0 for
// the column; MySQL's spatial types accept a different SRID per row unless
// the column carries the `SRID n` attribute, and report srs_id 0 without it.
// So the common shape — an unconstrained column holding rows written as
// `ST_GeomFromText('POINT(1 2)', 4326)` — decoded to bare WKB, then re-framed
// with the column's SRID 0, and landed on the target as a coordinate pair
// that no longer names a place. Valid geometry, wrong geometry, exit 0.
//
// Carrying the per-row SRID faithfully would mean changing the IR's geometry
// VALUE contract from WKB to EWKB, which every decoder, both writers, the
// mydumper literal path, the VStream cell decoder and the backup change
// codec each read and write — a chunk of its own with its own family matrix.
// Until then the honest answer is the loud one: refuse the row, name both
// SRIDs, and name the remedy, which is to declare the SRID on the COLUMN
// (`geometry(Point,4326)` / `POINT SRID 4326`) where sluice does carry it
// end to end.
//
// columnSRID is [Geometry.SRID] as the schema reader resolved it. rowSRID is
// what the value's own framing carried. A match returns nil; so does the
// pair (0, 0), which is the overwhelmingly common "no spatial reference
// declared anywhere" case and costs one comparison.
func CheckGeometryRowSRID(columnSRID int, rowSRID uint32) error {
	if uint32(columnSRID) == rowSRID {
		return nil
	}
	return fmt.Errorf(
		"%w: value carries SRID %d, column declares SRID %d — the IR carries an SRID per COLUMN, "+
			"not per row, so applying this value would silently re-stamp it as SRID %d. Declare the "+
			"SRID on the column (PostGIS `geometry(<Subtype>,%d)`, MySQL `<TYPE> SRID %d`) so every "+
			"row agrees with it, or normalise the row's SRID at the source",
		ErrGeometryRowSRIDMismatch, rowSRID, columnSRID, columnSRID, rowSRID, rowSRID,
	)
}
