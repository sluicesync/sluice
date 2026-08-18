// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

// The `macaddr8[]` binary-COPY codec (roadmap item 117 / Bug 225).
//
// # The measured defect
//
// pgx v5 registers a codec for the `macaddr8` SCALAR (OID 774) and for the
// `_macaddr` array (OID 1040), but NOT for the `_macaddr8` array (OID 775).
// So a macaddr8[] column in a binary COPY got no ArrayCodec, pgx fell back to
// encoding the elements as their Go type, and the server refused the stream:
//
//	ERROR: binary data has array element type 25 (text) instead of
//	expected 774 (macaddr8) (SQLSTATE 42804)
//
// This is the Bug-74 shape exactly — sluice's own code path is BYTE-IDENTICAL
// for the two widths (convertArray dispatches on ir.Macaddr and builds
// pgtype.Text leaves either way), and the outcome differs because the pgx
// codec keyed on the TARGET OID differs. It was found by the four-cell probe
// {macaddr, macaddr8} × {scalar, array}, MEASURED against a real PG 16, not by
// reasoning: three cells pass on stock pgx and the fourth does not.
//
// # Why register rather than fall back to the batched-INSERT path
//
// [tableHasVerbatimColumn] and [tableHasIntervalColumn] answer a similar
// problem by routing the whole table off binary COPY. That is right for those
// two, whose values have no sluice-side wire codec at all. Here the element
// codec exists and is already correct — pgx encodes a macaddr8 SCALAR fine —
// and only the array wrapper is unregistered. Registering it costs four lines
// and keeps the fast path; demoting the table would cost every OTHER column in
// it the COPY throughput because of one array.

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"sluicesync.dev/sluice/internal/ir"
)

// tableHasMacaddr8ArrayColumn reports whether table carries a `macaddr8[]`
// column — the one shape stock pgx cannot binary-encode. Mirrors
// [tableHasTimetzColumn]. The 6-byte array is deliberately NOT included: pgx
// registers `_macaddr` itself and re-registering is pointless noise.
func tableHasMacaddr8ArrayColumn(table *ir.Table) bool {
	if table == nil {
		return false
	}
	for _, col := range table.Columns {
		if col == nil {
			continue
		}
		// UnwrapDomain (Bug 233): a DOMAIN over macaddr8[] COPYs as the same
		// array whose element codec stock pgx does not register (OID 775).
		arr, ok := ir.UnwrapDomain(col.Type).(ir.Array)
		if !ok {
			continue
		}
		if m, ok := arr.Element.(ir.Macaddr); ok && m.Width == ir.MacaddrEUI64 {
			return true
		}
	}
	return false
}

// registerPGMacaddr8ArrayCodec registers an [pgtype.ArrayCodec] for
// `_macaddr8` on conn, wrapping whatever element codec pgx already has for the
// macaddr8 scalar — so the array inherits the scalar's encoding rather than
// this package inventing a second one. Like [registerPGTimetzCodec] there is
// no OID lookup: `_macaddr8` is a core type at the stable catalog OID
// [macaddr8ArrayOID]. Idempotent.
func registerPGMacaddr8ArrayCodec(conn *pgx.Conn) error {
	tm := conn.TypeMap()
	if _, already := tm.TypeForOID(macaddr8ArrayOID); already {
		return nil
	}
	elem, ok := tm.TypeForOID(pgtype.Macaddr8OID)
	if !ok {
		// pgx dropping its macaddr8 scalar codec would break the scalar
		// column too; say so rather than registering an array over nothing.
		return errors.New("postgres: copy: cannot register the macaddr8[] codec — " +
			"the driver has no codec for the macaddr8 scalar (OID 774) to wrap")
	}
	tm.RegisterType(&pgtype.Type{
		Name:  "_macaddr8",
		OID:   macaddr8ArrayOID,
		Codec: &pgtype.ArrayCodec{ElementType: elem},
	})
	return nil
}
