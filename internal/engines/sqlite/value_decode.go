// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"fmt"
	"strconv"

	"sluicesync.dev/sluice/internal/ir"
)

// storageClass names the SQLite storage class of a raw value as returned
// by modernc.org/sqlite when scanned into an *any: the driver hands back
// the value's ACTUAL on-disk storage class as a Go type:
//
//	NULL    → nil
//	INTEGER → int64
//	REAL    → float64
//	TEXT    → string
//	BLOB    → []byte
//
// This is what gives sluice per-row storage-class fidelity for free: the
// reported Go type IS the row's storage class, independent of the
// column's declared affinity.
func storageClass(raw any) string {
	switch raw.(type) {
	case nil:
		return "NULL"
	case int64:
		return "INTEGER"
	case float64:
		return "REAL"
	case string:
		return "TEXT"
	case []byte:
		return "BLOB"
	default:
		return fmt.Sprintf("unexpected-driver-type(%T)", raw)
	}
}

// decodeCell maps one SQLite value to its IR Row value, dispatching on
// the value's ACTUAL storage class and the column's RESOLVED affinity
// (carried as the IR type t). When the storage class cannot be
// faithfully represented in t, it returns a storage-class-mismatch error
// (see [mismatchError]) — the loud-failure tenet: a value the target
// can't faithfully hold is refused, never silently coerced to a
// wrong-but-plausible value.
//
// NULL is faithful for every type. The accepted (faithful) storage
// classes per affinity — and why the others are refused — are:
//
//	INTEGER affinity (ir.Integer):  INTEGER→int64.
//	  REAL/TEXT/BLOB refused (a fractional/non-numeric/binary value
//	  cannot be an int64 without loss; SQLite leaves such values in
//	  their original class rather than coercing on insert).
//	REAL affinity (ir.Float):       REAL→float64.
//	  INTEGER/TEXT/BLOB refused (REAL affinity coerces integers to
//	  float on insert, so a stored INTEGER here is anomalous; TEXT/BLOB
//	  are non-numeric).
//	TEXT affinity (ir.Text):        TEXT→string.
//	  BLOB refused (a blob may not be valid UTF-8 text); INTEGER/REAL
//	  do not occur (TEXT affinity coerces numbers to text on insert).
//	BLOB affinity (ir.Blob):        BLOB→[]byte.
//	  INTEGER/REAL/TEXT refused — BLOB affinity (and the no-declared-
//	  type case) applies NO coercion, so a column can hold any class;
//	  the prototype refuses non-blob values rather than choosing a byte
//	  encoding for them.
//	NUMERIC affinity (ir.Decimal):  INTEGER→decimal-string, REAL→decimal-string.
//	  TEXT/BLOB refused (a TEXT value in a NUMERIC column is one SQLite
//	  could not parse as a number; a BLOB is binary).
func decodeCell(raw any, t ir.Type) (any, error) {
	if raw == nil {
		return nil, nil // NULL — faithful for every IR type.
	}

	switch t.(type) {
	case ir.Integer:
		if v, ok := raw.(int64); ok {
			return v, nil
		}
		return nil, mismatchError(raw, t)

	case ir.Float:
		if v, ok := raw.(float64); ok {
			return v, nil
		}
		return nil, mismatchError(raw, t)

	case ir.Text:
		if v, ok := raw.(string); ok {
			return v, nil
		}
		return nil, mismatchError(raw, t)

	case ir.Blob:
		if v, ok := raw.([]byte); ok {
			// modernc may reuse the backing buffer across rows; copy so
			// the IR Row value is safe to retain.
			out := make([]byte, len(v))
			copy(out, v)
			return out, nil
		}
		return nil, mismatchError(raw, t)

	case ir.Decimal:
		// Both exact-numeric storage classes carry losslessly into the
		// IR's decimal-as-string contract: an int64 is an exact integer,
		// and FormatFloat(g, -1) is the shortest round-trippable decimal
		// for the float64. TEXT/BLOB are refused by falling through.
		switch v := raw.(type) {
		case int64:
			return strconv.FormatInt(v, 10), nil
		case float64:
			return strconv.FormatFloat(v, 'g', -1, 64), nil
		}
		return nil, mismatchError(raw, t)
	}

	return nil, fmt.Errorf("sqlite: no decoder for IR type %T", t)
}

// mismatchError builds the loud refusal for a storage-class / affinity
// mismatch. It names the offending storage class and the resolved IR
// type; the row reader wraps it with the table, column, and rowid so the
// operator can find the exact offending cell. The message points at the
// future opt-in override that may relax the refusal.
func mismatchError(raw any, t ir.Type) error {
	return fmt.Errorf(
		"storage-class mismatch: value stored as %s cannot be faithfully represented as IR %s; "+
			"refusing to coerce (loud-failure prototype — a future --sqlite-relax-affinity override may relax this)",
		storageClass(raw), t.String(),
	)
}
