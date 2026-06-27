// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"strings"

	"sluicesync.dev/sluice/internal/ir"
)

// affinity is one of SQLite's five column type affinities. The affinity
// is derived from a column's DECLARED type (not its per-row stored
// value) by the rules in https://www.sqlite.org/datatype3.html §3.1.
type affinity uint8

const (
	affinityInteger affinity = iota
	affinityText
	affinityBlob
	affinityReal
	affinityNumeric
)

func (a affinity) String() string {
	switch a {
	case affinityInteger:
		return "INTEGER"
	case affinityText:
		return "TEXT"
	case affinityBlob:
		return "BLOB"
	case affinityReal:
		return "REAL"
	case affinityNumeric:
		return "NUMERIC"
	default:
		return "unknown"
	}
}

// affinityOf computes a column's affinity from its declared type string,
// applying SQLite's rules IN ORDER (the order is load-bearing — e.g.
// "FLOATING POINT" contains both "INT" and "FLOA", and SQLite's rule 1
// — INT wins — means it gets INTEGER affinity, not REAL):
//
//  1. declared type contains "INT"                       → INTEGER
//  2. contains "CHAR", "CLOB", or "TEXT"                 → TEXT
//  3. contains "BLOB", OR no declared type at all        → BLOB
//  4. contains "REAL", "FLOA", or "DOUBLE"               → REAL
//  5. otherwise                                          → NUMERIC
//
// The match is case-insensitive (SQLite uppercases for the comparison).
// Note: rule 4's third token is the SQLite-spec four-letter prefix of
// DOUBLE (so it matches DOUBLE / DOUBLE PRECISION), spelled literally in
// the switch below.
func affinityOf(declaredType string) affinity {
	t := strings.ToUpper(strings.TrimSpace(declaredType))
	switch {
	case strings.Contains(t, "INT"):
		return affinityInteger
	case strings.Contains(t, "CHAR"), strings.Contains(t, "CLOB"), strings.Contains(t, "TEXT"):
		return affinityText
	case t == "", strings.Contains(t, "BLOB"):
		return affinityBlob
	case strings.Contains(t, "REAL"), strings.Contains(t, "FLOA"), strings.Contains(t, "DOUB"): //nolint:misspell // "DOUB" is the SQLite-spec substring for DOUBLE
		return affinityReal
	default:
		return affinityNumeric
	}
}

// resolveColumnType maps a column's DECLARED type string to its IR type.
// It is the single schema-side entry point the reader uses: it applies the
// ADR-0129 declared-temporal/bool policy FIRST (a column the operator named
// DATE / DATETIME / TIMESTAMP / TIME / BOOL[EAN] becomes the corresponding
// IR temporal/boolean type) and falls back to the affinity mapping for
// everything else. The value half (decoding each row per the resolved type
// and the operator's date encoding) lives in value_decode.go.
func resolveColumnType(declaredType string) ir.Type {
	if t, ok := declaredTemporalBoolType(declaredType); ok {
		return t
	}
	return irTypeFor(affinityOf(declaredType))
}

// declaredTemporalBoolType implements ADR-0129's declared-type → IR
// temporal/boolean inference. It matches case-insensitively on a SUBSTRING
// of the declared type (the same matching philosophy as SQLite's own
// affinity rules), in a load-bearing PRECEDENCE order:
//
//  1. contains "DATETIME" or "TIMESTAMP" → ir.Timestamp (no tz; SQLite is
//     tz-naive). Checked first because "DATETIME" also contains "DATE" and
//     "TIME"; without this precedence a DATETIME column would mis-map to Date.
//  2. else contains "DATE" → ir.Date
//  3. else contains "TIME" → ir.Time (no tz)
//  4. else contains "BOOL" → ir.Boolean (covers BOOL and BOOLEAN)
//
// Only these explicit spellings override the affinity default; an
// INTEGER-declared 0/1 column is NOT guessed as bool (that requires an
// explicit BOOL/BOOLEAN declaration). Substring matching is deliberate and
// documented: a contrived declared type like "BIGDATE" contains "DATE" and
// therefore resolves to ir.Date — the same false-positive surface SQLite's
// own affinity matching has, and the price of matching SQLite's convention.
// An operator who hits such a case carries the column raw with
// `--type-override <col>=text`.
//
// ok is false when no temporal/bool spelling is present, so the caller falls
// back to the affinity mapping.
func declaredTemporalBoolType(declaredType string) (ir.Type, bool) {
	t := strings.ToUpper(strings.TrimSpace(declaredType))
	switch {
	case strings.Contains(t, "DATETIME"), strings.Contains(t, "TIMESTAMP"):
		return ir.Timestamp{}, true
	case strings.Contains(t, "DATE"):
		return ir.Date{}, true
	case strings.Contains(t, "TIME"):
		return ir.Time{}, true
	case strings.Contains(t, "BOOL"):
		return ir.Boolean{}, true
	default:
		return nil, false
	}
}

// irTypeFor maps a column affinity to its IR type. This is the affinity
// half of the schema-side value-fidelity contract; value_decode.go enforces
// the per-row storage-class half against the same affinity. Declared
// temporal/bool spellings are handled ahead of this by
// [declaredTemporalBoolType] (ADR-0129).
func irTypeFor(a affinity) ir.Type {
	switch a {
	case affinityInteger:
		// SQLite INTEGERs are 64-bit signed (up to 8 bytes on disk).
		return ir.Integer{Width: 64}
	case affinityText:
		// SQLite TEXT is unbounded; the largest IR text bucket is the
		// faithful, lossless target (declared VARCHAR(n) lengths are not
		// enforced by SQLite, so we don't carry a misleading bound).
		return ir.Text{Size: ir.TextLong}
	case affinityBlob:
		return ir.Blob{Size: ir.BlobLong}
	case affinityReal:
		// SQLite REAL is an 8-byte IEEE-754 float.
		return ir.Float{Precision: ir.FloatDouble}
	case affinityNumeric:
		// NUMERIC affinity is the arbitrary-precision exact-numeric
		// catch-all; map to bare (unconstrained) decimal so a target
		// NUMERIC holds it without an invented precision/scale.
		return ir.Decimal{Unconstrained: true}
	default:
		// Unreachable: affinityOf only returns the five constants.
		return ir.Blob{Size: ir.BlobLong}
	}
}
