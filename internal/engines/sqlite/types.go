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

// irTypeFor maps a column affinity to its IR type. This is the schema-
// side half of the value-fidelity contract; value_decode.go enforces the
// per-row storage-class half against the same affinity.
//
// Note: SQLite has no native DATE / TIME / BOOLEAN storage class, so the
// prototype carries date/bool columns as whatever affinity their
// declared type yields (a DATE column declared `TEXT` → ir.Text; a 0/1
// flag declared `INTEGER`/`BOOLEAN` → ir.Integer). It deliberately does
// not guess a temporal/boolean IR type — see the package doc comment.
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
