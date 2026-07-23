// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package postgres

import "sluicesync.dev/sluice/internal/ir"

// CollationResolver implements [ir.CollationResolverProvider] for continuous
// FILTERED sync (ADR-0173 Phase 2; audit 2026-07-18 M2.1). Postgres string `=`
// is BYTE-EXACT for the default collation and for any DETERMINISTIC named
// collation (libc C/POSIX/en_US, deterministic ICU — pg_collation.
// collisdeterministic=true), and collation-aware — not client-reproducible —
// for a NON-deterministic ICU collation. The engine-neutral
// [ir.ByteExactCollationResolver] encodes exactly that determinism lens, so the
// evaluator reasons about a Postgres source through Postgres's own semantics
// with no MySQL/Vitess collation library in the path — PLUS the one
// Postgres-specific twist that lens deliberately omits: bpchar PAD SPACE.
func (Engine) CollationResolver() ir.CollationResolver { return pgCollationResolver{} }

// pgCollationResolver is the Postgres [ir.CollationResolver]: the generic
// byte-exact-or-refuse determinism lens PLUS Postgres char(n)/bpchar semantics.
// bpchar `=` is PAD SPACE (trailing-space-INSENSITIVE) regardless of collation —
// `'EU'::char(4)` equals `'EU  '` — UNLIKE text/varchar, and logical decoding
// delivers bpchar space-PADDED to width. So a fixed-length CHAR column under a
// faithful (deterministic/default) collation must trim trailing spaces before a
// byte compare, or a CDC row-move is silently mis-classified (audit 2026-07-19
// A2). This padding is Postgres-specific and layered HERE rather than in the
// engine-neutral [ir.ByteExactCollationResolver], which stays pad-agnostic so a
// future SQLite source (whose CHAR does not pad) is not tainted.
type pgCollationResolver struct{}

func (pgCollationResolver) ResolveStringEquality(collation string, determinism ir.CollationDeterminism, strict, fixedChar bool) ir.StringEquality {
	eq := ir.ByteExactCollationResolver{}.ResolveStringEquality(collation, determinism, strict, fixedChar)
	if eq.Faithful && fixedChar {
		// char(n)/bpchar `=` pads to width and ignores trailing spaces.
		eq.PadSpace = true
	}
	return eq
}

// ResolveTemporalLiteralSemantics implements [ir.TemporalLiteralResolver]:
// Postgres casts an unknown-typed temporal literal to the COLUMN's type.
// Observed on PG 16.14 (2026-07-23): `d < '2026-01-15 12:00'` on a date
// column plans — and is stored in a publication row filter — as
// `(d < '2026-01-15'::date)`, the time-of-day silently truncated; a
// fractional second beyond the µs timestamp resolution rounds HALF-EVEN
// ('.1234565'::timestamp → .123456, '.1234575' → .123458), carrying into
// the seconds ('.9999995' → +1s); a typmod column (timestamp(0)) does NOT
// truncate the literal — comparison runs at the type's µs resolution.
// rowpredicate.Compile normalizes literals under this rule so the client
// evaluator, the snapshot SELECT, and the pushed publication filter agree
// (audit 2026-07-23 D0-5 / Q1).
func (pgCollationResolver) ResolveTemporalLiteralSemantics() ir.TemporalLiteralSemantics {
	return ir.TemporalLiteralCastToColumn
}
