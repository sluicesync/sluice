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
