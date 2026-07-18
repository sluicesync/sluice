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
// with no MySQL/Vitess collation library in the path.
func (Engine) CollationResolver() ir.CollationResolver { return ir.ByteExactCollationResolver{} }
