// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package pgtrigger

import "sluicesync.dev/sluice/internal/ir"

// SourceIdentity implements [ir.SourceIdentityDescriber] by DELEGATING
// to the composed Postgres engine, because a trigger-CDC source speaks
// exactly the same DSN grammar — the engine's own [parseDSNCompat] is a
// deliberate copy of that parser for the same reason.
//
// Delegating rather than copying is load-bearing here: the identity is
// compared as one string, so if the two parsers ever disagreed about a
// database or a schema, a `migrate --resume` across the two drivers
// against ONE database would be refused, and no test that exercised
// either engine alone would notice.
func (e Engine) SourceIdentity(dsn string) ir.SourceIdentity {
	return e.pg.SourceIdentity(dsn)
}

// The surface is discovered by runtime type-assertion off the registry,
// so a method-set break would silently downgrade the resume door to "no
// discriminator". Pinned here, beside the method, so the two move
// together; the registry-derived roster
// (docsync.TestEverySourceEngineDescribesItsIdentity) is what enforces
// that EVERY engine carries one.
var _ ir.SourceIdentityDescriber = Engine{}
