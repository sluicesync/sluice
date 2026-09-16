// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlitetrigger

import "sluicesync.dev/sluice/internal/ir"

// SourceIdentity implements [ir.SourceIdentityDescriber] by DELEGATING
// to the composed cold-start engine: the `sqlite-trigger` driver reads
// the same local file, through the same DSN grammar, as `sqlite`.
//
// Delegation rather than a copy is the point. The identity is persisted
// and compared as one string, so two parsers that disagreed about a path
// would refuse a `migrate --resume` across the two drivers against ONE
// file — and a test exercising either engine alone would not notice.
func (e Engine) SourceIdentity(dsn string) ir.SourceIdentity {
	return e.sq.SourceIdentity(dsn)
}

// Pinned beside the method; the registry-derived roster
// (docsync.TestEverySourceEngineDescribesItsIdentity) enforces coverage.
var _ ir.SourceIdentityDescriber = Engine{}
