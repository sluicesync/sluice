// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package d1trigger

import (
	"sluicesync.dev/sluice/internal/engines/sqlite"
	"sluicesync.dev/sluice/internal/ir"
)

// SourceIdentity implements [ir.SourceIdentityDescriber] by CALLING the
// composed `d1` engine's rule ([sqlite.D1SourceIdentity]) rather than
// re-deriving it: the two drivers address the same D1 database through
// the same `d1://` grammar, and an identity that differed between them
// would refuse a legitimate `migrate --resume` across the pair.
//
// The dataset is the D1 database id; the account id is excluded for the
// reason ADR-0015 excludes the host. Nothing secret appears — the API
// token is environment-only and never rides in a DSN.
func (Engine) SourceIdentity(dsn string) ir.SourceIdentity {
	return sqlite.D1SourceIdentity(dsn)
}

// Pinned beside the method; the registry-derived roster
// (docsync.TestEverySourceEngineDescribesItsIdentity) enforces coverage.
var _ ir.SourceIdentityDescriber = Engine{}
