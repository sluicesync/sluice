// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package mysql

import "sluicesync.dev/sluice/internal/ir"

// SourceIdentity implements [ir.SourceIdentityDescriber]: the database a
// MySQL DSN names, read by the DRIVER'S OWN parser so every spelling the
// driver accepts resolves to the same answer the connection will use.
//
// That is the fix, not an implementation detail. The pipeline's first
// cut sniffed for `@tcp(` / `@unix(` and then cut the remainder at the
// first `?`, so `root:pw@/db` — a form the shipped driver accepts,
// carrying no protocol section — was not recognised as a MySQL DSN at
// all and rendered an EMPTY database, as did `@tcp(...)` whenever the
// password contained a `?`. Two different databases then produced one
// identity and `migrate --resume` adopted the wrong copy (audit
// 2026-09-15 F-1). It also means the two spellings of one source —
// `root:pw@/db1` and `root:pw@tcp(127.0.0.1:3306)/db1` — now render the
// SAME identity, where before they rendered two and a re-spelled DSN
// refused a legitimate resume.
//
// [parseServerDSN], not [parseDSN]: the database-OPTIONAL entry point,
// because a server-level DSN (the multi-database fan-out's, ADR-0074)
// has a legitimate identity — its empty database — and refusing to
// describe it would hand the door less evidence, not more.
//
// The SCHEMA is empty for every flavor: MySQL's namespace scope is flat
// ([ir.SchemaScopeFlat] in each flavor's capabilities), its database IS
// its namespace, and that is already carried by Database. Duplicating it
// would add a second field and no discrimination.
//
// The host, the port and the credentials are all dropped (ADR-0015).
func (Engine) SourceIdentity(dsn string) ir.SourceIdentity {
	cfg, err := parseServerDSN(dsn)
	if err != nil {
		// Refused loudly at open with the driver's own message plus the
		// shape hint; the resume door reports "no discriminator" rather
		// than diagnosing a connection here.
		return ir.SourceIdentity{}
	}
	return ir.SourceIdentity{Database: cfg.DBName}
}

// Pinned beside the method: the orchestrator discovers this surface by
// runtime type-assertion, so a method-set break would silently downgrade
// the resume door instead of failing the build. The registry-derived
// roster (docsync.TestEverySourceEngineDescribesItsIdentity) enforces
// that every engine carries one; this covers all four flavors, which
// share the type.
var _ ir.SourceIdentityDescriber = Engine{}
