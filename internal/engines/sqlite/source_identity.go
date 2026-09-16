// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package sqlite

import (
	"path/filepath"

	"sluicesync.dev/sluice/internal/ir"
)

// SourceIdentity implements [ir.SourceIdentityDescriber] for the local
// SQLite-file engine: the FILE the DSN names is the dataset.
//
// A file path is not a host, so it belongs in the identity (ADR-0015
// excludes the host so a DNS move or a failover still resumes; two
// different files are two different sources by any reading). Before
// this existed the orchestrator recognised only Postgres and MySQL DSN
// shapes, so EVERY SQLite source — every file, every dump — rendered one
// identity and `migrate --resume` pointed at a second database adopted
// the first one's completed copy, exiting 0 having copied nothing
// (audit 2026-09-15 A0915-VF2-F1).
//
// [dsnFormParts] is the shared normaliser the read and write paths both
// use, so the three accepted spellings collapse the way the connection
// collapses them: a bare path, `file:...` (query trimmed), and
// `sqlite://...`. It touches no filesystem. The result is
// [filepath.Clean]ed so `./app.db` and `app.db` are one source rather
// than two — the remaining difference, a relative path resolved from a
// different working directory, refuses rather than adopts, which is the
// safe direction.
//
// For an ADR-0130 text dump the path is still the operator's original
// source, never the materialised temp database, which is what keeps the
// identity stable across re-runs.
func (Engine) SourceIdentity(dsn string) ir.SourceIdentity {
	_, path, _, err := dsnFormParts(dsn)
	if err != nil {
		return ir.SourceIdentity{}
	}
	return ir.SourceIdentity{Database: filepath.Clean(path)}
}

// SourceIdentity implements [ir.SourceIdentityDescriber] for the `d1`
// engine; see [D1SourceIdentity] for what it reports and why.
func (d1Engine) SourceIdentity(dsn string) ir.SourceIdentity {
	return D1SourceIdentity(dsn)
}

// D1SourceIdentity reports the dataset a `d1://` DSN names: the D1
// DATABASE ID, which is globally unique and carries nothing secret (the
// API token is environment-only and never appears in a DSN).
//
// The ACCOUNT id is deliberately excluded, for the reason ADR-0015
// excludes the host: it is how to reach the database, not which database
// it is, and `d1://<account>/<db>` and `d1://<db>` (account from
// CLOUDFLARE_ACCOUNT_ID) are two spellings of one source that must
// resume across each other.
//
// Exported so the `d1-trigger` engine can report the identical identity
// by CALLING it rather than by copying the rule — the two engines read
// the same database, and an identity that differed between the drivers
// would refuse a legitimate resume across them.
func D1SourceIdentity(dsn string) ir.SourceIdentity {
	_, databaseID, _, err := parseD1DSN(dsn)
	if err != nil {
		return ir.SourceIdentity{}
	}
	return ir.SourceIdentity{Database: databaseID}
}

// Pinned beside the methods: the surface is discovered by runtime
// type-assertion off the registry, so a method-set break would silently
// downgrade the resume door rather than fail the build. Both engine
// types this package registers are covered; the registry-derived roster
// (docsync.TestEverySourceEngineDescribesItsIdentity) is what enforces
// that no registered engine is missing one.
var (
	_ ir.SourceIdentityDescriber = Engine{}
	_ ir.SourceIdentityDescriber = d1Engine{}
)
