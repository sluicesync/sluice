// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package lineage

import (
	"context"
	"log/slog"

	irbackup "sluicesync.dev/sluice/internal/ir/backup"
)

// A green signature does not mean the same thing on every chain.
//
// ADR-0183 added the manifest's `schema` member to the signed canonical
// bytes at `sluice-manifest-canon/v5`. Every earlier canon covered the
// schema only through the `schema_hash` STRING recorded next to it —
// which authenticates nothing against an attacker who edits the schema
// and leaves the hash alone, on the chain shapes whose restore never
// recomputes the fingerprint. That is the forgery ADR-0183 closed.
//
// It closed it for chains signed FROM v5 ONWARD, and only those. A
// signature cannot be strengthened retroactively: the MAC covers the
// bytes it covered, ADR-0183 preserves every retired canon rendering
// byte-for-byte precisely so old chains keep verifying, and there is no
// re-seal migration. So an operator who reads a headline about a
// schema-forgery fix, upgrades, and points the new binary at the chains
// they already have gets `signature valid` — over a manifest whose
// schema is still fully forgeable.
//
// The design is right and does not change. What was missing is that
// nothing SAID so: `--require-signature`, the flag whose entire purpose
// is to fail closed, reported a forged pre-v5 manifest as valid with no
// qualification of any kind. This is the same shape as ADR-0154 §11's
// format-floor warning (see format_floor.go) — a correct per-artifact
// decision whose consequence the operator could not see — and it takes
// the same remedy: say it, at every verification, and name the fix.
//
// Deliberately a WARN and not a refusal. Refusing a pre-v5 canon by
// default would break every chain in existence on upgrade day, which is
// the trade ADR-0181 got right by warning. A strict `refuse anything
// below v5` policy is a reasonable future flag; it is NOT wired today,
// and that absence is recorded in ADR-0183's residual section rather
// than implied.
//
// Bug 220, found by the v0.108.0 regression cycle.

// WarnPreSchemaCanonSignature logs a warning when a signature that
// VERIFIED GREEN was recorded under a canon version whose bytes do not
// cover the manifest's schema — naming the artifact, the recorded
// version, exactly what stays forgeable, and the only remedy (re-take
// the backup with this release; a pre-v5 chain cannot be re-signed into
// coverage).
//
// A no-op for a signature at [irbackup.ManifestCanonVersion] or any
// future version that covers the schema, so a chain written entirely by
// a current release never warns. It is called from [VerifyManifest]
// itself rather than from each of the three call sites (single-manifest
// restore, chain restore, `backup verify`) so no verification entry
// point — present or future — can be added without it. Scope is the
// MANIFEST signature specifically: the lineage-catalog signature covers
// the link ENUMERATION, which has no schema in it, so its canon version
// is not part of this residual.
//
// One line per manifest, matching the per-manifest INFO the verify
// paths already emit, so a long chain's output stays symmetric — and a
// mixed chain (old links at v4, links taken since the upgrade at v5)
// shows exactly which links lack coverage.
func WarnPreSchemaCanonSignature(ctx context.Context, artifact, canonVersion string) {
	if irbackup.ManifestCanonCoversSchema(canonVersion) {
		return
	}
	msg := "backup: this signature verified under " + canonVersion +
		", a canonicalization that does NOT authenticate the manifest's schema — only the schema_hash string beside it, which an editor of the schema can restate. " +
		"The rows and the chain structure are covered; the SCHEMA is not, so dropped CHECK/UNIQUE/FK/EXCLUDE constraints and flipped RLS flags would restore silently and NOT fail this signature. " +
		"An existing chain cannot be re-signed into coverage — take a fresh full backup with this release (" + irbackup.ManifestCanonVersion + ") for a signature that covers the schema"
	slog.WarnContext(
		ctx, msg,
		slog.String("artifact", artifact),
		slog.String("canon_version", canonVersion),
		slog.String("schema_covered_from", irbackup.ManifestCanonVersion),
	)
}
