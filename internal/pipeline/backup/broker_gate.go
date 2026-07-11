// Copyright 2026 Omar Ramos
// SPDX-License-Identifier: Apache-2.0

package backup

// Broker-callable entry points into the restore-side integrity gates
// (BRK-2/3/4). The live-apply broker (`sync from-backup`) lives in
// package pipeline and cannot see this package's unexported gate
// functions; these thin exported wrappers give it chain-restore parity
// without widening the package surface — verifyMaterial and the
// per-gate helpers stay unexported, and the wrappers delegate to the
// SAME code ChainRestore.Run runs inline (so the two consumers can
// never drift). Each wrapper is a pure forward; the doc for the
// behaviour lives on the wrapped function.

import (
	"context"
	stdcrypto "crypto"

	"sluicesync.dev/sluice/internal/crypto"
	irbackup "sluicesync.dev/sluice/internal/ir/backup"
	"sluicesync.dev/sluice/internal/pipeline/lineage"
)

// VerifyChainSignatures runs the ADR-0154 whole-chain signature +
// freshness gate (see [verifyChainSignatures]) for the broker. env is
// the read envelope (HMAC-off-KEK verifier) and verifyPub the asymmetric
// public key (`--verify-key`, Ed25519 / KMS); requireStrict is
// `--require-signature`. A genuinely unsigned chain with no signature
// objects is a no-op unless requireStrict is set.
func VerifyChainSignatures(
	ctx context.Context,
	rootStore irbackup.Store,
	links []lineage.SegmentRecord,
	env crypto.EnvelopeEncryption,
	verifyPub stdcrypto.PublicKey,
	requireStrict bool,
) error {
	return verifyChainSignatures(ctx, rootStore, links, verifyMaterial{env: env, verifyPub: verifyPub}, requireStrict)
}

// ValidateManifestStructure is the exported form of
// [validateManifestStructure]: it rejects a manifest carrying a null
// structural element so the broker gives the coded refusal restore does
// instead of nil-derefing mid-tick (Bug 182 / BRK-4).
func ValidateManifestStructure(m *irbackup.Manifest) error {
	return validateManifestStructure(m)
}

// VerifySchemaHashes is the exported form of [verifySchemaHashes]: it
// recomputes every link's schema fingerprint and compares it against the
// manifest-recorded hash, catching schema-fingerprint corruption before
// the broker applies deltas (BRK-4).
func VerifySchemaHashes(ctx context.Context, links []lineage.SegmentRecord) error {
	return verifySchemaHashes(ctx, links)
}

// CheckMixedModeChain is the exported form of [checkMixedModeChain]: it
// refuses a lineage where a segment full and one of its incrementals
// disagree on encryption shape (an encrypted chain must not carry a
// plaintext incremental), the manifest-level half of the broker's
// mixed-mode refusal (BRK-3).
func CheckMixedModeChain(chain []lineage.SegmentRecord) error {
	return checkMixedModeChain(chain)
}
